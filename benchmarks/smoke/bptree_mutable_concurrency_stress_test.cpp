/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "TestErrorHandlers.hpp"

#include "akk/engine/memtable/BPTreeMemTable.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <exception>
#include <mutex>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {
    using akkaradb::core::RecordView;
    using akkaradb::engine::memtable::BPTreeMemTable;

    [[nodiscard]] std::span<const uint8_t> bytes(const std::vector<uint8_t>& value) noexcept {
        return {value.data(), value.size()};
    }

    [[nodiscard]] std::vector<uint8_t> keyFor(size_t value) {
        std::string text = "key-";
        text += std::to_string(1'000'000'000ULL + value);
        return {text.begin(), text.end()};
    }

    [[nodiscard]] std::vector<uint8_t> valueFor(size_t keyIndex) {
        std::string text = "value-";
        text += std::to_string(keyIndex);
        return {text.begin(), text.end()};
    }

    [[nodiscard]] uint64_t nextRandom(uint64_t& state) noexcept {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        return state;
    }

    void require(bool condition, const char* message) {
        if (!condition) { throw std::runtime_error(message); }
    }

    void recordFailure(std::atomic<bool>& failed, std::mutex& mutex, std::string& message, std::string failure) {
        bool expected = false;
        if (failed.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
            std::lock_guard lock{mutex};
            message = std::move(failure);
        }
    }

    void verifyFullTreeAfterStress(
        BPTreeMemTable& table,
        const std::vector<std::vector<uint8_t>>& keys,
        uint64_t snapshotSeq
    ) {
        for (size_t i = 0; i < keys.size(); ++i) {
            RecordView found;
            require(table.get(std::as_bytes(bytes(keys[i])), snapshotSeq, &found), "post-stress point lookup missed a key");
            require(std::ranges::equal(found.key(), bytes(keys[i])), "post-stress point lookup returned the wrong key");
        }

        size_t expected = 0;
        for (const RecordView& current : table.iterator({}, {}, snapshotSeq)) {
            require(expected < keys.size(), "post-stress full scan returned too many records");
            require(std::ranges::equal(current.key(), bytes(keys[expected])), "post-stress full scan returned out-of-order keys");
            ++expected;
        }
        require(expected == keys.size(), "post-stress full scan missed keys");
    }

    void verifyConcurrentMutablePointLookups() {
        constexpr size_t keyCount = 8'192;
        constexpr size_t readerThreads = 4;
        constexpr auto maxRuntime = std::chrono::seconds{10};

        BPTreeMemTable table;
        std::vector<std::vector<uint8_t>> keys;
        std::vector<std::vector<uint8_t>> values;
        std::vector<size_t> keyIndexBySeq(keyCount + 1);
        keys.reserve(keyCount);
        values.reserve(keyCount);

        for (size_t i = 0; i < keyCount; ++i) {
            keys.push_back(keyFor(i));
            values.push_back(valueFor(i));
        }

        for (size_t seq = 1; seq <= keyCount; ++seq) {
            keyIndexBySeq[seq] = ((seq - 1) * 9973) % keyCount;
        }

        std::atomic<uint64_t> appliedSeq{0};
        std::atomic<bool> writerDone{false};
        std::atomic<bool> failed{false};
        std::mutex failureMutex;
        std::string failureMessage;

        std::thread writer{[&] {
            try {
                for (uint64_t seq = 1; seq <= keyCount && !failed.load(std::memory_order_acquire); ++seq) {
                    const size_t keyIndex = keyIndexBySeq[seq];
                    const auto status = table.put(
                        std::as_bytes(bytes(keys[keyIndex])),
                        std::as_bytes(bytes(values[keyIndex])),
                        seq,
                        0
                    );
                    if (!status.ok()) {
                        recordFailure(failed, failureMutex, failureMessage, "writer put failed");
                        break;
                    }
                    appliedSeq.store(seq, std::memory_order_release);
                }
            }
            catch (const std::exception& ex) {
                recordFailure(failed, failureMutex, failureMessage, std::string{"writer threw: "} + ex.what());
            }
            writerDone.store(true, std::memory_order_release);
        }};

        std::vector<std::thread> readers;
        readers.reserve(readerThreads);
        for (size_t readerId = 0; readerId < readerThreads; ++readerId) {
            readers.emplace_back([&, readerId] {
                uint64_t rng = 0x9e3779b97f4a7c15ULL ^ (readerId + 1);
                size_t lookups = 0;
                while (!writerDone.load(std::memory_order_acquire) && !failed.load(std::memory_order_acquire)) {
                    const uint64_t snapshot = appliedSeq.load(std::memory_order_acquire);
                    if (snapshot == 0) {
                        std::this_thread::yield();
                        continue;
                    }

                    const uint64_t seq = (nextRandom(rng) % snapshot) + 1;
                    const size_t keyIndex = keyIndexBySeq[seq];
                    RecordView found;
                    if (!table.get(std::as_bytes(bytes(keys[keyIndex])), snapshot, &found)) {
                        recordFailure(
                            failed,
                            failureMutex,
                            failureMessage,
                            "concurrent get missed visible key at snapshot " + std::to_string(snapshot)
                        );
                        break;
                    }
                    if (!std::ranges::equal(found.key(), bytes(keys[keyIndex]))) {
                        recordFailure(failed, failureMutex, failureMessage, "concurrent get returned the wrong key");
                        break;
                    }
                    if ((++lookups & 0x3fU) == 0) { std::this_thread::yield(); }
                }
            });
        }

        const auto deadline = std::chrono::steady_clock::now() + maxRuntime;
        while (!writerDone.load(std::memory_order_acquire) && !failed.load(std::memory_order_acquire)) {
            if (std::chrono::steady_clock::now() > deadline) {
                recordFailure(failed, failureMutex, failureMessage, "stress timed out");
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{10});
        }

        writer.join();
        for (auto& reader : readers) { reader.join(); }

        if (failed.load(std::memory_order_acquire)) {
            std::lock_guard lock{failureMutex};
            throw std::runtime_error(failureMessage);
        }

        verifyFullTreeAfterStress(table, keys, keyCount);
    }
} // namespace

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();

    try {
        verifyConcurrentMutablePointLookups();
        return 0;
    }
    catch (const std::exception& ex) {
        std::fprintf(stderr, "BPTree mutable concurrency stress failed: %s\n", ex.what());
        return 1;
    }
}

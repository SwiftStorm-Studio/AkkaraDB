/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// benchmarks/smoke/bptree_mutable_concurrency_stress_test.cpp
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
    using akkaradb::engine::memtable::BPTreeIteratorMode;
    using akkaradb::engine::memtable::BPTreeMemTable;
    using akkaradb::engine::memtable::MemTableBackendOptions;

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

    void verifyLockedIteratorDoesNotHoldWriteLock() {
        BPTreeMemTable table;
        std::vector<std::vector<uint8_t>> keys;
        keys.reserve(128);
        for (size_t i = 0; i < 128; ++i) {
            keys.push_back(keyFor(i));
            const std::vector<uint8_t> value = valueFor(i);
            require(
                table.put(std::as_bytes(bytes(keys.back())), std::as_bytes(bytes(value)), i + 1, 0).ok(),
                "iterator materialization setup insert failed"
            );
        }

        auto heldIterator = table.iterator({}, {}, 128);
        auto current = heldIterator.begin();
        require(current != heldIterator.end(), "materialized iterator must yield the first record");
        require(std::ranges::equal((*current).key(), bytes(keys.front())), "materialized iterator returned the wrong first key");

        const std::vector<uint8_t> extraKey = keyFor(1'000);
        const std::vector<uint8_t> extraValue = valueFor(1'000);
        std::atomic<bool> writerDone{false};
        bool writerOk = false;

        std::thread writer{[&] {
            writerOk = table.put(std::as_bytes(bytes(extraKey)), std::as_bytes(bytes(extraValue)), 129, 0).ok();
            writerDone.store(true, std::memory_order_release);
        }};

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
        while (!writerDone.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }

        if (!writerDone.load(std::memory_order_acquire)) {
            heldIterator = decltype(heldIterator){};
            writer.join();
            throw std::runtime_error("materialized iterator held the BPTree write lock");
        }

        writer.join();
        require(writerOk, "writer must succeed while a materialized iterator is held");

        size_t count = 0;
        for (; current != heldIterator.end(); ++current) {
            require(count < keys.size(), "materialized iterator returned too many records");
            require(std::ranges::equal((*current).key(), bytes(keys[count])), "materialized iterator returned out-of-order keys");
            ++count;
        }
        require(count == keys.size(), "materialized iterator must preserve the snapshot captured at iterator creation");
    }

    void verifyBatchedIteratorReleasesWriteLockBetweenBatches() {
        MemTableBackendOptions options;
        options.bptreeIteratorMode = BPTreeIteratorMode::MATERIALIZE_BATCHED_UNPINNED;
        options.bptreeIteratorBatchSize = 8;

        BPTreeMemTable table(
            akkaradb::core::BufferArena::DEFAULT_INITIAL_BLOCK_SIZE,
            akkaradb::core::BufferArena::DEFAULT_MAX_BLOCK_SIZE,
            64 * 1024,
            2 * 1024 * 1024,
            options
        );

        std::vector<std::vector<uint8_t>> keys;
        keys.reserve(128);
        for (size_t i = 0; i < 128; ++i) {
            keys.push_back(keyFor(i));
            const std::vector<uint8_t> value = valueFor(i);
            require(
                table.put(std::as_bytes(bytes(keys.back())), std::as_bytes(bytes(value)), i + 1, 0).ok(),
                "batched iterator setup insert failed"
            );
        }

        auto iterator = table.iterator({}, {}, 128);
        auto current = iterator.begin();
        require(current != iterator.end(), "batched iterator must yield the first record");
        require(std::ranges::equal((*current).key(), bytes(keys.front())), "batched iterator returned the wrong first key");

        const std::vector<uint8_t> extraKey = keyFor(1'000);
        const std::vector<uint8_t> extraValue = valueFor(1'000);
        std::atomic<bool> writerDone{false};
        bool writerOk = false;
        std::thread writer{[&] {
            writerOk = table.put(std::as_bytes(bytes(extraKey)), std::as_bytes(bytes(extraValue)), 129, 0).ok();
            writerDone.store(true, std::memory_order_release);
        }};

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
        while (!writerDone.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }

        if (!writerDone.load(std::memory_order_acquire)) {
            iterator = decltype(iterator){};
            writer.join();
            throw std::runtime_error("batched iterator held the BPTree write lock between batches");
        }

        writer.join();
        require(writerOk, "writer must succeed while a batched iterator is held between batches");

        size_t count = 1;
        ++current;
        for (; current != iterator.end(); ++current) {
            require(count < keys.size(), "batched iterator returned too many snapshot records");
            require(std::ranges::equal((*current).key(), bytes(keys[count])), "batched iterator returned out-of-order keys");
            ++count;
        }
        require(count == keys.size(), "batched iterator must preserve visibility for the existing key set");
    }

    void verifyFrozenIteratorIgnoresLockedMaterializationMode() {
        MemTableBackendOptions options;
        options.bptreeIteratorMode = BPTreeIteratorMode::MATERIALIZE_BATCHED_UNPINNED;
        options.bptreeIteratorBatchSize = 1;

        BPTreeMemTable table(
            akkaradb::core::BufferArena::DEFAULT_INITIAL_BLOCK_SIZE,
            akkaradb::core::BufferArena::DEFAULT_MAX_BLOCK_SIZE,
            64 * 1024,
            2 * 1024 * 1024,
            options
        );

        std::vector<std::vector<uint8_t>> keys;
        keys.reserve(256);
        for (size_t i = 0; i < 256; ++i) {
            keys.push_back(keyFor(i));
            const std::vector<uint8_t> value = valueFor(i);
            require(table.put(std::as_bytes(bytes(keys.back())), std::as_bytes(bytes(value)), i + 1, 0).ok(), "frozen setup insert failed");
        }

        table.freeze();

        size_t count = 0;
        for (const RecordView& record : table.iterator({}, {}, 256)) {
            require(count < keys.size(), "frozen iterator returned too many records");
            require(std::ranges::equal(record.key(), bytes(keys[count])), "frozen iterator returned out-of-order keys");
            ++count;
        }
        require(count == keys.size(), "frozen iterator must stream every record");
    }
} // namespace

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();

    try {
        verifyLockedIteratorDoesNotHoldWriteLock();
        verifyBatchedIteratorReleasesWriteLockBetweenBatches();
        verifyFrozenIteratorIgnoresLockedMaterializationMode();
        verifyConcurrentMutablePointLookups();
        return 0;
    }
    catch (const std::exception& ex) {
        std::fprintf(stderr, "BPTree mutable concurrency stress failed: %s\n", ex.what());
        return 1;
    }
}

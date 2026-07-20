/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "TestErrorHandlers.hpp"

#include "akk/core/record/KeyFingerprint.hpp"
#include "akk/engine/memtable/ARTMemTable.hpp"
#include "akk/engine/memtable/BPTreeMemTable.hpp"
#include "akk/engine/memtable/MemTable.hpp"
#include "akk/engine/memtable/SkipListMemTable.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {
    using akkaradb::core::RecordView;
    using akkaradb::engine::memtable::MemTable;

    [[nodiscard]] size_t sizeArg(int argc, char** argv, const char* flag, size_t fallback) {
        const std::string prefix = std::string{flag} + "=";
        for (int i = 1; i < argc; ++i) {
            const std::string_view arg{argv[i]};
            if (arg.starts_with(prefix)) { return static_cast<size_t>(std::stoull(std::string{arg.substr(prefix.size())})); }
            if (arg == flag && i + 1 < argc) { return static_cast<size_t>(std::stoull(argv[i + 1])); }
        }
        return fallback;
    }

    [[nodiscard]] std::string stringArg(int argc, char** argv, const char* flag, std::string fallback) {
        const std::string prefix = std::string{flag} + "=";
        for (int i = 1; i < argc; ++i) {
            const std::string_view arg{argv[i]};
            if (arg.starts_with(prefix)) { return std::string{arg.substr(prefix.size())}; }
            if (arg == flag && i + 1 < argc) { return argv[i + 1]; }
        }
        return fallback;
    }

    [[nodiscard]] std::span<const uint8_t> bytes(const std::vector<uint8_t>& value) noexcept {
        return {value.data(), value.size()};
    }

    [[nodiscard]] MemTable::MemTableFactory backendFactory(std::string_view backend) {
        if (backend == "skiplist") {
            return [] { return std::make_unique<akkaradb::engine::memtable::SkipListMemTable>(); };
        }
        if (backend == "bptree") {
            return [] { return std::make_unique<akkaradb::engine::memtable::BPTreeMemTable>(); };
        }
        if (backend == "art") {
            return [] { return std::make_unique<akkaradb::engine::memtable::ARTMemTable>(); };
        }
        throw std::invalid_argument("invalid --backend: expected skiplist, bptree, or art");
    }

    enum class WriteOrder {
        APPLY_ORDER,
        KEY_SEQUENCE,
    };

    [[nodiscard]] WriteOrder writeOrderArg(int argc, char** argv) {
        const std::string order = stringArg(argc, argv, "--order", "apply-order");
        if (order == "apply-order") { return WriteOrder::APPLY_ORDER; }
        if (order == "key-sequence") { return WriteOrder::KEY_SEQUENCE; }
        throw std::invalid_argument("invalid --order: expected apply-order or key-sequence");
    }

    [[nodiscard]] const char* writeOrderName(WriteOrder order) noexcept {
        return order == WriteOrder::KEY_SEQUENCE ? "key-sequence" : "apply-order";
    }
}

int main(int argc, char** argv) {
    akkaradb::test::installMsvcTestErrorHandlers();

    try {
        const size_t totalWrites = std::max<size_t>(1, sizeArg(argc, argv, "--ops", 2'500'000));
        const size_t writers = std::max<size_t>(2, sizeArg(argc, argv, "--writers", 8));
        const std::string backend = stringArg(argc, argv, "--backend", "bptree");
        const WriteOrder writeOrder = writeOrderArg(argc, argv);
        if (totalWrites % writers != 0) {
            throw std::invalid_argument("--ops must be divisible by --writers so every hot-key wave has the same width");
        }

        MemTable::Options options;
        options.shardCount = 1;
        options.expectedConcurrentWriters = writers;
        options.flushMode = akkaradb::engine::memtable::MemTableFlushMode::MANUAL_ONLY;
        options.thresholdBytesPerShard = 0;
        options.backendFactory = backendFactory(backend);
        auto table = MemTable::create(options);

        const std::vector<uint8_t> hotKey{'p', 'a', 'r', 'a', 'l', 'l', 'e', 'l', '-', 'h', 'o', 't', '-', 'k', 'e', 'y'};
        const std::vector<uint8_t> value{'v'};
        static constexpr size_t ORDER_STRIPES = 256;
        std::array<std::mutex, ORDER_STRIPES> keySequenceOrderMu;
        const uint64_t hotKeyFp64 = akkaradb::core::computeKeyFp64(bytes(hotKey));
        const uint64_t route = hotKeyFp64 ^ (static_cast<uint64_t>(hotKey.size()) * 0x9E3779B97F4A7C15ULL);
        std::mutex& hotKeyOrderMu = keySequenceOrderMu[static_cast<size_t>(route) & (ORDER_STRIPES - 1u)];
        const size_t waves = totalWrites / writers;
        std::atomic<uint64_t> outOfOrderSelections{0};
        std::atomic<uint64_t> largestSequenceLag{0};
        std::barrier writesApplied{static_cast<std::ptrdiff_t>(writers)};
        std::barrier nextWave{static_cast<std::ptrdiff_t>(writers)};
        std::vector<std::thread> workers;
        workers.reserve(writers);

        const auto started = std::chrono::steady_clock::now();
        for (size_t writer = 0; writer < writers; ++writer) {
            workers.emplace_back([&, writer] {
                for (size_t wave = 0; wave < waves; ++wave) {
                    if (writeOrder == WriteOrder::KEY_SEQUENCE) {
                        std::lock_guard lock{hotKeyOrderMu};
                        const uint64_t seq = table->nextSeq();
                        table->put(bytes(hotKey), bytes(value), seq);
                    }
                    else {
                        const uint64_t seq = table->nextSeq();
                        table->put(bytes(hotKey), bytes(value), seq);
                    }
                    writesApplied.arrive_and_wait();

                    if (writer == 0) {
                        const uint64_t expectedSeq = table->lastSeq() - 1;
                        RecordView observed;
                        if (!table->get(bytes(hotKey), expectedSeq, &observed)) {
                            throw std::runtime_error("hot key disappeared from MemTable");
                        }
                        if (observed.seq() != expectedSeq) {
                            outOfOrderSelections.fetch_add(1, std::memory_order_relaxed);
                            const uint64_t lag = expectedSeq - observed.seq();
                            uint64_t current = largestSequenceLag.load(std::memory_order_relaxed);
                            while (current < lag && !largestSequenceLag.compare_exchange_weak(
                                current,
                                lag,
                                std::memory_order_relaxed,
                                std::memory_order_relaxed
                            )) {}
                        }
                    }
                    nextWave.arrive_and_wait();
                }
            });
        }
        for (auto& worker : workers) { worker.join(); }
        const double seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();

        const uint64_t outOfOrder = outOfOrderSelections.load(std::memory_order_relaxed);
        std::cout << "totalWrites = " << totalWrites << '\n';
        std::cout << "writers = " << writers << '\n';
        std::cout << "backend = " << backend << '\n';
        std::cout << "order = " << writeOrderName(writeOrder) << '\n';
        std::cout << "hotKeys = 1\n";
        std::cout << "waves = " << waves << '\n';
        std::cout << "outOfOrderSelections = " << outOfOrder << '\n';
        std::cout << "outOfOrderRatePerWave = " << static_cast<double>(outOfOrder) / static_cast<double>(waves) << '\n';
        std::cout << "outOfOrderEventsPerWrite = " << static_cast<double>(outOfOrder) / static_cast<double>(totalWrites) << '\n';
        std::cout << "largestSequenceLag = " << largestSequenceLag.load(std::memory_order_relaxed) << '\n';
        std::cout << "seconds = " << seconds << '\n';
        std::cout << "throughputWritesSec = " << static_cast<double>(totalWrites) / seconds << '\n';
        return 0;
    }
    catch (const std::exception& ex) {
        std::fprintf(stderr, "parallel memtable visibility-rate benchmark failed: %s\n", ex.what());
        return 2;
    }
}

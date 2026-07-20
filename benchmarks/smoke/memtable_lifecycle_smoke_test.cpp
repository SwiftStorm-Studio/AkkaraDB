/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "akk/engine/AkkEngine.hpp"
#include "akk/engine/memtable/MemTable.hpp"

#include "TestErrorHandlers.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

namespace {
    namespace fs = std::filesystem;

    void require(bool condition, const char* message) {
        if (!condition) { throw std::runtime_error(message); }
    }

    [[nodiscard]] std::span<const uint8_t> bytes(const std::vector<uint8_t>& value) noexcept {
        return {value.data(), value.size()};
    }

    void testFlushFailurePropagation() {
        namespace memtable = akkaradb::engine::memtable;

        std::atomic<uint32_t> callbackAttempts{0};
        memtable::MemTable::Options options;
        options.shardCount = 1;
        options.flushMode = memtable::MemTableFlushMode::BYTES_PER_SHARD;
        options.thresholdBytesPerShard = 1;
        options.onFlush = [&callbackAttempts](std::span<const memtable::MemTable::RecordView>) {
            callbackAttempts.fetch_add(1, std::memory_order_relaxed);
            throw std::runtime_error("injected immutable flush failure");
        };

        auto table = memtable::MemTable::create(options);
        const std::vector<uint8_t> key{'k', 'e', 'y'};
        const std::vector<uint8_t> value{'v', 'a', 'l', 'u', 'e'};
        table->put(bytes(key), bytes(value), 1);

        bool forceFlushFailed = false;
        try { table->forceFlush(); }
        catch (const std::runtime_error&) { forceFlushFailed = true; }
        require(forceFlushFailed, "forceFlush must report an asynchronous flush failure");
        require(callbackAttempts.load(std::memory_order_relaxed) == 1, "flush callback must run exactly once");

        bool subsequentWriteFailed = false;
        try { table->put(bytes(key), bytes(value), 2); }
        catch (const std::runtime_error&) { subsequentWriteFailed = true; }
        require(subsequentWriteFailed, "writes after a failed immutable flush must be rejected");
    }

    [[nodiscard]] akkaradb::engine::AkkEngineOptions memoryOptions() {
        akkaradb::engine::AkkEngineOptions options;
        options.components.walEnabled = false;
        options.components.blobEnabled = false;
        options.components.manifestEnabled = false;
        options.components.sstEnabled = false;
        options.components.versionLogEnabled = false;
        options.runtime.forceFlushOnClose = false;
        options.runtime.forceSyncOnClose = false;
        options.memtable.shardCount = 1;
        options.memtable.flushMode = akkaradb::engine::memtable::MemTableFlushMode::MANUAL_ONLY;
        options.memtable.thresholdBytesPerShard = 0;
        return options;
    }

    void testConcurrentOperationsAndClose() {
        auto engine = akkaradb::engine::AkkEngine::open(memoryOptions());
        std::atomic<uint32_t> ready{0};
        std::atomic<uint32_t> completed{0};
        std::atomic<uint32_t> rejected{0};
        std::atomic<bool> unexpectedFailure{false};

        constexpr uint32_t workerCount = 4;
        std::vector<std::thread> workers;
        workers.reserve(workerCount);
        for (uint32_t i = 0; i < workerCount; ++i) {
            workers.emplace_back([&, i] {
                const std::vector<uint8_t> key{'k', static_cast<uint8_t>('0' + i)};
                const std::vector<uint8_t> value{'v', static_cast<uint8_t>('0' + i)};
                ready.fetch_add(1, std::memory_order_release);
                for (;;) {
                    try {
                        engine->put(bytes(key), bytes(value));
                        (void)engine->get(bytes(key));
                        completed.fetch_add(1, std::memory_order_relaxed);
                    }
                    catch (const std::runtime_error&) {
                        rejected.fetch_add(1, std::memory_order_relaxed);
                        return;
                    }
                    catch (...) {
                        unexpectedFailure.store(true, std::memory_order_release);
                        return;
                    }
                }
            });
        }

        while (ready.load(std::memory_order_acquire) != workerCount) { std::this_thread::yield(); }
        std::this_thread::sleep_for(std::chrono::milliseconds{20});
        engine->close();
        for (auto& worker : workers) { worker.join(); }

        require(completed.load(std::memory_order_relaxed) > 0, "workers must complete operations before close");
        require(rejected.load(std::memory_order_relaxed) == workerCount, "each worker must observe the closed engine");
        require(!unexpectedFailure.load(std::memory_order_acquire), "close must not expose invalid storage to concurrent operations");
    }

    void testCloseWaitsForActiveScan() {
        auto engine = akkaradb::engine::AkkEngine::open(memoryOptions());
        const std::vector<uint8_t> key{'s', 'c', 'a', 'n'};
        const std::vector<uint8_t> value{'v', 'a', 'l', 'u', 'e'};
        engine->put(bytes(key), bytes(value));

        akkaradb::core::BufferArena arena;
        auto scan = engine->scan(arena);
        std::atomic<bool> closeFinished{false};
        std::thread closer([&] {
            engine->close();
            closeFinished.store(true, std::memory_order_release);
        });

        std::this_thread::sleep_for(std::chrono::milliseconds{20});
        require(!closeFinished.load(std::memory_order_acquire), "close must wait for an active scan");
        scan = {};
        closer.join();
        require(closeFinished.load(std::memory_order_acquire), "close did not complete after the scan was released");
    }

    void testSstBackpressureFailFast() {
        namespace engine = akkaradb::engine;
        namespace memtable = akkaradb::engine::memtable;
        namespace sst = akkaradb::engine::sst;

        const fs::path dir = fs::temp_directory_path() / "akkaradb_sst_backpressure_fail_fast_smoke";
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir, ec);
        require(!ec, "failed to create backpressure smoke directory");

        auto options = memoryOptions();
        options.paths.dataDir = dir;
        options.components.sstEnabled = true;
        options.memtable.flushMode = memtable::MemTableFlushMode::MANUAL_ONLY;
        options.memtable.thresholdBytesPerShard = 0;
        options.sst.compactionMode = sst::SSTCompactionMode::DISABLED;
        options.sst.compactThreads = 0;
        options.runtime.backpressure.maxSstL0Files = 1;
        options.runtime.backpressure.sstCompactionBacklog = engine::AkkEngineOptions::BackpressureMode::FAIL_FAST;

        {
            auto akkaradb = engine::AkkEngine::open(options);
            const std::vector<uint8_t> firstKey{'b', 'p', '-', '1'};
            const std::vector<uint8_t> secondKey{'b', 'p', '-', '2'};
            const std::vector<uint8_t> value{'v'};

            akkaradb->put(bytes(firstKey), bytes(value));
            akkaradb->forceFlush();

            bool rejected = false;
            try { akkaradb->put(bytes(secondKey), bytes(value)); }
            catch (const std::runtime_error&) { rejected = true; }
            require(rejected, "SST L0 fail-fast backpressure must reject a write when the backlog limit is reached");
            const auto stats = akkaradb->stats().backpressure;
            require(stats.rejectedWrites == 1, "fail-fast backpressure must count its rejected write");
            require(stats.timedOutWrites == 0, "fail-fast backpressure must not be reported as a timeout");
            require(stats.sstStalls == 1, "fail-fast backpressure must identify the SST stall source");
            akkaradb->close();
        }

        fs::remove_all(dir, ec);
    }

    void testBlockedBackpressureWriterObservesClose() {
        namespace engine = akkaradb::engine;
        namespace memtable = akkaradb::engine::memtable;
        namespace sst = akkaradb::engine::sst;

        const fs::path dir = fs::temp_directory_path() / "akkaradb_backpressure_close_smoke";
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir, ec);
        require(!ec, "failed to create backpressure close smoke directory");

        auto options = memoryOptions();
        options.paths.dataDir = dir;
        options.components.sstEnabled = true;
        options.memtable.flushMode = memtable::MemTableFlushMode::MANUAL_ONLY;
        options.memtable.thresholdBytesPerShard = 0;
        options.sst.compactionMode = sst::SSTCompactionMode::BACKGROUND;
        options.sst.compactThreads = 1;
        options.runtime.backpressure.maxSstL0Files = 1;
        options.runtime.backpressure.waitMicros = 1000;
        options.runtime.backpressure.sstCompactionBacklog = engine::AkkEngineOptions::BackpressureMode::BLOCK;

        auto akkaradb = engine::AkkEngine::open(options);
        const std::vector<uint8_t> firstKey{'b', 'l', 'k', '-', '1'};
        const std::vector<uint8_t> secondKey{'b', 'l', 'k', '-', '2'};
        const std::vector<uint8_t> value{'v'};
        akkaradb->put(bytes(firstKey), bytes(value));
        akkaradb->forceFlush();

        std::atomic<bool> writerStarted{false};
        std::atomic<bool> writerReturned{false};
        std::atomic<bool> writerRejected{false};
        std::thread writer([&] {
            writerStarted.store(true, std::memory_order_release);
            try { akkaradb->put(bytes(secondKey), bytes(value)); }
            catch (const std::runtime_error&) { writerRejected.store(true, std::memory_order_release); }
            writerReturned.store(true, std::memory_order_release);
        });

        while (!writerStarted.load(std::memory_order_acquire)) { std::this_thread::yield(); }
        std::this_thread::sleep_for(std::chrono::milliseconds{20});
        require(!writerReturned.load(std::memory_order_acquire), "writer must block on SST L0 backpressure before close");
        akkaradb->close();
        writer.join();
        require(writerRejected.load(std::memory_order_acquire), "blocked backpressure writer must observe close and reject");

        fs::remove_all(dir, ec);
    }

    void testBlockedBackpressureTimesOutAndReportsStats() {
        namespace engine = akkaradb::engine;
        namespace memtable = akkaradb::engine::memtable;
        namespace sst = akkaradb::engine::sst;

        const fs::path dir = fs::temp_directory_path() / "akkaradb_backpressure_timeout_smoke";
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir, ec);
        require(!ec, "failed to create backpressure timeout smoke directory");

        auto options = memoryOptions();
        options.paths.dataDir = dir;
        options.components.sstEnabled = true;
        options.memtable.flushMode = memtable::MemTableFlushMode::MANUAL_ONLY;
        options.memtable.thresholdBytesPerShard = 0;
        options.sst.compactionMode = sst::SSTCompactionMode::BACKGROUND;
        options.sst.compactThreads = 1;
        options.runtime.backpressure.maxSstL0Files = 1;
        options.runtime.backpressure.waitMicros = 1000;
        options.runtime.backpressure.timeoutMs = 20;
        options.runtime.backpressure.sstCompactionBacklog = engine::AkkEngineOptions::BackpressureMode::BLOCK;

        auto akkaradb = engine::AkkEngine::open(options);
        const std::vector<uint8_t> firstKey{'t', 'i', 'm', 'e', 'o', 'u', 't', '-', '1'};
        const std::vector<uint8_t> secondKey{'t', 'i', 'm', 'e', 'o', 'u', 't', '-', '2'};
        const std::vector<uint8_t> value{'v'};
        akkaradb->put(bytes(firstKey), bytes(value));
        akkaradb->forceFlush();

        const auto started = std::chrono::steady_clock::now();
        bool timedOut = false;
        try { akkaradb->put(bytes(secondKey), bytes(value)); }
        catch (const std::runtime_error&) { timedOut = true; }
        const auto elapsed = std::chrono::steady_clock::now() - started;
        require(timedOut, "blocking SST backpressure must time out when no progress is possible");
        require(elapsed >= std::chrono::milliseconds{15}, "backpressure timeout returned too early");
        require(elapsed < std::chrono::seconds{1}, "backpressure timeout exceeded its bounded wait");

        const auto stats = akkaradb->stats().backpressure;
        require(stats.blockedWrites == 1, "timed-out backpressure must count the blocked write once");
        require(stats.rejectedWrites == 1 && stats.timedOutWrites == 1, "timed-out backpressure counters are incorrect");
        require(stats.sstStalls == 1, "timed-out backpressure must identify the SST stall source");
        require(stats.waitMicrosTotal >= 15'000 && stats.waitMicrosMax >= 15'000, "backpressure wait duration was not recorded");
        akkaradb->close();
        fs::remove_all(dir, ec);
    }
}

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();
    try {
        testFlushFailurePropagation();
        testConcurrentOperationsAndClose();
        testCloseWaitsForActiveScan();
        testSstBackpressureFailFast();
        testBlockedBackpressureWriterObservesClose();
        testBlockedBackpressureTimesOutAndReportsStats();
        return 0;
    }
    catch (const std::exception& ex) {
        std::fprintf(stderr, "memtable/lifecycle smoke failed: %s\n", ex.what());
        return 1;
    }
}

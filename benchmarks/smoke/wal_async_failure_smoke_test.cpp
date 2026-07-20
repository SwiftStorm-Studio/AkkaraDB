/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "TestErrorHandlers.hpp"

#include "akk/engine/AkkEngine.hpp"
#include "akk/engine/wal/WalWriter.hpp"

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <system_error>
#include <vector>

#ifdef _WIN32
#include <stdlib.h>
#endif

namespace {
    namespace fs = std::filesystem;
    namespace wal = akkaradb::engine::wal;

    void require(bool condition, const char* message) {
        if (!condition) { throw std::runtime_error(message); }
    }

    [[nodiscard]] std::span<const uint8_t> bytes(const std::vector<uint8_t>& value) noexcept {
        return {value.data(), value.size()};
    }

    void setWalFailureInjection(const char* value) {
#ifdef _WIN32
        if (_putenv_s("AKKARADB_TEST_WAL_FAIL_AFTER_ENTRIES", value) != 0) {
            throw std::runtime_error("failed to set WAL failure injection env");
        }
#else
        if (::setenv("AKKARADB_TEST_WAL_FAIL_AFTER_ENTRIES", value, 1) != 0) {
            throw std::runtime_error("failed to set WAL failure injection env");
        }
#endif
    }

    void clearWalFailureInjection() {
#ifdef _WIN32
        (void)_putenv_s("AKKARADB_TEST_WAL_FAIL_AFTER_ENTRIES", "");
#else
        (void)::unsetenv("AKKARADB_TEST_WAL_FAIL_AFTER_ENTRIES");
#endif
    }

    void testAsyncRotationFailurePoisonsWriter() {
        const fs::path dir = fs::temp_directory_path() / "akkaradb_wal_async_failure_smoke";
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir, ec);
        require(!ec, "failed to create WAL failure smoke directory");

        setWalFailureInjection("4");
        try {
            wal::WalOptions options;
            options.walDir = dir;
            options.execution = wal::WalExecutionMode::ASYNC;
            options.syncPolicy = wal::WalSyncPolicy::NEVER;
            options.backpressure = wal::WalBackpressureMode::BLOCK;
            options.shardCount = 1;
            options.groupN = 128;
            options.groupMicros = 1000;
            options.groupBytes = 4ULL * 1024ULL * 1024ULL;
            options.asyncMaxPendingBytes = 8ULL * 1024ULL * 1024ULL;

            auto writer = wal::WalWriter::create(options);
            const std::vector<uint8_t> key{'w', 'a', 'l', '-', 'k'};
            const std::vector<uint8_t> value(4096, static_cast<uint8_t>('v'));

            bool appendFailed = false;
            for (uint64_t seq = 1; seq <= 16; ++seq) {
                try { writer->append(bytes(key), bytes(value), seq, 0, 0, wal::WalAppendAck::ENQUEUED); }
                catch (const std::runtime_error&) {
                    appendFailed = true;
                    break;
                }
            }

            bool syncFailed = false;
            try { writer->forceSync(); }
            catch (const std::runtime_error&) { syncFailed = true; }
            require(appendFailed || syncFailed, "async WAL rotation failure must be reported to the caller");

            const auto snap = writer->snapshot();
            require(!snap.healthy, "async WAL failure must mark the writer unhealthy");
            require(snap.asyncFailures > 0, "async WAL failure counter must be incremented");
            require(snap.pendingEntries == 0, "failed async WAL queue must be drained");
            require(snap.pendingBytes == 0, "failed async WAL queue bytes must be cleared");

            bool subsequentAppendFailed = false;
            try { writer->append(bytes(key), bytes(value), 100, 0, 0, wal::WalAppendAck::ENQUEUED); }
            catch (const std::runtime_error&) { subsequentAppendFailed = true; }
            require(subsequentAppendFailed, "poisoned async WAL writer must reject subsequent appends");
        }
        catch (...) {
            clearWalFailureInjection();
            throw;
        }
        clearWalFailureInjection();
        fs::remove_all(dir, ec);
    }

    [[nodiscard]] akkaradb::engine::AkkEngineOptions asyncEnqueuedEngineOptions(const fs::path& dir) {
        namespace engine = akkaradb::engine;
        namespace memtable = engine::memtable;
        namespace wal = engine::wal;

        engine::AkkEngineOptions options;
        options.paths.dataDir = dir;
        options.components.walEnabled = true;
        options.components.blobEnabled = false;
        options.components.manifestEnabled = false;
        options.components.sstEnabled = false;
        options.components.versionLogEnabled = false;
        options.memtable.shardCount = 1;
        options.memtable.flushMode = memtable::MemTableFlushMode::MANUAL_ONLY;
        options.runtime.writeDurability = engine::AkkEngineOptions::WriteDurabilityMode::ENQUEUED;
        options.wal.execution = wal::WalExecutionMode::ASYNC;
        options.wal.syncPolicy = wal::WalSyncPolicy::NEVER;
        options.wal.shardCount = 1;
        options.wal.groupN = 1;
        return options;
    }

    template <typename Operation>
    void requireRuntimeError(Operation&& operation, const char* message) {
        try {
            operation();
        }
        catch (const std::runtime_error&) { return; }
        throw std::runtime_error(message);
    }

    void testEnginePropagatesAsyncWalFailure() {
        const fs::path dir = fs::temp_directory_path() / "akkaradb_engine_wal_async_failure_smoke";
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir, ec);
        require(!ec, "failed to create engine WAL failure smoke directory");

        setWalFailureInjection("1");
        try {
            auto engine = akkaradb::engine::AkkEngine::open(asyncEnqueuedEngineOptions(dir));
            const std::vector<uint8_t> key{'e', 'n', 'g', 'i', 'n', 'e', '-', 'k'};
            const std::vector<uint8_t> value{'e', 'n', 'g', 'i', 'n', 'e', '-', 'v'};

            engine->put(bytes(key), bytes(value));
            engine->forceSync();
            const auto durable = engine->get(bytes(key));
            require(durable.has_value() && *durable == value, "first WAL append must complete before injecting the next flusher failure");

            // This call only waits for queue admission. appendAll applies the same
            // record to the MemTable before it returns; forceSync below observes
            // the delayed flusher failure for that accepted ENQUEUED write.
            engine->put(bytes(key), bytes(value));
            requireRuntimeError([&] { engine->forceSync(); }, "engine forceSync must report the asynchronous WAL failure");
            const auto stats = engine->stats();
            require(!stats.wal.healthy, "engine stats must expose an unhealthy WAL");
            require(stats.wal.asyncFailures > 0, "engine stats must expose the asynchronous WAL failure count");
            require(stats.wal.pendingEntries == 0 && stats.wal.pendingBytes == 0, "engine stats must expose an empty failed WAL queue");

            requireRuntimeError([&] { (void)engine->get(bytes(key)); }, "engine get must reject reads after WAL failure");
            requireRuntimeError([&] { engine->put(bytes(key), bytes(value)); }, "engine put must reject writes after WAL failure");
            requireRuntimeError([&] { (void)engine->exists(bytes(key)); }, "engine exists must reject reads after WAL failure");
            requireRuntimeError([&] { engine->forceFlush(); }, "engine forceFlush must reject work after WAL failure");
            requireRuntimeError([&] { engine->close(); }, "engine close must surface the asynchronous WAL failure");
        }
        catch (...) {
            clearWalFailureInjection();
            throw;
        }
        clearWalFailureInjection();
        fs::remove_all(dir, ec);
    }
}

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();
    try {
        testAsyncRotationFailurePoisonsWriter();
        testEnginePropagatesAsyncWalFailure();
        return 0;
    }
    catch (const std::exception& ex) {
        std::fprintf(stderr, "wal async failure smoke failed: %s\n", ex.what());
        return 1;
    }
}

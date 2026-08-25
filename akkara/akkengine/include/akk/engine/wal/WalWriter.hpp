/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/wal/WalWriter.hpp
/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/wal/WalWriter.hpp
#pragma once

#include "akkaradb/Export.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>

namespace akkaradb::engine::wal {
    enum class WalSyncMode : uint8_t {
        SYNC = 0, ASYNC = 1, OFF = 2,
    };

    enum class WalAppendAck : uint8_t {
        // The entry is admitted to an async queue. It has no durability guarantee.
        ENQUEUED = 0,
        // The entry's WAL batch was written and flushed to the OS file cache.
        WRITTEN = 1,
        // The entry's WAL batch completed fdatasync.
        SYNCED = 2,
    };

    enum class WalExecutionMode : uint8_t {
        AUTO = 0, INLINE = 1, ASYNC = 2,
    };

    enum class WalSyncPolicy : uint8_t {
        AUTO = 0, NEVER = 1, ON_SYNC_ACK = 2, ALWAYS = 3,
    };

    enum class WalBackpressureMode : uint8_t {
        BLOCK = 0, FAIL_FAST = 1,
    };

    struct AKDB_API WalOptions {
        // Compatibility shortcut. When execution or syncPolicy is AUTO, this is expanded into the explicit knobs below.
        WalSyncMode syncMode = WalSyncMode::SYNC;
        // Physical append execution. INLINE writes on the caller thread; ASYNC enqueues to a shard flusher.
        WalExecutionMode execution = WalExecutionMode::AUTO;
        // Physical sync discipline. Write ack controls what the caller waits for; this controls when fdatasync is performed.
        WalSyncPolicy syncPolicy = WalSyncPolicy::AUTO;
        // Queue overflow behavior for ASYNC execution.
        WalBackpressureMode backpressure = WalBackpressureMode::BLOCK;
        // 0 means auto: one shard per hardware thread, capped at 16.
        uint16_t shardCount = 0;
        // ASYNC grouping and queue limits.
        uint32_t groupN = 128;
        uint32_t groupMicros = 100;
        uint64_t groupBytes = 4ULL * 1024ULL * 1024ULL;
        uint64_t asyncMaxPendingBytes = 64ULL * 1024ULL * 1024ULL;
    };

    struct AKDB_API WalWriterSnapshot {
        uint32_t shardCount = 0;
        uint64_t entriesWritten = 0;
        uint64_t bytesWritten = 0;
        uint64_t batchesFlushed = 0;
        uint64_t syncsExecuted = 0;
        uint64_t segmentRotations = 0;
        uint64_t asyncFailures = 0;
        uint64_t pendingEntries = 0;
        uint64_t pendingBytes = 0;
        uint64_t inFlightBytes = 0;
        bool healthy = true;
    };

    class AKDB_API WalWriter {
        public:
            [[nodiscard]] static std::unique_ptr<WalWriter> create(std::filesystem::path walDir, WalOptions options = {});
            ~WalWriter();

            WalWriter(const WalWriter&) = delete;
            WalWriter& operator=(const WalWriter&) = delete;
            WalWriter(WalWriter&&) = delete;
            WalWriter& operator=(WalWriter&&) = delete;

            void append(
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint64_t seq,
                uint16_t flags,
                uint64_t precomputedFp64 = 0,
                WalAppendAck ack = WalAppendAck::WRITTEN
            );

            void forceSync();
            void pruneUntil(uint64_t checkpointSeq);
            void requestClose() noexcept;
            void throwIfFailed() const;
            [[nodiscard]] WalWriterSnapshot snapshot() const noexcept;
            void close();

        private:
            WalWriter();

            class Impl;
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::wal

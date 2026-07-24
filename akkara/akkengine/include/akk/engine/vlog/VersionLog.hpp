/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/vlog/VersionLog.hpp
#pragma once

#include "akkaradb/Export.hpp"

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace akkaradb::engine::vlog {
    inline constexpr uint64_t ROLLBACK_NODE = UINT64_MAX;
    inline constexpr uint8_t VLOG_FLAG_ZSTD = 0x01;
    inline constexpr uint8_t VLOG_FLAG_ROLLBACK = 0x04;
    // Synthetic entry written by retention compaction to preserve the value at
    // the start of the retained history window.
    inline constexpr uint8_t VLOG_FLAG_RETENTION_BASE = 0x08;

    enum class VLogSyncMode : uint8_t {
        SYNC = 0, ASYNC = 1, BATCHED_SYNC = 2,
    };

    enum class VLogWriteAdmissionMode : uint8_t {
        SERIAL = 0,
        // Concurrent entry preparation; persistence remains on one append stream.
        PREPARE_PARALLEL = 1,
        // Concurrent persistence through independently locked active segments.
        PARALLEL = 2,
    };

    enum class VLogSerialAppendMode : uint8_t {
        // Submit an async append and let the next serial put proceed immediately.
        PIPELINED = 0,
        // A serial put waits for the preceding serial VLog append before submitting its own.
        WAIT_PREVIOUS_APPEND = 1,
    };

    enum class VLogParallelPendingLimitScope : uint8_t {
        // Each lane may independently hold asyncMaxPendingBytes.
        PER_LANE = 0,
        // All lane queues together may hold asyncMaxPendingBytes.
        GLOBAL = 1,
    };

    enum class VLogReadVisibilityMode : uint8_t {
        COMMIT_ORDER = 0, APPLIED = 1,
    };

    enum class VLogRecoveryMode : uint8_t {
        // Validate the log before VersionLog::create returns.
        EAGER = 0,
        // Return from create immediately. VersionLog operations wait until validation finishes.
        BACKGROUND = 1,
    };

    enum class VLogCodec : uint8_t {
        NONE = 0, ZSTD = 1,
    };

    struct AKDB_API VersionLogOptions {
        std::filesystem::path logPath;
        VLogSyncMode syncMode = VLogSyncMode::ASYNC;
        VLogWriteAdmissionMode writeAdmission = VLogWriteAdmissionMode::SERIAL;
        // Applies only to SERIAL admission. PARALLEL always submits to lane workers.
        VLogSerialAppendMode serialAppendMode = VLogSerialAppendMode::PIPELINED;
        VLogReadVisibilityMode readVisibility = VLogReadVisibilityMode::COMMIT_ORDER;
        VLogRecoveryMode recoveryMode = VLogRecoveryMode::EAGER;
        // Compression is opt-in. Compressed records retain their original value size
        // and are decoded before VersionEntry is exposed to callers.
        VLogCodec codec = VLogCodec::NONE;
        int zstdCompressionLevel = 1;
        uint32_t groupN = 128;
        uint32_t groupMicros = 500;
        uint64_t groupBytes = 1ULL * 1024ULL * 1024ULL;
        uint64_t asyncMaxPendingBytes = 64ULL * 1024ULL * 1024ULL;
        // Applies only to PARALLEL admission; the default preserves per-lane backpressure.
        VLogParallelPendingLimitScope parallelPendingLimitScope = VLogParallelPendingLimitScope::PER_LANE;
        // 0 selects a bounded hardware-derived lane count when writeAdmission is PARALLEL.
        uint32_t parallelWriteLanes = 0;
        // 0 keeps a single file. Positive values rotate into numbered sibling segments.
        uint64_t segmentBytes = 64ULL * 1024ULL * 1024ULL;
        // Optional retention boundaries. 0 disables each boundary. Before a
        // closed segment is removed at either boundary, VersionLog writes a
        // synthetic base entry for every state that would otherwise be lost.
        uint32_t retentionDays = 0;
        uint64_t retentionMinCommitSeq = 0;
        // Seeds COMMIT_ORDER when the engine has already recovered a higher sequence from WAL or SST.
        uint64_t initialCommittedSeq = 0;
    };

    struct AKDB_API VersionEntry {
        uint64_t seq = 0;
        uint64_t sourceNodeId = 0;
        uint64_t timestampNs = 0;
        uint8_t flags = 0;
        std::vector<uint8_t> value;
    };

    struct AKDB_API VersionLogSnapshot {
        uint8_t syncMode = static_cast<uint8_t>(VLogSyncMode::ASYNC);
        uint8_t codec = static_cast<uint8_t>(VLogCodec::NONE);
        int zstdCompressionLevel = 1;
        uint32_t groupN = 0;
        uint32_t groupMicros = 0;
        uint64_t groupBytes = 0;
        uint64_t asyncMaxPendingBytes = 0;
        uint64_t indexedKeys = 0;
        uint64_t indexedEntries = 0;
        uint64_t rollbackEntries = 0;
        uint64_t pendingWrites = 0;
        uint64_t pendingBytes = 0;
        uint64_t durableBytes = 0;
        uint64_t segmentCount = 0;
        uint64_t activeSegmentBytes = 0;
        uint32_t retentionDays = 0;
        uint64_t retentionMinCommitSeq = 0;
        uint64_t recoveryDurationMicros = 0;
        uint64_t recoveredSegmentCount = 0;
        uint64_t recoveredEntryCount = 0;
        uint64_t sidecarFallbackCount = 0;
        uint64_t sidecarRebuildFailures = 0;
        uint64_t retentionPrunedSegments = 0;
        uint64_t retentionBaseEntriesWritten = 0;
        uint64_t parallelQueueRejects = 0;
        uint64_t parallelLaneCount = 0;
        uint64_t parallelPendingWrites = 0;
        uint64_t parallelPendingBytes = 0;
        bool flushThreadRunning = false;
    };

    class AKDB_API VersionLog {
        public:
            [[nodiscard]] static std::unique_ptr<VersionLog> create(VersionLogOptions opts);

            ~VersionLog();

            VersionLog(const VersionLog&) = delete;
            VersionLog& operator=(const VersionLog&) = delete;
            VersionLog(VersionLog&&) = delete;
            VersionLog& operator=(VersionLog&&) = delete;

            void append(
                std::span<const uint8_t> key,
                uint64_t seq,
                uint64_t sourceNodeId,
                uint64_t timestampNs,
                uint8_t flags,
                std::span<const uint8_t> value
            );

            // Appends a record without advancing the COMMIT_ORDER frontier.
            // AkkEngine calls markCommitted after applying the matching MemTable mutation.
            void appendDeferred(
                std::span<const uint8_t> key,
                uint64_t seq,
                uint64_t sourceNodeId,
                uint64_t timestampNs,
                uint8_t flags,
                std::span<const uint8_t> value
            );

            void markCommitted(uint64_t seq);

            // Waits for startup validation in BACKGROUND recovery mode.
            void waitUntilReady() const;

            [[nodiscard]] std::optional<VersionEntry> getAt(std::span<const uint8_t> key, uint64_t atSeq) const;
            [[nodiscard]] std::vector<VersionEntry> history(std::span<const uint8_t> key) const;

            [[nodiscard]] std::vector<std::pair<std::vector<uint8_t>, std::optional<VersionEntry>>> collectRollbackTargets(
                uint64_t targetSeq
            ) const;

            [[nodiscard]] VersionLogSnapshot snapshot() const noexcept;
            void forceSync();
            void close();

        private:
            VersionLog();

            class Impl;
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::vlog

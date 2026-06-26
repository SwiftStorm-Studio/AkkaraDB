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
    inline constexpr uint8_t VLOG_FLAG_ROLLBACK = 0x04;

    enum class VLogSyncMode : uint8_t {
        SYNC = 0, ASYNC = 1, BATCHED_SYNC = 2,
    };

    struct AKDB_API VersionLogOptions {
        std::filesystem::path logPath;
        VLogSyncMode syncMode = VLogSyncMode::ASYNC;
        uint32_t groupN = 128;
        uint32_t groupMicros = 500;
        uint64_t groupBytes = 1ULL * 1024ULL * 1024ULL;
        uint64_t asyncMaxPendingBytes = 64ULL * 1024ULL * 1024ULL;
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

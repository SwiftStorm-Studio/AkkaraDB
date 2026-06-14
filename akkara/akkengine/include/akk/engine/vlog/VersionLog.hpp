/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
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
        SYNC = 0, ASYNC = 1,
    };

    struct AKDB_API VersionLogOptions {
        std::filesystem::path logPath;
        VLogSyncMode syncMode = VLogSyncMode::ASYNC;
    };

    struct AKDB_API VersionEntry {
        uint64_t seq = 0;
        uint64_t sourceNodeId = 0;
        uint64_t timestampNs = 0;
        uint8_t flags = 0;
        std::vector<uint8_t> value;
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

            void close();

        private:
            VersionLog();

            class Impl;
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::vlog

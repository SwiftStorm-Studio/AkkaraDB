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
        Sync = 0, Async = 1,
    };

    struct VersionLogOptions {
        std::filesystem::path log_path;
        VLogSyncMode sync_mode = VLogSyncMode::Async;
    };

    struct VersionEntry {
        uint64_t seq = 0;
        uint64_t source_node_id = 0;
        uint64_t timestamp_ns = 0;
        uint8_t flags = 0;
        std::vector<uint8_t> value;
    };

    class VersionLog {
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
                uint64_t source_node_id,
                uint64_t timestamp_ns,
                uint8_t flags,
                std::span<const uint8_t> value
            );

            [[nodiscard]] std::optional<VersionEntry> get_at(std::span<const uint8_t> key, uint64_t at_seq) const;
            [[nodiscard]] std::vector<VersionEntry> history(std::span<const uint8_t> key) const;

            [[nodiscard]] std::vector<std::pair<std::vector<uint8_t>, std::optional<VersionEntry>>> collect_rollback_targets(uint64_t target_seq) const;

            void close();

        private:
            VersionLog() = default;

            class Impl;
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::vlog

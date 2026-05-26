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

// akkengine/include/akk/engine/wal/WalRecovery.hpp
#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <span>
#include <vector>

namespace akkaradb::engine::memtable {
    class MemTable;
}

namespace akkaradb::engine::wal {
    struct WalRecoveryOptions {
        std::filesystem::path wal_dir;
        uint64_t checkpoint_seq = 0;
        size_t max_entry_bytes = 64ULL * 1024ULL * 1024ULL;
    };

    struct WalRecoveredEntry {
        std::vector<uint8_t> key;
        std::vector<uint8_t> value;
        uint64_t seq = 0;
        uint64_t key_fp64 = 0;
        uint16_t flags = 0;
        uint16_t shard_id = 0;
        uint64_t segment_id = 0;
    };

    struct WalRecoveryResult {
        uint64_t segments_seen = 0;
        uint64_t segments_replayed = 0;
        uint64_t corrupt_segments = 0;
        uint64_t entries_seen = 0;
        uint64_t entries_replayed = 0;
        uint64_t max_seq = 0;
    };

    class WalRecovery {
        public:
            using Callback = std::function<void(const WalRecoveredEntry&)>;

            [[nodiscard]] static WalRecoveryResult recover(const WalRecoveryOptions& options, const Callback& callback);
            [[nodiscard]] static WalRecoveryResult recover_into(const WalRecoveryOptions& options, memtable::MemTable& memtable);
    };
} // namespace akkaradb::engine::wal

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

// akkengine/include/akk/engine/blob/BlobManager.hpp
#pragma once

#include "akk/engine/blob/BlobFraming.hpp"

#include <filesystem>
#include <functional>
#include <memory>
#include <span>
#include <vector>

namespace akkaradb::engine::blob {
    class BlobManager {
        public:
            struct Options {
                std::filesystem::path blob_dir;
                uint64_t threshold_bytes = DEFAULT_THRESHOLD_BYTES;
                BlobCodec codec = BlobCodec::None;
            };

            struct Snapshot {
                uint64_t blobs_written = 0;
                uint64_t bytes_uncompressed = 0;
                uint64_t bytes_on_disk = 0;
                uint64_t blobs_deleted = 0;
                uint64_t gc_cycles = 0;
            };

            [[nodiscard]] static std::unique_ptr<BlobManager> create(Options options);

            ~BlobManager();

            BlobManager(const BlobManager&) = delete;
            BlobManager& operator=(const BlobManager&) = delete;
            BlobManager(BlobManager&&) = delete;
            BlobManager& operator=(BlobManager&&) = delete;

            void start();
            void close();

            [[nodiscard]] uint64_t threshold() const noexcept;
            [[nodiscard]] std::filesystem::path blob_path(uint64_t blob_id) const;

            void write(uint64_t blob_id, std::span<const uint8_t> content);

            [[nodiscard]] std::vector<uint8_t> read(uint64_t blob_id) const;
            [[nodiscard]] std::vector<uint8_t> read(uint64_t blob_id, uint32_t expected_crc32c) const;

            void schedule_delete(uint64_t blob_id);
            void scan_orphans(std::function<bool(uint64_t)> is_referenced);
            [[nodiscard]] Snapshot snapshot() const noexcept;

        private:
            BlobManager() = default;

            class Impl;
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::blob

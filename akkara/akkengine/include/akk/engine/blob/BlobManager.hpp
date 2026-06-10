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
                std::filesystem::path blobDir;
                uint64_t thresholdBytes = DEFAULT_THRESHOLD_BYTES;
                BlobCodec codec = BlobCodec::NONE;
            };

            struct Snapshot {
                uint64_t blobsWritten = 0;
                uint64_t bytesUncompressed = 0;
                uint64_t bytesOnDisk = 0;
                uint64_t blobsDeleted = 0;
                uint64_t gcCycles = 0;
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
            [[nodiscard]] std::filesystem::path blobPath(uint64_t blobId) const;

            void write(uint64_t blobId, std::span<const uint8_t> content);

            [[nodiscard]] std::vector<uint8_t> read(uint64_t blobId) const;
            [[nodiscard]] std::vector<uint8_t> read(uint64_t blobId, uint32_t expectedCrc32c) const;

            void scheduleDelete(uint64_t blobId);
            void scanOrphans(std::function<bool(uint64_t)> isReferenced);
            [[nodiscard]] Snapshot snapshot() const noexcept;

        private:
            BlobManager() = default;

            class Impl;
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::blob

/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/blob/BlobManager.hpp
#pragma once

#include "akkaradb/Export.hpp"

#include "akk/engine/blob/BlobFraming.hpp"

#include <filesystem>
#include <functional>
#include <memory>
#include <span>
#include <vector>

namespace akkaradb::engine::blob {
    class AKDB_API BlobManager {
        public:
            struct Options {
                std::filesystem::path blobDir;
                uint64_t thresholdBytes = DEFAULT_THRESHOLD_BYTES;
                BlobCodec codec = BlobCodec::NONE;
                bool gcOnFlush = false;
                bool gcOnClose = false;
                std::function<void(
                    uint64_t blobId,
                    uint64_t totalSize,
                    uint64_t storedSize,
                    uint32_t contentCrc32c,
                    uint32_t codec
                )> onBlobPut;
                std::function<void(uint64_t blobId)> onBlobDelete;
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
            BlobManager();

            class Impl;
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::blob

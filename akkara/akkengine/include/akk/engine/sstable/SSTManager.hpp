

// akkengine/include/akk/engine/sstable/SSTManager.hpp
/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/sstable/SSTManager.hpp
#pragma once

#include "akkaradb/Export.hpp"

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "akk/core/record/RecordView.hpp"
#include "akk/engine/manifest/Manifest.hpp"
#include "akk/engine/sstable/SSTReader.hpp"
#include "akk/engine/sstable/SSTWriter.hpp"

namespace akkaradb::engine::sst {
    enum class SSTCompactionMode : uint8_t {
        AUTO = 0, BACKGROUND = 1, DISABLED = 2,
    };

    class AKDB_API SSTManager {
        private:
            class Impl;

        public:
            struct Options {
                // Sorted storage level layout and compaction trigger policy.
                int maxLevels = 7;
                int maxL0Files = 4;
                uint64_t l1MaxBytes = 64ULL * 1024ULL * 1024ULL;
                double levelSizeMultiplier = 10.0;
                SSTCompactionMode compactionMode = SSTCompactionMode::AUTO;
                // Flush/compaction output format and read amplification knobs.
                uint64_t targetFileSize = SST_DEFAULT_TARGET_FILE_SIZE;
                uint32_t blockSize = SST_DEFAULT_BLOCK_SIZE;
                uint32_t bloomBitsPerKey = SST_DEFAULT_BLOOM_BITS_PER_KEY;
                uint64_t blockCacheBytes = 64ULL * 1024ULL * 1024ULL;
                // AUTO compaction uses this worker count. 0 disables background compaction in AUTO mode.
                int compactThreads = 2;
                SSTWriter::Codec codec = SSTWriter::Codec::ZSTD;
                int zstdCompressionLevel = 1;
            };

            struct LevelStats {
                int level = 0;
                size_t fileCount = 0;
                uint64_t bytes = 0;
                uint64_t budgetBytes = 0;
            };

            struct CompactionSnapshot {
                uint64_t compactionsCompleted = 0;
                uint64_t filesCompacted = 0;
                uint64_t bytesCompactedIn = 0;
                uint64_t bytesCompactedOut = 0;
                uint64_t compactionFailures = 0;
            };

            class AKDB_API Iterator {
                public:
                    Iterator();
                    ~Iterator();
                    Iterator(Iterator&&) noexcept;
                    Iterator& operator=(Iterator&&) noexcept;
                    Iterator(const Iterator&) = delete;
                    Iterator& operator=(const Iterator&) = delete;

                    [[nodiscard]] bool hasNext() const noexcept;
                    [[nodiscard]] std::optional<SSTRecord> next();

                private:
                    friend class Impl;
                    class Impl;
                    explicit Iterator(std::unique_ptr<Impl> impl);
                    std::unique_ptr<Impl> impl_;
            };

            [[nodiscard]] static std::unique_ptr<SSTManager> create(
                std::filesystem::path sstDir,
                Options options,
                manifest::Manifest* manifest = nullptr
            );

            ~SSTManager();
            SSTManager(const SSTManager&) = delete;
            SSTManager& operator=(const SSTManager&) = delete;

            void recover();
            void shutdown();
            uint64_t flush(std::span<const core::RecordView> records);
            void throwIfBackgroundFailed() const;

            [[nodiscard]] std::optional<SSTRecord> get(std::span<const uint8_t> key, uint64_t snapshotSeq = UINT64_MAX) const;
            [[nodiscard]] std::optional<bool> contains(std::span<const uint8_t> key, uint64_t snapshotSeq = UINT64_MAX) const;
            [[nodiscard]] std::optional<bool> getInto(
                std::span<const uint8_t> key,
                std::vector<uint8_t>& out,
                uint64_t snapshotSeq = UINT64_MAX
            ) const;
            [[nodiscard]] Iterator scanIter(
                std::span<const uint8_t> startKey = {},
                std::span<const uint8_t> endKey = {},
                uint64_t snapshotSeq = UINT64_MAX
            ) const;

            [[nodiscard]] std::vector<LevelStats> levelStats() const;
            [[nodiscard]] uint64_t maxSequence() const noexcept;
            [[nodiscard]] bool compactionPending() const noexcept;
            [[nodiscard]] CompactionSnapshot compactionSnapshot() const noexcept;

        private:
            SSTManager(std::filesystem::path sstDir, Options options, manifest::Manifest* manifest);
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::sst

/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/sstable/SSTReader.hpp
/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/sstable/SSTReader.hpp
#pragma once

#include "akkaradb/Export.hpp"

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "akk/core/record/SSTHdr32.hpp"
#include "akk/core/record/RecordView.hpp"
#include "akk/core/utils/ArenaGenerator.hpp"
#include "akk/engine/sstable/SSTFormat.hpp"

namespace akkaradb::engine::sst {
    struct AKDB_API SSTRecord {
        std::vector<uint8_t> key;
        std::vector<uint8_t> value;
        uint64_t seq = 0;
        uint8_t flags = 0;
        uint64_t keyFp64 = 0;
        uint64_t miniKey = 0;

        [[nodiscard]] bool isTombstone() const noexcept { return (flags & core::SSTHdr32::FLAG_TOMBSTONE) != 0; }
    };

    class AKDB_API SSTReader {
            class Impl;
        public:
            struct CacheStats {
                uint64_t capacityBytes = 0;
                uint64_t residentBytes = 0;
                size_t entries = 0;
                uint64_t hits = 0;
                uint64_t misses = 0;
            };

            class AKDB_API BlockCache {
                public:
                    explicit BlockCache(uint64_t capacityBytes);
                    ~BlockCache();
                    BlockCache(const BlockCache&) = delete;
                    BlockCache& operator=(const BlockCache&) = delete;
                    [[nodiscard]] CacheStats stats() const;
                private:
                    friend class SSTReader::Impl;
                    class Impl;
                    std::unique_ptr<Impl> impl_;
            };

            struct Options {
                // Used for a private cache when sharedBlockCache is empty.
                uint64_t blockCacheBytes = 64ULL * 1024ULL * 1024ULL;
                std::shared_ptr<BlockCache> sharedBlockCache{};
            };

            struct AKDB_API PinnedRecord {
                core::RecordView view;
                std::shared_ptr<const void> lifetime;

                [[nodiscard]] SSTRecord toOwned() const;
            };

            [[nodiscard]] static std::unique_ptr<SSTReader> open(const std::filesystem::path& path);
            [[nodiscard]] static std::unique_ptr<SSTReader> open(const std::filesystem::path& path, const Options& options);

            ~SSTReader();
            SSTReader(const SSTReader&) = delete;
            SSTReader& operator=(const SSTReader&) = delete;
            SSTReader(SSTReader&&) noexcept;
            SSTReader& operator=(SSTReader&&) noexcept;

            // Point reads select the largest retained sequence <= snapshotSeq, including tombstones.
            [[nodiscard]] std::optional<SSTRecord> get(std::span<const uint8_t> key, uint64_t snapshotSeq = UINT64_MAX) const;
            // Borrows decoded bytes, retaining their block across cache eviction
            // and reader destruction. Copy the pin while retaining the view.
            [[nodiscard]] std::optional<PinnedRecord> getPinned(std::span<const uint8_t> key, uint64_t snapshotSeq = UINT64_MAX) const;
            [[nodiscard]] std::optional<bool> contains(std::span<const uint8_t> key, uint64_t snapshotSeq = UINT64_MAX) const;
            [[nodiscard]] std::optional<bool> getInto(
                std::span<const uint8_t> key,
                std::vector<uint8_t>& out,
                uint64_t snapshotSeq = UINT64_MAX
            ) const;
            // Raw scan yields every visible physical version; SSTManager merges versions by key.
            [[nodiscard]] core::ArenaGenerator<SSTRecord> scan(
                std::span<const uint8_t> startKey = {},
                std::span<const uint8_t> endKey = {},
                uint64_t snapshotSeq = UINT64_MAX
            ) const;

            [[nodiscard]] bool keyInRange(std::span<const uint8_t> key) const noexcept;
            [[nodiscard]] const SSTFileHeaderV1& header() const noexcept;
            [[nodiscard]] std::span<const uint8_t> firstKey() const noexcept;
            [[nodiscard]] std::span<const uint8_t> lastKey() const noexcept;
            [[nodiscard]] const std::filesystem::path& path() const noexcept;

        private:
            friend class SSTManager;
            // Defer physical deletion until every retained reader is released.
            void retire() noexcept;
            [[nodiscard]] static bool isPinned(const std::filesystem::path& path);
            SSTReader();
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::sst

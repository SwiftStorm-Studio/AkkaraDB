

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
        public:
            struct Options {
                uint64_t blockCacheBytes = 64ULL * 1024ULL * 1024ULL;
            };

            [[nodiscard]] static std::unique_ptr<SSTReader> open(const std::filesystem::path& path);
            [[nodiscard]] static std::unique_ptr<SSTReader> open(const std::filesystem::path& path, const Options& options);

            ~SSTReader();
            SSTReader(const SSTReader&) = delete;
            SSTReader& operator=(const SSTReader&) = delete;
            SSTReader(SSTReader&&) noexcept;
            SSTReader& operator=(SSTReader&&) noexcept;

            [[nodiscard]] std::optional<SSTRecord> get(std::span<const uint8_t> key) const;
            [[nodiscard]] std::optional<bool> contains(std::span<const uint8_t> key) const;
            [[nodiscard]] std::optional<bool> getInto(std::span<const uint8_t> key, std::vector<uint8_t>& out) const;
            [[nodiscard]] core::ArenaGenerator<SSTRecord> scan(
                std::span<const uint8_t> startKey = {},
                std::span<const uint8_t> endKey = {}
            ) const;

            [[nodiscard]] bool keyInRange(std::span<const uint8_t> key) const noexcept;
            [[nodiscard]] const SSTFileHeaderV2& header() const noexcept;
            [[nodiscard]] std::span<const uint8_t> firstKey() const noexcept;
            [[nodiscard]] std::span<const uint8_t> lastKey() const noexcept;
            [[nodiscard]] const std::filesystem::path& path() const noexcept;

        private:
            SSTReader();
            class Impl;
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::sst

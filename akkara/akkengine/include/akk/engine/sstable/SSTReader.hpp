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

// akkengine/include/akk/engine/sstable/SSTReader.hpp
#pragma once

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
    struct SSTRecord {
        std::vector<uint8_t> key;
        std::vector<uint8_t> value;
        uint64_t seq = 0;
        uint8_t flags = 0;
        uint64_t keyFp64 = 0;
        uint64_t miniKey = 0;

        [[nodiscard]] bool isTombstone() const noexcept { return (flags & core::SSTHdr32::FLAG_TOMBSTONE) != 0; }
    };

    class SSTReader {
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

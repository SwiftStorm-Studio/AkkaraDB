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

// akkengine/include/akk/engine/sstable/SSTFormat.hpp
#pragma once

#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace akkaradb::engine::sst {
    inline constexpr uint32_t SST_MAGIC_V2 = 0x32534B41u; // "AKS2", little-endian
    inline constexpr uint32_t SST_FOOTER_MAGIC_V2 = 0x46533241u; // "A2SF", little-endian
    inline constexpr uint16_t SST_VERSION_V2 = 2;
    inline constexpr uint32_t SST_DEFAULT_BLOCK_SIZE = 32u * 1024u;
    inline constexpr uint32_t SST_DEFAULT_BLOOM_BITS_PER_KEY = 10;
    inline constexpr uint64_t SST_DEFAULT_TARGET_FILE_SIZE = 64ULL * 1024ULL * 1024ULL;

    inline constexpr uint32_t SST_FILE_FLAG_BLOCK_ZSTD = 0x00000001u;

    inline constexpr uint32_t SST_BLOCK_FLAG_COMPRESSED = 0x00000001u;
    inline constexpr uint32_t SST_BLOCK_FLAG_RAW = 0x00000002u;

    inline constexpr uint8_t SST_RECORD_FLAG_TOMBSTONE = 0x01u;
    inline constexpr uint8_t SST_RECORD_FLAG_BLOB = 0x02u;

    [[nodiscard]] constexpr uint64_t align_up_u64(uint64_t value, uint64_t alignment) noexcept { return (value + alignment - 1u) & ~(alignment - 1u); }

    [[nodiscard]] constexpr uint32_t align_up_u32(uint32_t value, uint32_t alignment) noexcept {
        return static_cast<uint32_t>((static_cast<uint64_t>(value) + alignment - 1u) & ~(static_cast<uint64_t>(alignment) - 1u));
    }

    #pragma pack(push, 1)
    struct SSTFileHeaderV2 {
        uint32_t magic;
        uint16_t version;
        uint16_t header_size;
        uint32_t flags;
        uint32_t level;
        uint64_t file_size;
        uint64_t entry_count;
        uint64_t block_count;
        uint64_t data_offset;
        uint64_t index_offset;
        uint64_t index_size;
        uint64_t key_arena_offset;
        uint64_t key_arena_size;
        uint64_t bloom_offset;
        uint64_t bloom_size;
        uint64_t footer_offset;
        uint64_t min_seq;
        uint64_t max_seq;
        uint32_t block_size;
        uint32_t reserved0;
        uint8_t reserved[124];
        uint32_t crc32c;
    };

    struct SSTBlockHeaderV2 {
        uint32_t header_size;
        uint32_t flags;
        uint32_t record_count;
        uint32_t compressed_size;
        uint32_t uncompressed_size;
        uint32_t offsets_size;
        uint64_t first_seq;
        uint64_t last_seq;
        uint64_t first_key_fp64;
        uint64_t last_key_fp64;
        uint32_t crc32c;
        uint32_t reserved;
    };

    struct SSTBlockIndexEntryV2 {
        uint64_t block_offset;
        uint32_t block_size;
        uint32_t uncompressed_size;
        uint64_t first_mini_key;
        uint64_t last_mini_key;
        uint64_t first_key_fp64;
        uint64_t last_key_fp64;
        uint32_t first_key_offset;
        uint32_t first_key_len;
        uint32_t last_key_offset;
        uint32_t last_key_len;
        uint32_t record_count;
        uint32_t flags;
    };

    struct SSTBloomHeaderV2 {
        uint32_t num_bits;
        uint32_t num_hashes;
        uint32_t bits_size;
        uint32_t reserved;
    };

    struct SSTFooterV2 {
        uint64_t file_size;
        uint64_t index_offset;
        uint64_t key_arena_offset;
        uint64_t bloom_offset;
        uint32_t header_crc32c;
        uint32_t footer_crc32c;
        uint32_t magic;
        uint32_t version;
    };
    #pragma pack(pop)

    static_assert(sizeof(SSTFileHeaderV2) == 256, "SSTFileHeaderV2 must be 256 bytes");
    static_assert(sizeof(SSTBlockHeaderV2) == 64, "SSTBlockHeaderV2 must be 64 bytes");
    static_assert(sizeof(SSTBlockIndexEntryV2) == 72, "SSTBlockIndexEntryV2 must be 72 bytes");
    static_assert(sizeof(SSTBloomHeaderV2) == 16, "SSTBloomHeaderV2 must be 16 bytes");
    static_assert(sizeof(SSTFooterV2) == 48, "SSTFooterV2 must be 48 bytes");
    static_assert(std::is_trivially_copyable_v<SSTFileHeaderV2>);
    static_assert(std::is_trivially_copyable_v<SSTBlockHeaderV2>);
    static_assert(std::is_trivially_copyable_v<SSTBlockIndexEntryV2>);
    static_assert(std::is_trivially_copyable_v<SSTBloomHeaderV2>);
    static_assert(std::is_trivially_copyable_v<SSTFooterV2>);
} // namespace akkaradb::engine::sst

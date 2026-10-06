/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/sstable/SSTFormat.hpp
#pragma once

#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace akkaradb::engine::sst {
    inline constexpr uint32_t SST_MAGIC_V1 = 0x31534B41u; // "AKS1", little-endian
    inline constexpr uint32_t SST_FOOTER_MAGIC_V1 = 0x46533141u; // "A1SF", little-endian
    inline constexpr uint16_t SST_VERSION_V1 = 1;
    inline constexpr uint32_t SST_DEFAULT_BLOCK_SIZE = 32u * 1024u;
    inline constexpr uint32_t SST_DEFAULT_BLOOM_BITS_PER_KEY = 10;
    inline constexpr uint64_t SST_DEFAULT_TARGET_FILE_SIZE = 64ULL * 1024ULL * 1024ULL;

    inline constexpr uint32_t SST_FILE_FLAG_BLOCK_ZSTD = 0x00000001u;
    inline constexpr uint32_t SST_FILE_FLAG_METADATA_CRC = 0x00000002u;

    inline constexpr uint32_t SST_BLOCK_FLAG_COMPRESSED = 0x00000001u;
    inline constexpr uint32_t SST_BLOCK_FLAG_RAW = 0x00000002u;
    inline constexpr uint32_t SST_BLOCK_FLAG_PREFIX_COMPRESSED = 0x00000004u;

    inline constexpr uint8_t SST_RECORD_FLAG_TOMBSTONE = 0x01u;
    inline constexpr uint8_t SST_RECORD_FLAG_BLOB = 0x02u;

    [[nodiscard]] constexpr uint64_t alignUpU64(uint64_t value, uint64_t alignment) noexcept {
        return (value + alignment - 1u) & ~(alignment - 1u);
    }

    [[nodiscard]] constexpr uint32_t alignUpU32(uint32_t value, uint32_t alignment) noexcept {
        return static_cast<uint32_t>((static_cast<uint64_t>(value) + alignment - 1u) & ~(static_cast<uint64_t>(alignment) - 1u));
    }

    #pragma pack(push, 1)
    struct SSTFileHeaderV1 {
        uint32_t magic;
        uint16_t version;
        uint16_t headerSize;
        uint32_t flags;
        uint32_t level;
        uint64_t file_size;
        uint64_t entryCount;
        uint64_t blockCount;
        uint64_t dataOffset;
        uint64_t indexOffset;
        uint64_t indexSize;
        uint64_t keyArenaOffset;
        uint64_t keyArenaSize;
        uint64_t bloomOffset;
        uint64_t bloomSize;
        uint64_t footerOffset;
        uint64_t minSeq;
        uint64_t maxSeq;
        uint32_t blockSize;
        uint32_t reserved0;
        uint8_t reserved[124];
        uint32_t crc32c;
    };

    struct SSTBlockHeaderV1 {
        uint32_t headerSize;
        uint32_t flags;
        uint32_t recordCount;
        uint32_t compressedSize;
        uint32_t uncompressedSize;
        uint32_t offsetsSize;
        uint64_t firstSeq;
        uint64_t lastSeq;
        uint64_t firstKeyFp64;
        uint64_t lastKeyFp64;
        uint32_t crc32c;
        uint32_t reserved;
    };

    struct SSTBlockIndexEntryV1 {
        uint64_t blockOffset;
        uint32_t blockSize;
        uint32_t uncompressedSize;
        uint64_t firstMiniKey;
        uint64_t lastMiniKey;
        uint64_t firstKeyFp64;
        uint64_t lastKeyFp64;
        uint32_t firstKeyOffset;
        uint32_t firstKeyLen;
        uint32_t lastKeyOffset;
        uint32_t lastKeyLen;
        uint32_t recordCount;
        uint32_t flags;
    };

    struct SSTBloomHeaderV1 {
        uint32_t numBits;
        uint32_t numHashes;
        uint32_t bitsSize;
        uint32_t reserved;
    };

    struct SSTMetadataCrcV1 {
        uint32_t indexCrc32c;
        uint32_t keyArenaCrc32c;
        uint32_t bloomCrc32c;
    };

    struct SSTFooterV1 {
        uint64_t file_size;
        uint64_t indexOffset;
        uint64_t keyArenaOffset;
        uint64_t bloomOffset;
        uint32_t headerCrc32c;
        uint32_t footerCrc32c;
        uint32_t magic;
        uint32_t version;
    };
    #pragma pack(pop)

    static_assert(sizeof(SSTFileHeaderV1) == 256, "SSTFileHeaderV1 must be 256 bytes");
    static_assert(sizeof(SSTBlockHeaderV1) == 64, "SSTBlockHeaderV1 must be 64 bytes");
    static_assert(sizeof(SSTBlockIndexEntryV1) == 72, "SSTBlockIndexEntryV1 must be 72 bytes");
    static_assert(sizeof(SSTBloomHeaderV1) == 16, "SSTBloomHeaderV1 must be 16 bytes");
    static_assert(sizeof(SSTMetadataCrcV1) == 12, "SSTMetadataCrcV1 must be 12 bytes");
    static_assert(sizeof(SSTFooterV1) == 48, "SSTFooterV1 must be 48 bytes");
    static_assert(std::is_trivially_copyable_v<SSTFileHeaderV1>);
    static_assert(std::is_trivially_copyable_v<SSTBlockHeaderV1>);
    static_assert(std::is_trivially_copyable_v<SSTBlockIndexEntryV1>);
    static_assert(std::is_trivially_copyable_v<SSTBloomHeaderV1>);
    static_assert(std::is_trivially_copyable_v<SSTMetadataCrcV1>);
    static_assert(std::is_trivially_copyable_v<SSTFooterV1>);
} // namespace akkaradb::engine::sst

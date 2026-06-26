/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/wal/WalFraming.hpp
#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <type_traits>
#include <vector>

#include "akkaradb/Export.hpp"

namespace akkaradb::engine::wal {
    struct AKDB_API WalSegmentHeader {
        static constexpr uint32_t MAGIC = 0x414B5741; // "AKWA"
        static constexpr uint16_t VERSION = 0x0001;
        static constexpr uint16_t SIZE = 48;

        uint64_t segmentId = 0;
        uint64_t createdUs = 0;
        uint64_t firstSeq = 0;
        uint64_t lastSeq = 0;
        uint32_t magic = MAGIC;
        uint32_t crc32c = 0;
        uint16_t version = VERSION;
        uint16_t headerSize = SIZE;
        uint16_t shardId = 0;
        uint16_t flags = 0;

        [[nodiscard]] bool verifyMagic() const noexcept { return magic == MAGIC; }
        [[nodiscard]] bool verifyVersion() const noexcept { return version == VERSION && headerSize == SIZE; }
        [[nodiscard]] bool verifyChecksum() const noexcept;
        [[nodiscard]] static WalSegmentHeader build(uint16_t shardId, uint64_t segmentId, uint64_t createdUs) noexcept;
        void serialize(uint8_t out[SIZE]) const noexcept;
        [[nodiscard]] static WalSegmentHeader deserialize(const uint8_t in[SIZE]) noexcept;
    };

    static_assert(sizeof(WalSegmentHeader) == WalSegmentHeader::SIZE, "WalSegmentHeader must be 48 bytes");
    static_assert(alignof(WalSegmentHeader) == 8, "WalSegmentHeader must be 8-byte aligned");
    static_assert(std::is_standard_layout_v<WalSegmentHeader>);
    static_assert(std::is_trivially_copyable_v<WalSegmentHeader>);

    struct AKDB_API WalEntryHeader {
        static constexpr uint16_t SIZE = 32;

        uint64_t seq = 0;
        uint64_t keyFp64 = 0;
        uint32_t entryLen = SIZE;
        uint32_t valueLen = 0;
        uint16_t keyLen = 0;
        uint16_t flags = 0;
        uint32_t crc32c = 0;

        [[nodiscard]] bool verifyLengths(size_t maxEntryBytes) const noexcept;
        [[nodiscard]] bool verifyChecksum(std::span<const uint8_t> key, std::span<const uint8_t> value) const noexcept;
        [[nodiscard]] static WalEntryHeader build(
            uint64_t seq,
            uint64_t keyFp64,
            uint16_t keyLen,
            uint32_t valueLen,
            uint16_t flags
        ) noexcept;
        void serialize(uint8_t out[SIZE]) const noexcept;
        [[nodiscard]] static WalEntryHeader deserialize(const uint8_t in[SIZE]) noexcept;
    };

    static_assert(sizeof(WalEntryHeader) == WalEntryHeader::SIZE, "WalEntryHeader must be 32 bytes");
    static_assert(alignof(WalEntryHeader) == 8, "WalEntryHeader must be 8-byte aligned");
    static_assert(std::is_standard_layout_v<WalEntryHeader>);
    static_assert(std::is_trivially_copyable_v<WalEntryHeader>);

    [[nodiscard]] AKDB_API std::vector<uint8_t> serializeEntry(
        std::span<const uint8_t> key,
        std::span<const uint8_t> value,
        uint64_t seq,
        uint64_t keyFp64,
        uint16_t flags
    );

    [[nodiscard]] AKDB_API uint32_t entryCrc32c(const WalEntryHeader& header, std::span<const uint8_t> key, std::span<const uint8_t> value);
} // namespace akkaradb::engine::wal

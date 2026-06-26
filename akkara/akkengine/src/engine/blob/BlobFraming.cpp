/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/blob/BlobFraming.cpp
#include "akk/engine/blob/BlobFraming.hpp"

#include "akk/cpu/CRC32C.hpp"

#include <cstddef>

namespace akkaradb::engine::blob {
    namespace {
        inline void putU16(uint8_t* out, uint16_t value) noexcept {
            out[0] = static_cast<uint8_t>(value);
            out[1] = static_cast<uint8_t>(value >> 8);
        }

        inline void putU32(uint8_t* out, uint32_t value) noexcept {
            out[0] = static_cast<uint8_t>(value);
            out[1] = static_cast<uint8_t>(value >> 8);
            out[2] = static_cast<uint8_t>(value >> 16);
            out[3] = static_cast<uint8_t>(value >> 24);
        }

        inline void putU64(uint8_t* out, uint64_t value) noexcept {
            for (size_t i = 0; i < 8; ++i) { out[i] = static_cast<uint8_t>(value >> (8u * i)); }
        }

        [[nodiscard]] inline uint16_t getU16(const uint8_t* in) noexcept {
            return static_cast<uint16_t>(in[0]) | static_cast<uint16_t>(static_cast<uint16_t>(in[1]) << 8);
        }

        [[nodiscard]] inline uint32_t getU32(const uint8_t* in) noexcept {
            return static_cast<uint32_t>(in[0]) | (static_cast<uint32_t>(in[1]) << 8) | (static_cast<uint32_t>(in[2]) << 16) | (static_cast<
                uint32_t>(in[3]) << 24);
        }

        [[nodiscard]] inline uint64_t getU64(const uint8_t* in) noexcept {
            uint64_t value = 0;
            for (size_t i = 0; i < 8; ++i) { value |= static_cast<uint64_t>(in[i]) << (8u * i); }
            return value;
        }
    } // namespace

    uint32_t crc32c(std::span<const uint8_t> bytes) noexcept {
        if (bytes.empty()) { return cpu::CRC32C(nullptr, 0); }
        return cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size());
    }

    void encodeBlobRef(uint8_t* out, BlobRef ref) noexcept {
        putU64(out, ref.blobId);
        putU64(out + 8, ref.totalSize);
        putU32(out + 16, ref.contentCrc32c);
    }

    BlobRef decodeBlobRef(const uint8_t* data) noexcept { return BlobRef{getU64(data), getU64(data + 8), getU32(data + 16),}; }

    void serializeBlobHeader(const AkBlobHeaderV5& header, uint8_t out[AKBLOB_HEADER_SIZE_V5]) noexcept {
        putU32(out, header.magic);
        putU16(out + 4, header.version);
        putU16(out + 6, header.headerSize);
        putU32(out + 8, header.flags);
        putU32(out + 12, header.codec);
        putU64(out + 16, header.blobId);
        putU64(out + 24, header.totalSize);
        putU64(out + 32, header.storedSize);
        putU32(out + 40, header.contentCrc32c);
        putU32(out + 44, header.headerCrc32c);
    }

    AkBlobHeaderV5 deserializeBlobHeader(const uint8_t in[AKBLOB_HEADER_SIZE_V5]) noexcept {
        AkBlobHeaderV5 header{};
        header.magic = getU32(in);
        header.version = getU16(in + 4);
        header.headerSize = getU16(in + 6);
        header.flags = getU32(in + 8);
        header.codec = getU32(in + 12);
        header.blobId = getU64(in + 16);
        header.totalSize = getU64(in + 24);
        header.storedSize = getU64(in + 32);
        header.contentCrc32c = getU32(in + 40);
        header.headerCrc32c = getU32(in + 44);
        return header;
    }

    AkBlobHeaderV5 buildBlobHeader(
        uint64_t blobId,
        uint64_t totalSize,
        uint64_t storedSize,
        BlobCodec codec,
        uint32_t contentCrc32c
    ) noexcept {
        AkBlobHeaderV5 header{};
        header.magic = AKBLOB_MAGIC_V5;
        header.version = AKBLOB_VERSION_V5;
        header.headerSize = AKBLOB_HEADER_SIZE_V5;
        header.flags = codec == BlobCodec::ZSTD ? AKBLOB_FLAG_ZSTD : 0;
        header.codec = static_cast<uint32_t>(codec);
        header.blobId = blobId;
        header.totalSize = totalSize;
        header.storedSize = storedSize;
        header.contentCrc32c = contentCrc32c;
        header.headerCrc32c = 0;

        uint8_t bytes[AKBLOB_HEADER_SIZE_V5]{};
        serializeBlobHeader(header, bytes);
        header.headerCrc32c = crc32c(std::span<const uint8_t>{bytes, AKBLOB_HEADER_SIZE_V5 - sizeof(uint32_t)});
        return header;
    }

    bool verifyBlobHeader(const AkBlobHeaderV5& header) noexcept {
        if (header.magic != AKBLOB_MAGIC_V5 || header.version != AKBLOB_VERSION_V5 || header.headerSize != AKBLOB_HEADER_SIZE_V5) {
            return false;
        }
        if (header.codec != static_cast<uint32_t>(BlobCodec::NONE) && header.codec != static_cast<uint32_t>(BlobCodec::ZSTD)) {
            return false;
        }

        AkBlobHeaderV5 copy = header;
        copy.headerCrc32c = 0;
        uint8_t bytes[AKBLOB_HEADER_SIZE_V5]{};
        serializeBlobHeader(copy, bytes);
        return crc32c(std::span<const uint8_t>{bytes, AKBLOB_HEADER_SIZE_V5 - sizeof(uint32_t)}) == header.headerCrc32c;
    }
} // namespace akkaradb::engine::blob

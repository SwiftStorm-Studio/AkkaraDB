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

// akkengine/src/engine/blob/BlobFraming.cpp
#include "akk/engine/blob/BlobFraming.hpp"

#include "akk/cpu/CRC32C.hpp"

#include <cstddef>

namespace akkaradb::engine::blob {
    namespace {
        inline void put_u16(uint8_t* out, uint16_t value) noexcept {
            out[0] = static_cast<uint8_t>(value);
            out[1] = static_cast<uint8_t>(value >> 8);
        }

        inline void put_u32(uint8_t* out, uint32_t value) noexcept {
            out[0] = static_cast<uint8_t>(value);
            out[1] = static_cast<uint8_t>(value >> 8);
            out[2] = static_cast<uint8_t>(value >> 16);
            out[3] = static_cast<uint8_t>(value >> 24);
        }

        inline void put_u64(uint8_t* out, uint64_t value) noexcept { for (size_t i = 0; i < 8; ++i) { out[i] = static_cast<uint8_t>(value >> (8u * i)); } }

        [[nodiscard]] inline uint16_t get_u16(const uint8_t* in) noexcept {
            return static_cast<uint16_t>(in[0]) | static_cast<uint16_t>(static_cast<uint16_t>(in[1]) << 8);
        }

        [[nodiscard]] inline uint32_t get_u32(const uint8_t* in) noexcept {
            return static_cast<uint32_t>(in[0]) | (static_cast<uint32_t>(in[1]) << 8) | (static_cast<uint32_t>(in[2]) << 16) | (static_cast<uint32_t>(in[3]) <<
                24);
        }

        [[nodiscard]] inline uint64_t get_u64(const uint8_t* in) noexcept {
            uint64_t value = 0;
            for (size_t i = 0; i < 8; ++i) { value |= static_cast<uint64_t>(in[i]) << (8u * i); }
            return value;
        }
    } // namespace

    uint32_t crc32c(std::span<const uint8_t> bytes) noexcept {
        if (bytes.empty()) { return cpu::CRC32C(nullptr, 0); }
        return cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size());
    }

    void encode_blob_ref(uint8_t* out, BlobRef ref) noexcept {
        put_u64(out, ref.blob_id);
        put_u64(out + 8, ref.total_size);
        put_u32(out + 16, ref.content_crc32c);
    }

    BlobRef decode_blob_ref(const uint8_t* data) noexcept { return BlobRef{get_u64(data), get_u64(data + 8), get_u32(data + 16),}; }

    void serialize_blob_header(const AkBlobHeaderV5& header, uint8_t out[AKBLOB_HEADER_SIZE_V5]) noexcept {
        put_u32(out, header.magic);
        put_u16(out + 4, header.version);
        put_u16(out + 6, header.header_size);
        put_u32(out + 8, header.flags);
        put_u32(out + 12, header.codec);
        put_u64(out + 16, header.blob_id);
        put_u64(out + 24, header.total_size);
        put_u64(out + 32, header.stored_size);
        put_u32(out + 40, header.content_crc32c);
        put_u32(out + 44, header.header_crc32c);
    }

    AkBlobHeaderV5 deserialize_blob_header(const uint8_t in[AKBLOB_HEADER_SIZE_V5]) noexcept {
        AkBlobHeaderV5 header{};
        header.magic = get_u32(in);
        header.version = get_u16(in + 4);
        header.header_size = get_u16(in + 6);
        header.flags = get_u32(in + 8);
        header.codec = get_u32(in + 12);
        header.blob_id = get_u64(in + 16);
        header.total_size = get_u64(in + 24);
        header.stored_size = get_u64(in + 32);
        header.content_crc32c = get_u32(in + 40);
        header.header_crc32c = get_u32(in + 44);
        return header;
    }

    AkBlobHeaderV5 build_blob_header(uint64_t blob_id, uint64_t total_size, uint64_t stored_size, BlobCodec codec, uint32_t content_crc32c) noexcept {
        AkBlobHeaderV5 header{};
        header.magic = AKBLOB_MAGIC_V5;
        header.version = AKBLOB_VERSION_V5;
        header.header_size = AKBLOB_HEADER_SIZE_V5;
        header.flags = codec == BlobCodec::Zstd ? AKBLOB_FLAG_ZSTD : 0;
        header.codec = static_cast<uint32_t>(codec);
        header.blob_id = blob_id;
        header.total_size = total_size;
        header.stored_size = stored_size;
        header.content_crc32c = content_crc32c;
        header.header_crc32c = 0;

        uint8_t bytes[AKBLOB_HEADER_SIZE_V5]{};
        serialize_blob_header(header, bytes);
        header.header_crc32c = crc32c(std::span<const uint8_t>{bytes, AKBLOB_HEADER_SIZE_V5 - sizeof(uint32_t)});
        return header;
    }

    bool verify_blob_header(const AkBlobHeaderV5& header) noexcept {
        if (header.magic != AKBLOB_MAGIC_V5 || header.version != AKBLOB_VERSION_V5 || header.header_size != AKBLOB_HEADER_SIZE_V5) { return false; }
        if (header.codec != static_cast<uint32_t>(BlobCodec::None) && header.codec != static_cast<uint32_t>(BlobCodec::Zstd)) { return false; }

        AkBlobHeaderV5 copy = header;
        copy.header_crc32c = 0;
        uint8_t bytes[AKBLOB_HEADER_SIZE_V5]{};
        serialize_blob_header(copy, bytes);
        return crc32c(std::span<const uint8_t>{bytes, AKBLOB_HEADER_SIZE_V5 - sizeof(uint32_t)}) == header.header_crc32c;
    }
} // namespace akkaradb::engine::blob

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

// akkengine/include/akk/engine/blob/BlobFraming.hpp
#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace akkaradb::engine::blob {
    enum class BlobCodec : uint32_t {
        None = 0, Zstd = 1,
    };

    inline constexpr size_t BLOB_REF_SIZE = 20;
    inline constexpr uint64_t DEFAULT_THRESHOLD_BYTES = 16ULL * 1024ULL;

    inline constexpr uint32_t AKBLOB_MAGIC_V5 = 0x35424B41u; // "AKB5", little-endian
    inline constexpr uint16_t AKBLOB_VERSION_V5 = 1;
    inline constexpr uint16_t AKBLOB_HEADER_SIZE_V5 = 48;
    inline constexpr uint32_t AKBLOB_FLAG_ZSTD = 0x00000001u;

    struct BlobRef {
        uint64_t blob_id = 0;
        uint64_t total_size = 0;
        uint32_t content_crc32c = 0;
    };

    #pragma pack(push, 1)
    struct AkBlobHeaderV5 {
        uint32_t magic = AKBLOB_MAGIC_V5;
        uint16_t version = AKBLOB_VERSION_V5;
        uint16_t header_size = AKBLOB_HEADER_SIZE_V5;
        uint32_t flags = 0;
        uint32_t codec = static_cast<uint32_t>(BlobCodec::None);
        uint64_t blob_id = 0;
        uint64_t total_size = 0;
        uint64_t stored_size = 0;
        uint32_t content_crc32c = 0;
        uint32_t header_crc32c = 0;
    };
    #pragma pack(pop)

    static_assert(sizeof(AkBlobHeaderV5) == AKBLOB_HEADER_SIZE_V5);

    void encode_blob_ref(uint8_t* out, BlobRef ref) noexcept;
    [[nodiscard]] BlobRef decode_blob_ref(const uint8_t* data) noexcept;

    [[nodiscard]] AkBlobHeaderV5 build_blob_header(
        uint64_t blob_id,
        uint64_t total_size,
        uint64_t stored_size,
        BlobCodec codec,
        uint32_t content_crc32c
    ) noexcept;

    void serialize_blob_header(const AkBlobHeaderV5& header, uint8_t out[AKBLOB_HEADER_SIZE_V5]) noexcept;
    [[nodiscard]] AkBlobHeaderV5 deserialize_blob_header(const uint8_t in[AKBLOB_HEADER_SIZE_V5]) noexcept;
    [[nodiscard]] bool verify_blob_header(const AkBlobHeaderV5& header) noexcept;

    [[nodiscard]] uint32_t crc32c(std::span<const uint8_t> bytes) noexcept;
} // namespace akkaradb::engine::blob

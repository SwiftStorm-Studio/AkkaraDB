/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 */

// internal/src/cpu/crc32c/CRC32CX86AVX2.cpp
#if defined(__x86_64__) || defined(_M_X64) || defined(_M_IX86)

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <nmmintrin.h>

#if defined(__GNUC__) || defined(__clang__)
#  define AKKARADB_TARGET_AVX2 __attribute__((target("avx2,sse4.2")))
#else
#  define AKKARADB_TARGET_AVX2
#endif

namespace akkaradb::cpu {
    [[nodiscard]] AKKARADB_TARGET_AVX2 uint32_t CRC32C_X86_AVX2(const std::byte* data, size_t length) noexcept {
        if (length == 0) { return 0u; }

        const auto* p = reinterpret_cast<const uint8_t*>(data);
        uint64_t crc = 0xFFFFFFFFu;

        while (length >= 8) {
            uint64_t chunk{};
            std::memcpy(&chunk, p, sizeof(chunk));
            crc = _mm_crc32_u64(crc, chunk);
            p += 8;
            length -= 8;
        }

        if (length >= 4) {
            uint32_t chunk{};
            std::memcpy(&chunk, p, sizeof(chunk));
            crc = _mm_crc32_u32(static_cast<uint32_t>(crc), chunk);
            p += 4;
            length -= 4;
        }

        while (length != 0) {
            crc = _mm_crc32_u8(static_cast<uint32_t>(crc), *p);
            ++p;
            --length;
        }

        return static_cast<uint32_t>(~crc);
    }
} // namespace akkaradb::cpu

#undef AKKARADB_TARGET_AVX2

#endif

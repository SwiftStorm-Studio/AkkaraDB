/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/cpu/crc32c/CRC32CX86SSE42.cpp
#if defined(__x86_64__) || defined(_M_X64) || defined(_M_IX86)

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <nmmintrin.h>

#if defined(__GNUC__) || defined(__clang__)
#  define AKKARADB_TARGET_SSE42 __attribute__((target("sse4.2")))
#else
#  define AKKARADB_TARGET_SSE42
#endif

namespace akkaradb::cpu {
    namespace {
        /*
         * SSE4.2 provides scalar CRC32C instructions rather than a SIMD folding
         * primitive. This path is intentionally a tight dependency chain over
         * 64-bit words, with 32-bit and byte tails, and serves as both the
         * standalone SSE4.2 implementation and the low-overhead tail path used
         * after wider AVX2/AVX-512 folds.
         */
        [[nodiscard]] inline uint64_t loadU64(const uint8_t* p) noexcept {
            uint64_t value{};
            std::memcpy(&value, p, sizeof(value));
            return value;
        }

        [[nodiscard]] inline uint32_t loadU32(const uint8_t* p) noexcept {
            uint32_t value{};
            std::memcpy(&value, p, sizeof(value));
            return value;
        }

        [[nodiscard]] inline uint32_t crc32cSse42Update(uint32_t crc, const uint8_t* p, size_t length) noexcept {
            uint64_t wideCrc = crc;

            while (length >= 8) {
                wideCrc = _mm_crc32_u64(wideCrc, loadU64(p));
                p += 8;
                length -= 8;
            }

            if (length >= 4) {
                wideCrc = _mm_crc32_u32(static_cast<uint32_t>(wideCrc), loadU32(p));
                p += 4;
                length -= 4;
            }

            while (length != 0) {
                wideCrc = _mm_crc32_u8(static_cast<uint32_t>(wideCrc), *p);
                ++p;
                --length;
            }

            return static_cast<uint32_t>(wideCrc);
        }
    } // namespace

    /**
     * @brief CRC32C implementation using x86 SSE4.2 CRC instructions.
     *
     * The x86 CRC32 instruction computes the reflected Castagnoli polynomial.
     * The public CRC value is represented with the usual initial and final
     * one's complement, matching the portable slicing-by-8 implementation.
     *
     * @param data Pointer to the input bytes.
     * @param length Number of bytes to process.
     * @return CRC32C checksum for the input.
     */
    [[nodiscard]] AKKARADB_TARGET_SSE42 uint32_t CRC32C_X86_SSE42(const std::byte* data, size_t length) noexcept {
        if (length == 0) { return 0u; }

        const auto* p = reinterpret_cast<const uint8_t*>(data);
        const uint32_t crc = crc32cSse42Update(0xFFFFFFFFu, p, length);

        return ~crc;
    }
} // namespace akkaradb::cpu

#undef AKKARADB_TARGET_SSE42

#endif

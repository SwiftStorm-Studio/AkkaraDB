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

// akkengine/src/cpu/crc32c/CRC32CX86AVX2.cpp
#if defined(__x86_64__) || defined(_M_X64) || defined(_M_IX86)

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <immintrin.h>

#if defined(__GNUC__) || defined(__clang__)
#  define AKKARADB_TARGET_AVX2 __attribute__((target("avx2,pclmul,sse4.2")))
#else
#  define AKKARADB_TARGET_AVX2
#endif

namespace akkaradb::cpu {
    namespace {
        /*
         * AVX2 has 256-bit integer loads but no 256-bit carry-less multiply.
         * This path therefore processes each 64-byte stripe as four 128-bit
         * PCLMULQDQ folds, matching the AVX-512 folding structure while using
         * the widest multiply available on AVX2-era CPUs.
         *
         * CRC32C folding constants are the corresponding 128-bit slices from
         * the same corsix/fast-crc32 generated constant family used by the
         * AVX-512 path in CRC32CX86AVX512.cpp.
         */
        #define clmul_lo(a, b) (_mm_clmulepi64_si128((a), (b), 0x00))
        #define clmul_hi(a, b) (_mm_clmulepi64_si128((a), (b), 0x11))

        [[nodiscard]] inline uint64_t load_u64(const uint8_t* p) noexcept {
            uint64_t value{};
            std::memcpy(&value, p, sizeof(value));
            return value;
        }

        [[nodiscard]] inline uint32_t load_u32(const uint8_t* p) noexcept {
            uint32_t value{};
            std::memcpy(&value, p, sizeof(value));
            return value;
        }

        [[nodiscard]] inline uint32_t crc32c_sse42(uint32_t crc, const uint8_t* p, size_t length) noexcept {
            uint64_t wide_crc = crc;

            while (length >= 8) {
                wide_crc = _mm_crc32_u64(wide_crc, load_u64(p));
                p += 8;
                length -= 8;
            }

            if (length >= 4) {
                wide_crc = _mm_crc32_u32(static_cast<uint32_t>(wide_crc), load_u32(p));
                p += 4;
                length -= 4;
            }

            while (length != 0) {
                wide_crc = _mm_crc32_u8(static_cast<uint32_t>(wide_crc), *p);
                ++p;
                --length;
            }

            return static_cast<uint32_t>(wide_crc);
        }

        [[nodiscard]] inline __m128i fold_16(__m128i value, __m128i next, __m128i k) noexcept {
            return _mm_xor_si128(_mm_xor_si128(clmul_lo(value, k), clmul_hi(value, k)), next);
        }
    } // namespace

    [[nodiscard]] AKKARADB_TARGET_AVX2 uint32_t CRC32C_X86_AVX2(const std::byte* data, size_t length) noexcept {
        if (length == 0) { return 0u; }

        const auto* p = reinterpret_cast<const uint8_t*>(data);
        uint32_t crc = 0xFFFFFFFFu;

        if (length > 256) {
            // Align large inputs to 32 bytes so the folding loop uses stable stripes.
            while (length != 0 && (reinterpret_cast<std::uintptr_t>(p) & 7u) != 0) {
                crc = _mm_crc32_u8(crc, *p);
                ++p;
                --length;
            }

            while ((reinterpret_cast<std::uintptr_t>(p) & 24u) != 0 && length >= 8) {
                crc = static_cast<uint32_t>(_mm_crc32_u64(crc, load_u64(p)));
                p += 8;
                length -= 8;
            }
        }

        // Keep short inputs on the low-overhead SSE4.2 tail path.
        if (length >= 64) {
            const uint8_t* const end = p + length;
            const uint8_t* const limit = p + length - 64;
            const __m128i k64 = _mm_setr_epi32(0x740eef02, 0, 0x9e4addf8, 0);

            __m256i v01 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p));
            __m256i v23 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p + 32));
            __m128i x0 = _mm_xor_si128(_mm256_castsi256_si128(v01), _mm_cvtsi32_si128(static_cast<int>(crc)));
            __m128i x1 = _mm256_extracti128_si256(v01, 1);
            __m128i x2 = _mm256_castsi256_si128(v23);
            __m128i x3 = _mm256_extracti128_si256(v23, 1);
            p += 64;

            while (p <= limit) {
                v01 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p));
                v23 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p + 32));
                x0 = fold_16(x0, _mm256_castsi256_si128(v01), k64);
                x1 = fold_16(x1, _mm256_extracti128_si256(v01, 1), k64);
                x2 = fold_16(x2, _mm256_castsi256_si128(v23), k64);
                x3 = fold_16(x3, _mm256_extracti128_si256(v23, 1), k64);
                p += 64;
            }

            const __m128i k0 = _mm_setr_epi32(0x1c291d04, 0, 0xddc0152b, 0);
            const __m128i k1 = _mm_setr_epi32(0x3da6d0cb, 0, 0xba4fc28e, 0);
            const __m128i k2 = _mm_setr_epi32(0xf20c0dfe, 0, 0x493c7d27, 0);
            const __m128i y0 = _mm_xor_si128(clmul_lo(x0, k0), clmul_hi(x0, k0));
            const __m128i y1 = _mm_xor_si128(clmul_lo(x1, k1), clmul_hi(x1, k1));
            const __m128i y2 = _mm_xor_si128(clmul_lo(x2, k2), clmul_hi(x2, k2));
            const __m128i folded = _mm_xor_si128(_mm_xor_si128(_mm_xor_si128(y0, y1), y2), x3);

            crc = static_cast<uint32_t>(_mm_crc32_u64(0, static_cast<uint64_t>(_mm_cvtsi128_si64(folded))));
            crc = static_cast<uint32_t>(_mm_crc32_u64(crc, static_cast<uint64_t>(_mm_extract_epi64(folded, 1))));
            length = static_cast<size_t>(end - p);
        }

        return ~crc32c_sse42(crc, p, length);
    }
} // namespace akkaradb::cpu

#undef clmul_lo
#undef clmul_hi
#undef AKKARADB_TARGET_AVX2

#endif

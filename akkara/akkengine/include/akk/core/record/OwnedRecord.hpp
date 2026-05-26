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

// akkengine/include/akk/core/record/OwnedRecord.hpp
#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <span>
#include <string_view>

#include "MemHdr16.hpp"
#include "akk/core/buffer/BufferArena.hpp"
#include "akk/core/memory/SmallBuffer.hpp"

namespace akkaradb::core {
    /**
     * OwnedRecord 64B fixed-size owning in-memory record
     *
     * Layout (exactly 64 bytes):
     *
     *   [0..15]  MemHdr16     hdr
     *   [16..23] uint64_t     key_fp64
     *   [24..31] uint64_t     mini_key
     *   [32..63] SmallBuffer  data
     *
     * Design goals:
     *   - Single cache-line footprint (64B)
     *   - Zero heap allocation for small records
     *   - Fast key comparison (mini_key fast path)
     *
     * Notes:
     *   - data = [key][value]
     *   - NOT thread-safe
     */
    struct OwnedRecord {
        [[nodiscard]] static constexpr uint64_t bswap64(uint64_t v) noexcept {
            v = ((v & 0x00FF00FF00FF00FFULL) << 8) | ((v >> 8) & 0x00FF00FF00FF00FFULL);
            v = ((v & 0x0000FFFF0000FFFFULL) << 16) | ((v >> 16) & 0x0000FFFF0000FFFFULL);
            return (v << 32) | (v >> 32);
        }

        [[nodiscard]] static uint64_t load_u64_unaligned(const uint8_t* p) noexcept {
            uint64_t v = 0;
            v |= static_cast<uint64_t>(p[0]);
            v |= static_cast<uint64_t>(p[1]) << 8;
            v |= static_cast<uint64_t>(p[2]) << 16;
            v |= static_cast<uint64_t>(p[3]) << 24;
            v |= static_cast<uint64_t>(p[4]) << 32;
            v |= static_cast<uint64_t>(p[5]) << 40;
            v |= static_cast<uint64_t>(p[6]) << 48;
            v |= static_cast<uint64_t>(p[7]) << 56;
            return v;
        }

        MemHdr16 hdr{};
        uint64_t key_fp64{};
        uint64_t mini_key{};
        SmallBuffer data;

        // ==================== Factory (implemented in .cpp) ====================

        [[nodiscard]] static OwnedRecord create(
            std::span<const uint8_t> key,
            std::span<const uint8_t> value,
            uint64_t seq,
            uint8_t flags,
            BufferArena& arena,
            uint64_t fp64 = 0,
            uint64_t mk = 0
        );
        static void create_inplace(
            OwnedRecord& dst,
            std::span<const uint8_t> key,
            std::span<const uint8_t> value,
            uint64_t seq,
            uint8_t flags,
            BufferArena& arena,
            uint64_t fp64 = 0,
            uint64_t mk = 0
        );

        [[nodiscard]] static OwnedRecord create(std::string_view key, std::string_view value, uint64_t seq, uint8_t flags, BufferArena& arena);

        [[nodiscard]] static OwnedRecord tombstone(std::span<const uint8_t> key, uint64_t seq, BufferArena& arena, uint64_t fp64 = 0);

        // ==================== Accessors (hot path) ====================

        [[nodiscard]] std::span<const uint8_t> key() const noexcept { return {data.data(), hdr.k_len}; }

        [[nodiscard]] std::span<const uint8_t> value() const noexcept { return {data.data() + hdr.k_len, hdr.v_len}; }

        [[nodiscard]] std::string_view key_string() const noexcept { return {reinterpret_cast<const char*>(data.data()), hdr.k_len}; }

        [[nodiscard]] std::string_view value_string() const noexcept { return {reinterpret_cast<const char*>(data.data() + hdr.k_len), hdr.v_len}; }

        [[nodiscard]] uint64_t seq() const noexcept { return hdr.seq; }

        [[nodiscard]] bool is_tombstone() const noexcept { return hdr.is_tombstone(); }

        // ==================== Comparison (hot path) ====================

        [[nodiscard]] int compare_key(const OwnedRecord& other) const noexcept {
            const size_t min_len = std::min(hdr.k_len, other.hdr.k_len);

            if (min_len >= 8) {
                const uint64_t lhs8 = bswap64(mini_key);
                const uint64_t rhs8 = bswap64(other.mini_key);
                if (lhs8 != rhs8) { return lhs8 < rhs8 ? -1 : 1; }
            }
            else if (min_len > 0) { if (int c = std::memcmp(&mini_key, &other.mini_key, min_len); c != 0) { return c < 0 ? -1 : 1; } }

            if (min_len > 8) { if (int c = std::memcmp(data.data() + 8, other.data.data() + 8, min_len - 8); c != 0) return c < 0 ? -1 : 1; }

            if (hdr.k_len < other.hdr.k_len) return -1;
            if (hdr.k_len > other.hdr.k_len) return 1;
            return 0;
        }

        [[nodiscard]] int compare_key(std::span<const uint8_t> other) const noexcept {
            const size_t min_len = std::min<size_t>(hdr.k_len, other.size());

            if (min_len >= 8) {
                const uint64_t lhs8 = bswap64(mini_key);
                const uint64_t rhs8 = bswap64(load_u64_unaligned(other.data()));
                if (lhs8 != rhs8) { return lhs8 < rhs8 ? -1 : 1; }
            }
            else if (min_len > 0) { if (int c = std::memcmp(&mini_key, other.data(), min_len); c != 0) { return c < 0 ? -1 : 1; } }

            if (min_len > 8) { if (int c = std::memcmp(data.data() + 8, other.data() + 8, min_len - 8); c != 0) return c < 0 ? -1 : 1; }

            if (hdr.k_len < other.size()) return -1;
            if (hdr.k_len > other.size()) return 1;
            return 0;
        }

        [[nodiscard]] bool key_equals(const OwnedRecord& other) const noexcept {
            if (hdr.k_len != other.hdr.k_len) return false;
            if (hdr.k_len == 0) return true;

            if (mini_key != other.mini_key) return false;
            if (hdr.k_len <= 8) return true;

            return std::memcmp(data.data() + 8, other.data.data() + 8, hdr.k_len - 8) == 0;
        }

        [[nodiscard]] bool operator<(const OwnedRecord& o) const noexcept { return compare_key(o) < 0; }

        [[nodiscard]] bool operator==(const OwnedRecord& o) const noexcept { return key_equals(o); }
    };

    static_assert(sizeof(OwnedRecord) == 64, "OwnedRecord must be exactly 64 bytes");
} // namespace akkaradb::core

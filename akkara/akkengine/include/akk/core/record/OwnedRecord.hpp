/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
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
     *   [16..23] uint64_t     keyFp64
     *   [24..31] uint64_t     miniKey
     *   [32..63] SmallBuffer  data
     *
     * Design goals:
     *   - Single cache-line footprint (64B)
     *   - Zero heap allocation for small records
     *   - Fast key comparison (miniKey fast path)
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

        [[nodiscard]] static uint64_t loadU64Unaligned(const uint8_t* p) noexcept {
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
        uint64_t keyFp64{};
        uint64_t miniKey{};
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
        static void createInplace(
            OwnedRecord& dst,
            std::span<const uint8_t> key,
            std::span<const uint8_t> value,
            uint64_t seq,
            uint8_t flags,
            BufferArena& arena,
            uint64_t fp64 = 0,
            uint64_t mk = 0
        );

        [[nodiscard]] static OwnedRecord create(
            std::string_view key,
            std::string_view value,
            uint64_t seq,
            uint8_t flags,
            BufferArena& arena
        );

        [[nodiscard]] static OwnedRecord tombstone(std::span<const uint8_t> key, uint64_t seq, BufferArena& arena, uint64_t fp64 = 0);

        // ==================== Accessors (hot path) ====================

        [[nodiscard]] std::span<const uint8_t> key() const noexcept { return {data.data(), hdr.kLen}; }

        [[nodiscard]] std::span<const uint8_t> value() const noexcept { return {data.data() + hdr.kLen, hdr.vLen}; }

        [[nodiscard]] std::string_view keyString() const noexcept { return {reinterpret_cast<const char*>(data.data()), hdr.kLen}; }

        [[nodiscard]] std::string_view valueString() const noexcept {
            return {reinterpret_cast<const char*>(data.data() + hdr.kLen), hdr.vLen};
        }

        [[nodiscard]] uint64_t seq() const noexcept { return hdr.seq; }

        [[nodiscard]] bool isTombstone() const noexcept { return hdr.isTombstone(); }

        // ==================== Comparison (hot path) ====================

        [[nodiscard]] int compareKey(const OwnedRecord& other) const noexcept {
            const size_t minLen = std::min(hdr.kLen, other.hdr.kLen);

            if (minLen >= 8) {
                const uint64_t lhs8 = bswap64(miniKey);
                const uint64_t rhs8 = bswap64(other.miniKey);
                if (lhs8 != rhs8) { return lhs8 < rhs8 ? -1 : 1; }
            }
            else if (minLen > 0) { if (int c = std::memcmp(&miniKey, &other.miniKey, minLen); c != 0) { return c < 0 ? -1 : 1; } }

            if (minLen > 8) { if (int c = std::memcmp(data.data() + 8, other.data.data() + 8, minLen - 8); c != 0) return c < 0 ? -1 : 1; }

            if (hdr.kLen < other.hdr.kLen) return -1;
            if (hdr.kLen > other.hdr.kLen) return 1;
            return 0;
        }

        [[nodiscard]] int compareKey(std::span<const uint8_t> other) const noexcept {
            const size_t minLen = std::min<size_t>(hdr.kLen, other.size());

            if (minLen >= 8) {
                const uint64_t lhs8 = bswap64(miniKey);
                const uint64_t rhs8 = bswap64(loadU64Unaligned(other.data()));
                if (lhs8 != rhs8) { return lhs8 < rhs8 ? -1 : 1; }
            }
            else if (minLen > 0) { if (int c = std::memcmp(&miniKey, other.data(), minLen); c != 0) { return c < 0 ? -1 : 1; } }

            if (minLen > 8) { if (int c = std::memcmp(data.data() + 8, other.data() + 8, minLen - 8); c != 0) return c < 0 ? -1 : 1; }

            if (hdr.kLen < other.size()) return -1;
            if (hdr.kLen > other.size()) return 1;
            return 0;
        }

        [[nodiscard]] bool keyEquals(const OwnedRecord& other) const noexcept {
            if (hdr.kLen != other.hdr.kLen) return false;
            if (hdr.kLen == 0) return true;

            if (miniKey != other.miniKey) return false;
            if (hdr.kLen <= 8) return true;

            return std::memcmp(data.data() + 8, other.data.data() + 8, hdr.kLen - 8) == 0;
        }

        [[nodiscard]] bool operator<(const OwnedRecord& o) const noexcept { return compareKey(o) < 0; }

        [[nodiscard]] bool operator==(const OwnedRecord& o) const noexcept { return keyEquals(o); }
    };

    static_assert(sizeof(OwnedRecord) == 64, "OwnedRecord must be exactly 64 bytes");
} // namespace akkaradb::core

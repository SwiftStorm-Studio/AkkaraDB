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

// akkaradb/include/akkaradb/binpack/detail/WireHelpers.hpp
#pragma once

#include <cstdint>
#include <span>
#include <stdexcept>

namespace akkaradb::binpack::detail {
    template <typename Out>
    inline void writeU8(uint8_t v, Out& out) { out.push_back(v); }

    template <typename Out>
    inline void writeU16(uint16_t v, Out& out) {
        out.push_back(static_cast<uint8_t>(v));
        out.push_back(static_cast<uint8_t>(v >> 8));
    }

    template <typename Out>
    inline void writeU32(uint32_t v, Out& out) {
        out.push_back(static_cast<uint8_t>(v));
        out.push_back(static_cast<uint8_t>(v >> 8));
        out.push_back(static_cast<uint8_t>(v >> 16));
        out.push_back(static_cast<uint8_t>(v >> 24));
    }

    template <typename Out>
    inline void writeU64(uint64_t v, Out& out) { for (size_t i = 0; i < 8; ++i) { out.push_back(static_cast<uint8_t>(v >> (8 * i))); } }

    [[nodiscard]] inline uint8_t readU8(std::span<const uint8_t>& in) {
        if (in.size() < 1) { throw std::runtime_error("BinPack: buffer underflow (u8)"); }
        const uint8_t v = in[0];
        in = in.subspan(1);
        return v;
    }

    [[nodiscard]] inline uint16_t readU16(std::span<const uint8_t>& in) {
        if (in.size() < 2) { throw std::runtime_error("BinPack: buffer underflow (u16)"); }
        const uint16_t v = static_cast<uint16_t>(in[0]) | (static_cast<uint16_t>(in[1]) << 8);
        in = in.subspan(2);
        return v;
    }

    [[nodiscard]] inline uint32_t readU32(std::span<const uint8_t>& in) {
        if (in.size() < 4) { throw std::runtime_error("BinPack: buffer underflow (u32)"); }
        const uint32_t v = static_cast<uint32_t>(in[0]) | (static_cast<uint32_t>(in[1]) << 8) | (static_cast<uint32_t>(in[2]) << 16) | (
            static_cast<uint32_t>(in[3]) << 24);
        in = in.subspan(4);
        return v;
    }

    [[nodiscard]] inline uint64_t readU64(std::span<const uint8_t>& in) {
        if (in.size() < 8) { throw std::runtime_error("BinPack: buffer underflow (u64)"); }
        uint64_t v = 0;
        for (size_t i = 0; i < 8; ++i) { v |= static_cast<uint64_t>(in[i]) << (8 * i); }
        in = in.subspan(8);
        return v;
    }
} // namespace akkaradb::binpack::detail

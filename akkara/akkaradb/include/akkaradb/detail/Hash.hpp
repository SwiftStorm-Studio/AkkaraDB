

// akkaradb/include/akkaradb/detail/Hash.hpp
/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkaradb/include/akkaradb/detail/Hash.hpp
#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace akkaradb::detail {
    [[nodiscard]] constexpr uint64_t fnv1a64(std::string_view s) noexcept {
        uint64_t h = 14695981039346656037ULL;
        for (const unsigned char c : s) {
            h ^= static_cast<uint64_t>(c);
            h *= 1099511628211ULL;
        }
        return h;
    }

    inline void writeLe32(uint32_t v, uint8_t* dst) noexcept {
        dst[0] = static_cast<uint8_t>(v);
        dst[1] = static_cast<uint8_t>(v >> 8);
        dst[2] = static_cast<uint8_t>(v >> 16);
        dst[3] = static_cast<uint8_t>(v >> 24);
    }

    inline void writeLe64(uint64_t v, uint8_t* dst) noexcept {
        for (size_t i = 0; i < 8; ++i) { dst[i] = static_cast<uint8_t>(v >> (8 * i)); }
    }

    [[nodiscard]] inline bool incrementLexicographicBytes(uint8_t* data, size_t size) noexcept {
        for (size_t i = size; i > 0; --i) { if (++data[i - 1] != 0) { return true; } }
        return false;
    }
} // namespace akkaradb::detail

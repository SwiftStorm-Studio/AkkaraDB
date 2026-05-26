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

// akkengine/src/core/record/SSTHdr32.cpp
#include "akk/core/record/SSTHdr32.hpp"

#include <cassert>

namespace akkaradb::core {
    // ==================== SSTHdr32 Implementation ====================
    SSTHdr32 SSTHdr32::create(const uint8_t* key, size_t key_len, size_t value_len, uint64_t seq, uint8_t flags) noexcept {
        assert(key_len <= 0xFFFFu && "SSTHdr32::create: key_len exceeds u16 range");
        assert(value_len <= 0xFFFFu && "SSTHdr32::create: value_len exceeds u16 range");
        assert((flags & ~(FLAG_TOMBSTONE | FLAG_BLOB)) == 0 && "SSTHdr32::create: invalid flags");

        return SSTHdr32{
            .seq = seq,
            .k_len = static_cast<uint16_t>(key_len),
            .v_len = static_cast<uint16_t>(value_len),
            .flags = flags,
            .reserved0 = 0,
            .reserved1 = 0,
            .key_fp64 = compute_key_fp64(key, key_len),
            .mini_key = build_mini_key(key, key_len)
        };
    }
} // namespace akkaradb::core

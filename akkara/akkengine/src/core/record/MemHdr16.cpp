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

// akkengine/src/core/record/MemHdr16.cpp
#include "akk/core/record/MemHdr16.hpp"

#include <cassert>
#include <limits>

namespace akkaradb::core {
    MemHdr16 MemHdr16::create(size_t key_len, size_t value_len, uint64_t seq, uint8_t flags) noexcept {
        assert(key_len <= std::numeric_limits<uint16_t>::max() && "MemHdr16::create: key_len exceeds u16 range");
        assert(value_len <= std::numeric_limits<uint16_t>::max() && "MemHdr16::create: value_len exceeds u16 range");
        assert((flags & ~(FLAG_TOMBSTONE | FLAG_BLOB)) == 0 && "MemHdr16::create: invalid flags");
        return MemHdr16{
            .seq = seq,
            .k_len = static_cast<uint16_t>(key_len),
            .v_len = static_cast<uint16_t>(value_len),
            .flags = flags,
            .version = CURRENT_VERSION,
            .reserved = 0
        };
    }
} // namespace akkaradb::core

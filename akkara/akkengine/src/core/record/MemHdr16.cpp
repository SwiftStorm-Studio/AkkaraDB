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

#include <limits>
#include <stdexcept>

namespace akkaradb::core {
    MemHdr16 MemHdr16::create(size_t keyLen, size_t valueLen, uint64_t seq, uint8_t flags) {
        if (keyLen > std::numeric_limits<uint16_t>::max()) { throw std::length_error("MemHdr16::create: keyLen exceeds u16 range"); }
        if (valueLen > std::numeric_limits<uint16_t>::max()) { throw std::length_error("MemHdr16::create: valueLen exceeds u16 range"); }
        if ((flags & ~(FLAG_TOMBSTONE | FLAG_BLOB)) != 0) { throw std::invalid_argument("MemHdr16::create: invalid flags"); }
        return MemHdr16{
            .seq = seq,
            .kLen = static_cast<uint16_t>(keyLen),
            .vLen = static_cast<uint16_t>(valueLen),
            .flags = flags,
            .version = CURRENT_VERSION,
            .reserved = 0
        };
    }
} // namespace akkaradb::core

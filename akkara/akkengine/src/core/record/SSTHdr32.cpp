/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/core/record/SSTHdr32.cpp
#include "akk/core/record/SSTHdr32.hpp"

#include <limits>
#include <stdexcept>

namespace akkaradb::core {
    // ==================== SSTHdr32 Implementation ====================
    SSTHdr32 SSTHdr32::create(const uint8_t* key, size_t keyLen, size_t valueLen, uint64_t seq, uint8_t flags) {
        if (keyLen > std::numeric_limits<uint16_t>::max()) { throw std::length_error("SSTHdr32::create: keyLen exceeds u16 range"); }
        if (valueLen > std::numeric_limits<uint16_t>::max()) { throw std::length_error("SSTHdr32::create: valueLen exceeds u16 range"); }
        if ((flags & ~(FLAG_TOMBSTONE | FLAG_BLOB)) != 0) { throw std::invalid_argument("SSTHdr32::create: invalid flags"); }

        return SSTHdr32{
            .seq = seq,
            .kLen = static_cast<uint16_t>(keyLen),
            .vLen = static_cast<uint16_t>(valueLen),
            .flags = flags,
            .reserved0 = 0,
            .reserved1 = 0,
            .keyFp64 = computeKeyFp64(key, keyLen),
            .miniKey = buildMiniKey(key, keyLen)
        };
    }
} // namespace akkaradb::core

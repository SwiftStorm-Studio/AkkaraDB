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

// akkengine/src/core/record/OwnedRecord.cpp
#include "akk/core/record/OwnedRecord.hpp"
#include "akk/core/record/KeyFingerprint.hpp"

namespace akkaradb::core {
    // ==================== Factory ====================

    void OwnedRecord::createInplace(
        OwnedRecord& dst,
        std::span<const uint8_t> key,
        std::span<const uint8_t> value,
        uint64_t seq,
        uint8_t flags,
        BufferArena& arena,
        uint64_t fp64,
        uint64_t mk
    ) {
        dst.hdr.kLen = static_cast<uint16_t>(key.size());
        dst.hdr.vLen = static_cast<uint16_t>(value.size());
        dst.hdr.seq = seq;
        dst.hdr.flags = flags;

        dst.keyFp64 = fp64;
        dst.miniKey = (mk != 0) ? mk : buildMiniKey(key);

        dst.data = SmallBuffer(key.data(), key.size(), value.data(), value.size(), arena);
    }

    OwnedRecord OwnedRecord::create(
        std::span<const uint8_t> key,
        std::span<const uint8_t> value,
        uint64_t seq,
        uint8_t flags,
        BufferArena& arena,
        uint64_t fp64,
        uint64_t mk
    ) {
        OwnedRecord r;
        createInplace(r, key, value, seq, flags, arena, fp64, mk);
        return r;
    }

    OwnedRecord OwnedRecord::create(std::string_view key, std::string_view value, uint64_t seq, uint8_t flags, BufferArena& arena) {
        return create(
            std::span{reinterpret_cast<const uint8_t*>(key.data()), key.size()},
            std::span{reinterpret_cast<const uint8_t*>(value.data()), value.size()},
            seq,
            flags,
            arena
        );
    }

    OwnedRecord OwnedRecord::tombstone(std::span<const uint8_t> key, uint64_t seq, BufferArena& arena, uint64_t fp64) {
        return create(key, {}, seq, MemHdr16::FLAG_TOMBSTONE, arena, fp64);
    }
} // namespace akkaradb::core

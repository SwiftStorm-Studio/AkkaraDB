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

// akkengine/include/akk/core/record/MemHdr16.hpp
#pragma once

#include <cstddef>
#include <cstdint>

namespace akkaradb::core {
    /**
     * MemHdr16 - 16-byte fixed-size in-memory record header for MemTable.
     *
     * This header is used exclusively for in-memory records (OwnedRecord) and is
     * intentionally minimal. It contains only the essential metadata required
     * to interpret a record (key/value boundaries, versioning, and flags),
     * excluding any I/O or comparison-specific optimizations.
     *
     * Binary layout (Little-Endian, 16 bytes total):
     * [0..7]   seq      (u64)  - Global sequence number (monotonic)
     * [8..9]   kLen    (u16)  - Key length (0..65535)
     * [10..11] vLen    (u16)  - Value length (0..65535)
     * [12]     flags    (u8)   - Flags (0x01 = TOMBSTONE, etc.)
     * [13]     version  (u8)   - Header format version
     * [14..15] reserved (u16)  - Reserved (must be 0)
     *
     * Design principles:
     * - Minimal footprint: Exactly 16 bytes to maximize cache density
     * - Naturally aligned: No packing directives; relies on standard alignment
     * - In-memory only: Not intended for direct persistence or wire format
     * - Forward-compatible: version + reserved allow safe evolution
     * - POD type: Trivially copyable, standard layout
     *
     * Notes:
     * - Does NOT include key fingerprints or prefix hints (those belong to
     *   higher-level structures or optional runtime optimizations)
     * - Interpreting key/value data requires pairing with the associated
     *   contiguous data buffer ([key | value])
     */
    struct MemHdr16 {
        // ==================== Constants ====================

        static constexpr uint8_t CURRENT_VERSION = 1;

        // ==================== Fields ====================

        uint64_t seq; ///< Global sequence number

        uint16_t kLen; ///< Key length (0..65535)
        uint16_t vLen; ///< Value length (0..65535)

        uint8_t flags; ///< Flags (see FLAG_* constants)

        uint8_t version; ///< Header layout version; must be checked before interpreting fields

        uint16_t reserved; ///< Reserved for future use; must be zero on write, ignored on read

        // ==================== Flag Constants ====================

        static constexpr uint8_t FLAG_NORMAL = 0x00; ///< Normal record
        static constexpr uint8_t FLAG_TOMBSTONE = 0x01; ///< Deleted record (tombstone)
        static constexpr uint8_t FLAG_BLOB = 0x02; ///< Value is a blob reference (external storage)

        // ==================== Methods ====================

        /**
         * Checks if this record is a tombstone (deleted).
         */
        [[nodiscard]] constexpr bool isTombstone() const noexcept { return (flags & FLAG_TOMBSTONE) != 0; }

        /**
         * Returns the total size of the record (header + key + value).
         */
        [[nodiscard]] constexpr size_t totalSize() const noexcept { return sizeof(MemHdr16) + kLen + vLen; }

        /**
         * Creates a MemHdr16 from key/value metadata.
         *
         * @param keyLen Key length
         * @param valueLen Value length
         * @param seq Sequence number
         * @param flags Flags (FLAG_NORMAL or FLAG_TOMBSTONE)
         * @return Initialized header
         */
        [[nodiscard]] static MemHdr16 create(size_t keyLen, size_t valueLen, uint64_t seq, uint8_t flags = FLAG_NORMAL);
    };

    static_assert(sizeof(MemHdr16) == 16);
    static_assert(alignof(MemHdr16) == 8);
} // namespace akkaradb::core

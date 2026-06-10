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

// akkengine/include/akk/core/record/SSTHdr32.hpp
#pragma once

#include "akk/core/record/KeyFingerprint.hpp"

#include <cstdint>
#include <type_traits>

namespace akkaradb::core {
    /**
     * SSTHdr32 - 32-byte fixed-size on-disk record header for SST files.
     *
     * This structure is used as the per-record header inside SST data blocks.
     * It extends the minimal MemHdr16 layout with additional fields used for
     * fast lookup and comparison (fingerprint + prefix hint).
     *
     * Binary layout (Little-Endian, 32 bytes total):
     * [0..7]   seq       (u64)  - Global sequence number (monotonic)
     * [8..9]   kLen     (u16)  - Key length (0..65535)
     * [10..11] vLen     (u16)  - Value length (0..65535)
     * [12]     flags     (u8)   - Flags (0x01 = TOMBSTONE, 0x02 = BLOB)
     * [13]     reserved0 (u8)   - Reserved (must be 0)
     * [14..15] reserved1  (u16)  - Reserved (must be 0)
     * [16..23] keyFp64  (u64)  - Key fingerprint (SipHash-2-4)
     * [24..31] miniKey  (u64)  - First 8 bytes of key (LE-packed)
     *
     * Design principles:
     * - Fixed size: Exactly 32 bytes (cache-line friendly, predictable IO)
     * - Naturally aligned: No packing directives; preserves 8-byte alignment
     * - On-disk optimized: Includes fingerprint and prefix hint for fast seek
     * - Forward-compatible: SST file header owns the format version; reserved
     *   bytes allow record-local evolution without changing the hot header size
     * - POD type: Trivially copyable, standard layout
     *
     * Notes:
     * - keyFp64 is used for fast rejection before full key comparison
     * - miniKey acts as a prefix accelerator for ordered structures
     * - Full key comparison is still required for correctness
     */
    struct SSTHdr32 {
        // ==================== Fields ====================

        uint64_t seq; ///< Global sequence number; used for MVCC visibility and conflict resolution (monotonic within DB)

        uint16_t kLen; ///< Key length in bytes (0..65535); used to locate key in record payload
        uint16_t vLen; ///< Value length in bytes (0..65535); inline or blob-ref depending on flags

        uint8_t flags; ///< Record flags (e.g. TOMBSTONE, BLOB); affects interpretation of value bytes

        uint8_t reserved0; ///< Reserved for future extensions; must be zero on write, ignored on read
        uint16_t reserved1; ///< Reserved for future extensions; must be zero on write, ignored on read

        uint64_t keyFp64; ///< 64-bit key fingerprint (SipHash-2-4); fast negative check before full key compare
        uint64_t miniKey; ///< First 8 bytes of key (LE-packed); prefix hint for ordered search / branch reduction

        // ==================== Flag Constants ====================

        static constexpr uint8_t FLAG_NORMAL = 0x00;
        static constexpr uint8_t FLAG_TOMBSTONE = 0x01;
        static constexpr uint8_t FLAG_BLOB = 0x02;

        // ==================== Methods ====================

        /**
         * Checks if this record is a tombstone (deleted).
         */
        [[nodiscard]] constexpr bool isTombstone() const noexcept { return (flags & FLAG_TOMBSTONE) != 0; }

        /**
         * Returns the total size of the record (header + key + value).
         */
        [[nodiscard]] constexpr size_t totalSize() const noexcept { return sizeof(SSTHdr32) + kLen + vLen; }

        /**
         * Computes SipHash-2-4 fingerprint of a key.
         *
         * Compatibility wrapper for core::computeKeyFp64().
         *
         * @param key Key data
         * @param keyLen Key length
         * @return 64-bit fingerprint
         */
        [[nodiscard]] static uint64_t computeKeyFp64(const uint8_t* key, size_t keyLen) noexcept {
            return core::computeKeyFp64(key, keyLen);
        }

        /**
         * Builds miniKey from the first 8 bytes of the key (Little-Endian packed).
         *
         * If keyLen < 8, remaining bytes are zero.
         *
         * @param key Key data
         * @param keyLen Key length
         * @return 64-bit miniKey value
         */
        [[nodiscard]] static uint64_t buildMiniKey(const uint8_t* key, size_t keyLen) noexcept { return core::buildMiniKey(key, keyLen); }

        /**
         * Creates an SSTHdr32 from key/value metadata. Also computes keyFp64 and miniKey.
         *
         * @param key Key data
         * @param keyLen Key length
         * @param valueLen Value length
         * @param seq Sequence number
         * @param flags Flags (FLAG_NORMAL or FLAG_TOMBSTONE)
         * @return Initialized header
         */
        [[nodiscard]] static SSTHdr32 create(const uint8_t* key, size_t keyLen, size_t valueLen, uint64_t seq, uint8_t flags = FLAG_NORMAL);
    };

    static_assert(sizeof(SSTHdr32) == 32, "SSTHdr32 must be exactly 32 bytes");
    static_assert(alignof(SSTHdr32) == 8, "SSTHdr32 must be 8-byte aligned");
    static_assert(std::is_standard_layout_v<SSTHdr32>);
    static_assert(std::is_trivially_copyable_v<SSTHdr32>);
} // namespace akkaradb::core

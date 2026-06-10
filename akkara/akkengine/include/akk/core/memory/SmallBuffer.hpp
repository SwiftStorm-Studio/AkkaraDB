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

// akkengine/include/akk/core/memory/SmallBuffer.hpp
#pragma once

#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>

#include "akk/core/buffer/BufferArena.hpp"

namespace akkaradb::core {
    /**
     * SmallBuffer SSO + Arena backed contiguous byte buffer (32B fixed)
     *
     * Purpose:
     *   - Replace std::vector<uint8_t> with a cache-friendly fixed-size struct
     *   - Eliminate heap allocation for small payloads (SSO)
     *   - Use BufferArena for large payloads (amortized allocation, no free)
     *
     * Storage layout (exactly 32 bytes):
     *
     *   [0..7]   uint8_t* activePtr_
     *            ↁEAlways points to valid data
     *              - inl_  (inline storage)
     *              - arena memory
     *
     *   [8..9]   uint16_t meta_
     *            ↁETotal byte size (key + value), max 65535
     *
     *   [10..31] uint8_t inl_[22]
     *            ↁEInline storage (INLINE_CAP = 22)
     *
     * Key design decisions:
     *   - No explicit "kind" field (inline vs arena)
     *     ↁEDetermined via pointer comparison (activePtr_ != inl_)
     *     ↁESaves space, preserves 32B invariant
     *
     *   - Branch-free data() access
     *     ↁEactive_ptr_ always valid
     *
     *   - No destructor work
     *     ↁEArena owns memory lifetime
     *
     * Lifetime model:
     *   - Inline: owned by this object
     *   - Arena: owned externally (BufferArena)
     *
     * Thread-safety:
     *   - NOT thread-safe
     */
    struct SmallBuffer {
        static constexpr size_t INLINE_CAP = 22;

        // ==================== Fields ====================

        /**
         * Pointer to active data region.
         *
         * Invariant:
         *   - Always non-null
         *   - Either points to inl_ or arena memory
         */
        uint8_t* activePtr_ = nullptr;

        /**
         * Total size in bytes (key + value).
         */
        uint16_t meta_ = 0;

        /**
         * Inline storage buffer.
         */
        uint8_t inl_[INLINE_CAP] = {};

        // ==================== Accessors ====================

        [[nodiscard]] size_t size() const noexcept { return meta_; }

        [[nodiscard]] bool empty() const noexcept { return meta_ == 0; }

        /**
         * Returns true if buffer uses arena-backed memory.
         *
         * No extra metadata needed; pointer comparison is sufficient.
         */
        [[nodiscard]] bool isArena() const noexcept { return activePtr_ != inl_; }

        /**
         * Branch-free data access.
         */
        [[nodiscard]] const uint8_t* data() const noexcept { return activePtr_; }

        [[nodiscard]] uint8_t* data() noexcept { return activePtr_; }

        // ==================== Constructors ====================

        /**
         * Default: empty inline buffer.
         */
        SmallBuffer() noexcept : activePtr_{inl_} {}

        /**
         * Construct contiguous [key | value].
         *
         * Allocation strategy:
         *   - <= 22 bytes ↁEinline
         *   - > 22 bytes  ↁEarena
         */
        SmallBuffer(const uint8_t* key, size_t kLen, const uint8_t* val, size_t vLen, BufferArena& arena) {
            constexpr size_t maxSize = std::numeric_limits<uint16_t>::max();
            if (kLen > maxSize || vLen > maxSize || kLen > maxSize - vLen) {
                throw std::length_error("SmallBuffer: combined key/value length exceeds u16 range");
            }
            const size_t n = kLen + vLen;

            meta_ = static_cast<uint16_t>(n);

            // Select storage (branch happens once at construction)
            activePtr_ = n <= INLINE_CAP ? inl_ : reinterpret_cast<uint8_t*>(arena.allocate(n));

            // Copy payload
            if (kLen) std::memcpy(activePtr_, key, kLen);
            if (vLen) std::memcpy(activePtr_ + kLen, val, vLen);
        }

        /**
         * Destructor:
         *   - No-op (arena owns external memory)
         */
        ~SmallBuffer() noexcept = default;

        // ==================== Move ====================

        /**
         * Move constructor.
         * * For arena-backed buffers, this transfers the pointer (O(1)).
         * For inline buffers, this performs a small physical copy to the new object's
         * internal storage and resets the source to maintain SSO invariants.
         */
        SmallBuffer(SmallBuffer&& o) noexcept
            : meta_{o.meta_} {
            if (o.isArena()) {
                // Arena-backed: steal the external pointer
                activePtr_ = o.activePtr_;
            }
            else {
                // Inline: copy bytes to our own 'inl_' and point activePtr_ here
                activePtr_ = inl_;
                if (meta_ > 0) { std::memcpy(inl_, o.inl_, meta_); }
            }

            // Ensure the source is left in a valid, empty inline state
            o.resetToEmpty();
        }

        /**
         * Move assignment operator.
         */
        SmallBuffer& operator=(SmallBuffer&& o) noexcept {
            if (this == &o) return *this;

            meta_ = o.meta_;
            if (o.isArena()) { activePtr_ = o.activePtr_; }
            else {
                activePtr_ = inl_;
                if (meta_ > 0) { std::memcpy(inl_, o.inl_, meta_); }
            }

            o.resetToEmpty();
            return *this;
        }

        // ==================== Copy disabled ====================

        /**
         * Copy is intentionally disabled.
         *
         * Reason:
         *   - Arena-backed memory cannot be safely duplicated
         *     without explicit allocation policy
         */
        SmallBuffer(const SmallBuffer&) = delete;
        SmallBuffer& operator=(const SmallBuffer&) = delete;

        private:
            /**
             * Resets the buffer to an empty inline state.
             * Internal helper to ensure SSO invariants after move operations.
             */
            void resetToEmpty() noexcept {
                activePtr_ = inl_;
                meta_ = 0;
            }
    };

    // Enforce strict layout guarantee (critical for OwnedRecord = 64B)
    static_assert(sizeof(SmallBuffer) == 32, "SmallBuffer must be 32 bytes");
} // namespace akkaradb::core

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

// akkengine/include/akk/core/record/RecordView.hpp
#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <span>
#include <string_view>

namespace akkaradb::core {
    /**
     * RecordView unified zero-copy record view (MemTable + SST)
     *
     * Purpose:
     *   - Provide a single read interface across:
     *       - OwnedRecord (in-memory, arena-backed)
     *       - SST record (on-disk / mmap)
     *
     *   - Eliminate copies in read/iterator path
     *   - Enable type unification (no variant, no branching on type)
     *
     * Design:
     *   - Non-owning (view only)
     *   - Trivially copyable (just pointers + integers)
     *   - Immutable
     *
     * Layout (no fixed memory layout; logical structure):
     *
     *   keyPtr_   ↁEkey bytes
     *   valPtr_   ↁEvalue bytes
     *   kLen_     ↁEkey length
     *   vLen_     ↁEvalue length
     *   seq_       ↁEsequence number
     *   flags_     ↁEtombstone etc.
     *   keyFp64_  ↁEhash fingerprint
     *   miniKey_  ↁEfirst ≤8 bytes of key (LE packed)
     *
     * Lifetime:
     *   - MemTable: tied to BufferArena lifetime
     *   - SST: tied to mmap / block buffer lifetime
     *
     * Thread-safety:
     *   - Safe if underlying memory is immutable
     */
    class RecordView {
        public:
            [[nodiscard]] static constexpr uint64_t bswap64(uint64_t v) noexcept {
                v = ((v & 0x00FF00FF00FF00FFULL) << 8) | ((v >> 8) & 0x00FF00FF00FF00FFULL);
                v = ((v & 0x0000FFFF0000FFFFULL) << 16) | ((v >> 16) & 0x0000FFFF0000FFFFULL);
                return (v << 32) | (v >> 32);
            }

            [[nodiscard]] static uint64_t loadU64Unaligned(const uint8_t* p) noexcept {
                uint64_t v = 0;
                v |= static_cast<uint64_t>(p[0]);
                v |= static_cast<uint64_t>(p[1]) << 8;
                v |= static_cast<uint64_t>(p[2]) << 16;
                v |= static_cast<uint64_t>(p[3]) << 24;
                v |= static_cast<uint64_t>(p[4]) << 32;
                v |= static_cast<uint64_t>(p[5]) << 40;
                v |= static_cast<uint64_t>(p[6]) << 48;
                v |= static_cast<uint64_t>(p[7]) << 56;
                return v;
            }

            // ==================== Constructors ====================

            /**
             * Empty view (null state).
             */
            constexpr RecordView() noexcept = default;

            /**
             * Full constructor.
             */
            constexpr RecordView(
                const uint8_t* key,
                uint16_t kLen,
                const uint8_t* value,
                uint16_t vLen,
                uint64_t seq,
                uint8_t flags,
                uint64_t keyFp64,
                uint64_t miniKey
            ) noexcept
                : key_{key}, value_{value}, kLen_{kLen}, vLen_{vLen}, seq_{seq}, flags_{flags}, keyFp64_{keyFp64}, miniKey_{miniKey} {}

            // ==================== Accessors ====================

            [[nodiscard]] bool empty() const noexcept { return key_ == nullptr; }

            [[nodiscard]] std::span<const uint8_t> key() const noexcept { return {key_, kLen_}; }

            [[nodiscard]] std::span<const uint8_t> value() const noexcept { return {value_, vLen_}; }

            [[nodiscard]] std::string_view keyString() const noexcept { return {reinterpret_cast<const char*>(key_), kLen_}; }

            [[nodiscard]] std::string_view valueString() const noexcept { return {reinterpret_cast<const char*>(value_), vLen_}; }

            [[nodiscard]] uint16_t keySize() const noexcept { return kLen_; }
            [[nodiscard]] uint16_t valueSize() const noexcept { return vLen_; }

            [[nodiscard]] uint64_t seq() const noexcept { return seq_; }
            [[nodiscard]] uint8_t flags() const noexcept { return flags_; }

            [[nodiscard]] bool isTombstone() const noexcept { return (flags_ & FLAG_TOMBSTONE) != 0; }

            [[nodiscard]] uint64_t keyFp64() const noexcept { return keyFp64_; }
            [[nodiscard]] uint64_t miniKey() const noexcept { return miniKey_; }

            // ==================== Comparison ====================

            /**
             * Lexicographic key comparison (fast-path optimized).
             *
             * Fast path:
             *   - Compare miniKey (≤8 bytes, register-only)
             *
             * Slow path:
             *   - memcmp remaining bytes
             */
            [[nodiscard]] int compareKey(const RecordView& other) const noexcept {
                const size_t minLen = std::min(kLen_, other.kLen_);
                if (minLen >= 8) {
                    const uint64_t lhs8 = bswap64(miniKey_);
                    const uint64_t rhs8 = bswap64(other.miniKey_);
                    if (lhs8 != rhs8) { return lhs8 < rhs8 ? -1 : 1; }
                }
                else if (minLen > 0) { if (int c = std::memcmp(&miniKey_, &other.miniKey_, minLen); c != 0) { return c < 0 ? -1 : 1; } }

                if (minLen > 8) { if (int c = std::memcmp(key_ + 8, other.key_ + 8, minLen - 8); c != 0) return c < 0 ? -1 : 1; }

                if (kLen_ < other.kLen_) return -1;
                if (kLen_ > other.kLen_) return 1;
                return 0;
            }

            /**
             * Compare with raw key.
             */
            [[nodiscard]] int compareKey(std::span<const uint8_t> other) const noexcept {
                const size_t minLen = std::min<size_t>(kLen_, other.size());
                if (minLen >= 8) {
                    const uint64_t lhs8 = bswap64(miniKey_);
                    const uint64_t rhs8 = bswap64(loadU64Unaligned(other.data()));
                    if (lhs8 != rhs8) { return lhs8 < rhs8 ? -1 : 1; }
                }
                else if (minLen > 0) { if (int c = std::memcmp(&miniKey_, other.data(), minLen); c != 0) { return c < 0 ? -1 : 1; } }

                if (minLen > 8) { if (int c = std::memcmp(key_ + 8, other.data() + 8, minLen - 8); c != 0) return c < 0 ? -1 : 1; }

                if (kLen_ < other.size()) return -1;
                if (kLen_ > other.size()) return 1;
                return 0;
            }

            /**
             * Equality check.
             */
            [[nodiscard]] bool keyEquals(const RecordView& other) const noexcept {
                if (kLen_ != other.kLen_) return false;
                if (kLen_ == 0) return true;

                if (miniKey_ != other.miniKey_) return false;
                if (kLen_ <= 8) return true;

                return std::memcmp(key_ + 8, other.key_ + 8, kLen_ - 8) == 0;
            }

            // ==================== Operators ====================

            [[nodiscard]] bool operator<(const RecordView& o) const noexcept { return compareKey(o) < 0; }

            [[nodiscard]] bool operator==(const RecordView& o) const noexcept { return keyEquals(o); }

            // ==================== Flags ====================

            static constexpr uint8_t FLAG_TOMBSTONE = 0x1;

        private:
            const uint8_t* key_{nullptr};
            const uint8_t* value_{nullptr};

            uint16_t kLen_{0};
            uint16_t vLen_{0};

            uint64_t seq_{0};
            uint8_t flags_{0};

            uint64_t keyFp64_{0};
            uint64_t miniKey_{0};
    };
} // namespace akkaradb::core

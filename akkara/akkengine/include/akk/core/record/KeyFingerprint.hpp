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

// akkengine/include/akk/core/record/KeyFingerprint.hpp
#pragma once

#include "akkaradb/Export.hpp"

#include <cstddef>
#include <cstdint>
#include <span>

namespace akkaradb::core {
    /**
     * Computes the 64-bit fingerprint used for fast key rejection.
     *
     * The current implementation is SipHash-2-4 with AkkaraDB's stable default
     * seed.  The fingerprint is an optimization hint only; callers must still
     * compare full keys for correctness.
     *
     * @param key     Pointer to key bytes. May be null only when keyLen == 0.
     * @param keyLen Key length in bytes.
     * @return 64-bit key fingerprint.
     */
    [[nodiscard]] AKDB_API uint64_t computeKeyFp64(const uint8_t* key, size_t keyLen) noexcept;

    /**
     * Computes the 64-bit fingerprint used for fast key rejection.
     */
    [[nodiscard]] inline uint64_t computeKeyFp64(std::span<const uint8_t> key) noexcept {
        return key.empty() ? 0ULL : computeKeyFp64(key.data(), key.size());
    }

    /**
     * Builds the mini-key prefix hint from the first up to eight key bytes.
     *
     * Bytes are packed little-endian.  Missing bytes are zero-filled when the
     * key is shorter than eight bytes.
     *
     * @param key     Pointer to key bytes. May be null only when keyLen == 0.
     * @param keyLen Key length in bytes.
     * @return 64-bit mini-key prefix hint.
     */
    [[nodiscard]] AKDB_API uint64_t buildMiniKey(const uint8_t* key, size_t keyLen) noexcept;

    /**
     * Builds the mini-key prefix hint from the first up to eight key bytes.
     */
    [[nodiscard]] inline uint64_t buildMiniKey(std::span<const uint8_t> key) noexcept {
        return key.empty() ? 0ULL : buildMiniKey(key.data(), key.size());
    }
} // namespace akkaradb::core

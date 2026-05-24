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

// internal/include/cpu/CRC32C.hpp
#pragma once

#include <cstddef>
#include <cstdint>

namespace akkaradb::cpu {
    /**
     * @brief Compute CRC32C (Castagnoli) checksum.
     *
     * The dispatcher selects the fastest available implementation at runtime:
     * - x86 / x64 AVX-512-capable path when available
     * - x86 / x64 AVX2-capable path when available
     * - x86 / x64 SSE4.2 CRC instructions
     * - AArch64 CRC instructions
     * - portable slicing-by-8 fallback
     *
     * @param data Pointer to the input bytes.
     *             May be null only when @p length is 0.
     * @param length Number of bytes to process.
     * @return CRC32C checksum for the input.
     */
    [[nodiscard]] uint32_t CRC32C(const std::byte* data, size_t length) noexcept;
} // namespace akkaradb::cpu

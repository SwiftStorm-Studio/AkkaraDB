/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/cpu/CRC32C.hpp
#pragma once

#include "akkaradb/Export.hpp"

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
    [[nodiscard]] AKDB_API uint32_t CRC32C(const std::byte* data, size_t length) noexcept;
} // namespace akkaradb::cpu

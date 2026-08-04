/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/cpu/crc32c/CRC32CArmCRC.cpp
#if defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)

#include "akk/cpu/CRC32C.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <armAcle.h>

namespace akkaradb::cpu {
    /**
     * @brief CRC32C implementation using AArch64 CRC instructions.
     *
     * @param data Pointer to the input bytes.
     * @param length Number of bytes to process.
     * @return CRC32C checksum for the input.
     */
    [[nodiscard]] uint32_t CRC32C_ARM_CRC(const std::byte* data, size_t length) noexcept {
        if (length == 0) { return 0u; }

        const auto* p = reinterpret_cast<const uint8_t*>(data);
        uint32_t crc = 0xFFFFFFFFu;

        while (length >= 8) {
            uint64_t chunk{};
            std::memcpy(&chunk, p, sizeof(chunk));
            crc = __crc32cd(crc, chunk);
            p += 8;
            length -= 8;
        }

        if (length >= 4) {
            uint32_t chunk{};
            std::memcpy(&chunk, p, sizeof(chunk));
            crc = __crc32cw(crc, chunk);
            p += 4;
            length -= 4;
        }

        while (length != 0) {
            crc = __crc32cb(crc, *p);
            ++p;
            --length;
        }

        return ~crc;
    }
} // namespace akkaradb::cpu

#endif

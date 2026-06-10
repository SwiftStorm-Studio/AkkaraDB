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

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

// akkengine/src/cpu/crc32c/CRC32CRef.cpp
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace akkaradb::cpu {
    namespace {
        constexpr uint32_t kPolynomial = 0x82F63B78u;

        using Table = std::array<uint32_t, 256>;
        using Tables = std::array<Table, 8>;

        /**
         * @brief Builds the base CRC32C table.
         *
         * @return A 256-entry lookup table for one-byte updates.
         */
        constexpr Table BuildBaseTable() noexcept {
            Table table{};

            for (uint32_t i = 0; i < 256; ++i) {
                uint32_t crc = i;
                for (int bit = 0; bit < 8; ++bit) { crc = (crc & 1u) ? ((crc >> 1) ^ kPolynomial) : (crc >> 1); }
                table[i] = crc;
            }

            return table;
        }

        /**
         * @brief Builds slicing-by-8 lookup tables.
         *
         * @return Eight tables that allow processing 8 bytes per iteration.
         */
        constexpr Tables BuildTables() noexcept {
            Tables tables{};
            tables[0] = BuildBaseTable();

            for (size_t n = 1; n < tables.size(); ++n) {
                for (size_t i = 0; i < 256; ++i) {
                    const uint32_t crc = tables[n - 1][i];
                    tables[n][i] = (crc >> 8) ^ tables[0][crc & 0xFFu];
                }
            }

            return tables;
        }

        constinit const Tables kTables = BuildTables();

        /**
         * @brief Reads a little-endian 32-bit word from unaligned memory.
         *
         * @param p Source pointer.
         * @return 32-bit little-endian word.
         */
        [[nodiscard]] inline uint32_t LoadU32LE(const std::byte* p) noexcept {
            uint32_t value{};
            std::memcpy(&value, p, sizeof(value));
            return value;
        }
    } // namespace

    /**
     * @brief Portable CRC32C implementation.
     *
     * This version uses slicing-by-8, which is significantly faster than a
     * byte-at-a-time loop while staying fully portable.
     *
     * @param data Pointer to the input bytes.
     * @param length Number of bytes to process.
     * @return CRC32C checksum for the input.
     */
    [[nodiscard]] uint32_t CRC32C_Ref(const std::byte* data, size_t length) noexcept {
        if (length == 0) { return 0u; }

        const auto* p = reinterpret_cast<const uint8_t*>(data);
        uint32_t crc = 0xFFFFFFFFu;

        while (length >= 8) {
            const uint32_t d0 = LoadU32LE(reinterpret_cast<const std::byte*>(p)) ^ crc;
            const uint32_t d1 = LoadU32LE(reinterpret_cast<const std::byte*>(p + 4));

            crc = kTables[7][d0 & 0xFFu] ^ kTables[6][(d0 >> 8) & 0xFFu] ^ kTables[5][(d0 >> 16) & 0xFFu] ^ kTables[4][(d0 >> 24) & 0xFFu] ^ kTables[3][d1 &
                0xFFu] ^ kTables[2][(d1 >> 8) & 0xFFu] ^ kTables[1][(d1 >> 16) & 0xFFu] ^ kTables[0][(d1 >> 24) & 0xFFu];

            p += 8;
            length -= 8;
        }

        while (length != 0) {
            crc = (crc >> 8) ^ kTables[0][(crc ^ *p) & 0xFFu];
            ++p;
            --length;
        }

        return ~crc;
    }
} // namespace akkaradb::cpu

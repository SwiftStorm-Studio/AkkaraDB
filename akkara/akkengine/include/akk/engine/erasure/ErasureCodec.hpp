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

// akkengine/include/akk/engine/erasure/ErasureCodec.hpp
#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace akkaradb::engine::erasure {
    enum class ErasureCodecKind : uint8_t {
        Xor = 1,
        DualXor = 2,
    };

    struct ErasureLayout {
        uint16_t data_shards = 0;
        uint16_t parity_shards = 1;

        [[nodiscard]] uint16_t total_shards() const noexcept;
    };

    struct ErasureShard {
        uint16_t index = 0;
        uint64_t original_size = 0;
        ErasureCodecKind codec = ErasureCodecKind::Xor;
        std::vector<uint8_t> payload;
        uint32_t crc32c = 0;

        [[nodiscard]] bool verify_crc() const noexcept;
    };

    class XorErasureCodec {
        public:
            /**
             * Encodes value into k data shards and one XOR parity shard.
             *
             * XOR repair can recover at most one missing shard. Payloads are
             * fixed-width and zero-padded; original_size records the logical
             * value length for decode trimming.
             */
            [[nodiscard]] static std::vector<ErasureShard> encode(std::span<const uint8_t> value, ErasureLayout layout);

            /**
             * Decodes the original value from all data shards, or from one
             * missing data shard plus the parity shard.
             *
             * @throws std::invalid_argument on invalid layout or shard set.
             * @throws std::runtime_error on CRC mismatch or insufficient shards.
             */
            [[nodiscard]] static std::vector<uint8_t> decode(std::span<const ErasureShard> shards, ErasureLayout layout);

            /**
             * Repairs one missing data or parity shard.
             *
             * @throws std::invalid_argument when missing_index is out of range
             *         or the input set is malformed.
             * @throws std::runtime_error on CRC mismatch or insufficient shards.
             */
            [[nodiscard]] static ErasureShard repair_one(uint16_t missing_index, std::span<const ErasureShard> shards, ErasureLayout layout);

            [[nodiscard]] static size_t shard_payload_size(uint64_t original_size, uint16_t data_shards);
    };
} // namespace akkaradb::engine::erasure

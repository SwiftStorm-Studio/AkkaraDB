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

#include "akkaradb/Export.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace akkaradb::engine::erasure {
    enum class ErasureCodecKind : uint8_t {
        XOR = 1, DUAL_XOR = 2,
    };

    struct AKDB_API ErasureLayout {
        uint16_t dataShards = 0;
        uint16_t parityShards = 1;

        [[nodiscard]] uint16_t totalShards() const noexcept;
    };

    struct AKDB_API ErasureShard {
        uint16_t index = 0;
        uint64_t originalSize = 0;
        ErasureCodecKind codec = ErasureCodecKind::XOR;
        std::vector<uint8_t> payload;
        uint32_t crc32c = 0;

        [[nodiscard]] bool verifyCrc() const noexcept;
    };

    class AKDB_API XorErasureCodec {
        public:
            /**
             * Encodes value into k data shards and one XOR parity shard.
             *
             * XOR repair can recover at most one missing shard. Payloads are
             * fixed-width and zero-padded; originalSize records the logical
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
             * @throws std::invalid_argument when missingIndex is out of range
             *         or the input set is malformed.
             * @throws std::runtime_error on CRC mismatch or insufficient shards.
             */
            [[nodiscard]] static ErasureShard repairOne(uint16_t missingIndex, std::span<const ErasureShard> shards, ErasureLayout layout);

            [[nodiscard]] static size_t shardPayloadSize(uint64_t originalSize, uint16_t dataShards);
    };
} // namespace akkaradb::engine::erasure

/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
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
        RS = 1, ERS = 2
    };

    struct AKDB_API ErasureLayout {
        uint16_t dataShards = 0;
        uint16_t parityShards = 2;

        [[nodiscard]] uint16_t totalShards() const noexcept;
    };

    struct AKDB_API ErasureShard {
        uint16_t index = 0;
        uint64_t originalSize = 0;
        ErasureCodecKind codec = ErasureCodecKind::RS;
        std::vector<uint8_t> payload;
        uint32_t crc32c = 0;

        [[nodiscard]] bool verifyCrc() const noexcept;
    };

    class AKDB_API RsErasureCodec {
        public:
            /**
             * Encodes value into k systematic data shards plus m Reed-Solomon
             * parity shards over GF(256).
             */
            [[nodiscard]] static std::vector<ErasureShard> encode(std::span<const uint8_t> value, ErasureLayout layout);

            /**
             * Decodes the original value from any k valid shards.
             *
             * @throws std::invalid_argument on invalid layout or shard set.
             * @throws std::runtime_error on CRC mismatch or insufficient shards.
             */
            [[nodiscard]] static std::vector<uint8_t> decode(std::span<const ErasureShard> shards, ErasureLayout layout);

            /**
             * Repairs one missing shard when the remaining shard set still has
             * enough information to reconstruct the original data.
             */
            [[nodiscard]] static ErasureShard repairOne(uint16_t missingIndex, std::span<const ErasureShard> shards, ErasureLayout layout);

            [[nodiscard]] static size_t shardPayloadSize(uint64_t originalSize, uint16_t dataShards);
    };
} // namespace akkaradb::engine::erasure

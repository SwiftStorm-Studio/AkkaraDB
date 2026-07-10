/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/erasure/ErasureCodecExt.hpp
#pragma once

#include "akk/engine/erasure/ErasureCodec.hpp"

#include <span>
#include <vector>

namespace akkaradb::engine::erasure {
    struct AKDB_API ErsRecoveryResult {
        std::vector<uint8_t> value;
        std::vector<ErasureShard> repairedShards;
        std::vector<uint16_t> missingIndices;
        std::vector<uint16_t> detectedErrorIndices;
    };

    class AKDB_API ErsCodec {
        public:
            /**
             * Encodes value into the same systematic Reed-Solomon shard layout
             * as RsErasureCodec, but tags the shards as ERS for the extended
             * error-correcting recovery path.
             */
            [[nodiscard]] static std::vector<ErasureShard> encode(std::span<const uint8_t> value, ErasureLayout layout);

            /**
             * Recovers the original value while also identifying repaired
             * shards. knownBadIndices may contain missing or suspicious shard
             * indices that should be treated as erasures up front.
             *
             * The current implementation performs bounded search for up to two
             * unknown corrupted shards in addition to known erasures.
             */
            [[nodiscard]] static ErsRecoveryResult recover(
                std::span<const ErasureShard> shards,
                ErasureLayout layout,
                std::span<const uint16_t> knownBadIndices = {}
            );

            [[nodiscard]] static std::vector<uint8_t> decode(
                std::span<const ErasureShard> shards,
                ErasureLayout layout,
                std::span<const uint16_t> knownBadIndices = {}
            );

            [[nodiscard]] static size_t shardPayloadSize(uint64_t originalSize, uint16_t dataShards);
    };
} // namespace akkaradb::engine::erasure

/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/erasure/RsErasureCodec.cpp
#include "akk/engine/erasure/detail/ErasureCodecInternal.hpp"

namespace akkaradb::engine::erasure {
    using namespace detail;

    size_t RsErasureCodec::shardPayloadSize(uint64_t originalSize, uint16_t dataShards) {
        if (dataShards == 0) { throw std::invalid_argument("RsErasureCodec: dataShards must be > 0"); }
        const uint64_t size = (originalSize / dataShards) + ((originalSize % dataShards) != 0 ? 1u : 0u);
        if (size > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
            throw std::runtime_error("RsErasureCodec: shard payload is too large for this platform");
        }
        return static_cast<size_t>(size);
    }

    std::vector<ErasureShard> RsErasureCodec::encode(std::span<const uint8_t> value, ErasureLayout layout) {
        validateRsLayout(layout);

        const size_t payloadSize = shardPayloadSize(static_cast<uint64_t>(value.size()), layout.dataShards);
        std::vector<ErasureShard> shards;
        shards.reserve(layout.totalShards());

        std::vector<ErasureShard> data;
        data.reserve(layout.dataShards);
        for (uint16_t i = 0; i < layout.dataShards; ++i) {
            ErasureShard shard = makeRsShard(i, static_cast<uint64_t>(value.size()), payloadSize);
            const size_t off = static_cast<size_t>(i) * payloadSize;
            if (off < value.size()) {
                const size_t remaining = value.size() - off;
                const size_t take = remaining < payloadSize ? remaining : payloadSize;
                std::memcpy(shard.payload.data(), value.data() + off, take);
            }
            shard.crc32c = checksum(shard.payload);
            data.push_back(shard);
        }

        for (const auto& shard : data) { shards.push_back(shard); }

        for (uint16_t parityOrdinal = 0; parityOrdinal < layout.parityShards; ++parityOrdinal) {
            ErasureShard parity = makeRsShard(
                static_cast<uint16_t>(layout.dataShards + parityOrdinal),
                static_cast<uint64_t>(value.size()),
                payloadSize
            );
            for (uint16_t dataIndex = 0; dataIndex < layout.dataShards; ++dataIndex) {
                xorMulRowInto(
                    parity.payload.data(),
                    data[dataIndex].payload.data(),
                    gfMulRow(rsCoefficient(parityOrdinal, dataIndex, layout.dataShards)),
                    payloadSize
                );
            }
            parity.crc32c = checksum(parity.payload);
            shards.push_back(std::move(parity));
        }

        return shards;
    }

    std::vector<uint8_t> RsErasureCodec::decode(std::span<const ErasureShard> shards, ErasureLayout layout) {
        return reconstructRsValue(validateRsShards(shards, layout), layout);
    }

    ErasureShard RsErasureCodec::repairOne(uint16_t missingIndex, std::span<const ErasureShard> shards, ErasureLayout layout) {
        validateRsLayout(layout);
        const ShardSet set = validateRsShards(shards, layout);
        if (missingIndex >= layout.totalShards()) { throw std::invalid_argument("RsErasureCodec: missing index out of range"); }
        if (set.byIndex[missingIndex] != nullptr) { throw std::invalid_argument("RsErasureCodec: missing index is already present"); }

        if (missingIndex < layout.dataShards) { return reconstructRsDataShard(set, layout, missingIndex); }

        const uint16_t parityOrdinal = static_cast<uint16_t>(missingIndex - layout.dataShards);
        ErasureShard parity = makeRsShard(missingIndex, set.originalSize, set.payloadSize);
        for (uint16_t dataIndex = 0; dataIndex < layout.dataShards; ++dataIndex) {
            const auto* source = set.byIndex[dataIndex];
            ErasureShard reconstructed;
            if (source == nullptr) {
                reconstructed = reconstructRsDataShard(set, layout, dataIndex);
                source = &reconstructed;
            }
            xorMulRowInto(
                parity.payload.data(),
                source->payload.data(),
                gfMulRow(rsCoefficient(parityOrdinal, dataIndex, layout.dataShards)),
                set.payloadSize
            );
        }
        parity.crc32c = checksum(parity.payload);
        return parity;
    }
} // namespace akkaradb::engine::erasure

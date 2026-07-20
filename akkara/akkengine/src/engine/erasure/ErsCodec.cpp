/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/erasure/ErsCodec.cpp
#include "akk/engine/erasure/detail/ErasureCodecInternal.hpp"

namespace akkaradb::engine::erasure {
    using namespace detail;

    size_t ErsCodec::shardPayloadSize(uint64_t originalSize, uint16_t dataShards) {
        if (dataShards == 0) { throw std::invalid_argument("ErsCodec: dataShards must be > 0"); }
        const uint64_t size = (originalSize / dataShards) + ((originalSize % dataShards) != 0 ? 1u : 0u);
        if (size > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
            throw std::runtime_error("ErsCodec: shard payload is too large for this platform");
        }
        return static_cast<size_t>(size);
    }

    std::vector<ErasureShard> ErsCodec::encode(std::span<const uint8_t> value, ErasureLayout layout) {
        validateErsLayout(layout);

        const size_t payloadSize = shardPayloadSize(static_cast<uint64_t>(value.size()), layout.dataShards);
        std::vector<ErasureShard> shards;
        shards.reserve(layout.totalShards());

        std::vector<ErasureShard> data;
        data.reserve(layout.dataShards);
        for (uint16_t i = 0; i < layout.dataShards; ++i) {
            ErasureShard shard = makeErsShard(i, static_cast<uint64_t>(value.size()), payloadSize);
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
            ErasureShard parity = makeErsShard(
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

    ErsRecoveryResult ErsCodec::recover(
        std::span<const ErasureShard> shards,
        ErasureLayout layout,
        std::span<const uint16_t> knownBadIndices
    ) {
        validateErsLayout(layout);
        const ErsShardSet input = validateErsShards(shards, layout);

        std::vector<uint8_t> forcedBad(static_cast<size_t>(layout.totalShards()), 0);
        for (const uint16_t index : input.missingIndices) { forcedBad[index] = 1; }
        for (const uint16_t index : input.crcMismatchIndices) { forcedBad[index] = 1; }
        for (const uint16_t index : knownBadIndices) {
            if (index >= layout.totalShards()) { throw std::invalid_argument("ErsCodec: known bad index out of range"); }
            forcedBad[index] = 1;
        }

        size_t forcedBadCount = 0;
        for (const uint8_t flag : forcedBad) { forcedBadCount += flag != 0 ? 1u : 0u; }
        if (forcedBadCount > layout.parityShards) {
            throw std::runtime_error("ErsCodec: too many known bad shards for the available parity");
        }

        const size_t maxUnknownErrors = (layout.parityShards - forcedBadCount) / 2u;
        const size_t searchUnknownErrors = maxUnknownErrors < 2u ? maxUnknownErrors : 2u;

        std::vector<uint16_t> candidatePool;
        candidatePool.reserve(layout.totalShards());
        for (uint16_t shardIndex = 0; shardIndex < layout.totalShards(); ++shardIndex) {
            if (input.shards.byIndex[shardIndex] != nullptr && forcedBad[shardIndex] == 0) { candidatePool.push_back(shardIndex); }
        }

        ErsRecoveryResult result;
        const auto tryCandidate = [&](std::span<const uint16_t> extraBadIndices) -> bool {
            std::vector<uint8_t> excluded = forcedBad;
            for (const uint16_t index : extraBadIndices) { excluded[index] = 1; }

            size_t availableCount = 0;
            for (uint16_t shardIndex = 0; shardIndex < layout.totalShards(); ++shardIndex) {
                if (excluded[shardIndex] == 0 && input.shards.byIndex[shardIndex] != nullptr) { ++availableCount; }
            }
            if (availableCount < layout.dataShards) { return false; }

            ShardSet candidate = input.shards;
            for (uint16_t shardIndex = 0; shardIndex < layout.totalShards(); ++shardIndex) {
                if (excluded[shardIndex] != 0) { candidate.byIndex[shardIndex] = nullptr; }
            }

            std::vector<ErasureShard> data;
            try { data = convertRsDataToErs(reconstructRsData(candidate, layout)); }
            catch (const std::runtime_error&) { return false; }

            const std::vector<uint8_t> value = joinDataShards(data, input.shards.originalSize);
            const auto expected = encode(value, layout);

            for (uint16_t shardIndex = 0; shardIndex < layout.totalShards(); ++shardIndex) {
                const auto* actual = input.shards.byIndex[shardIndex];
                if (actual == nullptr || excluded[shardIndex] != 0) { continue; }
                if (!shardMatches(*actual, expected[shardIndex])) { return false; }
            }

            result.value = value;
            result.missingIndices = input.missingIndices;
            result.detectedErrorIndices.clear();
            result.repairedShards.clear();
            result.repairedShards.reserve(static_cast<size_t>(layout.totalShards()));

            for (uint16_t shardIndex = 0; shardIndex < layout.totalShards(); ++shardIndex) {
                if (excluded[shardIndex] == 0) { continue; }
                result.repairedShards.push_back(expected[shardIndex]);
                if (input.shards.byIndex[shardIndex] != nullptr) { result.detectedErrorIndices.push_back(shardIndex); }
            }
            return true;
        };

        for (size_t extraErrors = 0; extraErrors <= searchUnknownErrors; ++extraErrors) {
            if (forEachCombination(candidatePool, extraErrors, [&](const std::vector<uint16_t>& picked) { return tryCandidate(picked); })) {
                return result;
            }
        }

        if (maxUnknownErrors > searchUnknownErrors) {
            throw std::runtime_error("ErsCodec: recovery would require searching more than two unknown error shards");
        }
        throw std::runtime_error("ErsCodec: failed to identify a recoverable shard error/erasure set");
    }

    std::vector<uint8_t> ErsCodec::decode(
        std::span<const ErasureShard> shards,
        ErasureLayout layout,
        std::span<const uint16_t> knownBadIndices
    ) { return recover(shards, layout, knownBadIndices).value; }


} // namespace akkaradb::engine::erasure

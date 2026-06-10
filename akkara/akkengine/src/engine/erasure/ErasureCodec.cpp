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

// akkengine/src/engine/erasure/ErasureCodec.cpp
#include "akk/engine/erasure/ErasureCodec.hpp"

#include "akk/cpu/CRC32C.hpp"

#include <cstring>
#include <limits>
#include <stdexcept>
#include <utility>

namespace akkaradb::engine::erasure {
    namespace {
        constexpr uint16_t XOR_PARITY_SHARDS = 1;

        void validateXorLayout(const ErasureLayout& layout) {
            if (layout.dataShards == 0) { throw std::invalid_argument("XorErasureCodec: dataShards must be > 0"); }
            if (layout.parityShards != XOR_PARITY_SHARDS) {
                throw std::invalid_argument("XorErasureCodec: XOR requires exactly one parity shard");
            }
            if (layout.dataShards == std::numeric_limits<uint16_t>::max()) {
                throw std::invalid_argument("XorErasureCodec: too many shards");
            }
        }

        uint32_t checksum(std::span<const uint8_t> payload) noexcept {
            return cpu::CRC32C(reinterpret_cast<const std::byte*>(payload.data()), payload.size());
        }

        void xorInto(uint8_t* dst, const uint8_t* src, size_t size) noexcept {
            size_t i = 0;
            constexpr size_t WORD = sizeof(uint64_t);
            for (; i + (WORD * 4) <= size; i += WORD * 4) {
                uint64_t a0;
                uint64_t a1;
                uint64_t a2;
                uint64_t a3;
                uint64_t b0;
                uint64_t b1;
                uint64_t b2;
                uint64_t b3;
                std::memcpy(&a0, dst + i, WORD);
                std::memcpy(&a1, dst + i + WORD, WORD);
                std::memcpy(&a2, dst + i + WORD * 2, WORD);
                std::memcpy(&a3, dst + i + WORD * 3, WORD);
                std::memcpy(&b0, src + i, WORD);
                std::memcpy(&b1, src + i + WORD, WORD);
                std::memcpy(&b2, src + i + WORD * 2, WORD);
                std::memcpy(&b3, src + i + WORD * 3, WORD);
                a0 ^= b0;
                a1 ^= b1;
                a2 ^= b2;
                a3 ^= b3;
                std::memcpy(dst + i, &a0, WORD);
                std::memcpy(dst + i + WORD, &a1, WORD);
                std::memcpy(dst + i + WORD * 2, &a2, WORD);
                std::memcpy(dst + i + WORD * 3, &a3, WORD);
            }
            for (; i + WORD <= size; i += WORD) {
                uint64_t a;
                uint64_t b;
                std::memcpy(&a, dst + i, WORD);
                std::memcpy(&b, src + i, WORD);
                a ^= b;
                std::memcpy(dst + i, &a, WORD);
            }
            for (; i < size; ++i) { dst[i] ^= src[i]; }
        }

        struct ShardSet {
            std::vector<const ErasureShard*> byIndex;
            uint64_t originalSize = 0;
            size_t payloadSize = 0;
        };

        ShardSet validateShards(std::span<const ErasureShard> shards, const ErasureLayout& layout) {
            validateXorLayout(layout);
            if (shards.empty()) { throw std::invalid_argument("XorErasureCodec: no shards supplied"); }

            ShardSet set;
            set.byIndex.resize(layout.totalShards(), nullptr);
            bool haveMeta = false;

            for (const auto& shard : shards) {
                if (shard.codec != ErasureCodecKind::XOR) { throw std::invalid_argument("XorErasureCodec: shard codec mismatch"); }
                if (shard.index >= set.byIndex.size()) { throw std::invalid_argument("XorErasureCodec: shard index out of range"); }
                if (set.byIndex[shard.index] != nullptr) { throw std::invalid_argument("XorErasureCodec: duplicate shard index"); }
                if (!shard.verifyCrc()) { throw std::runtime_error("XorErasureCodec: shard CRC mismatch"); }

                if (!haveMeta) {
                    set.originalSize = shard.originalSize;
                    set.payloadSize = shard.payload.size();
                    haveMeta = true;
                }
                else {
                    if (set.originalSize != shard.originalSize) { throw std::invalid_argument("XorErasureCodec: originalSize mismatch"); }
                    if (set.payloadSize != shard.payload.size()) { throw std::invalid_argument("XorErasureCodec: payload size mismatch"); }
                }
                set.byIndex[shard.index] = &shard;
            }

            const size_t expectedPayloadSize = XorErasureCodec::shardPayloadSize(set.originalSize, layout.dataShards);
            if (set.payloadSize != expectedPayloadSize) {
                throw std::invalid_argument("XorErasureCodec: payload size does not match layout");
            }
            return set;
        }

        std::vector<uint8_t> joinDataShards(const std::vector<ErasureShard>& dataShards, uint64_t originalSize) {
            if (originalSize > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
                throw std::runtime_error("XorErasureCodec: original value is too large for this platform");
            }

            std::vector<uint8_t> out;
            out.reserve(static_cast<size_t>(originalSize));
            for (const auto& shard : dataShards) {
                const size_t remaining = static_cast<size_t>(originalSize) - out.size();
                const size_t take = remaining < shard.payload.size() ? remaining : shard.payload.size();
                out.insert(out.end(), shard.payload.begin(), shard.payload.begin() + static_cast<std::ptrdiff_t>(take));
                if (out.size() == static_cast<size_t>(originalSize)) { break; }
            }
            return out;
        }
    } // namespace

    uint16_t ErasureLayout::totalShards() const noexcept { return static_cast<uint16_t>(dataShards + parityShards); }

    bool ErasureShard::verifyCrc() const noexcept { return crc32c == checksum(payload); }

    size_t XorErasureCodec::shardPayloadSize(uint64_t originalSize, uint16_t dataShards) {
        if (dataShards == 0) { throw std::invalid_argument("XorErasureCodec: dataShards must be > 0"); }
        const uint64_t size = (originalSize + dataShards - 1) / dataShards;
        if (size > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
            throw std::runtime_error("XorErasureCodec: shard payload is too large for this platform");
        }
        return static_cast<size_t>(size);
    }

    std::vector<ErasureShard> XorErasureCodec::encode(std::span<const uint8_t> value, ErasureLayout layout) {
        validateXorLayout(layout);

        const size_t payloadSize = shardPayloadSize(static_cast<uint64_t>(value.size()), layout.dataShards);
        std::vector<ErasureShard> shards;
        shards.reserve(layout.totalShards());

        ErasureShard parity{
            .index = layout.dataShards,
            .originalSize = static_cast<uint64_t>(value.size()),
            .codec = ErasureCodecKind::XOR,
            .payload = std::vector<uint8_t>(payloadSize, 0),
            .crc32c = 0,
        };

        for (uint16_t i = 0; i < layout.dataShards; ++i) {
            ErasureShard shard{
                .index = i,
                .originalSize = static_cast<uint64_t>(value.size()),
                .codec = ErasureCodecKind::XOR,
                .payload = std::vector<uint8_t>(payloadSize, 0),
                .crc32c = 0,
            };

            const size_t off = static_cast<size_t>(i) * payloadSize;
            if (off < value.size()) {
                const size_t remaining = value.size() - off;
                const size_t take = remaining < payloadSize ? remaining : payloadSize;
                std::memcpy(shard.payload.data(), value.data() + off, take);
            }
            if (payloadSize != 0) { xorInto(parity.payload.data(), shard.payload.data(), payloadSize); }
            shard.crc32c = checksum(shard.payload);
            shards.push_back(std::move(shard));
        }

        parity.crc32c = checksum(parity.payload);
        shards.push_back(std::move(parity));
        return shards;
    }

    ErasureShard XorErasureCodec::repairOne(uint16_t missingIndex, std::span<const ErasureShard> shards, ErasureLayout layout) {
        const ShardSet set = validateShards(shards, layout);
        if (missingIndex >= set.byIndex.size()) { throw std::invalid_argument("XorErasureCodec: missing index out of range"); }
        if (set.byIndex[missingIndex] != nullptr) { throw std::invalid_argument("XorErasureCodec: missing index is already present"); }

        ErasureShard repaired{
            .index = missingIndex,
            .originalSize = set.originalSize,
            .codec = ErasureCodecKind::XOR,
            .payload = std::vector<uint8_t>(set.payloadSize, 0),
            .crc32c = 0,
        };

        const uint16_t parityIndex = layout.dataShards;
        if (missingIndex == parityIndex) {
            for (uint16_t i = 0; i < layout.dataShards; ++i) {
                const auto* shard = set.byIndex[i];
                if (shard == nullptr) { throw std::runtime_error("XorErasureCodec: cannot repair parity without all data shards"); }
                if (set.payloadSize != 0) { xorInto(repaired.payload.data(), shard->payload.data(), set.payloadSize); }
            }
        }
        else {
            const auto* parity = set.byIndex[parityIndex];
            if (parity == nullptr) { throw std::runtime_error("XorErasureCodec: cannot repair data shard without parity"); }
            if (set.payloadSize != 0) { std::memcpy(repaired.payload.data(), parity->payload.data(), set.payloadSize); }
            for (uint16_t i = 0; i < layout.dataShards; ++i) {
                if (i == missingIndex) { continue; }
                const auto* shard = set.byIndex[i];
                if (shard == nullptr) { throw std::runtime_error("XorErasureCodec: more than one data shard is missing"); }
                if (set.payloadSize != 0) { xorInto(repaired.payload.data(), shard->payload.data(), set.payloadSize); }
            }
        }

        repaired.crc32c = checksum(repaired.payload);
        return repaired;
    }

    std::vector<uint8_t> XorErasureCodec::decode(std::span<const ErasureShard> shards, ErasureLayout layout) {
        const ShardSet set = validateShards(shards, layout);

        std::vector<ErasureShard> data;
        data.reserve(layout.dataShards);
        uint16_t missingData = std::numeric_limits<uint16_t>::max();
        uint16_t missingCount = 0;

        for (uint16_t i = 0; i < layout.dataShards; ++i) {
            if (set.byIndex[i] == nullptr) {
                missingData = i;
                ++missingCount;
            }
        }
        if (missingCount > 1) { throw std::runtime_error("XorErasureCodec: more than one data shard is missing"); }

        for (uint16_t i = 0; i < layout.dataShards; ++i) {
            if (set.byIndex[i] != nullptr) { data.push_back(*set.byIndex[i]); }
            else { data.push_back(repairOne(missingData, shards, layout)); }
        }
        return joinDataShards(data, set.originalSize);
    }
} // namespace akkaradb::engine::erasure

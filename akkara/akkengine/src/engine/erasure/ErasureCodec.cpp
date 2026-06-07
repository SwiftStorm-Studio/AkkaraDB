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

        void validate_xor_layout(const ErasureLayout& layout) {
            if (layout.data_shards == 0) { throw std::invalid_argument("XorErasureCodec: data_shards must be > 0"); }
            if (layout.parity_shards != XOR_PARITY_SHARDS) { throw std::invalid_argument("XorErasureCodec: XOR requires exactly one parity shard"); }
            if (layout.data_shards == std::numeric_limits<uint16_t>::max()) { throw std::invalid_argument("XorErasureCodec: too many shards"); }
        }

        uint32_t checksum(std::span<const uint8_t> payload) noexcept {
            return cpu::CRC32C(reinterpret_cast<const std::byte*>(payload.data()), payload.size());
        }

        void xor_into(uint8_t* dst, const uint8_t* src, size_t size) noexcept {
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
            std::vector<const ErasureShard*> by_index;
            uint64_t original_size = 0;
            size_t payload_size = 0;
        };

        ShardSet validate_shards(std::span<const ErasureShard> shards, const ErasureLayout& layout) {
            validate_xor_layout(layout);
            if (shards.empty()) { throw std::invalid_argument("XorErasureCodec: no shards supplied"); }

            ShardSet set;
            set.by_index.resize(layout.total_shards(), nullptr);
            bool have_meta = false;

            for (const auto& shard : shards) {
                if (shard.codec != ErasureCodecKind::Xor) { throw std::invalid_argument("XorErasureCodec: shard codec mismatch"); }
                if (shard.index >= set.by_index.size()) { throw std::invalid_argument("XorErasureCodec: shard index out of range"); }
                if (set.by_index[shard.index] != nullptr) { throw std::invalid_argument("XorErasureCodec: duplicate shard index"); }
                if (!shard.verify_crc()) { throw std::runtime_error("XorErasureCodec: shard CRC mismatch"); }

                if (!have_meta) {
                    set.original_size = shard.original_size;
                    set.payload_size = shard.payload.size();
                    have_meta = true;
                }
                else {
                    if (set.original_size != shard.original_size) { throw std::invalid_argument("XorErasureCodec: original_size mismatch"); }
                    if (set.payload_size != shard.payload.size()) { throw std::invalid_argument("XorErasureCodec: payload size mismatch"); }
                }
                set.by_index[shard.index] = &shard;
            }

            const size_t expected_payload_size = XorErasureCodec::shard_payload_size(set.original_size, layout.data_shards);
            if (set.payload_size != expected_payload_size) { throw std::invalid_argument("XorErasureCodec: payload size does not match layout"); }
            return set;
        }

        std::vector<uint8_t> join_data_shards(const std::vector<ErasureShard>& data_shards, uint64_t original_size) {
            if (original_size > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
                throw std::runtime_error("XorErasureCodec: original value is too large for this platform");
            }

            std::vector<uint8_t> out;
            out.reserve(static_cast<size_t>(original_size));
            for (const auto& shard : data_shards) {
                const size_t remaining = static_cast<size_t>(original_size) - out.size();
                const size_t take = remaining < shard.payload.size() ? remaining : shard.payload.size();
                out.insert(out.end(), shard.payload.begin(), shard.payload.begin() + static_cast<std::ptrdiff_t>(take));
                if (out.size() == static_cast<size_t>(original_size)) { break; }
            }
            return out;
        }
    } // namespace

    uint16_t ErasureLayout::total_shards() const noexcept { return static_cast<uint16_t>(data_shards + parity_shards); }

    bool ErasureShard::verify_crc() const noexcept { return crc32c == checksum(payload); }

    size_t XorErasureCodec::shard_payload_size(uint64_t original_size, uint16_t data_shards) {
        if (data_shards == 0) { throw std::invalid_argument("XorErasureCodec: data_shards must be > 0"); }
        const uint64_t size = (original_size + data_shards - 1) / data_shards;
        if (size > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
            throw std::runtime_error("XorErasureCodec: shard payload is too large for this platform");
        }
        return static_cast<size_t>(size);
    }

    std::vector<ErasureShard> XorErasureCodec::encode(std::span<const uint8_t> value, ErasureLayout layout) {
        validate_xor_layout(layout);

        const size_t payload_size = shard_payload_size(static_cast<uint64_t>(value.size()), layout.data_shards);
        std::vector<ErasureShard> shards;
        shards.reserve(layout.total_shards());

        ErasureShard parity{
            .index = layout.data_shards,
            .original_size = static_cast<uint64_t>(value.size()),
            .codec = ErasureCodecKind::Xor,
            .payload = std::vector<uint8_t>(payload_size, 0),
            .crc32c = 0,
        };

        for (uint16_t i = 0; i < layout.data_shards; ++i) {
            ErasureShard shard{
                .index = i,
                .original_size = static_cast<uint64_t>(value.size()),
                .codec = ErasureCodecKind::Xor,
                .payload = std::vector<uint8_t>(payload_size, 0),
                .crc32c = 0,
            };

            const size_t off = static_cast<size_t>(i) * payload_size;
            if (off < value.size()) {
                const size_t remaining = value.size() - off;
                const size_t take = remaining < payload_size ? remaining : payload_size;
                std::memcpy(shard.payload.data(), value.data() + off, take);
            }
            if (payload_size != 0) { xor_into(parity.payload.data(), shard.payload.data(), payload_size); }
            shard.crc32c = checksum(shard.payload);
            shards.push_back(std::move(shard));
        }

        parity.crc32c = checksum(parity.payload);
        shards.push_back(std::move(parity));
        return shards;
    }

    ErasureShard XorErasureCodec::repair_one(uint16_t missing_index, std::span<const ErasureShard> shards, ErasureLayout layout) {
        const ShardSet set = validate_shards(shards, layout);
        if (missing_index >= set.by_index.size()) { throw std::invalid_argument("XorErasureCodec: missing index out of range"); }
        if (set.by_index[missing_index] != nullptr) { throw std::invalid_argument("XorErasureCodec: missing index is already present"); }

        ErasureShard repaired{
            .index = missing_index,
            .original_size = set.original_size,
            .codec = ErasureCodecKind::Xor,
            .payload = std::vector<uint8_t>(set.payload_size, 0),
            .crc32c = 0,
        };

        const uint16_t parity_index = layout.data_shards;
        if (missing_index == parity_index) {
            for (uint16_t i = 0; i < layout.data_shards; ++i) {
                const auto* shard = set.by_index[i];
                if (shard == nullptr) { throw std::runtime_error("XorErasureCodec: cannot repair parity without all data shards"); }
                if (set.payload_size != 0) { xor_into(repaired.payload.data(), shard->payload.data(), set.payload_size); }
            }
        }
        else {
            const auto* parity = set.by_index[parity_index];
            if (parity == nullptr) { throw std::runtime_error("XorErasureCodec: cannot repair data shard without parity"); }
            if (set.payload_size != 0) { std::memcpy(repaired.payload.data(), parity->payload.data(), set.payload_size); }
            for (uint16_t i = 0; i < layout.data_shards; ++i) {
                if (i == missing_index) { continue; }
                const auto* shard = set.by_index[i];
                if (shard == nullptr) { throw std::runtime_error("XorErasureCodec: more than one data shard is missing"); }
                if (set.payload_size != 0) { xor_into(repaired.payload.data(), shard->payload.data(), set.payload_size); }
            }
        }

        repaired.crc32c = checksum(repaired.payload);
        return repaired;
    }

    std::vector<uint8_t> XorErasureCodec::decode(std::span<const ErasureShard> shards, ErasureLayout layout) {
        const ShardSet set = validate_shards(shards, layout);

        std::vector<ErasureShard> data;
        data.reserve(layout.data_shards);
        uint16_t missing_data = std::numeric_limits<uint16_t>::max();
        uint16_t missing_count = 0;

        for (uint16_t i = 0; i < layout.data_shards; ++i) {
            if (set.by_index[i] == nullptr) {
                missing_data = i;
                ++missing_count;
            }
        }
        if (missing_count > 1) { throw std::runtime_error("XorErasureCodec: more than one data shard is missing"); }

        for (uint16_t i = 0; i < layout.data_shards; ++i) {
            if (set.by_index[i] != nullptr) {
                data.push_back(*set.by_index[i]);
            }
            else {
                data.push_back(repair_one(missing_data, shards, layout));
            }
        }
        return join_data_shards(data, set.original_size);
    }
} // namespace akkaradb::engine::erasure

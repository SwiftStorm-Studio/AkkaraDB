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
#include "akk/engine/erasure/ErasureCodecExt.hpp"

#include "akk/cpu/CRC32C.hpp"

#include <array>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <utility>

namespace akkaradb::engine::erasure {
    namespace {
        constexpr uint16_t XOR_PARITY_SHARDS = 1;
        constexpr uint16_t DUAL_XOR_PARITY_SHARDS = 2;
        constexpr uint8_t GF256_POLY = 0x1Du;

        void validateXorLayout(const ErasureLayout& layout) {
            if (layout.dataShards == 0) { throw std::invalid_argument("XorErasureCodec: dataShards must be > 0"); }
            if (layout.parityShards != XOR_PARITY_SHARDS) {
                throw std::invalid_argument("XorErasureCodec: XOR requires exactly one parity shard");
            }
            if (layout.dataShards == std::numeric_limits<uint16_t>::max()) {
                throw std::invalid_argument("XorErasureCodec: too many shards");
            }
        }

        void validateDualXorLayout(const ErasureLayout& layout) {
            if (layout.dataShards == 0) { throw std::invalid_argument("DualXorErasureCodec: dataShards must be > 0"); }
            if (layout.parityShards != DUAL_XOR_PARITY_SHARDS) {
                throw std::invalid_argument("DualXorErasureCodec: DualXOR requires exactly two parity shards");
            }
            if (layout.dataShards > 255) { throw std::invalid_argument("DualXorErasureCodec: dataShards must be <= 255"); }
            if (layout.totalShards() < layout.dataShards) { throw std::invalid_argument("DualXorErasureCodec: too many shards"); }
        }

        void validateRsLayout(const ErasureLayout& layout) {
            if (layout.dataShards == 0) { throw std::invalid_argument("RsErasureCodec: dataShards must be > 0"); }
            if (layout.parityShards == 0) { throw std::invalid_argument("RsErasureCodec: parityShards must be > 0"); }
            if (layout.totalShards() > 255) { throw std::invalid_argument("RsErasureCodec: totalShards must be <= 255"); }
            if (layout.totalShards() < layout.dataShards) { throw std::invalid_argument("RsErasureCodec: too many shards"); }
        }

        void validateErsLayout(const ErasureLayout& layout) {
            if (layout.dataShards == 0) { throw std::invalid_argument("ErsCodec: dataShards must be > 0"); }
            if (layout.parityShards == 0) { throw std::invalid_argument("ErsCodec: parityShards must be > 0"); }
            if (layout.totalShards() > 255) { throw std::invalid_argument("ErsCodec: totalShards must be <= 255"); }
            if (layout.totalShards() < layout.dataShards) { throw std::invalid_argument("ErsCodec: too many shards"); }
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

        [[nodiscard]] uint8_t gfMul(uint8_t a, uint8_t b) noexcept {
            uint8_t out = 0;
            while (b != 0) {
                if ((b & 1u) != 0) { out ^= a; }
                const bool carry = (a & 0x80u) != 0;
                a = static_cast<uint8_t>(a << 1u);
                if (carry) { a ^= GF256_POLY; }
                b = static_cast<uint8_t>(b >> 1u);
            }
            return out;
        }

        [[nodiscard]] uint8_t gfPow(uint8_t value, uint8_t exponent) noexcept {
            uint8_t out = 1;
            while (exponent != 0) {
                if ((exponent & 1u) != 0) { out = gfMul(out, value); }
                value = gfMul(value, value);
                exponent = static_cast<uint8_t>(exponent >> 1u);
            }
            return out;
        }

        [[nodiscard]] uint8_t gfInv(uint8_t value) {
            if (value == 0) { throw std::runtime_error("DualXorErasureCodec: coefficient is not invertible"); }
            return gfPow(value, 254);
        }

        struct Gf256Tables {
            std::array<std::array<uint8_t, 256>, 256> mul{};
            std::array<uint8_t, 256> inv{};
        };

        [[nodiscard]] const Gf256Tables& gf256Tables() {
            static const Gf256Tables tables = [] {
                Gf256Tables built;
                for (uint32_t a = 0; a < 256; ++a) {
                    for (uint32_t b = 0; b < 256; ++b) { built.mul[a][b] = gfMul(static_cast<uint8_t>(a), static_cast<uint8_t>(b)); }
                }
                built.inv[0] = 0;
                for (uint32_t value = 1; value < 256; ++value) { built.inv[value] = gfInv(static_cast<uint8_t>(value)); }
                return built;
            }();
            return tables;
        }

        [[nodiscard]] const uint8_t* gfMulRow(uint8_t coefficient) noexcept { return gf256Tables().mul[coefficient].data(); }

        [[nodiscard]] uint8_t gfMulFast(uint8_t coefficient, uint8_t value) noexcept { return gf256Tables().mul[coefficient][value]; }

        [[nodiscard]] uint8_t gfInvFast(uint8_t value) {
            if (value == 0) { throw std::runtime_error("DualXorErasureCodec: coefficient is not invertible"); }
            return gf256Tables().inv[value];
        }

        [[nodiscard]] uint8_t dualXorCoefficient(uint16_t dataIndex) { return static_cast<uint8_t>(dataIndex + 1u); }

        [[nodiscard]] uint8_t rsBaseCoefficient(uint16_t dataIndex) { return static_cast<uint8_t>(dataIndex + 1u); }

        [[nodiscard]] uint8_t rsCoefficient(uint16_t parityOrdinal, uint16_t dataIndex) noexcept {
            return gfPow(rsBaseCoefficient(dataIndex), static_cast<uint8_t>(parityOrdinal + 1u));
        }

        void xorMulRowInto(uint8_t* dst, const uint8_t* src, const uint8_t* mulRow, size_t size) noexcept {
            for (size_t i = 0; i < size; ++i) { dst[i] ^= mulRow[src[i]]; }
        }

        struct ShardSet {
            std::vector<const ErasureShard*> byIndex;
            uint64_t originalSize = 0;
            size_t payloadSize = 0;
        };

        struct ErsShardSet {
            ShardSet shards;
            std::vector<uint16_t> missingIndices;
            std::vector<uint16_t> crcMismatchIndices;
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

        ShardSet validateDualXorShards(std::span<const ErasureShard> shards, const ErasureLayout& layout) {
            validateDualXorLayout(layout);
            if (shards.empty()) { throw std::invalid_argument("DualXorErasureCodec: no shards supplied"); }

            ShardSet set;
            set.byIndex.resize(layout.totalShards(), nullptr);
            bool haveMeta = false;

            for (const auto& shard : shards) {
                if (shard.codec != ErasureCodecKind::DUAL_XOR) { throw std::invalid_argument("DualXorErasureCodec: shard codec mismatch"); }
                if (shard.index >= set.byIndex.size()) { throw std::invalid_argument("DualXorErasureCodec: shard index out of range"); }
                if (set.byIndex[shard.index] != nullptr) { throw std::invalid_argument("DualXorErasureCodec: duplicate shard index"); }
                if (!shard.verifyCrc()) { throw std::runtime_error("DualXorErasureCodec: shard CRC mismatch"); }

                if (!haveMeta) {
                    set.originalSize = shard.originalSize;
                    set.payloadSize = shard.payload.size();
                    haveMeta = true;
                }
                else {
                    if (set.originalSize != shard.originalSize) {
                        throw std::invalid_argument("DualXorErasureCodec: originalSize mismatch");
                    }
                    if (set.payloadSize != shard.payload.size()) {
                        throw std::invalid_argument("DualXorErasureCodec: payload size mismatch");
                    }
                }
                set.byIndex[shard.index] = &shard;
            }

            const size_t expectedPayloadSize = DualXorErasureCodec::shardPayloadSize(set.originalSize, layout.dataShards);
            if (set.payloadSize != expectedPayloadSize) {
                throw std::invalid_argument("DualXorErasureCodec: payload size does not match layout");
            }
            return set;
        }

        [[nodiscard]] ErasureShard makeDualXorShard(uint16_t index, uint64_t originalSize, size_t payloadSize) {
            return ErasureShard{
                .index = index,
                .originalSize = originalSize,
                .codec = ErasureCodecKind::DUAL_XOR,
                .payload = std::vector<uint8_t>(payloadSize, 0),
                .crc32c = 0,
            };
        }

        [[nodiscard]] ErasureShard makeRsShard(uint16_t index, uint64_t originalSize, size_t payloadSize) {
            return ErasureShard{
                .index = index,
                .originalSize = originalSize,
                .codec = ErasureCodecKind::RS,
                .payload = std::vector<uint8_t>(payloadSize, 0),
                .crc32c = 0,
            };
        }

        [[nodiscard]] ErasureShard makeErsShard(uint16_t index, uint64_t originalSize, size_t payloadSize) {
            return ErasureShard{
                .index = index,
                .originalSize = originalSize,
                .codec = ErasureCodecKind::ERS,
                .payload = std::vector<uint8_t>(payloadSize, 0),
                .crc32c = 0,
            };
        }

        ShardSet validateRsShards(std::span<const ErasureShard> shards, const ErasureLayout& layout) {
            validateRsLayout(layout);
            if (shards.empty()) { throw std::invalid_argument("RsErasureCodec: no shards supplied"); }

            ShardSet set;
            set.byIndex.resize(layout.totalShards(), nullptr);
            bool haveMeta = false;

            for (const auto& shard : shards) {
                if (shard.codec != ErasureCodecKind::RS) { throw std::invalid_argument("RsErasureCodec: shard codec mismatch"); }
                if (shard.index >= set.byIndex.size()) { throw std::invalid_argument("RsErasureCodec: shard index out of range"); }
                if (set.byIndex[shard.index] != nullptr) { throw std::invalid_argument("RsErasureCodec: duplicate shard index"); }
                if (!shard.verifyCrc()) { throw std::runtime_error("RsErasureCodec: shard CRC mismatch"); }

                if (!haveMeta) {
                    set.originalSize = shard.originalSize;
                    set.payloadSize = shard.payload.size();
                    haveMeta = true;
                }
                else {
                    if (set.originalSize != shard.originalSize) { throw std::invalid_argument("RsErasureCodec: originalSize mismatch"); }
                    if (set.payloadSize != shard.payload.size()) { throw std::invalid_argument("RsErasureCodec: payload size mismatch"); }
                }
                set.byIndex[shard.index] = &shard;
            }

            const size_t expectedPayloadSize = RsErasureCodec::shardPayloadSize(set.originalSize, layout.dataShards);
            if (set.payloadSize != expectedPayloadSize) {
                throw std::invalid_argument("RsErasureCodec: payload size does not match layout");
            }
            return set;
        }

        ErsShardSet validateErsShards(std::span<const ErasureShard> shards, const ErasureLayout& layout) {
            validateErsLayout(layout);
            if (shards.empty()) { throw std::invalid_argument("ErsCodec: no shards supplied"); }

            ErsShardSet out;
            out.shards.byIndex.resize(layout.totalShards(), nullptr);
            bool haveMeta = false;

            for (const auto& shard : shards) {
                if (shard.codec != ErasureCodecKind::ERS) { throw std::invalid_argument("ErsCodec: shard codec mismatch"); }
                if (shard.index >= out.shards.byIndex.size()) { throw std::invalid_argument("ErsCodec: shard index out of range"); }
                if (out.shards.byIndex[shard.index] != nullptr) { throw std::invalid_argument("ErsCodec: duplicate shard index"); }

                if (!haveMeta) {
                    out.shards.originalSize = shard.originalSize;
                    out.shards.payloadSize = shard.payload.size();
                    haveMeta = true;
                }
                else {
                    if (out.shards.originalSize != shard.originalSize) { throw std::invalid_argument("ErsCodec: originalSize mismatch"); }
                    if (out.shards.payloadSize != shard.payload.size()) { throw std::invalid_argument("ErsCodec: payload size mismatch"); }
                }

                if (!shard.verifyCrc()) { out.crcMismatchIndices.push_back(shard.index); }
                out.shards.byIndex[shard.index] = &shard;
            }

            const size_t expectedPayloadSize = ErsCodec::shardPayloadSize(out.shards.originalSize, layout.dataShards);
            if (out.shards.payloadSize != expectedPayloadSize) {
                throw std::invalid_argument("ErsCodec: payload size does not match layout");
            }

            for (uint16_t shardIndex = 0; shardIndex < layout.totalShards(); ++shardIndex) {
                if (out.shards.byIndex[shardIndex] == nullptr) { out.missingIndices.push_back(shardIndex); }
            }
            return out;
        }

        void fillRsGeneratorRow(uint8_t* row, uint16_t shardIndex, const ErasureLayout& layout) {
            std::memset(row, 0, layout.dataShards);
            if (shardIndex < layout.dataShards) {
                row[shardIndex] = 1;
                return;
            }

            const uint16_t parityOrdinal = static_cast<uint16_t>(shardIndex - layout.dataShards);
            for (uint16_t dataIndex = 0; dataIndex < layout.dataShards; ++dataIndex) {
                row[dataIndex] = rsCoefficient(parityOrdinal, dataIndex);
            }
        }

        [[nodiscard]] std::vector<uint8_t> invertGfMatrix(const std::vector<uint8_t>& matrix, uint16_t dimension) {
            std::vector<uint8_t> augmented(static_cast<size_t>(dimension) * static_cast<size_t>(dimension) * 2u, 0);
            auto at = [dimension, &augmented](uint16_t row, uint16_t col) -> uint8_t& {
                return augmented[static_cast<size_t>(row) * static_cast<size_t>(dimension * 2u) + col];
            };

            for (uint16_t row = 0; row < dimension; ++row) {
                for (uint16_t col = 0; col < dimension; ++col) { at(row, col) = matrix[static_cast<size_t>(row) * dimension + col]; }
                at(row, static_cast<uint16_t>(dimension + row)) = 1;
            }

            for (uint16_t pivot = 0; pivot < dimension; ++pivot) {
                uint16_t pivotRow = pivot;
                while (pivotRow < dimension && at(pivotRow, pivot) == 0) { ++pivotRow; }
                if (pivotRow == dimension) { throw std::runtime_error("RsErasureCodec: generator matrix is singular"); }
                if (pivotRow != pivot) {
                    for (uint16_t col = 0; col < dimension * 2u; ++col) { std::swap(at(pivot, col), at(pivotRow, col)); }
                }

                const uint8_t invPivot = gfInv(at(pivot, pivot));
                for (uint16_t col = 0; col < dimension * 2u; ++col) { at(pivot, col) = gfMul(at(pivot, col), invPivot); }

                for (uint16_t row = 0; row < dimension; ++row) {
                    if (row == pivot) { continue; }
                    const uint8_t factor = at(row, pivot);
                    if (factor == 0) { continue; }
                    for (uint16_t col = 0; col < dimension * 2u; ++col) { at(row, col) ^= gfMul(factor, at(pivot, col)); }
                }
            }

            std::vector<uint8_t> inverse(static_cast<size_t>(dimension) * dimension, 0);
            for (uint16_t row = 0; row < dimension; ++row) {
                for (uint16_t col = 0; col < dimension; ++col) {
                    inverse[static_cast<size_t>(row) * dimension + col] = at(row, static_cast<uint16_t>(dimension + col));
                }
            }
            return inverse;
        }

        [[nodiscard]] std::vector<ErasureShard> reconstructRsData(const ShardSet& set, const ErasureLayout& layout) {
            std::vector<uint16_t> available;
            available.reserve(layout.dataShards);
            for (uint16_t shardIndex = 0; shardIndex < layout.totalShards() && available.size() < layout.dataShards; ++shardIndex) {
                if (set.byIndex[shardIndex] != nullptr) { available.push_back(shardIndex); }
            }
            if (available.size() < layout.dataShards) {
                throw std::runtime_error("RsErasureCodec: insufficient shards to reconstruct data");
            }

            std::vector<uint8_t> matrix(static_cast<size_t>(layout.dataShards) * layout.dataShards, 0);
            for (uint16_t row = 0; row < layout.dataShards; ++row) {
                fillRsGeneratorRow(matrix.data() + static_cast<size_t>(row) * layout.dataShards, available[row], layout);
            }

            const auto inverse = invertGfMatrix(matrix, layout.dataShards);
            std::vector<ErasureShard> data;
            data.reserve(layout.dataShards);
            for (uint16_t dataIndex = 0; dataIndex < layout.dataShards; ++dataIndex) {
                data.push_back(makeRsShard(dataIndex, set.originalSize, set.payloadSize));
            }

            for (size_t byteIndex = 0; byteIndex < set.payloadSize; ++byteIndex) {
                for (uint16_t outRow = 0; outRow < layout.dataShards; ++outRow) {
                    uint8_t value = 0;
                    for (uint16_t inRow = 0; inRow < layout.dataShards; ++inRow) {
                        value ^= gfMul(
                            inverse[static_cast<size_t>(outRow) * layout.dataShards + inRow],
                            set.byIndex[available[inRow]]->payload[byteIndex]
                        );
                    }
                    data[outRow].payload[byteIndex] = value;
                }
            }

            for (auto& shard : data) { shard.crc32c = checksum(shard.payload); }
            return data;
        }

        [[nodiscard]] std::vector<ErasureShard> reconstructRsData(std::span<const ErasureShard> shards, const ErasureLayout& layout) {
            return reconstructRsData(validateRsShards(shards, layout), layout);
        }

        [[nodiscard]] std::vector<ErasureShard> convertRsDataToErs(std::vector<ErasureShard> data) {
            for (auto& shard : data) { shard.codec = ErasureCodecKind::ERS; }
            return data;
        }

        [[nodiscard]] bool shardMatches(const ErasureShard& lhs, const ErasureShard& rhs) noexcept {
            return lhs.index == rhs.index && lhs.originalSize == rhs.originalSize && lhs.payload == rhs.payload && lhs.crc32c == rhs.crc32c;
        }

        template <typename Fn>
        bool forEachCombination(const std::vector<uint16_t>& values, size_t choose, Fn&& fn) {
            std::vector<uint16_t> picked;
            picked.reserve(choose);

            const auto walk = [&](auto&& self, size_t offset) -> bool {
                if (picked.size() == choose) { return fn(picked); }

                const size_t remaining = choose - picked.size();
                for (size_t i = offset; i + remaining <= values.size(); ++i) {
                    picked.push_back(values[i]);
                    if (self(self, i + 1)) { return true; }
                    picked.pop_back();
                }
                return false;
            };

            if (choose > values.size()) { return false; }
            return walk(walk, 0);
        }
    } // namespace

    uint16_t ErasureLayout::totalShards() const noexcept { return static_cast<uint16_t>(dataShards + parityShards); }

    bool ErasureShard::verifyCrc() const noexcept { return crc32c == checksum(payload); }

    size_t ErsCodec::shardPayloadSize(uint64_t originalSize, uint16_t dataShards) {
        if (dataShards == 0) { throw std::invalid_argument("ErsCodec: dataShards must be > 0"); }
        const uint64_t size = (originalSize + dataShards - 1) / dataShards;
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
                    gfMulRow(rsCoefficient(parityOrdinal, dataIndex)),
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

    size_t DualXorErasureCodec::shardPayloadSize(uint64_t originalSize, uint16_t dataShards) {
        if (dataShards == 0) { throw std::invalid_argument("DualXorErasureCodec: dataShards must be > 0"); }
        const uint64_t size = (originalSize + dataShards - 1) / dataShards;
        if (size > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
            throw std::runtime_error("DualXorErasureCodec: shard payload is too large for this platform");
        }
        return static_cast<size_t>(size);
    }

    std::vector<ErasureShard> DualXorErasureCodec::encode(std::span<const uint8_t> value, ErasureLayout layout) {
        validateDualXorLayout(layout);

        const size_t payloadSize = shardPayloadSize(static_cast<uint64_t>(value.size()), layout.dataShards);
        std::vector<ErasureShard> shards;
        shards.reserve(layout.totalShards());

        ErasureShard p = makeDualXorShard(layout.dataShards, static_cast<uint64_t>(value.size()), payloadSize);
        ErasureShard q = makeDualXorShard(static_cast<uint16_t>(layout.dataShards + 1u), static_cast<uint64_t>(value.size()), payloadSize);

        for (uint16_t i = 0; i < layout.dataShards; ++i) {
            ErasureShard shard = makeDualXorShard(i, static_cast<uint64_t>(value.size()), payloadSize);
            const size_t off = static_cast<size_t>(i) * payloadSize;
            if (off < value.size()) {
                const size_t remaining = value.size() - off;
                const size_t take = remaining < payloadSize ? remaining : payloadSize;
                std::memcpy(shard.payload.data(), value.data() + off, take);
            }
            if (payloadSize != 0) {
                xorInto(p.payload.data(), shard.payload.data(), payloadSize);
                xorMulRowInto(q.payload.data(), shard.payload.data(), gfMulRow(dualXorCoefficient(i)), payloadSize);
            }
            shard.crc32c = checksum(shard.payload);
            shards.push_back(std::move(shard));
        }

        p.crc32c = checksum(p.payload);
        q.crc32c = checksum(q.payload);
        shards.push_back(std::move(p));
        shards.push_back(std::move(q));
        return shards;
    }

    ErasureShard DualXorErasureCodec::repairOne(uint16_t missingIndex, std::span<const ErasureShard> shards, ErasureLayout layout) {
        const ShardSet set = validateDualXorShards(shards, layout);
        if (missingIndex >= set.byIndex.size()) { throw std::invalid_argument("DualXorErasureCodec: missing index out of range"); }
        if (set.byIndex[missingIndex] != nullptr) { throw std::invalid_argument("DualXorErasureCodec: missing index is already present"); }

        uint16_t missingCount = 0;
        for (const auto* shard : set.byIndex) { if (shard == nullptr) { ++missingCount; } }
        if (missingCount != 1) { throw std::runtime_error("DualXorErasureCodec: repairOne requires exactly one missing shard"); }

        const uint16_t pIndex = layout.dataShards;
        const uint16_t qIndex = static_cast<uint16_t>(layout.dataShards + 1u);
        ErasureShard repaired = makeDualXorShard(missingIndex, set.originalSize, set.payloadSize);

        if (missingIndex == pIndex) {
            for (uint16_t i = 0; i < layout.dataShards; ++i) {
                const auto* shard = set.byIndex[i];
                if (shard == nullptr) { throw std::runtime_error("DualXorErasureCodec: cannot rebuild P parity without all data shards"); }
                if (set.payloadSize != 0) { xorInto(repaired.payload.data(), shard->payload.data(), set.payloadSize); }
            }
        }
        else if (missingIndex == qIndex) {
            for (uint16_t i = 0; i < layout.dataShards; ++i) {
                const auto* shard = set.byIndex[i];
                if (shard == nullptr) { throw std::runtime_error("DualXorErasureCodec: cannot rebuild Q parity without all data shards"); }
                xorMulRowInto(repaired.payload.data(), shard->payload.data(), gfMulRow(dualXorCoefficient(i)), set.payloadSize);
            }
        }
        else {
            const auto* p = set.byIndex[pIndex];
            const auto* q = set.byIndex[qIndex];
            if (p != nullptr) {
                if (set.payloadSize != 0) { std::memcpy(repaired.payload.data(), p->payload.data(), set.payloadSize); }
                for (uint16_t i = 0; i < layout.dataShards; ++i) {
                    if (i == missingIndex) { continue; }
                    const auto* shard = set.byIndex[i];
                    if (shard == nullptr) { throw std::runtime_error("DualXorErasureCodec: unexpected missing data shard"); }
                    if (set.payloadSize != 0) { xorInto(repaired.payload.data(), shard->payload.data(), set.payloadSize); }
                }
            }
            else if (q != nullptr) {
                const uint8_t invCoeff = gfInvFast(dualXorCoefficient(missingIndex));
                const auto* invCoeffRow = gfMulRow(invCoeff);
                for (size_t byteIndex = 0; byteIndex < set.payloadSize; ++byteIndex) {
                    uint8_t value = q->payload[byteIndex];
                    for (uint16_t i = 0; i < layout.dataShards; ++i) {
                        if (i == missingIndex) { continue; }
                        const auto* shard = set.byIndex[i];
                        if (shard == nullptr) { throw std::runtime_error("DualXorErasureCodec: unexpected missing data shard"); }
                        value ^= gfMulFast(dualXorCoefficient(i), shard->payload[byteIndex]);
                    }
                    repaired.payload[byteIndex] = invCoeffRow[value];
                }
            }
            else { throw std::runtime_error("DualXorErasureCodec: cannot repair data shard without parity"); }
        }

        repaired.crc32c = checksum(repaired.payload);
        return repaired;
    }

    std::vector<uint8_t> DualXorErasureCodec::decode(std::span<const ErasureShard> shards, ErasureLayout layout) {
        const ShardSet set = validateDualXorShards(shards, layout);
        const uint16_t pIndex = layout.dataShards;
        const uint16_t qIndex = static_cast<uint16_t>(layout.dataShards + 1u);

        std::vector<uint16_t> missingData;
        missingData.reserve(2);
        for (uint16_t i = 0; i < layout.dataShards; ++i) { if (set.byIndex[i] == nullptr) { missingData.push_back(i); } }
        if (missingData.size() > 2) { throw std::runtime_error("DualXorErasureCodec: more than two data shards are missing"); }

        std::vector<ErasureShard> data;
        data.reserve(layout.dataShards);
        if (missingData.empty()) {
            for (uint16_t i = 0; i < layout.dataShards; ++i) { data.push_back(*set.byIndex[i]); }
            return joinDataShards(data, set.originalSize);
        }

        if (missingData.size() == 1) {
            const uint16_t missingIndex = missingData.front();
            ErasureShard repaired = makeDualXorShard(missingIndex, set.originalSize, set.payloadSize);
            const auto* p = set.byIndex[pIndex];
            const auto* q = set.byIndex[qIndex];
            if (p != nullptr) {
                if (set.payloadSize != 0) { std::memcpy(repaired.payload.data(), p->payload.data(), set.payloadSize); }
                for (uint16_t i = 0; i < layout.dataShards; ++i) {
                    if (i == missingIndex) { continue; }
                    const auto* shard = set.byIndex[i];
                    if (shard == nullptr) { throw std::runtime_error("DualXorErasureCodec: unexpected missing data shard"); }
                    if (set.payloadSize != 0) { xorInto(repaired.payload.data(), shard->payload.data(), set.payloadSize); }
                }
            }
            else if (q != nullptr) {
                const auto* invCoeffRow = gfMulRow(gfInvFast(dualXorCoefficient(missingIndex)));
                for (size_t byteIndex = 0; byteIndex < set.payloadSize; ++byteIndex) {
                    uint8_t value = q->payload[byteIndex];
                    for (uint16_t i = 0; i < layout.dataShards; ++i) {
                        if (i == missingIndex) { continue; }
                        const auto* shard = set.byIndex[i];
                        if (shard == nullptr) { throw std::runtime_error("DualXorErasureCodec: unexpected missing data shard"); }
                        value ^= gfMulFast(dualXorCoefficient(i), shard->payload[byteIndex]);
                    }
                    repaired.payload[byteIndex] = invCoeffRow[value];
                }
            }
            else { throw std::runtime_error("DualXorErasureCodec: cannot repair data shard without parity"); }
            repaired.crc32c = checksum(repaired.payload);
            for (uint16_t i = 0; i < layout.dataShards; ++i) {
                if (i == missingIndex) { data.push_back(repaired); }
                else { data.push_back(*set.byIndex[i]); }
            }
            return joinDataShards(data, set.originalSize);
        }

        const auto* p = set.byIndex[pIndex];
        const auto* q = set.byIndex[qIndex];
        if (p == nullptr || q == nullptr) {
            throw std::runtime_error("DualXorErasureCodec: two missing data shards require both parity shards");
        }

        const uint16_t firstMissing = missingData[0];
        const uint16_t secondMissing = missingData[1];
        ErasureShard first = makeDualXorShard(firstMissing, set.originalSize, set.payloadSize);
        ErasureShard second = makeDualXorShard(secondMissing, set.originalSize, set.payloadSize);

        const uint8_t coeffA = dualXorCoefficient(firstMissing);
        const uint8_t coeffB = dualXorCoefficient(secondMissing);
        const uint8_t invCoeffDelta = gfInvFast(static_cast<uint8_t>(coeffA ^ coeffB));
        const auto* invCoeffDeltaRow = gfMulRow(invCoeffDelta);
        const auto* coeffBRow = gfMulRow(coeffB);

        for (size_t byteIndex = 0; byteIndex < set.payloadSize; ++byteIndex) {
            uint8_t pResidual = p->payload[byteIndex];
            uint8_t qResidual = q->payload[byteIndex];
            for (uint16_t i = 0; i < layout.dataShards; ++i) {
                if (i == firstMissing || i == secondMissing) { continue; }
                const auto* shard = set.byIndex[i];
                if (shard == nullptr) { throw std::runtime_error("DualXorErasureCodec: unexpected missing data shard"); }
                pResidual ^= shard->payload[byteIndex];
                qResidual ^= gfMulFast(dualXorCoefficient(i), shard->payload[byteIndex]);
            }

            const uint8_t firstByte = invCoeffDeltaRow[static_cast<uint8_t>(qResidual ^ coeffBRow[pResidual])];
            const uint8_t secondByte = static_cast<uint8_t>(firstByte ^ pResidual);
            first.payload[byteIndex] = firstByte;
            second.payload[byteIndex] = secondByte;
        }

        first.crc32c = checksum(first.payload);
        second.crc32c = checksum(second.payload);

        for (uint16_t i = 0; i < layout.dataShards; ++i) {
            if (i == firstMissing) { data.push_back(first); }
            else if (i == secondMissing) { data.push_back(second); }
            else { data.push_back(*set.byIndex[i]); }
        }
        return joinDataShards(data, set.originalSize);
    }

    size_t RsErasureCodec::shardPayloadSize(uint64_t originalSize, uint16_t dataShards) {
        if (dataShards == 0) { throw std::invalid_argument("RsErasureCodec: dataShards must be > 0"); }
        const uint64_t size = (originalSize + dataShards - 1) / dataShards;
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
                    gfMulRow(rsCoefficient(parityOrdinal, dataIndex)),
                    payloadSize
                );
            }
            parity.crc32c = checksum(parity.payload);
            shards.push_back(std::move(parity));
        }

        return shards;
    }

    std::vector<uint8_t> RsErasureCodec::decode(std::span<const ErasureShard> shards, ErasureLayout layout) {
        const auto data = reconstructRsData(shards, layout);
        return joinDataShards(data, data.empty() ? 0 : data.front().originalSize);
    }

    ErasureShard RsErasureCodec::repairOne(uint16_t missingIndex, std::span<const ErasureShard> shards, ErasureLayout layout) {
        validateRsLayout(layout);
        const ShardSet set = validateRsShards(shards, layout);
        if (missingIndex >= layout.totalShards()) { throw std::invalid_argument("RsErasureCodec: missing index out of range"); }
        if (set.byIndex[missingIndex] != nullptr) { throw std::invalid_argument("RsErasureCodec: missing index is already present"); }

        const auto data = reconstructRsData(set, layout);
        if (missingIndex < layout.dataShards) { return data[missingIndex]; }

        const uint16_t parityOrdinal = static_cast<uint16_t>(missingIndex - layout.dataShards);
        ErasureShard parity = makeRsShard(missingIndex, set.originalSize, set.payloadSize);
        for (uint16_t dataIndex = 0; dataIndex < layout.dataShards; ++dataIndex) {
            xorMulRowInto(
                parity.payload.data(),
                data[dataIndex].payload.data(),
                gfMulRow(rsCoefficient(parityOrdinal, dataIndex)),
                set.payloadSize
            );
        }
        parity.crc32c = checksum(parity.payload);
        return parity;
    }
} // namespace akkaradb::engine::erasure

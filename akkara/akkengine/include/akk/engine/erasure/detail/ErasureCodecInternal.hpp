#pragma once
#include "akk/engine/erasure/ErasureCodec.hpp"
#include "akk/engine/erasure/ErasureCodecExt.hpp"
#include "akk/cpu/CRC32C.hpp"
#include <array>
#include <cstring>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
namespace akkaradb::engine::erasure {
    namespace detail {
        constexpr uint8_t GF256_POLY = 0x1Du;

        inline void validateRsLayout(const ErasureLayout& layout) {
            if (layout.dataShards == 0) { throw std::invalid_argument("RsErasureCodec: dataShards must be > 0"); }
            if (layout.parityShards == 0) { throw std::invalid_argument("RsErasureCodec: parityShards must be > 0"); }
            if (layout.totalShards() > 255) { throw std::invalid_argument("RsErasureCodec: totalShards must be <= 255"); }
            if (layout.totalShards() < layout.dataShards) { throw std::invalid_argument("RsErasureCodec: too many shards"); }
        }

        inline void validateErsLayout(const ErasureLayout& layout) {
            if (layout.dataShards == 0) { throw std::invalid_argument("ErsCodec: dataShards must be > 0"); }
            if (layout.parityShards == 0) { throw std::invalid_argument("ErsCodec: parityShards must be > 0"); }
            if (layout.totalShards() > 255) { throw std::invalid_argument("ErsCodec: totalShards must be <= 255"); }
            if (layout.totalShards() < layout.dataShards) { throw std::invalid_argument("ErsCodec: too many shards"); }
        }

        inline uint32_t checksum(std::span<const uint8_t> payload) noexcept {
            return cpu::CRC32C(reinterpret_cast<const std::byte*>(payload.data()), payload.size());
        }

        inline void xorInto(uint8_t* dst, const uint8_t* src, size_t size) noexcept {
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

        [[nodiscard]] inline uint8_t gfMul(uint8_t a, uint8_t b) noexcept {
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

        [[nodiscard]] inline uint8_t gfPow(uint8_t value, uint8_t exponent) noexcept {
            uint8_t out = 1;
            while (exponent != 0) {
                if ((exponent & 1u) != 0) { out = gfMul(out, value); }
                value = gfMul(value, value);
                exponent = static_cast<uint8_t>(exponent >> 1u);
            }
            return out;
        }

        [[nodiscard]] inline uint8_t gfInv(uint8_t value) {
            if (value == 0) { throw std::runtime_error("RsErasureCodec: coefficient is not invertible"); }
            return gfPow(value, 254);
        }

        struct Gf256Tables {
            std::array<std::array<uint8_t, 256>, 256> mul{};
            std::array<uint8_t, 256> inv{};
        };

        [[nodiscard]] inline const Gf256Tables& gf256Tables() {
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

        [[nodiscard]] inline const uint8_t* gfMulRow(uint8_t coefficient) noexcept { return gf256Tables().mul[coefficient].data(); }

        [[nodiscard]] inline uint8_t gfMulFast(uint8_t coefficient, uint8_t value) noexcept { return gf256Tables().mul[coefficient][value]; }

        [[nodiscard]] inline uint8_t gfInvFast(uint8_t value) {
            if (value == 0) { throw std::runtime_error("RsErasureCodec: coefficient is not invertible"); }
            return gf256Tables().inv[value];
        }

        [[nodiscard]] inline uint8_t rsCoefficient(uint16_t parityOrdinal, uint16_t dataIndex, uint16_t dataShards) {
            const auto parityPoint = static_cast<uint8_t>(dataShards + parityOrdinal);
            const auto dataPoint = static_cast<uint8_t>(dataIndex);
            return gfInvFast(static_cast<uint8_t>(parityPoint ^ dataPoint));
        }

        inline void xorMulRowInto(uint8_t* dst, const uint8_t* src, const uint8_t* mulRow, size_t size) noexcept {
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

        inline std::vector<uint8_t> joinDataShards(const std::vector<ErasureShard>& dataShards, uint64_t originalSize) {
            if (originalSize > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
                throw std::runtime_error("RsErasureCodec: original value is too large for this platform");
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

        [[nodiscard]] inline ErasureShard makeRsShard(uint16_t index, uint64_t originalSize, size_t payloadSize) {
            return ErasureShard{
                .index = index,
                .originalSize = originalSize,
                .codec = ErasureCodecKind::RS,
                .payload = std::vector<uint8_t>(payloadSize, 0),
                .crc32c = 0,
            };
        }

        [[nodiscard]] inline ErasureShard makeErsShard(uint16_t index, uint64_t originalSize, size_t payloadSize) {
            return ErasureShard{
                .index = index,
                .originalSize = originalSize,
                .codec = ErasureCodecKind::ERS,
                .payload = std::vector<uint8_t>(payloadSize, 0),
                .crc32c = 0,
            };
        }

        inline ShardSet validateRsShards(std::span<const ErasureShard> shards, const ErasureLayout& layout) {
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

        inline ErsShardSet validateErsShards(std::span<const ErasureShard> shards, const ErasureLayout& layout) {
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

        inline void fillRsGeneratorRow(uint8_t* row, uint16_t shardIndex, const ErasureLayout& layout) {
            std::memset(row, 0, layout.dataShards);
            if (shardIndex < layout.dataShards) {
                row[shardIndex] = 1;
                return;
            }

            const uint16_t parityOrdinal = static_cast<uint16_t>(shardIndex - layout.dataShards);
            for (uint16_t dataIndex = 0; dataIndex < layout.dataShards; ++dataIndex) {
                row[dataIndex] = rsCoefficient(parityOrdinal, dataIndex, layout.dataShards);
            }
        }

        [[nodiscard]] inline std::vector<uint16_t> selectAvailableRsRows(const ShardSet& set, const ErasureLayout& layout) {
            std::vector<uint16_t> available;
            available.reserve(layout.dataShards);
            for (uint16_t shardIndex = 0; shardIndex < layout.totalShards() && available.size() < layout.dataShards; ++shardIndex) {
                if (set.byIndex[shardIndex] != nullptr) { available.push_back(shardIndex); }
            }
            if (available.size() < layout.dataShards) {
                throw std::runtime_error("RsErasureCodec: insufficient shards to reconstruct data");
            }
            return available;
        }

        [[nodiscard]] inline std::vector<uint8_t> invertGfMatrix(const std::vector<uint8_t>& matrix, uint16_t dimension) {
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

        [[nodiscard]] inline std::string rsInverseCacheKey(const std::vector<uint16_t>& available, const ErasureLayout& layout) {
            std::string key;
            key.reserve(4 + available.size() * 2);
            const auto appendU16 = [&key](uint16_t value) {
                key.push_back(static_cast<char>(value & 0xFFu));
                key.push_back(static_cast<char>(value >> 8u));
            };
            appendU16(layout.dataShards);
            appendU16(layout.parityShards);
            for (const uint16_t shardIndex : available) { appendU16(shardIndex); }
            return key;
        }

        [[nodiscard]] inline std::vector<uint8_t> buildRsInverseMatrix(const std::vector<uint16_t>& available, const ErasureLayout& layout) {
            std::vector<uint8_t> matrix(static_cast<size_t>(layout.dataShards) * layout.dataShards, 0);
            for (uint16_t row = 0; row < layout.dataShards; ++row) {
                fillRsGeneratorRow(matrix.data() + static_cast<size_t>(row) * layout.dataShards, available[row], layout);
            }
            return invertGfMatrix(matrix, layout.dataShards);
        }

        [[nodiscard]] inline std::vector<uint8_t> cachedRsInverseMatrix(const std::vector<uint16_t>& available, const ErasureLayout& layout) {
            static std::mutex mutex;
            static std::unordered_map<std::string, std::vector<uint8_t>> cache;
            constexpr size_t MAX_CACHE_ENTRIES = 256;

            const std::string key = rsInverseCacheKey(available, layout);
            {
                std::lock_guard lock{mutex};
                const auto found = cache.find(key);
                if (found != cache.end()) { return found->second; }
            }

            auto inverse = buildRsInverseMatrix(available, layout);
            {
                std::lock_guard lock{mutex};
                if (cache.size() >= MAX_CACHE_ENTRIES) { cache.clear(); }
                cache.emplace(key, inverse);
            }
            return inverse;
        }

        [[nodiscard]] inline ErasureShard reconstructRsDataShard(
            const ShardSet& set,
            const ErasureLayout& layout,
            uint16_t dataIndex,
            const std::vector<uint16_t>& available,
            const std::vector<uint8_t>& inverse
        ) {
            ErasureShard shard = makeRsShard(dataIndex, set.originalSize, set.payloadSize);
            const auto* inverseRow = inverse.data() + static_cast<size_t>(dataIndex) * layout.dataShards;
            for (uint16_t inRow = 0; inRow < layout.dataShards; ++inRow) {
                const auto* source = set.byIndex[available[inRow]];
                if (source == nullptr) { throw std::runtime_error("RsErasureCodec: missing selected source shard"); }
                const uint8_t coefficient = inverseRow[inRow];
                if (coefficient == 0) { continue; }
                xorMulRowInto(shard.payload.data(), source->payload.data(), gfMulRow(coefficient), set.payloadSize);
            }
            shard.crc32c = checksum(shard.payload);
            return shard;
        }

        [[nodiscard]] inline ErasureShard reconstructRsDataShard(const ShardSet& set, const ErasureLayout& layout, uint16_t dataIndex) {
            const auto available = selectAvailableRsRows(set, layout);
            const auto inverse = cachedRsInverseMatrix(available, layout);
            return reconstructRsDataShard(set, layout, dataIndex, available, inverse);
        }

        inline void reconstructRsDataShardInto(
            uint8_t* dst,
            size_t size,
            const ShardSet& set,
            const ErasureLayout& layout,
            uint16_t dataIndex,
            const std::vector<uint16_t>& available,
            const std::vector<uint8_t>& inverse
        ) {
            if (size == 0) { return; }
            std::memset(dst, 0, size);
            const auto* inverseRow = inverse.data() + static_cast<size_t>(dataIndex) * layout.dataShards;
            for (uint16_t inRow = 0; inRow < layout.dataShards; ++inRow) {
                const auto* source = set.byIndex[available[inRow]];
                if (source == nullptr) { throw std::runtime_error("RsErasureCodec: missing selected source shard"); }
                const uint8_t coefficient = inverseRow[inRow];
                if (coefficient == 0) { continue; }
                xorMulRowInto(dst, source->payload.data(), gfMulRow(coefficient), size);
            }
        }

        [[nodiscard]] inline std::vector<ErasureShard> reconstructRsData(const ShardSet& set, const ErasureLayout& layout) {
            const auto available = selectAvailableRsRows(set, layout);
            const auto inverse = cachedRsInverseMatrix(available, layout);
            std::vector<ErasureShard> data;
            data.reserve(layout.dataShards);
            for (uint16_t dataIndex = 0; dataIndex < layout.dataShards; ++dataIndex) {
                data.push_back(reconstructRsDataShard(set, layout, dataIndex, available, inverse));
            }
            return data;
        }

        [[nodiscard]] inline std::vector<ErasureShard> reconstructRsData(std::span<const ErasureShard> shards, const ErasureLayout& layout) {
            return reconstructRsData(validateRsShards(shards, layout), layout);
        }

        [[nodiscard]] inline std::vector<uint8_t> reconstructRsValue(const ShardSet& set, const ErasureLayout& layout) {
            if (set.originalSize > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
                throw std::runtime_error("RsErasureCodec: original value is too large for this platform");
            }

            std::vector<uint8_t> out(static_cast<size_t>(set.originalSize));
            const auto available = selectAvailableRsRows(set, layout);
            if (out.empty()) { return out; }

            const auto inverse = cachedRsInverseMatrix(available, layout);
            std::vector<uint8_t> reconstructed;

            for (uint16_t dataIndex = 0; dataIndex < layout.dataShards; ++dataIndex) {
                const size_t offset = static_cast<size_t>(dataIndex) * set.payloadSize;
                if (offset >= out.size()) { break; }

                const size_t remaining = out.size() - offset;
                const size_t take = remaining < set.payloadSize ? remaining : set.payloadSize;
                if (const auto* shard = set.byIndex[dataIndex]) {
                    std::memcpy(out.data() + offset, shard->payload.data(), take);
                    continue;
                }

                reconstructed.resize(set.payloadSize);
                reconstructRsDataShardInto(reconstructed.data(), set.payloadSize, set, layout, dataIndex, available, inverse);
                std::memcpy(out.data() + offset, reconstructed.data(), take);
            }
            return out;
        }

        [[nodiscard]] inline std::vector<ErasureShard> convertRsDataToErs(std::vector<ErasureShard> data) {
            for (auto& shard : data) { shard.codec = ErasureCodecKind::ERS; }
            return data;
        }

        [[nodiscard]] inline bool shardMatches(const ErasureShard& lhs, const ErasureShard& rhs) noexcept {
            return lhs.index == rhs.index && lhs.originalSize == rhs.originalSize && lhs.payload == rhs.payload && lhs.crc32c == rhs.crc32c;
        }

        template <typename Fn>
        inline bool forEachCombination(const std::vector<uint16_t>& values, size_t choose, Fn&& fn) {
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

    } // namespace detail
} // namespace akkaradb::engine::erasure

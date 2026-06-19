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

// benchmarks/smoke/erasureSmokeTest.cpp
#include "TestErrorHandlers.hpp"

#include "akk/engine/erasure/ErasureCodec.hpp"
#include "akk/engine/erasure/ErasureCodecExt.hpp"
#include "akk/cpu/CRC32C.hpp"

#include <array>
#include <cstddef>
#include <cstdio>
#include <stdexcept>
#include <string>
#include <vector>

using namespace akkaradb::engine::erasure;

namespace {
    std::span<const uint8_t> bytesOf(const std::string& value) {
        return {reinterpret_cast<const uint8_t*>(value.data()), value.size()};
    }

    std::vector<uint8_t> makePayload(size_t size) {
        std::vector<uint8_t> out(size);
        uint32_t state = 0xA55A1234u;
        for (auto& byte : out) {
            state = state * 1664525u + 1013904223u;
            byte = static_cast<uint8_t>(state >> 24);
        }
        return out;
    }

    uint32_t crcOf(std::span<const uint8_t> payload) {
        return akkaradb::cpu::CRC32C(reinterpret_cast<const std::byte*>(payload.data()), payload.size());
    }

    std::vector<ErasureShard> withoutIndex(const std::vector<ErasureShard>& shards, uint16_t missingIndex) {
        std::vector<ErasureShard> out;
        for (const auto& shard : shards) {
            if (shard.index != missingIndex) { out.push_back(shard); }
        }
        return out;
    }

    std::vector<ErasureShard> withoutIndices(const std::vector<ErasureShard>& shards, std::span<const uint16_t> missingIndices) {
        std::vector<ErasureShard> out;
        for (const auto& shard : shards) {
            bool missing = false;
            for (const auto index : missingIndices) {
                if (shard.index == index) {
                    missing = true;
                    break;
                }
            }
            if (!missing) { out.push_back(shard); }
        }
        return out;
    }

    void testXorRoundtripUneven() {
        const ErasureLayout layout{.dataShards = 4, .parityShards = 1};
        const std::string value = "akkaradb xor erasure smoke payload";
        const auto shards = XorErasureCodec::encode(bytesOf(value), layout);
        AKK_TEST_CHECK(shards.size() == 5);
        AKK_TEST_CHECK(shards[0].payload.size() == XorErasureCodec::shardPayloadSize(value.size(), layout.dataShards));
        for (const auto& shard : shards) { AKK_TEST_CHECK(shard.verifyCrc()); }

        const auto decoded = XorErasureCodec::decode(shards, layout);
        AKK_TEST_CHECK(std::string(reinterpret_cast<const char*>(decoded.data()), decoded.size()) == value);
    }

    void testXorRepairEachShard() {
        const ErasureLayout layout{.dataShards = 5, .parityShards = 1};
        const auto value = makePayload(1024 * 1024 + 137);
        const auto shards = XorErasureCodec::encode(value, layout);

        for (uint16_t missing = 0; missing < layout.totalShards(); ++missing) {
            auto present = withoutIndex(shards, missing);
            const auto repaired = XorErasureCodec::repairOne(missing, present, layout);
            AKK_TEST_CHECK(repaired.index == missing);
            AKK_TEST_CHECK(repaired.verifyCrc());
            AKK_TEST_CHECK(repaired.payload == shards[missing].payload);

            present.push_back(repaired);
            const auto decoded = XorErasureCodec::decode(present, layout);
            AKK_TEST_CHECK(decoded == value);
        }
    }

    void testXorZeroLength() {
        const ErasureLayout layout{.dataShards = 3, .parityShards = 1};
        const std::vector<uint8_t> empty;
        const auto shards = XorErasureCodec::encode(empty, layout);
        AKK_TEST_CHECK(shards.size() == 4);
        for (const auto& shard : shards) {
            AKK_TEST_CHECK(shard.payload.empty());
            AKK_TEST_CHECK(shard.verifyCrc());
        }
        const auto decoded = XorErasureCodec::decode(shards, layout);
        AKK_TEST_CHECK(decoded.empty());
    }

    void testXorCrcRejectsCorruption() {
        const ErasureLayout layout{.dataShards = 3, .parityShards = 1};
        const auto shards = XorErasureCodec::encode(bytesOf("crc-check"), layout);

        auto corrupt = shards;
        corrupt[1].payload[0] ^= 0x80;
        bool rejected = false;
        try {
            (void)XorErasureCodec::decode(corrupt, layout);
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }
        AKK_TEST_CHECK(rejected);
    }

    void testXorRejectsInsufficientShards() {
        const ErasureLayout layout{.dataShards = 4, .parityShards = 1};
        const auto shards = XorErasureCodec::encode(bytesOf("too many missing shards"), layout);
        auto present = withoutIndex(shards, 0);
        present = withoutIndex(present, 1);

        bool rejected = false;
        try {
            (void)XorErasureCodec::decode(present, layout);
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }
        AKK_TEST_CHECK(rejected);
    }

    void testXorRejectsInvalidLayout() {
        bool rejected = false;
        try {
            (void)XorErasureCodec::encode(bytesOf("bad"), ErasureLayout{.dataShards = 2, .parityShards = 2});
        }
        catch (const std::invalid_argument&) {
            rejected = true;
        }
        AKK_TEST_CHECK(rejected);
    }

    void testDualXorRoundtripUneven() {
        const ErasureLayout layout{.dataShards = 6, .parityShards = 2};
        const std::string value = "akkaradb dual xor erasure smoke payload";
        const auto shards = DualXorErasureCodec::encode(bytesOf(value), layout);
        AKK_TEST_CHECK(shards.size() == 8);
        AKK_TEST_CHECK(shards[0].payload.size() == DualXorErasureCodec::shardPayloadSize(value.size(), layout.dataShards));
        for (const auto& shard : shards) { AKK_TEST_CHECK(shard.verifyCrc()); }

        const auto decoded = DualXorErasureCodec::decode(shards, layout);
        AKK_TEST_CHECK(std::string(reinterpret_cast<const char*>(decoded.data()), decoded.size()) == value);
    }

    void testDualXorRepairSingleMissingEachShard() {
        const ErasureLayout layout{.dataShards = 7, .parityShards = 2};
        const auto value = makePayload(512 * 1024 + 91);
        const auto shards = DualXorErasureCodec::encode(value, layout);

        for (uint16_t missing = 0; missing < layout.totalShards(); ++missing) {
            auto present = withoutIndex(shards, missing);
            const auto repaired = DualXorErasureCodec::repairOne(missing, present, layout);
            AKK_TEST_CHECK(repaired.index == missing);
            AKK_TEST_CHECK(repaired.verifyCrc());
            AKK_TEST_CHECK(repaired.payload == shards[missing].payload);

            present.push_back(repaired);
            const auto decoded = DualXorErasureCodec::decode(present, layout);
            AKK_TEST_CHECK(decoded == value);
        }
    }

    void testDualXorDecodeTwoMissingDataShards() {
        const ErasureLayout layout{.dataShards = 8, .parityShards = 2};
        const auto value = makePayload(1024 * 1024 + 211);
        const auto shards = DualXorErasureCodec::encode(value, layout);

        auto present = withoutIndex(shards, 2);
        present = withoutIndex(present, 5);
        const auto decoded = DualXorErasureCodec::decode(present, layout);
        AKK_TEST_CHECK(decoded == value);
    }

    void testDualXorDecodeAllMissingDataPairs() {
        const ErasureLayout layout{.dataShards = 6, .parityShards = 2};
        const auto value = makePayload(64 * 1024 + 19);
        const auto shards = DualXorErasureCodec::encode(value, layout);

        for (uint16_t first = 0; first < layout.dataShards; ++first) {
            for (uint16_t second = static_cast<uint16_t>(first + 1u); second < layout.dataShards; ++second) {
                auto present = withoutIndex(shards, first);
                present = withoutIndex(present, second);
                const auto decoded = DualXorErasureCodec::decode(present, layout);
                AKK_TEST_CHECK(decoded == value);
            }
        }
    }

    void testDualXorRejectsTwoMissingWithoutBothParityShards() {
        const ErasureLayout layout{.dataShards = 6, .parityShards = 2};
        const auto shards = DualXorErasureCodec::encode(bytesOf("dual xor parity requirement"), layout);
        auto present = withoutIndex(shards, 1);
        present = withoutIndex(present, 4);
        present = withoutIndex(present, static_cast<uint16_t>(layout.dataShards + 1u));

        bool rejected = false;
        try {
            (void)DualXorErasureCodec::decode(present, layout);
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }
        AKK_TEST_CHECK(rejected);
    }

    void testDualXorRejectsInvalidLayout() {
        bool rejected = false;
        try {
            (void)DualXorErasureCodec::encode(bytesOf("bad"), ErasureLayout{.dataShards = 2, .parityShards = 1});
        }
        catch (const std::invalid_argument&) {
            rejected = true;
        }
        AKK_TEST_CHECK(rejected);
    }

    void testRsRoundtripUneven() {
        const ErasureLayout layout{.dataShards = 6, .parityShards = 3};
        const std::string value = "akkaradb rs erasure smoke payload";
        const auto shards = RsErasureCodec::encode(bytesOf(value), layout);
        AKK_TEST_CHECK(shards.size() == 9);
        AKK_TEST_CHECK(shards[0].payload.size() == RsErasureCodec::shardPayloadSize(value.size(), layout.dataShards));
        for (const auto& shard : shards) { AKK_TEST_CHECK(shard.verifyCrc()); }

        const auto decoded = RsErasureCodec::decode(shards, layout);
        AKK_TEST_CHECK(std::string(reinterpret_cast<const char*>(decoded.data()), decoded.size()) == value);
    }

    void testRsRepairSingleMissingEachShard() {
        const ErasureLayout layout{.dataShards = 7, .parityShards = 3};
        const auto value = makePayload(256 * 1024 + 77);
        const auto shards = RsErasureCodec::encode(value, layout);

        for (uint16_t missing = 0; missing < layout.totalShards(); ++missing) {
            auto present = withoutIndex(shards, missing);
            const auto repaired = RsErasureCodec::repairOne(missing, present, layout);
            AKK_TEST_CHECK(repaired.index == missing);
            AKK_TEST_CHECK(repaired.verifyCrc());
            AKK_TEST_CHECK(repaired.payload == shards[missing].payload);

            present.push_back(repaired);
            const auto decoded = RsErasureCodec::decode(present, layout);
            AKK_TEST_CHECK(decoded == value);
        }
    }

    void testRsDecodeMultipleMissingShards() {
        const ErasureLayout layout{.dataShards = 8, .parityShards = 4};
        const auto value = makePayload(1024 * 1024 + 503);
        const auto shards = RsErasureCodec::encode(value, layout);

        auto present = withoutIndex(shards, 1);
        present = withoutIndex(present, 3);
        present = withoutIndex(present, 8);
        present = withoutIndex(present, 10);
        const auto decoded = RsErasureCodec::decode(present, layout);
        AKK_TEST_CHECK(decoded == value);
    }

    void testRsDecodeAllMissingCombosUpToParity() {
        const ErasureLayout layout{.dataShards = 5, .parityShards = 3};
        const auto value = makePayload(32 * 1024 + 7);
        const auto shards = RsErasureCodec::encode(value, layout);

        for (uint16_t first = 0; first < layout.totalShards(); ++first) {
            const std::array<uint16_t, 1> missing1{first};
            AKK_TEST_CHECK(RsErasureCodec::decode(withoutIndices(shards, missing1), layout) == value);

            for (uint16_t second = static_cast<uint16_t>(first + 1u); second < layout.totalShards(); ++second) {
                const std::array<uint16_t, 2> missing2{first, second};
                AKK_TEST_CHECK(RsErasureCodec::decode(withoutIndices(shards, missing2), layout) == value);

                for (uint16_t third = static_cast<uint16_t>(second + 1u); third < layout.totalShards(); ++third) {
                    const std::array<uint16_t, 3> missing3{first, second, third};
                    AKK_TEST_CHECK(RsErasureCodec::decode(withoutIndices(shards, missing3), layout) == value);
                }
            }
        }
    }

    void testRsRejectsTooManyMissingShards() {
        const ErasureLayout layout{.dataShards = 6, .parityShards = 2};
        const auto shards = RsErasureCodec::encode(bytesOf("rs too many missing"), layout);
        auto present = withoutIndex(shards, 0);
        present = withoutIndex(present, 1);
        present = withoutIndex(present, 2);

        bool rejected = false;
        try {
            (void)RsErasureCodec::decode(present, layout);
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }
        AKK_TEST_CHECK(rejected);
    }

    void testRsRejectsInvalidLayout() {
        bool rejected = false;
        try {
            (void)RsErasureCodec::encode(bytesOf("bad"), ErasureLayout{.dataShards = 0, .parityShards = 3});
        }
        catch (const std::invalid_argument&) {
            rejected = true;
        }
        AKK_TEST_CHECK(rejected);
    }

    void testErsRoundtripUneven() {
        const ErasureLayout layout{.dataShards = 6, .parityShards = 3};
        const std::string value = "akkaradb ers erasure smoke payload";
        const auto shards = ErsCodec::encode(bytesOf(value), layout);
        AKK_TEST_CHECK(shards.size() == 9);
        AKK_TEST_CHECK(shards[0].payload.size() == ErsCodec::shardPayloadSize(value.size(), layout.dataShards));
        for (const auto& shard : shards) { AKK_TEST_CHECK(shard.verifyCrc()); }

        const auto decoded = ErsCodec::decode(shards, layout);
        AKK_TEST_CHECK(std::string(reinterpret_cast<const char*>(decoded.data()), decoded.size()) == value);
    }

    void testErsRecoverSingleHiddenCorruptShard() {
        const ErasureLayout layout{.dataShards = 6, .parityShards = 3};
        const auto value = makePayload(128 * 1024 + 29);
        auto shards = ErsCodec::encode(value, layout);
        const auto expected = shards;

        shards[2].payload[17] ^= 0x5Au;
        shards[2].payload[31] ^= 0xC3u;
        shards[2].crc32c = crcOf(shards[2].payload);

        const auto recovered = ErsCodec::recover(shards, layout);
        AKK_TEST_CHECK(recovered.value == value);
        AKK_TEST_CHECK(recovered.missingIndices.empty());
        AKK_TEST_CHECK(recovered.detectedErrorIndices.size() == 1);
        AKK_TEST_CHECK(recovered.detectedErrorIndices[0] == 2);
        AKK_TEST_CHECK(recovered.repairedShards.size() == 1);
        AKK_TEST_CHECK(recovered.repairedShards[0].index == 2);
        AKK_TEST_CHECK(recovered.repairedShards[0].payload == expected[2].payload);
    }

    void testErsRecoverMissingAndCorruptShard() {
        const ErasureLayout layout{.dataShards = 5, .parityShards = 3};
        const auto value = makePayload(96 * 1024 + 5);
        auto shards = ErsCodec::encode(value, layout);
        const auto expected = shards;

        shards[6].payload[9] ^= 0x11u;
        shards[6].payload[25] ^= 0xEEu;
        shards[6].crc32c = crcOf(shards[6].payload);
        auto present = withoutIndex(shards, 1);

        const auto recovered = ErsCodec::recover(present, layout);
        AKK_TEST_CHECK(recovered.value == value);
        AKK_TEST_CHECK(recovered.missingIndices.size() == 1);
        AKK_TEST_CHECK(recovered.missingIndices[0] == 1);
        AKK_TEST_CHECK(recovered.detectedErrorIndices.size() == 1);
        AKK_TEST_CHECK(recovered.detectedErrorIndices[0] == 6);
        AKK_TEST_CHECK(recovered.repairedShards.size() == 2);

        bool sawMissing = false;
        bool sawCorrupt = false;
        for (const auto& shard : recovered.repairedShards) {
            if (shard.index == 1) {
                sawMissing = true;
                AKK_TEST_CHECK(shard.payload == expected[1].payload);
            }
            if (shard.index == 6) {
                sawCorrupt = true;
                AKK_TEST_CHECK(shard.payload == expected[6].payload);
            }
        }
        AKK_TEST_CHECK(sawMissing);
        AKK_TEST_CHECK(sawCorrupt);
    }

    void testErsRecoverTwoHiddenCorruptShards() {
        const ErasureLayout layout{.dataShards = 6, .parityShards = 4};
        const auto value = makePayload(64 * 1024 + 13);
        auto shards = ErsCodec::encode(value, layout);

        shards[1].payload[3] ^= 0xA7u;
        shards[1].payload[59] ^= 0x61u;
        shards[1].crc32c = crcOf(shards[1].payload);
        shards[8].payload[7] ^= 0x44u;
        shards[8].payload[41] ^= 0x99u;
        shards[8].crc32c = crcOf(shards[8].payload);

        const auto recovered = ErsCodec::recover(shards, layout);
        AKK_TEST_CHECK(recovered.value == value);
        AKK_TEST_CHECK(recovered.missingIndices.empty());
        AKK_TEST_CHECK(recovered.detectedErrorIndices.size() == 2);
        AKK_TEST_CHECK(recovered.detectedErrorIndices[0] == 1);
        AKK_TEST_CHECK(recovered.detectedErrorIndices[1] == 8);
    }
} // namespace

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();

    testXorRoundtripUneven();
    testXorRepairEachShard();
    testXorZeroLength();
    testXorCrcRejectsCorruption();
    testXorRejectsInsufficientShards();
    testXorRejectsInvalidLayout();
    testDualXorRoundtripUneven();
    testDualXorRepairSingleMissingEachShard();
    testDualXorDecodeTwoMissingDataShards();
    testDualXorDecodeAllMissingDataPairs();
    testDualXorRejectsTwoMissingWithoutBothParityShards();
    testDualXorRejectsInvalidLayout();
    testRsRoundtripUneven();
    testRsRepairSingleMissingEachShard();
    testRsDecodeMultipleMissingShards();
    testRsDecodeAllMissingCombosUpToParity();
    testRsRejectsTooManyMissingShards();
    testRsRejectsInvalidLayout();
    testErsRoundtripUneven();
    testErsRecoverSingleHiddenCorruptShard();
    testErsRecoverMissingAndCorruptShard();
    testErsRecoverTwoHiddenCorruptShards();
    std::printf("erasure smoke test passed\n");
    return 0;
}

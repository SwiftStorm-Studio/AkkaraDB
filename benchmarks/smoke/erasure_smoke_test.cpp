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

    std::vector<ErasureShard> withoutIndex(const std::vector<ErasureShard>& shards, uint16_t missingIndex) {
        std::vector<ErasureShard> out;
        for (const auto& shard : shards) {
            if (shard.index != missingIndex) { out.push_back(shard); }
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
} // namespace

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();

    testXorRoundtripUneven();
    testXorRepairEachShard();
    testXorZeroLength();
    testXorCrcRejectsCorruption();
    testXorRejectsInsufficientShards();
    testXorRejectsInvalidLayout();
    std::printf("erasure smoke test passed\n");
    return 0;
}

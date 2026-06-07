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

// benchmarks/smoke/erasure_smoke_test.cpp
#include "TestErrorHandlers.hpp"

#include "akk/engine/erasure/ErasureCodec.hpp"

#include <cassert>
#include <cstdio>
#include <stdexcept>
#include <string>
#include <vector>

using namespace akkaradb::engine::erasure;

namespace {
    std::span<const uint8_t> bytes_of(const std::string& value) {
        return {reinterpret_cast<const uint8_t*>(value.data()), value.size()};
    }

    std::vector<uint8_t> make_payload(size_t size) {
        std::vector<uint8_t> out(size);
        uint32_t state = 0xA55A1234u;
        for (auto& byte : out) {
            state = state * 1664525u + 1013904223u;
            byte = static_cast<uint8_t>(state >> 24);
        }
        return out;
    }

    std::vector<ErasureShard> without_index(const std::vector<ErasureShard>& shards, uint16_t missing_index) {
        std::vector<ErasureShard> out;
        for (const auto& shard : shards) {
            if (shard.index != missing_index) { out.push_back(shard); }
        }
        return out;
    }

    void test_xor_roundtrip_uneven() {
        const ErasureLayout layout{.data_shards = 4, .parity_shards = 1};
        const std::string value = "akkaradb xor erasure smoke payload";
        const auto shards = XorErasureCodec::encode(bytes_of(value), layout);
        assert(shards.size() == 5);
        assert(shards[0].payload.size() == XorErasureCodec::shard_payload_size(value.size(), layout.data_shards));
        for (const auto& shard : shards) { assert(shard.verify_crc()); }

        const auto decoded = XorErasureCodec::decode(shards, layout);
        assert(std::string(reinterpret_cast<const char*>(decoded.data()), decoded.size()) == value);
    }

    void test_xor_repair_each_shard() {
        const ErasureLayout layout{.data_shards = 5, .parity_shards = 1};
        const auto value = make_payload(1024 * 1024 + 137);
        const auto shards = XorErasureCodec::encode(value, layout);

        for (uint16_t missing = 0; missing < layout.total_shards(); ++missing) {
            auto present = without_index(shards, missing);
            const auto repaired = XorErasureCodec::repair_one(missing, present, layout);
            assert(repaired.index == missing);
            assert(repaired.verify_crc());
            assert(repaired.payload == shards[missing].payload);

            present.push_back(repaired);
            const auto decoded = XorErasureCodec::decode(present, layout);
            assert(decoded == value);
        }
    }

    void test_xor_zero_length() {
        const ErasureLayout layout{.data_shards = 3, .parity_shards = 1};
        const std::vector<uint8_t> empty;
        const auto shards = XorErasureCodec::encode(empty, layout);
        assert(shards.size() == 4);
        for (const auto& shard : shards) {
            assert(shard.payload.empty());
            assert(shard.verify_crc());
        }
        const auto decoded = XorErasureCodec::decode(shards, layout);
        assert(decoded.empty());
    }

    void test_xor_crc_rejects_corruption() {
        const ErasureLayout layout{.data_shards = 3, .parity_shards = 1};
        const auto shards = XorErasureCodec::encode(bytes_of("crc-check"), layout);

        auto corrupt = shards;
        corrupt[1].payload[0] ^= 0x80;
        bool rejected = false;
        try {
            (void)XorErasureCodec::decode(corrupt, layout);
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }
        assert(rejected);
    }

    void test_xor_rejects_insufficient_shards() {
        const ErasureLayout layout{.data_shards = 4, .parity_shards = 1};
        const auto shards = XorErasureCodec::encode(bytes_of("too many missing shards"), layout);
        auto present = without_index(shards, 0);
        present = without_index(present, 1);

        bool rejected = false;
        try {
            (void)XorErasureCodec::decode(present, layout);
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }
        assert(rejected);
    }

    void test_xor_rejects_invalid_layout() {
        bool rejected = false;
        try {
            (void)XorErasureCodec::encode(bytes_of("bad"), ErasureLayout{.data_shards = 2, .parity_shards = 2});
        }
        catch (const std::invalid_argument&) {
            rejected = true;
        }
        assert(rejected);
    }
} // namespace

int main() {
    akkara::test::install_msvc_test_error_handlers();

    test_xor_roundtrip_uneven();
    test_xor_repair_each_shard();
    test_xor_zero_length();
    test_xor_crc_rejects_corruption();
    test_xor_rejects_insufficient_shards();
    test_xor_rejects_invalid_layout();
    std::printf("erasure smoke test passed\n");
    return 0;
}

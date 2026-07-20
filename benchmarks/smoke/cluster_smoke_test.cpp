/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// benchmarks/smoke/cluster_smoke_test.cpp
#include "TestErrorHandlers.hpp"

#include "akk/engine/cluster/ClusterConfig.hpp"
#include "akk/engine/cluster/ClusterRuntime.hpp"
#include "akk/engine/cluster/ClusterRouter.hpp"
#include "akk/engine/cluster/ReplFraming.hpp"
#include "akk/engine/cluster/ReplicationClient.hpp"
#include "akk/engine/cluster/ReplicationServer.hpp"
#include "akk/engine/erasure/ErasureCodec.hpp"
#include "akk/engine/erasure/ErasureCodecExt.hpp"

#include <array>
#include <atomic>
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <limits>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace akkaradb::engine::cluster;
namespace erasure = akkaradb::engine::erasure;

extern "C" bool akkaradb_cluster_register() noexcept;

namespace {
    constexpr uint32_t DATA_AND_COORDINATOR =
        static_cast<uint32_t>(NodeCapability::DATA_BEARING) |
        static_cast<uint32_t>(NodeCapability::COORDINATOR_ELIGIBLE);

    [[noreturn]] void fail(const char* expr, const char* file, int line) {
        std::fprintf(stderr, "check failed: %s at %s:%d\n", expr, file, line);
        throw std::runtime_error(std::string{"check failed: "} + expr + " at " + file + ":" + std::to_string(line));
    }

#define AKK_CLUSTER_CHECK(expr) do { if (!(expr)) { fail(#expr, __FILE__, __LINE__); } } while (false)

    std::filesystem::path makeTempDir(const std::string& suffix) {
        const auto dir = std::filesystem::temp_directory_path() / ("akkaradbClusterTest_" + suffix);
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
        std::filesystem::create_directories(dir, ec);
        AKK_CLUSTER_CHECK(!ec);
        return dir;
    }

    NodeInfo node(uint64_t id, uint16_t dataPort, uint16_t replPort, uint32_t capabilities = DATA_AND_COORDINATOR) {
        return NodeInfo{
            .nodeId = id,
            .host = "127.0.0.1",
            .dataPort = dataPort,
            .replPort = replPort,
            .capabilities = capabilities,
        };
    }

    RaftOptions onlineRaftMembership() {
        RaftOptions raft;
        raft.membership.mode = RaftMembershipMode::JOINT_CONSENSUS;
        raft.membership.allowOnlineVoterChanges = true;
        return raft;
    }

    std::span<const uint8_t> bytesOf(const std::string& value) {
        return {reinterpret_cast<const uint8_t*>(value.data()), value.size()};
    }

    std::string textOf(std::span<const uint8_t> value) {
        return {reinterpret_cast<const char*>(value.data()), value.size()};
    }

    uint64_t nextPrng(uint64_t& state) noexcept {
        state = state * 6364136223846793005ull + 1442695040888963407ull;
        return state;
    }

    std::vector<uint8_t> deterministicBytes(size_t size, uint64_t seed) {
        std::vector<uint8_t> out(size);
        uint64_t state = seed;
        for (auto& byte : out) { byte = static_cast<uint8_t>(nextPrng(state) >> 56u); }
        return out;
    }

    std::vector<erasure::ErasureShard> selectShards(
        const std::vector<erasure::ErasureShard>& shards,
        erasure::ErasureLayout layout,
        uint64_t seed
    ) {
        std::vector<uint16_t> indices;
        indices.reserve(shards.size());
        for (uint16_t i = 0; i < layout.totalShards(); ++i) { indices.push_back(i); }

        uint64_t state = seed;
        for (size_t i = indices.size(); i > 1; --i) {
            const size_t j = static_cast<size_t>(nextPrng(state) % i);
            std::swap(indices[i - 1], indices[j]);
        }

        std::vector<erasure::ErasureShard> selected;
        selected.reserve(layout.dataShards);
        for (uint16_t i = 0; i < layout.dataShards; ++i) { selected.push_back(shards[indices[i]]); }
        return selected;
    }

    template <typename Predicate>
    bool waitUntil(Predicate predicate, std::chrono::milliseconds timeout = std::chrono::milliseconds{5000}) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (predicate()) { return true; }
            std::this_thread::sleep_for(std::chrono::milliseconds{25});
        }
        return predicate();
    }

    struct RuntimeHarness {
        uint64_t nodeId = 0;
        std::atomic<uint64_t> lastSeq{0};
        std::atomic<uint64_t> appliedCount{0};
        std::atomic<uint64_t> durableCount{0};
        std::atomic<uint64_t> snapshotsInstalled{0};
        mutable std::mutex stateMutex;
        std::string lastKey;
        std::string lastValue;
        std::string pendingSnapshotKey;
        std::string pendingSnapshotValue;
        std::unique_ptr<ClusterRuntime> runtime;

        void setState(uint64_t seq, std::span<const uint8_t> key, std::span<const uint8_t> value) {
            std::lock_guard lock{stateMutex};
            lastKey = textOf(key);
            lastValue = textOf(value);
            lastSeq.store(seq);
        }

        bool hasState(uint64_t seq, const std::string& key, const std::string& value) const {
            std::lock_guard lock{stateMutex};
            return lastSeq.load() == seq && lastKey == key && lastValue == value;
        }
    };

    std::unique_ptr<RuntimeHarness> makeRuntime(
        const std::filesystem::path& dir,
        const ClusterConfig& cfg,
        uint64_t nodeId,
        ClusterRuntimeOptions options = {}
    ) {
        auto harness = std::make_unique<RuntimeHarness>();
        harness->nodeId = nodeId;
        options.transportMode = TransportMode::PLAIN;
        ClusterEngineCallbacks callbacks;
        callbacks.getLastSeq = [harness = harness.get()] { return harness->lastSeq.load(); };
        callbacks.getCurrentSeq = [harness = harness.get()] { return harness->lastSeq.load(); };
        callbacks.exportSnapshot = [harness = harness.get()]() -> std::optional<ClusterSnapshot> {
            std::lock_guard lock{harness->stateMutex};
            const uint64_t seq = harness->lastSeq.load();
            if (seq == 0) { return std::nullopt; }
            ClusterSnapshot snapshot;
            snapshot.seq = seq;
            snapshot.entries.push_back(ClusterHistoryEntry{
                .seq = seq,
                .sourceNodeId = harness->nodeId,
                .op = static_cast<uint8_t>(ReplOpType::PUT),
                .recordFlags = 0,
                .key = std::vector<uint8_t>{harness->lastKey.begin(), harness->lastKey.end()},
                .value = std::vector<uint8_t>{harness->lastValue.begin(), harness->lastValue.end()},
            });
            return snapshot;
        };
        callbacks.beginSnapshot = [harness = harness.get()](uint64_t, uint64_t) {
            std::lock_guard lock{harness->stateMutex};
            harness->pendingSnapshotKey.clear();
            harness->pendingSnapshotValue.clear();
        };
        callbacks.applySnapshotEntry = [harness = harness.get()](std::span<const uint8_t> key, std::span<const uint8_t> value) {
            std::lock_guard lock{harness->stateMutex};
            harness->pendingSnapshotKey = textOf(key);
            harness->pendingSnapshotValue = textOf(value);
        };
        callbacks.finishSnapshot = [harness = harness.get()](uint64_t snapshotSeq) {
            std::lock_guard lock{harness->stateMutex};
            harness->lastKey = harness->pendingSnapshotKey;
            harness->lastValue = harness->pendingSnapshotValue;
            harness->lastSeq.store(snapshotSeq);
            harness->snapshotsInstalled.fetch_add(1);
        };
        callbacks.apply = [harness = harness.get()](
            uint64_t seq,
            ReplOpType,
            std::span<const uint8_t> key,
            std::span<const uint8_t> value,
            uint8_t,
            uint64_t
        ) {
            harness->setState(seq, key, value);
            harness->appliedCount.fetch_add(1);
        };
        callbacks.forceDurable = [harness = harness.get()] { harness->durableCount.fetch_add(1); };
        harness->runtime = ClusterRuntime::create(dir, cfg, nodeId, std::move(callbacks), options);
        return harness;
    }

    RuntimeHarness* findLeader(std::span<RuntimeHarness* const> nodes) {
        for (auto* n : nodes) {
            if (n->runtime->role() == NodeRole::PRIMARY) { return n; }
        }
        return nullptr;
    }

    void testRaftOptionsRoundtripAndValidation() {
        const auto dir = makeTempDir("raft-options");
        const ClusterConfig cfg{
            {
                node(1, 20151, 20251),
                node(2, 20152, 20252),
                node(3, 20153, 20253),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM},
            onlineRaftMembership(),
        };
        const auto path = dir / "cluster.cfg";
        ClusterConfig::save(path, cfg);
        const auto loaded = ClusterConfig::load(path);
        AKK_CLUSTER_CHECK(loaded.raft().membership.mode == RaftMembershipMode::JOINT_CONSENSUS);
        AKK_CLUSTER_CHECK(loaded.raft().membership.allowOnlineVoterChanges);

        bool rejected = false;
        try {
            (void)ClusterConfig{
                {
                    node(1, 20154, 20254),
                    node(2, 20155, 20255),
                },
                ReplicationMode::MIRROR,
                AckPolicy{},
                ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK},
                onlineRaftMembership(),
            };
        }
        catch (const std::invalid_argument&) {
            rejected = true;
        }
        AKK_CLUSTER_CHECK(rejected);
    }

    void testPartitionedAndStripeRouting() {
        const ClusterConfig partitioned{
            {
                node(1, 20301, 20401),
                node(2, 20302, 20402),
                node(3, 20303, 20403),
            },
            ReplicationMode::PARTITIONED,
            AckPolicy{},
        };
        ClusterRouter partitionedRouter{partitioned};
        const auto partitionedTargets = partitionedRouter.writeTargets(bytesOf("partitioned-key"));
        AKK_CLUSTER_CHECK(partitionedTargets.size() == 1);

        StripeOptions stripeOptions;
        stripeOptions.dataShards = 3;
        stripeOptions.parityShards = 2;
        const ClusterConfig stripe{
            {
                node(1, 20311, 20411),
                node(2, 20312, 20412),
                node(3, 20313, 20413),
                node(4, 20314, 20414),
                node(5, 20315, 20415),
            },
            ReplicationMode::STRIPE,
            AckPolicy{},
            {},
            {},
            stripeOptions,
        };
        ClusterRouter stripeRouter{stripe};
        const auto shardTargets = stripeRouter.stripeShardTargets(bytesOf("stripe-key"));
        AKK_CLUSTER_CHECK(shardTargets.size() == 5);
        std::vector<uint64_t> nodeIds;
        for (size_t i = 0; i < shardTargets.size(); ++i) {
            AKK_CLUSTER_CHECK(shardTargets[i].shardIndex == i);
            nodeIds.push_back(shardTargets[i].node.nodeId);
        }
        std::sort(nodeIds.begin(), nodeIds.end());
        AKK_CLUSTER_CHECK(std::adjacent_find(nodeIds.begin(), nodeIds.end()) == nodeIds.end());

        const auto dir = makeTempDir("stripe-config");
        const auto path = dir / "cluster.cfg";
        ClusterConfig::save(path, stripe);
        const auto loaded = ClusterConfig::load(path);
        AKK_CLUSTER_CHECK(loaded.mode() == ReplicationMode::STRIPE);
        AKK_CLUSTER_CHECK(loaded.stripe().dataShards == 3);
        AKK_CLUSTER_CHECK(loaded.stripe().parityShards == 2);
    }

    void testStripeErasureCodecRecovery() {
        const std::string value = "AkkaraDB parity stripe recovery across missing shards";
        const erasure::ErasureLayout layout{.dataShards = 4, .parityShards = 2};
        auto shards = erasure::RsErasureCodec::encode(bytesOf(value), layout);
        AKK_CLUSTER_CHECK(shards.size() == 6);

        for (size_t a = 0; a < shards.size(); ++a) {
            for (size_t b = a + 1; b < shards.size(); ++b) {
                for (size_t c = b + 1; c < shards.size(); ++c) {
                    for (size_t d = c + 1; d < shards.size(); ++d) {
                        std::vector<erasure::ErasureShard> available{shards[a], shards[b], shards[c], shards[d]};
                        const auto recovered = erasure::RsErasureCodec::decode(available, layout);
                        AKK_CLUSTER_CHECK(textOf(recovered) == value);
                    }
                }
            }
        }

        for (uint16_t missingIndex = 0; missingIndex < layout.totalShards(); ++missingIndex) {
            std::vector<erasure::ErasureShard> availableForRepair;
            availableForRepair.reserve(shards.size() - 1);
            for (const auto& shard : shards) {
                if (shard.index != missingIndex) { availableForRepair.push_back(shard); }
            }
            const auto repairedShard = erasure::RsErasureCodec::repairOne(missingIndex, availableForRepair, layout);
            AKK_CLUSTER_CHECK(repairedShard.index == missingIndex);
            AKK_CLUSTER_CHECK(repairedShard.originalSize == shards[missingIndex].originalSize);
            AKK_CLUSTER_CHECK(repairedShard.payload == shards[missingIndex].payload);
            AKK_CLUSTER_CHECK(repairedShard.crc32c == shards[missingIndex].crc32c);
        }
        const auto repairedParityWithMissingData =
            erasure::RsErasureCodec::repairOne(4, std::vector<erasure::ErasureShard>{shards[0], shards[2], shards[3], shards[5]}, layout);
        AKK_CLUSTER_CHECK(repairedParityWithMissingData.index == 4);
        AKK_CLUSTER_CHECK(repairedParityWithMissingData.payload == shards[4].payload);
        AKK_CLUSTER_CHECK(repairedParityWithMissingData.crc32c == shards[4].crc32c);

        const std::string cauchyValue = "AkkaraDB Cauchy RS matrix regression";
        const erasure::ErasureLayout cauchyLayout{.dataShards = 3, .parityShards = 4};
        const auto cauchyShards = erasure::RsErasureCodec::encode(bytesOf(cauchyValue), cauchyLayout);
        for (size_t a = 0; a < cauchyShards.size(); ++a) {
            for (size_t b = a + 1; b < cauchyShards.size(); ++b) {
                for (size_t c = b + 1; c < cauchyShards.size(); ++c) {
                    std::vector<erasure::ErasureShard> available{cauchyShards[a], cauchyShards[b], cauchyShards[c]};
                    const auto recovered = erasure::RsErasureCodec::decode(available, cauchyLayout);
                    AKK_CLUSTER_CHECK(textOf(recovered) == cauchyValue);
                }
            }
        }

        const auto assertBoundaryRoundTrip = [](const std::vector<uint8_t>& payload) {
            const erasure::ErasureLayout boundaryLayout{.dataShards = 4, .parityShards = 2};
            const auto encoded = erasure::RsErasureCodec::encode(std::span<const uint8_t>{payload.data(), payload.size()}, boundaryLayout);
            AKK_CLUSTER_CHECK(encoded.size() == boundaryLayout.totalShards());
            const auto decoded = erasure::RsErasureCodec::decode(
                std::vector<erasure::ErasureShard>{encoded[0], encoded[2], encoded[3], encoded[5]},
                boundaryLayout
            );
            AKK_CLUSTER_CHECK(decoded == payload);
        };
        assertBoundaryRoundTrip({});
        assertBoundaryRoundTrip({0xA5});
        assertBoundaryRoundTrip({0, 1, 2, 3, 4, 5, 6, 7});
        assertBoundaryRoundTrip({0, 1, 2, 3, 4, 5, 6, 7, 8});
        AKK_CLUSTER_CHECK(erasure::RsErasureCodec::shardPayloadSize(std::numeric_limits<uint64_t>::max(), 2) ==
                          (std::numeric_limits<uint64_t>::max() / 2u) + 1u);

        const auto repaired = erasure::RsErasureCodec::repairOne(1, std::vector<erasure::ErasureShard>{shards[0], shards[2], shards[3], shards[4]}, layout);
        AKK_CLUSTER_CHECK(repaired.index == 1);
        AKK_CLUSTER_CHECK(repaired.payload == shards[1].payload);
        AKK_CLUSTER_CHECK(repaired.crc32c == shards[1].crc32c);

        auto corrupted = shards;
        corrupted[0].payload[0] ^= 0x7Fu;
        bool crcRejected = false;
        try {
            (void)erasure::RsErasureCodec::decode(std::vector<erasure::ErasureShard>{corrupted[0], corrupted[1], corrupted[2], corrupted[3]}, layout);
        }
        catch (const std::runtime_error&) {
            crcRejected = true;
        }
        AKK_CLUSTER_CHECK(crcRejected);

        std::vector<erasure::ErasureShard> available{shards[0], shards[2], shards[4]};
        bool rejected = false;
        try {
            (void)erasure::RsErasureCodec::decode(available, layout);
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }
        AKK_CLUSTER_CHECK(rejected);
    }

    void testRsErasureCodecProperties() {
        const auto runDecodeSample = [](erasure::ErasureLayout layout, size_t payloadSize, uint64_t seed) {
            const auto payload = deterministicBytes(payloadSize, seed);
            const auto shards = erasure::RsErasureCodec::encode(std::span<const uint8_t>{payload.data(), payload.size()}, layout);
            const auto selected = selectShards(shards, layout, seed ^ 0x9E3779B97F4A7C15ull);
            const auto decoded = erasure::RsErasureCodec::decode(selected, layout);
            AKK_CLUSTER_CHECK(decoded == payload);
        };

        const auto runFullCombinationDecode = [](erasure::ErasureLayout layout, size_t payloadSize, uint64_t seed) {
            const auto payload = deterministicBytes(payloadSize, seed);
            const auto shards = erasure::RsErasureCodec::encode(std::span<const uint8_t>{payload.data(), payload.size()}, layout);

            std::vector<uint16_t> picked;
            picked.reserve(layout.dataShards);
            const auto walk = [&](auto&& self, uint16_t offset) -> void {
                if (picked.size() == layout.dataShards) {
                    std::vector<erasure::ErasureShard> selected;
                    selected.reserve(layout.dataShards);
                    for (const auto index : picked) { selected.push_back(shards[index]); }
                    const auto decoded = erasure::RsErasureCodec::decode(selected, layout);
                    AKK_CLUSTER_CHECK(decoded == payload);
                    return;
                }

                const auto remaining = static_cast<uint16_t>(layout.dataShards - picked.size());
                for (uint16_t i = offset; i + remaining <= layout.totalShards(); ++i) {
                    picked.push_back(i);
                    self(self, static_cast<uint16_t>(i + 1u));
                    picked.pop_back();
                }
            };
            walk(walk, 0);
        };

        runFullCombinationDecode({.dataShards = 2, .parityShards = 1}, 17, 0x2001);
        runFullCombinationDecode({.dataShards = 2, .parityShards = 3}, 31, 0x2002);
        runFullCombinationDecode({.dataShards = 3, .parityShards = 4}, 43, 0x2003);
        runFullCombinationDecode({.dataShards = 4, .parityShards = 4}, 71, 0x2004);
        runFullCombinationDecode({.dataShards = 6, .parityShards = 3}, 97, 0x2005);

        const std::array sampledLayouts{
            erasure::ErasureLayout{.dataShards = 8, .parityShards = 4},
            erasure::ErasureLayout{.dataShards = 10, .parityShards = 6},
            erasure::ErasureLayout{.dataShards = 16, .parityShards = 8},
            erasure::ErasureLayout{.dataShards = 32, .parityShards = 8},
            erasure::ErasureLayout{.dataShards = 64, .parityShards = 16},
        };

        for (size_t layoutIndex = 0; layoutIndex < sampledLayouts.size(); ++layoutIndex) {
            const auto sampledLayout = sampledLayouts[layoutIndex];
            for (uint64_t sample = 0; sample < 32; ++sample) {
                runDecodeSample(sampledLayout, 128 + layoutIndex * 31 + static_cast<size_t>(sample * 7), 0x5000 + layoutIndex * 257 + sample);
            }

            const auto payload = deterministicBytes(256 + layoutIndex * 17, 0x7000 + layoutIndex);
            const auto shards = erasure::RsErasureCodec::encode(std::span<const uint8_t>{payload.data(), payload.size()}, sampledLayout);
            for (const uint16_t missingIndex : {
                     uint16_t{0},
                     static_cast<uint16_t>(sampledLayout.dataShards - 1u),
                     sampledLayout.dataShards,
                     static_cast<uint16_t>(sampledLayout.totalShards() - 1u),
                 }) {
                std::vector<erasure::ErasureShard> available;
                available.reserve(shards.size() - 1);
                for (const auto& shard : shards) {
                    if (shard.index != missingIndex) { available.push_back(shard); }
                }
                const auto repaired = erasure::RsErasureCodec::repairOne(missingIndex, available, sampledLayout);
                AKK_CLUSTER_CHECK(repaired.index == missingIndex);
                AKK_CLUSTER_CHECK(repaired.payload == shards[missingIndex].payload);
                AKK_CLUSTER_CHECK(repaired.crc32c == shards[missingIndex].crc32c);
            }
        }
    }

    void testErsCodecRecovery() {
        const auto containsIndex = [](const std::vector<uint16_t>& indices, uint16_t expected) {
            return std::find(indices.begin(), indices.end(), expected) != indices.end();
        };
        const auto containsShard = [](const std::vector<erasure::ErasureShard>& shards, const erasure::ErasureShard& expected) {
            return std::find_if(
                       shards.begin(),
                       shards.end(),
                       [&expected](const erasure::ErasureShard& shard) {
                           return shard.index == expected.index &&
                                  shard.originalSize == expected.originalSize &&
                                  shard.payload == expected.payload &&
                                  shard.crc32c == expected.crc32c;
                       }
                   ) != shards.end();
        };

        const std::string value = "AkkaraDB ERS corruption and erasure recovery";
        const erasure::ErasureLayout layout{.dataShards = 4, .parityShards = 3};
        const auto shards = erasure::ErsCodec::encode(bytesOf(value), layout);

        {
            std::vector<erasure::ErasureShard> missing{shards[0], shards[1], shards[3], shards[4], shards[6]};
            const auto recovered = erasure::ErsCodec::recover(missing, layout);
            AKK_CLUSTER_CHECK(textOf(recovered.value) == value);
            AKK_CLUSTER_CHECK(recovered.missingIndices.size() == 2);
            AKK_CLUSTER_CHECK(containsIndex(recovered.missingIndices, 2));
            AKK_CLUSTER_CHECK(containsIndex(recovered.missingIndices, 5));
            AKK_CLUSTER_CHECK(recovered.detectedErrorIndices.empty());
            AKK_CLUSTER_CHECK(containsShard(recovered.repairedShards, shards[2]));
            AKK_CLUSTER_CHECK(containsShard(recovered.repairedShards, shards[5]));
        }

        {
            auto corrupted = shards;
            corrupted[1].payload[0] ^= 0x33u;
            const auto recovered = erasure::ErsCodec::recover(corrupted, layout);
            AKK_CLUSTER_CHECK(textOf(recovered.value) == value);
            AKK_CLUSTER_CHECK(recovered.missingIndices.empty());
            AKK_CLUSTER_CHECK(recovered.detectedErrorIndices.size() == 1);
            AKK_CLUSTER_CHECK(recovered.detectedErrorIndices[0] == 1);
            AKK_CLUSTER_CHECK(containsShard(recovered.repairedShards, shards[1]));
        }

        {
            auto suspicious = shards;
            suspicious[4].payload[0] ^= 0x55u;
            const std::array<uint16_t, 1> knownBad{4};
            const auto recovered = erasure::ErsCodec::recover(suspicious, layout, knownBad);
            AKK_CLUSTER_CHECK(textOf(recovered.value) == value);
            AKK_CLUSTER_CHECK(recovered.missingIndices.empty());
            AKK_CLUSTER_CHECK(recovered.detectedErrorIndices.size() == 1);
            AKK_CLUSTER_CHECK(recovered.detectedErrorIndices[0] == 4);
            AKK_CLUSTER_CHECK(containsShard(recovered.repairedShards, shards[4]));
        }

        {
            auto corrupted = shards;
            corrupted[0].payload[0] ^= 0x11u;
            corrupted[5].payload[0] ^= 0x22u;
            const auto recovered = erasure::ErsCodec::recover(corrupted, layout);
            AKK_CLUSTER_CHECK(textOf(recovered.value) == value);
            AKK_CLUSTER_CHECK(recovered.detectedErrorIndices.size() == 2);
            AKK_CLUSTER_CHECK(containsIndex(recovered.detectedErrorIndices, 0));
            AKK_CLUSTER_CHECK(containsIndex(recovered.detectedErrorIndices, 5));
            AKK_CLUSTER_CHECK(containsShard(recovered.repairedShards, shards[0]));
            AKK_CLUSTER_CHECK(containsShard(recovered.repairedShards, shards[5]));
        }

        {
            std::vector<erasure::ErasureShard> insufficient{shards[0], shards[2], shards[4], shards[6]};
            bool rejected = false;
            try {
                const std::array<uint16_t, 1> knownBad{0};
                (void)erasure::ErsCodec::recover(insufficient, layout, knownBad);
            }
            catch (const std::runtime_error&) {
                rejected = true;
            }
            AKK_CLUSTER_CHECK(rejected);
        }

        {
            bool rejected = false;
            try {
                const std::array<uint16_t, 1> knownBad{layout.totalShards()};
                (void)erasure::ErsCodec::recover(shards, layout, knownBad);
            }
            catch (const std::invalid_argument&) {
                rejected = true;
            }
            AKK_CLUSTER_CHECK(rejected);
        }
    }

    void testRaftElectionAndLoopbackReplication() {
        const auto dir = makeTempDir("raft-loopback");
        const ClusterConfig cfg{
            {
                node(1, 20101, 20201),
                node(2, 20102, 20202),
                node(3, 20103, 20203),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 1000},
        };

        auto n1 = makeRuntime(dir / "n1", cfg, 1);
        auto n2 = makeRuntime(dir / "n2", cfg, 2);
        auto n3 = makeRuntime(dir / "n3", cfg, 3);

        n1->runtime->start();
        n2->runtime->start();
        n3->runtime->start();

        std::array<RuntimeHarness*, 3> nodes{n1.get(), n2.get(), n3.get()};
        AKK_CLUSTER_CHECK(waitUntil(
            [&] {
                size_t leaders = 0;
                for (const auto* n : nodes) { if (n->runtime->role() == NodeRole::PRIMARY) { ++leaders; } }
                return leaders == 1;
            },
            std::chrono::milliseconds{8000}
        ));

        RuntimeHarness* leader = nullptr;
        for (auto* n : nodes) {
            if (n->runtime->role() == NodeRole::PRIMARY) {
                leader = n;
                break;
            }
        }
        AKK_CLUSTER_CHECK(leader != nullptr);

        const std::string key = "raft-key";
        const std::string value = "raft-value";
        leader->runtime->shipEntry(1, ReplOpType::PUT, bytesOf(key), bytesOf(value), 7, leader->nodeId);

        AKK_CLUSTER_CHECK(waitUntil(
            [&] {
                size_t followersApplied = 0;
                for (const auto* n : nodes) {
                    if (n != leader && n->hasState(1, key, value)) { ++followersApplied; }
                }
                return followersApplied == 2;
            }
        ));

        RuntimeHarness* follower = leader == n1.get() ? n2.get() : n1.get();
        bool rejected = false;
        try {
            follower->runtime->shipEntry(2, ReplOpType::PUT, bytesOf(key), bytesOf(value), 0, follower->nodeId);
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }
        AKK_CLUSTER_CHECK(rejected);

        n3->runtime->close();
        n2->runtime->close();
        n1->runtime->close();
    }

    void testRaftSnapshotInstallForCompactedFollower() {
        const auto dir = makeTempDir("raft-snapshot-install");
        const ClusterConfig cfg{
            {
                node(1, 20131, 20231),
                node(2, 20132, 20232, static_cast<uint32_t>(NodeCapability::DATA_BEARING)),
                node(3, 20133, 20233),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 250},
        };

        auto n1 = makeRuntime(dir / "n1", cfg, 1);
        auto n2 = makeRuntime(dir / "n2", cfg, 2);
        auto n3 = makeRuntime(dir / "n3", cfg, 3);

        n1->runtime->start();
        n2->runtime->start();
        n3->runtime->start();

        std::array<RuntimeHarness*, 3> nodes{n1.get(), n2.get(), n3.get()};
        AKK_CLUSTER_CHECK(waitUntil(
            [&] {
                size_t leaders = 0;
                for (const auto* n : nodes) { if (n->runtime->role() == NodeRole::PRIMARY) { ++leaders; } }
                return leaders == 1;
            },
            std::chrono::milliseconds{8000}
        ));

        RuntimeHarness* leader = n1->runtime->role() == NodeRole::PRIMARY ? n1.get() : n3.get();
        RuntimeHarness* onlineFollower = leader == n1.get() ? n3.get() : n1.get();

        const std::string key1 = "snapshot-key-1";
        const std::string value1 = "snapshot-value-1";
        leader->runtime->shipEntry(1, ReplOpType::PUT, bytesOf(key1), bytesOf(value1), 0, leader->nodeId);
        AKK_CLUSTER_CHECK(waitUntil([&] { return onlineFollower->hasState(1, key1, value1); }));
        AKK_CLUSTER_CHECK(waitUntil([&] { return n2->hasState(1, key1, value1); }));
        leader->setState(1, bytesOf(key1), bytesOf(value1));

        n2->runtime->close();
        n2.reset();
        std::error_code ec;
        std::filesystem::remove_all(dir / "n2", ec);
        AKK_CLUSTER_CHECK(!ec);
        n2 = makeRuntime(dir / "n2", cfg, 2);
        n2->runtime->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return n2->runtime->role() != NodeRole::PRIMARY; }, std::chrono::milliseconds{1000}));

        const std::string key2 = "snapshot-key-2";
        const std::string value2 = "snapshot-value-2";
        leader->runtime->shipEntry(2, ReplOpType::PUT, bytesOf(key2), bytesOf(value2), 0, leader->nodeId);

        AKK_CLUSTER_CHECK(waitUntil(
            [&] {
                return n2->snapshotsInstalled.load() == 1 && n2->hasState(2, key2, value2);
            },
            std::chrono::milliseconds{8000}
        ));

        n3->runtime->close();
        n2->runtime->close();
        n1->runtime->close();
    }

    void testRaftLeaderTransfer() {
        const auto dir = makeTempDir("raft-leader-transfer");
        const ClusterConfig cfg{
            {
                node(1, 20161, 20261),
                node(2, 20162, 20262),
                node(3, 20163, 20263),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 500},
        };

        auto n1 = makeRuntime(dir / "n1", cfg, 1);
        auto n2 = makeRuntime(dir / "n2", cfg, 2);
        auto n3 = makeRuntime(dir / "n3", cfg, 3);

        n1->runtime->start();
        n2->runtime->start();
        n3->runtime->start();

        std::array<RuntimeHarness*, 3> nodes{n1.get(), n2.get(), n3.get()};
        AKK_CLUSTER_CHECK(waitUntil(
            [&] {
                size_t leaders = 0;
                for (const auto* n : nodes) { if (n->runtime->role() == NodeRole::PRIMARY) { ++leaders; } }
                return leaders == 1;
            },
            std::chrono::milliseconds{8000}
        ));

        RuntimeHarness* leader = findLeader(nodes);
        AKK_CLUSTER_CHECK(leader != nullptr);
        RuntimeHarness* target = nullptr;
        for (auto* n : nodes) {
            if (n != leader) {
                target = n;
                break;
            }
        }
        AKK_CLUSTER_CHECK(target != nullptr);

        leader->runtime->transferRaftLeadership(target->nodeId);
        AKK_CLUSTER_CHECK(waitUntil([&] { return target->runtime->role() == NodeRole::PRIMARY; }, std::chrono::milliseconds{8000}));

        const std::string key = "transfer-key";
        const std::string value = "transfer-value";
        target->runtime->shipEntry(1, ReplOpType::PUT, bytesOf(key), bytesOf(value), 0, target->nodeId);
        AKK_CLUSTER_CHECK(waitUntil(
            [&] {
                size_t applied = 0;
                for (const auto* n : nodes) {
                    if (n != target && n->hasState(1, key, value)) { ++applied; }
                }
                return applied == 2;
            },
            std::chrono::milliseconds{8000}
        ));

        n3->runtime->close();
        n2->runtime->close();
        n1->runtime->close();
    }

    void testRaftOnlineMembershipChange() {
        const auto dir = makeTempDir("raft-online-membership");
        const auto n1Info = node(1, 20141, 20241);
        const auto n2Info = node(2, 20142, 20242);
        const auto n3Info = node(3, 20143, 20243);
        const auto n4Info = node(4, 20144, 20244, static_cast<uint32_t>(NodeCapability::DATA_BEARING));
        const ClusterConfig initialCfg{
            {n1Info, n2Info, n3Info},
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 500},
            onlineRaftMembership(),
        };
        const ClusterConfig joiningCfg{
            {n1Info, n2Info, n3Info, n4Info},
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 500},
            onlineRaftMembership(),
        };

        auto n1 = makeRuntime(dir / "n1", initialCfg, 1);
        auto n2 = makeRuntime(dir / "n2", initialCfg, 2);
        auto n3 = makeRuntime(dir / "n3", initialCfg, 3);
        auto n4 = makeRuntime(dir / "n4", joiningCfg, 4);

        n1->runtime->start();
        n2->runtime->start();
        n3->runtime->start();
        n4->runtime->start();

        std::array<RuntimeHarness*, 3> initialNodes{n1.get(), n2.get(), n3.get()};
        AKK_CLUSTER_CHECK(waitUntil(
            [&] {
                size_t leaders = 0;
                for (const auto* n : initialNodes) { if (n->runtime->role() == NodeRole::PRIMARY) { ++leaders; } }
                return leaders == 1;
            },
            std::chrono::milliseconds{8000}
        ));

        RuntimeHarness* leader = findLeader(initialNodes);
        AKK_CLUSTER_CHECK(leader != nullptr);
        AKK_CLUSTER_CHECK(waitUntil([&] { return n4->runtime->role() != NodeRole::PRIMARY; }, std::chrono::milliseconds{1000}));
        std::this_thread::sleep_for(std::chrono::milliseconds{200});
        leader = findLeader(initialNodes);
        AKK_CLUSTER_CHECK(leader != nullptr);

        leader->runtime->addRaftVotingNode(n4Info);
        AKK_CLUSTER_CHECK(waitUntil([&] { return findLeader(initialNodes) != nullptr; }, std::chrono::milliseconds{3000}));
        leader = findLeader(initialNodes);
        AKK_CLUSTER_CHECK(leader != nullptr);

        const std::string key1 = "membership-key-1";
        const std::string value1 = "membership-value-1";
        leader->runtime->shipEntry(1, ReplOpType::PUT, bytesOf(key1), bytesOf(value1), 0, leader->nodeId);
        AKK_CLUSTER_CHECK(waitUntil([&] { return n4->hasState(1, key1, value1); }, std::chrono::milliseconds{8000}));

        leader = findLeader(initialNodes);
        AKK_CLUSTER_CHECK(leader != nullptr);
        leader->runtime->removeRaftVotingNode(4);
        AKK_CLUSTER_CHECK(waitUntil([&] { return findLeader(initialNodes) != nullptr; }, std::chrono::milliseconds{3000}));
        leader = findLeader(initialNodes);
        AKK_CLUSTER_CHECK(leader != nullptr);

        const std::string key2 = "membership-key-2";
        const std::string value2 = "membership-value-2";
        leader->runtime->shipEntry(2, ReplOpType::PUT, bytesOf(key2), bytesOf(value2), 0, leader->nodeId);
        AKK_CLUSTER_CHECK(waitUntil(
            [&] {
                size_t applied = 0;
                for (const auto* n : initialNodes) {
                    if (n->hasState(2, key2, value2)) { ++applied; }
                }
                return applied == 2;
            },
            std::chrono::milliseconds{8000}
        ));
        std::this_thread::sleep_for(std::chrono::milliseconds{300});
        AKK_CLUSTER_CHECK(!n4->hasState(2, key2, value2));

        n4->runtime->close();
        n3->runtime->close();
        n2->runtime->close();
        n1->runtime->close();
    }

    void testPrimaryAckPathStillUsesLegacyRuntime() {
        constexpr uint16_t port = 20211;
        ClusterRuntimeOptions options;
        options.transportMode = TransportMode::PLAIN;

        const AckPolicy all{.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::APPLIED};
        auto server = ReplicationServer::create(port, 1, [] { return uint64_t{1}; }, all, {}, 0, options);
        std::atomic<uint64_t> lastSeq{0};
        std::atomic<int> applied{0};

        auto client = ReplicationClient::create("127.0.0.1", port, 2, [&] { return lastSeq.load(); }, all, options);
        client->setApplyCallback(
            [&](uint64_t seq, ReplOpType op, std::span<const uint8_t> key, std::span<const uint8_t> value, uint8_t flags, uint64_t source) {
                AKK_CLUSTER_CHECK(seq == 1);
                AKK_CLUSTER_CHECK(op == ReplOpType::PUT);
                AKK_CLUSTER_CHECK(textOf(key) == "legacy-key");
                AKK_CLUSTER_CHECK(textOf(value) == "legacy-value");
                AKK_CLUSTER_CHECK(flags == 3);
                AKK_CLUSTER_CHECK(source == 1);
                lastSeq.store(seq);
                applied.fetch_add(1);
            }
        );

        server->start();
        client->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return server->replicaCount() == 1; }));

        const std::string key = "legacy-key";
        const std::string value = "legacy-value";
        server->shipEntry(1, ReplOpType::PUT, bytesOf(key), bytesOf(value), 3, 1);
        AKK_CLUSTER_CHECK(waitUntil([&] { return applied.load() == 1; }));

        client->close();
        server->close();
    }

    void testRaftMajorityFailureRejectsWrite() {
        const auto dir = makeTempDir("raft-majority-failure");
        const ClusterConfig cfg{
            {
                node(1, 20121, 20221),
                node(2, 20122, 20222),
                node(3, 20123, 20223),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 250},
        };

        auto n1 = makeRuntime(dir / "n1", cfg, 1);
        n1->runtime->start();

        AKK_CLUSTER_CHECK(!waitUntil([&] { return n1->runtime->role() == NodeRole::PRIMARY; }, std::chrono::milliseconds{1200}));

        const std::string key = "no-quorum-key";
        const std::string value = "no-quorum-value";
        bool rejected = false;
        try {
            n1->runtime->shipEntry(1, ReplOpType::PUT, bytesOf(key), bytesOf(value), 0, 1);
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }
        AKK_CLUSTER_CHECK(rejected);

        n1->runtime->close();
    }
}

int main() {
    try {
        akkaradb::test::installMsvcTestErrorHandlers();
        AKK_CLUSTER_CHECK(akkaradb_cluster_register());
        testRaftOptionsRoundtripAndValidation();
        testPartitionedAndStripeRouting();
        testStripeErasureCodecRecovery();
        testRsErasureCodecProperties();
        testErsCodecRecovery();
        testRaftElectionAndLoopbackReplication();
        testRaftSnapshotInstallForCompactedFollower();
        testRaftLeaderTransfer();
        testRaftOnlineMembershipChange();
        testPrimaryAckPathStillUsesLegacyRuntime();
        testRaftMajorityFailureRejectsWrite();
        return 0;
    }
    catch (const std::exception& ex) {
        std::fprintf(stderr, "cluster smoke failed: %s\n", ex.what());
        return 1;
    }
}

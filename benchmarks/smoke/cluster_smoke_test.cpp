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

#include "akk/engine/AkkEngine.hpp"
#include "akk/engine/cluster/ClusterConfig.hpp"
#include "akk/engine/cluster/ClusterManager.hpp"
#include "akk/engine/cluster/ClusterRuntime.hpp"
#include "akk/engine/cluster/ClusterRouter.hpp"
#include "akk/engine/generation/StorageGeneration.hpp"
#include "akk/engine/cluster/ReplFraming.hpp"
#include "akk/engine/cluster/ReplicationClient.hpp"
#include "akk/engine/cluster/ReplicationServer.hpp"
#include "akk/crypto/Identity.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <span>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace akkaradb::engine::cluster;

extern "C" bool akkaradb_cluster_register() noexcept;

namespace {
    std::filesystem::path makeTempDir(const std::string& suffix) {
        const auto dir = std::filesystem::temp_directory_path() / ("akkaradbClusterSmoke_" + suffix);
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
        std::filesystem::create_directories(dir, ec);
        AKK_TEST_CHECK(!ec);
        return dir;
    }

    NodeInfo node(uint64_t id, uint16_t dataPort, uint16_t replPort, uint32_t capabilities) {
        return NodeInfo{
            .nodeId = id,
            .host = "127.0.0.1",
            .dataPort = dataPort,
            .replPort = replPort,
            .capabilities = capabilities,
        };
    }

    std::span<const uint8_t> bytesOf(const std::string& value) {
        return {reinterpret_cast<const uint8_t*>(value.data()), value.size()};
    }

    std::string textOf(const std::vector<uint8_t>& value) {
        return {reinterpret_cast<const char*>(value.data()), value.size()};
    }

    void writeNodeId(const std::filesystem::path& path, uint64_t nodeId) {
        std::filesystem::create_directories(path.parent_path());
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        out.write(reinterpret_cast<const char*>(&nodeId), sizeof(nodeId));
        AKK_TEST_CHECK(out.good());
    }

    void expectThrowLoad(const std::filesystem::path& path) {
        bool threw = false;
        try {
            (void)ClusterConfig::load(path);
        }
        catch (const std::runtime_error&) {
            threw = true;
        }
        AKK_TEST_CHECK(threw);
    }

    void testConfigRoundtripAndRejection() {
        const auto dir = makeTempDir("config");
        const auto path = dir / "cluster.akcc";
        const AckPolicy ack{.mode = AckPolicyMode::QUORUM, .quorum = 2};
        const ConsistencyOptions consistency{
            .mode = ConsistencyMode::PRIMARY_ACK,
            .writeConsistency = WriteConsistency::QUORUM,
            .ackTimeoutAction = AckTimeoutAction::FAIL_WRITE,
            .replicaLagAction = ReplicaLagAction::BLOCK_WRITES,
            .readConsistency = ReadConsistency::PRIMARY,
            .ackTimeoutMs = 1250,
        };
        const ClusterConfig cfg{
            {
                node(1, 0, 19601, static_cast<uint32_t>(NodeCapability::COORDINATOR_ELIGIBLE)),
                node(2, 19702, 19602, static_cast<uint32_t>(NodeCapability::DATA_BEARING)),
                node(3, 19703, 19603, static_cast<uint32_t>(NodeCapability::DATA_BEARING)),
            },
            ReplicationMode::STRIPE,
            ack,
            consistency,
        };

        ClusterConfig::save(path, cfg);
        const auto loaded = ClusterConfig::load(path);
        AKK_TEST_CHECK(loaded.mode() == ReplicationMode::STRIPE);
        AKK_TEST_CHECK(loaded.ackPolicy().mode == AckPolicyMode::QUORUM);
        AKK_TEST_CHECK(loaded.ackPolicy().quorum == 2);
        AKK_TEST_CHECK(loaded.consistency().writeConsistency == WriteConsistency::QUORUM);
        AKK_TEST_CHECK(loaded.consistency().mode == ConsistencyMode::PRIMARY_ACK);
        AKK_TEST_CHECK(loaded.consistency().ackTimeoutAction == AckTimeoutAction::FAIL_WRITE);
        AKK_TEST_CHECK(loaded.consistency().replicaLagAction == ReplicaLagAction::BLOCK_WRITES);
        AKK_TEST_CHECK(loaded.consistency().ackTimeoutMs == 1250);
        AKK_TEST_CHECK(loaded.findById(2) != nullptr);
        AKK_TEST_CHECK(loaded.dataNodes().size() == 2);
        AKK_TEST_CHECK(loaded.coordinatorNodes().size() == 1);

        {
            auto corrupt = path;
            corrupt += ".crc";
            std::filesystem::copy_file(path, corrupt, std::filesystem::copy_options::overwrite_existing);
            std::fstream file(corrupt, std::ios::in | std::ios::out | std::ios::binary);
            file.seekp(40, std::ios::beg);
            char bad = '\x7f';
            file.write(&bad, 1);
            file.close();
            expectThrowLoad(corrupt);
        }

        {
            auto corrupt = path;
            corrupt += ".magic";
            std::filesystem::copy_file(path, corrupt, std::filesystem::copy_options::overwrite_existing);
            std::fstream file(corrupt, std::ios::in | std::ios::out | std::ios::binary);
            char bad = '\0';
            file.write(&bad, 1);
            file.close();
            expectThrowLoad(corrupt);
        }

        bool invalidCapability = false;
        try {
            (void)ClusterConfig{{node(9, 19709, 19609, 0x80)}, ReplicationMode::MIRROR, AckPolicy{}};
        }
        catch (const std::invalid_argument&) {
            invalidCapability = true;
        }
        AKK_TEST_CHECK(invalidCapability);
    }

    void testStorageGenerations() {
        const auto dir = makeTempDir("generations");
        const auto initial = akkaradb::engine::generation::StorageGeneration::openOrCreate(dir);
        AKK_TEST_CHECK(initial.activePath() == dir / "generations" / "gen-1");
        const auto staging = initial.createStaging("snapshot-42");
        AKK_TEST_CHECK(std::filesystem::is_directory(staging));
        initial.activate("snapshot-42");
        const auto reopened = akkaradb::engine::generation::StorageGeneration::openOrCreate(dir);
        AKK_TEST_CHECK(reopened.activePath() == dir / "generations" / "snapshot-42");
    }

    void testRouter() {
        const ClusterConfig mirror{
            {
                node(1, 0, 19611, static_cast<uint32_t>(NodeCapability::COORDINATOR_ELIGIBLE)),
                node(2, 19712, 19612, static_cast<uint32_t>(NodeCapability::DATA_BEARING)),
                node(3, 19713, 19613, static_cast<uint32_t>(NodeCapability::DATA_BEARING)),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
        };
        ClusterRouter mirrorRouter{mirror};
        const std::string key = "customer:42";
        AKK_TEST_CHECK(mirrorRouter.writeTargets(bytesOf(key)).size() == 2);

        const ClusterConfig stripeA{
            {
                node(1, 0, 19621, static_cast<uint32_t>(NodeCapability::COORDINATOR_ELIGIBLE)),
                node(2, 19722, 19622, static_cast<uint32_t>(NodeCapability::DATA_BEARING)),
                node(3, 19723, 19623, static_cast<uint32_t>(NodeCapability::DATA_BEARING)),
            },
            ReplicationMode::STRIPE,
            AckPolicy{},
        };
        const ClusterConfig stripeB{
            {
                node(3, 19723, 19623, static_cast<uint32_t>(NodeCapability::DATA_BEARING)),
                node(1, 0, 19621, static_cast<uint32_t>(NodeCapability::COORDINATOR_ELIGIBLE)),
                node(2, 19722, 19622, static_cast<uint32_t>(NodeCapability::DATA_BEARING)),
            },
            ReplicationMode::STRIPE,
            AckPolicy{},
        };
        ClusterRouter routerA{stripeA};
        ClusterRouter routerB{stripeB};
        const auto targetA = routerA.writeTargets(bytesOf(key));
        const auto targetB = routerB.writeTargets(bytesOf(key));
        AKK_TEST_CHECK(targetA.size() == 1);
        AKK_TEST_CHECK(targetB.size() == 1);
        AKK_TEST_CHECK(targetA[0].nodeId == targetB[0].nodeId);
        AKK_TEST_CHECK(routerA.readCandidates(bytesOf(key))[0].nodeId == targetA[0].nodeId);
    }

    void testFraming() {
        const std::vector<uint8_t> key{'k'};
        const std::vector<uint8_t> value{'v'};
        const auto entryWire = encodeEntry(ReplEntry{
            .seq = 9,
            .sourceNodeId = 1,
            .op = ReplOpType::PUT,
            .recordFlags = 7,
            .key = key,
            .value = value,
        });

        DecodedFrame frame;
        AKK_TEST_CHECK(decodeFrame(entryWire, frame));
        AKK_TEST_CHECK(frame.type == ReplMsgType::ENTRY);
        ReplEntry entry;
        AKK_TEST_CHECK(decodeEntry(frame.payload, entry));
        AKK_TEST_CHECK(entry.seq == 9);
        AKK_TEST_CHECK(entry.recordFlags == 7);
        AKK_TEST_CHECK(entry.key == key);
        AKK_TEST_CHECK(entry.value == value);

        const auto snapshotBeginWire = encodeSnapshotBegin(ReplSnapshotBegin{.snapshotSeq = 9, .entryCount = 1});
        AKK_TEST_CHECK(decodeFrame(snapshotBeginWire, frame));
        ReplSnapshotBegin snapshotBegin;
        AKK_TEST_CHECK(decodeSnapshotBegin(frame.payload, snapshotBegin));
        AKK_TEST_CHECK(snapshotBegin.snapshotSeq == 9);
        const auto snapshotEntryWire = encodeSnapshotEntry(ReplSnapshotEntry{.key = key, .value = value});
        AKK_TEST_CHECK(decodeFrame(snapshotEntryWire, frame));
        ReplSnapshotEntry snapshotEntry;
        AKK_TEST_CHECK(decodeSnapshotEntry(frame.payload, snapshotEntry));
        AKK_TEST_CHECK(snapshotEntry.key == key);
        AKK_TEST_CHECK(snapshotEntry.value == value);

        auto corrupt = entryWire;
        corrupt.back() ^= 0x55;
        AKK_TEST_CHECK(!decodeFrame(corrupt, frame));

        auto truncated = entryWire;
        truncated.pop_back();
        AKK_TEST_CHECK(!decodeFrame(truncated, frame));
    }

    void testManagerElection() {
        const auto dir1 = makeTempDir("manager1");
        const auto dir2 = makeTempDir("manager2");
        const ClusterConfig cfg{
            {
                node(1, 19781, 19801, static_cast<uint32_t>(NodeCapability::COORDINATOR_ELIGIBLE) | static_cast<uint32_t>(NodeCapability::DATA_BEARING)),
                node(2, 19782, 19802, static_cast<uint32_t>(NodeCapability::COORDINATOR_ELIGIBLE) | static_cast<uint32_t>(NodeCapability::DATA_BEARING)),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
        };

        ClusterRuntimeOptions primaryOptions{};
        primaryOptions.startupRole = NodeStartupRole::PRIMARY;
        ClusterRuntimeOptions replicaOptions{};
        replicaOptions.startupRole = NodeStartupRole::REPLICA;
        replicaOptions.primaryNodeId = 1;

        auto primary = ClusterManager::create(dir1, cfg, 1, primaryOptions);
        auto replica = ClusterManager::create(dir2, cfg, 2, replicaOptions);
        std::atomic<int> primaryChanges{0};
        primary->setRoleChangeCallback([&](NodeRole role) {
            if (role == NodeRole::PRIMARY) {
                ++primaryChanges;
            }
        });

        primary->start();
        AKK_TEST_CHECK(primary->role() == NodeRole::PRIMARY);
        replica->start();
        AKK_TEST_CHECK(replica->role() == NodeRole::REPLICA);
        AKK_TEST_CHECK(primaryChanges.load() >= 1);
        AKK_TEST_CHECK(replica->primaryHost() == "127.0.0.1");
        AKK_TEST_CHECK(replica->primaryReplPort() == 19801);

        replica->close();
        primary->close();
        AKK_TEST_CHECK(std::filesystem::exists(dir1 / "cluster.akmf"));
        AKK_TEST_CHECK(std::filesystem::exists(dir2 / "cluster.akmf"));
    }

    void testPlainReplication() {
        constexpr uint16_t port = 19971;
        ClusterRuntimeOptions plain{};
        plain.transportMode = TransportMode::PLAIN;
        const AckPolicy all{.mode = AckPolicyMode::ALL_TARGETS, .quorum = 0};

        auto server = ReplicationServer::create(port, 1, [] { return uint64_t{1}; }, all, {}, 0, plain);
        std::atomic<int> applied{0};
        std::atomic<int> blobs{0};

        auto client = ReplicationClient::create("127.0.0.1", port, 2, [] { return uint64_t{0}; }, all, plain);
        client->setApplyCallback([&](uint64_t seq, ReplOpType op, std::span<const uint8_t> key, std::span<const uint8_t> value, uint8_t flags, uint64_t source) {
            AKK_TEST_CHECK(seq == 1);
            AKK_TEST_CHECK(op == ReplOpType::PUT);
            AKK_TEST_CHECK(source == 1);
            AKK_TEST_CHECK(flags == 3);
            AKK_TEST_CHECK(std::string(reinterpret_cast<const char*>(key.data()), key.size()) == "k");
            AKK_TEST_CHECK(std::string(reinterpret_cast<const char*>(value.data()), value.size()) == "v");
            ++applied;
        });
        client->setBlobCallback([&](uint64_t seq, uint64_t blobId, std::span<const uint8_t> content) {
            AKK_TEST_CHECK(seq == 1);
            AKK_TEST_CHECK(blobId == 99);
            AKK_TEST_CHECK(std::string(reinterpret_cast<const char*>(content.data()), content.size()) == "blob");
            ++blobs;
        });

        server->start();
        client->start();

        for (int i = 0; i < 50 && server->replicaCount() == 0; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        AKK_TEST_CHECK(server->replicaCount() == 1);

        const std::string key = "k";
        const std::string value = "v";
        const std::string blob = "blob";
        server->shipBlob(1, 99, bytesOf(blob));
        server->shipEntry(1, ReplOpType::PUT, bytesOf(key), bytesOf(value), 3, 1);

        for (int i = 0; i < 50 && (applied.load() == 0 || blobs.load() == 0); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        AKK_TEST_CHECK(applied.load() == 1);
        AKK_TEST_CHECK(blobs.load() == 1);

        client->close();
        server->close();
    }

    void testTransportDefault() {
        ClusterRuntimeOptions options{};
        AKK_TEST_CHECK(options.transportMode == TransportMode::SECURE);
        AKK_TEST_CHECK(options.replBindHost == "0.0.0.0");

        constexpr uint16_t port = 19972;
        const AckPolicy all{.mode = AckPolicyMode::ALL_TARGETS, .quorum = 0};
        auto server = ReplicationServer::create(port, 1, [] { return uint64_t{1}; }, all, {}, 0, options);
        std::atomic<int> applied{0};

        auto client = ReplicationClient::create("127.0.0.1", port, 2, [] { return uint64_t{0}; }, all, options);
        client->setApplyCallback([&](uint64_t seq, ReplOpType op, std::span<const uint8_t> key, std::span<const uint8_t> value, uint8_t flags, uint64_t source) {
            AKK_TEST_CHECK(seq == 1);
            AKK_TEST_CHECK(op == ReplOpType::PUT);
            AKK_TEST_CHECK(source == 1);
            AKK_TEST_CHECK(flags == 4);
            AKK_TEST_CHECK(std::string(reinterpret_cast<const char*>(key.data()), key.size()) == "secure-k");
            AKK_TEST_CHECK(std::string(reinterpret_cast<const char*>(value.data()), value.size()) == "secure-v");
            ++applied;
        });

        server->start();
        client->start();

        for (int i = 0; i < 50 && server->replicaCount() == 0; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        AKK_TEST_CHECK(server->replicaCount() == 1);

        const std::string key = "secure-k";
        const std::string value = "secure-v";
        server->shipEntry(1, ReplOpType::PUT, bytesOf(key), bytesOf(value), 4, 1);

        for (int i = 0; i < 50 && applied.load() == 0; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        AKK_TEST_CHECK(applied.load() == 1);

        client->close();
        server->close();
    }

    void testSnapshotFallback() {
        constexpr uint16_t port = 19975;
        ClusterRuntimeOptions plain{};
        plain.transportMode = TransportMode::PLAIN;
        const AckPolicy none{};
        auto server = ReplicationServer::create(
            port,
            1,
            [] { return uint64_t{2}; },
            none,
            {},
            0,
            plain,
            [](uint64_t, uint64_t) -> std::optional<std::vector<ReplEntry>> { return std::nullopt; },
            []() -> std::optional<ReplicationServer::Snapshot> {
                ReplicationServer::Snapshot snapshot;
                snapshot.seq = 2;
                snapshot.entries.push_back(ReplSnapshotEntry{.key = {'s'}, .value = {'v'}});
                return snapshot;
            }
        );
        std::atomic<int> begins{0};
        std::atomic<int> entries{0};
        std::atomic<int> ends{0};
        auto client = ReplicationClient::create("127.0.0.1", port, 2, [] { return uint64_t{0}; }, none, plain);
        client->setSnapshotCallbacks(
            [&](uint64_t seq, uint64_t count) { AKK_TEST_CHECK(seq == 2 && count == 1); ++begins; },
            [&](std::span<const uint8_t> key, std::span<const uint8_t> value) {
                AKK_TEST_CHECK(key.size() == 1 && key[0] == 's' && value.size() == 1 && value[0] == 'v'); ++entries;
            },
            [&](uint64_t seq) { AKK_TEST_CHECK(seq == 2); ++ends; }
        );
        server->start();
        client->start();
        for (int i = 0; i < 50 && ends.load() == 0; ++i) { std::this_thread::sleep_for(std::chrono::milliseconds(20)); }
        AKK_TEST_CHECK(begins.load() == 1 && entries.load() == 1 && ends.load() == 1);
        client->close();
        server->close();
    }

    void testReplicaLagPolicies() {
        ClusterRuntimeOptions plain{};
        plain.transportMode = TransportMode::PLAIN;
        const AckPolicy none{};

        {
            constexpr uint16_t port = 19976;
            std::atomic<int> historyCalls{0};
            std::atomic<int> snapshotCalls{0};
            auto server = ReplicationServer::create(
                port,
                1,
                [] { return uint64_t{2}; },
                none,
                ConsistencyOptions{.replicaLagAction = ReplicaLagAction::REJECT_REPLICA},
                0,
                plain,
                [&](uint64_t, uint64_t) -> std::optional<std::vector<ReplEntry>> { ++historyCalls; return std::nullopt; },
                [&]() -> std::optional<ReplicationServer::Snapshot> { ++snapshotCalls; return ReplicationServer::Snapshot{}; }
            );
            auto client = ReplicationClient::create("127.0.0.1", port, 2, [] { return uint64_t{0}; }, none, plain);
            server->start();
            client->start();
            for (int i = 0; i < 50 && historyCalls.load() == 0; ++i) { std::this_thread::sleep_for(std::chrono::milliseconds(20)); }
            AKK_TEST_CHECK(historyCalls.load() == 1);
            AKK_TEST_CHECK(snapshotCalls.load() == 0);
            for (int i = 0; i < 50 && server->replicaCount() != 0; ++i) { std::this_thread::sleep_for(std::chrono::milliseconds(20)); }
            AKK_TEST_CHECK(server->replicaCount() == 0);
            client->close();
            server->close();
        }

        {
            constexpr uint16_t port = 19977;
            std::atomic<int> historyCalls{0};
            auto server = ReplicationServer::create(
                port,
                1,
                [] { return uint64_t{2}; },
                none,
                ConsistencyOptions{.replicaLagAction = ReplicaLagAction::BLOCK_WRITES},
                0,
                plain,
                [&](uint64_t, uint64_t) -> std::optional<std::vector<ReplEntry>> { ++historyCalls; return std::nullopt; }
            );
            auto client = ReplicationClient::create("127.0.0.1", port, 2, [] { return uint64_t{0}; }, none, plain);
            server->start();
            client->start();
            for (int i = 0; i < 50 && historyCalls.load() == 0; ++i) { std::this_thread::sleep_for(std::chrono::milliseconds(20)); }
            AKK_TEST_CHECK(historyCalls.load() == 1);

            const std::string key = "lag-block-k";
            const std::string value = "lag-block-v";
            bool blocked = false;
            try {
                server->shipEntry(3, ReplOpType::PUT, bytesOf(key), bytesOf(value), 0, 1);
            }
            catch (const std::runtime_error&) {
                blocked = true;
            }
            AKK_TEST_CHECK(blocked);
            client->close();
            server->close();
        }
    }

    void testEngineSnapshotResync() {
        const auto dir = makeTempDir("engineSnapshotResync");
        const auto primaryDir = dir / "primary";
        const auto replicaDir = dir / "replica";
        writeNodeId(primaryDir / "node.id", 1);
        writeNodeId(replicaDir / "node.id", 2);

        const ClusterConfig cfg{
            {
                node(1, 19841, 19978, static_cast<uint32_t>(NodeCapability::COORDINATOR_ELIGIBLE) | static_cast<uint32_t>(NodeCapability::DATA_BEARING)),
                node(2, 19842, 19979, static_cast<uint32_t>(NodeCapability::DATA_BEARING)),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.replicaLagAction = ReplicaLagAction::ASYNC_RESYNC},
        };

        akkaradb::engine::AkkEngineOptions primaryOptions;
        primaryOptions.paths.dataDir = primaryDir;
        primaryOptions.components.walEnabled = false;
        primaryOptions.components.blobEnabled = false;
        primaryOptions.components.manifestEnabled = false;
        primaryOptions.components.sstEnabled = false;
        primaryOptions.components.clusterEnabled = true;
        primaryOptions.cluster.config = cfg;
        primaryOptions.cluster.runtime.transportMode = TransportMode::PLAIN;
        primaryOptions.cluster.runtime.startupRole = NodeStartupRole::PRIMARY;

        auto primary = akkaradb::engine::AkkEngine::open(primaryOptions);
        const std::string key = "snapshot-engine-k";
        const std::string value = "snapshot-engine-v";
        primary->put(bytesOf(key), bytesOf(value));

        akkaradb::engine::AkkEngineOptions replicaOptions;
        replicaOptions.paths.dataDir = replicaDir;
        replicaOptions.components.walEnabled = false;
        replicaOptions.components.blobEnabled = false;
        replicaOptions.components.manifestEnabled = false;
        replicaOptions.components.sstEnabled = false;
        replicaOptions.components.clusterEnabled = true;
        replicaOptions.cluster.config = cfg;
        replicaOptions.cluster.runtime.transportMode = TransportMode::PLAIN;
        replicaOptions.cluster.runtime.startupRole = NodeStartupRole::REPLICA;
        replicaOptions.cluster.runtime.primaryNodeId = 1;

        auto replica = akkaradb::engine::AkkEngine::open(replicaOptions);
        std::optional<std::vector<uint8_t>> replicated;
        for (int i = 0; i < 100 && !replicated; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            replicated = replica->get(bytesOf(key));
        }
        AKK_TEST_CHECK(replicated.has_value());
        AKK_TEST_CHECK(textOf(*replicated) == value);

        replica->close();
        primary->close();
    }

    void testWriteConsistencyTimeout() {
        ClusterRuntimeOptions plain{};
        plain.transportMode = TransportMode::PLAIN;
        const AckPolicy all{.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::APPLIED};
        const ConsistencyOptions consistency{
            .writeConsistency = WriteConsistency::ALL_CONFIGURED,
            .ackTimeoutAction = AckTimeoutAction::FAIL_WRITE,
            .ackTimeoutMs = 25,
        };
        auto server = ReplicationServer::create(0, 1, [] { return uint64_t{1}; }, all, consistency, 1, plain);
        const std::string key = "consistency-k";
        const std::string value = "consistency-v";
        bool timedOut = false;
        try {
            server->shipEntry(1, ReplOpType::PUT, bytesOf(key), bytesOf(value), 0, 1);
        }
        catch (const std::runtime_error&) {
            timedOut = true;
        }
        AKK_TEST_CHECK(timedOut);
    }

    void testRaftQuorumReplication() {
        const auto dir = makeTempDir("raftQuorum");
        const auto primaryDir = dir / "primary";
        const auto replicaDir = dir / "replica";
        const ClusterConfig cfg{
            {
                node(1, 19821, 19980, static_cast<uint32_t>(NodeCapability::COORDINATOR_ELIGIBLE) | static_cast<uint32_t>(NodeCapability::DATA_BEARING)),
                node(2, 19822, 19981, static_cast<uint32_t>(NodeCapability::DATA_BEARING)),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{
                .mode = ConsistencyMode::RAFT_QUORUM,
                .replicaLagAction = ReplicaLagAction::BLOCK_WRITES,
                .ackTimeoutMs = 1000,
            },
        };

        ClusterRuntimeOptions primaryOptions{};
        primaryOptions.transportMode = TransportMode::PLAIN;
        primaryOptions.startupRole = NodeStartupRole::PRIMARY;
        ClusterEngineCallbacks primaryCallbacks;
        std::atomic<uint64_t> primarySeq{0};
        primaryCallbacks.getCurrentSeq = [&] { return primarySeq.load(); };

        ClusterRuntimeOptions replicaOptions{};
        replicaOptions.transportMode = TransportMode::PLAIN;
        replicaOptions.startupRole = NodeStartupRole::REPLICA;
        replicaOptions.primaryNodeId = 1;
        ClusterEngineCallbacks replicaCallbacks;
        std::atomic<uint64_t> replicaSeq{0};
        std::atomic<int> applied{0};
        std::atomic<int> forcedDurable{0};
        replicaCallbacks.getLastSeq = [&] { return replicaSeq.load(); };
        replicaCallbacks.apply = [&](
            uint64_t seq,
            ReplOpType op,
            std::span<const uint8_t> key,
            std::span<const uint8_t> value,
            uint8_t flags,
            uint64_t source
        ) {
            AKK_TEST_CHECK(seq == 1);
            AKK_TEST_CHECK(op == ReplOpType::PUT);
            AKK_TEST_CHECK(source == 1);
            AKK_TEST_CHECK(flags == 6);
            AKK_TEST_CHECK(std::string(reinterpret_cast<const char*>(key.data()), key.size()) == "raft-quorum-k");
            AKK_TEST_CHECK(std::string(reinterpret_cast<const char*>(value.data()), value.size()) == "raft-quorum-v");
            replicaSeq.store(seq);
            ++applied;
        };
        replicaCallbacks.forceDurable = [&] { ++forcedDurable; };

        auto primary = ClusterRuntime::create(primaryDir, cfg, 1, std::move(primaryCallbacks), primaryOptions);
        auto replica = ClusterRuntime::create(replicaDir, cfg, 2, std::move(replicaCallbacks), replicaOptions);
        primary->start();
        replica->start();

        for (int i = 0; i < 100 && primary->role() != NodeRole::PRIMARY; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }
        AKK_TEST_CHECK(primary->role() == NodeRole::PRIMARY);

        const std::string key = "raft-quorum-k";
        const std::string value = "raft-quorum-v";
        primarySeq.store(1);
        primary->shipEntry(1, ReplOpType::PUT, bytesOf(key), bytesOf(value), 6, 1);

        for (int i = 0; i < 100 && applied.load() == 0; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        AKK_TEST_CHECK(applied.load() == 1);
        AKK_TEST_CHECK(forcedDurable.load() == 1);

        replica->close();
        primary->close();
    }

    void testEngineRaftQuorumCoordinatorSingleNode() {
        const auto dir = makeTempDir("engineRaftCoordinator");
        writeNodeId(dir / "node.id", 1);
        const ClusterConfig cfg{
            {
                node(1, 19823, 0, static_cast<uint32_t>(NodeCapability::COORDINATOR_ELIGIBLE) | static_cast<uint32_t>(NodeCapability::DATA_BEARING)),
            },
            ReplicationMode::STANDALONE,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM},
        };

        akkaradb::engine::AkkEngineOptions options;
        options.paths.dataDir = dir;
        options.components.walEnabled = false;
        options.components.blobEnabled = false;
        options.components.manifestEnabled = false;
        options.components.sstEnabled = false;
        options.components.clusterEnabled = true;
        options.cluster.config = cfg;
        options.cluster.runtime.transportMode = TransportMode::PLAIN;

        auto engine = akkaradb::engine::AkkEngine::open(options);
        const std::string key = "engine-raft-coordinator-k";
        const std::string value = "engine-raft-coordinator-v";
        engine->put(bytesOf(key), bytesOf(value));
        const auto stored = engine->get(bytesOf(key));
        AKK_TEST_CHECK(stored.has_value());
        AKK_TEST_CHECK(textOf(*stored) == value);
        engine->close();
    }

    void testEngineRaftQuorumDoesNotApplyFailedProposal() {
        const auto dir = makeTempDir("engineRaftFailedProposal");
        writeNodeId(dir / "node.id", 1);
        const ClusterConfig cfg{
            {
                node(1, 19824, 19982, static_cast<uint32_t>(NodeCapability::COORDINATOR_ELIGIBLE) | static_cast<uint32_t>(NodeCapability::DATA_BEARING)),
                node(2, 19825, 19983, static_cast<uint32_t>(NodeCapability::DATA_BEARING)),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{
                .mode = ConsistencyMode::RAFT_QUORUM,
                .replicaLagAction = ReplicaLagAction::BLOCK_WRITES,
                .ackTimeoutMs = 25,
            },
        };

        akkaradb::engine::AkkEngineOptions options;
        options.paths.dataDir = dir;
        options.components.walEnabled = false;
        options.components.blobEnabled = false;
        options.components.manifestEnabled = false;
        options.components.sstEnabled = false;
        options.components.clusterEnabled = true;
        options.cluster.config = cfg;
        options.cluster.runtime.transportMode = TransportMode::PLAIN;
        options.cluster.runtime.startupRole = NodeStartupRole::PRIMARY;

        auto engine = akkaradb::engine::AkkEngine::open(options);
        const std::string key = "engine-raft-failed-k";
        const std::string value = "engine-raft-failed-v";
        bool failed = false;
        try {
            engine->put(bytesOf(key), bytesOf(value));
        }
        catch (const std::runtime_error&) {
            failed = true;
        }
        AKK_TEST_CHECK(failed);
        AKK_TEST_CHECK(!engine->get(bytesOf(key)).has_value());
        AKK_TEST_CHECK(std::filesystem::is_regular_file(dir / "raft.log"));
        engine->close();
    }

    void testPinnedSecureReplication() {
        const auto dir = makeTempDir("securePinned");
        constexpr uint16_t port = 19973;
        const auto serverSeed = dir / "server.identity";
        const auto clientSeed = dir / "client.identity";
        const auto serverIdentity = akkaradb::crypto::IdentityStore{serverSeed}.loadOrCreate();
        const auto clientIdentity = akkaradb::crypto::IdentityStore{clientSeed}.loadOrCreate();

        ClusterRuntimeOptions serverOptions{};
        serverOptions.secure.identitySeedPath = serverSeed;
        serverOptions.secure.pinnedPeers.push_back(ClusterPeerPublicKeyPin{.nodeId = 2, .publicKey = clientIdentity.publicKey});

        ClusterRuntimeOptions clientOptions{};
        clientOptions.secure.identitySeedPath = clientSeed;
        clientOptions.secure.expectedPrimaryNodeId = 1;
        clientOptions.secure.pinnedPeers.push_back(ClusterPeerPublicKeyPin{.nodeId = 1, .publicKey = serverIdentity.publicKey});

        const AckPolicy all{.mode = AckPolicyMode::ALL_TARGETS, .quorum = 0};
        auto server = ReplicationServer::create(port, 1, [] { return uint64_t{1}; }, all, {}, 0, serverOptions);
        std::atomic<int> applied{0};

        auto client = ReplicationClient::create("127.0.0.1", port, 2, [] { return uint64_t{0}; }, all, clientOptions);
        client->setApplyCallback([&](uint64_t seq, ReplOpType op, std::span<const uint8_t> key, std::span<const uint8_t> value, uint8_t flags, uint64_t source) {
            AKK_TEST_CHECK(seq == 1);
            AKK_TEST_CHECK(op == ReplOpType::PUT);
            AKK_TEST_CHECK(source == 1);
            AKK_TEST_CHECK(flags == 5);
            AKK_TEST_CHECK(std::string(reinterpret_cast<const char*>(key.data()), key.size()) == "pin-k");
            AKK_TEST_CHECK(std::string(reinterpret_cast<const char*>(value.data()), value.size()) == "pin-v");
            ++applied;
        });

        server->start();
        client->start();

        for (int i = 0; i < 50 && server->replicaCount() == 0; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        AKK_TEST_CHECK(server->replicaCount() == 1);

        const std::string key = "pin-k";
        const std::string value = "pin-v";
        server->shipEntry(1, ReplOpType::PUT, bytesOf(key), bytesOf(value), 5, 1);

        for (int i = 0; i < 50 && applied.load() == 0; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        AKK_TEST_CHECK(applied.load() == 1);

        client->close();
        server->close();
    }

    void testPlainTransportRejectsWanHosts() {
        const auto dir = makeTempDir("plainWanReject");
        const ClusterConfig cfg{
            {
                NodeInfo{
                    .nodeId = 1,
                    .host = "203.0.113.10",
                    .dataPort = 19791,
                    .replPort = 19811,
                    .capabilities = static_cast<uint32_t>(NodeCapability::COORDINATOR_ELIGIBLE) | static_cast<uint32_t>(NodeCapability::DATA_BEARING),
                },
                node(2, 19792, 19812, static_cast<uint32_t>(NodeCapability::DATA_BEARING)),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
        };

        ClusterRuntimeOptions plain{};
        plain.transportMode = TransportMode::PLAIN;

        bool rejected = false;
        try {
            ClusterEngineCallbacks callbacks;
            (void)ClusterRuntime::create(dir, cfg, 1, std::move(callbacks), plain);
        }
        catch (const std::invalid_argument&) {
            rejected = true;
        }
        AKK_TEST_CHECK(rejected);
    }

    void testRuntimeAcceptsStripeRouting() {
        const auto dir = makeTempDir("stripeRuntimeAccept");
        const ClusterConfig cfg{
            {
                node(1, 19793, 19813, static_cast<uint32_t>(NodeCapability::COORDINATOR_ELIGIBLE) | static_cast<uint32_t>(NodeCapability::DATA_BEARING)),
                node(2, 19794, 19814, static_cast<uint32_t>(NodeCapability::DATA_BEARING)),
            },
            ReplicationMode::STRIPE,
            AckPolicy{},
        };

        ClusterEngineCallbacks callbacks;
        auto runtime = ClusterRuntime::create(dir, cfg, 1, std::move(callbacks), ClusterRuntimeOptions{});
        const std::string key = "stripe-key";
        const auto targets = runtime->router().writeTargets(bytesOf(key));
        AKK_TEST_CHECK(targets.size() == 1);
        AKK_TEST_CHECK(runtime->router().readCandidates(bytesOf(key))[0].nodeId == targets[0].nodeId);
        runtime->close();
    }
} // namespace

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();
    AKK_TEST_CHECK(akkaradb_cluster_register());

    testConfigRoundtripAndRejection();
    testStorageGenerations();
    testRouter();
    testFraming();
    testManagerElection();
    testPlainReplication();
    testTransportDefault();
    testSnapshotFallback();
    testReplicaLagPolicies();
    testEngineSnapshotResync();
    testWriteConsistencyTimeout();
    testRaftQuorumReplication();
    testEngineRaftQuorumCoordinatorSingleNode();
    testEngineRaftQuorumDoesNotApplyFailedProposal();
    testPinnedSecureReplication();
    testPlainTransportRejectsWanHosts();
    testRuntimeAcceptsStripeRouting();
    std::printf("cluster smoke test passed\n");
    return 0;
}

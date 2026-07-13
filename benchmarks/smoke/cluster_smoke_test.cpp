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
#include "akk/engine/cluster/ReplFraming.hpp"
#include "akk/engine/cluster/ReplicationClient.hpp"
#include "akk/engine/cluster/ReplicationServer.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace akkaradb::engine::cluster;

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

/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#include "TestErrorHandlers.hpp"

#include "akk/engine/AkkEngine.hpp"
#include "akk/engine/cluster/ClusterRuntime.hpp"
#include "akk/engine/cluster/detail/ClusterHealth.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

using namespace akkaradb::engine;
using namespace akkaradb::engine::cluster;

extern "C" bool akkaradb_cluster_register() noexcept;

namespace {
    constexpr uint32_t DATA_AND_COORDINATOR =
        static_cast<uint32_t>(NodeCapability::DATA_BEARING) |
        static_cast<uint32_t>(NodeCapability::COORDINATOR_ELIGIBLE);

    [[noreturn]] void fail(const char* expression, const char* file, int line) {
        std::fprintf(stderr, "check failed: %s at %s:%d\n", expression, file, line);
        throw std::runtime_error(std::string{"check failed: "} + expression);
    }

#define AKK_OBSERVABILITY_CHECK(expression) \
    do { if (!(expression)) { fail(#expression, __FILE__, __LINE__); } } while (false)

    template <typename Predicate>
    bool waitUntil(Predicate predicate, std::chrono::milliseconds timeout = std::chrono::milliseconds{8'000}) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (predicate()) { return true; }
            std::this_thread::sleep_for(std::chrono::milliseconds{20});
        }
        return predicate();
    }

    std::span<const uint8_t> bytesOf(const std::string& value) {
        return {reinterpret_cast<const uint8_t*>(value.data()), value.size()};
    }

    std::string textOf(std::span<const uint8_t> value) {
        return {reinterpret_cast<const char*>(value.data()), value.size()};
    }

    class TempDirectory {
        public:
            explicit TempDirectory(std::string_view label) {
                static std::atomic<uint64_t> serial{0};
                const auto nonce = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
                path_ = std::filesystem::temp_directory_path() /
                    ("akkaradb_observability_" + std::string{label} + "_" + std::to_string(nonce) + "_" +
                     std::to_string(serial.fetch_add(1)));
                std::error_code error;
                std::filesystem::create_directories(path_, error);
                AKK_OBSERVABILITY_CHECK(!error);
            }

            ~TempDirectory() {
                std::error_code error;
                std::filesystem::remove_all(path_, error);
            }

            [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

        private:
            std::filesystem::path path_;
    };

    uint16_t unusedLoopbackPort() {
#ifdef _WIN32
        static std::once_flag winsockOnce;
        std::call_once(winsockOnce, [] {
            WSADATA data{};
            AKK_OBSERVABILITY_CHECK(::WSAStartup(MAKEWORD(2, 2), &data) == 0);
        });
#endif
        // Ephemeral client ports can occupy the adjacent partition listener
        // after a port-zero probe closes. Use distinct pairs below that range.
        static std::atomic<uint32_t> nextPort{35'000};
        for (;;) {
            const auto candidate = nextPort.fetch_add(2);
            if (candidate > 45'000) { throw std::runtime_error("No free observability test port pair"); }
            const auto open = [] {
#ifdef _WIN32
                return ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
#else
                return ::socket(AF_INET, SOCK_STREAM, 0);
#endif
            };
            const auto first = open(), second = open();
#ifdef _WIN32
            AKK_OBSERVABILITY_CHECK(first != INVALID_SOCKET && second != INVALID_SOCKET);
#else
            AKK_OBSERVABILITY_CHECK(first >= 0 && second >= 0);
#endif
            sockaddr_in address{}; address.sin_family = AF_INET; address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            address.sin_port = htons(static_cast<uint16_t>(candidate));
            const bool freeFirst = ::bind(first, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) == 0;
            address.sin_port = htons(static_cast<uint16_t>(candidate + 1));
            const bool freeSecond = ::bind(second, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) == 0;
#ifdef _WIN32
            ::closesocket(first); ::closesocket(second);
#else
            ::close(first); ::close(second);
#endif
            if (freeFirst && freeSecond) { return static_cast<uint16_t>(candidate); }
        }
    }

    NodeInfo makeNode(uint64_t nodeId) {
        const uint16_t replPort = unusedLoopbackPort();
        return NodeInfo{
            .nodeId = nodeId,
            .host = "127.0.0.1",
            .dataPort = unusedLoopbackPort(),
            .replPort = replPort,
            .stripeMetadataPort = unusedLoopbackPort(),
            .capabilities = DATA_AND_COORDINATOR,
        };
    }

    void enableDeterministicRaftElection() {
#ifdef _WIN32
        AKK_OBSERVABILITY_CHECK(_putenv_s("AKKARADB_TEST_DETERMINISTIC_RAFT_ELECTION", "1") == 0);
#else
        AKK_OBSERVABILITY_CHECK(::setenv("AKKARADB_TEST_DETERMINISTIC_RAFT_ELECTION", "1", 1) == 0);
#endif
    }

    void writeNodeId(const std::filesystem::path& path, uint64_t nodeId) {
        std::filesystem::create_directories(path.parent_path());
        std::ofstream output{path, std::ios::binary | std::ios::trunc};
        AKK_OBSERVABILITY_CHECK(output.good());
        output.write(reinterpret_cast<const char*>(&nodeId), sizeof(nodeId));
        AKK_OBSERVABILITY_CHECK(output.good());
    }

    void validateHistogram(const ClusterLatencyHistogram& histogram) {
        AKK_OBSERVABILITY_CHECK(std::ranges::is_sorted(histogram.bucketUpperBoundsUs));
        AKK_OBSERVABILITY_CHECK(std::adjacent_find(
            histogram.bucketUpperBoundsUs.begin(),
            histogram.bucketUpperBoundsUs.end(),
            std::greater_equal<>{}
        ) == histogram.bucketUpperBoundsUs.end());
        AKK_OBSERVABILITY_CHECK(histogram.bucketUpperBoundsUs.back() == std::numeric_limits<uint64_t>::max());
        AKK_OBSERVABILITY_CHECK(std::ranges::is_sorted(histogram.bucketCounts));
        AKK_OBSERVABILITY_CHECK(histogram.bucketCounts.back() == histogram.sampleCount);
        if (histogram.sampleCount != 0) {
            AKK_OBSERVABILITY_CHECK(histogram.totalUs >= histogram.maxUs);
        }
    }

    const RaftPeerStats& peerById(const RaftRuntimeStats& stats, uint64_t nodeId) {
        const auto peer = std::ranges::find(stats.peers, nodeId, &RaftPeerStats::nodeId);
        AKK_OBSERVABILITY_CHECK(peer != stats.peers.end());
        return *peer;
    }

    struct RuntimeNode {
        uint64_t nodeId = 0;
        std::atomic<uint64_t> lastSeq{0};
        mutable std::mutex valuesMutex;
        std::map<std::string, std::string> values;
        std::unique_ptr<ClusterRuntime> runtime;

        ~RuntimeNode() {
            try { if (runtime) { runtime->close(); } }
            catch (...) {}
        }

        void putLocal(uint64_t seq, std::string key, std::string value) {
            std::lock_guard lock{valuesMutex};
            values[std::move(key)] = std::move(value);
            lastSeq.store(std::max(lastSeq.load(), seq));
        }
    };

    std::unique_ptr<RuntimeNode> makeRuntimeNode(
        const std::filesystem::path& directory,
        const ClusterConfig& config,
        uint64_t nodeId,
        ClusterRuntimeOptions options
    ) {
        auto node = std::make_unique<RuntimeNode>();
        node->nodeId = nodeId;
        ClusterEngineCallbacks callbacks;
        callbacks.getCurrentSeq = [node = node.get()] { return node->lastSeq.load(); };
        callbacks.getLastSeq = [node = node.get()] { return node->lastSeq.load(); };
        callbacks.checkpointReplication = [node = node.get()] { return node->lastSeq.load(); };
        callbacks.apply = [node = node.get()](
            uint64_t seq,
            ReplOpType operation,
            std::span<const uint8_t> key,
            std::span<const uint8_t> value,
            uint8_t,
            uint64_t
        , uint64_t) {
            std::lock_guard lock{node->valuesMutex};
            const std::string decodedKey = textOf(key);
            if (operation == ReplOpType::REMOVE) { node->values.erase(decodedKey); }
            else { node->values[decodedKey] = textOf(value); }
            node->lastSeq.store(std::max(node->lastSeq.load(), seq));
        };
        callbacks.read = [node = node.get()](std::span<const uint8_t> key, uint64_t) {
            std::lock_guard lock{node->valuesMutex};
            ReadResponse response;
            const auto value = node->values.find(textOf(key));
            if (value == node->values.end()) {
                response.status = ReadStatus::NOT_FOUND;
                return response;
            }
            response.status = ReadStatus::FOUND;
            response.seq = node->lastSeq.load();
            response.value.assign(value->second.begin(), value->second.end());
            return response;
        };
        callbacks.readLocalStorage = callbacks.read;
        callbacks.forceDurable = [] {};
        node->runtime = ClusterRuntime::create(directory, config, nodeId, std::move(callbacks), std::move(options));
        return node;
    }

    RuntimeNode* findLeader(std::span<RuntimeNode* const> nodes) {
        RuntimeNode* leader = nullptr;
        for (auto* node : nodes) {
            if (node->runtime->role() != NodeRole::PRIMARY) { continue; }
            if (leader != nullptr) { return nullptr; }
            leader = node;
        }
        return leader;
    }

    void testDefaultHistogramContract() {
        ClusterLatencyHistogram histogram;
        validateHistogram(histogram);
        AKK_OBSERVABILITY_CHECK(histogram.sampleCount == 0);
        AKK_OBSERVABILITY_CHECK(histogram.totalUs == 0);
        AKK_OBSERVABILITY_CHECK(histogram.maxUs == 0);
    }

    void testDisabledClusterSnapshotContract() {
        TempDirectory directory{"disabled"};
        AkkEngineOptions options;
        options.paths.dataDir = directory.path();
        options.components.clusterEnabled = false;
        auto engine = AkkEngine::open(options);
        const auto stats = engine->stats().cluster;
        AKK_OBSERVABILITY_CHECK(!stats.enabled);
        AKK_OBSERVABILITY_CHECK(stats.sampledAtUs == 0 && stats.runtimeStartedAtUs == 0);
        AKK_OBSERVABILITY_CHECK(stats.configuredNodeCount == 0);
        AKK_OBSERVABILITY_CHECK(stats.configuredNodes.empty() && stats.peers.empty());
        AKK_OBSERVABILITY_CHECK(stats.writeAvailability == ClusterOperationAvailability::UNKNOWN);
        engine->close();
        AKK_OBSERVABILITY_CHECK(!engine->stats().cluster.enabled);
    }

    void testRuntimeSnapshotLifecycle() {
        TempDirectory directory{"lifecycle"};
        const ClusterConfig config{
            {makeNode(21), makeNode(22)}, ReplicationMode::PARTITIONED, {},
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 100}
        };
        ClusterRuntimeOptions options;
        options.transportMode = TransportMode::PLAIN;
        options.clusterGroupId = 0xA44A'0002;
        options.clusterGroupEpoch = 3;
        auto node = makeRuntimeNode(directory.path() / "n21", config, 21, options);

        const auto beforeStart = node->runtime->raftStats();
        AKK_OBSERVABILITY_CHECK(beforeStart.sampledAtUs != 0);
        AKK_OBSERVABILITY_CHECK(beforeStart.runtimeStartedAtUs == 0);
        AKK_OBSERVABILITY_CHECK(peerById(beforeStart, 22).roundTripsSucceeded == 0);
        node->runtime->start();
        const auto firstStart = node->runtime->raftStats();
        AKK_OBSERVABILITY_CHECK(firstStart.runtimeStartedAtUs != 0);
        AKK_OBSERVABILITY_CHECK(firstStart.sampledAtUs >= firstStart.runtimeStartedAtUs);
        node->runtime->close();
        const auto afterClose = node->runtime->raftStats();
        AKK_OBSERVABILITY_CHECK(afterClose.sampledAtUs >= firstStart.sampledAtUs);
        AKK_OBSERVABILITY_CHECK(afterClose.runtimeStartedAtUs == firstStart.runtimeStartedAtUs);
        AKK_OBSERVABILITY_CHECK(!peerById(afterClose, 22).connected);

        std::this_thread::sleep_for(std::chrono::milliseconds{2});
        node->runtime->start();
        const auto secondStart = node->runtime->raftStats();
        AKK_OBSERVABILITY_CHECK(secondStart.runtimeStartedAtUs > firstStart.runtimeStartedAtUs);
        node->runtime->close();
    }

    void testEndpointStartFailureSnapshot() {
        TempDirectory directory{"endpoint_failure"};
        const ClusterConfig config{
            {makeNode(31), makeNode(32)}, ReplicationMode::PARTITIONED, {},
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 100}
        };
        ClusterRuntimeOptions options;
        options.transportMode = TransportMode::PLAIN;
        options.clusterGroupId = 0xA44A'0003;
        options.clusterGroupEpoch = 1;
        options.replBindHost = "not a valid bind host";
        auto node = makeRuntimeNode(directory.path() / "n31", config, 31, options);
        bool rejected = false;
        try { node->runtime->start(); }
        catch (const std::runtime_error&) { rejected = true; }
        AKK_OBSERVABILITY_CHECK(rejected);
        const auto stats = node->runtime->raftStats();
        AKK_OBSERVABILITY_CHECK(stats.health == ClusterHealthState::FAILED);
        AKK_OBSERVABILITY_CHECK(stats.writeAvailability == ClusterOperationAvailability::UNAVAILABLE);
        AKK_OBSERVABILITY_CHECK(stats.linearizableReadAvailability == ClusterOperationAvailability::UNAVAILABLE);
        AKK_OBSERVABILITY_CHECK(stats.lastFailure == ClusterFailureCode::ENDPOINT_START);
        AKK_OBSERVABILITY_CHECK(stats.endpointStartFailures == 1);
        AKK_OBSERVABILITY_CHECK(stats.lastFailureAtUs != 0);
        AKK_OBSERVABILITY_CHECK(stats.runtimeStartedAtUs != 0);
        AKK_OBSERVABILITY_CHECK(stats.sampledAtUs >= stats.lastFailureAtUs);
    }

    void testEngineManagementSnapshotContract() {
        TempDirectory directory{"engine"};
        const auto local = makeNode(41);
        const ClusterConfig config{
            {local}, ReplicationMode::MIRROR, {},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 1'000}
        };
        AkkEngineOptions options;
        options.paths.dataDir = directory.path();
        options.components.clusterEnabled = true;
        options.components.blobEnabled = false;
        options.cluster.config = config;
        options.cluster.runtime.transportMode = TransportMode::PLAIN;
        writeNodeId(directory.path() / "node.id", local.nodeId);

        auto engine = AkkEngine::open(options);
        AKK_OBSERVABILITY_CHECK(waitUntil([&] {
            return engine->stats().cluster.linearizableReadAvailability == ClusterOperationAvailability::AVAILABLE;
        }));
        const auto first = engine->stats().cluster;
        std::this_thread::sleep_for(std::chrono::milliseconds{2});
        const auto second = engine->stats().cluster;

        AKK_OBSERVABILITY_CHECK(first.enabled && first.raftEnabled);
        AKK_OBSERVABILITY_CHECK(first.health == ClusterHealthState::HEALTHY);
        AKK_OBSERVABILITY_CHECK(first.quorumAvailability == ClusterOperationAvailability::AVAILABLE);
        AKK_OBSERVABILITY_CHECK(first.writeAvailability == ClusterOperationAvailability::AVAILABLE);
        AKK_OBSERVABILITY_CHECK(first.sampledAtUs >= first.runtimeStartedAtUs && first.runtimeStartedAtUs != 0);
        AKK_OBSERVABILITY_CHECK(second.sampledAtUs > first.sampledAtUs);
        AKK_OBSERVABILITY_CHECK(second.runtimeStartedAtUs == first.runtimeStartedAtUs);
        AKK_OBSERVABILITY_CHECK(first.clusterId == config.clusterId());
        AKK_OBSERVABILITY_CHECK(first.replicationMode == static_cast<uint32_t>(ReplicationMode::MIRROR));
        AKK_OBSERVABILITY_CHECK(first.consistencyMode == static_cast<uint32_t>(ConsistencyMode::RAFT_QUORUM));
        AKK_OBSERVABILITY_CHECK(first.transportMode == static_cast<uint32_t>(TransportMode::PLAIN));
        AKK_OBSERVABILITY_CHECK(first.configuredNodeCount == 1 && first.activeNodeCount == 1);
        AKK_OBSERVABILITY_CHECK(first.configuredNodes.size() == 1 && first.peers.empty());
        const auto& descriptor = first.configuredNodes.front();
        AKK_OBSERVABILITY_CHECK(descriptor.nodeId == local.nodeId);
        AKK_OBSERVABILITY_CHECK(descriptor.host == local.host);
        AKK_OBSERVABILITY_CHECK(descriptor.dataPort == local.dataPort);
        AKK_OBSERVABILITY_CHECK(descriptor.replPort == local.replPort);
        AKK_OBSERVABILITY_CHECK(descriptor.capabilities == local.capabilities);
        engine->close();
    }

    void testRaftPeerTelemetryAcrossDisconnect() {
        TempDirectory directory{"raft"};
        const ClusterConfig config{
            {makeNode(1), makeNode(2), makeNode(3)}, ReplicationMode::MIRROR, {},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 1'500}
        };
        ClusterRuntimeOptions options;
        options.transportMode = TransportMode::PLAIN;
        auto n1 = makeRuntimeNode(directory.path() / "n1", config, 1, options);
        auto n2 = makeRuntimeNode(directory.path() / "n2", config, 2, options);
        auto n3 = makeRuntimeNode(directory.path() / "n3", config, 3, options);
        n1->runtime->start();
        n2->runtime->start();
        n3->runtime->start();
        RuntimeNode* nodes[]{n1.get(), n2.get(), n3.get()};
        AKK_OBSERVABILITY_CHECK(waitUntil([&] { return findLeader(nodes) != nullptr; }, std::chrono::milliseconds{15'000}));
        RuntimeNode* leader = findLeader(nodes);
        AKK_OBSERVABILITY_CHECK(leader != nullptr);
        RuntimeNode* victim = leader == n3.get() ? n2.get() : n3.get();

        AKK_OBSERVABILITY_CHECK(waitUntil([&] {
            const auto stats = leader->runtime->raftStats();
            const auto& peer = peerById(stats, victim->nodeId);
            return peer.connected && peer.roundTripsSucceeded != 0 && stats.health == ClusterHealthState::HEALTHY &&
                stats.linearizableReadAvailability == ClusterOperationAvailability::AVAILABLE;
        }));
        const auto before = leader->runtime->raftStats();
        const auto beforeSuccess = peerById(before, victim->nodeId).roundTripsSucceeded;
        validateHistogram(peerById(before, victim->nodeId).roundTripLatencyUs);

        victim->runtime->close();
        AKK_OBSERVABILITY_CHECK(waitUntil([&] {
            const auto stats = leader->runtime->raftStats();
            const auto& peer = peerById(stats, victim->nodeId);
            return !peer.connected && peer.roundTripsFailed != 0 && peer.lastRoundTripFailureAtUs != 0;
        }));
        const auto disconnected = leader->runtime->raftStats();
        AKK_OBSERVABILITY_CHECK(disconnected.health == ClusterHealthState::DEGRADED);
        AKK_OBSERVABILITY_CHECK(disconnected.quorumAvailability == ClusterOperationAvailability::AVAILABLE);
        AKK_OBSERVABILITY_CHECK(disconnected.writeAvailability == ClusterOperationAvailability::AVAILABLE);
        const auto& failedPeer = peerById(disconnected, victim->nodeId);
        AKK_OBSERVABILITY_CHECK(failedPeer.consecutiveRoundTripFailures != 0);
        AKK_OBSERVABILITY_CHECK(failedPeer.lastRoundTripAtUs == failedPeer.lastRoundTripFailureAtUs);
        validateHistogram(failedPeer.roundTripLatencyUs);

        victim->runtime->start();
        AKK_OBSERVABILITY_CHECK(waitUntil([&] {
            const auto stats = leader->runtime->raftStats();
            const auto& peer = peerById(stats, victim->nodeId);
            return peer.connected && peer.roundTripsSucceeded > beforeSuccess && peer.consecutiveRoundTripFailures == 0;
        }, std::chrono::milliseconds{15'000}));
        const auto recovered = leader->runtime->raftStats();
        const auto& recoveredPeer = peerById(recovered, victim->nodeId);
        AKK_OBSERVABILITY_CHECK(recovered.sampledAtUs >= recoveredPeer.lastRoundTripAtUs);
        AKK_OBSERVABILITY_CHECK(recoveredPeer.roundTripLatencyUs.sampleCount == recoveredPeer.roundTripsSucceeded);
        validateHistogram(recoveredPeer.roundTripLatencyUs);

        victim->runtime->close();
        RuntimeNode* remaining = nullptr;
        for (auto* node : nodes) { if (node != leader && node != victim) { remaining = node; } }
        AKK_OBSERVABILITY_CHECK(remaining != nullptr);
        remaining->runtime->close();
        AKK_OBSERVABILITY_CHECK(waitUntil([&] {
            const auto stats = leader->runtime->raftStats();
            return stats.health == ClusterHealthState::DEGRADED &&
                stats.quorumAvailability == ClusterOperationAvailability::UNAVAILABLE &&
                stats.writeAvailability == ClusterOperationAvailability::UNAVAILABLE &&
                stats.linearizableReadAvailability == ClusterOperationAvailability::UNAVAILABLE;
        }));
        const auto isolated = leader->runtime->raftStats();
        for (unsigned i = 0; i < 50; ++i) {
            const auto stats = leader->runtime->raftStats();
            AKK_OBSERVABILITY_CHECK(stats.lastLogIndex == isolated.lastLogIndex);
            AKK_OBSERVABILITY_CHECK(stats.lastFailure == isolated.lastFailure);
        }
        victim->runtime->start(); remaining->runtime->start();
        AKK_OBSERVABILITY_CHECK(waitUntil([&] {
            auto* current = findLeader(nodes);
            return current && current->runtime->raftStats().health == ClusterHealthState::HEALTHY &&
                current->runtime->raftStats().linearizableReadAvailability == ClusterOperationAvailability::AVAILABLE;
        }, std::chrono::milliseconds{15'000}));
        leader->runtime->close();
        AKK_OBSERVABILITY_CHECK(leader->runtime->raftStats().writeAvailability == ClusterOperationAvailability::UNAVAILABLE);
    }

    void testLearnerCannotSupplyObservedQuorum() {
        TempDirectory directory{"learner_quorum"};
        auto learner = makeNode(3); learner.capabilities |= NodeCapability::RAFT_LEARNER;
        const ClusterConfig config{{makeNode(1), makeNode(2), learner}, ReplicationMode::MIRROR, {},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 1'000},
            RaftOptions{.membership = {.allowLearners = true}}};
        ClusterRuntimeOptions options; options.transportMode = TransportMode::PLAIN;
        auto n1 = makeRuntimeNode(directory.path() / "n1", config, 1, options);
        auto n2 = makeRuntimeNode(directory.path() / "n2", config, 2, options);
        auto n3 = makeRuntimeNode(directory.path() / "n3", config, 3, options);
        n1->runtime->start(); n2->runtime->start(); n3->runtime->start();
        RuntimeNode* voters[]{n1.get(), n2.get()};
        AKK_OBSERVABILITY_CHECK(waitUntil([&] {
            auto* leader = findLeader(voters);
            return leader && leader->runtime->raftStats().health == ClusterHealthState::HEALTHY;
        }, std::chrono::milliseconds{15'000}));
        auto* leader = findLeader(voters);
        auto* other = leader == n1.get() ? n2.get() : n1.get();
        AKK_OBSERVABILITY_CHECK(waitUntil([&] { return n3->runtime->raftStats().health == ClusterHealthState::HEALTHY; }));
        AKK_OBSERVABILITY_CHECK(n3->runtime->raftStats().quorumAvailability == ClusterOperationAvailability::UNKNOWN);
        AKK_OBSERVABILITY_CHECK(n3->runtime->raftStats().writeAvailability == ClusterOperationAvailability::UNKNOWN);
        other->runtime->close();
        AKK_OBSERVABILITY_CHECK(waitUntil([&] {
            const auto stats = leader->runtime->raftStats();
            return peerById(stats, 3).connected && stats.quorumAvailability == ClusterOperationAvailability::UNAVAILABLE &&
                stats.health == ClusterHealthState::DEGRADED;
        }));
        leader->runtime->close();
        AKK_OBSERVABILITY_CHECK(waitUntil([&] {
            const auto stats = n3->runtime->raftStats();
            return stats.health == ClusterHealthState::DEGRADED && stats.writeAvailability == ClusterOperationAvailability::UNAVAILABLE;
        }));
    }

    void testHealthAggregation() {
        RaftRuntimeStats root, child;
        root.quorumAvailability = root.writeAvailability = root.linearizableReadAvailability = ClusterOperationAvailability::AVAILABLE;
        child.health = ClusterHealthState::DEGRADED;
        child.writeAvailability = ClusterOperationAvailability::UNAVAILABLE;
        cluster::detail::mergeHealthObservation(root, child);
        AKK_OBSERVABILITY_CHECK(root.health == ClusterHealthState::DEGRADED);
        AKK_OBSERVABILITY_CHECK(root.writeAvailability == ClusterOperationAvailability::UNAVAILABLE);
        AKK_OBSERVABILITY_CHECK(root.quorumAvailability == ClusterOperationAvailability::UNKNOWN);
        child.health = ClusterHealthState::FAILED; child.lastFailure = ClusterFailureCode::RAFT_PERSISTENCE; child.lastFailureAtUs = 42;
        cluster::detail::mergeHealthObservation(root, child);
        AKK_OBSERVABILITY_CHECK(root.health == ClusterHealthState::FAILED && root.lastFailureAtUs == 42);
        AKK_OBSERVABILITY_CHECK(cluster::detail::combineHealth(root.health, ClusterHealthState::REBUILDING) == ClusterHealthState::FAILED);
    }

    void testRequestAuthorityHealthDependency() {
        TempDirectory directory{"authority_health"};
        const ClusterConfig config{{makeNode(1), makeNode(2), makeNode(3)}, ReplicationMode::MIRROR, {},
            ConsistencyOptions{.mode = ConsistencyMode::ASYNC, .ackTimeoutMs = 1'000}, {}, {}, 1};
        ClusterRuntimeOptions options; options.transportMode = TransportMode::PLAIN;
        options.requests.enabled = true;
        options.clusterGroupId = 0xA44A'0005;
        options.requests.authorityNodes = config.nodes();
        for (auto& node : options.requests.authorityNodes) { node.replPort = unusedLoopbackPort(); }
        options.startupRole = NodeStartupRole::PRIMARY;
        auto n1 = makeRuntimeNode(directory.path() / "n1", config, 1, options);
        options.startupRole = NodeStartupRole::REPLICA;
        auto n2 = makeRuntimeNode(directory.path() / "n2", config, 2, options);
        auto n3 = makeRuntimeNode(directory.path() / "n3", config, 3, options);
        n1->runtime->start(); n2->runtime->start(); n3->runtime->start();
        RuntimeNode* nodes[]{n1.get(), n2.get(), n3.get()};
        RuntimeNode* authorityLeader = nullptr;
        AKK_OBSERVABILITY_CHECK(waitUntil([&] {
            for (auto* node : nodes) {
                const auto stats = node->runtime->raftStats();
                if (stats.quorumAvailability == ClusterOperationAvailability::AVAILABLE && stats.health == ClusterHealthState::HEALTHY) {
                    authorityLeader = node; return true;
                }
            }
            return false;
        }, std::chrono::milliseconds{15'000}));
        for (auto* node : nodes) { if (node != authorityLeader) { node->runtime->close(); } }
        AKK_OBSERVABILITY_CHECK(waitUntil([&] {
            const auto stats = authorityLeader->runtime->raftStats();
            return stats.quorumAvailability == ClusterOperationAvailability::UNAVAILABLE &&
                stats.writeAvailability == ClusterOperationAvailability::UNAVAILABLE && stats.health == ClusterHealthState::DEGRADED;
        }));
    }

    void testPartitionChildHealthDependency(bool reduceCopies) {
        TempDirectory directory{"partition_health"};
        std::vector<NodeInfo> members{makeNode(1), makeNode(2), makeNode(3)};
        for (auto& node : members) {
            // Each group reserves its data port plus the adjacent placement port.
            do { node.partitionReplBasePort = unusedLoopbackPort(); }
            while (node.partitionReplBasePort == UINT16_MAX || std::ranges::any_of(members, [&](const auto& peer) {
                const uint32_t begin = node.partitionReplBasePort, end = begin + 1;
                return (peer.dataPort >= begin && peer.dataPort <= end) || (peer.replPort >= begin && peer.replPort <= end) ||
                    (peer.stripeMetadataPort >= begin && peer.stripeMetadataPort <= end) ||
                    (&peer != &node && peer.partitionReplBasePort != 0 &&
                        uint32_t{peer.partitionReplBasePort} <= end && uint32_t{peer.partitionReplBasePort} + 1 >= begin);
            }));
        }
        const ClusterConfig config{members, ReplicationMode::PARTITIONED, {},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 1'000},
            RaftOptions{.membership = {.mode = RaftMembershipMode::JOINT_CONSENSUS, .allowOnlineVoterChanges = true, .allowLearners = true}}, {}, 0, {},
            PartitionOptions{.replicationFactor = 3, .partitionCount = 1}};
        std::vector<std::unique_ptr<AkkEngine>> engines;
        for (const auto& node : members) {
            AkkEngineOptions options; options.paths.dataDir = directory.path() / std::to_string(node.nodeId);
            options.components.clusterEnabled = true; options.components.blobEnabled = false;
            options.cluster.config = config; options.cluster.runtime.transportMode = TransportMode::PLAIN;
            options.cluster.runtime.routingMode = ClusterRoutingMode::FORWARD;
            options.cluster.runtime.clusterGroupId = 0xA44A'0004;
            writeNodeId(options.paths.dataDir / "node.id", node.nodeId);
            engines.push_back(AkkEngine::open(options));
        }
        uint64_t leader = 0;
        AKK_OBSERVABILITY_CHECK(waitUntil([&] {
            const auto partitions = engines[0]->clusterPartitionStats();
            if (partitions.empty()) { return false; }
            leader = partitions.front().leaderNodeId;
            return leader && std::ranges::all_of(engines, [](const auto& engine) { return engine->stats().cluster.health == ClusterHealthState::HEALTHY; });
        }, std::chrono::milliseconds{15'000}));
        if (reduceCopies) {
            const ClusterConfig singleCopy{members, config.mode(), config.ackPolicy(), config.consistency(), config.raft(),
                config.stripe(), config.primaryNodeId(), config.clusterId(), {.replicationFactor = 1, .partitionCount = 1}};
            engines[0]->reconfigureCluster(singleCopy);
            AKK_OBSERVABILITY_CHECK(waitUntil([&] {
                return std::ranges::all_of(engines, [](const auto& engine) { return engine->stats().cluster.health == ClusterHealthState::HEALTHY; });
            }));
            return;
        }
        for (size_t index = 0; index < engines.size(); ++index) { if (index + 1 != leader) { engines[index]->close(); } }
        AKK_OBSERVABILITY_CHECK(waitUntil([&] {
            const auto stats = engines[leader - 1]->stats().cluster;
            return stats.health == ClusterHealthState::DEGRADED && stats.quorumAvailability == ClusterOperationAvailability::UNAVAILABLE &&
                stats.writeAvailability == ClusterOperationAvailability::UNAVAILABLE &&
                stats.linearizableReadAvailability == ClusterOperationAvailability::UNAVAILABLE;
        }));
    }

    void testNonRaftReadTelemetryAcrossFailureAndRecovery() {
        TempDirectory directory{"non_raft"};
        const ClusterConfig config{
            {makeNode(11), makeNode(12)}, ReplicationMode::PARTITIONED, {},
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 150}
        };
        ClusterRuntimeOptions options;
        options.transportMode = TransportMode::PLAIN;
        options.clusterGroupId = 0xA44A'0001;
        options.clusterGroupEpoch = 7;
        auto n1 = makeRuntimeNode(directory.path() / "n1", config, 11, options);
        auto n2 = makeRuntimeNode(directory.path() / "n2", config, 12, options);
        n2->putLocal(1, "managed-key", "managed-value");
        n1->runtime->start();
        n2->runtime->start();
        AKK_OBSERVABILITY_CHECK(waitUntil([&] { return peerById(n1->runtime->raftStats(), 12).connected; }));

        auto response = n1->runtime->readLocalStorageFromNode(12, bytesOf("managed-key"), 0);
        AKK_OBSERVABILITY_CHECK(response.status == ReadStatus::FOUND && textOf(response.value) == "managed-value");
        auto stats = n1->runtime->raftStats();
        const auto initialRuntimeStart = stats.runtimeStartedAtUs;
        const auto& successfulPeer = peerById(stats, 12);
        AKK_OBSERVABILITY_CHECK(stats.clusterGroupId == options.clusterGroupId);
        AKK_OBSERVABILITY_CHECK(stats.clusterGroupEpoch == options.clusterGroupEpoch);
        AKK_OBSERVABILITY_CHECK(successfulPeer.roundTripsSucceeded == 1);
        AKK_OBSERVABILITY_CHECK(successfulPeer.roundTripsFailed == 0);
        AKK_OBSERVABILITY_CHECK(successfulPeer.lastRoundTripAtUs != 0);
        validateHistogram(successfulPeer.roundTripLatencyUs);

        n2->runtime->close();
        AKK_OBSERVABILITY_CHECK(waitUntil([&] { return !peerById(n1->runtime->raftStats(), 12).connected; }));
        response = n1->runtime->readLocalStorageFromNode(12, bytesOf("managed-key"), 0);
        AKK_OBSERVABILITY_CHECK(response.status == ReadStatus::ERROR_STATUS);
        stats = n1->runtime->raftStats();
        const auto& failedPeer = peerById(stats, 12);
        AKK_OBSERVABILITY_CHECK(stats.health == ClusterHealthState::DEGRADED);
        AKK_OBSERVABILITY_CHECK(stats.lastFailure == ClusterFailureCode::PEER_READ_TIMEOUT);
        AKK_OBSERVABILITY_CHECK(failedPeer.roundTripsSucceeded == 1);
        AKK_OBSERVABILITY_CHECK(failedPeer.roundTripsFailed == 1);
        AKK_OBSERVABILITY_CHECK(failedPeer.consecutiveRoundTripFailures == 1);
        AKK_OBSERVABILITY_CHECK(failedPeer.lastRoundTripFailureAtUs != 0);
        AKK_OBSERVABILITY_CHECK(failedPeer.roundTripLatencyUs.sampleCount == 1);

        n2->runtime->start();
        AKK_OBSERVABILITY_CHECK(waitUntil([&] { return peerById(n1->runtime->raftStats(), 12).connected; }));
        response = n1->runtime->readLocalStorageFromNode(12, bytesOf("managed-key"), 0);
        AKK_OBSERVABILITY_CHECK(response.status == ReadStatus::FOUND);
        stats = n1->runtime->raftStats();
        const auto& recoveredPeer = peerById(stats, 12);
        AKK_OBSERVABILITY_CHECK(stats.health == ClusterHealthState::HEALTHY);
        AKK_OBSERVABILITY_CHECK(stats.runtimeStartedAtUs == initialRuntimeStart);
        AKK_OBSERVABILITY_CHECK(recoveredPeer.roundTripsSucceeded == 2);
        AKK_OBSERVABILITY_CHECK(recoveredPeer.roundTripsFailed == 1);
        AKK_OBSERVABILITY_CHECK(recoveredPeer.consecutiveRoundTripFailures == 0);
        AKK_OBSERVABILITY_CHECK(recoveredPeer.roundTripLatencyUs.sampleCount == 2);
        validateHistogram(recoveredPeer.roundTripLatencyUs);
    }
}

int main() {
    try {
        akkaradb::test::installMsvcTestErrorHandlers();
        enableDeterministicRaftElection();
        AKK_OBSERVABILITY_CHECK(akkaradb_cluster_register());
        std::fprintf(stderr, "[cluster-observability] default histogram contract\n");
        testDefaultHistogramContract();
        std::fprintf(stderr, "[cluster-observability] disabled cluster snapshot\n");
        testDisabledClusterSnapshotContract();
        std::fprintf(stderr, "[cluster-observability] runtime lifecycle\n");
        testRuntimeSnapshotLifecycle();
        std::fprintf(stderr, "[cluster-observability] endpoint startup failure\n");
        testEndpointStartFailureSnapshot();
        std::fprintf(stderr, "[cluster-observability] EngineStats management snapshot\n");
        testEngineManagementSnapshotContract();
        std::fprintf(stderr, "[cluster-observability] Raft disconnect and recovery\n");
        testRaftPeerTelemetryAcrossDisconnect();
        std::fprintf(stderr, "[cluster-observability] learner exclusion and stale leader contact\n");
        testLearnerCannotSupplyObservedQuorum();
        testHealthAggregation();
        std::fprintf(stderr, "[cluster-observability] required authority and partition health\n");
        testRequestAuthorityHealthDependency();
        testPartitionChildHealthDependency(false);
        testPartitionChildHealthDependency(true);
        std::fprintf(stderr, "[cluster-observability] non-Raft read failure and recovery\n");
        testNonRaftReadTelemetryAcrossFailureAndRecovery();
        return 0;
    }
    catch (const std::exception& error) {
        std::fprintf(stderr, "cluster observability test failed: %s\n", error.what());
        return 1;
    }
}

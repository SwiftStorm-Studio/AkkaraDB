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

// benchmarks/smoke/clusterSmokeTest.cpp
#include "TestErrorHandlers.hpp"

#include "akk/engine/cluster/ClusterConfig.hpp"
#include "akk/engine/cluster/ClusterManager.hpp"
#include "akk/engine/cluster/ClusterRuntime.hpp"
#include "akk/engine/cluster/ClusterRouter.hpp"
#include "akk/engine/cluster/ReplFraming.hpp"
#include "akk/engine/cluster/ReplicationClient.hpp"
#include "akk/engine/cluster/ReplicationServer.hpp"

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
        const ClusterConfig cfg{
            {
                node(1, 0, 19601, static_cast<uint32_t>(NodeCapability::COORDINATOR_ELIGIBLE)),
                node(2, 19702, 19602, static_cast<uint32_t>(NodeCapability::DATA_BEARING)),
                node(3, 19703, 19603, static_cast<uint32_t>(NodeCapability::DATA_BEARING)),
            },
            ReplicationMode::STRIPE,
            ack,
        };

        ClusterConfig::save(path, cfg);
        const auto loaded = ClusterConfig::load(path);
        AKK_TEST_CHECK(loaded.mode() == ReplicationMode::STRIPE);
        AKK_TEST_CHECK(loaded.ackPolicy().mode == AckPolicyMode::QUORUM);
        AKK_TEST_CHECK(loaded.ackPolicy().quorum == 2);
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

        auto primary = ClusterManager::create(dir1, cfg, 1);
        auto replica = ClusterManager::create(dir2, cfg, 2);
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
    }

    void testPlainReplication() {
        constexpr uint16_t port = 19971;
        ClusterRuntimeOptions plain{};
        plain.transportMode = TransportMode::PLAIN;
        const AckPolicy all{.mode = AckPolicyMode::ALL, .quorum = 0};

        auto server = ReplicationServer::create(port, 1, [] { return uint64_t{1}; }, all, plain);
        std::atomic<int> applied{0};
        std::atomic<int> blobs{0};

        auto client = ReplicationClient::create("127.0.0.1", port, 2, [] { return uint64_t{0}; }, plain);
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
        AKK_TEST_CHECK(options.transportMode == TransportMode::TLS);
        AKK_TEST_CHECK(options.replBindHost == "0.0.0.0");

        constexpr uint16_t port = 19972;
        const AckPolicy all{.mode = AckPolicyMode::ALL, .quorum = 0};
        auto server = ReplicationServer::create(port, 1, [] { return uint64_t{1}; }, all, options);
        std::atomic<int> applied{0};

        auto client = ReplicationClient::create("127.0.0.1", port, 2, [] { return uint64_t{0}; }, options);
        client->setApplyCallback([&](uint64_t seq, ReplOpType op, std::span<const uint8_t> key, std::span<const uint8_t> value, uint8_t flags, uint64_t source) {
            AKK_TEST_CHECK(seq == 2);
            AKK_TEST_CHECK(op == ReplOpType::PUT);
            AKK_TEST_CHECK(source == 1);
            AKK_TEST_CHECK(flags == 4);
            AKK_TEST_CHECK(std::string(reinterpret_cast<const char*>(key.data()), key.size()) == "tls-k");
            AKK_TEST_CHECK(std::string(reinterpret_cast<const char*>(value.data()), value.size()) == "tls-v");
            ++applied;
        });

        server->start();
        client->start();

        for (int i = 0; i < 50 && server->replicaCount() == 0; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        AKK_TEST_CHECK(server->replicaCount() == 1);

        const std::string key = "tls-k";
        const std::string value = "tls-v";
        server->shipEntry(2, ReplOpType::PUT, bytesOf(key), bytesOf(value), 4, 1);

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

    void testRuntimeRejectsStripeUntilRoutingExists() {
        const auto dir = makeTempDir("stripeRuntimeReject");
        const ClusterConfig cfg{
            {
                node(1, 19793, 19813, static_cast<uint32_t>(NodeCapability::COORDINATOR_ELIGIBLE) | static_cast<uint32_t>(NodeCapability::DATA_BEARING)),
                node(2, 19794, 19814, static_cast<uint32_t>(NodeCapability::DATA_BEARING)),
            },
            ReplicationMode::STRIPE,
            AckPolicy{},
        };

        bool rejected = false;
        try {
            ClusterEngineCallbacks callbacks;
            (void)ClusterRuntime::create(dir, cfg, 1, std::move(callbacks), ClusterRuntimeOptions{});
        }
        catch (const std::invalid_argument&) {
            rejected = true;
        }
        AKK_TEST_CHECK(rejected);
    }
} // namespace

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();

    testConfigRoundtripAndRejection();
    testRouter();
    testFraming();
    testManagerElection();
    testPlainReplication();
    testTransportDefault();
    testPlainTransportRejectsWanHosts();
    testRuntimeRejectsStripeUntilRoutingExists();
    std::printf("cluster smoke test passed\n");
    return 0;
}

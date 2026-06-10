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

// benchmarks/smoke/cluster_smoke_test.cpp
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
    std::filesystem::path make_temp_dir(const std::string& suffix) {
        const auto dir = std::filesystem::temp_directory_path() / ("akkaradb_cluster_smoke_" + suffix);
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
        std::filesystem::create_directories(dir, ec);
        assert(!ec);
        return dir;
    }

    NodeInfo node(uint64_t id, uint16_t data_port, uint16_t repl_port, uint32_t capabilities) {
        return NodeInfo{
            .node_id = id,
            .host = "127.0.0.1",
            .data_port = data_port,
            .repl_port = repl_port,
            .capabilities = capabilities,
        };
    }

    std::span<const uint8_t> bytes_of(const std::string& value) {
        return {reinterpret_cast<const uint8_t*>(value.data()), value.size()};
    }

    void expect_throw_load(const std::filesystem::path& path) {
        bool threw = false;
        try {
            (void)ClusterConfig::load(path);
        }
        catch (const std::runtime_error&) {
            threw = true;
        }
        assert(threw);
    }

    void test_config_roundtrip_and_rejection() {
        const auto dir = make_temp_dir("config");
        const auto path = dir / "cluster.akcc";
        const AckPolicy ack{.mode = AckPolicyMode::Quorum, .quorum = 2};
        const ClusterConfig cfg{
            {
                node(1, 0, 19601, static_cast<uint32_t>(NodeCapability::CoordinatorEligible)),
                node(2, 19702, 19602, static_cast<uint32_t>(NodeCapability::DataBearing)),
                node(3, 19703, 19603, static_cast<uint32_t>(NodeCapability::DataBearing)),
            },
            ReplicationMode::Stripe,
            ack,
        };

        ClusterConfig::save(path, cfg);
        const auto loaded = ClusterConfig::load(path);
        assert(loaded.mode() == ReplicationMode::Stripe);
        assert(loaded.ack_policy().mode == AckPolicyMode::Quorum);
        assert(loaded.ack_policy().quorum == 2);
        assert(loaded.find_by_id(2) != nullptr);
        assert(loaded.data_nodes().size() == 2);
        assert(loaded.coordinator_nodes().size() == 1);

        {
            auto corrupt = path;
            corrupt += ".crc";
            std::filesystem::copy_file(path, corrupt, std::filesystem::copy_options::overwrite_existing);
            std::fstream file(corrupt, std::ios::in | std::ios::out | std::ios::binary);
            file.seekp(40, std::ios::beg);
            char bad = '\x7f';
            file.write(&bad, 1);
            file.close();
            expect_throw_load(corrupt);
        }

        {
            auto corrupt = path;
            corrupt += ".magic";
            std::filesystem::copy_file(path, corrupt, std::filesystem::copy_options::overwrite_existing);
            std::fstream file(corrupt, std::ios::in | std::ios::out | std::ios::binary);
            char bad = '\0';
            file.write(&bad, 1);
            file.close();
            expect_throw_load(corrupt);
        }

        bool invalid_capability = false;
        try {
            (void)ClusterConfig{{node(9, 19709, 19609, 0x80)}, ReplicationMode::Mirror, AckPolicy{}};
        }
        catch (const std::invalid_argument&) {
            invalid_capability = true;
        }
        assert(invalid_capability);
    }

    void test_router() {
        const ClusterConfig mirror{
            {
                node(1, 0, 19611, static_cast<uint32_t>(NodeCapability::CoordinatorEligible)),
                node(2, 19712, 19612, static_cast<uint32_t>(NodeCapability::DataBearing)),
                node(3, 19713, 19613, static_cast<uint32_t>(NodeCapability::DataBearing)),
            },
            ReplicationMode::Mirror,
            AckPolicy{},
        };
        ClusterRouter mirror_router{mirror};
        const std::string key = "customer:42";
        assert(mirror_router.write_targets(bytes_of(key)).size() == 2);

        const ClusterConfig stripe_a{
            {
                node(1, 0, 19621, static_cast<uint32_t>(NodeCapability::CoordinatorEligible)),
                node(2, 19722, 19622, static_cast<uint32_t>(NodeCapability::DataBearing)),
                node(3, 19723, 19623, static_cast<uint32_t>(NodeCapability::DataBearing)),
            },
            ReplicationMode::Stripe,
            AckPolicy{},
        };
        const ClusterConfig stripe_b{
            {
                node(3, 19723, 19623, static_cast<uint32_t>(NodeCapability::DataBearing)),
                node(1, 0, 19621, static_cast<uint32_t>(NodeCapability::CoordinatorEligible)),
                node(2, 19722, 19622, static_cast<uint32_t>(NodeCapability::DataBearing)),
            },
            ReplicationMode::Stripe,
            AckPolicy{},
        };
        ClusterRouter router_a{stripe_a};
        ClusterRouter router_b{stripe_b};
        const auto target_a = router_a.write_targets(bytes_of(key));
        const auto target_b = router_b.write_targets(bytes_of(key));
        assert(target_a.size() == 1);
        assert(target_b.size() == 1);
        assert(target_a[0].node_id == target_b[0].node_id);
        assert(router_a.read_candidates(bytes_of(key))[0].node_id == target_a[0].node_id);
    }

    void test_framing() {
        const std::vector<uint8_t> key{'k'};
        const std::vector<uint8_t> value{'v'};
        const auto entry_wire = encode_entry(ReplEntry{
            .seq = 9,
            .source_node_id = 1,
            .op = ReplOpType::Put,
            .record_flags = 7,
            .key = key,
            .value = value,
        });

        DecodedFrame frame;
        assert(decode_frame(entry_wire, frame));
        assert(frame.type == ReplMsgType::Entry);
        ReplEntry entry;
        assert(decode_entry(frame.payload, entry));
        assert(entry.seq == 9);
        assert(entry.record_flags == 7);
        assert(entry.key == key);
        assert(entry.value == value);

        auto corrupt = entry_wire;
        corrupt.back() ^= 0x55;
        assert(!decode_frame(corrupt, frame));

        auto truncated = entry_wire;
        truncated.pop_back();
        assert(!decode_frame(truncated, frame));
    }

    void test_manager_election() {
        const auto dir1 = make_temp_dir("manager1");
        const auto dir2 = make_temp_dir("manager2");
        const ClusterConfig cfg{
            {
                node(1, 19781, 19801, static_cast<uint32_t>(NodeCapability::CoordinatorEligible) | static_cast<uint32_t>(NodeCapability::DataBearing)),
                node(2, 19782, 19802, static_cast<uint32_t>(NodeCapability::CoordinatorEligible) | static_cast<uint32_t>(NodeCapability::DataBearing)),
            },
            ReplicationMode::Mirror,
            AckPolicy{},
        };

        auto primary = ClusterManager::create(dir1, cfg, 1);
        auto replica = ClusterManager::create(dir2, cfg, 2);
        std::atomic<int> primary_changes{0};
        primary->set_role_change_callback([&](NodeRole role) {
            if (role == NodeRole::Primary) {
                ++primary_changes;
            }
        });

        primary->start();
        assert(primary->role() == NodeRole::Primary);
        replica->start();
        assert(replica->role() == NodeRole::Replica);
        assert(primary_changes.load() >= 1);
        assert(replica->primary_host() == "127.0.0.1");
        assert(replica->primary_repl_port() == 19801);

        replica->close();
        primary->close();
    }

    void test_plain_replication() {
        constexpr uint16_t port = 19971;
        ClusterRuntimeOptions plain{};
        plain.transport_mode = TransportMode::Plain;
        const AckPolicy all{.mode = AckPolicyMode::All, .quorum = 0};

        auto server = ReplicationServer::create(port, 1, [] { return uint64_t{1}; }, all, plain);
        std::atomic<int> applied{0};
        std::atomic<int> blobs{0};

        auto client = ReplicationClient::create("127.0.0.1", port, 2, [] { return uint64_t{0}; }, plain);
        client->set_apply_callback([&](uint64_t seq, ReplOpType op, std::span<const uint8_t> key, std::span<const uint8_t> value, uint8_t flags, uint64_t source) {
            assert(seq == 1);
            assert(op == ReplOpType::Put);
            assert(source == 1);
            assert(flags == 3);
            assert(std::string(reinterpret_cast<const char*>(key.data()), key.size()) == "k");
            assert(std::string(reinterpret_cast<const char*>(value.data()), value.size()) == "v");
            ++applied;
        });
        client->set_blob_callback([&](uint64_t seq, uint64_t blob_id, std::span<const uint8_t> content) {
            assert(seq == 1);
            assert(blob_id == 99);
            assert(std::string(reinterpret_cast<const char*>(content.data()), content.size()) == "blob");
            ++blobs;
        });

        server->start();
        client->start();

        for (int i = 0; i < 50 && server->replica_count() == 0; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        assert(server->replica_count() == 1);

        const std::string key = "k";
        const std::string value = "v";
        const std::string blob = "blob";
        server->ship_blob(1, 99, bytes_of(blob));
        server->ship_entry(1, ReplOpType::Put, bytes_of(key), bytes_of(value), 3, 1);

        for (int i = 0; i < 50 && (applied.load() == 0 || blobs.load() == 0); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        assert(applied.load() == 1);
        assert(blobs.load() == 1);

        client->close();
        server->close();
    }

    void test_transport_default() {
        ClusterRuntimeOptions options{};
        assert(options.transport_mode == TransportMode::TLS);
        assert(options.repl_bind_host == "0.0.0.0");

        constexpr uint16_t port = 19972;
        const AckPolicy all{.mode = AckPolicyMode::All, .quorum = 0};
        auto server = ReplicationServer::create(port, 1, [] { return uint64_t{1}; }, all, options);
        std::atomic<int> applied{0};

        auto client = ReplicationClient::create("127.0.0.1", port, 2, [] { return uint64_t{0}; }, options);
        client->set_apply_callback([&](uint64_t seq, ReplOpType op, std::span<const uint8_t> key, std::span<const uint8_t> value, uint8_t flags, uint64_t source) {
            assert(seq == 2);
            assert(op == ReplOpType::Put);
            assert(source == 1);
            assert(flags == 4);
            assert(std::string(reinterpret_cast<const char*>(key.data()), key.size()) == "tls-k");
            assert(std::string(reinterpret_cast<const char*>(value.data()), value.size()) == "tls-v");
            ++applied;
        });

        server->start();
        client->start();

        for (int i = 0; i < 50 && server->replica_count() == 0; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        assert(server->replica_count() == 1);

        const std::string key = "tls-k";
        const std::string value = "tls-v";
        server->ship_entry(2, ReplOpType::Put, bytes_of(key), bytes_of(value), 4, 1);

        for (int i = 0; i < 50 && applied.load() == 0; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        assert(applied.load() == 1);

        client->close();
        server->close();
    }

    void test_plain_transport_rejects_wan_hosts() {
        const auto dir = make_temp_dir("plain_wan_reject");
        const ClusterConfig cfg{
            {
                NodeInfo{
                    .node_id = 1,
                    .host = "203.0.113.10",
                    .data_port = 19791,
                    .repl_port = 19811,
                    .capabilities = static_cast<uint32_t>(NodeCapability::CoordinatorEligible) | static_cast<uint32_t>(NodeCapability::DataBearing),
                },
                node(2, 19792, 19812, static_cast<uint32_t>(NodeCapability::DataBearing)),
            },
            ReplicationMode::Mirror,
            AckPolicy{},
        };

        ClusterRuntimeOptions plain{};
        plain.transport_mode = TransportMode::Plain;

        bool rejected = false;
        try {
            ClusterEngineCallbacks callbacks;
            (void)ClusterRuntime::create(dir, cfg, 1, std::move(callbacks), plain);
        }
        catch (const std::invalid_argument&) {
            rejected = true;
        }
        assert(rejected);
    }

    void test_runtime_rejects_stripe_until_routing_exists() {
        const auto dir = make_temp_dir("stripe_runtime_reject");
        const ClusterConfig cfg{
            {
                node(1, 19793, 19813, static_cast<uint32_t>(NodeCapability::CoordinatorEligible) | static_cast<uint32_t>(NodeCapability::DataBearing)),
                node(2, 19794, 19814, static_cast<uint32_t>(NodeCapability::DataBearing)),
            },
            ReplicationMode::Stripe,
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
        assert(rejected);
    }
} // namespace

int main() {
    akkara::test::install_msvc_test_error_handlers();

    test_config_roundtrip_and_rejection();
    test_router();
    test_framing();
    test_manager_election();
    test_plain_replication();
    test_transport_default();
    test_plain_transport_rejects_wan_hosts();
    test_runtime_rejects_stripe_until_routing_exists();
    std::printf("cluster smoke test passed\n");
    return 0;
}

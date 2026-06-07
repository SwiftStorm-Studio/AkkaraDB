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

// akkengine/include/akk/engine/cluster/ClusterConfig.hpp
#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

namespace akkaradb::engine::cluster {
    /**
     * ReplicationMode - Placement strategy for write/read routing.
     *
     * Standalone keeps all traffic local.  Mirror sends writes to every
     * data-bearing node.  Stripe assigns each key to one data-bearing node
     * using rendezvous hashing.
     */
    enum class ReplicationMode : uint8_t {
        Standalone = 0, Mirror = 1, Stripe = 2,
    };

    /**
     * AckPolicyMode - Durability policy for primary-to-replica shipping.
     */
    enum class AckPolicyMode : uint8_t {
        Async = 0,
        ///< Return without waiting for replica acknowledgements.
        All = 1,
        ///< Wait until all currently live replicas acknowledge.
        Quorum = 2,
        ///< Wait until at least AckPolicy::quorum replicas acknowledge.
    };

    /**
     * TransportMode - Network transport used by replication links.
     */
    enum class TransportMode : uint8_t {
        TLS = 0, Plain = 1,
    };

    /**
     * NodeRole - Runtime role selected by ClusterManager.
     */
    enum class NodeRole : uint8_t {
        Standalone = 0, Primary = 1, Replica = 2,
    };

    /**
     * NodeCapability - Bit flags describing what a node may do.
     */
    enum NodeCapability : uint32_t {
        CoordinatorEligible = 1u << 0,
        ///< Node may be selected as primary by ClusterManager.
        DataBearing = 1u << 1,
        ///< Node can store key/value data and receive routed writes.
    };

    /**
     * AckPolicy - Acknowledgement rule applied by ReplicationServer.
     */
    struct AckPolicy {
        AckPolicyMode mode = AckPolicyMode::Async;
        uint16_t quorum = 0; ///< Required replica count when mode == AckPolicyMode::Quorum.
    };

    /**
     * NodeInfo - Persistent identity and connection endpoints for one node.
     */
    struct NodeInfo {
        uint64_t node_id = 0; ///< Stable node id.  Zero is reserved.
        std::string host; ///< Hostname or address used by peer nodes.
        uint16_t data_port = 0; ///< Public data API port.
        uint16_t repl_port = 0; ///< Replication listener port.
        uint32_t capabilities = DataBearing; ///< OR-ed NodeCapability flags.

        /** Returns true if this node may become primary. */
        [[nodiscard]] bool coordinator_eligible() const noexcept { return (capabilities & CoordinatorEligible) != 0; }

        /** Returns true if this node participates in data placement. */
        [[nodiscard]] bool data_bearing() const noexcept { return (capabilities & DataBearing) != 0; }
    };

    /**
     * ClusterTlsOptions - TLS certificate configuration for replication links.
     */
    struct ClusterTlsOptions {
        std::filesystem::path cert_path; ///< Local certificate path.
        std::filesystem::path key_path; ///< Local private-key path.
        std::filesystem::path ca_path; ///< CA bundle used for peer verification.
        bool verify_peer = true; ///< Whether TLS peers must validate against ca_path.
    };

    /**
     * ClusterRuntimeOptions - Runtime-only network options.
     */
    struct ClusterRuntimeOptions {
        TransportMode transport_mode = TransportMode::TLS;
        std::string repl_bind_host = "0.0.0.0"; ///< Local address used by the primary replication listener.
        ClusterTlsOptions tls;
    };

    /**
     * ClusterConfig - Durable cluster membership and replication policy.
     *
     * The config is stored as a compact CRC-protected binary file.  It records
     * node identities, data/replication ports, node capabilities, replication
     * mode, and acknowledgement policy.  Runtime-only options such as TLS paths
     * live in ClusterRuntimeOptions and are intentionally not serialized here.
     */
    class ClusterConfig {
        public:
            static constexpr uint32_t MAGIC = 0x35434B41; // "AKC5"
            static constexpr uint16_t VERSION = 1;

            ClusterConfig() = default;

            /**
             * Creates a config and validates it immediately.
             *
             * @throws std::invalid_argument if node ids, capabilities, mode, or
             *         acknowledgement policy are invalid.
             */
            ClusterConfig(std::vector<NodeInfo> nodes, ReplicationMode mode, AckPolicy ack_policy);

            /**
             * Loads and validates a cluster config file.
             *
             * @throws std::runtime_error on I/O, magic, version, CRC, or
             *         truncation errors.
             * @throws std::invalid_argument if the decoded config is invalid.
             */
            [[nodiscard]] static ClusterConfig load(const std::filesystem::path& path);

            /**
             * Atomically writes a validated cluster config file.
             *
             * @throws std::runtime_error on I/O failure.
             * @throws std::invalid_argument if config is invalid.
             */
            static void save(const std::filesystem::path& path, const ClusterConfig& config);

            /** Returns all configured nodes in file order. */
            [[nodiscard]] const std::vector<NodeInfo>& nodes() const noexcept { return nodes_; }

            /** Returns the configured data placement mode. */
            [[nodiscard]] ReplicationMode mode() const noexcept { return mode_; }

            /** Returns the configured replica acknowledgement policy. */
            [[nodiscard]] AckPolicy ack_policy() const noexcept { return ack_policy_; }

            /** Returns reserved config flags from the file header. */
            [[nodiscard]] uint16_t flags() const noexcept { return flags_; }

            /** Returns the node with the given id, or nullptr if absent. */
            [[nodiscard]] const NodeInfo* find_by_id(uint64_t node_id) const noexcept;

            /** Returns nodes with NodeCapability::DataBearing set. */
            [[nodiscard]] std::vector<NodeInfo> data_nodes() const;

            /** Returns nodes with NodeCapability::CoordinatorEligible set. */
            [[nodiscard]] std::vector<NodeInfo> coordinator_nodes() const;

            /** Returns true when the config should run without replication. */
            [[nodiscard]] bool is_standalone() const noexcept;

            /**
             * Validates internal consistency.
             *
             * @throws std::invalid_argument on invalid mode, policy, duplicate
             *         node ids, reserved node ids, empty hosts, unknown
             *         capabilities, or missing required node classes.
             */
            void validate() const;

        private:
            std::vector<NodeInfo> nodes_;
            ReplicationMode mode_ = ReplicationMode::Standalone;
            AckPolicy ack_policy_{};
            uint16_t flags_ = 0;
    };
} // namespace akkaradb::engine::cluster

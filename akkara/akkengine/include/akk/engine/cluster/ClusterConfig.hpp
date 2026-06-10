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
        STANDALONE = 0, MIRROR = 1, STRIPE = 2,
    };

    /**
     * AckPolicyMode - Durability policy for primary-to-replica shipping.
     */
    enum class AckPolicyMode : uint8_t {
        ASYNC = 0,
        ///< Return without waiting for replica acknowledgements.
        ALL = 1,
        ///< Wait until all currently live replicas acknowledge.
        QUORUM = 2,
        ///< Wait until at least AckPolicy::quorum replicas acknowledge.
    };

    /**
     * TransportMode - Network transport used by replication links.
     */
    enum class TransportMode : uint8_t {
        TLS = 0, PLAIN = 1,
    };

    /**
     * NodeRole - Runtime role selected by ClusterManager.
     */
    enum class NodeRole : uint8_t {
        STANDALONE = 0, PRIMARY = 1, REPLICA = 2,
    };

    /**
     * NodeCapability - Bit flags describing what a node may do.
     */
    enum NodeCapability : uint32_t {
        COORDINATOR_ELIGIBLE = 1u << 0,
        ///< Node may be selected as primary by ClusterManager.
        DATA_BEARING = 1u << 1,
        ///< Node can store key/value data and receive routed writes.
    };

    /**
     * AckPolicy - Acknowledgement rule applied by ReplicationServer.
     */
    struct AckPolicy {
        AckPolicyMode mode = AckPolicyMode::ASYNC;
        uint16_t quorum = 0; ///< Required replica count when mode == AckPolicyMode::QUORUM.
    };

    /**
     * NodeInfo - Persistent identity and connection endpoints for one node.
     */
    struct NodeInfo {
        uint64_t nodeId = 0; ///< Stable node id.  Zero is reserved.
        std::string host; ///< Hostname or address used by peer nodes.
        uint16_t dataPort = 0; ///< Public data API port.
        uint16_t replPort = 0; ///< Replication listener port.
        uint32_t capabilities = DATA_BEARING; ///< OR-ed NodeCapability flags.

        /** Returns true if this node may become primary. */
        [[nodiscard]] bool coordinatorEligible() const noexcept { return (capabilities & COORDINATOR_ELIGIBLE) != 0; }

        /** Returns true if this node participates in data placement. */
        [[nodiscard]] bool dataBearing() const noexcept { return (capabilities & DATA_BEARING) != 0; }
    };

    /**
     * ClusterTlsOptions - TLS certificate configuration for replication links.
     */
    struct ClusterTlsOptions {
        std::filesystem::path certPath; ///< Local certificate path.
        std::filesystem::path keyPath; ///< Local private-key path.
        std::filesystem::path caPath; ///< CA bundle used for peer verification.
        bool verifyPeer = true; ///< Whether TLS peers must validate against caPath.
    };

    /**
     * ClusterRuntimeOptions - Runtime-only network options.
     */
    struct ClusterRuntimeOptions {
        TransportMode transportMode = TransportMode::TLS;
        std::string replBindHost = "0.0.0.0"; ///< Local address used by the primary replication listener.
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
            ClusterConfig(std::vector<NodeInfo> nodes, ReplicationMode mode, AckPolicy ackPolicy);

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
            [[nodiscard]] AckPolicy ackPolicy() const noexcept { return ackPolicy_; }

            /** Returns reserved config flags from the file header. */
            [[nodiscard]] uint16_t flags() const noexcept { return flags_; }

            /** Returns the node with the given id, or nullptr if absent. */
            [[nodiscard]] const NodeInfo* findById(uint64_t nodeId) const noexcept;

            /** Returns nodes with NodeCapability::DATA_BEARING set. */
            [[nodiscard]] std::vector<NodeInfo> dataNodes() const;

            /** Returns nodes with NodeCapability::COORDINATOR_ELIGIBLE set. */
            [[nodiscard]] std::vector<NodeInfo> coordinatorNodes() const;

            /** Returns true when the config should run without replication. */
            [[nodiscard]] bool isStandalone() const noexcept;

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
            ReplicationMode mode_ = ReplicationMode::STANDALONE;
            AckPolicy ackPolicy_{};
            uint16_t flags_ = 0;
    };
} // namespace akkaradb::engine::cluster

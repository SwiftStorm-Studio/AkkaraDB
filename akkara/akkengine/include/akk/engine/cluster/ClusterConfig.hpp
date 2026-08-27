/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/cluster/ClusterConfig.hpp
#pragma once

#include "akkaradb/Export.hpp"

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include "akk/crypto/Identity.hpp"

namespace akkaradb::engine::cluster {
    /**
     * ReplicationMode - Placement strategy for write/read routing.
     *
     * Standalone keeps all traffic local.  Mirror sends writes to every
     * data-bearing node.  Partitioned assigns each key to one owner node
     * using rendezvous hashing.  Stripe splits values into data and parity
     * shards placed across distinct data-bearing nodes.
     */
    enum class ReplicationMode : uint8_t {
        STANDALONE = 0, MIRROR = 1, PARTITIONED = 2, STRIPE = 3,
    };

    /**
     * AckPolicyMode - Required acknowledgement count for primary-to-replica shipping.
     */
    enum class AckPolicyMode : uint8_t {
        NONE = 0,
        ///< Require zero replica acknowledgements.
        ALL_TARGETS = 1,
        ///< Require acknowledgements from every current replication target.
        QUORUM = 2,
        ///< Require acknowledgements from at least AckPolicy::quorum targets.
    };

    /**
     * AckStage - Replication lifecycle stage represented by an acknowledgement.
     */
    enum class AckStage : uint8_t {
        RECEIVED = 0,
        ///< Replica received the entry bytes.
        APPLIED = 1,
        ///< Replica applied the entry to the local engine.
        DURABLE = 2,
        ///< Replica forced the applied entry through local durability sync.
    };

    /**
     * TransportMode - Network transport used by replication links.
     */
    enum class TransportMode : uint8_t {
        PLAIN = 0, SECURE = 1,
    };

    /**
     * NodeRole - Runtime role selected by ClusterManager.
     */
    enum class NodeRole : uint8_t {
        STANDALONE = 0, PRIMARY = 1, REPLICA = 2,
    };

    /**
     * NodeStartupRole - Explicit startup role for non-standalone cluster modes.
     */
    enum class NodeStartupRole : uint8_t {
        AUTO = 0, PRIMARY = 1, REPLICA = 2,
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
    struct AKDB_API AckPolicy {
        AckPolicyMode mode = AckPolicyMode::NONE;
        AckStage stage = AckStage::APPLIED;
        uint16_t quorum = 0; ///< Required replica count when mode == AckPolicyMode::QUORUM.
    };

    /** Write completion rule requested by the cluster configuration. */
    enum class WriteConsistency : uint8_t {
        LEGACY_ACK_POLICY = 0,
        ///< Use AckPolicy; preserves the v1 runtime behaviour.
        LOCAL = 1,
        ONE_REPLICA = 2,
        QUORUM = 3,
        ALL_CONFIGURED = 4,
    };

    /** Replication algorithm selected for this cluster. */
    enum class ConsistencyMode : uint8_t {
        /** Existing primary-to-replica protocol; WriteConsistency selects its ACK rule. */
        PRIMARY_ACK = 0,
        /** Fire-and-forget primary-to-replica shipping with local completion. */
        ASYNC = 1,
        /** Raft-style quorum commit over the native cluster replication transport. */
        RAFT_QUORUM = 2,
    };

    /** Behaviour when the requested write acknowledgement does not arrive in time. */
    enum class AckTimeoutAction : uint8_t {
        ACCEPT_LOCAL = 0, FAIL_ACK = 1, FAIL_WRITE = 2,
    };

    /** Policy for replicas which cannot remain within the retained replication history. */
    enum class ReplicaLagAction : uint8_t {
        ASYNC_RESYNC = 0, REJECT_REPLICA = 1, BLOCK_WRITES = 2,
    };

    /** Recovery policy for corrupt small cluster state files. */
    enum class CorruptClusterStateAction : uint8_t {
        FAIL_STARTUP = 0, BACKUP_AND_RECREATE = 1, DELETE_AND_RECREATE = 2,
    };

    /** Recovery policy for Raft log damage after the last committed entry. */
    enum class RaftLogRecoveryAction : uint8_t {
        FAIL_STARTUP = 0, TRUNCATE_UNCOMMITTED_TAIL = 1,
    };

    /** Policy for Blob payloads when RAFT_QUORUM is selected. */
    enum class RaftBlobPolicy : uint8_t {
        REJECT = 0, PRIMARY_SIDE_ONLY = 1, RAFT_LOG = 2,
    };

    enum class RaftMembershipMode : uint8_t {
        STATIC = 0, JOINT_CONSENSUS = 1,
    };

    struct AKDB_API RaftMembershipOptions {
        RaftMembershipMode mode = RaftMembershipMode::STATIC;
        bool allowOnlineVoterChanges = false;
        bool allowLearners = false;
    };

    struct AKDB_API RaftOptions {
        RaftMembershipOptions membership;
    };

    struct AKDB_API StripeOptions {
        uint8_t dataShards = 4;
        uint8_t parityShards = 2;

        [[nodiscard]] uint16_t totalShards() const noexcept {
            return static_cast<uint16_t>(dataShards) + static_cast<uint16_t>(parityShards);
        }
    };

    /**
     * Consistency controls persisted with the cluster configuration.
     *
     * The defaults deliberately retain the existing AckPolicy-based runtime
     * behaviour.  The additional modes are configuration contracts for the
     * stricter runtime paths; they are not silently mapped to a weaker policy.
     * RAFT_QUORUM derives its write quorum from data-bearing membership and
     * forces failed writes on acknowledgement timeout.
     */
    struct AKDB_API ConsistencyOptions {
        ConsistencyMode mode = ConsistencyMode::PRIMARY_ACK;
        WriteConsistency writeConsistency = WriteConsistency::LEGACY_ACK_POLICY;
        AckTimeoutAction ackTimeoutAction = AckTimeoutAction::ACCEPT_LOCAL;
        ReplicaLagAction replicaLagAction = ReplicaLagAction::ASYNC_RESYNC;
        uint32_t ackTimeoutMs = 5000;
    };

    /**
     * NodeInfo - Persistent identity and connection endpoints for one node.
     */
    struct AKDB_API NodeInfo {
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

    struct AKDB_API ClusterPeerPublicKeyPin {
        uint64_t nodeId = 0; ///< Cluster node id this key is expected to identify.
        crypto::PublicKey publicKey{}; ///< Raw X25519 static public key.
    };

    /**
     * ClusterSecureOptions - Raw-public-key secure replication options.
     */
    struct AKDB_API ClusterSecureOptions {
        std::filesystem::path identitySeedPath; ///< Persistent local identity seed; generated if missing.
        std::vector<ClusterPeerPublicKeyPin> pinnedPeers; ///< Required peer public-key pins by cluster node id when transportMode=SECURE.
        uint64_t expectedPrimaryNodeId = 0; ///< Client-side expected primary id, or 0 if unknown.
    };

    /**
     * ClusterRuntimeOptions - Runtime-only network options.
     */
    struct AKDB_API ClusterRuntimeOptions {
        TransportMode transportMode = TransportMode::SECURE;
        std::string replBindHost = "0.0.0.0"; ///< Local address used by the primary replication listener.
        NodeStartupRole startupRole = NodeStartupRole::AUTO; ///< Explicit startup role used for non-standalone modes.
        std::string primaryHost; ///< Replica-side configured primary host override.
        uint16_t primaryReplPort = 0; ///< Replica-side configured primary replication port override.
        uint64_t primaryNodeId = 0; ///< Replica-side configured primary node id override.
        uint64_t clusterGroupId = 0; ///< Non-Raft group instance id; primary generates and persists one when zero.
        uint64_t clusterGroupEpoch = 0; ///< Non-Raft group epoch; primary defaults zero to epoch 1.
        std::filesystem::path clusterMembershipPath; ///< Persisted non-Raft group state for primary, membership for replica.
        bool resetClusterMembership = false; ///< Allows a replica to intentionally join a different non-Raft group.
        CorruptClusterStateAction corruptStateAction = CorruptClusterStateAction::FAIL_STARTUP;
        RaftLogRecoveryAction raftLogRecoveryAction = RaftLogRecoveryAction::FAIL_STARTUP;
        RaftBlobPolicy raftBlobPolicy = RaftBlobPolicy::REJECT;
        uint32_t raftBlobChunkSizeBytes = 1024u * 1024u;
        ClusterSecureOptions secure;
    };

    /**
     * ClusterConfig - Durable cluster membership and replication policy.
     *
     * The config is stored as a compact CRC-protected binary file.  It records
     * node identities, data/replication ports, node capabilities, replication
     * mode, and acknowledgement policy. Runtime-only transport options live in
     * ClusterRuntimeOptions and are intentionally not serialized here.
     */
    class AKDB_API ClusterConfig {
        public:
            static constexpr uint32_t MAGIC = 0x35434B41; // "AKC5"
            static constexpr uint16_t VERSION = 4;

            ClusterConfig() = default;

            /**
             * Creates a config and validates it immediately.
             *
             * @throws std::invalid_argument if node ids, capabilities, mode, or
             *         acknowledgement policy are invalid.
             */
            ClusterConfig(
                std::vector<NodeInfo> nodes,
                ReplicationMode mode,
                AckPolicy ackPolicy,
                ConsistencyOptions consistency = {},
                RaftOptions raft = {},
                StripeOptions stripe = {}
            );

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

            /** Returns the configured write and replica-lag consistency controls. */
            [[nodiscard]] ConsistencyOptions consistency() const noexcept { return consistency_; }

            /** Returns RAFT-specific configuration. */
            [[nodiscard]] RaftOptions raft() const noexcept { return raft_; }

            /** Returns erasure-stripe layout options. */
            [[nodiscard]] StripeOptions stripe() const noexcept { return stripe_; }

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
            ConsistencyOptions consistency_{};
            RaftOptions raft_{};
            StripeOptions stripe_{};
            uint16_t flags_ = 0;
    };
} // namespace akkaradb::engine::cluster

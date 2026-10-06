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

#include <array>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <stop_token>
#include <string>
#include <vector>

#include "akk/crypto/Identity.hpp"

namespace akkaradb::engine::cluster {
    using ClusterId = std::array<uint8_t, 16>;

    /**
     * ReplicationMode - Placement strategy for write/read routing.
     *
     * Standalone keeps all traffic local.  Mirror sends writes to every
     * data-bearing node. Partitioned selects an owner and a bounded set of
     * replicas using rendezvous hashing. Stripe splits values into data and optional
     * parity shards placed across distinct data-bearing nodes.
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
        PLAIN = 0, ///< Intentionally unauthenticated; node ids and ACKs are trusted only in trusted networks/processes.
        SECURE = 1,
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
        STRIPE_FAILOVER_ELIGIBLE = 1u << 2,
        ///< Sole STRIPE node allowed to replace an unavailable owner; RAID.10 uses it as its shared hot spare.
        PLACEMENT_STANDBY = 1u << 4, ///< STRIPE node joins as metadata learner without storing new placements.
        RAFT_LEARNER = 1u << 3,
        ///< Initial non-voting Raft member; committed membership controls subsequent promotion.
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
        AVAILABLE_REPLICAS = 5,
        ///< Wait for every currently connected replica; zero connected replicas still permits a local durable write.
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

    /** Automatic data-owner election and acknowledged-write guarantee. */
    enum class FailoverPolicy : uint8_t {
        NONE = 0, ///< Elections require an explicit campaign or leadership transfer.
        PRESERVE_ACKNOWLEDGED = 1, ///< Successful writes are committed by a data quorum.
        ALLOW_ACKNOWLEDGED_LOSS = 2, ///< Local journal acknowledgement may precede data-quorum commit.
        DEFAULT = 3, ///< Constructor resolves to PRESERVE for RAFT_QUORUM, otherwise NONE.
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

    /** Read routing policy for native cluster placement. */
    enum class ClusterRoutingMode : uint8_t { LOCAL_ONLY = 0, REDIRECT = 1, FORWARD = 2 };

    struct ClusterApiEndpoint {
        uint64_t nodeId = 0;
        uint16_t tcpPort = 0;
        uint16_t httpPort = 0;
        uint16_t grpcPort = 0;
    };

    enum class ClusterReadMode : uint8_t {
        LOCAL_STALE_OK = 0,
        ///< Serve reads from the local node without freshness coordination.
        OWNER_ONLY = 1,
        ///< Serve a key only when the local node owns that key.
        OWNER_LINEARIZABLE = 2,
        ///< Requires the owner; routingMode selects redirect/forward. Raft owners confirm a fresh quorum read barrier.
    };

    enum class StripeWriteCommitMode : uint8_t {
        ALL_SHARDS = 0,
        ///< Commit owner metadata only after all data/parity shards are durably acknowledged.
        DATA_SHARDS = 1,
        ///< With parity, commit after any dataShards shards are durable and rebuild the missing shards later.
    };

    enum class StripeReadCoordinatorMode : uint8_t {
        OWNER = 0,
        ///< Route stripe reads to the key owner, which gathers shards and repairs missing shards.
        LOCAL_COORDINATOR = 1,
        ///< Gather shards locally and revalidate the generation with the live owner before returning.
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
        uint8_t copiesPerShard = 1;

        [[nodiscard]] uint16_t totalShards() const noexcept {
            return static_cast<uint16_t>(dataShards) + static_cast<uint16_t>(parityShards);
        }

        [[nodiscard]] uint16_t totalPlacements() const noexcept {
            return static_cast<uint16_t>(totalShards() * copiesPerShard);
        }
    };

    struct AKDB_API PartitionOptions {
        uint16_t replicationFactor = 3; ///< Total copies, including the owner; capped by the data-node count.
        uint16_t partitionCount = 16; ///< Stable logical streams used by PARTITIONED Raft groups.
    };

    enum class ReconfigurationWritePolicy : uint8_t { WAIT, REJECT };
    enum class StripeMigrationMode : uint8_t { FREEZE, LIVE_COPY };
    enum class StripeInterruptionPolicy : uint8_t { RETAIN, CANCEL };
    struct ClusterReconfigurationOptions {
        ReconfigurationWritePolicy writePolicy = ReconfigurationWritePolicy::WAIT;
        StripeMigrationMode stripeMigrationMode = StripeMigrationMode::FREEZE; ///< LIVE_COPY freezes only for the final tail and activation.
        StripeInterruptionPolicy stripeInterruptionPolicy = StripeInterruptionPolicy::RETAIN; ///< CANCEL requires quorum; activation already started must finish.
        uint32_t timeoutMs = 60'000; ///< Overall reconfiguration budget, including admission and forwarding.
        uint64_t maxTransferBytesPerSecond = 0; ///< Per partition; zero is unlimited.
        uint16_t maxConcurrentPartitions = 1;
        bool resumePendingChanges = true; ///< Resume committed plans after failure or restart; false requires an explicit retry.
        std::function<bool()> cancelled; ///< Optional thread-safe hook; transfer and recovery workers may call it concurrently.
    };

    /** User-facing RAID presets resolved to the canonical placement fields. */
    enum class RaidPreset : uint8_t {
        RAID0 = 0,
        ///< `RAID.0`: split each value across data shards without parity or redundancy.
        RAID1 = 1,
        ///< `RAID.1`: keep a complete durable copy on every available data node.
        RAID5 = 5,
        ///< `RAID.5`: split each value across data shards with one distributed parity shard.
        RAID6 = 6,
        ///< `RAID.6`: split each value across data shards with two distributed parity shards.
        RAID10 = 10,
        ///< `RAID.10`: stripe data shards across fixed two-node mirror pairs.
    };

    /** Parameters which remain configurable within a RAID preset. */
    struct AKDB_API RaidOptions {
        RaidPreset preset = RaidPreset::RAID0;
        ///< Data-shard count for RAID.0, RAID.5, RAID.6, and RAID.10. Ignored by RAID.1.
        uint8_t dataShards = 4;
        ///< Fixed RAID.1 Primary. Zero selects the first eligible data node.
        uint64_t primaryNodeId = 0;
        ///< Optional RAID.10 shared hot-spare node. Zero disables hot-spare replacement.
        uint64_t hotSpareNodeId = 0;
    };

    /**
     * Consistency controls persisted with the cluster configuration.
     *
     * The defaults deliberately retain the existing AckPolicy-based runtime
     * behaviour.  The additional modes are configuration contracts for the
     * stricter runtime paths; they are not silently mapped to a weaker policy.
     * RAFT_QUORUM derives its write quorum from data-bearing voting membership and
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
        uint16_t stripeMetadataPort = 0; ///< STRIPE metadata Raft listener port; required for STRIPE data nodes.
        uint32_t capabilities = DATA_BEARING; ///< OR-ed NodeCapability flags.
        uint16_t partitionReplBasePort = 0; ///< PARTITIONED Raft reserves partitionCount + 1 consecutive ports here.

        /** Returns true if this node may become primary. */
        [[nodiscard]] bool coordinatorEligible() const noexcept { return (capabilities & COORDINATOR_ELIGIBLE) != 0; }

        /** Returns true if this node participates in data placement. */
        [[nodiscard]] bool dataBearing() const noexcept { return (capabilities & DATA_BEARING) != 0; }

        [[nodiscard]] bool placementStandby() const noexcept { return (capabilities & PLACEMENT_STANDBY) != 0; }
        [[nodiscard]] bool raftLearner() const noexcept { return (capabilities & RAFT_LEARNER) != 0; }

        /** Returns true if this node may replace an unavailable STRIPE owner or act as the RAID.10 hot spare. */
        [[nodiscard]] bool stripeFailoverEligible() const noexcept { return (capabilities & STRIPE_FAILOVER_ELIGIBLE) != 0; }
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
        void validatePins(std::span<const uint64_t> requiredPeerIds = {}) const;
    };

    /** Immutable identity retained by the caller across retries of one mutation. */
    struct ClusterRequestId {
        std::array<uint8_t, 16> nonce{};
        uint64_t expiresAtUnixMs = 0; ///< Immutable part of the request identity.
        auto operator<=>(const ClusterRequestId&) const = default;
    };

    enum class ClusterRequestStatus : uint8_t { APPLIED, NOT_FOUND, PENDING, EXPIRED };

    struct ClusterRequestResult {
        ClusterRequestStatus status = ClusterRequestStatus::NOT_FOUND;
        uint64_t sequence = 0;
        uint64_t logIndex = 0;
    };

    struct RaftRequestOptions {
        bool enabled = false;
        uint64_t maxRetentionMs = 24ull * 60 * 60 * 1000;
        uint32_t maxTrackedRequests = 4096;
    };

    /** Bounds Raft log growth without rebuilding a full database snapshot for ordinary writes. */
    struct RaftSnapshotOptions {
        bool enabled = true;
        uint64_t minLogEntries = 4096; ///< Compact after this many committed entries since the previous snapshot.
        uint64_t minLogBytes = 64ull * 1024 * 1024; ///< Approximate committed Raft-log bytes that trigger compaction.
        uint64_t maxIntervalMs = 5ull * 60 * 1000; ///< Maximum time between snapshots while committed entries remain uncompacted.
    };

    enum class MemoryOnlySnapshotMode : uint8_t {
        THROUGHPUT_FIRST = 0,
        COMPLETION_FIRST = 1,
    };

    /** Snapshot admission policy used when SST storage is disabled. */
    struct MemoryOnlySnapshotOptions {
        MemoryOnlySnapshotMode mode = MemoryOnlySnapshotMode::COMPLETION_FIRST;
        uint64_t maxPinnedBytes = 512ull * 1024 * 1024;
        uint32_t maxPinnedGenerations = 2;
    };

    struct AKDB_API ReplicationTransferOptions {
        void validate() const;
        uint32_t thresholdBytes = 32u * 1024u; ///< Above this payload size, use bounded chunks and temporary-file staging.
        uint32_t chunkBytes = 32u * 1024u;
        uint32_t maxConcurrentTransfers = 8;
        uint64_t maxMemoryBytes = 8ull * 1024 * 1024; ///< Transport heap reservations, not storage/cache or total process RSS.
        uint64_t maxSpoolBytes = 1024ull * 1024 * 1024; ///< Queued and incoming temporary payloads combined.
        std::filesystem::path spoolDirectory; ///< Empty selects the OS temporary directory.
        bool resumeEnabled = true; ///< Persist incomplete incoming payloads and resume them after reconnect.
        uint64_t resumeRetentionMs = 15ull * 60 * 1000; ///< Maximum age of an unclaimed partial payload.
        uint32_t maxResumeTransfers = 64; ///< Maximum retained partial payload count per spool directory.
        uint32_t resumeHandshakeTimeoutMs = 5'000; ///< Maximum wait for the receiver's durable resume offset.
    };

    /**
     * Explicit authorization for one offline non-Raft MIRROR Primary promotion.
     *
     * Promotion is accepted only when the candidate's persisted membership still
     * names previousPrimaryNodeId at previousGroupEpoch and its durable sequence
     * exactly matches expectedDurableSeq. The runtime then atomically advances the
     * group epoch and records the configured local node as Primary. Leaving this
     * disabled makes an existing membership file immutable with respect to the
     * Primary identity.
     */
    struct AKDB_API MirrorPromotionOptions {
        bool enabled = false;
        uint64_t previousPrimaryNodeId = 0;
        uint64_t previousGroupEpoch = 0;
        uint64_t expectedDurableSeq = 0;
    };

    enum class MirrorFencingMode : uint8_t { STATIC, QUORUM_FENCED, EXTERNAL_FENCED };

    struct MirrorFenceRequest {
        ClusterId clusterId{};
        uint64_t groupId = 0;
        uint64_t previousPrimaryNodeId = 0;
        uint64_t previousGroupEpoch = 0;
        uint64_t candidateNodeId = 0;
        uint64_t expectedDurableSeq = 0;
    };

    /** Embedding contract: throw on failure or uncertainty. Implementations must
     * prevent the fenced writer from restarting, not just check connectivity.
     * fence() must be idempotent for the same transition. Provider methods may
     * run concurrently and must cover already-running operations/process pauses. */
    class IMirrorFencingProvider {
        public:
            virtual ~IMirrorFencingProvider() = default;
            virtual void fence(const MirrorFenceRequest&) = 0;
            virtual void validatePrimary(const ClusterId&, uint64_t groupId,
                uint64_t nodeId, uint64_t epoch) = 0;
    };

    struct MirrorFencingOptions {
        MirrorFencingMode mode = MirrorFencingMode::STATIC;
        // Same ids as all configured nodes (including witnesses), with dedicated authority replication ports.
        // All participants must provide the same, ordered-independent topology.
        std::vector<NodeInfo> authorityNodes;
        std::shared_ptr<IMirrorFencingProvider> external;
    };

    enum class MirrorRecoveryMode : uint8_t { BLOCK, MANUAL, AUTOMATIC };
    enum class MirrorRecoveryAction : uint8_t { RESUME_PRIMARY, PROMOTE_PRIMARY };

    struct MirrorRecoveryRequest {
        ClusterId clusterId{};
        uint64_t groupId = 0;
        uint64_t previousPrimaryNodeId = 0;
        uint64_t previousGroupEpoch = 0;
        uint64_t operationId = 0;
        uint64_t candidateNodeId = 0;
        uint64_t expectedDurableSeq = 0;
        MirrorRecoveryAction action = MirrorRecoveryAction::RESUME_PRIMARY;
        bool operator==(const MirrorRecoveryRequest&) const = default;
    };

    struct MirrorRecoveryProof { MirrorRecoveryRequest request; };

    /** Trusted embedding contract: return a proof only after the exact old
     * operation cannot execute again. RESUME_PRIMARY must quiesce all older
     * execution instances while allowing this runtime to resume; any partial
     * effects must be recoverable by forceDurable. PROMOTE_PRIMARY must also
     * prevent the old Primary from writing/restarting under its old authority.
     * Cover in-flight operations and process pauses. Return nullopt or throw on
     * uncertainty. Be idempotent, support concurrent calls, honour cancellation,
     * and never re-enter the engine/runtime from this callback. */
    class IMirrorRecoveryProvider {
        public:
            virtual ~IMirrorRecoveryProvider() = default;
            virtual std::optional<MirrorRecoveryProof> recover(const MirrorRecoveryRequest&, std::stop_token) = 0;
    };

    /** Adapts an external fencer for promotion; cannot resume the fenced node.
     * Cancellation is checked around fence(); that callback must be bounded. */
    class AKDB_API MirrorFencingRecoveryProvider final : public IMirrorRecoveryProvider {
        std::shared_ptr<IMirrorFencingProvider> fencing_;
        public:
            explicit MirrorFencingRecoveryProvider(std::shared_ptr<IMirrorFencingProvider> fencing);
            std::optional<MirrorRecoveryProof> recover(const MirrorRecoveryRequest&, std::stop_token) override;
    };

    struct MirrorRecoveryOptions {
        MirrorRecoveryMode mode = MirrorRecoveryMode::BLOCK;
        std::shared_ptr<IMirrorRecoveryProvider> provider;
        uint32_t retryIntervalMs = 1000;
    };

    enum class QueryResultMode : uint8_t { PREPARED, STREAMING };
    enum class QuerySnapshotAdmission : uint8_t { WAIT, REJECT };

    /** Runtime-only cluster policy and network options. */
    struct AKDB_API ClusterRuntimeOptions {
        // Filled from ClusterConfig by ClusterRuntime. Direct transport users
        // must supply the same contract at both endpoints.
        ClusterId replicationClusterId{};
        std::array<uint8_t, 32> replicationConfigFingerprint{};
        ClusterRoutingMode routingMode = ClusterRoutingMode::REDIRECT;
        uint32_t forwardingTimeoutMs = 5000;
        // Result delivery and fixed-source admission are independent policies.
        QueryResultMode queryResultMode = QueryResultMode::PREPARED;
        QuerySnapshotAdmission querySnapshotAdmission = QuerySnapshotAdmission::REJECT;
        uint64_t queryMaxPinnedBytes = 512ull * 1024 * 1024;
        uint32_t queryMaxPinnedSnapshots = 64;
        uint32_t queryCursorMaxLifetimeMs = 300'000;
        // Cluster read-query cursors are ephemeral and bounded.
        uint64_t queryMaxSpoolBytes = 1024ull * 1024 * 1024;
        uint32_t queryMaxOpenCursors = 64;
        uint32_t queryCursorIdleTimeoutMs = 60'000;
        // Public ports advertised in routing errors. Without an override,
        // NodeInfo::dataPort is the TCP port; other protocols remain unspecified.
        std::vector<ClusterApiEndpoint> apiEndpoints;
        ReplicationTransferOptions transfer;
        ClusterReconfigurationOptions reconfiguration;
        uint16_t raftMaxForwardRequests = 64; ///< Active forwarding RPC channels per runtime (1-256); waiting consumes the original deadline.
        RaftRequestOptions requests;
        TransportMode transportMode = TransportMode::SECURE;
        std::string replBindHost = "0.0.0.0"; ///< Local address used by the primary replication listener.
        NodeStartupRole startupRole = NodeStartupRole::AUTO; ///< Explicit startup role used for non-standalone modes.
        std::string primaryHost; ///< Replica-side configured primary host override.
        uint16_t primaryReplPort = 0; ///< Replica-side configured primary replication port override.
        uint64_t primaryNodeId = 0; ///< Replica-side configured primary node id override.
        uint64_t clusterGroupId = 0; ///< Non-Raft group id; PARTITIONED and STRIPE require the same explicit nonzero value on every node.
        uint64_t clusterGroupEpoch = 0; ///< Non-Raft group epoch; primary defaults zero to epoch 1.
        std::filesystem::path clusterMembershipPath; ///< Persisted non-Raft group state for primary, membership for replica.
        bool resetClusterMembership = false; ///< Allows a replica to intentionally join a different non-Raft group.
        MirrorPromotionOptions mirrorPromotion; ///< One explicitly authorized offline MIRROR Primary promotion.
        MirrorFencingOptions mirrorFencing;
        MirrorRecoveryOptions mirrorRecovery; ///< Unresolved authority grants; independent of data acknowledgement policy.
        CorruptClusterStateAction corruptStateAction = CorruptClusterStateAction::FAIL_STARTUP;
        RaftLogRecoveryAction raftLogRecoveryAction = RaftLogRecoveryAction::FAIL_STARTUP;
        RaftBlobPolicy raftBlobPolicy = RaftBlobPolicy::REJECT;
        uint32_t raftBlobChunkSizeBytes = 1024u * 1024u;
        uint32_t raftHeartbeatIntervalMs = 100; ///< Leader heartbeat interval; valid range is 10-1000 ms.
        uint64_t raftMaxReceiveMemoryBytes = 64ull * 1024 * 1024; ///< Aggregate Raft frame receive/decode reservations across connections.
        ClusterReadMode readMode = ClusterReadMode::LOCAL_STALE_OK;
        StripeWriteCommitMode stripeWriteCommitMode = StripeWriteCommitMode::DATA_SHARDS;
        StripeReadCoordinatorMode stripeReadCoordinatorMode = StripeReadCoordinatorMode::OWNER;
        bool stripeReadRepair = true;
        bool stripeAutoRebuild = true;
        uint32_t stripeRebuildIntervalMs = 1000;
        uint32_t stripeRebuildBatchKeys = 64;
        uint32_t maxReplicaQueueFrames = 16u * 1024u; ///< Per-replica live/bootstrap outbound frame queue limit; 0 disables it.
        uint64_t maxReplicaQueueBytes = 256ull * 1024ull * 1024ull; ///< Per-replica live/bootstrap queued wire bytes; 0 disables it.
        ClusterSecureOptions secure;
        RaftSnapshotOptions raftSnapshot;
        MemoryOnlySnapshotOptions memoryOnlySnapshot;
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
            static constexpr uint32_t MAGIC = 0x31434B41; // "AKC1"
            static constexpr uint16_t VERSION = 1;

            ClusterConfig();

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
                StripeOptions stripe = {},
                uint64_t primaryNodeId = 0,
                ClusterId clusterId = {},
                PartitionOptions partition = {},
                FailoverPolicy failover = FailoverPolicy::DEFAULT
            );

            /**
             * Creates a config from a user-facing RAID preset. The preset is
             * immediately normalized to the same durable fields used by the
             * native placement runtime; no parallel preset state is persisted.
             */
            ClusterConfig(
                std::vector<NodeInfo> nodes,
                RaidOptions raid,
                AckPolicy ackPolicy,
                ConsistencyOptions consistency = {},
                ClusterId clusterId = {}
            );

            /**
             * Loads and validates a cluster config file.
             *
             * @throws std::runtime_error on I/O, magic, version, CRC, or
             *         truncation errors.
             * @throws std::invalid_argument if the decoded config is invalid.
             */
            [[nodiscard]] static ClusterConfig load(const std::filesystem::path& path);
            [[nodiscard]] static ClusterConfig decode(std::span<const uint8_t> image);
            [[nodiscard]] std::vector<uint8_t> encode() const;

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

            [[nodiscard]] FailoverPolicy failover() const noexcept { return failover_; }
            [[nodiscard]] bool usesDataConsensus() const noexcept {
                return consistency_.mode == ConsistencyMode::RAFT_QUORUM || failover_ == FailoverPolicy::ALLOW_ACKNOWLEDGED_LOSS ||
                    raft_.membership.mode == RaftMembershipMode::JOINT_CONSENSUS;
            }

            /** Returns RAFT-specific configuration. */
            [[nodiscard]] RaftOptions raft() const noexcept { return raft_; }

            /** Returns erasure-stripe layout options. */
            [[nodiscard]] StripeOptions stripe() const noexcept { return stripe_; }

            [[nodiscard]] PartitionOptions partition() const noexcept { return partition_; }
            /** Effective number of holders for each PARTITIONED key, including its owner. */
            [[nodiscard]] uint16_t partitionCopies() const noexcept;

            /** Returns the RAID preset represented by the normalized fields, when one is recognized. */
            [[nodiscard]] std::optional<RaidPreset> raidPreset() const noexcept;

            /** Returns the single configured Primary for non-Raft MIRROR, or zero for other modes. */
            [[nodiscard]] uint64_t primaryNodeId() const noexcept { return primaryNodeId_; }

            /** Returns the persistent identity shared by every node using this config. */
            [[nodiscard]] const ClusterId& clusterId() const noexcept { return clusterId_; }

            /** Stable digest of durable membership, placement and replication policy. */
            [[nodiscard]] std::array<uint8_t, 32> replicationFingerprint() const;

            /** Returns reserved config flags from the file header. */
            [[nodiscard]] uint16_t flags() const noexcept { return flags_; }

            /** Returns the node with the given id, or nullptr if absent. */
            [[nodiscard]] const NodeInfo* findById(uint64_t nodeId) const noexcept;

            /** Returns nodes with NodeCapability::DATA_BEARING set. */
            [[nodiscard]] std::vector<NodeInfo> dataNodes() const;

            /** Returns STRIPE placement nodes, excluding a RAID.10 hot spare. */
            [[nodiscard]] std::vector<NodeInfo> stripePlacementNodes() const;

            /** Returns nodes with NodeCapability::COORDINATOR_ELIGIBLE set. */
            [[nodiscard]] std::vector<NodeInfo> coordinatorNodes() const;

            /** Returns the sole STRIPE failover candidate/RAID.10 hot spare, or nullptr when disabled. */
            [[nodiscard]] const NodeInfo* stripeFailoverNode() const noexcept;

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
            void validatePlacementChange(const ClusterConfig& target) const;
            void validateRuntime(uint64_t selfNodeId, const ClusterRuntimeOptions& options) const;

        private:
            std::vector<NodeInfo> nodes_;
            ReplicationMode mode_ = ReplicationMode::STANDALONE;
            AckPolicy ackPolicy_{};
            ConsistencyOptions consistency_{};
            FailoverPolicy failover_ = FailoverPolicy::NONE;
            RaftOptions raft_{};
            StripeOptions stripe_{};
            PartitionOptions partition_{};
            uint64_t primaryNodeId_ = 0;
            ClusterId clusterId_{};
            uint16_t flags_ = 0;
    };
} // namespace akkaradb::engine::cluster

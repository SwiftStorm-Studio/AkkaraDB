/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/cluster/ClusterRuntimeProvider.hpp
#pragma once

#include "akkaradb/Export.hpp"
#include "akkaradb/Stats.hpp"

#include "akk/engine/cluster/ClusterConfig.hpp"
#include "akk/engine/cluster/ReplFraming.hpp"

#include <cstdint>
#include <filesystem>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

namespace akkaradb::engine::cluster {
    /** Core-to-runtime representation of a retained mutation. */
    struct ClusterHistoryEntry {
        uint64_t seq = 0;
        uint64_t sourceNodeId = 0;
        uint8_t op = 0;
        uint8_t recordFlags = 0;
        std::vector<uint8_t> key;
        std::vector<uint8_t> value;
    };

    /** Optional Blob payload committed atomically with a Raft mutation. */
    struct ClusterBlobPayload {
        uint64_t blobId = 0;
        std::vector<uint8_t> content;
    };

    /** Mutation prepared after the Raft runtime assigns its state-machine sequence. */
    struct ClusterMutation {
        uint64_t sourceNodeId = 0;
        ReplOpType op = ReplOpType::PUT;
        uint8_t recordFlags = 0;
        std::vector<uint8_t> key;
        std::vector<uint8_t> value;
        std::optional<ClusterBlobPayload> blob;
    };

    struct ClusterMutationSubmission {
        uint64_t sequence = 0;
        std::future<void> completion;
    };

    using ClusterMutationFactory = std::function<ClusterMutation(uint64_t sequence)>;

    struct ClusterSnapshot {
        using EntryVisitor = SnapshotEntryVisitor;

        uint64_t seq = 0;
        // Re-readable, point-in-time entry stream. Keys and chunks remain valid
        // only for the duration of each visitor call. Every entry emits one or
        // more contiguous chunks (an empty value emits one empty chunk). The
        // entry count is discovered as the producer completes.
        std::function<bool(const EntryVisitor&)> forEachEntry;
    };

    struct RaftPeerStats {
        uint64_t nodeId = 0;
        bool learner = false;
        uint64_t matchIndex = 0;
        uint64_t nextIndex = 0;
        uint64_t replicationLag = 0;
        bool connected = false;
        uint64_t lastSuccessfulContactAtUs = 0;
        uint64_t roundTripsSucceeded = 0;
        uint64_t roundTripsFailed = 0;
        uint64_t consecutiveRoundTripFailures = 0;
        uint64_t lastRoundTripAtUs = 0;
        uint64_t lastRoundTripFailureAtUs = 0;
        ClusterLatencyHistogram roundTripLatencyUs;
    };

    struct RaftRuntimeStats {
        bool enabled = false;
        uint64_t sampledAtUs = 0;
        uint64_t runtimeStartedAtUs = 0;
        uint64_t clusterGroupId = 0;
        uint64_t clusterGroupEpoch = 0;
        ClusterHealthState health = ClusterHealthState::HEALTHY;
        ClusterFailureCode lastFailure = ClusterFailureCode::NONE;
        uint64_t lastFailureAtUs = 0;
        uint64_t currentTerm = 0;
        uint64_t configurationGeneration = 0;
        bool jointConsensus = false;
        uint64_t leaderNodeId = 0;
        uint64_t commitIndex = 0;
        uint64_t appliedIndex = 0;
        uint64_t appliedStateMachineSeq = 0;
        uint64_t lastLogIndex = 0;
        uint64_t snapshotIndex = 0;
        uint64_t voterCount = 0;
        uint64_t learnerCount = 0;
        bool localLearner = false;
        uint64_t mirrorAuthorityLeaderNodeId = 0;
        uint64_t mirrorAuthorityPrimaryNodeId = 0;
        uint64_t mirrorAuthorityEpoch = 0;
        uint64_t mirrorAuthorityCommitIndex = 0;
        bool mirrorWritePending = false;
        uint64_t outboundConnections = 0;
        uint64_t peerWorkers = 0;
        uint64_t proposalBatches = 0;
        uint64_t proposalQueueDepth = 0;
        uint64_t pendingProposals = 0;
        uint64_t retainedRequestResults = 0;
        uint64_t pendingRequests = 0;
        uint64_t requestCapacity = 0;
        uint64_t expiredRequests = 0;
        uint64_t rejectedRequests = 0;
        uint64_t requestJournalBytes = 0;
        uint64_t requestJournalRecords = 0;
        uint64_t requestJournalBytesWritten = 0;
        uint64_t requestJournalCompactions = 0;
        uint64_t peerPolicyMismatchRejects = 0;
        uint64_t foreignClusterRejects = 0;
        uint64_t leaseRenewFailures = 0;
        uint64_t endpointStartFailures = 0;
        uint64_t peerReadTimeouts = 0;
        uint64_t replicationQueueFrames = 0;
        uint64_t replicationQueueBytes = 0;
        uint64_t transferMemoryBytes = 0;
        uint64_t transferSpoolBytes = 0;
        uint64_t activeTransfers = 0;
        uint64_t transferResumeAttempts = 0;
        uint64_t transferResumed = 0;
        uint64_t transferResumedBytes = 0;
        uint64_t transferDiscardedPartials = 0;
        uint64_t transferRetainedPartials = 0;
        std::vector<RaftPeerStats> peers;
    };

    struct AKDB_API ClusterEngineCallbacks {
        // Executes one public operation at its destination. Never forwards again.
        std::function<ForwardResponse(const ForwardRequest&)> forward;
        std::function<uint64_t()> getCurrentSeq;
        std::function<uint64_t()> getLastSeq;
        // The leader of the stable per-key Raft group, or zero while unknown.
        std::function<uint64_t(std::span<const uint8_t>)> partitionLeader;
        std::function<std::vector<NodeInfo>(std::span<const uint8_t>)> partitionCandidates;
        std::function<void(const ClusterConfig&, uint64_t generation)> placementChanged;
        // Returns a complete, contiguous (afterSeq, throughSeq] mutation range,
        // or nullopt when the retained WAL cannot satisfy the request.
        std::function<std::optional<std::vector<ClusterHistoryEntry>>(uint64_t afterSeq, uint64_t throughSeq)> getEntries;
        std::function<std::optional<ClusterSnapshot>()> exportSnapshot;
        // Replaces only this owner's PARTITIONED keys using local storage sequences.
        // The completed producer is repeatable; return only after durable install.
        std::function<void(uint64_t ownerNodeId, const ClusterSnapshot&)> installPartitionSnapshot;
        // entryCount is zero while a streaming producer has not completed.
        std::function<void(uint64_t snapshotSeq, uint64_t entryCount)> beginSnapshot;
        std::function<void(std::span<const uint8_t> key, uint64_t valueSize, uint32_t valueCrc32c)> beginSnapshotEntry;
        std::function<void(uint64_t offset, std::span<const uint8_t> chunk)> appendSnapshotEntryChunk;
        std::function<void()> finishSnapshotEntry;
        // Validate and sync receive staging before Raft publishes install intent.
        std::function<void(uint64_t snapshotSeq, uint64_t entryCount)> prepareSnapshot;
        // Receives the exact count observed by the completed producer.
        std::function<void(uint64_t snapshotSeq, uint64_t entryCount)> finishSnapshot;
        std::function<void(uint64_t snapshotSeq)> recoverSnapshot;
        std::function<bool(uint64_t snapshotSeq)> isSnapshotDurable;
        std::function<void(
            uint64_t seq,
            ReplOpType op,
            std::span<const uint8_t> key,
            std::span<const uint8_t> value,
            uint8_t recordFlags,
            uint64_t sourceNodeId,
            uint64_t timestampNs
        )> apply;
        std::function<ReadResponse(std::span<const uint8_t> key, uint64_t snapshotSeq)> read;
        // Applies an authoritative STRIPE metadata entry after the metadata
        // Raft leader validates its fencing token and reaches quorum commit.
        std::function<void(uint64_t logicalSeq, std::span<const uint8_t> publicKey, std::span<const uint8_t> metadata)> commitStripeMetadata;
        std::function<std::optional<std::vector<uint8_t>>(std::span<const uint8_t> publicKey)> readStripeMetadata;
        // Fixed cut of STRIPE metadata heads, retained revisions and placement aliases.
        std::function<std::optional<ClusterSnapshot>(uint64_t sequence)> exportStripeMetadataSnapshot;
        std::function<void(const ClusterSnapshot&, uint64_t afterAuthoritySequence)> installStripeMetadataSnapshot;
        // Node-to-node rollback/checkpoint control. The framing reuses the
        // bounded STRIPE control envelope and inherits the selected transport's
        // authentication properties; these actions are available to every
        // partition owner.
        std::function<StripeControlResponse(uint64_t requesterNodeId, const StripeControlRequest&)> rollbackControl;
        std::function<void()> forceDurable;
        std::function<void(uint64_t seq, uint64_t blobId, uint64_t totalSize, uint32_t contentCrc32c)> beginBlob;
        std::function<void(uint64_t seq, uint64_t blobId, uint64_t offset, std::span<const uint8_t> chunk)> appendBlobChunk;
        std::function<void(uint64_t seq, uint64_t blobId)> finishBlob;
        std::function<void(uint64_t seq, uint64_t blobId)> abortBlob;
        std::function<void(NodeRole role)> roleChange;
    };

    struct StripeOperationLease {
        uint64_t ownerNodeId = 0;
        uint64_t authorityNodeId = 0;
        uint64_t fenceToken = 0;
        bool coordinated = false;
        std::vector<uint8_t> metadata;
    };

    /** Transient loss of the STRIPE metadata-Raft leader or quorum. */
    class StripeMetadataUnavailable final : public std::runtime_error {
        public:
            using std::runtime_error::runtime_error;
    };

    struct ClusterPlacementState {
        uint64_t leaderNodeId = 0;
        uint64_t generation = 1;
        uint64_t pendingGeneration = 0;
        uint64_t lastIssuedGeneration = 1;
        bool stripeWritesBlocked = false;
        bool activationStarted = false;
        bool cancelOnInterruption = false;
        std::vector<uint8_t> activeConfig;
        std::vector<uint8_t> pendingConfig;
    };

    class AKDB_API IClusterRuntime {
        public:
            virtual ~IClusterRuntime() = default;

            IClusterRuntime(const IClusterRuntime&) = delete;
            IClusterRuntime& operator=(const IClusterRuntime&) = delete;

            virtual void start() = 0;
            virtual void close() = 0;
            [[nodiscard]] virtual RaftRuntimeStats raftStats() const { return {}; }
            [[nodiscard]] virtual RaftRuntimeStats stripeMetadataRaftStats() const { return {}; }
            [[nodiscard]] virtual NodeRole role() const noexcept { return NodeRole::STANDALONE; }
            [[nodiscard]] virtual std::vector<NodeInfo> activeNodes() const { return {}; }
            [[nodiscard]] virtual bool ownsWriteKey(std::span<const uint8_t>) const { return true; }
            [[nodiscard]] virtual ClusterRouteTarget routeTarget(std::span<const uint8_t>) const { return {}; }
            [[nodiscard]] virtual ForwardResponse forwardTo(uint64_t, ForwardRequest) {
                throw ClusterRoutingError(ClusterRoutingCode::FORWARD_UNAVAILABLE);
            }
            [[nodiscard]] virtual ReadResponse linearizableReadKey(std::span<const uint8_t> key) { return readKey(key, 0); }
            virtual void queryReadBarrier() {
                throw std::runtime_error("IClusterRuntime: cluster query read barrier is unsupported");
            }
            [[nodiscard]] virtual uint64_t ownerNodeId(std::span<const uint8_t>) const { return 0; }
            [[nodiscard]] virtual uint64_t configurationEpoch() const noexcept { return 1; }
            [[nodiscard]] virtual uint64_t stripeMetadataLinearizableWatermark() {
                throw std::runtime_error("IClusterRuntime: STRIPE metadata watermark is not supported");
            }
            [[nodiscard]] virtual StripeControlResponse rollbackControl(uint64_t, StripeControlRequest) {
                throw std::runtime_error("IClusterRuntime: distributed rollback control is not supported");
            }
            [[nodiscard]] virtual ReadResponse readKey(std::span<const uint8_t>, uint64_t) {
                throw std::runtime_error("IClusterRuntime: cluster reads are not supported");
            }
            [[nodiscard]] virtual ReadResponse readKeyFromNode(uint64_t, std::span<const uint8_t>, uint64_t) {
                throw std::runtime_error("IClusterRuntime: targeted cluster reads are not supported");
            }
            [[nodiscard]] virtual StripeOperationLease acquireStripeOperation(std::span<const uint8_t>, uint64_t ownerNodeId) {
                return {.ownerNodeId = ownerNodeId, .authorityNodeId = ownerNodeId, .fenceToken = 0, .coordinated = false, .metadata = {}};
            }
            virtual void commitStripeMetadata(const StripeOperationLease&, std::span<const uint8_t>, std::span<const uint8_t>) {
                throw std::runtime_error("IClusterRuntime: coordinated STRIPE metadata is not supported");
            }
            virtual void releaseStripeOperation(const StripeOperationLease&, std::span<const uint8_t>) noexcept {}
            [[nodiscard]] virtual uint64_t stripeFailoverNodeId() const noexcept { return 0; }
            [[nodiscard]] virtual uint64_t stripeMetadataLeaderNodeId() const noexcept { return 0; }
            [[nodiscard]] virtual bool stripeNodeReachable(uint64_t) const noexcept { return false; }
            [[nodiscard]] virtual std::optional<std::vector<uint8_t>> readStripeMetadata(std::span<const uint8_t>, uint64_t) {
                throw std::runtime_error("IClusterRuntime: coordinated STRIPE metadata is not supported");
            }
            virtual bool repairStripeMetadata(std::span<const uint8_t>, uint64_t, std::span<const uint8_t>) {
                throw std::runtime_error("IClusterRuntime: coordinated STRIPE metadata repair is not supported");
            }
            virtual void shipEntry(
                uint64_t seq,
                ReplOpType op,
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint8_t recordFlags,
                uint64_t sourceNodeId
            ) = 0;
            // Copies the mutation before returning; completion means quorum commit
            // and durable local apply. Only Raft runtimes support async admission.
            virtual std::future<void> submitEntry(
                uint64_t, ReplOpType, std::span<const uint8_t>, std::span<const uint8_t>, uint8_t, uint64_t
            ) { throw std::runtime_error("IClusterRuntime: async proposal admission is not supported"); }
            // Assigns the state-machine sequence, invokes prepare synchronously,
            // and commits its optional Blob and mutation in one Raft proposal.
            virtual ClusterMutationSubmission submitMutation(ClusterMutationFactory) {
                throw std::runtime_error("IClusterRuntime: runtime-assigned mutation admission requires Raft");
            }
            virtual std::shared_future<ClusterRequestResult> submitRequest(
                const ClusterRequestId&, const std::array<uint8_t, 32>&, ClusterMutationFactory
            ) { throw std::runtime_error("IClusterRuntime: retry-safe requests require Raft"); }
            virtual ClusterRequestResult queryRequest(const ClusterRequestId&) {
                throw std::runtime_error("IClusterRuntime: retry-safe requests require Raft");
            }
            virtual void shipEntryTo(
                uint64_t targetNodeId,
                uint64_t seq,
                ReplOpType op,
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint8_t recordFlags,
                uint64_t sourceNodeId,
                bool waitForAck = true
            ) {
                (void)targetNodeId;
                (void)waitForAck;
                shipEntry(seq, op, key, value, recordFlags, sourceNodeId);
            }
            virtual void shipBlob(uint64_t seq, uint64_t blobId, std::span<const uint8_t> content) = 0;
            virtual void campaignLeadership() { throw std::runtime_error("IClusterRuntime: explicit campaign requires data consensus"); }
            virtual void reconfigure(ClusterConfig, uint64_t = 0) {
                throw std::runtime_error("IClusterRuntime: cluster reconfiguration is not supported");
            }
            virtual void preparePlacementNodes(const ClusterConfig&) { throw std::logic_error("IClusterRuntime: placement transport unavailable"); }
            virtual void waitForStripeOperations() { throw std::logic_error("IClusterRuntime: stripe authority unavailable"); }
            virtual void publishStripePlacementAlias(std::span<const uint8_t>, std::span<const uint8_t>, uint64_t) {
                throw std::logic_error("IClusterRuntime: stripe authority unavailable");
            }
            [[nodiscard]] virtual ClusterPlacementState placementState(bool = false) { return {}; }
            virtual uint64_t beginPlacementChange(const ClusterConfig&) {
                throw std::runtime_error("IClusterRuntime: placement authority is unavailable");
            }
            virtual void finishPlacementChange(uint64_t) {
                throw std::runtime_error("IClusterRuntime: placement authority is unavailable");
            }
            virtual void freezeStripePlacement(uint64_t) { throw std::logic_error("IClusterRuntime: stripe authority unavailable"); }
            virtual void cancelPlacementChange(uint64_t) { throw std::logic_error("IClusterRuntime: placement authority unavailable"); }

            virtual void addRaftLearner(const NodeInfo&) {
                throw std::runtime_error("ClusterRuntimeProvider: Raft learners are unsupported");
            }
            virtual void executePrimaryWrite(const std::function<void()>& write) { write(); }
            // Returns true if a pending grant was resolved; false if none remains
            // or the configured provider cannot yet prove safe recovery.
            virtual bool recoverMirrorWrite() { throw std::runtime_error("IClusterRuntime: MIRROR authority recovery is unavailable"); }
            virtual void cancelMirrorRecovery() noexcept {}
            [[nodiscard]] virtual MirrorFencingMode mirrorFencingMode() const noexcept { return MirrorFencingMode::STATIC; }
            [[nodiscard]] virtual MirrorRecoveryMode mirrorRecoveryMode() const noexcept { return MirrorRecoveryMode::BLOCK; }
            virtual void transferMirrorAuthorityLeadership(uint64_t) {
                throw std::runtime_error("IClusterRuntime: MIRROR authority quorum is not enabled");
            }
            virtual void promoteRaftLearner(uint64_t) {
                throw std::runtime_error("ClusterRuntimeProvider: Raft learners are unsupported");
            }
            virtual void removeRaftLearner(uint64_t) {
                throw std::runtime_error("ClusterRuntimeProvider: Raft learners are unsupported");
            }
            virtual void addRaftVotingNode(const NodeInfo&) {
                throw std::runtime_error("IClusterRuntime: online Raft membership change is not supported");
            }

            virtual void removeRaftVotingNode(uint64_t) {
                throw std::runtime_error("IClusterRuntime: online Raft membership change is not supported");
            }

            virtual void transferRaftLeadership(uint64_t) {
                throw std::runtime_error("IClusterRuntime: Raft leader transfer is not supported");
            }

        protected:
            IClusterRuntime() = default;
    };

    using ClusterRuntimeFactory = std::unique_ptr<IClusterRuntime> (*)(
        std::filesystem::path,
        ClusterConfig,
        uint64_t,
        ClusterEngineCallbacks,
        ClusterRuntimeOptions
    );

    AKDB_API bool registerClusterRuntimeFactory(ClusterRuntimeFactory factory) noexcept;
    [[nodiscard]] AKDB_API bool clusterRuntimeFactoryAvailable() noexcept;
    [[nodiscard]] AKDB_API bool loadClusterRuntimeBackend(const std::filesystem::path& libraryPath = {});
    [[nodiscard]] AKDB_API std::string lastClusterRuntimeBackendLoadError();
    [[nodiscard]] AKDB_API std::unique_ptr<IClusterRuntime> createClusterRuntime(
        std::filesystem::path dbDir,
        ClusterConfig config,
        uint64_t selfNodeId,
        ClusterEngineCallbacks callbacks,
        ClusterRuntimeOptions runtimeOptions = {}
    );
}

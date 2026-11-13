/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/AkkEngineStats.cpp
#include "detail/AkkEngineImpl.hpp"

namespace akkaradb::engine {
    EngineStats AkkEngine::Impl::stats() noexcept {
    EngineStats out;
    OperationGuard operation{*this, false};
    if (!operation) { return out; }

    out.currentSeq = snapshotSeq();
    out.nodeId = nodeId;
    {
        std::lock_guard lock{stripeRepairStatsMu};
        out.stripeReadRepair = stripeRepairStats;
    }
    {
        std::lock_guard lock{stripeRebuildStatsMu};
        out.stripeRebuild = stripeRebuildStats;
    }

    out.putsTotal = putsTotal.load(std::memory_order_relaxed);
    out.removesTotal = removesTotal.load(std::memory_order_relaxed);
    out.getsTotal = getsTotal.load(std::memory_order_relaxed);
    out.getsMemtableHit = getsMemtableHit.load(std::memory_order_relaxed);
    out.getsSstHit = getsSstHit.load(std::memory_order_relaxed);
    out.getsMiss = getsMiss.load(std::memory_order_relaxed);
    out.existsTotal = existsTotal.load(std::memory_order_relaxed);
    out.scansTotal = scansTotal.load(std::memory_order_relaxed);
    out.blobPutsTotal = blobPutsTotal.load(std::memory_order_relaxed);
    out.config.writeAdmission = static_cast<uint32_t>(opts.runtime.writeAdmission);
    out.config.writeDurability = static_cast<uint32_t>(opts.runtime.writeDurability);
    out.config.writeVisibility = static_cast<uint32_t>(opts.runtime.writeVisibility);
    out.config.readVisibility = static_cast<uint32_t>(opts.runtime.visibility.readVisibility);
    out.config.sequenceAllocation = static_cast<uint32_t>(opts.runtime.sequence.allocation);
    out.config.sequenceThreadLocalRangeSize = opts.runtime.sequence.threadLocalRangeSize;
    out.config.commitWindowSize = commitWindowSize;
    out.config.memtableBackpressureMode = static_cast<uint32_t>(opts.runtime.backpressure.memtableFlushBacklog);
    out.config.maxMemtableImmutableTables = opts.runtime.backpressure.maxMemtableImmutableTables;
    out.config.sstBackpressureMode = static_cast<uint32_t>(opts.runtime.backpressure.sstCompactionBacklog);
    out.config.maxSstL0Files = opts.runtime.backpressure.maxSstL0Files;
    out.config.backpressureWaitMicros = opts.runtime.backpressure.waitMicros;
    out.config.backpressureTimeoutMs = opts.runtime.backpressure.timeoutMs;
    out.config.memtableFlushMode = static_cast<uint32_t>(opts.memtable.flushMode);
    out.config.walExecution = static_cast<uint32_t>(opts.wal.execution);
    out.config.walSyncPolicy = static_cast<uint32_t>(opts.wal.syncPolicy);
    out.config.walBackpressure = static_cast<uint32_t>(opts.wal.backpressure);
    out.config.sstCompactionMode = static_cast<uint32_t>(opts.sst.compactionMode);
    out.backpressure.blockedWrites = backpressureBlockedWrites.load(std::memory_order_relaxed);
    out.backpressure.rejectedWrites = backpressureRejectedWrites.load(std::memory_order_relaxed);
    out.backpressure.timedOutWrites = backpressureTimedOutWrites.load(std::memory_order_relaxed);
    out.backpressure.memtableStalls = backpressureMemtableStalls.load(std::memory_order_relaxed);
    out.backpressure.sstStalls = backpressureSstStalls.load(std::memory_order_relaxed);
    out.backpressure.waitMicrosTotal = backpressureWaitMicrosTotal.load(std::memory_order_relaxed);
    out.backpressure.waitMicrosMax = backpressureWaitMicrosMax.load(std::memory_order_relaxed);
    out.api.enabled = opts.components.apiEnabled;
    if (apiServer) { out.api = apiServer->stats(); }

    if (memtable) {
        const auto snap = memtable->snapshot();
        out.memtable.shardCount = snap.shardCount;
        out.memtable.thresholdBytesPerShard = snap.thresholdBytesPerShard;
        out.memtable.approxBytes = snap.approxBytes;
        out.memtable.putsApplied = snap.putsApplied;
        out.memtable.removesApplied = snap.removesApplied;
        out.memtable.flushesCompleted = snap.flushesCompleted;
        out.memtable.immutableTables = snap.immutableTables;
        out.memtable.memorySnapshotCompactionsCompleted = snap.memorySnapshotCompactionsCompleted;
        out.memtable.memorySnapshotCompactionFailures = snap.memorySnapshotCompactionFailures;
        out.memtable.memorySnapshotCompactionLastFailureAtUs = snap.memorySnapshotCompactionLastFailureAtUs;
        out.memtable.memorySnapshotCompactionPending = snap.memorySnapshotCompactionPending;
    }

    out.wal.enabled = opts.components.walEnabled;
    if (walWriter) {
        const auto snap = walWriter->snapshot();
        out.wal.shardCount = snap.shardCount;
        out.wal.entriesWritten = snap.entriesWritten;
        out.wal.bytesWritten = snap.bytesWritten;
        out.wal.batchesFlushed = snap.batchesFlushed;
        out.wal.syncsExecuted = snap.syncsExecuted;
        out.wal.segmentRotations = snap.segmentRotations;
        out.wal.asyncFailures = snap.asyncFailures;
        out.wal.pendingEntries = snap.pendingEntries;
        out.wal.pendingBytes = snap.pendingBytes;
        out.wal.inFlightBytes = snap.inFlightBytes;
        out.wal.healthy = snap.healthy;
    }

    out.blob.enabled = opts.components.blobEnabled && blobManager != nullptr;
    out.blob.thresholdBytes = opts.blob.thresholdBytes;
    if (blobManager) {
        const auto snap = blobManager->snapshot();
        out.blob.blobsWritten = snap.blobsWritten;
        out.blob.bytesUncompressed = snap.bytesUncompressed;
        out.blob.bytesOnDisk = snap.bytesOnDisk;
        out.blob.blobsDeleted = snap.blobsDeleted;
        out.blob.gcCycles = snap.gcCycles;
    }

    out.sst.enabled = sstManager != nullptr;
    if (sstManager) {
        const auto levels = sstManager->levelStats();
        out.sst.levels.reserve(levels.size());
        for (const auto& level : levels) {
            out.sst.levels.push_back(LevelStats{level.level, level.fileCount, level.bytes, level.budgetBytes});
            out.sst.fileCount += level.fileCount;
            out.sst.bytes += level.bytes;
            if (level.level == 0) { out.sst.l0FileCount = level.fileCount; }
        }
        out.sst.compactionPending = sstManager->compactionPending();
        const auto snap = sstManager->compactionSnapshot();
        out.sst.compactionsCompleted = snap.compactionsCompleted;
        out.sst.filesCompacted = snap.filesCompacted;
        out.sst.bytesCompactedIn = snap.bytesCompactedIn;
        out.sst.bytesCompactedOut = snap.bytesCompactedOut;
        out.sst.compactionFailures = snap.compactionFailures;
    }

    out.manifest.enabled = manifest != nullptr;
    if (manifest) {
        const auto checkpoint = manifest->lastCheckpoint();
        out.manifest.hasCheckpoint = checkpoint.has_value();
        if (checkpoint.has_value()) {
            out.manifest.lastCheckpointSeq = checkpoint->lastSeq.value_or(0);
            out.manifest.lastCheckpointStripe = checkpoint->stripe.value_or(0);
        }
        out.manifest.liveSstCount = manifest->liveSst().size();
        out.manifest.deletedSstCount = manifest->deletedSst().size();
        out.manifest.sstSealCount = manifest->sstSeals().size();
        out.manifest.sstReferencedBlobCount = manifest->sstReferencedBlobs().size();
        out.manifest.sstBlobRefsComplete = manifest->sstBlobRefsComplete();
        out.manifest.liveBlobCount = manifest->liveBlobs().size();
        out.manifest.deletedBlobCount = manifest->deletedBlobs().size();
        out.manifest.blobPutCount = manifest->blobPuts().size();
        out.manifest.blobDeleteCount = manifest->blobDeletes().size();
        if (const auto lease = manifest->lastPrimaryLease(); lease.has_value()) {
            out.manifest.lastPrimaryLeaseNodeId = lease->nodeId;
            out.manifest.lastPrimaryLeaseUntilUs = lease->leaseUntilUs;
        }
    }

    out.cluster.enabled = opts.components.clusterEnabled && clusterRuntime != nullptr;
    out.cluster.configuredNodeCount = clusterConfiguredNodeCount;
    if (out.cluster.enabled) {
        out.cluster.clusterId = clusterConfig.clusterId();
        out.cluster.replicationMode = static_cast<uint32_t>(clusterConfig.mode());
        out.cluster.consistencyMode = static_cast<uint32_t>(clusterConfig.consistency().mode);
        out.cluster.transportMode = static_cast<uint32_t>(opts.cluster.runtime.transportMode);
        out.cluster.clusterGroupId = opts.cluster.runtime.clusterGroupId;
        out.cluster.clusterGroupEpoch = opts.cluster.runtime.clusterGroupEpoch;
        const auto configured = usesPartitionRaft() || clusterReplicationMode == cluster::ReplicationMode::STRIPE
                                    ? currentPlacement().nodes()
                                    : clusterConfig.usesDataConsensus()
                                    ? clusterRuntime->activeNodes()
                                    : clusterConfig.nodes();
        out.cluster.configuredNodeCount = static_cast<uint32_t>(configured.size());
        out.cluster.configuredNodes.reserve(configured.size());
        for (const auto& node : configured) {
            out.cluster.configuredNodes.push_back(
                {
                    .nodeId = node.nodeId,
                    .host = node.host,
                    .dataPort = node.dataPort,
                    .replPort = node.replPort,
                    .stripeMetadataPort = node.stripeMetadataPort,
                    .capabilities = node.capabilities,
                    .hostId = node.hostId, .siteId = node.siteId,
                }
            );
        }
    }
    if (clusterRuntime) {
        {
            std::lock_guard lock{distributedRecoveryDiagnosticsMu};
            const auto& d = distributedRecoveryDiagnostics;
            out.cluster.transactionRecoveryFailuresTotal = d.failuresTotal;
            out.cluster.transactionRecoveryConsecutiveFailedPasses = d.consecutiveFailedPasses;
            out.cluster.transactionRecoveryLastFailureAtUs = d.lastFailureAtUs;
            out.cluster.transactionRecoveryFailingSinceUs = d.failingSinceUs;
            out.cluster.transactionRecoveryLastSuccessfulPassAtUs = d.lastSuccessfulPassAtUs;
            out.cluster.transactionRecoveryLastFailedId = d.lastFailedId;
            out.cluster.transactionRecoveryLastError.assign(d.lastError.data(), d.lastErrorSize);
        }
        {
            std::lock_guard lock{distributedStateMu};
            out.cluster.pendingDistributedTransactions = std::ranges::count_if(distributedState.decisions,
                [](const auto& item) { return !item.second.finished; });
        }
        out.cluster.role = static_cast<uint32_t>(clusterRuntime->role());
        out.cluster.activeNodeCount = clusterRuntime->activeNodes().size();
        const auto runtime = clusterRuntime->raftStats();
        const auto metadataRuntime = clusterRuntime->stripeMetadataRaftStats();
        out.cluster.sampledAtUs = runtime.sampledAtUs;
        out.cluster.runtimeStartedAtUs = runtime.runtimeStartedAtUs;
        out.cluster.clusterGroupId = runtime.clusterGroupId;
        out.cluster.clusterGroupEpoch = runtime.clusterGroupEpoch;
        out.cluster.health = runtime.health;
        out.cluster.quorumAvailability = runtime.quorumAvailability;
        out.cluster.writeAvailability = runtime.writeAvailability;
        out.cluster.linearizableReadAvailability = runtime.linearizableReadAvailability;
        out.cluster.lastFailure = runtime.lastFailure;
        out.cluster.lastFailureAtUs = runtime.lastFailureAtUs;
        const auto observeParticipant = [](auto& observed, Impl& engine) {
            if (!engine.distributedTransactionsEnabled()) { return; }
            std::lock_guard lock{engine.distributedStateMu};
            for (const auto& [id, participant] : engine.distributedState.participants) {
                if (!participant.keys.empty() || !participant.ranges.empty()) {
                    const bool entireKeyspace = std::ranges::any_of(participant.ranges, [](const auto& range) { return range.first.empty() && range.second.empty(); });
                    observed.writeAvailability = cluster::detail::combineAvailability(observed.writeAvailability,
                        entireKeyspace ? ClusterOperationAvailability::UNAVAILABLE : ClusterOperationAvailability::UNKNOWN);
                    if (participant.sealed || participant.applyingSequence) {
                        observed.linearizableReadAvailability = cluster::detail::combineAvailability(observed.linearizableReadAvailability,
                            entireKeyspace ? ClusterOperationAvailability::UNAVAILABLE : ClusterOperationAvailability::UNKNOWN);
                    }
                }
            }
        };
        if (metadataRuntime.enabled) { cluster::detail::mergeHealthObservation(out.cluster, metadataRuntime); }
        if (usesPartitionRaft()) {
            const auto placement = currentPlacement();
            const auto children = partitionSnapshot();
            for (size_t index = 0; index < children.size(); ++index) {
                const auto& child = children[index];
                // Removed holders retain storage/runtime objects; committed placement defines dependencies.
                if (!partitionConfig(placement, index).findById(nodeId)) {
                    out.cluster.quorumAvailability = cluster::detail::combineAvailability(out.cluster.quorumAvailability, ClusterOperationAvailability::UNKNOWN);
                    out.cluster.writeAvailability = cluster::detail::combineAvailability(out.cluster.writeAvailability, ClusterOperationAvailability::UNKNOWN);
                    out.cluster.linearizableReadAvailability = cluster::detail::combineAvailability(out.cluster.linearizableReadAvailability, ClusterOperationAvailability::UNKNOWN);
                    continue;
                }
                if (child) {
                    OperationGuard childOperation{*child->impl_, false};
                    cluster::RaftRuntimeStats observed;
                    if (childOperation) {
                        observed = child->impl_->clusterRuntime->raftStats();
                        observeParticipant(observed, *child->impl_);
                    }
                    else {
                        observed.health = ClusterHealthState::DEGRADED;
                        observed.quorumAvailability = observed.writeAvailability = observed.linearizableReadAvailability = ClusterOperationAvailability::UNAVAILABLE;
                    }
                    cluster::detail::mergeHealthObservation(out.cluster, observed);
                }
                else {
                    out.cluster.health = cluster::detail::combineHealth(out.cluster.health, ClusterHealthState::DEGRADED);
                    out.cluster.quorumAvailability = out.cluster.writeAvailability = out.cluster.linearizableReadAvailability = ClusterOperationAvailability::UNAVAILABLE;
                }
            }
        }
        observeParticipant(out.cluster, *this);
        // Preserve the existing rebuilding signal while service prerequisites remain observable.
        if (out.stripeRebuild.active && out.cluster.health != ClusterHealthState::FAILED &&
            out.cluster.quorumAvailability != ClusterOperationAvailability::UNAVAILABLE &&
            out.cluster.linearizableReadAvailability != ClusterOperationAvailability::UNAVAILABLE) {
            out.cluster.health = ClusterHealthState::REBUILDING;
        }
        out.cluster.raftEnabled = runtime.enabled;
        if (runtime.enabled) {
            out.cluster.activeNodeCount = 1;
            for (const auto& peer : runtime.peers) { if (peer.connected) { ++out.cluster.activeNodeCount; } }
        }
        out.cluster.raftTerm = runtime.currentTerm;
        out.cluster.leaderNodeId = runtime.leaderNodeId;
        out.cluster.commitIndex = runtime.commitIndex;
        out.cluster.appliedIndex = runtime.appliedIndex;
        out.cluster.lastLogIndex = runtime.lastLogIndex;
        out.cluster.snapshotIndex = runtime.snapshotIndex;
        out.cluster.voterCount = runtime.voterCount;
        out.cluster.learnerCount = runtime.learnerCount;
        out.cluster.localLearner = runtime.localLearner;
        out.cluster.mirrorAuthorityLeaderNodeId = runtime.mirrorAuthorityLeaderNodeId;
        out.cluster.mirrorAuthorityPrimaryNodeId = runtime.mirrorAuthorityPrimaryNodeId;
        out.cluster.mirrorAuthorityEpoch = runtime.mirrorAuthorityEpoch;
        out.cluster.mirrorAuthorityCommitIndex = runtime.mirrorAuthorityCommitIndex;
        out.cluster.mirrorWritePending = runtime.mirrorWritePending;
        out.cluster.stripeMetadataRaftEnabled = metadataRuntime.enabled;
        out.cluster.stripeMetadataRaftTerm = metadataRuntime.currentTerm;
        out.cluster.stripeMetadataLeaderNodeId = metadataRuntime.leaderNodeId;
        out.cluster.stripeMetadataCommitIndex = metadataRuntime.commitIndex;
        out.cluster.stripeMetadataAppliedIndex = metadataRuntime.appliedIndex;
        out.cluster.stripeMetadataSnapshotIndex = metadataRuntime.snapshotIndex;
        out.cluster.outboundConnectionsTotal = runtime.outboundConnections;
        out.cluster.peerWorkers = runtime.peerWorkers;
        out.cluster.incomingConsensusConnections = runtime.incomingConsensusConnections;
        out.cluster.incomingForwardConnections = runtime.incomingForwardConnections;
        out.cluster.incomingControlConnections = runtime.incomingControlConnections;
        out.cluster.forwardReceiveMemoryBytes = runtime.forwardReceiveMemoryBytes;
        if (opts.cluster.runtime.nodeResourceBudget) { opts.cluster.runtime.nodeResourceBudget->observe(out.cluster); }
        out.cluster.controlReceiveMemoryBytes = runtime.controlReceiveMemoryBytes;
        out.cluster.proposalBatches = runtime.proposalBatches;
        out.cluster.proposalQueueDepth = runtime.proposalQueueDepth;
        out.cluster.pendingProposals = runtime.pendingProposals;
        out.cluster.retainedRequestResults = runtime.retainedRequestResults;
        out.cluster.retainedRequestResultBytes = runtime.retainedRequestResultBytes;
        out.cluster.requestResultByteLimit = runtime.requestResultByteLimit;
        out.cluster.pendingRequests = runtime.pendingRequests;
        out.cluster.requestCapacity = runtime.requestCapacity;
        out.cluster.expiredRequestsTotal = runtime.expiredRequests;
        out.cluster.rejectedRequestsTotal = runtime.rejectedRequests;
        out.cluster.requestJournalBytes = runtime.requestJournalBytes;
        out.cluster.requestJournalRecords = runtime.requestJournalRecords;
        out.cluster.requestJournalBytesWrittenTotal = runtime.requestJournalBytesWritten;
        out.cluster.requestJournalCompactionsTotal = runtime.requestJournalCompactions;
        out.cluster.peerPolicyMismatchRejectsTotal = runtime.peerPolicyMismatchRejects;
        out.cluster.foreignClusterRejectsTotal = runtime.foreignClusterRejects;
        out.cluster.leaseRenewFailuresTotal = runtime.leaseRenewFailures;
        out.cluster.endpointStartFailuresTotal = runtime.endpointStartFailures;
        out.cluster.peerReadTimeoutsTotal = runtime.peerReadTimeouts;
        out.cluster.replicationQueueFrames = runtime.replicationQueueFrames;
        out.cluster.replicationQueueBytes = runtime.replicationQueueBytes;
        out.cluster.transferMemoryBytes = runtime.transferMemoryBytes;
        out.cluster.transferSpoolBytes = runtime.transferSpoolBytes;
        out.cluster.activeTransfers = runtime.activeTransfers;
        out.cluster.transferResumeAttemptsTotal = runtime.transferResumeAttempts;
        out.cluster.transferResumedTotal = runtime.transferResumed;
        out.cluster.transferResumedBytesTotal = runtime.transferResumedBytes;
        out.cluster.transferDiscardedPartialsTotal = runtime.transferDiscardedPartials;
        out.cluster.transferRetainedPartials = runtime.transferRetainedPartials;
        out.cluster.peers.reserve(runtime.peers.size());
        for (const auto& peer : runtime.peers) {
            out.cluster.peers.push_back(
                {
                    .nodeId = peer.nodeId,
                    .learner = peer.learner,
                    .matchIndex = peer.matchIndex,
                    .nextIndex = peer.nextIndex,
                    .replicationLag = peer.replicationLag,
                    .connected = peer.connected,
                    .lastSuccessfulContactAtUs = peer.lastSuccessfulContactAtUs,
                    .roundTripsSucceededTotal = peer.roundTripsSucceeded,
                    .roundTripsFailedTotal = peer.roundTripsFailed,
                    .consecutiveRoundTripFailures = peer.consecutiveRoundTripFailures,
                    .lastRoundTripAtUs = peer.lastRoundTripAtUs,
                    .lastRoundTripFailureAtUs = peer.lastRoundTripFailureAtUs,
                    .roundTripLatencyUs = peer.roundTripLatencyUs,
                }
            );
        }
    }

    out.vlog.enabled = versionLog != nullptr;
    if (versionLog) {
        const auto snap = versionLog->snapshot();
        out.vlog.syncMode = snap.syncMode;
        out.vlog.groupN = snap.groupN;
        out.vlog.groupMicros = snap.groupMicros;
        out.vlog.groupBytes = snap.groupBytes;
        out.vlog.asyncMaxPendingBytes = snap.asyncMaxPendingBytes;
        out.vlog.indexedKeys = snap.indexedKeys;
        out.vlog.indexedEntries = snap.indexedEntries;
        out.vlog.rollbackEntries = snap.rollbackEntries;
        out.vlog.pendingWrites = snap.pendingWrites;
        out.vlog.pendingBytes = snap.pendingBytes;
        out.vlog.durableBytes = snap.durableBytes;
        out.vlog.segmentCount = snap.segmentCount;
        out.vlog.activeSegmentBytes = snap.activeSegmentBytes;
        out.vlog.recoveryDurationMicros = snap.recoveryDurationMicros;
        out.vlog.recoveredSegmentCount = snap.recoveredSegmentCount;
        out.vlog.recoveredEntryCount = snap.recoveredEntryCount;
        out.vlog.sidecarFallbackCount = snap.sidecarFallbackCount;
        out.vlog.sidecarRebuildFailures = snap.sidecarRebuildFailures;
        out.vlog.retentionPrunedSegments = snap.retentionPrunedSegments;
        out.vlog.retentionBaseEntriesWritten = snap.retentionBaseEntriesWritten;
        out.vlog.parallelQueueRejects = snap.parallelQueueRejects;
        out.vlog.parallelLaneCount = snap.parallelLaneCount;
        out.vlog.parallelPendingWrites = snap.parallelPendingWrites;
        out.vlog.parallelPendingBytes = snap.parallelPendingBytes;
        out.vlog.flushThreadRunning = snap.flushThreadRunning;
    }
    return out;
}
}

/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/detail/AkkStats.inc
#pragma once

// Included at namespace scope after AkkEngine::Impl is complete.
inline EngineStats AkkEngine::stats() const noexcept {
    EngineStats out;
    if (!impl_) { return out; }
    Impl::OperationGuard operation{*impl_, false};
    if (!operation) { return out; }

    out.currentSeq = impl_->snapshotSeq();
    out.nodeId = impl_->nodeId;
    {
        std::lock_guard lock{impl_->stripeRepairStatsMu};
        out.stripeReadRepair = impl_->stripeRepairStats;
    }
    {
        std::lock_guard lock{impl_->stripeRebuildStatsMu};
        out.stripeRebuild = impl_->stripeRebuildStats;
    }

    out.putsTotal = impl_->putsTotal.load(std::memory_order_relaxed);
    out.removesTotal = impl_->removesTotal.load(std::memory_order_relaxed);
    out.getsTotal = impl_->getsTotal.load(std::memory_order_relaxed);
    out.getsMemtableHit = impl_->getsMemtableHit.load(std::memory_order_relaxed);
    out.getsSstHit = impl_->getsSstHit.load(std::memory_order_relaxed);
    out.getsMiss = impl_->getsMiss.load(std::memory_order_relaxed);
    out.existsTotal = impl_->existsTotal.load(std::memory_order_relaxed);
    out.scansTotal = impl_->scansTotal.load(std::memory_order_relaxed);
    out.blobPutsTotal = impl_->blobPutsTotal.load(std::memory_order_relaxed);
    out.config.writeAdmission = static_cast<uint32_t>(impl_->opts.runtime.writeAdmission);
    out.config.writeDurability = static_cast<uint32_t>(impl_->opts.runtime.writeDurability);
    out.config.writeVisibility = static_cast<uint32_t>(impl_->opts.runtime.writeVisibility);
    out.config.readVisibility = static_cast<uint32_t>(impl_->opts.runtime.visibility.readVisibility);
    out.config.sequenceAllocation = static_cast<uint32_t>(impl_->opts.runtime.sequence.allocation);
    out.config.sequenceThreadLocalRangeSize = impl_->opts.runtime.sequence.threadLocalRangeSize;
    out.config.commitWindowSize = impl_->commitWindowSize;
    out.config.memtableBackpressureMode = static_cast<uint32_t>(impl_->opts.runtime.backpressure.memtableFlushBacklog);
    out.config.maxMemtableImmutableTables = impl_->opts.runtime.backpressure.maxMemtableImmutableTables;
    out.config.sstBackpressureMode = static_cast<uint32_t>(impl_->opts.runtime.backpressure.sstCompactionBacklog);
    out.config.maxSstL0Files = impl_->opts.runtime.backpressure.maxSstL0Files;
    out.config.backpressureWaitMicros = impl_->opts.runtime.backpressure.waitMicros;
    out.config.backpressureTimeoutMs = impl_->opts.runtime.backpressure.timeoutMs;
    out.config.memtableFlushMode = static_cast<uint32_t>(impl_->opts.memtable.flushMode);
    out.config.walExecution = static_cast<uint32_t>(impl_->opts.wal.execution);
    out.config.walSyncPolicy = static_cast<uint32_t>(impl_->opts.wal.syncPolicy);
    out.config.walBackpressure = static_cast<uint32_t>(impl_->opts.wal.backpressure);
    out.config.sstCompactionMode = static_cast<uint32_t>(impl_->opts.sst.compactionMode);
    out.backpressure.blockedWrites = impl_->backpressureBlockedWrites.load(std::memory_order_relaxed);
    out.backpressure.rejectedWrites = impl_->backpressureRejectedWrites.load(std::memory_order_relaxed);
    out.backpressure.timedOutWrites = impl_->backpressureTimedOutWrites.load(std::memory_order_relaxed);
    out.backpressure.memtableStalls = impl_->backpressureMemtableStalls.load(std::memory_order_relaxed);
    out.backpressure.sstStalls = impl_->backpressureSstStalls.load(std::memory_order_relaxed);
    out.backpressure.waitMicrosTotal = impl_->backpressureWaitMicrosTotal.load(std::memory_order_relaxed);
    out.backpressure.waitMicrosMax = impl_->backpressureWaitMicrosMax.load(std::memory_order_relaxed);
    out.api.enabled = impl_->opts.components.apiEnabled;
    if (impl_->apiServer) { out.api = impl_->apiServer->stats(); }

    if (impl_->memtable) {
        const auto snap = impl_->memtable->snapshot();
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

    out.wal.enabled = impl_->opts.components.walEnabled;
    if (impl_->walWriter) {
        const auto snap = impl_->walWriter->snapshot();
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

    out.blob.enabled = impl_->opts.components.blobEnabled && impl_->blobManager != nullptr;
    out.blob.thresholdBytes = impl_->opts.blob.thresholdBytes;
    if (impl_->blobManager) {
        const auto snap = impl_->blobManager->snapshot();
        out.blob.blobsWritten = snap.blobsWritten;
        out.blob.bytesUncompressed = snap.bytesUncompressed;
        out.blob.bytesOnDisk = snap.bytesOnDisk;
        out.blob.blobsDeleted = snap.blobsDeleted;
        out.blob.gcCycles = snap.gcCycles;
    }

    out.sst.enabled = impl_->sstManager != nullptr;
    if (impl_->sstManager) {
        const auto levels = impl_->sstManager->levelStats();
        out.sst.levels.reserve(levels.size());
        for (const auto& level : levels) {
            out.sst.levels.push_back(LevelStats{level.level, level.fileCount, level.bytes, level.budgetBytes});
            out.sst.fileCount += level.fileCount;
            out.sst.bytes += level.bytes;
            if (level.level == 0) { out.sst.l0FileCount = level.fileCount; }
        }
        out.sst.compactionPending = impl_->sstManager->compactionPending();
        const auto snap = impl_->sstManager->compactionSnapshot();
        out.sst.compactionsCompleted = snap.compactionsCompleted;
        out.sst.filesCompacted = snap.filesCompacted;
        out.sst.bytesCompactedIn = snap.bytesCompactedIn;
        out.sst.bytesCompactedOut = snap.bytesCompactedOut;
        out.sst.compactionFailures = snap.compactionFailures;
    }

    out.manifest.enabled = impl_->manifest != nullptr;
    if (impl_->manifest) {
        const auto checkpoint = impl_->manifest->lastCheckpoint();
        out.manifest.hasCheckpoint = checkpoint.has_value();
        if (checkpoint.has_value()) {
            out.manifest.lastCheckpointSeq = checkpoint->lastSeq.value_or(0);
            out.manifest.lastCheckpointStripe = checkpoint->stripe.value_or(0);
        }
        out.manifest.liveSstCount = impl_->manifest->liveSst().size();
        out.manifest.deletedSstCount = impl_->manifest->deletedSst().size();
        out.manifest.sstSealCount = impl_->manifest->sstSeals().size();
        out.manifest.sstReferencedBlobCount = impl_->manifest->sstReferencedBlobs().size();
        out.manifest.sstBlobRefsComplete = impl_->manifest->sstBlobRefsComplete();
        out.manifest.liveBlobCount = impl_->manifest->liveBlobs().size();
        out.manifest.deletedBlobCount = impl_->manifest->deletedBlobs().size();
        out.manifest.blobPutCount = impl_->manifest->blobPuts().size();
        out.manifest.blobDeleteCount = impl_->manifest->blobDeletes().size();
        if (const auto lease = impl_->manifest->lastPrimaryLease(); lease.has_value()) {
            out.manifest.lastPrimaryLeaseNodeId = lease->nodeId;
            out.manifest.lastPrimaryLeaseUntilUs = lease->leaseUntilUs;
        }
    }

    out.cluster.enabled = impl_->opts.components.clusterEnabled && impl_->clusterRuntime != nullptr;
    out.cluster.configuredNodeCount = impl_->clusterConfiguredNodeCount;
    if (out.cluster.enabled) {
        out.cluster.clusterId = impl_->clusterConfig.clusterId();
        out.cluster.replicationMode = static_cast<uint32_t>(impl_->clusterConfig.mode());
        out.cluster.consistencyMode = static_cast<uint32_t>(impl_->clusterConfig.consistency().mode);
        out.cluster.transportMode = static_cast<uint32_t>(impl_->opts.cluster.runtime.transportMode);
        out.cluster.clusterGroupId = impl_->opts.cluster.runtime.clusterGroupId;
        out.cluster.clusterGroupEpoch = impl_->opts.cluster.runtime.clusterGroupEpoch;
        const auto configured = impl_->usesPartitionRaft() || impl_->clusterReplicationMode == cluster::ReplicationMode::STRIPE
                                    ? impl_->currentPlacement().nodes()
                                    : impl_->clusterConfig.usesDataConsensus()
                                    ? impl_->clusterRuntime->activeNodes()
                                    : impl_->clusterConfig.nodes();
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
    if (impl_->clusterRuntime) {
        {
            std::lock_guard lock{impl_->distributedRecoveryDiagnosticsMu};
            const auto& d = impl_->distributedRecoveryDiagnostics;
            out.cluster.transactionRecoveryFailuresTotal = d.failuresTotal;
            out.cluster.transactionRecoveryConsecutiveFailedPasses = d.consecutiveFailedPasses;
            out.cluster.transactionRecoveryLastFailureAtUs = d.lastFailureAtUs;
            out.cluster.transactionRecoveryFailingSinceUs = d.failingSinceUs;
            out.cluster.transactionRecoveryLastSuccessfulPassAtUs = d.lastSuccessfulPassAtUs;
            out.cluster.transactionRecoveryLastFailedId = d.lastFailedId;
            out.cluster.transactionRecoveryLastError.assign(d.lastError.data(), d.lastErrorSize);
        }
        {
            std::lock_guard lock{impl_->distributedStateMu};
            out.cluster.pendingDistributedTransactions = std::ranges::count_if(impl_->distributedState.decisions,
                [](const auto& item) { return !item.second.finished; });
        }
        out.cluster.role = static_cast<uint32_t>(impl_->clusterRuntime->role());
        out.cluster.activeNodeCount = impl_->clusterRuntime->activeNodes().size();
        const auto runtime = impl_->clusterRuntime->raftStats();
        const auto metadataRuntime = impl_->clusterRuntime->stripeMetadataRaftStats();
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
        if (impl_->usesPartitionRaft()) {
            const auto placement = impl_->currentPlacement();
            const auto children = impl_->partitionSnapshot();
            for (uint16_t index = 0; index < children.size(); ++index) {
                const auto& child = children[index];
                // Removed holders retain storage/runtime objects; committed placement defines dependencies.
                if (!impl_->partitionConfig(placement, index).findById(impl_->nodeId)) {
                    out.cluster.quorumAvailability = cluster::detail::combineAvailability(out.cluster.quorumAvailability, ClusterOperationAvailability::UNKNOWN);
                    out.cluster.writeAvailability = cluster::detail::combineAvailability(out.cluster.writeAvailability, ClusterOperationAvailability::UNKNOWN);
                    out.cluster.linearizableReadAvailability = cluster::detail::combineAvailability(out.cluster.linearizableReadAvailability, ClusterOperationAvailability::UNKNOWN);
                    continue;
                }
                if (child) {
                    Impl::OperationGuard childOperation{*child->impl_, false};
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
        observeParticipant(out.cluster, *impl_);
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
        if (impl_->opts.cluster.runtime.nodeResourceBudget) { impl_->opts.cluster.runtime.nodeResourceBudget->observe(out.cluster); }
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

    out.vlog.enabled = impl_->versionLog != nullptr;
    if (impl_->versionLog) {
        const auto snap = impl_->versionLog->snapshot();
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

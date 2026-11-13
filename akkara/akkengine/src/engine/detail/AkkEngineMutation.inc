/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/detail/AkkEngineMutation.inc
// Included inside AkkEngine::Impl by AkkEngineImpl.hpp.
static void validateStoredRecord(size_t keySize, size_t storedSize) {
    if (keySize > UINT16_MAX || storedSize > UINT16_MAX - keySize) {
        throw std::length_error("AkkEngine: key and stored value exceed the 65535-byte storage limit");
    }
}

void appendAll(
    uint64_t seq,
    std::span<const uint8_t> key,
    std::span<const uint8_t> storedValue,
    uint8_t flags,
    uint64_t sourceNodeId,
    uint64_t precomputedFp64 = 0,
    uint64_t precomputedMiniKey = 0,
    uint8_t versionLogFlags = 0xFF,
    bool knownContiguousCommit = false,
    uint64_t timestampNs = 0,
    const LocalRequestApplication* request = nullptr
) {
    validateStoredRecord(key.size(), storedValue.size());
    throwIfBackgroundFailed();
    if (localRequestRecoveryRequired) { throw std::runtime_error("Local request: reopen is required after interrupted publication"); }
    waitForVersionLogRecovery();
    memtable->throwIfFlushFailed();
    const uint64_t fp64 = precomputedFp64 != 0 ? precomputedFp64 : computeKeyFp64(key);
    const uint64_t mini = precomputedMiniKey != 0 ? precomputedMiniKey : buildMiniKey(key);
    const bool replayingRaft = clusterConfig.usesDataConsensus() && seq <= snapshotSeq();
    const uint16_t walFlags = flags | (sourceNodeId == vlog::ROLLBACK_NODE ? wal::WAL_FLAG_ROLLBACK : 0);
    bool applyHead = true;
    if (replayingRaft) {
        // A crash can leave later KV heads durable before Raft records
        // completion of the batch. Replay must repair missing history
        // without replacing those heads with earlier mutations.
        const auto head = memtable->getPinned(key, UINT64_MAX);
        if (head && head->view.seq() >= seq) { applyHead = false; }
        if (applyHead && sstManager) {
            const auto diskHead = sstManager->get(key);
            if (diskHead && diskHead->seq >= seq) { applyHead = false; }
        }
    }
    if (request) {
        reserveLocalRequest(*request);
        const auto metadata = localRequestMetadata(*request);
        std::vector<uint8_t> envelope{'A', 'K', 'L', 'A', '1'};
        envelope.append_range(metadata);
        envelope.append_range(storedValue);
        if (envelope.size() + key.size() + wal::WalEntryHeader::SIZE > 64ull * 1024 * 1024) {
            throw std::invalid_argument("Local request: mutation exceeds WAL recovery record limit");
        }
        localRequestRecoveryRequired = true;
        walWriter->append(key, envelope, seq, walFlags | wal::WAL_FLAG_REQUEST_APPLICATION, fp64, wal::WalAppendAck::SYNCED);
        crashAtTestPoint("request.local.after_wal");
        recordLocalRequest(metadata, seq);
        crashAtTestPoint("request.local.after_checkpoint");
    }
    else if (walWriter && applyHead) { walWriter->append(key, storedValue, seq, walFlags, fp64, walAckForWriteDurability()); }
    if (versionLog) {
        const uint8_t defaultVlogFlags = sourceNodeId == vlog::ROLLBACK_NODE ? flags | vlog::VLOG_FLAG_ROLLBACK : flags;
        const uint8_t vlogFlags = versionLogFlags == 0xFF ? defaultVlogFlags : versionLogFlags;
        const auto timestamp = timestampNs == 0 ? nowNs() : timestampNs;
        if (!replayingRaft || !versionLog->containsStoredRecord(key, seq, sourceNodeId, timestamp, vlogFlags)) {
            versionLog->appendDeferred(key, seq, sourceNodeId, timestamp, vlogFlags, storedValue);
        }
    }

    if (applyHead) {
        if ((flags & MemHdr16::FLAG_TOMBSTONE) != 0) { memtable->remove(key, seq, fp64, mini); }
        else { memtable->put(key, storedValue, seq, flags, fp64, mini); }
        noteTransactionMutation(key);
    }
    if (clusterReplicationMode == cluster::ReplicationMode::PARTITIONED && !clusterConfig.usesDataConsensus()) {
        replicationStorageSeq.store(std::max(replicationStorageSeq.load(std::memory_order_relaxed), seq), std::memory_order_release);
    }
    if (clusterConfig.usesDataConsensus()) { publishRaftAppliedSequence(seq); }
    else {
        markWriteCommitted(seq, knownContiguousCommit);
        if (versionLog) { versionLog->markCommitted(seq); }
    }
    if (request) { localRequestRecoveryRequired = false; }
}

[[nodiscard]] wal::WalAppendAck snapshotCommitWalAck() const noexcept {
    return opts.wal.syncPolicy == wal::WalSyncPolicy::NEVER ? wal::WalAppendAck::WRITTEN : wal::WalAppendAck::SYNCED;
}

void appendSnapshotWalRecord(
    uint64_t seq,
    std::span<const uint8_t> key,
    std::span<const uint8_t> storedValue,
    uint8_t flags,
    uint64_t precomputedFp64
) {
    if (!walWriter) { return; }
    const uint64_t fp64 = precomputedFp64 != 0 ? precomputedFp64 : computeKeyFp64(key);
    walWriter->append(key, storedValue, seq, flags | wal::WAL_FLAG_SNAPSHOT_RECORD, fp64, wal::WalAppendAck::WRITTEN);
}

void appendSnapshotWalCommit(uint64_t seq, uint64_t recordCount) {
    if (!walWriter) { return; }
    static constexpr std::array<uint8_t, 5> COMMIT_KEY = {'A', 'K', 'S', 'C', '1'};
    const std::vector<uint8_t> value = encodeSnapshotCommitValue(recordCount);
    walWriter->append(COMMIT_KEY, value, seq, wal::WAL_FLAG_SNAPSHOT_COMMIT, 0, snapshotCommitWalAck());
}

void applySnapshotRecordMemory(
    uint64_t seq,
    std::span<const uint8_t> key,
    std::span<const uint8_t> storedValue,
    uint8_t flags,
    uint64_t precomputedFp64 = 0,
    uint64_t precomputedMiniKey = 0
) {
    ++querySnapshotGeneration;
    const uint64_t fp64 = precomputedFp64 != 0 ? precomputedFp64 : computeKeyFp64(key);
    const uint64_t mini = precomputedMiniKey != 0 ? precomputedMiniKey : buildMiniKey(key);
    if ((flags & MemHdr16::FLAG_TOMBSTONE) != 0) { memtable->remove(key, seq, fp64, mini); }
    else { memtable->put(key, storedValue, seq, flags, fp64, mini); }
    if (versionLog && !clusterConfig.usesDataConsensus()) {
        versionLog->appendDeferred(key, seq, 0, nowNs(), flags, storedValue);
        versionLog->markCommitted(seq);
    }
}

[[nodiscard]] AppliedWrite applyLocalPut(
    std::span<const uint8_t> key,
    std::span<const uint8_t> value,
    uint64_t fp64 = 0,
    uint64_t miniKey = 0,
    uint64_t sourceNodeId = 0,
    const LocalRequestApplication* request = nullptr
) {
    AppliedWrite write;
    write.key = key;
    write.op = cluster::ReplOpType::PUT;
    write.sourceNodeId = sourceNodeId == 0 ? nodeId : sourceNodeId;
    applyWriteBackpressure();
    {
        std::lock_guard lock(writeMu);
        write.seq = reserveWriteSeq(1);
        write.stored = maybeExternalize(write.seq, value, write.flags);
        putsTotal.fetch_add(1, std::memory_order_relaxed);
        if ((write.flags & MemHdr16::FLAG_BLOB) != 0) { blobPutsTotal.fetch_add(1, std::memory_order_relaxed); }
        appendAll(write.seq, key, write.stored, write.flags, write.sourceNodeId, fp64, miniKey, 0xFF, false, 0, request);
    }
    return write;
}

void applyLocalPutUnreplicated(
    std::span<const uint8_t> key,
    std::span<const uint8_t> value,
    uint64_t fp64 = 0,
    uint64_t miniKey = 0,
    uint64_t sourceNodeId = 0
) {
    uint8_t flags = MemHdr16::FLAG_NORMAL;
    const uint64_t writeSourceNodeId = sourceNodeId == 0 ? nodeId : sourceNodeId;
    applyWriteBackpressure();
    if (canUseParallelWriteAdmission()) {
        const uint64_t resolvedFp64 = fp64 != 0 ? fp64 : (key.empty() ? 0 : computeKeyFp64(key.data(), key.size()));
        const uint64_t resolvedMiniKey = miniKey != 0 ? miniKey : (key.empty() ? 0 : buildMiniKey(key.data(), key.size()));
        if (usesKeySequenceOrder()) {
            std::lock_guard lock{keySequenceOrderMutex(resolvedFp64, key.size())};
            const uint64_t seq = reserveWriteSeq(1);
            try {
                putsTotal.fetch_add(1, std::memory_order_relaxed);
                appendAll(seq, key, value, flags, writeSourceNodeId, resolvedFp64, resolvedMiniKey);
            }
            catch (...) {
                markKeySequenceFailure(std::current_exception());
                throw;
            }
            return;
        }
        const uint64_t seq = reserveWriteSeq(1);
        putsTotal.fetch_add(1, std::memory_order_relaxed);
        appendAll(seq, key, value, flags, writeSourceNodeId, resolvedFp64, resolvedMiniKey);
        return;
    }

    std::lock_guard lock(writeMu);
    const uint64_t seq = reserveWriteSeq(1);
    putsTotal.fetch_add(1, std::memory_order_relaxed);

    if (blobManager && value.size() >= blobManager->threshold()) {
        auto stored = maybeExternalize(seq, value, flags);
        if ((flags & MemHdr16::FLAG_BLOB) != 0) { blobPutsTotal.fetch_add(1, std::memory_order_relaxed); }
        appendAll(seq, key, stored, flags, writeSourceNodeId, fp64, miniKey, 0xFF, true);
        return;
    }

    appendAll(seq, key, value, flags, writeSourceNodeId, fp64, miniKey, 0xFF, true);
}

[[nodiscard]] std::vector<AppliedWrite> applyLocalPutBatch(std::span<const BatchPutEntry> entries) {
    std::vector<AppliedWrite> writes;
    writes.reserve(entries.size());
    if (!entries.empty()) { applyWriteBackpressure(); }

    std::lock_guard lock(writeMu);
    const uint64_t baseSeq = reserveWriteSeq(entries.size());
    for (const auto& [i, entry] : std::views::enumerate(entries)) {
        const auto& [key, value] = entry;
        AppliedWrite write;
        write.seq = baseSeq + i;
        write.key = key;
        write.op = cluster::ReplOpType::PUT;
        write.sourceNodeId = nodeId;
        write.stored = maybeExternalize(write.seq, value, write.flags);
        putsTotal.fetch_add(1, std::memory_order_relaxed);
        if ((write.flags & MemHdr16::FLAG_BLOB) != 0) { blobPutsTotal.fetch_add(1, std::memory_order_relaxed); }
        appendAll(write.seq, write.key, write.stored, write.flags, nodeId);
        writes.push_back(std::move(write));
    }
    return writes;
}

void applyLocalPutBatchUnreplicated(std::span<const BatchPutEntry> entries) {
    if (!entries.empty()) { applyWriteBackpressure(); }
    if (canUseParallelWriteAdmission()) {
        std::vector<std::unique_lock<std::mutex>> keyOrderLocks;
        const bool keySequenceOrder = usesKeySequenceOrder();
        if (keySequenceOrder) { keyOrderLocks = lockKeySequenceOrderStripes(entries); }
        const uint64_t baseSeq = reserveWriteSeq(entries.size());
        try {
            for (const auto& [i, entry] : std::views::enumerate(entries)) {
                const auto& [key, value] = entry;
                putsTotal.fetch_add(1, std::memory_order_relaxed);
                appendAll(baseSeq + i, key, value, MemHdr16::FLAG_NORMAL, nodeId);
            }
        }
        catch (...) {
            if (keySequenceOrder) { markKeySequenceFailure(std::current_exception()); }
            throw;
        }
        return;
    }

    std::lock_guard lock(writeMu);
    const uint64_t baseSeq = reserveWriteSeq(entries.size());
    for (const auto& [i, entry] : std::views::enumerate(entries)) {
        const auto& [key, value] = entry;
        uint8_t flags = MemHdr16::FLAG_NORMAL;
        putsTotal.fetch_add(1, std::memory_order_relaxed);
        const uint64_t seq = baseSeq + i;

        if (blobManager && value.size() >= blobManager->threshold()) {
            auto stored = maybeExternalize(seq, value, flags);
            if ((flags & MemHdr16::FLAG_BLOB) != 0) { blobPutsTotal.fetch_add(1, std::memory_order_relaxed); }
            appendAll(seq, key, stored, flags, nodeId, 0, 0, 0xFF, true);
        }
        else { appendAll(seq, key, value, flags, nodeId, 0, 0, 0xFF, true); }
    }
}

[[nodiscard]] AppliedWrite applyLocalRemove(
    std::span<const uint8_t> key,
    uint64_t fp64 = 0,
    uint64_t miniKey = 0,
    uint64_t sourceNodeId = 0,
    const LocalRequestApplication* request = nullptr
) {
    AppliedWrite write;
    write.key = key;
    write.op = cluster::ReplOpType::REMOVE;
    write.flags = MemHdr16::FLAG_TOMBSTONE;
    write.sourceNodeId = sourceNodeId == 0 ? nodeId : sourceNodeId;
    applyWriteBackpressure();
    {
        std::lock_guard lock(writeMu);
        write.seq = reserveWriteSeq(1);
        removesTotal.fetch_add(1, std::memory_order_relaxed);
        appendAll(write.seq, key, {}, write.flags, write.sourceNodeId, fp64, miniKey, 0xFF, false, 0, request);
    }
    return write;
}

void applyLocalRemoveUnreplicated(std::span<const uint8_t> key, uint64_t fp64 = 0, uint64_t miniKey = 0, uint64_t sourceNodeId = 0) {
    const uint64_t writeSourceNodeId = sourceNodeId == 0 ? nodeId : sourceNodeId;
    applyWriteBackpressure();
    if (canUseParallelWriteAdmission()) {
        const uint64_t resolvedFp64 = fp64 != 0 ? fp64 : (key.empty() ? 0 : computeKeyFp64(key.data(), key.size()));
        const uint64_t resolvedMiniKey = miniKey != 0 ? miniKey : (key.empty() ? 0 : buildMiniKey(key.data(), key.size()));
        if (usesKeySequenceOrder()) {
            std::lock_guard lock{keySequenceOrderMutex(resolvedFp64, key.size())};
            const uint64_t seq = reserveWriteSeq(1);
            try {
                removesTotal.fetch_add(1, std::memory_order_relaxed);
                appendAll(seq, key, {}, MemHdr16::FLAG_TOMBSTONE, writeSourceNodeId, resolvedFp64, resolvedMiniKey);
            }
            catch (...) {
                markKeySequenceFailure(std::current_exception());
                throw;
            }
            return;
        }
        const uint64_t seq = reserveWriteSeq(1);
        removesTotal.fetch_add(1, std::memory_order_relaxed);
        appendAll(seq, key, {}, MemHdr16::FLAG_TOMBSTONE, writeSourceNodeId, resolvedFp64, resolvedMiniKey);
        return;
    }

    std::lock_guard lock(writeMu);
    const uint64_t seq = reserveWriteSeq(1);
    removesTotal.fetch_add(1, std::memory_order_relaxed);
    appendAll(seq, key, {}, MemHdr16::FLAG_TOMBSTONE, writeSourceNodeId, fp64, miniKey, 0xFF, true);
}

void replicateCommitted(const AppliedWrite& write) {
    if (clusterRuntime) { clusterRuntime->shipEntry(write.seq, write.op, write.key, write.stored, write.flags, write.sourceNodeId); }
}

[[nodiscard]] bool strictPrimaryAckFailWrite() const noexcept { return primaryAckTimeoutAction == cluster::AckTimeoutAction::FAIL_WRITE; }

void recordCommittedRaftMutationStats(cluster::ReplOpType op, uint8_t recordFlags) {
    const uint8_t flags = op == cluster::ReplOpType::REMOVE ? recordFlags | MemHdr16::FLAG_TOMBSTONE : recordFlags;
    if (op == cluster::ReplOpType::REMOVE) { removesTotal.fetch_add(1, std::memory_order_relaxed); }
    else {
        putsTotal.fetch_add(1, std::memory_order_relaxed);
        if ((flags & MemHdr16::FLAG_BLOB) != 0) { blobPutsTotal.fetch_add(1, std::memory_order_relaxed); }
    }
}

void forceClusterLocalDurable() {
    if (walWriter) { walWriter->forceSync(); }
    if (versionLog) { versionLog->forceSync(); }
}

void applyReplicaRecord(
    uint64_t seq,
    cluster::ReplOpType op,
    std::span<const uint8_t> key,
    std::span<const uint8_t> value,
    uint8_t recordFlags,
    uint64_t sourceNodeId,
    uint64_t timestampNs
) {
    validateStoredRecord(key.size(), value.size());
    std::lock_guard lock(writeMu);
    uint8_t flags = recordFlags;
    if (op == cluster::ReplOpType::REMOVE) { flags |= MemHdr16::FLAG_TOMBSTONE; }
    const uint8_t vlogFlags = sourceNodeId == vlog::ROLLBACK_NODE ? flags | vlog::VLOG_FLAG_ROLLBACK : flags;
    const bool partitionedRemote = (clusterReplicationMode == cluster::ReplicationMode::PARTITIONED || clusterReplicationMode ==
        cluster::ReplicationMode::STRIPE) && sourceNodeId != 0 && sourceNodeId != nodeId;
    if (clusterReplicationMode == cluster::ReplicationMode::STRIPE) {
        if (!isStripeInternalKey(key) || key[4] != 'S') { throw std::runtime_error("AkkEngine: invalid remote STRIPE key"); }
        size_t cursor = 6;
        uint64_t generation = 0;
        uint16_t index = 0;
        uint32_t keyLength = 0;
        if (!pullU64(key, cursor, generation) || !pullU16(key, cursor, index) || !pullU32(key, cursor, keyLength) || keyLength != key.size() -
            cursor) { throw std::runtime_error("AkkEngine: invalid remote STRIPE shard key"); }
        const auto accepts = [&](const cluster::ClusterConfig& placement) {
            const uint64_t originalOwner = stripeOwnerFor(placement, key.subspan(cursor));
            const bool fromOwner = sourceNodeId == originalOwner;
            const bool fromFailover = placement.isStripeFailoverNode(sourceNodeId);
            const auto ownerIds = stripeNodeIdsFor(placement, key.subspan(cursor));
            const uint8_t copiesPerShard = placement.stripe().copiesPerShard;
            const auto targetsShard = [&](const std::vector<uint64_t>& placements) {
                if (index >= placement.stripe().totalShards()) { return false; }
                const size_t firstPlacement = index * copiesPerShard;
                return std::ranges::any_of(
                    std::span<const uint64_t>{placements}.subspan(firstPlacement, copiesPerShard),
                    [&](uint64_t targetNodeId) { return targetNodeId == nodeId; }
                );
            };
            const bool ownerTarget = targetsShard(ownerIds);
            const bool hotSpareTarget = copiesPerShard == 2 && placement.isStripeFailoverNode(nodeId);
            bool failoverTarget = false;
            if (fromFailover && !fromOwner) {
                const auto failoverIds = stripeNodeIdsFor(placement, key.subspan(cursor), originalOwner, sourceNodeId);
                failoverTarget = targetsShard(failoverIds);
            }
            return (fromOwner || fromFailover) && (!fromOwner || ownerTarget || hotSpareTarget) && (!fromFailover || ownerTarget || failoverTarget
                || hotSpareTarget);
        };
        bool accepted = accepts(currentPlacement());
        if (!accepted && clusterRuntime) {
            const auto state = clusterRuntime->placementState(false);
            if (state.pendingGeneration != 0) { accepted = accepts(cluster::ClusterConfig::decode(state.pendingConfig)); }
        }
        if (!accepted) { throw std::runtime_error("AkkEngine: remote STRIPE write is not from its owner or targets the wrong shard"); }
    }
    const uint64_t storageSeq = clusterReplicationMode == cluster::ReplicationMode::STRIPE
                                    ? memtable->reserveSeq(1)
                                    : partitionedRemote
                                    ? reserveWriteSeq(1)
                                    : seq;
    appendAll(storageSeq, key, value, flags, sourceNodeId, 0, 0, vlogFlags, false, timestampNs);
    memtable->advanceSeq(storageSeq);
}

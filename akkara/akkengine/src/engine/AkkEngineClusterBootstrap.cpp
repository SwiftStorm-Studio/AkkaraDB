/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/AkkEngineClusterBootstrap.cpp
#include "detail/AkkEngineImpl.hpp"

namespace akkaradb::engine {
    void AkkEngine::Impl::initializeCluster() {
        auto& impl = *this;
        auto cfg = impl.opts.cluster.config.has_value()
                       ? *impl.opts.cluster.config
                       : cluster::ClusterConfig::load(impl.opts.paths.clusterConfigPath);
        impl.clusterConfig = cfg;
        impl.clusterConfiguredNodeCount = cfg.nodes().size();
        impl.clusterReplicationMode = cfg.mode();
        if (impl.distributedTransactionsEnabled()) {
            cfg.validateRuntime(impl.nodeId, impl.opts.cluster.runtime);
            if (!impl.walWriter || !impl.opts.runtime.recoverWal || (impl.sstManager && !impl.opts.runtime.recoverSst) || impl.opts.paths.transactionDir.
                empty()) { throw std::invalid_argument("AkkEngine: Cluster transactions require WAL, storage recovery and transactionDir"); }
            if (impl.blobManager && impl.opts.cluster.runtime.raftBlobPolicy != cluster::RaftBlobPolicy::RAFT_LOG) {
                throw std::invalid_argument("AkkEngine: Cluster transactions require replicated Blob payloads");
            }
        }
        impl.recoverDistributedState();
        if (cfg.raidPreset() == cluster::RaidPreset::RAID1 && !impl.walWriter) { throw std::invalid_argument("AkkEngine: RAID.1 requires WAL durability"); }
        if ((cfg.mode() == cluster::ReplicationMode::PARTITIONED || cfg.mode() == cluster::ReplicationMode::STRIPE) && !impl.walWriter) {
            throw std::invalid_argument("AkkEngine: PARTITIONED and STRIPE require WAL durability");
        }
        impl.primaryAckTimeoutAction = cfg.consistency().ackTimeoutAction;
        if (!cfg.usesDataConsensus() && impl.opts.components.blobEnabled) {
            throw std::invalid_argument("AkkEngine: non-Raft native cluster requires blob storage to be disabled");
        }
        if (cfg.usesDataConsensus()) {
            const auto raftBlobPolicy = impl.opts.cluster.runtime.raftBlobPolicy;
            if (raftBlobPolicy != cluster::RaftBlobPolicy::REJECT && raftBlobPolicy != cluster::RaftBlobPolicy::PRIMARY_SIDE_ONLY && raftBlobPolicy !=
                cluster::RaftBlobPolicy::RAFT_LOG) { throw std::invalid_argument("AkkEngine: invalid Raft Blob policy"); }
            if (impl.opts.components.blobEnabled && raftBlobPolicy == cluster::RaftBlobPolicy::REJECT) {
                throw std::invalid_argument("AkkEngine: RAFT_QUORUM does not support Blob payload replication");
            }
        }
        auto callbacks = impl.makeClusterCallbacks(cfg);

        if (!cluster::clusterRuntimeFactoryAvailable() && !cluster::loadClusterRuntimeBackend(impl.opts.cluster.runtimeBackendPath)) {
            const auto detail = cluster::lastClusterRuntimeBackendLoadError();
            throw std::runtime_error(
                detail.empty()
                    ? "AkkEngine: cluster component is enabled, but the cluster runtime backend library is not available"
                    : "AkkEngine: cluster component is enabled, but the cluster runtime backend library is not available: " + detail
            );
        }
        impl.clusterRuntime = cluster::createClusterRuntime(
            impl.opts.paths.dataDir,
            std::move(cfg),
            impl.nodeId,
            std::move(callbacks),
            impl.opts.cluster.runtime
        );
        impl.writeCoordinator = impl.createWriteCoordinator();
        if (impl.clusterRuntime->mirrorFencingMode() != impl.opts.cluster.runtime.mirrorFencing.mode) {
            throw std::runtime_error("AkkEngine: cluster backend does not implement the requested MIRROR fencing policy");
        }
        if (impl.clusterRuntime->mirrorRecoveryMode() != impl.opts.cluster.runtime.mirrorRecovery.mode) {
            throw std::runtime_error("AkkEngine: cluster backend does not implement the requested MIRROR recovery policy");
        }
        if (impl.clusterReplicationMode == cluster::ReplicationMode::STRIPE) {
            impl.stripeExecutor = std::make_unique<cluster::detail::BoundedExecutor>(8, 64);
        }
        impl.clusterRuntime->start();
        impl.startStripeGarbageCollector();
        impl.startQueryReaper();
    }

    cluster::ClusterEngineCallbacks AkkEngine::Impl::makeClusterCallbacks(const cluster::ClusterConfig& cfg) {
        auto& impl = *this;
        cluster::ClusterEngineCallbacks callbacks;
        if (impl.distributedTransactionsEnabled()) {
            callbacks.transactionMaxJournalBytes = impl.opts.transactions.maxJournalBytes;
            callbacks.transactionBlobThresholdBytes = impl.blobManager ? impl.blobManager->threshold() : 0;
            callbacks.applyTransactionControl = [&impl](uint64_t seq, auto bytes) { impl.applyDistributedCommand(seq, bytes); };
            callbacks.exportTransactionSnapshot = [&impl](uint64_t sequence) { return impl.exportDistributedSnapshot(sequence); };
            callbacks.installTransactionSnapshot = [&impl](const auto& snapshot) { impl.installDistributedSnapshot(snapshot); };
        }
        impl.openPartitionEngines();
        if (impl.usesPartitionRaft()) {
            callbacks.partitionLeader = [&impl](std::span<const uint8_t> key) { return impl.currentPartitionLeader(key); };
            callbacks.partitionCandidates = [&impl](std::span<const uint8_t> key) {
                return cluster::detail::partitionTargets(*impl.livePlacement.load(), key);
            };
        }
        if (impl.usesPartitionRaft() || cfg.mode() == cluster::ReplicationMode::STRIPE) {
            callbacks.placementChanged = [&impl](const cluster::ClusterConfig& config, uint64_t generation) { impl.acceptPlacement(config, generation); };
        }
        callbacks.forward = [&impl](const cluster::ForwardRequest& request) { return impl.receiveForward(request); };
        callbacks.getCurrentSeq = callbacks.getLastSeq = [&impl] { return impl.clusterReplicationSeq(); };
        callbacks.checkpointReplication = [&impl] {
            OperationGuard operation{impl};
            std::lock_guard writeLock{impl.writeMu};
            if (impl.walWriter) { impl.walWriter->forceSync(); }
            if (impl.versionLog) { impl.versionLog->forceSync(); }
            return impl.replicationStorageSeq.load(std::memory_order_relaxed);
        };
        callbacks.getEntries = [&impl](uint64_t afterSeq, uint64_t throughSeq) -> std::optional<std::vector<cluster::ClusterHistoryEntry>> {
            if (!impl.walWriter || throughSeq < afterSeq) { return std::nullopt; }
            std::vector<cluster::ClusterHistoryEntry> entries;
            bool hasExternalBlob = false;
            const auto recovery = wal::WalRecovery::recover(
                wal::WalRecoveryOptions{.walDir = impl.opts.paths.walDir},
                [&](const wal::WalRecoveredEntry& item) {
                    if ((item.flags & wal::WAL_FLAG_SNAPSHOT_MASK) != 0) { return; }
                    if (item.seq <= afterSeq || item.seq > throughSeq) { return; }
                    const uint8_t flags = item.flags;
                    if ((flags & MemHdr16::FLAG_BLOB) != 0) {
                        hasExternalBlob = true;
                        return;
                    }
                    entries.push_back(
                        cluster::ClusterHistoryEntry{
                            .seq = item.seq,
                            .sourceNodeId = (item.flags & wal::WAL_FLAG_ROLLBACK) != 0 ? vlog::ROLLBACK_NODE : impl.nodeId,
                            .op = std::to_underlying((flags & MemHdr16::FLAG_TOMBSTONE) != 0 ? cluster::ReplOpType::REMOVE : cluster::ReplOpType::PUT),
                            .recordFlags = flags,
                            .key = item.key,
                            .value = item.value,
                        }
                    );
                }
            );
            (void)recovery;
            if (hasExternalBlob) { return std::nullopt; }
            std::ranges::sort(entries, [](const auto& left, const auto& right) { return left.seq < right.seq; });
            uint64_t expected = afterSeq;
            for (const auto& entry : entries) {
                if (expected == UINT64_MAX || entry.seq != expected + 1) { return std::nullopt; }
                expected = entry.seq;
            }
            if (expected != throughSeq) { return std::nullopt; }
            return entries;
        };
        callbacks.exportSnapshot = [&impl]() -> std::optional<cluster::ClusterSnapshot> {
            std::unique_lock writeLock{impl.writeMu, std::try_to_lock};
            if (!writeLock.owns_lock()) { return std::nullopt; }
            if (!impl.memtable) { return std::nullopt; }
            const uint64_t seq = impl.clusterReplicationSeq();
            const auto makeSnapshot = [](const std::shared_ptr<ClusterSnapshotExportFile>& exported) {
                cluster::ClusterSnapshot snapshot;
                snapshot.seq = exported->seq();
                snapshot.forEachEntry = [exported](const cluster::ClusterSnapshot::EntryVisitor& visitor) { return exported->forEachEntry(visitor); };
                return snapshot;
            };
            if (auto cached = impl.clusterSnapshotExportCache.lock(); cached && cached->seq() == seq && cached->reusable()) { return makeSnapshot(cached); }
            // Blob deletion is pinned from fixed-view capture through the
            // final Blob read, but does not participate in write admission.
            auto blobReadPin = impl.blobManager ? impl.blobManager->pinReads() : blob::BlobManager::ReadPin{};
            memtable::MemTable::KeyRange range;
            std::optional<memtable::MemTable::RangeIterator> mt;
            if (impl.sstManager) { mt.emplace(impl.memtable->sealAndPinIterator(range, seq)); }
            else {
                const auto& policy = impl.opts.cluster.runtime.memoryOnlySnapshot;
                mt = impl.memtable->sealAndPinMemoryIterator(
                    range,
                    seq,
                    policy.mode == cluster::MemoryOnlySnapshotMode::COMPLETION_FIRST,
                    policy.maxPinnedBytes,
                    policy.maxPinnedGenerations,
                    false,
                    false
                );
                if (!mt) { return std::nullopt; }
            }
            sst::SSTManager::Iterator sst;
            if (impl.sstManager) { sst = impl.sstManager->scanIter({}, {}, seq, true); }

            // Both paths own immutable sources. Memory-only snapshots retain
            // in-memory generations and compact them after the final pin.
            ensureDir(impl.clusterSnapshotExportDirectory());

            struct SnapshotSource {
                memtable::MemTable::RangeIterator memtable;
                sst::SSTManager::Iterator sst;
                blob::BlobManager::ReadPin blobReadPin;
                blob::BlobManager* blobManager = nullptr;
                vlog::VersionLog::RecordSnapshot history;
                bool typed = false;
                std::optional<cluster::ClusterSnapshot> transactions;
            };
            auto history = impl.clusterConfig.usesDataConsensus() && impl.versionLog
                               ? impl.versionLog->captureRecords()
                               : vlog::VersionLog::RecordSnapshot{};
            auto source = std::make_shared<SnapshotSource>(
                SnapshotSource{
                    .memtable = std::move(*mt),
                    .sst = std::move(sst),
                    .blobReadPin = std::move(blobReadPin),
                    .blobManager = impl.blobManager.get(),
                    .history = std::move(history),
                    .typed = impl.clusterConfig.usesDataConsensus(),
                    .transactions = {},
                }
            );
            if (impl.distributedTransactionsEnabled()) { source->transactions = impl.exportDistributedSnapshot(seq); }
            writeLock.unlock();
            auto producer = [source](const cluster::ClusterSnapshot::EntryVisitor& visitor) mutable {
                if (source->transactions && !source->transactions->forEachEntry(visitor)) { return false; }
                if (source->history && !source->history(
                    [&](auto key, const vlog::VersionEntry& entry) {
                        const auto encoded = cluster::detail::encodeSnapshotKey(
                            {
                                cluster::detail::SnapshotRecordKind::HISTORY,
                                entry.flags,
                                entry.seq,
                                entry.sourceNodeId,
                                entry.timestampNs,
                                key,
                                snapshotBlobIdentity(entry.flags, entry.value)
                            }
                        );
                        return streamSnapshotValue(visitor, encoded, entry.value, entry.flags, source->blobManager);
                    }
                )) { return false; }
                source->history = {};
                for (const auto& record : storedScanGenerator(std::move(source->memtable), std::move(source->sst))) {
                    const auto encoded = source->typed
                                             ? cluster::detail::encodeSnapshotKey(
                                                 {
                                                     cluster::detail::SnapshotRecordKind::HEAD,
                                                     record.flags,
                                                     0,
                                                     0,
                                                     0,
                                                     record.key,
                                                     snapshotBlobIdentity(record.flags, record.value)
                                                 }
                                             )
                                             : std::vector<uint8_t>{};
                    if (!streamSnapshotValue(
                        visitor,
                        source->typed ? std::span<const uint8_t>{encoded} : record.key,
                        record.value,
                        record.flags,
                        source->blobManager
                    )) { return false; }
                }
                return true;
            };
            auto exported = std::make_shared<ClusterSnapshotExportFile>(impl.newClusterSnapshotExportPath(), seq, std::move(producer));
            impl.clusterSnapshotExportCache = exported;
            return makeSnapshot(exported);
        };
        callbacks.installPartitionSnapshot = [&impl](uint64_t owner, const cluster::ClusterSnapshot& snapshot) {
            impl.installPartitionSnapshot(owner, snapshot);
        };
        callbacks.beginSnapshot = [&impl](uint64_t seq, uint64_t entryCount) { impl.beginReplicaSnapshot(seq, entryCount); };
        callbacks.beginSnapshotEntry = [&impl](std::span<const uint8_t> key, uint64_t valueSize, uint32_t valueCrc32c) {
            impl.beginReplicaSnapshotEntry(key, valueSize, valueCrc32c);
        };
        callbacks.appendSnapshotEntryChunk = [&impl](uint64_t offset, std::span<const uint8_t> chunk) {
            impl.appendReplicaSnapshotEntryChunk(offset, chunk);
        };
        callbacks.finishSnapshotEntry = [&impl] { impl.finishReplicaSnapshotEntry(); };
        callbacks.prepareSnapshot = [&impl](uint64_t seq, uint64_t count) {
            std::lock_guard lock{impl.writeMu};
            impl.prepareReplicaSnapshotLocked(seq, count);
        };
        callbacks.finishSnapshot = [&impl](uint64_t seq, uint64_t entryCount) { impl.finishReplicaSnapshot(seq, entryCount); };
        callbacks.recoverSnapshot = [&impl](uint64_t seq) { impl.recoverReplicaSnapshot(seq); };
        callbacks.isSnapshotDurable = [&impl](uint64_t seq) { return impl.isReplicaSnapshotDurable(seq); };
        callbacks.forceDurable = [&impl] {
            OperationGuard operation{impl};
            if (impl.walWriter) { impl.walWriter->forceSync(); }
            if (impl.versionLog) { impl.versionLog->forceSync(); }
        };
        callbacks.apply = [&impl](
            uint64_t seq,
            cluster::ReplOpType op,
            std::span<const uint8_t> key,
            std::span<const uint8_t> value,
            uint8_t recordFlags,
            uint64_t sourceNodeId,
            uint64_t timestampNs
        ) {
                OperationGuard operation{impl};
                if (op == cluster::ReplOpType::TRANSACTION) {
                    impl.applyDistributedCommand(seq, value);
                    return;
                }
                impl.applyReplicaRecord(seq, op, key, value, recordFlags, sourceNodeId, timestampNs);
            };
        callbacks.fatalFailure = [&impl](const std::exception_ptr& failure) {
            std::lock_guard lock{impl.transactionFailureMu};
            if (!impl.transactionFailure) { impl.transactionFailure = failure; }
        };
        callbacks.read = [&impl](std::span<const uint8_t> key, uint64_t snapshotSeq) { return impl.readLocalForCluster(key, snapshotSeq); };
        callbacks.readLocalStorage = [&impl](std::span<const uint8_t> key, uint64_t snapshotSeq) {
            return impl.readLocalStorageForCluster(key, snapshotSeq);
        };
        callbacks.commitStripeMetadata = [&impl](uint64_t logicalSeq, std::span<const uint8_t> publicKey, std::span<const uint8_t> metadataBytes) {
            if (logicalSeq == UINT64_MAX - 1) {
                if (metadataBytes.size() < 8) { throw std::runtime_error("AkkEngine: truncated stripe placement alias"); }
                auto alias = decodeStripeMetadata(metadataBytes.subspan(8));
                if (!alias) { throw std::runtime_error("AkkEngine: invalid stripe placement alias"); }
                size_t cursor = 0;
                uint64_t placementGeneration = 0;
                if (!pullU64(metadataBytes, cursor, placementGeneration)) { throw std::runtime_error("AkkEngine: malformed placement generation"); }
                return impl.writeStripeLocal(stripeHistoryPlacementKey(publicKey, alias->logicalSeq, placementGeneration), metadataBytes);
            }
            auto metadata = decodeStripeMetadata(metadataBytes);
            if (!metadata) { throw std::runtime_error("AkkEngine: invalid committed STRIPE metadata"); }
            const bool snapshotBase = logicalSeq == UINT64_MAX;
            if (logicalSeq != 0 && !snapshotBase) { metadata->logicalSeq = logicalSeq; }
            const auto encoded = encodeStripeMetadata(*metadata);
            uint8_t vlogFlags = 0xFF;
            if (snapshotBase) { vlogFlags = vlog::VLOG_FLAG_RETENTION_BASE | (metadata->rollback ? vlog::VLOG_FLAG_ROLLBACK : 0); }
            return impl.writeStripeLocal(
                stripeMetaKey(publicKey),
                encoded,
                cluster::ReplOpType::PUT,
                0,
                metadata->rollback ? vlog::ROLLBACK_NODE : impl.nodeId,
                vlogFlags
            );
        };
        callbacks.readStripeMetadata = [&impl](std::span<const uint8_t> publicKey) {
            auto value = impl.getValueInternal(stripeMetaKey(publicKey), false);
            if (!value) { return value; }
            const auto metadata = decodeStripeMetadata(*value);
            if (!metadata) { throw std::runtime_error("AkkEngine: invalid local stripe metadata"); }
            return std::optional{encodeStripeMetadata(impl.effectiveStripeMetadata(publicKey, *metadata))};
        };
        callbacks.stripeGenerationRetained = [&impl](std::span<const uint8_t> key, uint64_t generation) -> std::optional<bool> {
            OperationGuard operation{impl};
            impl.throwIfBackgroundFailed();
            impl.waitForVersionLogRecovery();
            std::shared_lock lock{impl.stripeGcEpochMu};
            if (impl.stripeQueryPins->load(std::memory_order_acquire) != 0) { return {}; }
            return impl.stripeGenerationRetainedLocal(key, generation);
        };
        callbacks.reclaimStripeGeneration = [&impl](auto key, auto metadata, const std::function<bool()>& retainedByPeers) {
            OperationGuard operation{impl};
            impl.throwIfBackgroundFailed();
            impl.waitForVersionLogRecovery();
            return impl.reclaimStripeGeneration(key, metadata, retainedByPeers);
        };
        callbacks.rollbackControl = [&impl](uint64_t, const cluster::StripeControlRequest& request) {
            cluster::StripeControlResponse response;
            response.requestId = request.requestId;
            try {
                if (request.ownerNodeId != impl.nodeId) {
                    response.status = cluster::StripeControlStatus::REJECTED;
                    return response;
                }
                if (!impl.versionLog && !(impl.clusterReplicationMode == cluster::ReplicationMode::STRIPE &&
                    request.action == cluster::StripeControlAction::ROLLBACK_APPLY)) {
                    response.status = cluster::StripeControlStatus::ERROR_STATUS;
                    return response;
                }
                if (request.action == cluster::StripeControlAction::ROLLBACK_KEY && impl.clusterReplicationMode == cluster::ReplicationMode::PARTITIONED &&
                    impl.clusterRuntime->ownerNodeId(request.key) != impl.nodeId) {
                    response.status = cluster::StripeControlStatus::REJECTED;
                    return response;
                }
                OperationGuard operation{impl};
                impl.throwIfBackgroundFailed();
                impl.waitForVersionLogRecovery();
                std::unique_lock epochLock{impl.mutationEpochMu};
                if (impl.versionLog) { impl.versionLog->forceSync(); }
                if (request.action == cluster::StripeControlAction::ROLLBACK_WATERMARK) {
                    response.status = cluster::StripeControlStatus::COMMITTED;
                    response.authorityNodeId = impl.nodeId;
                    response.fenceToken = impl.localRollbackWatermark();
                    return response;
                }
                if (request.action == cluster::StripeControlAction::ROLLBACK_APPLY) {
                    if (request.ownerNodeId != impl.nodeId || request.metadata.size() < 10) {
                        response.status = cluster::StripeControlStatus::REJECTED;
                        return response;
                    }
                    size_t cursor = 0;
                    const auto conflict = static_cast<RollbackConflictPolicy>(request.metadata[cursor++]);
                    uint64_t expectedHead = 0;
                    if (conflict > RollbackConflictPolicy::OVERWRITE_LATEST || !pullU64(request.metadata, cursor, expectedHead)) {
                        response.status = cluster::StripeControlStatus::REJECTED;
                        return response;
                    }
                    const bool tombstone = request.metadata[cursor++] != 0;
                    const auto value = std::span<const uint8_t>{request.metadata}.subspan(cursor);
                    if (tombstone && !value.empty()) {
                        response.status = cluster::StripeControlStatus::REJECTED;
                        return response;
                    }
                    const auto result = impl.applyMaterializedRollback(request.key, value, tombstone, expectedHead, conflict);
                    const std::array items{result};
                    response.metadata = encodeRollbackItems(items);
                    response.status = cluster::StripeControlStatus::COMMITTED;
                    return response;
                }
                if (request.metadata.size() != 2 && request.metadata.size() != 10) {
                    response.status = cluster::StripeControlStatus::REJECTED;
                    return response;
                }
                const auto execution = static_cast<RollbackExecutionMode>(request.metadata[0]);
                const auto conflict = static_cast<RollbackConflictPolicy>(request.metadata[1]);
                if (execution != RollbackExecutionMode::IMMEDIATE || conflict > RollbackConflictPolicy::OVERWRITE_LATEST) {
                    response.status = cluster::StripeControlStatus::REJECTED;
                    return response;
                }
                uint64_t maximumAllowedHead = UINT64_MAX;
                if (request.metadata.size() == 10) {
                    size_t cursor = 2;
                    if (!pullU64(request.metadata, cursor, maximumAllowedHead)) {
                        response.status = cluster::StripeControlStatus::REJECTED;
                        return response;
                    }
                }
                std::vector<RollbackItemResult> results;
                if (request.action == cluster::StripeControlAction::ROLLBACK_KEY) {
                    results.push_back(
                        impl.clusterReplicationMode == cluster::ReplicationMode::STRIPE
                            ? impl.rollbackStripeCoordinated(request.key, request.fenceToken, conflict, maximumAllowedHead)
                            : impl.rollbackKeyLocal(request.key, request.fenceToken, conflict, 0, maximumAllowedHead)
                    );
                }
                else if (request.action == cluster::StripeControlAction::ROLLBACK_STREAM) {
                    static constexpr std::ptrdiff_t ROLLBACK_STREAM_BATCH_KEYS = 256;
                    const auto keys = impl.rollbackKeysChangedAfter(request.fenceToken);
                    const auto begin = request.key.empty() ? keys.begin() : std::ranges::upper_bound(keys, request.key);
                    const std::ptrdiff_t remaining = std::distance(begin, keys.end());
                    const auto batchSize = std::min(remaining, ROLLBACK_STREAM_BATCH_KEYS);
                    const auto end = std::next(begin, batchSize);
                    for (auto it = begin; it != end; ++it) {
                        const auto& key = *it;
                        results.push_back(
                            impl.clusterReplicationMode == cluster::ReplicationMode::STRIPE
                                ? impl.rollbackStripeCoordinated(key, request.fenceToken, conflict, maximumAllowedHead)
                                : impl.rollbackKeyLocal(key, request.fenceToken, conflict, 0, maximumAllowedHead)
                        );
                    }
                    const bool done = end == keys.end();
                    const std::span<const uint8_t> nextCursor = done || begin == end
                                                                    ? std::span<const uint8_t>{}
                                                                    : std::span<const uint8_t>{*std::prev(end)};
                    response.metadata = encodeRollbackStreamBatch(results, nextCursor, done);
                    response.status = cluster::StripeControlStatus::COMMITTED;
                    return response;
                }
                else {
                    response.status = cluster::StripeControlStatus::REJECTED;
                    return response;
                }
                response.metadata = encodeRollbackItems(results);
                response.status = cluster::StripeControlStatus::COMMITTED;
            }
            catch (...) { response.status = cluster::StripeControlStatus::ERROR_STATUS; }
            return response;
        };
        callbacks.exportStripeMetadataSnapshot = [&impl](uint64_t metadataSequence) -> std::optional<cluster::ClusterSnapshot> {
            std::unique_lock lock{impl.writeMu, std::try_to_lock};
            if (!lock.owns_lock()) { return std::nullopt; }
            if (metadataSequence == 0 || !impl.memtable) { return std::nullopt; }
            const auto visible = impl.snapshotSeq();
            memtable::MemTable::KeyRange range;
            std::optional<memtable::MemTable::RangeIterator> mt;
            if (impl.sstManager) { mt.emplace(impl.memtable->sealAndPinIterator(range, visible)); }
            else {
                const auto& policy = impl.opts.cluster.runtime.memoryOnlySnapshot;
                mt = impl.memtable->sealAndPinMemoryIterator(
                    range,
                    visible,
                    policy.mode == cluster::MemoryOnlySnapshotMode::COMPLETION_FIRST,
                    policy.maxPinnedBytes,
                    policy.maxPinnedGenerations,
                    false,
                    false
                );
                if (!mt) { return std::nullopt; }
            }
            sst::SSTManager::Iterator sst;
            if (impl.sstManager) { sst = impl.sstManager->scanIter({}, {}, visible, true); }
            struct MetadataSource {
                memtable::MemTable::RangeIterator memtable;
                sst::SSTManager::Iterator sst;
                vlog::VersionLog::RecordSnapshot history;
                blob::BlobManager::ReadPin blobPin;
                std::shared_ptr<StorageState> storage;
            };
            auto source = std::make_shared<MetadataSource>(
                MetadataSource{
                    .memtable = std::move(*mt),
                    .sst = std::move(sst),
                    .history = impl.versionLog ? impl.versionLog->captureRecords() : vlog::VersionLog::RecordSnapshot{},
                    .blobPin = impl.blobManager ? impl.blobManager->pinReads() : blob::BlobManager::ReadPin{},
                    .storage = impl.storage,
                }
            );
            ensureDir(impl.clusterSnapshotExportDirectory());
            auto exported = std::make_shared<ClusterSnapshotExportFile>(
                impl.newClusterSnapshotExportPath(),
                metadataSequence,
                [source](const cluster::SnapshotEntryVisitor& visitor) mutable {
                    auto* blobs = source->storage->blobManager.get();
                    if (source->history && !source->history(
                        [&](auto key, const vlog::VersionEntry& entry) {
                            if (!publicKeyFromStripeMetaKey(key)) { return true; }
                            const auto encoded = cluster::detail::encodeSnapshotKey(
                                {
                                    cluster::detail::SnapshotRecordKind::HISTORY,
                                    entry.flags,
                                    entry.seq,
                                    entry.sourceNodeId,
                                    entry.timestampNs,
                                    key,
                                    snapshotBlobIdentity(entry.flags, entry.value)
                                }
                            );
                            return streamSnapshotValue(visitor, encoded, entry.value, entry.flags, blobs);
                        }
                    )) { return false; }
                    source->history = {};
                    for (const auto& record : storedScanGenerator(std::move(source->memtable), std::move(source->sst))) {
                        const bool alias = record.key.size() >= 22 && isStripeInternalKey(record.key) && record.key[4] == 'H';
                        if (!alias && !publicKeyFromStripeMetaKey(record.key)) { continue; }
                        const auto encoded = cluster::detail::encodeSnapshotKey(
                            {
                                cluster::detail::SnapshotRecordKind::HEAD,
                                record.flags,
                                0,
                                0,
                                0,
                                record.key,
                                snapshotBlobIdentity(record.flags, record.value)
                            }
                        );
                        if (!streamSnapshotValue(visitor, encoded, record.value, record.flags, blobs)) { return false; }
                    }
                    return true;
                }
            );
            cluster::ClusterSnapshot snapshot;
            snapshot.seq = metadataSequence;
            snapshot.forEachEntry = [exported](const auto& visitor) { return exported->forEachEntry(visitor); };
            return snapshot;
        };
        callbacks.installStripeMetadataSnapshot = [&impl](const cluster::ClusterSnapshot& snapshot, uint64_t after) {
            std::vector<uint8_t> key, value;
            cluster::detail::SnapshotRecordKind kind{};
            uint8_t flags = 0;
            cluster::SnapshotEntryVisitor visitor;
            visitor.beginEntry = [&](auto encoded, uint64_t size, uint32_t) {
                if (size > 4ull * 1024 * 1024) { throw std::runtime_error("AkkEngine: oversized metadata snapshot record"); }
                const auto record = cluster::detail::decodeSnapshotKey(encoded);
                kind = record.kind;
                flags = record.flags & ~MemHdr16::FLAG_BLOB;
                key.assign_range(record.key);
                value.clear();
                return true;
            };
            visitor.appendValueChunk = [&](uint64_t offset, auto bytes) {
                if (offset != value.size()) { throw std::runtime_error("AkkEngine: invalid metadata snapshot offset"); }
                value.append_range(bytes);
                return true;
            };
            visitor.finishEntry = [&] {
                const bool alias = key.size() >= 22 && isStripeInternalKey(key) && key[4] == 'H';
                const bool metadataKey = publicKeyFromStripeMetaKey(key).has_value();
                if ((!alias && !metadataKey) || kind == cluster::detail::SnapshotRecordKind::STATE || (alias && kind !=
                    cluster::detail::SnapshotRecordKind::HEAD)) { throw std::runtime_error("AkkEngine: invalid metadata snapshot key"); }
                if (alias && value.size() < 8) { throw std::runtime_error("AkkEngine: truncated metadata placement alias"); }
                auto metadata = decodeStripeMetadata(alias ? std::span<const uint8_t>{value}.subspan(8) : std::span<const uint8_t>{value});
                if (!metadata || metadata->logicalSeq == 0 || metadata->logicalSeq > snapshot.seq) {
                    throw std::runtime_error("AkkEngine: invalid metadata snapshot revision");
                }
                if (kind == cluster::detail::SnapshotRecordKind::HISTORY && metadata->logicalSeq <= after) { return true; }
                if (kind == cluster::detail::SnapshotRecordKind::HEAD) {
                    const auto current = impl.getValueInternal(key, false);
                    if (current && *current == value) { return true; }
                    flags = alias ? 0 : metadata->rollback ? vlog::VLOG_FLAG_ROLLBACK : 0;
                }
                impl.writeStripeLocal(key, value, cluster::ReplOpType::PUT, 0, metadata->rollback ? vlog::ROLLBACK_NODE : metadata->originNodeId, flags);
                return true;
            };
            if (!snapshot.forEachEntry(visitor)) { throw std::runtime_error("AkkEngine: incomplete metadata snapshot install"); }
            std::lock_guard lock{impl.writeMu};
            impl.forceClusterLocalDurable();
            return impl.clusterReplicationSeq();
        };
        if (impl.blobManager) {
            callbacks.beginBlob = [&impl](uint64_t /*seq*/, uint64_t blobId, uint64_t totalSize, uint32_t contentCrc32c) {
                impl.blobManager->abortWrite(blobId);
                impl.blobManager->beginWrite(blobId, totalSize, contentCrc32c);
            };
            callbacks.appendBlobChunk = [&impl](uint64_t /*seq*/, uint64_t blobId, uint64_t offset, std::span<const uint8_t> chunk) {
                impl.blobManager->appendWriteChunk(blobId, offset, chunk);
            };
            callbacks.finishBlob = [&impl](uint64_t /*seq*/, uint64_t blobId) { impl.blobManager->finishWrite(blobId); };
            callbacks.abortBlob = [&impl](uint64_t /*seq*/, uint64_t blobId) { impl.blobManager->abortWrite(blobId); };
        }

        return callbacks;
    }
}

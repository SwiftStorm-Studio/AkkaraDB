/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/AkkEngineStorageRecovery.cpp
#include "detail/AkkEngineImpl.hpp"

namespace akkaradb::engine {
    void AkkEngine::Impl::initializeStorage() {
        auto& impl = *this;
        impl.loadRollbackJournal();
        if (!impl.deferredRollbacks.empty() && !impl.opts.components.versionLogEnabled) {
            throw std::runtime_error("AkkEngine: deferred rollback journal requires the version log to remain enabled");
        }
        impl.nodeId = loadOrCreateNodeId(impl.opts.paths.nodeIdPath);
        if (impl.opts.components.clusterEnabled) {
            if (!impl.opts.cluster.config) { impl.opts.cluster.config = cluster::ClusterConfig::load(impl.opts.paths.clusterConfigPath); }
            impl.opts.cluster.config->validateRuntime(impl.nodeId, impl.opts.cluster.runtime);
            if ((impl.opts.cluster.config->usesDataConsensus() || impl.opts.cluster.config->mode() == cluster::ReplicationMode::PARTITIONED ||
                 impl.opts.cluster.config->mode() == cluster::ReplicationMode::STRIPE) &&
                (!impl.opts.components.walEnabled || !impl.opts.runtime.recoverWal ||
                 (impl.opts.components.sstEnabled && !impl.opts.runtime.recoverSst) ||
                 impl.opts.runtime.truncateCorruptWalOnRecovery)) {
                throw std::invalid_argument("AkkEngine: data Raft, PARTITIONED and STRIPE require WAL/SST recovery and reject corrupt-WAL truncation");
            }
            if (impl.opts.cluster.runtime.queryConsistencyMode == cluster::QueryConsistencyMode::GLOBAL_SNAPSHOT && (impl.opts.runtime.writeVisibility !=
                AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER || impl.opts.runtime.sequence.allocation !=
                AkkEngineOptions::SequenceAllocationMode::GLOBAL_ATOMIC)) {
                throw std::invalid_argument("AkkEngine: GLOBAL_SNAPSHOT requires COMMIT_ORDER and GLOBAL_ATOMIC");
            }
            if (impl.opts.cluster.runtime.mirrorFencing.mode == cluster::MirrorFencingMode::QUORUM_FENCED && (!impl.opts.components.walEnabled || !impl.opts.
                runtime.recoverWal)) { throw std::invalid_argument("AkkEngine: MIRROR quorum fencing requires WAL durability and WAL recovery"); }
        }
        for (const auto& task : impl.deferredRollbacks) {
            if (task.clusterTask != impl.opts.components.clusterEnabled) {
                throw std::runtime_error("AkkEngine: deferred rollback journal does not match the current cluster mode");
            }
            if (task.clusterTask) {
                const uint64_t epoch = impl.opts.cluster.runtime.clusterGroupEpoch == 0 ? 1 : impl.opts.cluster.runtime.clusterGroupEpoch;
                if (task.clusterId != impl.opts.cluster.config->clusterId() || task.configEpoch != epoch) {
                    throw std::runtime_error("AkkEngine: deferred rollback journal belongs to a different cluster configuration");
                }
            }
        }
        if (!impl.opts.components.clusterEnabled || !impl.opts.cluster.config || !impl.opts.cluster.config->usesDataConsensus()) {
            removeFileIfExists(impl.replicaSnapshotStagingPath());
        }
        impl.loadLocalRequests();
        {
            std::error_code ec;
            fs::remove_all(impl.clusterSnapshotExportDirectory(), ec);
        }
        if (impl.opts.components.manifestEnabled && !impl.opts.paths.manifestPath.empty()) {
            impl.manifest = manifest::Manifest::create(impl.opts.paths.manifestPath, impl.opts.manifest.fastMode);
            impl.manifest->start();
        }

        if (impl.opts.components.sstEnabled && !impl.opts.paths.sstDir.empty()) {
            std::function<uint64_t()> compactionVisibilitySeq;
            if (impl.opts.runtime.writeVisibility == AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER) {
                compactionVisibilitySeq = [&impl] { return impl.committedSeq.load(std::memory_order_acquire); };
            }
            impl.sstManager = sst::SSTManager::create(
                impl.opts.paths.sstDir, impl.opts.sst, impl.manifest.get(), std::move(compactionVisibilitySeq));
            if (impl.opts.runtime.recoverSst) { impl.sstManager->recover(); }
        }

        if (impl.sstManager) {
            impl.opts.memtable.onFlushComplete = [&impl] {
                std::lock_guard checkpointLock{impl.sstCheckpointMu};
                // Sample publication first: every seq at or below this cut is already
                // in a MemTable (or durably flushed). Later publications cannot extend it.
                if (!impl.memtable || !impl.sstManager) { return; }
                uint64_t checkpointSeq = impl.committedSeq.load(std::memory_order_acquire);
                const uint64_t pending = impl.memtable->minimumUnflushedSequence();
                if (pending != UINT64_MAX) { checkpointSeq = std::min(checkpointSeq, pending == 0 ? 0 : pending - 1); }
                checkpointSeq = std::min(checkpointSeq, impl.sstManager->maxSequence());
                if (checkpointSeq > impl.sstCheckpointSeq) {
                    // Persist the recovery boundary before retiring any WAL files.
                    // checkpoint() also drains preceding SST seals in fast mode.
                    if (impl.manifest) { impl.manifest->checkpoint(std::optional<std::string>{"flush"}, std::nullopt, checkpointSeq); }
                    crashAtTestPoint("engine.flush.after_manifest_checkpoint");
                    if (impl.walWriter && impl.opts.runtime.pruneWalOnFlush) { impl.walWriter->pruneUntil(checkpointSeq); }
                    crashAtTestPoint("engine.flush.after_wal_prune");
                    impl.sstCheckpointSeq = checkpointSeq;
                }
                if (impl.blobManager && impl.opts.blob.gcOnFlush) { impl.runBlobGcIfSafe(); }
            };
            if (impl.opts.memtable.flushInputMode == memtable::MemTableFlushInputMode::STREAMING) {
                impl.opts.memtable.onFlushStream = [&impl](size_t estimatedRecordCount, ArenaGenerator<RecordView> records) {
                    (void)impl.sstManager->flush(estimatedRecordCount, std::move(records));
                    crashAtTestPoint("engine.flush.after_sst_flush");
                };
                impl.opts.memtable.onFlush = {};
            }
            else {
                impl.opts.memtable.onFlush = [&impl](std::span<const RecordView> records) {
                    if (records.empty()) { return; }
                    (void)impl.sstManager->flush(records);
                    crashAtTestPoint("engine.flush.after_sst_flush");
                };
                impl.opts.memtable.onFlushStream = {};
            }
        }
        else {
            impl.opts.memtable.onFlush = {};
            impl.opts.memtable.onFlushStream = {};
            impl.opts.memtable.onFlushComplete = {};
        }
        std::function<uint64_t()> retentionVisibilitySeq;
        if (impl.opts.runtime.writeVisibility == AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER) {
            retentionVisibilitySeq = [&impl] { return impl.committedSeq.load(std::memory_order_acquire); };
        }
        if (impl.sstManager) { impl.opts.memtable.sequenceOrderedVersions = true; }
        impl.memtable = memtable::MemTable::create(impl.opts.memtable, std::move(retentionVisibilitySeq));
        uint64_t manifestCheckpointSeq = 0;
        if (impl.manifest) {
            if (const auto checkpoint = impl.manifest->lastCheckpoint(); checkpoint.has_value() && checkpoint->lastSeq.has_value()) {
                manifestCheckpointSeq = *checkpoint->lastSeq;
            }
        }
        uint64_t walRecoveryCheckpointSeq = 0;
        if (manifestCheckpointSeq > 0 && impl.sstManager && impl.opts.runtime.recoverSst) {
            const uint64_t sstMaxSeq = impl.sstManager->maxSequence();
            if (manifestCheckpointSeq > sstMaxSeq) {
                throw std::runtime_error(
                    "AkkEngine: manifest checkpoint sequence exceeds recovered SST sequence (checkpoint=" + std::to_string(manifestCheckpointSeq) +
                    ", sstMaxSeq=" + std::to_string(sstMaxSeq) + ")"
                );
            }
            walRecoveryCheckpointSeq = manifestCheckpointSeq;
            impl.sstCheckpointSeq = manifestCheckpointSeq;
        }
        uint64_t preWalRecoveredSeq = 0;
        if (impl.sstManager) { preWalRecoveredSeq = std::max(preWalRecoveredSeq, impl.sstManager->maxSequence()); }
        std::exception_ptr versionLogStartupError;
        if (impl.opts.components.versionLogEnabled) {
            impl.opts.vlog.initialCommittedSeq = std::max(impl.opts.vlog.initialCommittedSeq, preWalRecoveredSeq);
            try { impl.versionLog = vlog::VersionLog::create(impl.opts.paths.versionLogPath, impl.opts.vlog); }
            catch (...) { versionLogStartupError = std::current_exception(); }
        }
        bool walRecoveryHadCorruption = false;
        if (impl.opts.components.walEnabled && impl.opts.runtime.recoverWal) {
            const auto recovery = wal::WalRecovery::recoverInto(
                wal::WalRecoveryOptions{
                    .walDir = impl.opts.paths.walDir,
                    .checkpointSeq = walRecoveryCheckpointSeq,
                    .truncateCorruptTail = impl.opts.runtime.truncateCorruptWalOnRecovery,
                    .requestApplication = impl.opts.cluster.runtime.requests.persistLocalApplication
                                              ? std::function<void(std::span<const uint8_t>, uint64_t)>{
                                                  [&impl](auto metadata, uint64_t seq) { impl.recordLocalRequest(metadata, seq); }
                                              }
                                              : std::function<void(std::span<const uint8_t>, uint64_t)>{}
                },
                *impl.memtable
            );
            walRecoveryHadCorruption = recovery.corruptSegments > 0;
            if (walRecoveryHadCorruption && impl.opts.components.clusterEnabled &&
                (impl.opts.cluster.config->usesDataConsensus() || impl.opts.cluster.config->mode() == cluster::ReplicationMode::PARTITIONED ||
                 impl.opts.cluster.config->mode() == cluster::ReplicationMode::STRIPE)) {
                throw std::runtime_error("AkkEngine: corrupt WAL prevents reliable Cluster state-machine recovery");
            }
            if (walRecoveryHadCorruption && impl.opts.cluster.runtime.requests.persistLocalApplication) {
                throw std::runtime_error("Local request: corrupt WAL prevents reliable application recovery");
            }
            if (walRecoveryHadCorruption && impl.versionLog) {
                try {
                    const uint64_t versionLogSupplementAfterSeq = std::max(walRecoveryCheckpointSeq, recovery.maxSeq);
                    const auto records = impl.versionLog->collectSince(versionLogSupplementAfterSeq);
                    for (const auto& record : records) {
                        const uint8_t flags = record.entry.flags & (MemHdr16::FLAG_TOMBSTONE | MemHdr16::FLAG_BLOB);
                        const std::span<const uint8_t> key{record.key.data(), record.key.size()};
                        const uint64_t fp64 = computeKeyFp64(key);
                        const uint64_t mini = buildMiniKey(key);
                        if ((flags & MemHdr16::FLAG_TOMBSTONE) != 0) { impl.memtable->remove(key, record.entry.seq, fp64, mini); }
                        else {
                            impl.memtable->put(
                                key,
                                std::span<const uint8_t>{record.entry.value.data(), record.entry.value.size()},
                                record.entry.seq,
                                flags,
                                fp64,
                                mini
                            );
                        }
                    }
                }
                catch (...) {
                    if (!impl.opts.runtime.ignoreVersionLogSupplementErrors) { throw; }
                    impl.versionLog.reset();
                }
            }
        }
        if (versionLogStartupError) {
            if (!walRecoveryHadCorruption || !impl.opts.runtime.ignoreVersionLogSupplementErrors) { std::rethrow_exception(versionLogStartupError); }
        }
        uint64_t recoveredSeq = 0;
        if (impl.memtable) {
            const uint64_t nextMemtableSeq = impl.memtable->lastSeq();
            recoveredSeq = nextMemtableSeq > 0 ? nextMemtableSeq - 1 : 0;
        }
        if (impl.sstManager) { recoveredSeq = std::max(recoveredSeq, impl.sstManager->maxSequence()); }
        if (impl.memtable && recoveredSeq > 0) { impl.memtable->advanceSeq(recoveredSeq); }
        impl.resetCommittedSeq(recoveredSeq);
        impl.replicationStorageSeq = recoveredSeq;
        if (impl.versionLog) { impl.versionLog->seedCommittedSeq(recoveredSeq); }

        if (impl.opts.components.walEnabled) { impl.walWriter = wal::WalWriter::create(impl.opts.paths.walDir, impl.opts.wal); }
        if (impl.walWriter && impl.opts.runtime.pruneWalOnFlush && walRecoveryCheckpointSeq > 0) { impl.walWriter->pruneUntil(walRecoveryCheckpointSeq); }

        if (impl.opts.components.blobEnabled) {
            impl.opts.blob.onBlobPut = [&impl](uint64_t blobId, uint64_t totalSize, uint64_t storedSize, uint32_t contentCrc32c, uint32_t codec) {
                if (impl.manifest) { impl.manifest->blobPut(blobId, totalSize, storedSize, contentCrc32c, codec); }
            };
            impl.opts.blob.onBlobDelete = [&impl](uint64_t blobId) {
                if (!impl.manifest) { return; }
                try { impl.manifest->blobDelete(blobId); }
                catch (...) {}
            };
            impl.blobManager = blob::BlobManager::create(impl.opts.paths.blobDir, impl.opts.blob);
            impl.blobManager->start();
        }

        impl.recoverTransactions();

    }
}

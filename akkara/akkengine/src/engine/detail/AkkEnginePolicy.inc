/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/detail/AkkEnginePolicy.inc
// Included inside detail::runtime by AkkEngineSupport.hpp.
    [[nodiscard]] inline uint32_t nextPow2(uint32_t value) noexcept {
        uint32_t out = 1;
        while (out < value) { out <<= 1; }
        return out;
    }

    inline constexpr uint64_t BLOB_ID_NAMESPACE_BITS = 2;
    inline constexpr uint64_t BLOB_ID_PAYLOAD_BITS = 64 - BLOB_ID_NAMESPACE_BITS;
    inline constexpr uint64_t BLOB_ID_NAMESPACE_RAFT_LOG = 1ULL << BLOB_ID_PAYLOAD_BITS;
    inline constexpr uint64_t BLOB_ID_NAMESPACE_SNAPSHOT = 2ULL << BLOB_ID_PAYLOAD_BITS;
    inline constexpr uint64_t LOCAL_BLOB_ID_LIMIT = 1ULL << BLOB_ID_PAYLOAD_BITS;
    inline constexpr size_t SNAPSHOT_STAGING_VALUE_CHUNK_BYTES = 1024ULL * 1024ULL;

    [[nodiscard]] inline uint64_t raftLogBlobId(uint64_t nodeId, uint64_t seq) {
        constexpr uint64_t SEQ_BITS = 46;
        constexpr uint64_t SEQ_MASK = (1ULL << SEQ_BITS) - 1ULL;
        constexpr uint64_t MAX_NODE_ID = (1ULL << (BLOB_ID_PAYLOAD_BITS - SEQ_BITS)) - 1ULL;
        if (nodeId == 0 || nodeId > MAX_NODE_ID) { throw std::invalid_argument("AkkEngine: RAFT_LOG Blob nodeId is outside encodable range"); }
        if (seq == 0 || seq > SEQ_MASK) { throw std::overflow_error("AkkEngine: RAFT_LOG Blob sequence is outside encodable range"); }
        return BLOB_ID_NAMESPACE_RAFT_LOG | (nodeId << SEQ_BITS) | seq;
    }

    [[nodiscard]] inline uint64_t snapshotBlobId(uint64_t seq, uint64_t ordinal) {
        constexpr uint64_t ORDINAL_BITS = 22;
        constexpr uint64_t ORDINAL_MASK = (1ULL << ORDINAL_BITS) - 1ULL;
        constexpr uint64_t SEQ_MASK = (1ULL << (BLOB_ID_PAYLOAD_BITS - ORDINAL_BITS)) - 1ULL;
        if (seq == 0 || seq > SEQ_MASK) { throw std::overflow_error("AkkEngine: snapshot Blob sequence is outside encodable range"); }
        if (ordinal == 0 || ordinal > ORDINAL_MASK) { throw std::overflow_error("AkkEngine: snapshot Blob ordinal is outside encodable range"); }
        return BLOB_ID_NAMESPACE_SNAPSHOT | (seq << ORDINAL_BITS) | ordinal;
    }

    [[nodiscard]] inline uint32_t shardsForThreads(uint32_t writers, uint32_t cap) noexcept {
        if (writers <= 1) { return 1; }
        const uint32_t target = std::max(writers * 4u, 2u);
        return std::min(nextPow2(target), cap);
    }

    [[nodiscard]] inline bool supportsParallelWriteAdmission(const AkkEngineOptions& options) noexcept {
        return !options.components.blobEnabled && !options.components.clusterEnabled;
    }

    inline constexpr size_t COMMIT_COMPLETION_RING_SIZE = 1u << 16u;

    [[nodiscard]] inline uint32_t resolveCommitWindowSize(uint32_t requested) noexcept {
        if (requested == 0) { return
            COMMIT_COMPLETION_RING_SIZE; }
        return nextPow2(std::clamp<uint32_t>(requested, 64u, 1u << 24u));
    }

    [[nodiscard]] inline wal::WalExecutionMode resolveWalExecutionMode(const wal::WalOptions& options) noexcept {
        if (options.execution != wal::WalExecutionMode::AUTO) { return options.execution; }
        return options.syncMode == wal::WalSyncMode::ASYNC ? wal::WalExecutionMode::ASYNC : wal::WalExecutionMode::INLINE;
    }

    [[nodiscard]] inline wal::WalSyncPolicy resolveWalSyncPolicy(const wal::WalOptions& options) noexcept {
        if (options.syncPolicy != wal::WalSyncPolicy::AUTO) { return options.syncPolicy; }
        switch (options.syncMode) {
            case wal::WalSyncMode::OFF: return wal::WalSyncPolicy::NEVER;
            case wal::WalSyncMode::ASYNC: return wal::WalSyncPolicy::ON_SYNC_ACK;
            case wal::WalSyncMode::SYNC: return wal::WalSyncPolicy::ALWAYS;
        }
        return wal::WalSyncPolicy::ALWAYS;
    }

    inline void normalizeWalOptions(wal::WalOptions& options) noexcept {
        options.execution = resolveWalExecutionMode(options);
        options.syncPolicy = resolveWalSyncPolicy(options);
        if (options.syncPolicy == wal::WalSyncPolicy::NEVER && options.syncMode == wal::WalSyncMode::SYNC) { options.syncMode = wal::WalSyncMode::OFF; }
        else if (options.execution == wal::WalExecutionMode::ASYNC) { options.syncMode = wal::WalSyncMode::ASYNC; }
        else if (options.syncPolicy == wal::WalSyncPolicy::NEVER) { options.syncMode = wal::WalSyncMode::OFF; }
        else { options.syncMode = wal::WalSyncMode::SYNC; }
    }

    inline void normalizeMemTableOptions(memtable::MemTable::Options& options) noexcept {
        if (options.flushMode == memtable::MemTableFlushMode::AUTO) {
            options.flushMode = options.thresholdBytesPerShard > 0
                                    ? memtable::MemTableFlushMode::BYTES_PER_SHARD
                                    : memtable::MemTableFlushMode::MANUAL_ONLY;
        }
    }

    inline void normalizeSstOptions(sst::SSTManager::Options& options) noexcept {
        if (options.compactionMode == sst::SSTCompactionMode::AUTO) {
            options.compactionMode = options.compactThreads == 0 ? sst::SSTCompactionMode::DISABLED : sst::SSTCompactionMode::BACKGROUND;
        }
    }

    [[nodiscard]] inline AkkEngineOptions::WriteVisibilityMode resolveReadVisibility(const AkkEngineOptions& options) noexcept {
        switch (options.runtime.visibility.readVisibility) {
            case AkkEngineOptions::ReadVisibilityMode::AUTO: return options.runtime.writeVisibility;
            case AkkEngineOptions::ReadVisibilityMode::COMMIT_ORDER: return AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER;
            case AkkEngineOptions::ReadVisibilityMode::APPLIED: return AkkEngineOptions::WriteVisibilityMode::APPLIED;
        }
        return options.runtime.writeVisibility;
    }

    inline void normalizeVisibilityOptions(AkkEngineOptions& options) noexcept {
        const auto effective = resolveReadVisibility(options);
        options.runtime.writeVisibility = effective;
        options.runtime.visibility.readVisibility = effective == AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER
                                                        ? AkkEngineOptions::ReadVisibilityMode::COMMIT_ORDER
                                                        : AkkEngineOptions::ReadVisibilityMode::APPLIED;
    }

    inline void resolveWritePolicy(AkkEngineOptions& options) {
        switch (options.runtime.writePolicy) {
            case AkkEngineOptions::WritePolicyPreset::CUSTOM: break;
            case AkkEngineOptions::WritePolicyPreset::SAFE: options.runtime.writeDurability = AkkEngineOptions::WriteDurabilityMode::SYNCED;
                options.runtime.writeVisibility = AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER;
                break;
            case AkkEngineOptions::WritePolicyPreset::BALANCED: options.runtime.writeDurability = AkkEngineOptions::WriteDurabilityMode::WRITTEN;
                options.runtime.writeVisibility = AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER;
                break;
            case AkkEngineOptions::WritePolicyPreset::FAST: options.runtime.writeDurability = options.components.walEnabled
                                                                                                  ? AkkEngineOptions::WriteDurabilityMode::ENQUEUED
                                                                                                  : AkkEngineOptions::WriteDurabilityMode::MEMORY;
                options.runtime.writeVisibility = AkkEngineOptions::WriteVisibilityMode::APPLIED;
                break;
        }

        if (options.runtime.writeDurability == AkkEngineOptions::WriteDurabilityMode::MEMORY && options.components.walEnabled) {
            throw std::invalid_argument("AkkEngine: writeDurability=MEMORY requires WAL to be disabled");
        }
    }

    inline void validateRuntimeOptions(const AkkEngineOptions& options) {
        if (options.runtime.sequence.allocation == AkkEngineOptions::SequenceAllocationMode::THREAD_LOCAL_RANGES && options.runtime.writeVisibility ==
            AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER) {
            throw std::invalid_argument("AkkEngine: THREAD_LOCAL_RANGES sequence allocation requires readVisibility=APPLIED");
        }
        if (options.runtime.sequence.allocation == AkkEngineOptions::SequenceAllocationMode::THREAD_LOCAL_RANGES && (options.components.clusterEnabled ||
            options.components.versionLogEnabled)) {
            throw std::invalid_argument("AkkEngine: THREAD_LOCAL_RANGES sequence allocation requires cluster and version log to be disabled");
        }
        if (options.runtime.sequence.allocation == AkkEngineOptions::SequenceAllocationMode::THREAD_LOCAL_RANGES && options.runtime.sequence.
            threadLocalRangeSize <= 1) { throw std::invalid_argument("AkkEngine: THREAD_LOCAL_RANGES requires sequence.threadLocalRangeSize > 1"); }
        if (options.runtime.parallelWriteOrder == AkkEngineOptions::ParallelWriteOrderMode::KEY_SEQUENCE && options.runtime.sequence.allocation !=
            AkkEngineOptions::SequenceAllocationMode::GLOBAL_ATOMIC) {
            throw std::invalid_argument("AkkEngine: parallelWriteOrder=KEY_SEQUENCE requires sequence.allocation=GLOBAL_ATOMIC");
        }
        if (options.runtime.parallelWriteOrder == AkkEngineOptions::ParallelWriteOrderMode::KEY_SEQUENCE && options.runtime.writeVisibility !=
            AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER) {
            throw std::invalid_argument("AkkEngine: parallelWriteOrder=KEY_SEQUENCE requires readVisibility=COMMIT_ORDER");
        }
        if (options.components.sstEnabled && options.runtime.backpressure.maxSstL0Files > 0 && options.runtime.backpressure.sstCompactionBacklog ==
            AkkEngineOptions::BackpressureMode::BLOCK && options.sst.compactionMode == sst::SSTCompactionMode::DISABLED) {
            throw std::invalid_argument("AkkEngine: blocking SST backpressure requires background compaction or fail-fast mode");
        }
    }

inline void prepareEngineOptions(AkkEngineOptions& options) {
    const auto& tx = options.transactions;
    if (tx.maxOpen == 0 || tx.maxOpen > 1024 || tx.maxLifetimeMs == 0 || tx.maxLifetimeMs > 3'600'000 || tx.maxWrites == 0 || tx.maxWrites > 1'048'576 || tx
       .maxWriteBytes < 1024 || tx.maxWriteBytes > 512ull * 1024 * 1024 || tx.maxReadSetBytes < 1024 || tx.maxReadSetBytes > 512ull * 1024 * 1024 || tx.
        maxPinnedBytes < 1024 * 1024 || tx.maxPinnedBytes > 1024ull * 1024 * 1024 * 1024 || tx.maxTrackedBytes < 1024 * 1024 || tx.maxTrackedBytes > 512ull
        * 1024 * 1024 || tx.maxJournalBytes < 1024 || tx.maxJournalBytes > 1024ull * 1024 * 1024 * 1024) {
        throw std::invalid_argument("AkkEngine: invalid transaction runtime limits");
    }
    if (options.components.sstEnabled && options.components.blobEnabled && !options.sst.trackBlobReferences) {
        throw std::invalid_argument("AkkEngine: Blob storage requires SST Blob reference tracking");
    }
    resolveWritePolicy(options);
    normalizeVisibilityOptions(options);
    if (options.runtime.generationLayoutEnabled && !options.paths.dataDir.empty()) {
        options.paths.dataDir = generation::StorageGeneration::openOrCreate(options.paths.dataDir).activePath();
    }
    auto fillPath = [&](fs::path& target, const char* fallback) {
        if (target.empty() && !options.paths.dataDir.empty()) { target = options.paths.dataDir / fallback; }
    };

    fillPath(options.paths.walDir, "wal");
    fillPath(options.paths.blobDir, "blobs");
    fillPath(options.paths.sstDir, "sstable");
    fillPath(options.paths.manifestPath, "manifest.akmf");
    fillPath(options.paths.versionLogPath, "history.akvlog");
    fillPath(options.paths.clusterConfigPath, "cluster.akcc");
    fillPath(options.paths.nodeIdPath, "node.id");
    fillPath(options.paths.rollbackJournalPath, "rollback-journal.akrb");
    fillPath(options.paths.transactionDir, "transactions");

    if (options.runtime.writerThreads > 0) {
        if (options.memtable.expectedConcurrentWriters == 0) { options.memtable.expectedConcurrentWriters = options.runtime.writerThreads; }
        if (options.memtable.shardCount == 0) { options.memtable.shardCount = shardsForThreads(options.runtime.writerThreads, 256); }
        if (options.wal.shardCount == 0) { options.wal.shardCount = shardsForThreads(options.runtime.writerThreads, 64); }
    }
    normalizeWalOptions(options.wal);
    normalizeMemTableOptions(options.memtable);
    normalizeSstOptions(options.sst);
    validateRuntimeOptions(options);

    if (!options.paths.dataDir.empty()) { ensureDir(options.paths.dataDir); }
    if (options.components.walEnabled) { ensureDir(options.paths.walDir); }
    if (options.components.blobEnabled) { ensureDir(options.paths.blobDir); }
    if (options.components.sstEnabled) { ensureDir(options.paths.sstDir); }
    if (options.components.manifestEnabled) { ensureDir(options.paths.manifestPath.parent_path()); }
    if (options.components.versionLogEnabled && options.paths.versionLogPath.empty()) {
        throw std::invalid_argument("AkkEngine: version log path is required when components.versionLogEnabled is true");
    }
    if (options.components.versionLogEnabled) { ensureDir(options.paths.versionLogPath.parent_path()); }
    if (options.components.apiEnabled && options.api.bindHost.empty()) {
        throw std::invalid_argument("AkkEngine: api.bindHost is required when components.apiEnabled is true");
    }
    if (options.runtime.writeAdmission == AkkEngineOptions::WriteAdmissionMode::PARALLEL && !supportsParallelWriteAdmission(options)) {
        throw std::invalid_argument("AkkEngine: runtime.writeAdmission=PARALLEL requires blob and cluster to be disabled");
    }
}

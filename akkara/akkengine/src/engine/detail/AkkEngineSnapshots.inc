/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/detail/AkkEngineSnapshots.inc
// Included inside AkkEngine::Impl by AkkEngineImpl.hpp.
[[nodiscard]] fs::path replicaSnapshotStagingPath() const {
    return opts.paths.dataDir.empty() ? fs::path{"replication-snapshot.staging"} : opts.paths.dataDir / "replication-snapshot.staging";
}

[[nodiscard]] fs::path clusterSnapshotExportDirectory() const {
    return opts.paths.dataDir.empty() ? fs::path{"cluster-snapshot-exports"} : opts.paths.dataDir / "cluster-snapshot-exports";
}

[[nodiscard]] fs::path newClusterSnapshotExportPath() const {
    std::array<uint8_t, 16> random{};
    crypto::secureRandom(random);
    static constexpr char HEX[] = "0123456789abcdef";
    std::string name{"snapshot-"};
    name.reserve(9 + random.size() * 2 + 4);
    for (const uint8_t byte : random) {
        name.push_back(HEX[byte >> 4]);
        name.push_back(HEX[byte & 0x0f]);
    }
    name += ".tmp";
    return clusterSnapshotExportDirectory() / name;
}

[[nodiscard]] std::pair<uint64_t, uint64_t> readReplicaSnapshotStagingHeader(std::istream& in) const {
    static constexpr char SNAPSHOT_STAGING_MAGIC[] = {'A', 'K', 'S', 'S', '1'};
    std::array<char, sizeof(SNAPSHOT_STAGING_MAGIC)> magic{};
    readExact(in, std::span{magic}, "header");
    if (!std::ranges::equal(magic, SNAPSHOT_STAGING_MAGIC)) {
        throw std::runtime_error("AkkEngine: corrupt replication snapshot staging file");
    }
    const uint64_t stagedSeq = readU64Le(in, "snapshot seq");
    const uint64_t stagedEntryCount = readU64Le(in, "entry count");
    return {stagedSeq, stagedEntryCount};
}

void resetReplicaSnapshotStagingLocked(bool preserveRecovery = false) {
    if (pendingSnapshotOut.is_open()) { pendingSnapshotOut.close(); }
    if (!preserveRecovery) { removeFileIfExists(replicaSnapshotStagingPath()); }
    snapshotInProgress = false;
    pendingSnapshotSeq = 0;
    pendingSnapshotEntryCount = 0;
    pendingSnapshotAppliedEntries = 0;
    pendingSnapshotKeys.clear();
    pendingSnapshotEntryInProgress = false;
    pendingSnapshotEntryValueSize = 0;
    pendingSnapshotEntryValueOffset = 0;
    pendingSnapshotEntryValueCrc32c = 0;
    pendingSnapshotEntryRecordCrc = Crc32cStream{};
    pendingSnapshotEntryValueCrc = Crc32cStream{};
}

void rememberSnapshotHead(std::span<const uint8_t> key) {
    if (clusterConfig.usesDataConsensus()) {
        const auto record = cluster::detail::decodeSnapshotKey(key);
        if (record.kind == cluster::detail::SnapshotRecordKind::HISTORY) { return; }
        if (record.kind >= cluster::detail::SnapshotRecordKind::TRANSACTION) { return; }
        if (record.kind != cluster::detail::SnapshotRecordKind::HEAD) { throw std::runtime_error("AkkEngine: invalid data snapshot record kind"); }
        key = record.key;
    }
    if (const auto [position, inserted] = pendingSnapshotKeys.emplace(reinterpret_cast<const char*>(key.data()), key.size()); !inserted) {
        throw std::runtime_error("AkkEngine: replication snapshot has duplicate heads");
    }
}

void loadReplicaSnapshotStagingForRecoveryLocked(uint64_t seq) {
    if (snapshotInProgress && seq == pendingSnapshotSeq && pendingSnapshotAppliedEntries == pendingSnapshotEntryCount && !
        pendingSnapshotEntryInProgress) {
        if (pendingSnapshotOut.is_open()) { pendingSnapshotOut.close(); }
        return;
    }
    if (pendingSnapshotOut.is_open()) { pendingSnapshotOut.close(); }

    const auto path = replicaSnapshotStagingPath();
    std::ifstream staged{path, std::ios::binary};
    if (!staged) { throw std::runtime_error("AkkEngine: missing replication snapshot staging file for recovery"); }

    const auto [stagedSeq, stagedEntryCount] = readReplicaSnapshotStagingHeader(staged);
    if (stagedSeq != seq) { throw std::runtime_error("AkkEngine: replication snapshot recovery sequence mismatch"); }

    pendingSnapshotKeys.clear();
    for (uint64_t i = 0; i < stagedEntryCount; ++i) {
        const auto entry = readStagedSnapshotRecord(staged, seq, i + 1, false);
        if (!entry.history && !entry.transaction) {
            if (const auto [position, inserted] = pendingSnapshotKeys.emplace(reinterpret_cast<const char*>(entry.key.data()), entry.key.size()); !inserted) {
                throw std::runtime_error("AkkEngine: replication snapshot has duplicate heads");
            }
        }
    }
    char trailing = 0;
    if (staged.get(trailing)) { throw std::runtime_error("AkkEngine: trailing bytes in replication snapshot staging file"); }

    snapshotInProgress = true;
    pendingSnapshotSeq = seq;
    pendingSnapshotEntryCount = stagedEntryCount;
    pendingSnapshotAppliedEntries = stagedEntryCount;
    pendingSnapshotEntryInProgress = false;
    pendingSnapshotEntryValueSize = 0;
    pendingSnapshotEntryValueOffset = 0;
    pendingSnapshotEntryValueCrc32c = 0;
    pendingSnapshotEntryRecordCrc = Crc32cStream{};
    pendingSnapshotEntryValueCrc = Crc32cStream{};
}

void beginReplicaSnapshot(uint64_t seq, uint64_t) {
    std::lock_guard lock(writeMu);
    resetReplicaSnapshotStagingLocked();
    const auto path = replicaSnapshotStagingPath();
    ensureDir(path.parent_path());
    pendingSnapshotOut.open(path, std::ios::binary | std::ios::trunc);
    if (!pendingSnapshotOut) { throw std::runtime_error("AkkEngine: failed to open replication snapshot staging file"); }

    static constexpr char SNAPSHOT_STAGING_MAGIC[] = {'A', 'K', 'S', 'S', '1'};
    pendingSnapshotOut.write(SNAPSHOT_STAGING_MAGIC, sizeof(SNAPSHOT_STAGING_MAGIC));
    writeU64Le(pendingSnapshotOut, seq);
    writeU64Le(pendingSnapshotOut, 0); // Patched with the final count on completion.

    snapshotInProgress = true;
    pendingSnapshotSeq = seq;
    pendingSnapshotEntryCount = UINT64_MAX;
    pendingSnapshotAppliedEntries = 0;
}

void beginReplicaSnapshotEntry(std::span<const uint8_t> key, uint64_t valueSize, uint32_t valueCrc32c) {
    std::lock_guard lock(writeMu);
    if (!snapshotInProgress || !pendingSnapshotOut.is_open()) {
        throw std::runtime_error("AkkEngine: received replication snapshot entry without begin");
    }
    if (pendingSnapshotEntryInProgress) { throw std::runtime_error("AkkEngine: replication snapshot entry is already active"); }
    if (pendingSnapshotAppliedEntries == UINT64_MAX) { throw std::runtime_error("AkkEngine: replication snapshot has too many entries"); }
    if (key.size() > UINT32_MAX) { throw std::invalid_argument("AkkEngine: replication snapshot key is too large"); }

    rememberSnapshotHead(key);

    const uint32_t keyLen = key.size();
    writeU32Le(pendingSnapshotOut, keyLen);
    writeU64Le(pendingSnapshotOut, valueSize);
    writeU32Le(pendingSnapshotOut, valueCrc32c);
    if (!key.empty()) { pendingSnapshotOut.write(reinterpret_cast<const char*>(key.data()), std::ssize(key)); }
    if (!pendingSnapshotOut) { throw std::runtime_error("AkkEngine: failed to write replication snapshot staging entry"); }

    pendingSnapshotEntryInProgress = true;
    pendingSnapshotEntryValueSize = valueSize;
    pendingSnapshotEntryValueOffset = 0;
    pendingSnapshotEntryValueCrc32c = valueCrc32c;
    pendingSnapshotEntryRecordCrc = Crc32cStream{};
    pendingSnapshotEntryValueCrc = Crc32cStream{};
    updateCrcU32Le(pendingSnapshotEntryRecordCrc, keyLen);
    updateCrcU64Le(pendingSnapshotEntryRecordCrc, valueSize);
    updateCrcU32Le(pendingSnapshotEntryRecordCrc, valueCrc32c);
    pendingSnapshotEntryRecordCrc.update(key);
}

void appendReplicaSnapshotEntryChunk(uint64_t offset, std::span<const uint8_t> chunk) {
    std::lock_guard lock(writeMu);
    if (!snapshotInProgress || !pendingSnapshotOut.is_open() || !pendingSnapshotEntryInProgress) {
        throw std::runtime_error("AkkEngine: received replication snapshot chunk without active entry");
    }
    if (offset != pendingSnapshotEntryValueOffset || chunk.size() > pendingSnapshotEntryValueSize - pendingSnapshotEntryValueOffset) {
        throw std::runtime_error("AkkEngine: invalid replication snapshot chunk");
    }
    if (!chunk.empty()) {
        pendingSnapshotOut.write(reinterpret_cast<const char*>(chunk.data()), std::ssize(chunk));
        if (!pendingSnapshotOut) { throw std::runtime_error("AkkEngine: failed to write replication snapshot chunk"); }
    }
    pendingSnapshotEntryRecordCrc.update(chunk);
    pendingSnapshotEntryValueCrc.update(chunk);
    pendingSnapshotEntryValueOffset += chunk.size();
}

void finishReplicaSnapshotEntry() {
    std::lock_guard lock(writeMu);
    if (!snapshotInProgress || !pendingSnapshotOut.is_open() || !pendingSnapshotEntryInProgress) {
        throw std::runtime_error("AkkEngine: finished replication snapshot entry without active entry");
    }
    if (pendingSnapshotEntryValueOffset != pendingSnapshotEntryValueSize || pendingSnapshotEntryValueCrc.finish() !=
        pendingSnapshotEntryValueCrc32c) { throw std::runtime_error("AkkEngine: replication snapshot entry checksum or size mismatch"); }
    writeU32Le(pendingSnapshotOut, pendingSnapshotEntryRecordCrc.finish());
    ++pendingSnapshotAppliedEntries;
    pendingSnapshotEntryInProgress = false;
    pendingSnapshotEntryValueSize = 0;
    pendingSnapshotEntryValueOffset = 0;
    pendingSnapshotEntryValueCrc32c = 0;
    pendingSnapshotEntryRecordCrc = Crc32cStream{};
    pendingSnapshotEntryValueCrc = Crc32cStream{};
}

void prepareReplicaSnapshotLocked(uint64_t seq, uint64_t entryCount) {
    if (!snapshotInProgress || seq != pendingSnapshotSeq || pendingSnapshotEntryInProgress || pendingSnapshotAppliedEntries != entryCount) {
        throw std::runtime_error("AkkEngine: invalid replication snapshot completion");
    }
    pendingSnapshotEntryCount = entryCount;
    if (pendingSnapshotOut.is_open()) {
        pendingSnapshotOut.seekp(5 + sizeof(uint64_t));
        writeU64Le(pendingSnapshotOut, entryCount);
        pendingSnapshotOut.flush();
        if (!pendingSnapshotOut) { throw std::runtime_error("AkkEngine: failed to flush replication snapshot staging file"); }
        pendingSnapshotOut.close();
    }
    const auto path = replicaSnapshotStagingPath();
    #ifdef _WIN32
    auto* const file = CreateFileW(path.c_str(), GENERIC_WRITE, FILE_SHARE_READ, nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (file == INVALID_HANDLE_VALUE || !FlushFileBuffers(file)) {
        if (file != INVALID_HANDLE_VALUE) { CloseHandle(file); }
        throw std::runtime_error("AkkEngine: failed to sync snapshot staging");
    }
    CloseHandle(file);
    #else
    const int file = ::open(path.c_str(), O_RDONLY); if (file < 0 || fsync(file) != 0) {
        if (file >= 0) { ::close(file); }
        throw std::runtime_error("AkkEngine: failed to sync snapshot staging");
    } ::close(file); const int directory = ::open(path.parent_path().c_str(), O_RDONLY | O_DIRECTORY); if (directory < 0 || fsync(directory) !=
        0) {
        if (directory >= 0) { ::close(directory); }
        throw std::runtime_error("AkkEngine: failed to sync snapshot staging directory");
    } ::close(directory);
    #endif
}

void finishReplicaSnapshotLocked(uint64_t seq, uint64_t entryCount, uint64_t partitionOwner = 0) {
    prepareReplicaSnapshotLocked(seq, entryCount);

    const auto path = replicaSnapshotStagingPath();
    corruptSnapshotStagingAtTestPoint(path, "snapshot.finish.before_staging_apply");
    std::ifstream staged{path, std::ios::binary};
    if (!staged) { throw std::runtime_error("AkkEngine: failed to open replication snapshot staging file for apply"); }

    const auto [stagedSeq, stagedEntryCount] = readReplicaSnapshotStagingHeader(staged);
    if (stagedSeq != seq || stagedEntryCount != pendingSnapshotEntryCount) {
        throw std::runtime_error("AkkEngine: replication snapshot staging metadata mismatch");
    }

    if (partitionOwner != 0) {
        // Validate scratch storage before reserving a local commit slot.
        // A corrupt receive can then reconnect without leaving a sequence gap.
        const auto entriesOffset = staged.tellg();
        for (uint64_t i = 0; i < stagedEntryCount; ++i) { (void)readStagedSnapshotRecord(staged, 0, i + 1, false); }
        char extra = 0;
        if (staged.get(extra)) { throw std::runtime_error("AkkEngine: trailing bytes in replication snapshot staging file"); }
        staged.clear();
        staged.seekg(entriesOffset);
        if (!staged) { throw std::runtime_error("AkkEngine: cannot rewind partition snapshot staging file"); }
    }
    const auto inScope = [&](std::span<const uint8_t> key) {
        if (partitionOwner == 0) { return true; }
        const auto targets = cluster::detail::partitionTargets(clusterConfig, key);
        return !targets.empty() && targets.front().nodeId == partitionOwner && std::ranges::any_of(
            targets,
            [&](const auto& target) { return target.nodeId == nodeId; }
        );
    };
    if (partitionOwner != 0 && stagedEntryCount == 0) {
        auto blobReadPin = blobManager ? blobManager->pinReads() : blob::BlobManager::ReadPin{};
        BufferArena emptyArena;
        memtable::MemTable::KeyRange emptyRange;
        const auto visible = snapshotSeq();
        auto emptyMt = memtable->iterator(emptyRange, visible);
        sst::SSTManager::Iterator emptySst;
        if (sstManager) { emptySst = sstManager->scanIter({}, {}, visible, true); }
        bool hasOwnedKey = false;
        for (const auto& record : scanGenerator(emptyArena, std::move(emptyMt), std::move(emptySst), blobManager.get(), std::move(blobReadPin))) {
            if (inScope(record.key)) {
                hasOwnedKey = true;
                break;
            }
        }
        // No storage change: only the sender's peer watermark advances.
        if (!hasOwnedKey) {
            staged.close();
            resetReplicaSnapshotStagingLocked();
            return;
        }
    }
    const uint64_t storageSeq = partitionOwner != 0 ? reserveWriteSeq(1) : seq;
    bool transactionState = false;
    uint64_t snapshotWalRecordCount = 0;
    auto blobReadPin = blobManager ? blobManager->pinReads() : blob::BlobManager::ReadPin{};
    BufferArena arena;
    memtable::MemTable::KeyRange range;
    const uint64_t visibleSeq = snapshotSeq();
    uint64_t storedHistoryThrough = 0;
    if (versionLog && clusterConfig.usesDataConsensus()) {
        versionLog->forceSync();
        storedHistoryThrough = versionLog->highestStoredSequence();
    }
    auto mt = memtable->iterator(range, visibleSeq);
    sst::SSTManager::Iterator sst;
    if (sstManager) { sst = sstManager->scanIter({}, {}, visibleSeq, true); }
    for (const auto& record : scanGenerator(arena, std::move(mt), std::move(sst), blobManager.get(), std::move(blobReadPin))) {
        const std::string key{reinterpret_cast<const char*>(record.key.data()), record.key.size()};
        if (inScope(record.key) && !pendingSnapshotKeys.contains(key)) {
            appendSnapshotWalRecord(storageSeq, record.key, {}, MemHdr16::FLAG_TOMBSTONE, 0);
            ++snapshotWalRecordCount;
        }
    }

    for (uint64_t i = 0; i < stagedEntryCount; ++i) {
        const auto entry = readStagedSnapshotRecord(staged, storageSeq, i + 1, true, storedHistoryThrough);
        if (entry.transaction) {
            if (entry.transactionKind == cluster::detail::SnapshotRecordKind::TRANSACTION) {
                if (transactionState || decodeDistributedState(entry.storedValue).sequence != seq) {
                    throw std::runtime_error("AkkEngine: invalid transaction snapshot cut");
                }
                transactionState = true;
            }
            continue;
        }
        if (entry.history) {
            // Applied Raft prefixes are common to all holders. History
            // reaches disk before the KV snapshot commit advances that prefix.
            if (versionLog && !entry.historyPresent) {
                versionLog->appendDeferred(entry.key, entry.sequence, entry.source, entry.timestamp, entry.flags, entry.storedValue);
            }
            continue;
        }
        appendSnapshotWalRecord(storageSeq, entry.key, entry.storedValue, entry.flags, entry.fp64);
        ++snapshotWalRecordCount;
    }

    char trailing = 0;
    if (staged.get(trailing)) { throw std::runtime_error("AkkEngine: trailing bytes in replication snapshot staging file"); }
    staged.close();
    if (distributedTransactionsEnabled() && !transactionState) { throw std::runtime_error("AkkEngine: missing transaction snapshot state"); }

    if (versionLog && clusterConfig.usesDataConsensus()) { versionLog->forceSync(); }

    if (walWriter) {
        crashAtTestPoint("snapshot.finish.after_wal_records");
        appendSnapshotWalCommit(storageSeq, snapshotWalRecordCount);
        if (partitionOwner == 0) { durableReplicaSnapshotSeq = std::max(durableReplicaSnapshotSeq, seq); }
        crashAtTestPoint("snapshot.finish.after_wal_commit");
    }

    BufferArena applyArena;
    auto applyBlobReadPin = blobManager ? blobManager->pinReads() : blob::BlobManager::ReadPin{};
    memtable::MemTable::KeyRange applyRange;
    auto applyMt = memtable->iterator(applyRange, visibleSeq);
    sst::SSTManager::Iterator applySst;
    if (sstManager) { applySst = sstManager->scanIter({}, {}, visibleSeq, true); }
    for (const auto& record : scanGenerator(applyArena, std::move(applyMt), std::move(applySst), blobManager.get(), std::move(applyBlobReadPin))) {
        const std::string key{reinterpret_cast<const char*>(record.key.data()), record.key.size()};
        if (inScope(record.key) && !pendingSnapshotKeys.contains(key)) {
            applySnapshotRecordMemory(storageSeq, record.key, {}, MemHdr16::FLAG_TOMBSTONE);
        }
    }
    crashAtTestPoint("snapshot.finish.after_tombstone_apply");
    pendingSnapshotKeys.clear();

    std::ifstream applyStaged{path, std::ios::binary};
    if (!applyStaged) { throw std::runtime_error("AkkEngine: failed to reopen replication snapshot staging file for apply"); }
    const auto [applyStagedSeq, applyStagedEntryCount] = readReplicaSnapshotStagingHeader(applyStaged);
    if (applyStagedSeq != seq || applyStagedEntryCount != stagedEntryCount) {
        throw std::runtime_error("AkkEngine: replication snapshot staging metadata changed during apply");
    }
    for (uint64_t i = 0; i < stagedEntryCount; ++i) {
        const auto entry = readStagedSnapshotRecord(applyStaged, storageSeq, i + 1, false);
        if (entry.history || entry.transaction) { continue; }
        applySnapshotRecordMemory(storageSeq, entry.key, entry.storedValue, entry.flags, entry.fp64, entry.miniKey);
    }
    if (applyStaged.get(trailing)) { throw std::runtime_error("AkkEngine: trailing bytes in replication snapshot staging file"); }

    if (transactionState) {
        cluster::ClusterSnapshot transactionsSnapshot;
        transactionsSnapshot.seq = seq;
        transactionsSnapshot.forEachEntry = [&](const cluster::SnapshotEntryVisitor& visitor) {
            std::ifstream source{path, std::ios::binary};
            const auto [cut, count] = readReplicaSnapshotStagingHeader(source);
            if (cut != seq || count != stagedEntryCount) { throw std::runtime_error("AkkEngine: changed transaction snapshot staging"); }
            for (uint64_t i = 0; i < count; ++i) {
                const auto entry = readStagedSnapshotRecord(source, seq, i + 1, false);
                if (!entry.transaction) { continue; }
                const auto key = cluster::detail::encodeSnapshotKey({entry.transactionKind, 0, entry.sequence, entry.source, 0, entry.key});
                if (!streamSnapshotValue(visitor, key, entry.storedValue, 0, nullptr)) { return false; }
            }
            return true;
        };
        installDistributedSnapshot(transactionsSnapshot);
    }

    memtable->advanceSeq(storageSeq);
    if (partitionOwner != 0) {
        replicationStorageSeq.store(std::max(replicationStorageSeq.load(std::memory_order_relaxed), storageSeq), std::memory_order_release);
        markWriteCommitted(storageSeq);
    }
    else {
        // A whole-database snapshot replaces the entire prefix.
        resetCommittedSeq(std::max(committedSeq.load(std::memory_order_acquire), seq));
        if (versionLog && clusterConfig.usesDataConsensus()) { versionLog->seedCommittedSeq(seq); }
    }
    commitCv.notify_all();
    applyStaged.close();
    resetReplicaSnapshotStagingLocked();
}

void installPartitionSnapshot(uint64_t owner, const cluster::ClusterSnapshot& snapshot) {
    if (clusterReplicationMode != cluster::ReplicationMode::PARTITIONED || owner == nodeId || !snapshot.forEachEntry) {
        throw std::runtime_error("AkkEngine: invalid partition snapshot owner");
    }
    // Incoming peers have independent spools; serialize use of the
    // engine's transaction staging file, without holding writeMu over network I/O.
    std::lock_guard installLock{partitionSnapshotMu};
    beginReplicaSnapshot(snapshot.seq, 0);
    try {
        uint64_t count = 0;
        const cluster::SnapshotEntryVisitor visitor{
            .beginEntry = [&](auto key, uint64_t size, uint32_t crc) {
                if (key.size() > UINT16_MAX || (!blobManager && size > UINT16_MAX)) {
                    throw std::runtime_error("AkkEngine: partition snapshot record is too large");
                }
                const auto targets = cluster::detail::partitionTargets(clusterConfig, key);
                if (targets.empty() || targets.front().nodeId != owner || std::ranges::none_of(
                    targets,
                    [&](const auto& target) { return target.nodeId == nodeId; }
                )) { throw std::runtime_error("AkkEngine: partition snapshot violates placement"); }
                beginReplicaSnapshotEntry(key, size, crc);
                return true;
            },
            .appendValueChunk = [&](uint64_t offset, auto chunk) {
                appendReplicaSnapshotEntryChunk(offset, chunk);
                return true;
            },
            .finishEntry = [&] {
                finishReplicaSnapshotEntry();
                ++count;
                return true;
            },
            .fileEntry = {},
        };
        if (!snapshot.forEachEntry(visitor)) { throw std::runtime_error("AkkEngine: incomplete partition snapshot"); }
        std::lock_guard lock{writeMu};
        finishReplicaSnapshotLocked(snapshot.seq, count, owner);
    }
    catch (...) {
        std::lock_guard lock{writeMu};
        resetReplicaSnapshotStagingLocked();
        throw;
    }
}

void finishReplicaSnapshot(uint64_t seq, uint64_t entryCount) {
    std::lock_guard lock(writeMu);
    finishReplicaSnapshotLocked(seq, entryCount);
}

void recoverReplicaSnapshot(uint64_t seq) {
    std::lock_guard lock(writeMu);
    bool transactionStateDurable = true;
    if (distributedTransactionsEnabled()) {
        std::lock_guard stateLock{distributedStateMu};
        transactionStateDurable = distributedState.sequence >= seq;
    }
    if (snapshotSeq() >= seq && transactionStateDurable) {
        durableReplicaSnapshotSeq = std::max(durableReplicaSnapshotSeq, seq);
        resetReplicaSnapshotStagingLocked();
        return;
    }
    loadReplicaSnapshotStagingForRecoveryLocked(seq);
    finishReplicaSnapshotLocked(seq, pendingSnapshotEntryCount);
}

[[nodiscard]] bool isReplicaSnapshotDurable(uint64_t seq) {
    std::lock_guard lock(writeMu);
    if (distributedTransactionsEnabled()) {
        std::lock_guard stateLock{distributedStateMu};
        if (distributedState.sequence < seq) { return false; }
    }
    return snapshotSeq() >= seq || durableReplicaSnapshotSeq >= seq;
}

/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/detail/Transactions.inc
// Included inside AkkEngine::Impl. Public writers already share mutationEpochMu.
struct TransactionRegistry {
    std::mutex mutex;
    std::atomic<uint32_t> active{0};
    uint64_t revision = 0, generation = 0, nextId = 0;
    uint64_t pinnedBytes = 0, trackedBytes = 0;
    uint32_t snapshots = 0;
    std::map<uint64_t, uint64_t> cuts;
    std::map<std::vector<uint8_t>, uint64_t> changes;
};
std::shared_ptr<TransactionRegistry> transactions = std::make_shared<TransactionRegistry>();
mutable std::shared_mutex transactionVisibilityMu;
mutable std::mutex transactionFailureMu;
std::exception_ptr transactionFailure;
// WAL replay can trigger flush callbacks before transaction recovery starts.
std::atomic<bool> transactionJournalPending{true};
uint64_t transactionJournalBytes = 0;
std::vector<fs::path> transactionJournals;

void noteTransactionMutation(std::span<const uint8_t> key) {
    auto& state = *transactions;
    if (state.active.load(std::memory_order_acquire) == 0) { return; }
    std::lock_guard lock{state.mutex};
    if (state.cuts.empty()) { return; }
    const auto revision = ++state.revision;
    std::vector<uint8_t> owned{key.begin(), key.end()};
    auto found = state.changes.find(owned);
    if (found == state.changes.end()) {
        const uint64_t charge = owned.size() + 64;
        if (charge > opts.transactions.maxTrackedBytes || state.trackedBytes > opts.transactions.maxTrackedBytes - charge) {
            // Ordinary writers must keep making progress. Old transactions now
            // fail validation rather than relying on an incomplete change log.
            ++state.generation; state.changes.clear(); state.trackedBytes = 0;
        }
        state.changes.emplace(std::move(owned), revision); state.trackedBytes += charge;
    } else { found->second = revision; }
}

struct TransactionReadSnapshot {
    std::shared_ptr<OperationGuard> operation;
    FixedQuerySource source;
};
std::shared_ptr<TransactionReadSnapshot> captureTransactionReadSnapshot(std::shared_ptr<OperationGuard> operation) {
    // Caller holds exclusive mutation admission, so parallel writers are also
    // finished before freezing sources. No lock survives the call.
    const auto bytes = memtable->approxSize();
    auto state = transactions;
    {
        std::lock_guard lock{state->mutex};
        if (bytes > opts.transactions.maxPinnedBytes || state->pinnedBytes > opts.transactions.maxPinnedBytes - bytes ||
            state->snapshots >= opts.transactions.maxOpen) { throw std::runtime_error("AkkEngine: transaction snapshot capacity exceeded"); }
        state->pinnedBytes += bytes; ++state->snapshots;
    }
    std::shared_ptr<void> reservation{state.get(), [state, bytes](void*) {
        std::lock_guard lock{state->mutex}; state->pinnedBytes -= bytes; --state->snapshots;
    }};
    std::lock_guard writeLock{writeMu};
    std::shared_lock storageLock{storageMu};
    const auto seq = snapshotSeq();
    memtable::MemTable::KeyRange range;
    std::optional<memtable::MemTable::RangeIterator> mt;
    if (sstManager) { mt.emplace(memtable->sealAndPinIterator(range, seq)); }
    else { mt = memtable->sealAndPinMemoryIterator(range, seq, false, opts.transactions.maxPinnedBytes, opts.transactions.maxOpen, true); }
    if (!mt) { throw std::runtime_error("AkkEngine: transaction snapshot capacity exceeded"); }
    auto st = sstManager ? sstManager->scanIter({}, {}, seq, true) : sst::SSTManager::Iterator{};
    auto blobPin = blobManager ? blobManager->pinReads() : blob::BlobManager::ReadPin{};
    return std::make_shared<TransactionReadSnapshot>(TransactionReadSnapshot{std::move(operation),
        {storage, std::move(reservation), std::move(blobPin), std::move(*mt), std::move(st)}});
}

struct TransactionRecord { std::vector<uint8_t> key, value; uint8_t flags = 0; };
struct TransactionDecision {
    uint64_t base, seq, source, timestamp;
    std::vector<TransactionRecord> records;
};
static std::vector<uint8_t> encodeTransactionDecision(const TransactionDecision& decision) {
    std::vector<uint8_t> out{'A','K','T','X','1'};
    pushU64(out, decision.base); pushU64(out, decision.seq); pushU64(out, decision.source); pushU64(out, decision.timestamp);
    pushU64(out, decision.records.size());
    for (const auto& record : decision.records) {
        out.push_back(record.flags); pushU32(out, static_cast<uint32_t>(record.key.size())); pushU64(out, record.value.size());
        out.insert(out.end(), record.key.begin(), record.key.end()); out.insert(out.end(), record.value.begin(), record.value.end());
    }
    pushU32(out, blob::crc32c(out)); return out;
}
static TransactionDecision readTransactionDecision(const fs::path& path) {
    const auto size = fs::file_size(path);
    if (size < 49 || size > 1024ull * 1024 * 1024) { throw std::runtime_error("AkkEngine: invalid transaction journal size"); }
    std::vector<uint8_t> bytes(static_cast<size_t>(size));
    std::ifstream file{path, std::ios::binary};
    if (!file.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(size))) { throw std::runtime_error("AkkEngine: unreadable transaction journal"); }
    size_t checksumPos = bytes.size() - 4; uint32_t checksum = 0;
    if (!pullU32(bytes, checksumPos, checksum) || blob::crc32c(std::span{bytes}.first(bytes.size() - 4)) != checksum ||
        !std::equal(bytes.begin(), bytes.begin() + 5, "AKTX1")) { throw std::runtime_error("AkkEngine: corrupt transaction journal"); }
    const std::span<const uint8_t> input{bytes.data(), bytes.size() - 4};
    size_t pos = 5; TransactionDecision decision{}; uint64_t count = 0;
    if (!pullU64(input, pos, decision.base) || !pullU64(input, pos, decision.seq) || !pullU64(input, pos, decision.source) ||
        !pullU64(input, pos, decision.timestamp) || !pullU64(input, pos, count) || count == 0 || count > 1'048'576 ||
        decision.base == 0 || decision.seq < decision.base || decision.seq - decision.base != count - 1 || decision.seq == UINT64_MAX) {
        throw std::runtime_error("AkkEngine: invalid transaction journal header");
    }
    std::vector<uint8_t> previous;
    for (uint64_t i = 0; i < count; ++i) {
        TransactionRecord record; uint32_t keySize = 0; uint64_t valueSize = 0;
        if (pos == input.size()) { throw std::runtime_error("AkkEngine: truncated transaction journal"); }
        record.flags = input[pos++];
        if ((record.flags & ~(MemHdr16::FLAG_TOMBSTONE | MemHdr16::FLAG_BLOB)) != 0 ||
            !pullU32(input, pos, keySize) || !pullU64(input, pos, valueSize) || keySize > UINT16_MAX ||
            keySize > input.size() - pos || valueSize > input.size() - pos - keySize ||
            ((record.flags & MemHdr16::FLAG_TOMBSTONE) != 0 && valueSize != 0) ||
            ((record.flags & MemHdr16::FLAG_BLOB) != 0 && valueSize != blob::BLOB_REF_SIZE)) {
            throw std::runtime_error("AkkEngine: invalid transaction journal record");
        }
        record.key.assign(input.begin() + pos, input.begin() + pos + keySize); pos += keySize;
        record.value.assign(input.begin() + pos, input.begin() + pos + valueSize); pos += static_cast<size_t>(valueSize);
        if (i != 0 && compareKey(previous, record.key) >= 0) { throw std::runtime_error("AkkEngine: unordered transaction journal"); }
        previous = record.key; decision.records.push_back(std::move(record));
    }
    if (pos != input.size()) { throw std::runtime_error("AkkEngine: trailing transaction journal data"); }
    return decision;
}

bool transactionHasPersistence() const noexcept { return walWriter || sstManager || versionLog || blobManager; }
void writeTransactionWatermark(const char* name, uint64_t seq) {
    std::vector<uint8_t> bytes{'A','K','T','M','1'}; pushU64(bytes, seq); pushU32(bytes, blob::crc32c(bytes));
    writeFileAtomicallyDurable(opts.paths.transactionDir / name, bytes);
}
uint64_t readTransactionWatermark(const char* name) const {
    const auto path = opts.paths.transactionDir / name;
    if (!fs::exists(path)) { return 0; }
    std::array<uint8_t, 17> bytes{}; std::ifstream file{path, std::ios::binary};
    if (fs::file_size(path) != bytes.size() || !file.read(reinterpret_cast<char*>(bytes.data()), bytes.size()) ||
        !std::equal(bytes.begin(), bytes.begin() + 5, "AKTM1")) { throw std::runtime_error("AkkEngine: invalid transaction watermark"); }
    size_t pos = 5; uint64_t seq = 0; uint32_t crc = 0;
    if (!pullU64(bytes, pos, seq) || !pullU32(bytes, pos, crc) || seq == UINT64_MAX ||
        blob::crc32c(std::span{bytes}.first(13)) != crc) { throw std::runtime_error("AkkEngine: corrupt transaction watermark"); }
    return seq;
}
void checkpointTransactions() {
    if (transactionJournals.empty()) { return; }
    {
        std::lock_guard lock{transactionFailureMu};
        if (transactionFailure) { std::rethrow_exception(transactionFailure); }
    }
    // A single commit sequence is shared by all keys. Flush the complete state
    // before releasing redo, since a partial shard flush may checkpoint that seq.
    if (sstManager) { memtable->forceFlush(); }
    if (walWriter) { walWriter->forceSync(); }
    if (versionLog) { versionLog->forceSync(); }
    // A durable watermark makes deletion idempotent even if old directory
    // entries reappear after a power loss; their Blob refs can now be reclaimed.
    writeTransactionWatermark("checkpoint.aktm", readTransactionDecision(transactionJournals.back()).seq);
    for (const auto& path : transactionJournals) { fs::remove(path); }
    transactionJournals.clear(); transactionJournalBytes = 0;
    transactionJournalPending.store(false, std::memory_order_release);
}
void applyTransactionDecision(const TransactionDecision& decision, bool recovering) {
    memtable->advanceSeq(decision.seq);
    for (const auto& record : decision.records) {
        if ((record.flags & MemHdr16::FLAG_BLOB) != 0) { (void)materializeQueryValue(record.flags, record.value, blobManager.get()); }
        const bool historyPresent = recovering && versionLog && versionLog->containsStoredRecord(record.key,
            decision.seq, decision.source, decision.timestamp, record.flags);
        if (walWriter) { walWriter->append(record.key, record.value, decision.seq, record.flags, 0, wal::WalAppendAck::WRITTEN); }
        if (versionLog && !historyPresent) { versionLog->appendDeferred(record.key, decision.seq, decision.source, decision.timestamp, record.flags, record.value); }
        bool apply = true;
        if (recovering) {
            const auto current = memtable->getPinned(record.key, UINT64_MAX);
            if (current && current->view.seq() >= decision.seq) { apply = false; }
            if (apply && sstManager) { const auto currentSst = sstManager->get(record.key); if (currentSst && currentSst->seq >= decision.seq) { apply = false; } }
        }
        if (apply) {
            if ((record.flags & MemHdr16::FLAG_TOMBSTONE) != 0) { memtable->remove(record.key, decision.seq); }
            else { memtable->put(record.key, record.value, decision.seq, record.flags); }
        }
        if (!recovering) { noteTransactionMutation(record.key); }
        crashAtTestPoint("transaction.after_apply_record");
    }
    if (recovering) { resetCommittedSeq(std::max(snapshotSeq(), decision.seq)); }
    else { for (auto seq = decision.base; seq <= decision.seq; ++seq) { markWriteCommitted(seq); } }
    if (versionLog) { versionLog->seedCommittedSeq(std::max(snapshotSeq(), decision.seq)); }
}
void recoverTransactions() {
    const auto& directory = opts.paths.transactionDir;
    if (directory.empty() || !fs::exists(directory)) { transactionJournalPending.store(false, std::memory_order_release); return; }
    const auto allocated = readTransactionWatermark("allocated.aktm");
    const auto checkpoint = readTransactionWatermark("checkpoint.aktm");
    if (!opts.components.clusterEnabled) {
        // Allocation survives a crash before the decision, so orphan Blob ids
        // are never reused for different contents on the next engine open.
        memtable->advanceSeq(allocated); resetCommittedSeq(std::max(snapshotSeq(), allocated));
        if (versionLog) { versionLog->seedCommittedSeq(std::max(snapshotSeq(), allocated)); }
    }
    struct Journal { fs::path path; uint64_t base, seq; };
    std::vector<Journal> journals;
    for (const auto& entry : fs::directory_iterator(directory)) {
        if (entry.is_regular_file() && entry.path().extension() == ".aktxn") {
            const auto decision = readTransactionDecision(entry.path());
            if (decision.seq > checkpoint) { journals.push_back({entry.path(), decision.base, decision.seq}); }
        }
    }
    if (journals.empty()) { transactionJournalPending.store(false, std::memory_order_release); return; }
    if (opts.components.clusterEnabled) { throw std::runtime_error("AkkEngine: pending standalone transactions cannot be opened as a Cluster"); }
    if (!transactionHasPersistence()) { throw std::runtime_error("AkkEngine: pending transactions require their persistence components"); }
    transactionJournalPending.store(true, std::memory_order_release);
    std::ranges::sort(journals, {}, &Journal::seq);
    uint64_t previous = 0;
    for (const auto& journal : journals) {
        if (previous >= journal.base) { throw std::runtime_error("AkkEngine: overlapping transaction journals"); }
        previous = journal.seq;
    }
    for (const auto& journal : journals) {
        const auto decision = readTransactionDecision(journal.path);
        applyTransactionDecision(decision, true);
        transactionJournals.push_back(journal.path); transactionJournalBytes += fs::file_size(journal.path);
    }
    checkpointTransactions();
}

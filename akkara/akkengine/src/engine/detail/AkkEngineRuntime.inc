/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/detail/AkkEngineRuntime.inc
// Included inside AkkEngine::Impl by AkkEngineImpl.hpp.
[[nodiscard]] uint64_t snapshotSeq() const noexcept {
    if (opts.runtime.writeVisibility == AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER) {
        return committedSeq.load(std::memory_order_acquire);
    }
    const uint64_t nextSeq = memtable ? memtable->lastSeq() : 0;
    return nextSeq > 0 ? nextSeq - 1 : 0;
}

[[nodiscard]] uint64_t clusterReplicationSeq() const noexcept {
    return clusterReplicationMode == cluster::ReplicationMode::PARTITIONED && !clusterConfig.usesDataConsensus()
               ? replicationStorageSeq.load(std::memory_order_acquire) : snapshotSeq();
}

void markWriteCommitted(uint64_t seq, bool knownContiguous = false) {
    // APPLIED reads can see out of order writes, but WAL retirement still
    // requires the contiguous publication frontier maintained here.
    if (knownContiguous && seq == committedSeq.load(std::memory_order_acquire) + 1u) {
        committedSeq.store(seq, std::memory_order_release);
        if (opts.runtime.parallelWriteOrder == AkkEngineOptions::ParallelWriteOrderMode::KEY_SEQUENCE) { commitCv.notify_all(); }
        return;
    }
    auto advanced = false;
    {
        std::lock_guard lock{commitMu};
        uint64_t current = committedSeq.load(std::memory_order_relaxed);
        if (seq <= current) { return; }

        ensureCompletedSeqRing();
        const size_t mask = completedSeqRing.size() - 1u;
        const uint64_t distance = seq - current;
        if (distance <= completedSeqRing.size()) {
            const size_t slot = seq & mask;
            if (completedSeqRing[slot] == 0 || completedSeqRing[slot] <= current) { completedSeqRing[slot] = seq; }
            else if (completedSeqRing[slot] != seq) { completedSeqOverflow.insert(seq); }
        }
        else { completedSeqOverflow.insert(seq); }

        while (true) {
            const uint64_t next = current + 1u;
            const size_t slot = next & mask;
            if (completedSeqRing[slot] == next) {
                completedSeqRing[slot] = 0;
                current = next;
                advanced = true;
                continue;
            }
            if (completedSeqOverflow.erase(next) != 0) {
                current = next;
                advanced = true;
                continue;
            }
            break;
        }
        if (advanced) { committedSeq.store(current, std::memory_order_release); }
    }
    if (advanced) { commitCv.notify_all(); }
}

void waitForKeySequenceReadVisibility() const {
    if (opts.runtime.parallelWriteOrder != AkkEngineOptions::ParallelWriteOrderMode::KEY_SEQUENCE || opts.runtime.writeVisibility !=
        AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER || !memtable) { return; }
    const uint64_t nextSeq = memtable->lastSeq();
    const uint64_t target = nextSeq > 0 ? nextSeq - 1 : 0;
    if (committedSeq.load(std::memory_order_acquire) >= target) { return; }

    std::unique_lock lock{commitMu};
    commitCv.wait(
        lock,
        [this, target] {
            return committedSeq.load(std::memory_order_acquire) >= target || keySequenceFailure || closed.load(std::memory_order_acquire);
        }
    );
    const auto failure = keySequenceFailure;
    lock.unlock();
    if (failure) { std::rethrow_exception(failure); }
}

void markKeySequenceFailure(const std::exception_ptr& failure) {
    {
        std::lock_guard lock{commitMu};
        if (!keySequenceFailure) { keySequenceFailure = failure; }
    }
    commitCv.notify_all();
}

void resetCommittedSeq(uint64_t seq) {
    std::lock_guard lock{commitMu};
    committedSeq.store(seq, std::memory_order_release);
    std::ranges::fill(completedSeqRing, 0);
    completedSeqOverflow.clear();
}

void publishRaftAppliedSequence(uint64_t sequence) {
    if (!clusterConfig.usesDataConsensus()) { return; }
    // Raft applies the committed log in order. Rejected preparations
    // and discarded proposals can leave unused sequence numbers.
    memtable->advanceSeq(sequence);
    const auto visible = std::max(snapshotSeq(), sequence);
    resetCommittedSeq(visible);
    if (versionLog) { versionLog->seedCommittedSeq(visible); }
    commitCv.notify_all();
}

void ensureCompletedSeqRing() { if (completedSeqRing.empty()) { completedSeqRing.resize(commitWindowSize); } }

void throwIfBackgroundFailed() const {
    {
        std::lock_guard lock{transactionFailureMu};
        if (transactionFailure) { std::rethrow_exception(transactionFailure); }
    }
    if (opts.runtime.parallelWriteOrder == AkkEngineOptions::ParallelWriteOrderMode::KEY_SEQUENCE) {
        std::exception_ptr failure;
        {
            std::lock_guard lock{commitMu};
            failure = keySequenceFailure;
        }
        if (failure) { std::rethrow_exception(failure); }
    }
    if (walWriter) { walWriter->throwIfFailed(); }
    if (sstManager) { sstManager->throwIfBackgroundFailed(); }
}

[[nodiscard]] wal::WalAppendAck walAckForWriteDurability() const noexcept {
    switch (opts.runtime.writeDurability) {
        case AkkEngineOptions::WriteDurabilityMode::MEMORY:
        case AkkEngineOptions::WriteDurabilityMode::ENQUEUED: return wal::WalAppendAck::ENQUEUED;
        case AkkEngineOptions::WriteDurabilityMode::WRITTEN: return wal::WalAppendAck::WRITTEN;
        case AkkEngineOptions::WriteDurabilityMode::SYNCED: return wal::WalAppendAck::SYNCED;
    }
    return wal::WalAppendAck::WRITTEN;
}

[[nodiscard]] uint64_t reserveWriteSeq(uint64_t count) {
    if (count != 1 || opts.runtime.sequence.allocation != AkkEngineOptions::SequenceAllocationMode::THREAD_LOCAL_RANGES) {
        return memtable->reserveSeq(count);
    }

    struct SeqRangeCache {
        const Impl* owner = nullptr;
        uint64_t next = 0;
        uint64_t end = 0;
    };
    thread_local SeqRangeCache cache;

    if (cache.owner != this || cache.next >= cache.end) {
        const uint32_t rangeSize = std::max<uint32_t>(2, opts.runtime.sequence.threadLocalRangeSize);
        const uint64_t base = memtable->reserveSeq(rangeSize);
        cache.owner = this;
        cache.next = base;
        cache.end = base + rangeSize;
    }
    return cache.next++;
}

[[nodiscard]] std::shared_ptr<StorageState> pinStorage() const {
    std::shared_lock lock{storageMu};
    return storage;
}

[[nodiscard]] bool tryBeginOperation() {
    uint64_t state = lifecycleState.load(std::memory_order_acquire);
    while ((state & LIFECYCLE_CLOSED) == 0) {
        if ((state & LIFECYCLE_OPERATION_MASK) == LIFECYCLE_OPERATION_MASK) {
            throw std::runtime_error("AkkEngine: too many in-flight operations");
        }
        if (lifecycleState.compare_exchange_weak(state, state + 1u, std::memory_order_acq_rel, std::memory_order_acquire)) { return true; }
    }
    return false;
}

void endOperation() noexcept {
    const uint64_t previous = lifecycleState.fetch_sub(1u, std::memory_order_acq_rel);
    if ((previous & LIFECYCLE_OPERATION_MASK) == 1u) { lifecycleCv.notify_all(); }
}

[[nodiscard]] bool beginClose() {
    std::unique_lock lock{lifecycleMu};
    if (closed.load(std::memory_order_acquire)) {
        lifecycleCv.wait(lock, [this]() { return !closing; });
        return false;
    }
    lifecycleState.fetch_or(LIFECYCLE_CLOSED, std::memory_order_acq_rel);
    closed.store(true, std::memory_order_release);
    closing = true;
    lock.unlock();
    commitCv.notify_all();
    if (clusterRuntime) { clusterRuntime->cancelMirrorRecovery(); }
    if (walWriter) { walWriter->requestClose(); }
    lock.lock();
    lifecycleCv.wait(lock, [this]() { return (lifecycleState.load(std::memory_order_acquire) & LIFECYCLE_OPERATION_MASK) == 0; });
    return true;
}

void finishClose() noexcept {
    {
        std::lock_guard lock{lifecycleMu};
        closing = false;
    }
    lifecycleCv.notify_all();
}

void replaceStorage(std::shared_ptr<StorageState> replacement) {
    if (!replacement) { throw std::invalid_argument("AkkEngine: replacement storage is required"); }
    std::unique_lock lock{storageMu};
    storage = std::move(replacement);
    manifest.bind(&storage->manifest);
    sstManager.bind(&storage->sstManager);
    memtable.bind(&storage->memtable);
    walWriter.bind(&storage->walWriter);
    blobManager.bind(&storage->blobManager);
    versionLog.bind(&storage->versionLog);
}

[[nodiscard]] bool persistent() const noexcept {
    return opts.components.walEnabled || opts.components.sstEnabled || opts.components.manifestEnabled;
}

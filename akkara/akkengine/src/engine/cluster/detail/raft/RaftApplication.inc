/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */


// Included inside RaftConsensusRuntime::Impl.
struct BlobApplyBatch {
    bool complete = false;
    size_t nextEntryOffset = 0;
    uint64_t finalLogIndex = 0;
    uint64_t seq = 0;
    uint64_t blobId = 0;
    uint64_t totalSize = 0;
    uint32_t contentCrc32c = 0;
};

BlobApplyBatch applyBlobBatch(const std::vector<RaftLogEntry>& entries, size_t start) const {
    BlobApplyBatch batch;
    batch.nextEntryOffset = start;
    if (start >= entries.size()) { return batch; }
    if (!hasRaftBlobStorage(callbacks_)) { throw std::runtime_error("RaftConsensusRuntime: Blob storage callbacks are required to apply Blob data"); }
    const auto& first = entries[start];
    RaftBlobChunk chunk;
    if (first.kind != RaftEntryKind::BLOB || !decodeBlobChunkKey(first.key, chunk) || chunk.offset != 0 || first.value.size() > chunk.totalSize) {
        throw std::runtime_error("RaftConsensusRuntime: corrupt Blob log entry");
    }
    const uint64_t seq = first.clientSeq;
    Crc32cStream contentCrc;

    uint64_t expectedOffset = 0;
    size_t pos = start;
    while (pos < entries.size()) {
        const auto& entry = entries[pos];
        RaftBlobChunk current;
        const uint64_t entrySeq = entry.clientSeq;
        if (entry.kind != RaftEntryKind::BLOB || !decodeBlobChunkKey(entry.key, current) || current.blobId != chunk.blobId || entrySeq != seq ||
            current.totalSize != chunk.totalSize || current.contentCrc32c != chunk.contentCrc32c || current.offset != expectedOffset || entry.value.
            size() > current.totalSize - current.offset) { throw std::runtime_error("RaftConsensusRuntime: out-of-order Blob log chunk"); }
        contentCrc.update(entry.value);
        expectedOffset += entry.value.size();
        batch.finalLogIndex = entry.index;
        ++pos;
        if (expectedOffset == chunk.totalSize) {
            if (contentCrc.finish() != chunk.contentCrc32c) { throw std::runtime_error("RaftConsensusRuntime: Blob log payload CRC mismatch"); }
            bool blobStarted = false;
            try {
                callbacks_.beginBlob(seq, chunk.blobId, chunk.totalSize, chunk.contentCrc32c);
                blobStarted = true;
                for (size_t callbackPos = start; callbackPos < pos; ++callbackPos) {
                    RaftBlobChunk callbackChunk;
                    if (!decodeBlobChunkKey(entries[callbackPos].key, callbackChunk)) {
                        throw std::runtime_error("RaftConsensusRuntime: corrupt Blob log entry");
                    }
                    callbacks_.appendBlobChunk(seq, chunk.blobId, callbackChunk.offset, entries[callbackPos].value);
                }
                callbacks_.finishBlob(seq, chunk.blobId);
            }
            catch (...) {
                if (blobStarted && callbacks_.abortBlob) { callbacks_.abortBlob(seq, chunk.blobId); }
                throw;
            }
            batch.complete = true;
            batch.nextEntryOffset = pos;
            batch.seq = seq;
            batch.blobId = chunk.blobId;
            batch.totalSize = chunk.totalSize;
            batch.contentCrc32c = chunk.contentCrc32c;
            return batch;
        }
    }
    if (expectedOffset > chunk.totalSize) { throw std::runtime_error("RaftConsensusRuntime: Blob log payload exceeds declared size"); }
    batch.nextEntryOffset = pos;
    return batch;
}

void failApplication(std::exception_ptr failure) {
    applyFailed_.store(true);
    {
        std::lock_guard lock{mutex_};
        recordFailure(logIoFailed_ ? ClusterFailureCode::RAFT_PERSISTENCE : ClusterFailureCode::RAFT_APPLY, ClusterHealthState::FAILED);
        stopRuntimeLocked(); finishProposalsLocked(); proposalQueue_.clear();
    }
    if (callbacks_.fatalFailure) { try { callbacks_.fatalFailure(failure); } catch (...) {} }
}

void applyCommitted() {
    std::lock_guard applyLock{applyMutex_};
    if (applyFailed_.load()) { throw std::runtime_error("Raft: reopen required after state-machine application failure"); }
    try {
        std::vector<RaftLogEntry> toApply;
        {
            std::lock_guard lock{mutex_};
            const auto offset = static_cast<size_t>(std::min<uint64_t>(lastApplied_ - lastIncludedIndex_, log_.size()));
            for (size_t i = offset; i < log_.size() && log_[i].index <= commitIndex_; ++i) { toApply.push_back(log_[i]); }
        }

        uint64_t appliedThrough = 0;
        bool appliedDurableMutation = false;
        for (size_t i = 0; i < toApply.size();) {
            const auto& entry = toApply[i];
            if (entry.kind == RaftEntryKind::CONFIG_JOINT || entry.kind == RaftEntryKind::CONFIG_FINAL) {
                std::lock_guard lock{mutex_};
                applyConfigEntryLocked(entry);
                appliedThrough = entry.index;
                ++i;
            }
            else if (entry.kind == RaftEntryKind::BLOB) {
                auto batch = applyBlobBatch(toApply, i);
                if (!batch.complete) { break; }
                appliedDurableMutation = true;
                appliedThrough = batch.finalLogIndex;
                i = batch.nextEntryOffset;
            }
            else {
                if (entry.kind != RaftEntryKind::NOOP && callbacks_.apply) {
                    callbacks_.apply(entry.clientSeq, entry.op, entry.key, entry.value, entry.flags, entry.sourceNodeId, entry.timestampNs);
                    appliedDurableMutation = true;
                }
                appliedThrough = entry.index;
                ++i;
            }
        }

        if (appliedThrough == 0) { return; }
        if (appliedDurableMutation && callbacks_.forceDurable) {
            crashAtLogTestPoint("raft.apply.before_durable");
            try { callbacks_.forceDurable(); }
            catch (...) {
                std::lock_guard lock{mutex_};
                stopRuntimeLocked();
                throw;
            }
        }
        crashAtLogTestPoint("raft.request.after_apply");
        std::lock_guard lock{mutex_};
        if (appliedThrough > lastApplied_) {
            const auto backup = captureDurableLogStateLocked(lastLogIndex() + 1);
            lastApplied_ = appliedThrough;
            rebuildRequestResultsLocked(backup.lastApplied);
            try { persistLog(); }
            catch (...) {
                restoreDurableLogStateLocked(backup);
                stopRuntimeLocked();
                throw;
            }
        }
    } catch (...) {
        const auto failure = std::current_exception();
        failApplication(failure);
        std::rethrow_exception(failure);
    }
}


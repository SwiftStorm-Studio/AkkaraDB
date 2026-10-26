/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */


// Included inside RaftConsensusRuntime::Impl.
std::future<void> submitProposal(std::vector<RaftLogEntry> entries, std::shared_ptr<Proposal> proposal = {}) {
    if (entries.empty()) { throw std::invalid_argument("RaftConsensusRuntime: proposal must contain at least one entry"); }
    for (const auto& entry : entries) {
        if (!isValidStateMachineEntry(entry)) { throw std::invalid_argument("RaftConsensusRuntime: proposal exceeds frame or data storage limits"); }
    }
    for (auto& entry : entries) { entry.proposalFinal = false; }
    entries.back().proposalFinal = true;
    if (!proposal) { proposal = std::make_shared<Proposal>(); }
    proposal->entries = std::move(entries);
    for (const auto& entry : proposal->entries) { proposal->bytes += entry.key.size() + entry.value.size() + sizeof(RaftLogEntry); }
    auto future = proposal->completion.get_future();
    std::unique_lock lock{mutex_};
    waitForAdministrationLocked(lock);
    if (!running_ || role_.load() != RaftRole::LEADER || administrativeChange_ || (proposal->term != 0 && proposal->term != currentTerm_)) {
        throw std::runtime_error("RaftConsensusRuntime: proposal requires an active leader without a membership/transfer operation");
    }
    constexpr size_t limit = 256ull * 1024 * 1024;
    size_t pendingBytes = 0;
    // Timed-out entries may still commit. Keep counting their retained
    // payloads so a partition cannot bypass admission backpressure.
    for (auto it = log_.rbegin(); it != log_.rend() && it->index > lastApplied_; ++it) {
        pendingBytes += it->key.size() + it->value.size() + sizeof(RaftLogEntry);
    }
    for (const auto& queued : proposalQueue_) { if (!queued->error) { pendingBytes += queued->bytes; } }
    if (proposals_.size() >= 4096 || proposal->bytes > limit || pendingBytes > limit - proposal->bytes) {
        throw std::runtime_error("RaftConsensusRuntime: proposal queue is full");
    }
    proposal->term = currentTerm_;
    proposal->deadline = detail::ReconfigurationDeadline::cap(Clock::now() + std::chrono::milliseconds{config_.consistency().ackTimeoutMs});
    proposals_.push_back(proposal);
    proposalQueue_.push_back(proposal);
    cv_.notify_all();
    return future;
}

void finishProposalsLocked() {
    std::erase_if(
        proposals_,
        [&](const auto& proposal) {
            const auto applied = currentTerm_ == proposal->term && role_.load() == RaftRole::LEADER && proposal->index != 0 && lastApplied_ >=
                proposal->index && (proposal->index <= lastIncludedIndex_ || termAt(proposal->index) == (proposal->entryTerm
                    ? proposal->entryTerm
                    : proposal->term));
            if (!proposal->error && !applied && running_ && role_.load() == RaftRole::LEADER && currentTerm_ == proposal->term && Clock::now() <
                proposal->deadline) { return false; }
            if (!proposal->error && applied) {
                proposal->completion.set_value();
                if (proposal->request) {
                    proposal->request->promise.set_value({ClusterRequestStatus::APPLIED, proposal->request->sequence, proposal->index});
                }
            }
            else {
                const auto appended = proposal->index != 0;
                std::string message = appended
                                          ? "Raft: proposal interrupted after append; outcome unknown"
                                          : "Raft: proposal rejected before append";
                if (appended && proposal->request) { message += "; query or retry with the same request id"; }
                if (proposal->error) {
                    try { std::rethrow_exception(proposal->error); }
                    catch (const std::exception& error) {
                        message += ": ";
                        message += error.what();
                    }
                    catch (...) {}
                }
                proposal->error = std::make_exception_ptr(
                    ClusterRoutingError(
                        appended ? ClusterRoutingCode::OUTCOME_UNKNOWN : ClusterRoutingCode::WRITE_REJECTED,
                        {},
                        std::move(message),
                        proposal->request ? std::optional{proposal->request->id} : std::nullopt
                    )
                );
                proposal->completion.set_exception(proposal->error);
                if (proposal->request) { proposal->request->promise.set_exception(proposal->error); }
            }
            return true;
        }
    );
    cv_.notify_all();
}

void advanceLeaderCommit() {
    bool advanced = false;
    {
        std::lock_guard lock{mutex_};
        if (!running_ || role_.load() != RaftRole::LEADER) { return; }
        for (auto it = log_.rbegin(); it != log_.rend() && it->index > commitIndex_; ++it) {
            if (!canCommitEntryLocked(*it)) { continue; }
            commitIndex_ = it->index;
            persistLog();
            advanced = true;
            break;
        }
    }
    applyCommitted();
    {
        std::lock_guard lock{mutex_};
        finishProposalsLocked();
        peerReplicationCv_.notify_all();
    }
    if (advanced) { sendHeartbeats(); }
}

void proposalLoop() {
    for (;;) {
        try {
            bool appended = false;
            {
                std::unique_lock lock{mutex_};
                cv_.wait_for(lock, std::chrono::milliseconds{10}, [&] { return !running_ || !proposalQueue_.empty(); });
                if (!running_) {
                    finishProposalsLocked();
                    proposalQueue_.clear();
                    return;
                }
                if (!proposalQueue_.empty()) {
                    // Coalesce concurrent admissions without waiting for prior quorum I/O.
                    cv_.wait_for(lock, std::chrono::milliseconds{1}, [&] { return !running_ || proposalQueue_.size() >= 256; });
                }
                size_t batchBytes = 0;
                for (size_t count = 0; running_ && !proposalQueue_.empty() && count < 256; ++count) {
                    auto proposal = proposalQueue_.front();
                    proposalQueue_.pop_front();
                    if (proposal->error) { continue; }
                    if (role_.load() != RaftRole::LEADER || currentTerm_ != proposal->term || Clock::now() >= proposal->deadline) {
                        proposal->error = std::make_exception_ptr(std::runtime_error("Raft proposal expired before append"));
                        continue;
                    }
                    const auto previousSize = log_.size();
                    try {
                        for (auto& entry : proposal->entries) {
                            entry.term = currentTerm_;
                            entry.index = lastLogIndex() + 1;
                            if (!isValidStateMachineEntry(entry) || !entryPreservesStateMachineSequenceLocked(entry)) {
                                throw std::invalid_argument("RaftConsensusRuntime: invalid proposal or state-machine sequence");
                            }
                            log_.push_back(std::move(entry));
                        }
                        proposal->index = lastLogIndex();
                        proposal->entries.clear();
                        appended = true;
                        batchBytes += proposal->bytes;
                    }
                    catch (...) {
                        invalidateLogSuffixLocked(lastIncludedIndex_ + previousSize + 1);
                        log_.resize(previousSize);
                        proposal->error = std::current_exception();
                    }
                    if (batchBytes >= 4 * 1024 * 1024) { break; }
                }
                if (appended) {
                    persistLog();
                    ++proposalBatches_;
                    for (const auto& proposal : proposals_) { if (proposal->index != 0 && !proposal->error) { proposal->localDurable = true; } }
                    cv_.notify_all();
                }
                finishProposalsLocked();
            }
            if (appended) { sendHeartbeats(); }
            advanceLeaderCommit();
        }
        catch (...) {
            std::lock_guard lock{mutex_};
            stopRuntimeLocked();
            finishProposalsLocked();
            proposalQueue_.clear();
            return;
        }
    }
}

struct AdministrativeGuard {
    Impl& runtime;
    std::unique_lock<std::timed_mutex> serialization;
    bool paused = false;

    explicit AdministrativeGuard(Impl& owner, bool pause = true)
        : runtime{owner}, serialization{detail::ReconfigurationDeadline::lock(owner.administrationMutex_)} { if (pause) { pauseWrites(); } }

    void pauseWrites() {
        std::unique_lock lock{runtime.mutex_};
        runtime.administrativeChange_ = true;
        if (!runtime.cv_.wait_until(
            lock,
            detail::ReconfigurationDeadline::cap(Clock::now() + std::chrono::milliseconds{runtime.runtimeOptions_.reconfiguration.timeoutMs}),
            [this] { return runtime.proposals_.empty() || !runtime.running_; }
        ) || !runtime.running_) {
            runtime.administrativeChange_ = false;
            runtime.cv_.notify_all();
            throw std::runtime_error("RaftConsensusRuntime: outstanding writes did not drain before administrative change");
        }
        paused = true;
    }

    ~AdministrativeGuard() {
        if (!paused) { return; }
        std::lock_guard lock{runtime.mutex_};
        runtime.administrativeChange_ = false;
        runtime.cv_.notify_all();
    }
};

void waitForAdministrationLocked(std::unique_lock<std::mutex>& lock) {
    if (!administrativeChange_) { return; }
    if (runtimeOptions_.reconfiguration.writePolicy == ReconfigurationWritePolicy::REJECT || !cv_.wait_until(
        lock,
        detail::ReconfigurationDeadline::cap(Clock::now() + std::chrono::milliseconds{runtimeOptions_.reconfiguration.timeoutMs}),
        [this] { return !administrativeChange_ || !running_; }
    )) { throw std::runtime_error("Raft: write admission paused by membership change"); }
}

static uint64_t requestNow() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).
        count());
}

void expireRequestsLocked(uint64_t time) {
    if (!resultStore_) { return; }
    const auto before = resultStore_->count(); resultStore_->advanceFloor(time); observeResultStore();
    expiredRequests_.fetch_add(before - requestState_.count);
}
void rebuildRequestResultsLocked(uint64_t afterIndex = 0) {
    const auto first = std::ranges::upper_bound(log_, afterIndex, {}, &RaftLogEntry::index);
    for (auto it = first; it != log_.end() && it->index <= lastApplied_; ++it) {
        if (!it->requestId) { continue; }
        ensureResultStore(); expireRequestsLocked(it->requestTime);
        if (it->requestId->expiresAtUnixMs <= requestState_.expiryFloor) { continue; }
        const RequestRecord record{it->requestFingerprint, it->clientSeq, it->index};
        const auto old = retainedRequest(*it->requestId);
        if (old && (old->index != record.index || old->fingerprint != record.fingerprint)) { throw std::runtime_error("Raft: conflicting retained result"); }
        std::vector<uint8_t> value; writeRequestRecord(value, record);
        resultStore_->put(requestStoreKey(*it->requestId), {it->requestId->expiresAtUnixMs, it->index, std::move(value)}); observeResultStore();
    }
}

static std::shared_future<ClusterRequestResult> readyRequest(const ClusterRequestResult& result) {
    std::promise<ClusterRequestResult> promise;
    promise.set_value(result);
    return promise.get_future().share();
}

uint64_t reserveStateMachineSequenceLocked() {
    uint64_t highest = std::max(lastAssignedStateMachineSeq_, lastIncludedStateMachineSeq_);
    validateLogPrefixLocked(log_.size());
    if (!validatedLog_.empty()) { highest = std::max(highest, validatedLog_.back().sequence); }
    if (highest == UINT64_MAX) { throw std::overflow_error("RaftConsensusRuntime: state-machine sequence exhausted"); }
    lastAssignedStateMachineSeq_ = highest + 1;
    return lastAssignedStateMachineSeq_;
}

std::vector<RaftLogEntry> mutationEntries(uint64_t sequence, ClusterMutation mutation) const {
    if (sequence == 0) { throw std::invalid_argument("RaftConsensusRuntime: state-machine sequence must be non-zero"); }
    std::vector<RaftLogEntry> entries;
    if (mutation.blob) {
        if (runtimeOptions_.raftBlobPolicy != RaftBlobPolicy::RAFT_LOG) {
            throw std::invalid_argument("RaftConsensusRuntime: atomic Blob payload requires RAFT_LOG policy");
        }
        const auto& content = mutation.blob->content;
        const auto contentCrc32c = crcBytes(content);
        const size_t chunkSize = runtimeOptions_.raftBlobChunkSizeBytes;
        size_t offset = 0;
        do {
            const auto count = std::min(chunkSize, content.size() - offset);
            RaftLogEntry blob;
            blob.clientSeq = sequence;
            blob.kind = RaftEntryKind::BLOB;
            blob.op = ReplOpType::PUT;
            blob.sourceNodeId = mutation.sourceNodeId;
            blob.key = encodeBlobChunkKey(
                RaftBlobChunk{
                    .blobId = mutation.blob->blobId,
                    .offset = offset,
                    .totalSize = content.size(),
                    .contentCrc32c = contentCrc32c,
                }
            );
            blob.value.assign_range(std::span{content}.subspan(offset, count));
            entries.push_back(std::move(blob));
            offset += count;
        }
        while (offset < content.size());
    }

    RaftLogEntry entry;
    entry.clientSeq = sequence;
    entry.kind = RaftEntryKind::MUTATION;
    entry.op = mutation.op;
    entry.flags = mutation.recordFlags;
    entry.sourceNodeId = mutation.sourceNodeId;
    entry.timestampNs = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count());
    entry.key = std::move(mutation.key);
    entry.value = std::move(mutation.value);
    if (!isValidStateMachineEntry(entry)) { throw std::invalid_argument("RaftConsensusRuntime: prepared mutation exceeds frame or data storage limits"); }
    entries.push_back(std::move(entry));
    return entries;
}

uint64_t validateRequestLocked(const ClusterRequestId& id) {
    if (!runtimeOptions_.requests.enabled) {
        rejectedRequests_.fetch_add(1, std::memory_order_relaxed);
        throw std::logic_error("Raft: retry-safe requests are disabled");
    }
    if (id.expiresAtUnixMs == 0 || !std::ranges::any_of(id.nonce, [](uint8_t b) { return b != 0; })) {
        rejectedRequests_.fetch_add(1, std::memory_order_relaxed);
        throw std::invalid_argument("Raft: invalid request identity");
    }
    const auto now = std::max(requestNow(), requestState_.expiryFloor);
    if (id.expiresAtUnixMs > now && id.expiresAtUnixMs - now > runtimeOptions_.requests.maxRetentionMs) {
        rejectedRequests_.fetch_add(1, std::memory_order_relaxed);
        throw std::invalid_argument("Raft: request exceeds maximum retry retention");
    }
    return now;
}


/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */


// Included inside RaftConsensusRuntime::Impl.
bool claimPeerReplication(uint64_t peerId) {
    std::unique_lock lock{mutex_};
    const auto available = [&] {
        const auto* state = peerState(peerId);
        return !running_ || role_.load() != RaftRole::LEADER || state == nullptr || !state->inFlight;
    };
    const auto deadline = detail::ReconfigurationDeadline::current().deadline;
    if (deadline == Clock::time_point::max()) { peerReplicationCv_.wait(lock, available); }
    else {
        while (!available()) {
            detail::ReconfigurationDeadline::check();
            peerReplicationCv_.wait_until(lock, std::min(deadline, Clock::now() + std::chrono::milliseconds{25}));
        }
        detail::ReconfigurationDeadline::check();
    }
    if (!running_ || role_.load() != RaftRole::LEADER) { return false; }
    auto* state = peerState(peerId);
    if (state == nullptr) { return false; }
    state->inFlight = true;
    return true;
}

void releasePeerReplication(uint64_t peerId) noexcept {
    {
        std::lock_guard lock{mutex_};
        if (auto* state = peerState(peerId); state != nullptr) { state->inFlight = false; }
    }
    peerReplicationCv_.notify_all();
}

bool replicateEntryToMajority(const RaftLogEntry& entry, std::chrono::milliseconds retryWindow = std::chrono::milliseconds{0}) {
    const auto deadline = detail::ReconfigurationDeadline::cap(
        Clock::now() + (retryWindow.count() == 0 ? std::chrono::milliseconds{config_.consistency().ackTimeoutMs} : retryWindow)
    );
    const auto entryTargets = replicationTargetsForEntry(entry);
    do {
        detail::ReconfigurationDeadline::check();
        std::vector<NodeInfo> targets;
        {
            std::lock_guard lock{mutex_};
            if (!running_ || role_.load() != RaftRole::LEADER || currentTerm_ != entry.term) { return false; }
            if (peerReplication_.empty()) { resetLeaderReplicationState(); }
            ensurePeerReplicationTargetsLocked(entryTargets ? &*entryTargets : nullptr);
            if (hasCommitQuorumLocked(entry.index, &entry)) { return true; }
            targets = peers_;
        }
        for (const auto& peer : targets) { scheduleReplication(peer, entry.index, false); }
        std::unique_lock lock{mutex_};
        peerReplicationCv_.wait_until(
            lock,
            std::min(deadline, Clock::now() + std::chrono::milliseconds{50}),
            [&] {
                return !running_ || role_.load() != RaftRole::LEADER || currentTerm_ != entry.term || hasCommitQuorumLocked(entry.index, &entry);
            }
        );
        if (!running_ || role_.load() != RaftRole::LEADER || currentTerm_ != entry.term) { return false; }
        if (hasCommitQuorumLocked(entry.index, &entry)) { return true; }
        if (Clock::now() >= deadline) { return false; }
    }
    while (true);
}

bool commitOutstandingEntry(std::chrono::milliseconds retryWindow) {
    RaftLogEntry entry;
    uint64_t term = 0;
    {
        std::lock_guard lock{mutex_};
        if (!running_ || role_.load() != RaftRole::LEADER || commitIndex_ == lastLogIndex() || log_.empty()) { return true; }
        entry = log_.back();
        term = currentTerm_;
    }
    if (!replicateEntryToMajority(entry, retryWindow)) { return false; }
    {
        std::lock_guard lock{mutex_};
        if (role_.load() != RaftRole::LEADER || currentTerm_ != term) { return false; }
        if (entry.index > commitIndex_ && canCommitEntryLocked(entry)) {
            const auto backup = captureDurableLogStateLocked(lastLogIndex() + 1);
            try {
                commitIndex_ = entry.index;
                persistLog();
            }
            catch (...) {
                restoreDurableLogStateLocked(backup);
                throw;
            }
        }
    }
    applyCommitted();
    sendHeartbeats();
    return true;
}

bool replicatePeerTo(uint64_t peerId, uint64_t targetIndex, bool forceHeartbeat = false, uint64_t readRound = 0, bool migration = false) {
    const auto transferStarted = Clock::now();
    const auto transferDeadline = detail::ReconfigurationDeadline::cap(
        transferStarted + std::chrono::milliseconds{runtimeOptions_.reconfiguration.timeoutMs}
    );
    uint64_t transferred = 0;
    if (!claimPeerReplication(peerId)) { return false; }
    struct PeerReplicationGuard {
        Impl* runtime;
        uint64_t peerId;
        ~PeerReplicationGuard() { if (runtime != nullptr) { runtime->releasePeerReplication(peerId); } }
    } guard{this, peerId};

    while (true) {
        detail::ReconfigurationDeadline::check();
        if (migration && Clock::now() >= transferDeadline) { return false; }
        NodeInfo peer;
        uint64_t requestTerm = 0;
        auto needsSnapshot = false;
        AppendEntries appendRequest;
        {
            std::lock_guard lock{mutex_};
            if (!running_ || role_.load() != RaftRole::LEADER) { return false; }
            if (!containsNode(committedMembers_, peerId)) { migration = true; }
            auto* state = peerState(peerId);
            if (state == nullptr) { return false; }
            if (state->matchIndex >= targetIndex && !forceHeartbeat) { return true; }

            peer = state->node;
            requestTerm = currentTerm_;
            if (state->nextIndex <= lastIncludedIndex_) { needsSnapshot = true; }
            else {
                appendRequest.term = currentTerm_;
                appendRequest.leaderId = selfNodeId_;
                appendRequest.prevLogIndex = state->nextIndex - 1;
                appendRequest.prevLogTerm = termAt(appendRequest.prevLogIndex);
                appendRequest.leaderCommit = commitIndex_;
                if (state->matchIndex < targetIndex) {
                    appendRequest.entries = entriesFrom(state->nextIndex);
                    if (appendRequest.entries.empty()) { return false; }
                }
            }
        }
        if (needsSnapshot) {
            InstallSnapshotResponse response;
            if (!installSnapshotStream(peer, requestTerm, response)) {
                if (response.term > requestTerm) { becomeFollower(response.term); }
                return false;
            }
            std::lock_guard lock{mutex_};
            if (!running_ || role_.load() != RaftRole::LEADER || currentTerm_ != requestTerm || response.term != requestTerm) { return false; }
            if (auto* state = peerState(peerId); state != nullptr) {
                state->matchIndex = std::max(state->matchIndex, response.lastIncludedIndex);
                state->nextIndex = state->matchIndex + 1;
            }
            continue;
        }

        AppendEntriesResponse response;
        if (migration && runtimeOptions_.reconfiguration.maxTransferBytesPerSecond != 0 && !appendRequest.entries.empty()) {
            transferred += encodeAppendEntries(appendRequest).size();
            const auto seconds = std::chrono::duration<double>{
                static_cast<double>(transferred) / static_cast<double>(runtimeOptions_.reconfiguration.maxTransferBytesPerSecond)
            };
            const auto scheduled = transferStarted + std::chrono::duration_cast<Clock::duration>(seconds);
            std::unique_lock lock{mutex_};
            while (Clock::now() < scheduled) {
                if (!running_ || Clock::now() >= transferDeadline || (runtimeOptions_.reconfiguration.cancelled && runtimeOptions_.reconfiguration.
                    cancelled())) { return false; }
                detail::ReconfigurationDeadline::check();
                cv_.wait_until(lock, std::min(transferDeadline, Clock::now() + std::chrono::milliseconds{25}));
            }
        }
        const auto appendStarted = Clock::now();
        if (!appendEntries(peer, appendRequest, response)) {
            std::lock_guard lock{mutex_};
            if (currentTerm_ == requestTerm) { if (auto* state = peerState(peerId)) { state->successfulAppend.reset(); } }
            return false;
        }
        if (response.term > appendRequest.term) {
            becomeFollower(response.term);
            return false;
        }
        std::lock_guard lock{mutex_};
        if (!running_ || role_.load() != RaftRole::LEADER || currentTerm_ != requestTerm || response.term != requestTerm) { return false; }
        auto* state = peerState(peerId);
        if (state == nullptr) { return false; }
        if (response.success) {
            state->observedTerm = requestTerm;
            state->successfulAppend = appendStarted;
            auto degraded = ClusterHealthState::DEGRADED;
            health_.compare_exchange_strong(degraded, ClusterHealthState::HEALTHY, std::memory_order_relaxed);
            state->matchIndex = std::max(state->matchIndex, response.matchIndex);
            state->nextIndex = state->matchIndex + 1;
            forceHeartbeat = false;
            auto& [ackTerm, ackRound] = readAcks_[peerId];
            if (ackTerm != requestTerm) { ackTerm = requestTerm; ackRound = readRound; }
            else { ackRound = std::max(ackRound, readRound); }
            peerReplicationCv_.notify_all();
        }
        else {
            state->successfulAppend.reset();
            const auto before = state->nextIndex;
            updateNextIndexAfterConflictLocked(*state, response);
            // A peer which repeatedly refuses the same prefix must
            // release its worker so heartbeats and fencing can proceed.
            if (state->nextIndex == before) { return false; }
        }
    }
}

void sendHeartbeats() {
    std::vector<NodeInfo> targets;
    uint64_t targetCommit = 0;
    {
        std::lock_guard lock{mutex_};
        if (!running_ || role_.load() != RaftRole::LEADER) { return; }
        if (peerReplication_.empty()) { resetLeaderReplicationState(); }
        ensurePeerReplicationTargetsLocked();
        targetCommit = lastLogIndex();
        targets = peers_;
    }
    for (const auto& peer : targets) { scheduleReplication(peer, targetCommit, true); }
}

AppendEntriesResponse handleAppendEntries(const AppendEntries& request) {
    AppendEntriesResponse response;
    std::optional<NodeRole> roleChange;
    std::optional<AppendEntriesResponse> earlyResponse;
    {
        std::lock_guard lock{mutex_};
        if (!running_ || applyFailed_.load() || !isReplicationMemberLocked(selfNodeId_) || !isVotingMemberLocked(request.leaderId)) {
            return AppendEntriesResponse{.term = currentTerm_, .success = false, .matchIndex = lastLogIndex()};
        }
        if (request.term < currentTerm_) { return AppendEntriesResponse{.term = currentTerm_, .success = false, .matchIndex = lastLogIndex()}; }
        if (request.term > currentTerm_) {
            const auto hardBackup = captureDurableHardStateLocked();
            currentTerm_ = request.term;
            votedFor_ = 0;
            try { persistState(); }
            catch (...) {
                restoreDurableHardStateLocked(hardBackup);
                stopRuntimeLocked();
                throw;
            }
        }
        roleChange = setRole(RaftRole::FOLLOWER);
        recordLeaderContactLocked();

        if (request.prevLogIndex < lastIncludedIndex_) { earlyResponse = conflictResponseLocked(request.prevLogIndex); }
        else if (request.prevLogIndex > lastLogIndex()) { earlyResponse = conflictResponseLocked(request.prevLogIndex); }
        else if (termAt(request.prevLogIndex) != request.prevLogTerm) { earlyResponse = conflictResponseLocked(request.prevLogIndex); }
        if (earlyResponse) { response = *earlyResponse; }
        else {
            const auto backup = captureDurableLogStateLocked(request.prevLogIndex + 1);
            bool mutatedLogState = false;
            uint64_t expectedIndex = request.prevLogIndex + 1;
            uint64_t acceptedMatchIndex = request.prevLogIndex;
            try {
                for (const auto& entry : request.entries) {
                    if (entry.index != expectedIndex++) {
                        earlyResponse = AppendEntriesResponse{.term = currentTerm_, .success = false, .matchIndex = acceptedMatchIndex};
                        response = *earlyResponse;
                        break;
                    }
                    if (entry.index <= lastIncludedIndex_) { continue; }
                    if (!isValidStateMachineEntry(entry)) {
                        earlyResponse = AppendEntriesResponse{.term = currentTerm_, .success = false, .matchIndex = acceptedMatchIndex};
                        response = *earlyResponse;
                        break;
                    }
                    if (!entryPreservesStateMachineSequenceLocked(entry)) {
                        earlyResponse = AppendEntriesResponse{.term = currentTerm_, .success = false, .matchIndex = acceptedMatchIndex};
                        response = *earlyResponse;
                        break;
                    }
                    const auto existing = entryAt(entry.index);
                    if (!existing || !sameEntry(*existing, entry)) {
                        if (entry.index <= commitIndex_) {
                            earlyResponse = AppendEntriesResponse{.term = currentTerm_, .success = false, .matchIndex = acceptedMatchIndex};
                            response = *earlyResponse;
                            break;
                        }
                        invalidateLogSuffixLocked(entry.index);
                        log_.resize(static_cast<size_t>(entry.index - lastIncludedIndex_ - 1));
                        log_.push_back(entry);
                        mutatedLogState = true;
                    }
                    acceptedMatchIndex = entry.index;
                }

                if (earlyResponse && mutatedLogState) { restoreDurableLogStateLocked(backup); }
                if (!earlyResponse) {
                    if (request.leaderCommit > commitIndex_) {
                        commitIndex_ = std::min(request.leaderCommit, acceptedMatchIndex);
                        mutatedLogState = true;
                    }
                    if (mutatedLogState) { persistLog(); }
                    observeLeaderLocked(request.term, request.leaderId);
                    response = AppendEntriesResponse{.term = currentTerm_, .success = true, .matchIndex = acceptedMatchIndex};
                    auto degraded = ClusterHealthState::DEGRADED;
                    health_.compare_exchange_strong(degraded, ClusterHealthState::HEALTHY, std::memory_order_relaxed);
                }
            }
            catch (...) {
                if (mutatedLogState) { restoreDurableLogStateLocked(backup); }
                throw;
            }
        }
    }
    notifyRoleChange(roleChange);
    if (earlyResponse) { return response; }
    applyCommitted();
    return response;
}


/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */


// Included inside RaftConsensusRuntime::Impl.
uint32_t baselineElectionTimeoutMs() const noexcept {
    const auto configuredAckTimeoutMs = config_.consistency().ackTimeoutMs;
    return static_cast<uint32_t>(std::clamp<uint64_t>(uint64_t{configuredAckTimeoutMs} * 2u, 3000u, 6000u));
}

Clock::time_point nextElectionDeadline() {
    const auto baseTimeoutMs = baselineElectionTimeoutMs();
    // Test hook: keep production Raft randomized, but let smoke tests make leadership reproducible.
    if (const char* deterministic = std::getenv("AKKARADB_TEST_DETERMINISTIC_RAFT_ELECTION"); deterministic != nullptr && std::string_view{
        deterministic
    } == "1") {
        uint32_t rank = 0;
        for (const auto& node : config_.dataNodes()) { if (node.nodeId < selfNodeId_) { ++rank; } }
        return Clock::now() + std::chrono::milliseconds{baseTimeoutMs + rank * 250u};
    }
    static thread_local std::mt19937_64 rng{std::random_device{}()};
    std::uniform_int_distribution<int> dist(static_cast<int>(baseTimeoutMs), static_cast<int>(baseTimeoutMs * 2u));
    return Clock::now() + std::chrono::milliseconds(dist(rng));
}

std::optional<NodeRole> setRole(RaftRole role) {
    const auto old = role_.exchange(role);
    if (old == role) { return std::nullopt; }
    peerReplicationCv_.notify_all();
    return role == RaftRole::LEADER ? NodeRole::PRIMARY : NodeRole::REPLICA;
}

void notifyRoleChange(std::optional<NodeRole> role) { if (role && callbacks_.roleChange) { callbacks_.roleChange(*role); } }

void becomeFollower(uint64_t term) {
    std::optional<NodeRole> roleChange;
    {
        std::lock_guard lock{mutex_};
        // RPCs run concurrently; an old reply must not demote a newer leader.
        if (!running_ || term <= currentTerm_) { return; }
        const auto hardBackup = captureDurableHardStateLocked();
        currentTerm_ = term;
        votedFor_ = 0;
        clearObservedLeaderLocked();
        try { persistState(); }
        catch (...) {
            restoreDurableHardStateLocked(hardBackup);
            stopRuntimeLocked();
            throw;
        }
        electionDeadline_ = nextElectionDeadline();
        roleChange = setRole(RaftRole::FOLLOWER);
    }
    notifyRoleChange(roleChange);
}

void becomeLeader(uint64_t term) {
    std::optional<NodeRole> roleChange;
    {
        std::lock_guard lock{mutex_};
        const auto role = role_.load();
        const auto directSingleNodeElection = role == RaftRole::FOLLOWER && peers_.empty() && term >= currentTerm_;
        if (!running_ || term < currentTerm_ || (!directSingleNodeElection && (currentTerm_ != term || role != RaftRole::CANDIDATE)) || !self_->
            coordinatorEligible() || !isVotingMemberLocked(selfNodeId_)) { return; }
        const auto hardBackup = captureDurableHardStateLocked();
        const auto logBackup = captureDurableLogStateLocked();
        currentTerm_ = std::max(currentTerm_, term);
        votedFor_ = selfNodeId_;
        observeLeaderLocked(currentTerm_, selfNodeId_);
        bool hardStateDurable = false;
        try {
            persistState();
            hardStateDurable = true;
            resetLeaderReplicationState();
            log_.push_back(makeNoopEntryLocked());
            persistLog();
        }
        catch (...) {
            if (!hardStateDurable) { restoreDurableHardStateLocked(hardBackup); }
            restoreDurableLogStateLocked(logBackup);
            stopRuntimeLocked();
            throw;
        }
        electionDeadline_ = Clock::now() + std::chrono::hours(24);
        roleChange = setRole(RaftRole::LEADER);
    }
    notifyRoleChange(roleChange);
    (void)commitOutstandingEntry(std::chrono::milliseconds{0});
}

void resetLeaderReplicationState() {
    refreshPeersFromMembershipLocked();
    peerReplication_.clear();
    peerReplication_.reserve(peers_.size());
    const auto next = lastLogIndex() + 1;
    for (const auto& peer : peers_) {
        peerReplication_.push_back(PeerReplicationState{.node = peer, .nextIndex = next, .matchIndex = 0, .inFlight = false});
    }
}

PeerReplicationState* peerState(uint64_t nodeId) {
    for (auto& state : peerReplication_) { if (state.node.nodeId == nodeId) { return &state; } }
    return nullptr;
}

DurableLogState captureDurableLogStateLocked(uint64_t fromLogIndex = 0) const {
    const auto prefix = fromLogIndex <= lastIncludedIndex_
                              ? 0
                              : static_cast<size_t>(std::min<uint64_t>(fromLogIndex - lastIncludedIndex_ - 1, log_.size()));
    return DurableLogState{
        .requestState = requestState_,
        .journalState = journalState_,
        .requestJournalGeneration = requestJournalGeneration_,
        .requestJournalBytes = requestJournalBytes_,
        .requestJournalRecords = requestJournalRecords_,
        .commitIndex = commitIndex_,
        .lastApplied = lastApplied_,
        .lastIncludedIndex = lastIncludedIndex_,
        .lastIncludedTerm = lastIncludedTerm_,
        .lastIncludedStateMachineSeq = lastIncludedStateMachineSeq_,
        .configurationGeneration = lastConfigurationGeneration_,
        .logPrefixSize = prefix,
        .learnerBootstrapPending = learnerBootstrapPending_,
        .log = std::vector(log_.begin() + prefix, log_.end()),
        .committedMembers = committedMembers_,
        .jointOldMembers = jointOldMembers_,
        .jointNewMembers = jointNewMembers_,
        .peers = peers_,
        .peerReplication = peerReplication_,
        .durableSnapshotInstall = durableSnapshotInstall_
    };
}

void restoreDurableLogStateLocked(DurableLogState state) {
    requestState_ = state.requestState;
    journalState_ = state.journalState;
    requestJournalGeneration_ = state.requestJournalGeneration;
    requestJournalBytes_ = state.requestJournalBytes;
    requestJournalRecords_ = state.requestJournalRecords;
    commitIndex_ = state.commitIndex;
    lastApplied_ = state.lastApplied;
    lastIncludedIndex_ = state.lastIncludedIndex;
    lastIncludedTerm_ = state.lastIncludedTerm;
    lastIncludedStateMachineSeq_ = state.lastIncludedStateMachineSeq;
    lastConfigurationGeneration_ = state.configurationGeneration;
    learnerBootstrapPending_ = state.learnerBootstrapPending;
    log_.resize(state.logPrefixSize);
    log_.insert(log_.end(), std::make_move_iterator(state.log.begin()), std::make_move_iterator(state.log.end()));
    invalidateLogSuffixLocked(lastIncludedIndex_ + state.logPrefixSize + 1);
    committedMembers_ = std::move(state.committedMembers);
    jointOldMembers_ = std::move(state.jointOldMembers);
    jointNewMembers_ = std::move(state.jointNewMembers);
    peers_ = std::move(state.peers);
    peerReplication_ = std::move(state.peerReplication);
    durableSnapshotInstall_ = std::move(state.durableSnapshotInstall);
    peerReplicationCv_.notify_all();
}

DurableHardState captureDurableHardStateLocked() const noexcept { return DurableHardState{.currentTerm = currentTerm_, .votedFor = votedFor_}; }

void restoreDurableHardStateLocked(DurableHardState state) noexcept {
    currentTerm_ = state.currentTerm;
    votedFor_ = state.votedFor;
}

void stopRuntimeLocked() noexcept {
    running_ = false;
    forceElection_ = false;
    role_.store(RaftRole::FOLLOWER);
    cv_.notify_all();
    peerReplicationCv_.notify_all();
}

void observeLeaderLocked(uint64_t term, uint64_t leaderId) noexcept {
    if (term >= observedLeaderTerm_) {
        observedLeaderTerm_ = term;
        observedLeaderId_ = leaderId;
    }
}

void clearObservedLeaderLocked() noexcept {
    observedLeaderTerm_ = 0;
    observedLeaderId_ = 0;
}

bool observedLeaderLocked(uint64_t term, uint64_t leaderId) const noexcept { return observedLeaderTerm_ == term && observedLeaderId_ == leaderId; }

void recordLeaderContactLocked() {
    lastLeaderContact_ = Clock::now();
    ++leaderContactGeneration_;
    electionDeadline_ = nextElectionDeadline();
}

void timerLoop() {
    try {
        for (;;) {
            retirePeerWorkers();
            std::unique_lock lock{mutex_};
            if (!running_) { return; }
            if (role_.load() == RaftRole::LEADER) {
                lock.unlock();
                sendHeartbeats();
                lock.lock();
                cv_.wait_for(lock, std::chrono::milliseconds{runtimeOptions_.raftHeartbeatIntervalMs}, [&] { return !running_; });
                continue;
            }
            if (!forceElection_ && config_.failover() == FailoverPolicy::NONE) {
                cv_.wait_for(lock, std::chrono::milliseconds{100});
                continue;
            }
            if (!forceElection_ && Clock::now() < electionDeadline_) {
                cv_.wait_until(lock, std::min(electionDeadline_, Clock::now() + std::chrono::milliseconds{100}));
                continue;
            }
            lock.unlock();
            startElection();
        }
    }
    catch (...) {
        std::lock_guard lock{mutex_};
        stopRuntimeLocked();
    }
}

void startElection() {
    uint64_t term = 0;
    uint64_t baseTerm = 0;
    uint64_t contactGeneration = 0;
    uint64_t lastIndex = 0;
    uint64_t lastTerm = 0;
    bool forced = false;
    std::vector<NodeInfo> electionPeers;
    std::vector<NodeInfo> electionMembers;
    std::optional<std::vector<NodeInfo>> electionOldMembers, electionNewMembers;
    std::optional<std::pair<std::vector<NodeInfo>, std::vector<NodeInfo>>> pendingJoint;
    {
        std::lock_guard lock{mutex_};
        if (!running_ || role_.load() == RaftRole::LEADER) { return; }
        if (!self_->coordinatorEligible() || !isVotingMemberLocked(selfNodeId_)) {
            forceElection_ = false;
            electionDeadline_ = nextElectionDeadline();
            return;
        }
        if (!forceElection_ && (config_.failover() == FailoverPolicy::NONE || Clock::now() < electionDeadline_)) { return; }
        if (currentTerm_ == UINT64_MAX) { throw std::runtime_error("RaftConsensusRuntime: election term exhausted"); }
        forced = forceElection_;
        baseTerm = currentTerm_;
        term = baseTerm + 1;
        contactGeneration = leaderContactGeneration_;
        lastIndex = lastLogIndex();
        lastTerm = lastLogTerm();
        electionDeadline_ = nextElectionDeadline();
        refreshPeersFromMembershipLocked();
        electionPeers = peers_;
        std::erase_if(electionPeers, [this](const NodeInfo& node) { return !isVotingMemberLocked(node.nodeId); });
        electionMembers = committedMembers_;
        electionOldMembers = jointOldMembers_;
        electionNewMembers = jointNewMembers_;
        pendingJoint = pendingJointMembershipLocked();
    }

    std::vector<uint64_t> granted{selfNodeId_};
    // TimeoutNow is an authenticated, explicit transfer from the observed
    // leader. Ordinary campaigns must first obtain a non-binding quorum.
    if (!forced) { granted = collectElectionVotes(electionPeers, RequestVote{term, selfNodeId_, lastIndex, lastTerm}, true); }

    {
        std::lock_guard lock{mutex_};
        const auto currentPendingJoint = pendingJointMembershipLocked();
        const auto samePendingJoint = pendingJoint.has_value() == currentPendingJoint.has_value() && (!pendingJoint || (sameNodeSet(
            pendingJoint->first,
            currentPendingJoint->first
        ) && sameNodeSet(pendingJoint->second, currentPendingJoint->second)));
        if (!running_ || currentTerm_ != baseTerm || role_.load() == RaftRole::LEADER || !self_->coordinatorEligible() || !
            isVotingMemberLocked(selfNodeId_) || !sameNodeSet(electionMembers, committedMembers_) || !
            sameOptionalNodeSet(electionOldMembers, jointOldMembers_) || !sameOptionalNodeSet(electionNewMembers, jointNewMembers_) || !
            samePendingJoint || (!forced && (leaderContactGeneration_ != contactGeneration || !hasElectionQuorumLocked(granted)))) { return; }
        if (!forced && forceElection_) { return; } // Let the next loop honor a concurrent TimeoutNow.
        forceElection_ = false;
        const auto hardBackup = captureDurableHardStateLocked();
        role_.store(RaftRole::CANDIDATE);
        peerReplicationCv_.notify_all();
        currentTerm_ = term;
        votedFor_ = selfNodeId_;
        clearObservedLeaderLocked();
        lastIndex = lastLogIndex();
        lastTerm = lastLogTerm();
        electionDeadline_ = nextElectionDeadline();
        try { persistState(); }
        catch (...) {
            restoreDurableHardStateLocked(hardBackup);
            role_.store(RaftRole::FOLLOWER);
            stopRuntimeLocked();
            throw;
        }
    }

    granted = collectElectionVotes(electionPeers, RequestVote{term, selfNodeId_, lastIndex, lastTerm}, false);

    bool won = false;
    {
        std::lock_guard lock{mutex_};
        won = running_ && currentTerm_ == term && role_.load() == RaftRole::CANDIDATE && hasElectionQuorumLocked(granted);
    }
    if (won) { becomeLeader(term); }
}

std::vector<uint64_t> collectElectionVotes(const std::vector<NodeInfo>& peers, RequestVote request, bool probe) {
    struct Votes {
        std::mutex mutex;
        std::vector<uint64_t> granted;
    };
    auto votes = std::make_shared<Votes>();
    votes->granted.push_back(selfNodeId_);
    std::vector<std::future<void>> workers;
    for (const auto& peer : peers) {
        workers.push_back(
            schedulePeerTask(
                peer.nodeId,
                [this, peer, request, probe, votes] {
                    RequestVoteResponse response;
                    if (!(probe ? preVote(peer, request, response) : requestVote(peer, request, response))) { return; }
                    // Probes report actual hard state, not the prospective term.
                    const auto actualTerm = probe ? request.term - 1 : request.term;
                    if (response.term > actualTerm) {
                        becomeFollower(response.term);
                        return;
                    }
                    if (response.voteGranted && (probe || response.term == request.term)) {
                        std::lock_guard lock{votes->mutex};
                        votes->granted.push_back(peer.nodeId);
                    }
                }
            )
        );
    }
    for (auto& worker : workers) { worker.wait(); }
    for (auto& worker : workers) { worker.get(); }
    return std::move(votes->granted);
}

RequestVoteResponse handlePreVote(const RequestVote& request) {
    std::lock_guard lock{mutex_};
    const auto recentLeader = lastLeaderContact_ && Clock::now() - *lastLeaderContact_ < std::chrono::milliseconds{baselineElectionTimeoutMs()};
    const auto upToDate = request.lastLogTerm > lastLogTerm() || (request.lastLogTerm == lastLogTerm() && request.lastLogIndex >= lastLogIndex());
    const auto granted = running_ && !applyFailed_.load() && isVotingMemberLocked(selfNodeId_) && isVotingMemberLocked(request.candidateId) && request.term > currentTerm_
        && role_.load() != RaftRole::LEADER && !recentLeader && upToDate;
    return RequestVoteResponse{.term = currentTerm_, .voteGranted = granted};
}

RequestVoteResponse handleRequestVote(const RequestVote& request) {
    std::optional<NodeRole> roleChange;
    RequestVoteResponse response;
    {
        std::lock_guard lock{mutex_};
        if (!running_ || applyFailed_.load() || !isVotingMemberLocked(selfNodeId_) || !isVotingMemberLocked(request.candidateId)) {
            return RequestVoteResponse{.term = currentTerm_, .voteGranted = false};
        }
        if (request.term < currentTerm_) { return RequestVoteResponse{.term = currentTerm_, .voteGranted = false}; }
        if (request.term > currentTerm_) {
            const auto hardBackup = captureDurableHardStateLocked();
            currentTerm_ = request.term;
            votedFor_ = 0;
            clearObservedLeaderLocked();
            roleChange = setRole(RaftRole::FOLLOWER);
            try { persistState(); }
            catch (...) {
                restoreDurableHardStateLocked(hardBackup);
                stopRuntimeLocked();
                throw;
            }
        }
        const auto upToDate = request.lastLogTerm > lastLogTerm() || (request.lastLogTerm == lastLogTerm() && request.lastLogIndex >=
            lastLogIndex());
        const auto canVote = votedFor_ == 0 || votedFor_ == request.candidateId;
        const auto granted = canVote && upToDate;
        const auto voteBackup = captureDurableHardStateLocked();
        if (granted) {
            votedFor_ = request.candidateId;
            electionDeadline_ = nextElectionDeadline();
            try { persistState(); }
            catch (...) {
                restoreDurableHardStateLocked(voteBackup);
                stopRuntimeLocked();
                throw;
            }
        }
        response = RequestVoteResponse{.term = currentTerm_, .voteGranted = granted};
    }
    notifyRoleChange(roleChange);
    return response;
}

TimeoutNowResponse handleTimeoutNow(const TimeoutNow& request) {
    uint64_t responseTerm = 0;
    std::optional<NodeRole> roleChange;
    {
        std::lock_guard lock{mutex_};
        if (!running_ || applyFailed_.load() || request.targetId != selfNodeId_ || !isVotingMemberLocked(selfNodeId_) || !isVotingMemberLocked(request.leaderId)) {
            return TimeoutNowResponse{.term = currentTerm_, .accepted = false};
        }
        if (request.term < currentTerm_) { return TimeoutNowResponse{.term = currentTerm_, .accepted = false}; }
        if (request.term > currentTerm_) {
            const auto hardBackup = captureDurableHardStateLocked();
            currentTerm_ = request.term;
            votedFor_ = 0;
            clearObservedLeaderLocked();
            try { persistState(); }
            catch (...) {
                restoreDurableHardStateLocked(hardBackup);
                stopRuntimeLocked();
                throw;
            }
        }
        if (request.term != currentTerm_ || !observedLeaderLocked(request.term, request.leaderId)) {
            return TimeoutNowResponse{.term = currentTerm_, .accepted = false};
        }
        roleChange = setRole(RaftRole::FOLLOWER);
        forceElection_ = true;
        electionDeadline_ = Clock::now();
        responseTerm = currentTerm_;
    }
    notifyRoleChange(roleChange);
    cv_.notify_all();
    return TimeoutNowResponse{.term = responseTerm, .accepted = true};
}


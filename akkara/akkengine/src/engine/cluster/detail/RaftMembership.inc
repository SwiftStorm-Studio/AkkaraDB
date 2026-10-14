/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */


// Included inside RaftConsensusRuntime::Impl.
bool onlineMembershipChangeEnabled() const noexcept {
    const auto raft = config_.raft();
    return raft.membership.mode == RaftMembershipMode::JOINT_CONSENSUS && raft.membership.allowOnlineVoterChanges;
}

static bool containsNode(const std::vector<NodeInfo>& nodes, uint64_t nodeId) {
    return std::ranges::contains(nodes, nodeId, &NodeInfo::nodeId);
}

static std::vector<NodeInfo> sortedUniqueMembers(std::vector<NodeInfo> nodes) {
    std::ranges::sort(
        nodes,
        [](const NodeInfo& lhs, const NodeInfo& rhs) {
            if (lhs.nodeId != rhs.nodeId) { return lhs.nodeId < rhs.nodeId; }
            return lhs.raftLearner() < rhs.raftLearner();
        }
    );
    nodes.erase(std::ranges::unique(nodes, {}, &NodeInfo::nodeId).begin(), nodes.end());
    return nodes;
}

std::optional<std::pair<std::vector<NodeInfo>, std::vector<NodeInfo>>> pendingJointMembershipLocked() const {
    for (auto entry = log_.rbegin(); entry != log_.rend() && entry->index > commitIndex_; ++entry) {
        if (entry->kind != RaftEntryKind::CONFIG_JOINT) { continue; }
        std::pair<std::vector<NodeInfo>, std::vector<NodeInfo>> members;
        auto& [oldMembers, newMembers] = members;
        if (!decodeNodeSet(entry->key, oldMembers) || !decodeNodeSet(entry->value, newMembers)) {
            throw std::runtime_error("RaftConsensusRuntime: corrupt pending joint membership");
        }
        return members;
    }
    return std::nullopt;
}

std::vector<NodeInfo> replicationTargetsLocked() const {
    std::vector<NodeInfo> out = committedMembers_;
    if (jointNewMembers_) { out.insert(out.end(), jointNewMembers_->begin(), jointNewMembers_->end()); }
    if (const auto pending = pendingJointMembershipLocked()) {
        out.insert(out.end(), pending->first.begin(), pending->first.end());
        out.insert(out.end(), pending->second.begin(), pending->second.end());
    }
    out = sortedUniqueMembers(std::move(out));
    return out;
}

void refreshPeersFromMembershipLocked() {
    peers_.clear();
    for (const auto& node : replicationTargetsLocked()) { if (node.nodeId != selfNodeId_) { peers_.push_back(node); } }
}

void ensurePeerReplicationTargetsLocked(const std::vector<NodeInfo>* targetsOverride = nullptr) {
    if (targetsOverride != nullptr) {
        peers_.clear();
        for (const auto& node : *targetsOverride) { if (node.nodeId != selfNodeId_) { peers_.push_back(node); } }
    }
    else { refreshPeersFromMembershipLocked(); }
    for (const auto& peer : peers_) {
        if (this->peerState(peer.nodeId) == nullptr) {
            peerReplication_.push_back(PeerReplicationState{.node = peer, .nextIndex = 1, .matchIndex = 0, .inFlight = false});
        }
    }
    std::erase_if(peerReplication_, [&](const PeerReplicationState& state) { return !containsNode(peers_, state.node.nodeId); });
}

bool replicatedByMajorityLocked(const std::vector<NodeInfo>& voters, uint64_t index) {
    size_t replicated = 0;
    size_t voterCount = 0;
    for (const auto& voter : voters) {
        if (voter.raftLearner()) { continue; }
        ++voterCount;
        if (voter.nodeId == selfNodeId_) { if (index <= this->lastLogIndex()) { ++replicated; } }
        else if (const auto* state = this->peerState(voter.nodeId); state != nullptr && state->matchIndex >= index) { ++replicated; }
    }
    return voterCount != 0 && replicated >= (voterCount / 2) + 1;
}

static bool votedByMajority(const std::vector<NodeInfo>& voters, const std::vector<uint64_t>& grantedVotes) {
    size_t granted = 0;
    size_t voterCount = 0;
    for (const auto& voter : voters) {
        if (voter.raftLearner()) { continue; }
        ++voterCount;
        if (std::ranges::contains(grantedVotes, voter.nodeId)) { ++granted; }
    }
    return voterCount != 0 && granted >= (voterCount / 2) + 1;
}

bool hasElectionQuorumLocked(const std::vector<uint64_t>& grantedVotes) const {
    // A promoted learner may only have the joint entry, not its
    // commit notification, when the leader dies. The appended
    // joint configuration must govern that recovery election.
    if (const auto pending = pendingJointMembershipLocked()) {
        return votedByMajority(pending->first, grantedVotes) && votedByMajority(pending->second, grantedVotes);
    }
    if (jointOldMembers_ && jointNewMembers_) {
        return votedByMajority(*jointOldMembers_, grantedVotes) && votedByMajority(*jointNewMembers_, grantedVotes);
    }
    return votedByMajority(committedMembers_, grantedVotes);
}

bool hasCommitQuorumLocked(uint64_t index, const RaftLogEntry* entry = nullptr) {
    if (entry != nullptr && entry->kind == RaftEntryKind::CONFIG_JOINT) {
        std::vector<NodeInfo> oldMembers;
        std::vector<NodeInfo> newMembers;
        if (!decodeNodeSet(entry->key, oldMembers) || !decodeNodeSet(entry->value, newMembers)) { return false; }
        return replicatedByMajorityLocked(oldMembers, index) && replicatedByMajorityLocked(newMembers, index);
    }
    if (entry != nullptr && entry->kind == RaftEntryKind::CONFIG_FINAL && !entry->key.empty()) {
        std::vector<NodeInfo> oldMembers;
        std::vector<NodeInfo> newMembers;
        if (!decodeNodeSet(entry->key, oldMembers) || !decodeNodeSet(entry->value, newMembers)) { return false; }
        return replicatedByMajorityLocked(oldMembers, index) && replicatedByMajorityLocked(newMembers, index);
    }
    if (jointOldMembers_ && jointNewMembers_) {
        return replicatedByMajorityLocked(*jointOldMembers_, index) && replicatedByMajorityLocked(*jointNewMembers_, index);
    }
    for (const auto& pending : log_) {
        if (pending.index <= commitIndex_ || pending.index > index) { continue; }
        if (pending.kind == RaftEntryKind::CONFIG_JOINT) {
            std::vector<NodeInfo> oldMembers;
            std::vector<NodeInfo> newMembers;
            if (!decodeNodeSet(pending.key, oldMembers) || !decodeNodeSet(pending.value, newMembers)) { return false; }
            return replicatedByMajorityLocked(oldMembers, index) && replicatedByMajorityLocked(newMembers, index);
        }
        if (pending.kind == RaftEntryKind::CONFIG_FINAL && !pending.key.empty()) {
            std::vector<NodeInfo> oldMembers;
            std::vector<NodeInfo> newMembers;
            if (!decodeNodeSet(pending.key, oldMembers) || !decodeNodeSet(pending.value, newMembers)) { return false; }
            return replicatedByMajorityLocked(oldMembers, index) && replicatedByMajorityLocked(newMembers, index);
        }
    }
    return replicatedByMajorityLocked(committedMembers_, index);
}

bool canCommitEntryLocked(const RaftLogEntry& entry) {
    return entry.proposalFinal && entry.term == currentTerm_ && hasCommitQuorumLocked(entry.index, &entry);
}

static bool sameNodeSet(const std::vector<NodeInfo>& lhs, const std::vector<NodeInfo>& rhs) { return encodeNodeSet(lhs) == encodeNodeSet(rhs); }

static bool sameOptionalNodeSet(const std::optional<std::vector<NodeInfo>>& lhs, const std::optional<std::vector<NodeInfo>>& rhs) {
    if (lhs.has_value() != rhs.has_value()) { return false; }
    return !lhs || sameNodeSet(*lhs, *rhs);
}

std::optional<std::vector<NodeInfo>> replicationTargetsForEntry(const RaftLogEntry& entry) const {
    if (entry.kind == RaftEntryKind::CONFIG_JOINT) {
        std::vector<NodeInfo> oldMembers;
        std::vector<NodeInfo> newMembers;
        if (!decodeNodeSet(entry.key, oldMembers) || !decodeNodeSet(entry.value, newMembers)) { return std::nullopt; }
        oldMembers.insert(oldMembers.end(), newMembers.begin(), newMembers.end());
        return sortedUniqueMembers(std::move(oldMembers));
    }
    if (entry.kind == RaftEntryKind::CONFIG_FINAL) {
        std::vector<NodeInfo> newMembers;
        if (!decodeNodeSet(entry.value, newMembers)) { return std::nullopt; }
        if (!entry.key.empty()) {
            std::vector<NodeInfo> oldMembers;
            if (!decodeNodeSet(entry.key, oldMembers)) { return std::nullopt; }
            oldMembers.insert(oldMembers.end(), newMembers.begin(), newMembers.end());
            return sortedUniqueMembers(std::move(oldMembers));
        }
        return sortedUniqueMembers(std::move(newMembers));
    }
    return std::nullopt;
}

bool isVotingMemberLocked(uint64_t nodeId) const {
    const auto voting = [nodeId](const std::vector<NodeInfo>& members) {
        return std::ranges::any_of(members, [nodeId](const NodeInfo& node) { return node.nodeId == nodeId && !node.raftLearner(); });
    };
    if (voting(committedMembers_)) { return true; }
    if (jointOldMembers_ && voting(*jointOldMembers_)) { return true; }
    if (jointNewMembers_ && voting(*jointNewMembers_)) { return true; }
    if (const auto pending = pendingJointMembershipLocked()) { return voting(pending->first) || voting(pending->second); }
    return false;
}

bool isReplicationMemberLocked(uint64_t nodeId) const {
    // A configured joining learner can receive the authoritative
    // pre-admission snapshot, whose membership does not yet include
    // it. This grants no vote and ends at committed admission.
    if (nodeId == selfNodeId_ && learnerBootstrapPending_) { return true; }
    if (containsNode(committedMembers_, nodeId) || (jointOldMembers_ && containsNode(*jointOldMembers_, nodeId)) || (jointNewMembers_ &&
        containsNode(*jointNewMembers_, nodeId))) { return true; }
    if (const auto pending = pendingJointMembershipLocked()) {
        return containsNode(pending->first, nodeId) || containsNode(pending->second, nodeId);
    }
    return false;
}

void proposeMembershipChange(const std::vector<NodeInfo>& oldMembers, const std::vector<NodeInfo>& newMembers, uint64_t generation = 0) {
    auto voters = newMembers;
    std::erase_if(voters, [](const auto& n) { return n.raftLearner(); });
    detail::validateFaultDomains(voters, config_.faultDomains(), true);
    RaftLogEntry joint = makeConfigEntry(RaftEntryKind::CONFIG_JOINT, oldMembers, newMembers, generation);
    appendReplicateAndCommitConfig(joint);
    crashAtLogTestPoint("raft.learner.after_joint_commit");
    RaftLogEntry final = makeConfigEntry(RaftEntryKind::CONFIG_FINAL, oldMembers, newMembers, generation);
    appendReplicateAndCommitConfig(final);
}

RaftLogEntry makeConfigEntry(
    RaftEntryKind kind,
    const std::vector<NodeInfo>& oldMembers,
    const std::vector<NodeInfo>& newMembers,
    uint64_t generation = 0
) {
    std::lock_guard lock{mutex_};
    if (commitIndex_ != lastLogIndex()) {
        throw std::runtime_error("RaftConsensusRuntime: cannot change membership while a prior entry is uncommitted");
    }
    RaftLogEntry entry;
    entry.term = currentTerm_;
    entry.index = lastLogIndex() + 1;
    entry.clientSeq = 0;
    entry.kind = kind;
    entry.sourceNodeId = generation;
    entry.key = encodeNodeSet(oldMembers);
    entry.value = encodeNodeSet(newMembers);
    if (!isValidRaftLogEntryShape(entry)) { throw std::runtime_error("RaftConsensusRuntime: invalid membership log entry"); }
    return entry;
}

void appendReplicateAndCommitConfig(const RaftLogEntry& entry, bool appendEntry = true) {
    detail::ReconfigurationDeadline::check();
    uint64_t term = 0;
    {
        std::lock_guard lock{mutex_};
        if (role_.load() != RaftRole::LEADER) { throw std::runtime_error("RaftConsensusRuntime: local node is not Raft leader"); }
        term = currentTerm_;
        if (!isValidRaftLogEntryShape(entry)) { throw std::runtime_error("RaftConsensusRuntime: invalid membership log entry"); }
        const auto backup = captureDurableLogStateLocked();
        try {
            if (appendEntry) { log_.push_back(entry); }
            if (entry.kind == RaftEntryKind::CONFIG_JOINT) {
                std::vector<NodeInfo> oldMembers;
                std::vector<NodeInfo> newMembers;
                if (!decodeNodeSet(entry.key, oldMembers) || !decodeNodeSet(entry.value, newMembers)) {
                    throw std::runtime_error("RaftConsensusRuntime: corrupt joint membership entry");
                }
                auto targets = oldMembers;
                targets.insert(targets.end(), newMembers.begin(), newMembers.end());
                targets = sortedUniqueMembers(std::move(targets));
                for (const auto& node : targets) {
                    if (node.nodeId != selfNodeId_ && peerState(node.nodeId) == nullptr) {
                        peerReplication_.push_back(PeerReplicationState{.node = node, .nextIndex = 1, .matchIndex = 0, .inFlight = false});
                    }
                }
                peers_.clear();
                for (const auto& node : targets) { if (node.nodeId != selfNodeId_) { peers_.push_back(node); } }
            }
            persistLog();
        }
        catch (...) {
            restoreDurableLogStateLocked(backup);
            throw;
        }
    }
    if (!replicateEntryToMajority(entry, std::chrono::milliseconds{20000})) {
        throw std::runtime_error("RaftConsensusRuntime: failed to replicate membership change to Raft quorum");
    }
    if (entry.kind == RaftEntryKind::CONFIG_JOINT) { crashAtLogTestPoint("raft.learner.after_joint_replication"); }
    {
        std::lock_guard lock{mutex_};
        if (role_.load() != RaftRole::LEADER || currentTerm_ != term) {
            throw std::runtime_error("RaftConsensusRuntime: leadership changed before membership commit");
        }
        const auto backup = captureDurableLogStateLocked();
        try {
            commitIndex_ = std::max(commitIndex_, entry.index);
            persistLog();
        }
        catch (...) {
            restoreDurableLogStateLocked(backup);
            throw;
        }
    }
    applyCommitted();
    sendHeartbeats();
}

void applyConfigEntryLocked(const RaftLogEntry& entry) {
    lastConfigurationGeneration_ = std::max(lastConfigurationGeneration_, entry.sourceNodeId);
    if (entry.kind == RaftEntryKind::CONFIG_JOINT) {
        std::vector<NodeInfo> oldMembers;
        std::vector<NodeInfo> newMembers;
        if (!decodeNodeSet(entry.key, oldMembers) || !decodeNodeSet(entry.value, newMembers)) {
            throw std::runtime_error("RaftConsensusRuntime: corrupt joint membership entry");
        }
        jointOldMembers_ = sortedUniqueMembers(std::move(oldMembers));
        jointNewMembers_ = sortedUniqueMembers(std::move(newMembers));
    }
    else if (entry.kind == RaftEntryKind::CONFIG_FINAL) {
        std::vector<NodeInfo> newMembers;
        if (!decodeNodeSet(entry.value, newMembers)) { throw std::runtime_error("RaftConsensusRuntime: corrupt final membership entry"); }
        committedMembers_ = sortedUniqueMembers(std::move(newMembers));
        jointOldMembers_.reset();
        jointNewMembers_.reset();
    }
    if (containsNode(committedMembers_, selfNodeId_) || (jointNewMembers_ && containsNode(*jointNewMembers_, selfNodeId_))) {
        learnerBootstrapPending_ = false;
    }
    refreshPeersFromMembershipLocked();
    ensurePeerReplicationTargetsLocked();
}

bool applySnapshotMembershipLocked(const InstallSnapshot& request) {
    if (!hasVoter(request.committedMembers) || request.jointOldMembers.has_value() != request.jointNewMembers.has_value()) { return false; }
    committedMembers_ = sortedUniqueMembers(request.committedMembers);
    if (request.jointOldMembers && request.jointNewMembers) {
        jointOldMembers_ = sortedUniqueMembers(*request.jointOldMembers);
        jointNewMembers_ = sortedUniqueMembers(*request.jointNewMembers);
        if (jointOldMembers_->empty() || jointNewMembers_->empty()) { return false; }
    }
    else {
        jointOldMembers_.reset();
        jointNewMembers_.reset();
    }
    if (containsNode(committedMembers_, selfNodeId_) || (jointNewMembers_ && containsNode(*jointNewMembers_, selfNodeId_))) {
        learnerBootstrapPending_ = false;
    }
    refreshPeersFromMembershipLocked();
    ensurePeerReplicationTargetsLocked();
    return true;
}

bool applySnapshotMembershipLocked(const SnapshotInstallTransaction& transaction) {
    if (!hasVoter(transaction.committedMembers) || transaction.jointOldMembers.has_value() != transaction.jointNewMembers.has_value()) {
        return false;
    }
    committedMembers_ = sortedUniqueMembers(transaction.committedMembers);
    if (transaction.jointOldMembers && transaction.jointNewMembers) {
        jointOldMembers_ = sortedUniqueMembers(*transaction.jointOldMembers);
        jointNewMembers_ = sortedUniqueMembers(*transaction.jointNewMembers);
        if (jointOldMembers_->empty() || jointNewMembers_->empty()) { return false; }
    }
    else {
        jointOldMembers_.reset();
        jointNewMembers_.reset();
    }
    if (containsNode(committedMembers_, selfNodeId_) || (jointNewMembers_ && containsNode(*jointNewMembers_, selfNodeId_))) {
        learnerBootstrapPending_ = false;
    }
    refreshPeersFromMembershipLocked();
    ensurePeerReplicationTargetsLocked();
    return true;
}

bool finalizeSnapshotInstallMetadataLocked(const SnapshotInstallTransaction& transaction) {
    if (!applySnapshotMembershipLocked(transaction)) { return false; }
    lastConfigurationGeneration_ = transaction.configurationGeneration;
    if (transaction.requestState.sequence > transaction.lastIncludedIndex) { return false; }
    const auto floor = std::max(requestState_.expiryFloor, transaction.requestState.expiryFloor);
    if (transaction.requestState.generation) { ensureResultStore(); resultStore_->activate(transaction.requestState.generation); }
    requestState_ = transaction.requestState;
    expireRequestsLocked(floor);
    // A received snapshot can replace a different log prefix. Only
    // a matching boundary proves its retained suffix belongs to it.
    if (transaction.lastIncludedIndex > lastIncludedIndex_ &&
        (transaction.lastIncludedIndex > lastLogIndex() || termAt(transaction.lastIncludedIndex) != transaction.lastIncludedTerm)) {
        log_.clear();
    }
    compactLogThrough(transaction.lastIncludedIndex, transaction.lastIncludedTerm, transaction.snapshotSeq);
    // The installed state represents exactly this cut. Recovery may
    // have inferred later progress from the pre-install state; replay
    // the retained committed suffix against the replacement image.
    lastApplied_ = transaction.lastIncludedIndex;
    return true;
}

void replayCommittedMembership() {
    for (const auto& entry : log_) {
        if (entry.index > commitIndex_) { break; }
        if (entry.kind == RaftEntryKind::CONFIG_JOINT || entry.kind == RaftEntryKind::CONFIG_FINAL) { applyConfigEntryLocked(entry); }
    }
}

void validatePeerConfiguration(const std::vector<NodeInfo>& nodes, bool voterGroup = false) const {
    // A joint-configuration union is a transport set, not a voting
    // group. Each old/new group must independently retain quorum.
    auto domains = config_.faultDomains();
    domains.requireSiteQuorum &= voterGroup;
    // Recovered/online membership can differ from the original config.
    (void)ClusterConfig{
        nodes,
        ReplicationMode::STANDALONE,
        {},
        ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM},
        config_.raft(),
        {},
        0,
        {},
        {},
        FailoverPolicy::DEFAULT,
        domains
    };
    if (runtimeOptions_.transportMode == TransportMode::SECURE) {
        std::vector<uint64_t> required;
        for (const auto& peer : nodes) { if (peer.nodeId != selfNodeId_) { required.push_back(peer.nodeId); } }
        runtimeOptions_.secure.validatePins(required);
    }
}

void changeLearner(NodeInfo node, bool remove) {
    AdministrativeGuard admission{*this};
    if (!config_.raft().membership.allowLearners) { throw std::runtime_error("RaftConsensusRuntime: learners are disabled"); }
    (void)commitOutstandingEntry(std::chrono::milliseconds{20000});
    std::vector<NodeInfo> oldMembers;
    std::vector<NodeInfo> newMembers;
    {
        std::lock_guard lock{mutex_};
        if (role_.load() != RaftRole::LEADER) { throw std::runtime_error("RaftConsensusRuntime: local node is not Raft leader"); }
        if (jointOldMembers_ || jointNewMembers_ || commitIndex_ != lastLogIndex()) {
            throw std::runtime_error("RaftConsensusRuntime: membership change already in progress");
        }
        const auto existing = std::ranges::find(committedMembers_, node.nodeId, &NodeInfo::nodeId);
        oldMembers = committedMembers_;
        newMembers = oldMembers;
        if (remove) {
            if (existing == committedMembers_.end() || !existing->raftLearner()) {
                throw std::invalid_argument("RaftConsensusRuntime: node is not a learner");
            }
            std::erase_if(newMembers, [&](const NodeInfo& member) { return member.nodeId == node.nodeId; });
        }
        else {
            if (existing != committedMembers_.end()) { throw std::invalid_argument("RaftConsensusRuntime: node is already a member"); }
            if (!node.dataBearing()) { throw std::invalid_argument("RaftConsensusRuntime: learner must be data-bearing"); }
            node.capabilities |= RAFT_LEARNER;
            newMembers.push_back(node);
            newMembers = sortedUniqueMembers(std::move(newMembers));
        }
        validatePeerConfiguration(newMembers);
    }
    // Voters are identical on both sides, so one committed entry
    // suffices without a joint voting transition.
    auto entry = makeConfigEntry(RaftEntryKind::CONFIG_FINAL, oldMembers, newMembers);
    appendReplicateAndCommitConfig(entry);
}

void transferLeadershipAdmitted(uint64_t targetNodeId) {
    if (targetNodeId == selfNodeId_) { return; }
    NodeInfo target;
    uint64_t targetIndex = 0;
    uint64_t term = 0;
    {
        std::lock_guard lock{mutex_};
        if (role_.load() != RaftRole::LEADER) { throw std::runtime_error("RaftConsensusRuntime: local node is not Raft leader"); }
        if (!isVotingMemberLocked(targetNodeId)) { throw std::invalid_argument("RaftConsensusRuntime: transfer target is not a voting member"); }
        auto* state = this->peerState(targetNodeId);
        if (state == nullptr) { throw std::invalid_argument("RaftConsensusRuntime: transfer target is not a replication peer"); }
        target = state->node;
        targetIndex = this->lastLogIndex();
        term = currentTerm_;
    }

    if (!this->replicatePeerTo(targetNodeId, targetIndex, true)) {
        throw std::runtime_error("RaftConsensusRuntime: failed to catch up transfer target");
    }
    {
        std::lock_guard lock{mutex_};
        const auto* state = this->peerState(targetNodeId);
        if (role_.load() != RaftRole::LEADER || currentTerm_ != term) {
            throw std::runtime_error("RaftConsensusRuntime: leadership changed before transfer");
        }
        if (state == nullptr || state->matchIndex < targetIndex) {
            throw std::runtime_error("RaftConsensusRuntime: transfer target is not caught up");
        }
    }

    TimeoutNowResponse response;
    if (!this->timeoutNow(target, TimeoutNow{.term = term, .leaderId = selfNodeId_, .targetId = targetNodeId}, response)) {
        throw std::runtime_error("RaftConsensusRuntime: failed to send leader transfer request");
    }
    if (response.term > term) {
        this->becomeFollower(response.term);
        throw std::runtime_error("RaftConsensusRuntime: leadership changed during transfer");
    }
    if (!response.accepted) { throw std::runtime_error("RaftConsensusRuntime: leader transfer target rejected request"); }
    std::optional<NodeRole> roleChange;
    {
        std::lock_guard lock{mutex_};
        if (role_.load() == RaftRole::LEADER && currentTerm_ == term) {
            electionDeadline_ = this->nextElectionDeadline();
            roleChange = setRole(RaftRole::FOLLOWER);
        }
    }
    notifyRoleChange(roleChange);
}


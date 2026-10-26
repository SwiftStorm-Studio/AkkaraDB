/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */


// Included inside RaftConsensusRuntime::Impl.
int rpcTimeoutMs(int fallbackMs) const {
    const auto configured = config_.consistency().ackTimeoutMs;
    if (configured == 0) { return fallbackMs; }
    return static_cast<int>(std::clamp<uint32_t>(configured, 100, static_cast<uint32_t>(fallbackMs)));
}

uint64_t lastLogIndex() const noexcept { return log_.empty() ? lastIncludedIndex_ : log_.back().index; }

uint64_t lastLogTerm() const noexcept { return log_.empty() ? lastIncludedTerm_ : log_.back().term; }

std::optional<RaftLogEntry> entryAt(uint64_t index) const {
    if (index <= lastIncludedIndex_) { return std::nullopt; }
    const auto offset = index - lastIncludedIndex_ - 1;
    if (offset >= log_.size()) { return std::nullopt; }
    const auto& entry = log_[static_cast<size_t>(offset)];
    return entry.index == index ? std::optional<RaftLogEntry>{entry} : std::nullopt;
}

uint64_t termAt(uint64_t index) const {
    if (index == 0) { return 0; }
    if (index == lastIncludedIndex_) { return lastIncludedTerm_; }
    if (index < lastIncludedIndex_) { return 0; }
    const auto offset = index - lastIncludedIndex_ - 1;
    return offset < log_.size() && log_[static_cast<size_t>(offset)].index == index ? log_[static_cast<size_t>(offset)].term : 0;
}

std::vector<RaftLogEntry> entriesFrom(uint64_t nextIndex) const {
    std::vector<RaftLogEntry> entries;
    size_t payloadSize = RAFT_APPEND_ENTRIES_BASE_SIZE;
    const auto offset = nextIndex <= lastIncludedIndex_ ? 0 : nextIndex - lastIncludedIndex_ - 1;
    for (size_t i = static_cast<size_t>(std::min<uint64_t>(offset, log_.size())); i < log_.size(); ++i) {
        const auto& entry = log_[i];
        const auto entryPayload = encodeEntryPayload(entry);
        const size_t entrySize = RAFT_ENTRY_LENGTH_PREFIX_SIZE + entryPayload.size();
        if (payloadSize + entrySize > ReplFrameHeader::MAX_RAFT_PAYLOAD_SIZE) {
            if (entries.empty()) { return {}; }
            break;
        }
        payloadSize += entrySize;
        entries.push_back(entry);
    }
    return entries;
}

void invalidateLogSuffixLocked(uint64_t index) {
    changedLogFrom_ = std::min(changedLogFrom_, index);
    const auto offset = index <= lastIncludedIndex_ ? 0 : index - lastIncludedIndex_ - 1;
    validatedLog_.resize(static_cast<size_t>(std::min<uint64_t>(offset, validatedLog_.size())));
}

void validateLogPrefixLocked(size_t count) {
    if (validatedLog_.size() > log_.size()) { validatedLog_.resize(log_.size()); }
    for (size_t i = validatedLog_.size(); i < count; ++i) {
        const auto& entry = log_[i];
        auto state = validatedLog_.empty() ? SequenceState{lastIncludedStateMachineSeq_, false} : validatedLog_.back();
        if (entry.index != lastIncludedIndex_ + i + 1 || !isValidStateMachineEntry(entry) || (isStateMachineEntry(entry.kind) && entry.clientSeq <=
            lastIncludedStateMachineSeq_) || !advanceStateMachineSequenceTracker(entry, state.sequence, state.hasMutation)) {
            throw std::runtime_error("RaftConsensusRuntime: invalid log index or state-machine sequence");
        }
        validatedLog_.push_back(state);
    }
}

bool entryPreservesStateMachineSequenceLocked(const RaftLogEntry& incoming) {
    if (incoming.index <= lastIncludedIndex_ || (isStateMachineEntry(incoming.kind) && incoming.clientSeq <= lastIncludedStateMachineSeq_)) {
        return false;
    }
    const auto offset = incoming.index - lastIncludedIndex_ - 1;
    if (offset > log_.size()) { return false; }
    validateLogPrefixLocked(static_cast<size_t>(offset));
    auto state = offset == 0 ? SequenceState{lastIncludedStateMachineSeq_, false} : validatedLog_[static_cast<size_t>(offset - 1)];
    return advanceStateMachineSequenceTracker(incoming, state.sequence, state.hasMutation);
}

std::optional<uint64_t> compactableLogIndexForSnapshotSeq(uint64_t snapshotSeq) const {
    std::optional<uint64_t> target;
    for (const auto& entry : log_) {
        if (entry.index > lastApplied_) { break; }
        if ((entry.kind == RaftEntryKind::MUTATION || entry.kind == RaftEntryKind::BLOB) && entry.clientSeq <= snapshotSeq) {
            target = entry.index;
            continue;
        }
        if (entry.kind == RaftEntryKind::MUTATION || entry.kind == RaftEntryKind::BLOB) { return target; }
        if (entry.kind == RaftEntryKind::CONFIG_JOINT || entry.kind == RaftEntryKind::CONFIG_FINAL || entry.kind == RaftEntryKind::NOOP) {
            target = entry.index;
        }
    }
    return target;
}

uint64_t firstIndexOfTermLocked(uint64_t term, uint64_t atOrBeforeIndex) const {
    if (term == 0) { return 0; }
    if (lastIncludedIndex_ != 0 && lastIncludedIndex_ <= atOrBeforeIndex && lastIncludedTerm_ == term) { return lastIncludedIndex_; }
    for (const auto& entry : log_) {
        if (entry.index > atOrBeforeIndex) { break; }
        if (entry.term == term) { return entry.index; }
    }
    return 0;
}

uint64_t lastIndexOfTermLocked(uint64_t term) const {
    if (term == 0) { return 0; }
    uint64_t out = 0;
    if (lastIncludedIndex_ != 0 && lastIncludedTerm_ == term) { out = lastIncludedIndex_; }
    for (const auto& entry : log_) {
        if (entry.term == term) { out = entry.index; }
        else if (out != 0 && entry.term > term) { break; }
    }
    return out;
}

AppendEntriesResponse conflictResponseLocked(uint64_t prevLogIndex) const {
    if (prevLogIndex > lastLogIndex()) {
        return AppendEntriesResponse{
            .term = currentTerm_,
            .success = false,
            .matchIndex = lastLogIndex(),
            .conflictIndex = lastLogIndex() + 1,
            .conflictTerm = 0
        };
    }
    if (prevLogIndex <= lastIncludedIndex_) {
        return AppendEntriesResponse{
            .term = currentTerm_,
            .success = false,
            .matchIndex = lastIncludedIndex_,
            .conflictIndex = lastIncludedIndex_ + 1,
            .conflictTerm = lastIncludedTerm_
        };
    }
    const auto conflictTerm = termAt(prevLogIndex);
    const auto firstConflictIndex = firstIndexOfTermLocked(conflictTerm, prevLogIndex);
    return AppendEntriesResponse{
        .term = currentTerm_,
        .success = false,
        .matchIndex = firstConflictIndex == 0 ? 0 : firstConflictIndex - 1,
        .conflictIndex = firstConflictIndex == 0 ? prevLogIndex : firstConflictIndex,
        .conflictTerm = conflictTerm
    };
}

void updateNextIndexAfterConflictLocked(PeerReplicationState& state, const AppendEntriesResponse& response) const {
    // A follower may have compacted farther than this peer worker's
    // acknowledgement. Verify its boundary against our committed
    // prefix before advancing rather than backing up forever.
    if (response.conflictTerm != 0 && response.matchIndex >= state.nextIndex && response.matchIndex <= commitIndex_ && response.conflictIndex ==
        response.matchIndex + 1 && termAt(response.matchIndex) == response.conflictTerm) {
        state.matchIndex = std::max(state.matchIndex, response.matchIndex);
        state.nextIndex = response.matchIndex + 1;
        return;
    }
    uint64_t nextIndex = 1;
    if (response.conflictTerm != 0) {
        if (const auto leaderLastIndexForTerm = lastIndexOfTermLocked(response.conflictTerm); leaderLastIndexForTerm != 0) {
            nextIndex = leaderLastIndexForTerm + 1;
        }
        else if (response.conflictIndex != 0) { nextIndex = response.conflictIndex; }
        else { nextIndex = response.matchIndex + 1; }
    }
    else if (response.conflictIndex != 0) { nextIndex = response.conflictIndex; }
    else { nextIndex = response.matchIndex + 1; }
    if (nextIndex >= state.nextIndex && state.nextIndex > 1) { nextIndex = state.nextIndex - 1; }
    state.nextIndex = std::max<uint64_t>(1, nextIndex);
}

RaftLogEntry makeNoopEntryLocked() const {
    RaftLogEntry entry;
    entry.term = currentTerm_;
    entry.index = lastLogIndex() + 1;
    entry.clientSeq = 0;
    entry.kind = RaftEntryKind::NOOP;
    entry.op = ReplOpType::PUT;
    entry.sourceNodeId = selfNodeId_;
    return entry;
}

bool committedLogBytesAtLeastLocked(uint64_t threshold) const {
    uint64_t bytes = 0;
    for (const auto& entry : log_) {
        if (entry.index > commitIndex_) { break; }
        const uint64_t entryBytes = sizeof(RaftLogEntry) + static_cast<uint64_t>(entry.key.size()) + static_cast<uint64_t>(entry.value.size());
        if (entryBytes >= threshold - bytes) { return true; }
        bytes += entryBytes;
    }
    return false;
}

bool shouldCompactLogLocked(Clock::time_point now) const {
    if (commitIndex_ <= lastIncludedIndex_) { return false; }
    if (commitIndex_ - lastIncludedIndex_ >= runtimeOptions_.raftSnapshot.minLogEntries) { return true; }
    if (now >= nextCompactionDeadline_) { return true; }
    return committedLogBytesAtLeastLocked(runtimeOptions_.raftSnapshot.minLogBytes);
}

void maybeCompactLog(bool force = false) {
    if (!runtimeOptions_.raftSnapshot.enabled || !callbacks_.exportSnapshot) { return; }
    {
        std::lock_guard lock{mutex_};
        if (!running_ || durableSnapshotInstall_ || pendingSnapshotInstall_.active) { return; }
        const auto now = Clock::now();
        if (!force && now < nextCompactionCheck_) { return; }
        nextCompactionCheck_ = now + std::chrono::seconds{1};
        if (!force && !shouldCompactLogLocked(now)) { return; }
    }
    std::unique_lock applyLock{applyMutex_, std::try_to_lock};
    if (!applyLock.owns_lock()) { return; }
    {
        std::lock_guard lock{mutex_};
        if (!running_ || durableSnapshotInstall_ || pendingSnapshotInstall_.active) { return; }
        if (!force && !shouldCompactLogLocked(Clock::now())) { return; }
    }
    std::optional<ClusterSnapshot> snapshot;
    try { snapshot = callbacks_.exportSnapshot(); }
    catch (const TransactionSnapshotUnavailable&) { return; }
    if (!snapshot) { return; }
    std::lock_guard lock{mutex_};
    if (!running_ || durableSnapshotInstall_ || pendingSnapshotInstall_.active) { return; }
    const auto logIndex = compactableLogIndexForSnapshotSeq(snapshot->seq);
    if (!logIndex || *logIndex <= lastIncludedIndex_ || *logIndex > commitIndex_) { return; }
    const auto includedTerm = termAt(*logIndex);
    if (includedTerm == 0) { return; }
    const auto backup = captureDurableLogStateLocked();
    compactLogThrough(*logIndex, includedTerm, snapshot->seq);
    try { persistLog(); }
    catch (...) {
        restoreDurableLogStateLocked(backup);
        throw;
    }
    nextCompactionDeadline_ = Clock::now() + std::chrono::milliseconds{runtimeOptions_.raftSnapshot.maxIntervalMs};
}

void compactLogThrough(uint64_t index, uint64_t term, uint64_t stateMachineSeq) {
    if (index <= lastIncludedIndex_) { return; }
    log_.erase(std::ranges::remove_if(log_, [&](const RaftLogEntry& entry) { return entry.index <= index; }).begin(), log_.end());
    if (log_.capacity() > 2 * log_.size() + runtimeOptions_.raftSnapshot.minLogEntries) { log_.shrink_to_fit(); }
    validatedLog_.clear();
    if (validatedLog_.capacity() > runtimeOptions_.raftSnapshot.minLogEntries * 2) { validatedLog_.shrink_to_fit(); }
    lastIncludedIndex_ = index;
    lastIncludedTerm_ = term;
    lastIncludedStateMachineSeq_ = std::max(lastIncludedStateMachineSeq_, stateMachineSeq);
    if (commitIndex_ < lastIncludedIndex_) { commitIndex_ = lastIncludedIndex_; }
    if (lastApplied_ < lastIncludedIndex_) { lastApplied_ = lastIncludedIndex_; }
}


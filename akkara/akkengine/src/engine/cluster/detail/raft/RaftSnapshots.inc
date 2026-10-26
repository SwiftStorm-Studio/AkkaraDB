/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */


// Included inside RaftConsensusRuntime::Impl.
bool installSnapshotStream(const NodeInfo& peer, uint64_t term, InstallSnapshotResponse& response) {
    if (!callbacks_.exportSnapshot) { return false; }
    std::optional<ClusterSnapshot> snapshot;
    RequestState snapshotRequests;
    std::optional<detail::DiskResults::Snapshot> resultSnapshot;
    uint64_t logIndexValue = 0;
    uint64_t logTermValue = 0;
    uint64_t configurationGeneration = 0;
    std::vector<NodeInfo> committedMembers;
    std::optional<std::vector<NodeInfo>> jointOldMembers;
    std::optional<std::vector<NodeInfo>> jointNewMembers;
    {
        std::unique_lock applyLock{applyMutex_, std::try_to_lock};
        if (!applyLock.owns_lock() || applyFailed_.load()) { return false; }
        try { snapshot = callbacks_.exportSnapshot(); }
        catch (const TransactionSnapshotUnavailable&) { return false; }
        if (!snapshot || !snapshot->forEachEntry) { return false; }
        std::lock_guard lock{mutex_};
        auto logIndex = compactableLogIndexForSnapshotSeq(snapshot->seq);
        if (!logIndex && lastIncludedIndex_ > 0) { logIndex = lastIncludedIndex_; }
        if (!logIndex || *logIndex > commitIndex_) { return false; }
        logIndexValue = *logIndex;
        snapshotRequests = requestState_;
        if (resultStore_) {
            try { resultSnapshot = resultStore_->capture(); }
            catch (const TransactionSnapshotUnavailable&) { return false; }
        }
        snapshotRequests.sequence = resultSnapshot ? logIndexValue : 0;
        snapshotRequests.count = snapshotRequests.bytes = 0;
        logTermValue = termAt(logIndexValue);
        configurationGeneration = lastConfigurationGeneration_;
        committedMembers = committedMembers_;
        jointOldMembers = jointOldMembers_;
        jointNewMembers = jointNewMembers_;
    }
    if (logTermValue == 0) { return false; }

    const auto makeRequest = [&] {
        InstallSnapshot request;
        request.term = term;
        request.leaderId = selfNodeId_;
        request.lastIncludedIndex = logIndexValue;
        request.lastIncludedTerm = logTermValue;
        request.snapshotSeq = snapshot->seq;
        request.configurationGeneration = configurationGeneration;
        request.committedMembers = committedMembers;
        request.jointOldMembers = jointOldMembers;
        request.jointNewMembers = jointNewMembers;
        request.requestState.expiryFloor = snapshotRequests.expiryFloor;
        request.requestState.generation = snapshotRequests.generation;
        return request;
    };

    InstallSnapshot current = makeRequest();
    const auto sendRequest = [&](const InstallSnapshot& request) {
        detail::ReconfigurationDeadline::check();
        return installSnapshot(peer, request, response) && response.term <= term && response.success;
    };
    const size_t chunkSize = runtimeOptions_.raftBlobChunkSizeBytes;
    if (chunkSize == 0) { return false; }
    uint64_t entryIndex = 0;
    bool entryActive = false;
    bool emittedEntryChunk = false;
    uint64_t entryValueSize = 0;
    uint64_t entryValueOffset = 0;
    uint32_t entryValueCrc32c = 0;
    std::vector<uint8_t> entryKey;
    const auto appendChunk = [&](uint64_t valueOffset, std::span<const uint8_t> value) {
        size_t chunkOffset = 0;
        bool emittedInputChunk = false;
        do {
            const auto remaining = value.size() - chunkOffset;
            size_t currentChunkSize = std::min(chunkSize, remaining);
            auto makeChunk = [&](size_t valueBytes) {
                SnapshotKvChunk chunk;
                chunk.entryIndex = entryIndex;
                chunk.valueOffset = valueOffset + chunkOffset;
                chunk.valueSize = entryValueSize;
                chunk.valueCrc32c = entryValueCrc32c;
                if (chunk.valueOffset == 0) { chunk.key = entryKey; }
                chunk.value.assign(
                    value.begin() + static_cast<std::ptrdiff_t>(chunkOffset),
                    value.begin() + static_cast<std::ptrdiff_t>(chunkOffset + valueBytes)
                );
                return chunk;
            };
            SnapshotKvChunk chunk = makeChunk(currentChunkSize);
            InstallSnapshot candidate = current;
            candidate.chunks.push_back(chunk);
            if (encodeInstallSnapshot(candidate).empty() && !current.chunks.empty()) {
                if (!sendRequest(current)) { return false; }
                current = makeRequest();
                candidate = current;
                candidate.chunks.push_back(chunk);
            }
            if (encodeInstallSnapshot(candidate).empty()) {
                if (currentChunkSize == 0) { return false; }
                size_t low = 1;
                size_t high = currentChunkSize;
                size_t best = 0;
                while (low <= high) {
                    const auto mid = low + (high - low) / 2;
                    chunk = makeChunk(mid);
                    candidate = current;
                    candidate.chunks.push_back(chunk);
                    if (encodeInstallSnapshot(candidate).empty()) { high = mid - 1; }
                    else {
                        best = mid;
                        low = mid + 1;
                    }
                }
                if (best == 0) { return false; }
                currentChunkSize = best;
                chunk = makeChunk(currentChunkSize);
                candidate = current;
                candidate.chunks.push_back(std::move(chunk));
            }
            current = std::move(candidate);
            chunkOffset += currentChunkSize;
            emittedInputChunk = true;
            emittedEntryChunk = true;
        }
        while (chunkOffset < value.size() || !emittedInputChunk);
        return true;
    };
    const ClusterSnapshot::EntryVisitor snapshotVisitor{
        .beginEntry = [&](std::span<const uint8_t> key, uint64_t valueSize, uint32_t valueCrc32c) {
            if (entryActive || entryIndex == UINT64_MAX) { return false; }
            entryActive = true;
            emittedEntryChunk = false;
            entryValueSize = valueSize;
            entryValueOffset = 0;
            entryValueCrc32c = valueCrc32c;
            entryKey.assign(key.begin(), key.end());
            return true;
        },
        .appendValueChunk = [&](uint64_t offset, std::span<const uint8_t> chunk) {
            if (!entryActive || offset != entryValueOffset || chunk.size() > entryValueSize - entryValueOffset || (chunk.empty() && (entryValueSize
                != 0 || emittedEntryChunk))) { return false; }
            if (!appendChunk(offset, chunk)) { return false; }
            entryValueOffset += chunk.size();
            return true;
        },
        .finishEntry = [&] {
            if (!entryActive || !emittedEntryChunk || entryValueOffset != entryValueSize) { return false; }
            entryActive = false;
            entryKey.clear();
            ++entryIndex;
            return true;
        },
        .fileEntryThresholdBytes = UINT64_MAX,
        .fileEntry = {},
    };
    const auto streamed = snapshot->forEachEntry(snapshotVisitor);
    if (!streamed || entryActive) { return false; }

    size_t resultPageBytes = encodeInstallSnapshot(current).size();
    constexpr size_t resultPageLimit = 64 * 1024;
    if (resultSnapshot && !resultSnapshot->forEach([&](auto key, const detail::DiskResults::Record& stored) {
        if (stored.sequence > logIndexValue) { return true; }
        size_t offset = 0; ClusterRequestId id; RequestRecord record;
        if (!readRequestId(key, offset, id) || offset != key.size()) { return false; }
        offset = 0;
        if (!readRequestRecord(stored.value, offset, record) || offset != stored.value.size() || record.index != stored.sequence) { return false; }
        if (resultPageBytes + 72 > resultPageLimit && (!current.chunks.empty() || !current.results.empty())) {
            if (!sendRequest(current)) { return false; }
            current = makeRequest(); resultPageBytes = encodeInstallSnapshot(current).size();
        }
        current.results.emplace_back(id, record); resultPageBytes += 72; ++snapshotRequests.count;
        snapshotRequests.bytes += detail::DiskResults::recordCharge(24, 48); return true;
    })) { return false; }
    current.requestState = snapshotRequests;
    current.entryCount = entryIndex;
    current.done = true;
    if (encodeInstallSnapshot(current).empty()) {
        auto final = makeRequest();
        final.requestState = std::move(current.requestState);
        final.entryCount = entryIndex;
        final.done = true;
        current.done = false;
        current.entryCount = 0;
        current.requestState = {}; current.requestState.generation = snapshotRequests.generation;
        current.requestState.expiryFloor = snapshotRequests.expiryFloor;
        if ((!current.chunks.empty() || !current.results.empty()) && !sendRequest(current)) { return false; }
        if (encodeInstallSnapshot(final).empty()) { return false; }
        return sendRequest(final);
    }
    return sendRequest(current);
}

bool applySnapshotChunkRequestLocked(const InstallSnapshot& request) {
    if (request.chunks.empty() && request.results.empty() && !request.done) { return false; }
    if (!request.done && request.entryCount != 0) { return false; }
    if (request.committedMembers.empty() || request.jointOldMembers.has_value() != request.jointNewMembers.has_value()) { return false; }
    if (request.jointOldMembers && (request.jointOldMembers->empty() || request.jointNewMembers->empty())) { return false; }
    const auto sameSnapshot = pendingSnapshotInstall_.active && pendingSnapshotInstall_.snapshotSeq == request.snapshotSeq &&
        pendingSnapshotInstall_.configurationGeneration == request.configurationGeneration && pendingSnapshotInstall_.lastIncludedIndex == request.
        lastIncludedIndex && pendingSnapshotInstall_.lastIncludedTerm == request.lastIncludedTerm && sameNodeSet(
            pendingSnapshotInstall_.committedMembers,
            request.committedMembers
        ) && sameOptionalNodeSet(pendingSnapshotInstall_.jointOldMembers, request.jointOldMembers) && sameOptionalNodeSet(
            pendingSnapshotInstall_.jointNewMembers,
            request.jointNewMembers
        );
    if (pendingSnapshotInstall_.active && !sameSnapshot) { return false; }
    const auto restartsSnapshot = sameSnapshot && !request.chunks.empty() && request.chunks.front().entryIndex == 0 && request.chunks.front().
        valueOffset == 0 && (pendingSnapshotInstall_.nextEntryIndex != 0 || pendingSnapshotInstall_.hasCurrentEntry);
    if (!sameSnapshot || restartsSnapshot) {
        try {
            validatePeerConfiguration(request.committedMembers, true);
            if (request.jointOldMembers) { validatePeerConfiguration(*request.jointOldMembers, true); }
            if (request.jointNewMembers) { validatePeerConfiguration(*request.jointNewMembers, true); }
        }
        catch (...) { return false; }
        {
            std::lock_guard lock{mutex_};
            pendingSnapshotInstall_ = PendingSnapshotInstall{};
            pendingSnapshotInstall_.active = true;
        }
        pendingSnapshotInstall_.term = request.term;
        pendingSnapshotInstall_.snapshotSeq = request.snapshotSeq;
        pendingSnapshotInstall_.lastIncludedIndex = request.lastIncludedIndex;
        pendingSnapshotInstall_.lastIncludedTerm = request.lastIncludedTerm;
        pendingSnapshotInstall_.committedMembers = request.committedMembers;
        pendingSnapshotInstall_.jointOldMembers = request.jointOldMembers;
        pendingSnapshotInstall_.jointNewMembers = request.jointNewMembers;
        pendingSnapshotInstall_.configurationGeneration = request.configurationGeneration;
        pendingSnapshotInstall_.resultFloor = request.requestState.expiryFloor;
        pendingSnapshotInstall_.resultSourceGeneration = request.requestState.generation;
        if (request.requestState.generation) {
            ensureResultStore(); pendingSnapshotInstall_.results = resultStore_->beginImport(request.lastIncludedIndex, request.requestState.expiryFloor);
        }
        if (callbacks_.beginSnapshot) { callbacks_.beginSnapshot(request.snapshotSeq, request.entryCount); }
    }

    for (const auto& chunk : request.chunks) {
        if ((request.done && chunk.entryIndex >= request.entryCount) || chunk.valueOffset > chunk.valueSize || chunk.value.size() > chunk.valueSize
            - chunk.valueOffset) { return false; }
        if (!pendingSnapshotInstall_.hasCurrentEntry) {
            if (chunk.entryIndex != pendingSnapshotInstall_.nextEntryIndex || chunk.valueOffset != 0) { return false; }
            pendingSnapshotInstall_.hasCurrentEntry = true;
            pendingSnapshotInstall_.currentEntryIndex = chunk.entryIndex;
            pendingSnapshotInstall_.currentValueSize = chunk.valueSize;
            pendingSnapshotInstall_.currentValueOffset = 0;
            pendingSnapshotInstall_.currentValueCrc32c = chunk.valueCrc32c;
            pendingSnapshotInstall_.currentKey = chunk.key;
            pendingSnapshotInstall_.currentValueCrc = Crc32cStream{};
            if (callbacks_.beginSnapshotEntry) {
                callbacks_.beginSnapshotEntry(
                    pendingSnapshotInstall_.currentKey,
                    pendingSnapshotInstall_.currentValueSize,
                    pendingSnapshotInstall_.currentValueCrc32c
                );
            }
        }
        else if (chunk.entryIndex != pendingSnapshotInstall_.currentEntryIndex || !chunk.key.empty()) { return false; }

        if (pendingSnapshotInstall_.currentValueSize != chunk.valueSize || pendingSnapshotInstall_.currentValueCrc32c != chunk.valueCrc32c ||
            pendingSnapshotInstall_.currentValueOffset != chunk.valueOffset) { return false; }
        pendingSnapshotInstall_.currentValueCrc.update(chunk.value);
        if (callbacks_.appendSnapshotEntryChunk) { callbacks_.appendSnapshotEntryChunk(chunk.valueOffset, chunk.value); }
        pendingSnapshotInstall_.currentValueOffset += chunk.value.size();
        if (pendingSnapshotInstall_.currentValueOffset == pendingSnapshotInstall_.currentValueSize) {
            if (pendingSnapshotInstall_.currentValueCrc.finish() != pendingSnapshotInstall_.currentValueCrc32c) { return false; }
            if (callbacks_.finishSnapshotEntry) { callbacks_.finishSnapshotEntry(); }
            if (pendingSnapshotInstall_.nextEntryIndex == UINT64_MAX) { return false; }
            ++pendingSnapshotInstall_.nextEntryIndex;
            pendingSnapshotInstall_.currentKey.clear();
            pendingSnapshotInstall_.currentValueCrc = Crc32cStream{};
            pendingSnapshotInstall_.hasCurrentEntry = false;
        }
    }

    if (request.requestState.expiryFloor != pendingSnapshotInstall_.resultFloor || request.requestState.generation != pendingSnapshotInstall_.resultSourceGeneration) { return false; }
    for (const auto& [id, record] : request.results) {
        if (!pendingSnapshotInstall_.results) { return false; }
        std::vector<uint8_t> value; writeRequestRecord(value, record);
        resultStore_->import(*pendingSnapshotInstall_.results, requestStoreKey(id), {id.expiresAtUnixMs, record.index, std::move(value)});
    }
    if (!request.done) { return true; }
    if (pendingSnapshotInstall_.results && request.requestState.sequence != request.lastIncludedIndex) { return false; }
    if (pendingSnapshotInstall_.results && (pendingSnapshotInstall_.results->count != request.requestState.count ||
        pendingSnapshotInstall_.results->bytes != request.requestState.bytes)) { return false; }
    if (!pendingSnapshotInstall_.results && (request.requestState.count || request.requestState.bytes)) { return false; }
    if (pendingSnapshotInstall_.hasCurrentEntry || pendingSnapshotInstall_.nextEntryIndex != request.entryCount) { return false; }
    pendingSnapshotInstall_.entryCount = request.entryCount;
    return true;
}

// Both application and consensus locks are held by the caller.
std::optional<InstallSnapshotResponse> validateSnapshotInstallLocked(const InstallSnapshot& request) {
    if (!running_ || applyFailed_.load() || request.term != currentTerm_ ||
        !isReplicationMemberLocked(selfNodeId_) || !isVotingMemberLocked(request.leaderId)) {
        return InstallSnapshotResponse{.term = currentTerm_, .success = false, .lastIncludedIndex = lastIncludedIndex_};
    }
    if (pendingSnapshotInstall_.active && (pendingSnapshotInstall_.term != currentTerm_ ||
        pendingSnapshotInstall_.lastIncludedIndex <= commitIndex_)) {
        pendingSnapshotInstall_ = PendingSnapshotInstall{};
    }
    if (request.lastIncludedIndex <= commitIndex_) {
        // A committed prefix already belongs to this state machine.
        // Acknowledge the sender's cut without replacing any state or
        // claiming that an unapplied suffix is part of the snapshot.
        observeLeaderLocked(request.term, request.leaderId);
        return InstallSnapshotResponse{.term = currentTerm_, .success = true,
            .lastIncludedIndex = std::max(lastIncludedIndex_, request.lastIncludedIndex)};
    }
    return std::nullopt;
}

InstallSnapshotResponse handleInstallSnapshot(const InstallSnapshot& request) {
    std::optional<NodeRole> roleChange;
    {
        std::lock_guard lock{mutex_};
        if (!running_ || applyFailed_.load() || !isReplicationMemberLocked(selfNodeId_) || !isVotingMemberLocked(request.leaderId)) {
            return InstallSnapshotResponse{.term = currentTerm_, .success = false, .lastIncludedIndex = lastIncludedIndex_};
        }
        if (request.term < currentTerm_) {
            return InstallSnapshotResponse{.term = currentTerm_, .success = false, .lastIncludedIndex = lastIncludedIndex_};
        }
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
    }
    notifyRoleChange(roleChange);

    bool snapshotInstallIntentDurable = false;
    try {
        std::lock_guard applyLock{applyMutex_};
        if (applyFailed_.load()) { throw std::runtime_error("Raft: snapshot application requires reopen"); }
        {
            std::lock_guard lock{mutex_};
            if (const auto response = validateSnapshotInstallLocked(request)) { return *response; }
        }
        if (!applySnapshotChunkRequestLocked(request)) {
            std::lock_guard lock{mutex_};
            return InstallSnapshotResponse{.term = currentTerm_, .success = false, .lastIncludedIndex = lastIncludedIndex_};
        }
        if (!request.done) {
            std::lock_guard lock{mutex_};
            return InstallSnapshotResponse{.term = currentTerm_, .success = true, .lastIncludedIndex = lastIncludedIndex_};
        }
        SnapshotInstallTransaction transaction{
            .requestState = request.requestState,
            .snapshotSeq = request.snapshotSeq,
            .lastIncludedIndex = request.lastIncludedIndex,
            .lastIncludedTerm = request.lastIncludedTerm,
            .entryCount = request.entryCount,
            .configurationGeneration = request.configurationGeneration,
            .committedMembers = request.committedMembers,
            .jointOldMembers = request.jointOldMembers,
            .jointNewMembers = request.jointNewMembers
        };
        if (pendingSnapshotInstall_.results) {
            resultStore_->prepare(*pendingSnapshotInstall_.results);
            transaction.requestState.generation = pendingSnapshotInstall_.results->generation;
            transaction.requestState.sequence = pendingSnapshotInstall_.results->sequence;
        }
        if (callbacks_.prepareSnapshot) { callbacks_.prepareSnapshot(request.snapshotSeq, request.entryCount); }
        {
            std::lock_guard lock{mutex_};
            // AppendEntries can commit while chunk staging or scratch
            // synchronization runs, even though application is locked.
            if (const auto response = validateSnapshotInstallLocked(request)) { return *response; }
            const auto backup = captureDurableLogStateLocked();
            durableSnapshotInstall_ = transaction;
            try { persistLog(); }
            catch (...) {
                restoreDurableLogStateLocked(backup);
                return InstallSnapshotResponse{.term = currentTerm_, .success = false, .lastIncludedIndex = lastIncludedIndex_};
            }
            snapshotInstallIntentDurable = true;
        }
        if (callbacks_.finishSnapshot) { callbacks_.finishSnapshot(request.snapshotSeq, request.entryCount); }
        if (callbacks_.forceDurable) { callbacks_.forceDurable(); }
        {
            std::lock_guard lock{mutex_};
            if (currentTerm_ != request.term || !isVotingMemberLocked(request.leaderId)) {
                throw std::runtime_error("Raft: leadership changed during durable snapshot installation; reopen required");
            }
            const auto backup = captureDurableLogStateLocked();
            if (!finalizeSnapshotInstallMetadataLocked(transaction)) {
                restoreDurableLogStateLocked(backup);
                throw std::runtime_error("Raft: snapshot metadata finalization failed; reopen required");
            }
            observeLeaderLocked(request.term, request.leaderId);
            durableSnapshotInstall_.reset();
            pendingSnapshotInstall_ = PendingSnapshotInstall{};
            try { persistLog(); }
            catch (...) {
                restoreDurableLogStateLocked(backup);
                throw;
            }
            if (resultStore_) { resultStore_->collectGarbage(); }
            return InstallSnapshotResponse{.term = currentTerm_, .success = true, .lastIncludedIndex = lastIncludedIndex_};
        }
    }
    catch (...) {
        const auto failure = std::current_exception();
        {
            std::lock_guard applyLock{applyMutex_};
            {
                std::lock_guard lock{mutex_};
                pendingSnapshotInstall_ = PendingSnapshotInstall{};
            }
            if (snapshotInstallIntentDurable) { failApplication(failure); }
        }
        std::lock_guard lock{mutex_};
        return InstallSnapshotResponse{.term = currentTerm_, .success = false, .lastIncludedIndex = lastIncludedIndex_};
    }
}


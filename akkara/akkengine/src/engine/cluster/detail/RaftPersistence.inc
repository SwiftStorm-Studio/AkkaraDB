/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */


// Included inside RaftConsensusRuntime::Impl.
void persistState() {
    std::vector<uint8_t> bytes;
    bytes.insert(bytes.end(), {'A', 'K', 'R', 'S', '2'});
    bytes.insert(bytes.end(), config_.clusterId().begin(), config_.clusterId().end());
    bytes.insert(bytes.end(), groupIdentity_.begin(), groupIdentity_.end());
    bytes.insert(bytes.end(), transactionApplyFingerprint_.begin(), transactionApplyFingerprint_.end());
    writeU64(bytes, currentTerm_);
    writeU64(bytes, votedFor_);
    const auto crcOffset = bytes.size();
    writeU32(bytes, 0);
    writeU32At(bytes, crcOffset, crcWithZeroedField(bytes, crcOffset));
    writeFileAtomically(statePath_, bytes, "RaftConsensusRuntime state");
}

void enforceLogInvariantsLocked(const char* context) {
    if (!hasVoter(committedMembers_)) { throw std::runtime_error(std::string{context} + ": empty committed voter set"); }
    if ((jointOldMembers_ && !hasVoter(*jointOldMembers_)) || (jointNewMembers_ && !hasVoter(*jointNewMembers_))) {
        throw std::runtime_error(std::string{context} + ": empty joint voter set");
    }
    if (lastIncludedIndex_ > commitIndex_) { throw std::runtime_error(std::string{context} + ": lastIncludedIndex is ahead of commitIndex"); }
    if (lastApplied_ < lastIncludedIndex_) { throw std::runtime_error(std::string{context} + ": lastApplied is behind lastIncludedIndex"); }
    if (lastApplied_ > commitIndex_) { throw std::runtime_error(std::string{context} + ": lastApplied is ahead of commitIndex"); }
    if (commitIndex_ > lastLogIndex()) { throw std::runtime_error(std::string{context} + ": commitIndex is ahead of log"); }
    if (requestState_.sequence > lastApplied_) { throw std::runtime_error(std::string{context} + ": result store is ahead of applied log"); }
    if (jointOldMembers_.has_value() != jointNewMembers_.has_value()) {
        throw std::runtime_error(std::string{context} + ": incomplete joint voter set");
    }
    validateLogPrefixLocked(log_.size());
}

void recoverState() {
    if (!std::filesystem::exists(statePath_)) { return; }
    try {
        const auto bytes = readWholeFile(statePath_, "RaftConsensusRuntime state");
        constexpr size_t expectedSize = 5 + 16 + 32 + 32 + 8 + 8 + 4;
        constexpr size_t crcOffset = expectedSize - 4;
        if (bytes.size() != expectedSize) { throw std::runtime_error("invalid state file size"); }
        if (std::string_view{reinterpret_cast<const char*>(bytes.data()), 5} != "AKRS2") { throw std::runtime_error("bad state file magic"); }
        if (readU32(bytes, crcOffset) != crcWithZeroedField(bytes, crcOffset)) { throw std::runtime_error("state file CRC mismatch"); }
        if (!std::ranges::equal(config_.clusterId(), std::span{bytes}.subspan(5, config_.clusterId().size()))) {
            throw std::runtime_error("hard state belongs to a different cluster");
        }
        if (!std::ranges::equal(groupIdentity_, std::span{bytes}.subspan(21, groupIdentity_.size()))) {
            throw std::runtime_error("hard state belongs to a different Raft group");
        }
        if (!std::ranges::equal(transactionApplyFingerprint_, std::span{bytes}.subspan(53, transactionApplyFingerprint_.size()))) {
            throw std::runtime_error("hard state transaction apply policy mismatch");
        }
        currentTerm_ = readU64(bytes, 85);
        votedFor_ = readU64(bytes, 93);
    }
    catch (const std::exception& ex) { throw std::runtime_error(std::string{"RaftConsensusRuntime: corrupt hard state: "} + ex.what()); }
}

void writeSnapshotInstallTransactionField(std::vector<uint8_t>& out) const {
    writeU8(out, durableSnapshotInstall_ ? 1 : 0);
    if (!durableSnapshotInstall_) { return; }
    writeU64(out, durableSnapshotInstall_->snapshotSeq);
    writeU64(out, durableSnapshotInstall_->lastIncludedIndex);
    writeU64(out, durableSnapshotInstall_->lastIncludedTerm);
    writeU64(out, durableSnapshotInstall_->entryCount);
    writeU64(out, durableSnapshotInstall_->configurationGeneration);
    writeNodeSetField(out, durableSnapshotInstall_->committedMembers);
    writeOptionalNodeSetField(out, durableSnapshotInstall_->jointOldMembers);
    writeOptionalNodeSetField(out, durableSnapshotInstall_->jointNewMembers);
    writeRequestState(out, durableSnapshotInstall_->requestState);
}

static bool readSnapshotInstallTransactionField(
    std::span<const uint8_t> in,
    size_t& cursor,
    std::optional<SnapshotInstallTransaction>& transaction
) {
    if (cursor + 1 > in.size()) { return false; }
    const auto hasTransaction = in[cursor++] != 0;
    if (!hasTransaction) {
        transaction.reset();
        return true;
    }
    if (cursor + 40 > in.size()) { return false; }
    SnapshotInstallTransaction decoded;
    decoded.snapshotSeq = readU64(in, cursor);
    cursor += 8;
    decoded.lastIncludedIndex = readU64(in, cursor);
    cursor += 8;
    decoded.lastIncludedTerm = readU64(in, cursor);
    cursor += 8;
    decoded.entryCount = readU64(in, cursor);
    cursor += 8;
    decoded.configurationGeneration = readU64(in, cursor);
    cursor += 8;
    if (!readNodeSetField(in, cursor, decoded.committedMembers) || !readOptionalNodeSetField(in, cursor, decoded.jointOldMembers) || !
        readOptionalNodeSetField(in, cursor, decoded.jointNewMembers)) { return false; }
    if (!readRequestState(in, cursor, decoded.requestState)) { return false; }
    if (decoded.committedMembers.empty() || decoded.jointOldMembers.has_value() != decoded.jointNewMembers.has_value()) { return false; }
    if (decoded.jointOldMembers && (decoded.jointOldMembers->empty() || decoded.jointNewMembers->empty())) { return false; }
    transaction = std::move(decoded);
    return true;
}

[[nodiscard]] std::filesystem::path logSegmentDir() const {
    auto path = logPath_;
    path += ".segments";
    return path;
}

[[nodiscard]] std::filesystem::path logSegmentPath(uint64_t id) const { return logSegmentDir() / ("segment-" + std::to_string(id) + ".akrl"); }

[[nodiscard]] std::filesystem::path requestJournalPath(uint64_t generation) const {
    auto path = requestJournalBasePath_;
    path += "." + std::to_string(generation);
    return path;
}

void ensureResultStore() {
    if (!resultStore_) {
        resultStore_ = std::make_shared<detail::DiskResults>(dbDir_.empty() ? std::filesystem::path{"cluster-raft-results"} : dbDir_ / "raft-results",
            runtimeOptions_.requests.maxRetainedResultBytes);
    }
}
void observeResultStore() {
    if (!resultStore_) { requestState_ = {}; return; }
    requestState_ = {resultStore_->floor(), resultStore_->generation(), resultStore_->sequence(), resultStore_->count(), resultStore_->bytes()};
}
std::optional<RequestRecord> retainedRequest(const ClusterRequestId& id) const {
    if (!resultStore_) { return {}; }
    const auto stored = resultStore_->get(requestStoreKey(id));
    if (!stored) { return {}; }
    size_t cursor = 0; RequestRecord record;
    if (stored->expires != id.expiresAtUnixMs || !readRequestRecord(stored->value, cursor, record) || cursor != stored->value.size() || record.index != stored->sequence) {
        throw std::runtime_error("Raft: malformed retained result");
    }
    return record;
}
static std::vector<uint8_t> requestJournalRecord(uint8_t type, const RequestState& state) {
    std::vector<uint8_t> payload{type}; writeRequestState(payload, state);
    std::vector<uint8_t> framed; writeU32(framed, static_cast<uint32_t>(payload.size())); writeU32(framed, crcBytes(payload));
    framed.insert(framed.end(), payload.begin(), payload.end()); return framed;
}
void writeRequestJournalCheckpointLocked() {
    if (requestJournalGeneration_ == UINT64_MAX) { throw std::runtime_error("Raft request journal: generations exhausted"); }
    uint64_t generation = requestJournalGeneration_ + 1;
    while (std::filesystem::exists(requestJournalPath(generation))) {
        if (generation == UINT64_MAX) { throw std::runtime_error("Raft request journal: generations exhausted"); }
        ++generation;
    }
    std::vector<uint8_t> bytes{'A', 'K', 'R', 'Q', '2'};
    const auto record = requestJournalRecord(0, requestState_); bytes.insert(bytes.end(), record.begin(), record.end());
    writeFileAtomically(requestJournalPath(generation), bytes, "Raft result-store checkpoint");
    crashAtLogTestPoint("raft.request.after_journal_sync");
    requestJournalBytesWritten_.fetch_add(bytes.size()); ++requestJournalCompactions_;
    requestJournalGeneration_ = generation; requestJournalBytes_ = bytes.size(); requestJournalRecords_ = 1; journalState_ = requestState_;
}
void prepareRequestJournalLocked() {
    if (journalState_ == requestState_) { return; }
    if (resultStore_) { resultStore_->sync(); }
    const auto forced = requestJournalCompactRecords();
    if (!requestJournalGeneration_ || requestJournalBytes_ >= REQUEST_JOURNAL_COMPACT_BYTES ||
        (forced && requestJournalRecords_ >= forced) || requestState_.generation != journalState_.generation) {
        writeRequestJournalCheckpointLocked(); return;
    }
    const auto path = requestJournalPath(requestJournalGeneration_);
    if (!std::filesystem::exists(path) || std::filesystem::file_size(path) < requestJournalBytes_) { throw std::runtime_error("Raft result checkpoint is missing"); }
    if (std::filesystem::file_size(path) != requestJournalBytes_) { std::filesystem::resize_file(path, requestJournalBytes_); }
    const auto record = requestJournalRecord(1, requestState_);
    {
        std::ofstream out{path, std::ios::binary | std::ios::app};
        out.write(reinterpret_cast<const char*>(record.data()), static_cast<std::streamsize>(record.size())); out.flush();
        if (!out) { throw std::runtime_error("Raft result checkpoint write failed"); }
    }
    syncLogSegment(path); crashAtLogTestPoint("raft.request.after_journal_sync");
    requestJournalBytesWritten_.fetch_add(record.size()); requestJournalBytes_ += record.size(); ++requestJournalRecords_; journalState_ = requestState_;
}
void recoverRequestJournalLocked() {
    requestState_ = {}; journalState_ = {};
    if (!requestJournalGeneration_) {
        if (requestJournalBytes_ || requestJournalRecords_) { throw std::runtime_error("Raft: invalid empty result checkpoint"); }
        if (resultStore_ && resultStore_->sequence() > commitIndex_) { throw std::runtime_error("Raft: result store is ahead of committed log"); }
        observeResultStore(); return;
    }
    const auto path = requestJournalPath(requestJournalGeneration_);
    const auto bytes = readWholeFile(path, "Raft result checkpoint");
    if (requestJournalBytes_ < 5 || bytes.size() < requestJournalBytes_ || !std::ranges::starts_with(bytes, std::string_view{"AKRQ2"})) {
        throw std::runtime_error("Raft: invalid result checkpoint header");
    }
    size_t cursor = 5; uint64_t records = 0;
    while (cursor < requestJournalBytes_) {
        if (requestJournalBytes_ - cursor < 8) { throw std::runtime_error("Raft: truncated result checkpoint"); }
        const auto length = readU32(bytes, cursor), crc = readU32(bytes, cursor + 4); cursor += 8;
        if (length != 41 || length > requestJournalBytes_ - cursor) { throw std::runtime_error("Raft: invalid result checkpoint length"); }
        const auto payload = std::span{bytes}.subspan(cursor, length); cursor += length;
        size_t offset = 1; RequestState state;
        if (payload[0] > 1 || crcBytes(payload) != crc || !readRequestState(payload, offset, state) || offset != payload.size()) {
            throw std::runtime_error("Raft: corrupt result checkpoint");
        }
        requestState_ = state; ++records;
    }
    if (records != requestJournalRecords_) { throw std::runtime_error("Raft: result checkpoint count mismatch"); }
    journalState_ = requestState_;
    if (requestState_.generation) {
        if (!resultStore_) { throw std::runtime_error("Raft: retained result storage is missing"); }
        if (resultStore_->generation() != requestState_.generation) { resultStore_->activate(requestState_.generation); }
        if (resultStore_->sequence() < requestState_.sequence || resultStore_->sequence() > commitIndex_) {
            throw std::runtime_error("Raft: retained result storage does not match committed log");
        }
        lastApplied_ = std::max(lastApplied_, resultStore_->sequence()); observeResultStore();
    }
    if (bytes.size() != requestJournalBytes_) { std::filesystem::resize_file(path, requestJournalBytes_); }
}

void collectUnusedRequestJournals() const noexcept {
    try {
        const auto parent = requestJournalBasePath_.parent_path().empty() ? std::filesystem::path{"."} : requestJournalBasePath_.parent_path();
        const auto prefix = requestJournalBasePath_.filename().string() + ".";
        if (!std::filesystem::exists(parent)) { return; }
        for (const auto& file : std::filesystem::directory_iterator(parent)) {
            const auto name = file.path().filename().string();
            if (!file.is_regular_file() || !name.starts_with(prefix)) { continue; }
            const auto generation = name.substr(prefix.size());
            if (generation.empty() || generation.find_first_not_of("0123456789") != std::string::npos || file.path() == requestJournalPath(
                requestJournalGeneration_
            )) { continue; }
            std::error_code ec;
            std::filesystem::remove(file.path(), ec);
        }
    }
    catch (...) { /* Unreferenced generations are harmless and can be collected after the next publish. */ }
}

static void syncLogSegment(const std::filesystem::path& path) {
    #ifdef _WIN32
    const auto fd = _wopen(path.c_str(), _O_RDWR | _O_BINARY);
    if (fd < 0) { throw std::runtime_error("Raft log: cannot open segment for sync"); }
    const auto rc = _commit(fd);
    const auto closeRc = _close(fd);
    #else
    const auto fd = ::open(path.c_str(), O_RDONLY); if (fd < 0) { throw std::runtime_error("Raft log: cannot open segment for sync"); } const auto rc
        = ::fsync(fd); const auto closeRc = ::close(fd);
    #endif
    if (rc != 0 || closeRc != 0) { throw std::runtime_error("Raft log: segment sync failed"); }
}

void collectUnusedLogSegments() const noexcept {
    // The metadata is already durable. Unreferenced files, including
    // tails left by interrupted writes, are never part of recovery.
    try {
        std::unordered_set<std::string> live;
        for (const auto& extent : persistedLogLocations_.extents()) { live.insert(logSegmentPath(extent.segment).filename().string()); }
        if (!std::filesystem::exists(logSegmentDir())) { return; }
        for (const auto& file : std::filesystem::directory_iterator(logSegmentDir())) {
            const auto name = file.path().filename().string();
            if (!file.is_regular_file() || !name.starts_with("segment-") || !name.ends_with(".akrl") || live.contains(name)) { continue; }
            const auto number = name.substr(8, name.size() - 13);
            if (number.empty() || number.find_first_not_of("0123456789") != std::string::npos) { continue; }
            std::error_code ec;
            std::filesystem::remove(file.path(), ec);
        }
    }
    catch (...) { /* Cleanup is retryable; it cannot invalidate durable log state. */ }
}

void persistLog() {
    enforceLogInvariantsLocked("RaftConsensusRuntime log");
    const auto removed = persistedLogLocations_.reconcile(lastIncludedIndex_ + 1, lastLogIndex() + 1, changedLogFrom_);
    const auto common = persistedLogLocations_.size();
    auto& locations = persistedLogLocations_;
    try {
        if (common < log_.size()) {
            std::filesystem::create_directories(logSegmentDir());
            uint64_t segment = 0;
            uint64_t offset = 0;
            if (!locations.empty() && std::filesystem::file_size(logSegmentPath(locations.back().segment)) == locations.back().end) {
                segment = locations.back().segment;
                offset = locations.back().end;
            }
            std::ofstream out;
            const auto finishSegment = [&] {
                if (!out.is_open()) { return; }
                out.flush();
                if (!out) { throw std::runtime_error("Raft log: segment write failed"); }
                out.close();
                if (out.fail()) { throw std::runtime_error("Raft log: segment close failed"); }
                syncLogSegment(logSegmentPath(segment));
                #ifndef _WIN32
                syncParentDirectory(logSegmentPath(segment), "Raft log segment");
                #endif
            };
            for (size_t i = common; i < log_.size(); ++i) {
                const auto payload = encodeEntryPayload(log_[i]);
                std::vector<uint8_t> record;
                writeU64(record, payload.size());
                writeU32(record, crcBytes(payload));
                record.insert(record.end(), payload.begin(), payload.end());
                if (segment == 0 || (offset != 0 && offset + record.size() > LOG_SEGMENT_BYTES)) {
                    finishSegment();
                    do {
                        if (nextLogSegment_ == UINT64_MAX) { throw std::runtime_error("Raft log: segment ids exhausted"); }
                        segment = nextLogSegment_++;
                    }
                    while (std::filesystem::exists(logSegmentPath(segment)));
                    offset = 0;
                }
                if (!out.is_open()) {
                    out.open(logSegmentPath(segment), std::ios::binary | std::ios::app);
                    if (!out) { throw std::runtime_error("Raft log: cannot append segment"); }
                }
                out.write(reinterpret_cast<const char*>(record.data()), static_cast<std::streamsize>(record.size()));
                locations.append(LogLocation{log_[i].index, log_[i].term, segment, offset, offset + record.size()});
                offset += record.size();
            }
            finishSegment();
            #ifndef _WIN32
            syncParentDirectory(logSegmentDir(), "Raft log segment directory");
            #endif
            crashAtLogTestPoint("raft.after_segment_sync");
        }

        const auto& extents = locations.extents();
        prepareRequestJournalLocked();
        std::vector<uint8_t> bytes{'A', 'K', 'R', 'L', '2'};
        writeU64(bytes, commitIndex_);
        writeU64(bytes, lastApplied_);
        writeU64(bytes, lastIncludedIndex_);
        writeU64(bytes, lastIncludedTerm_);
        writeU64(bytes, lastIncludedStateMachineSeq_);
        writeU64(bytes, log_.size());
        bytes.insert(bytes.end(), groupIdentity_.begin(), groupIdentity_.end());
        bytes.insert(bytes.end(), transactionApplyFingerprint_.begin(), transactionApplyFingerprint_.end());
        writeNodeSetField(bytes, committedMembers_);
        writeOptionalNodeSetField(bytes, jointOldMembers_);
        writeOptionalNodeSetField(bytes, jointNewMembers_);
        writeU64(bytes, lastConfigurationGeneration_);
        writeU8(bytes, learnerBootstrapPending_ ? 1 : 0);
        writeSnapshotInstallTransactionField(bytes);
        writeU64(bytes, requestJournalGeneration_);
        writeU64(bytes, requestJournalBytes_);
        writeU64(bytes, requestJournalRecords_);
        writeU64(bytes, extents.size());
        for (const auto& extent : extents) {
            writeU64(bytes, extent.segment);
            writeU64(bytes, extent.begin);
            writeU64(bytes, extent.end);
        }
        writeU32(bytes, crcBytes(bytes));
        // Publish only after every referenced segment has reached disk.
        writeFileAtomically(logPath_, bytes, "RaftConsensusRuntime log metadata");
        crashAtLogTestPoint("raft.after_log_metadata");
        changedLogFrom_ = UINT64_MAX;
    }
    catch (...) {
        // An atomic replacement can have succeeded before a sync error.
        // Never continue appending against an uncertain durable boundary.
        logIoFailed_ = true;
        recordFailure(ClusterFailureCode::RAFT_PERSISTENCE, ClusterHealthState::FAILED);
        stopRuntimeLocked();
        throw;
    }
    if (removed) { collectUnusedLogSegments(); }
    collectUnusedRequestJournals();
}

void recoverLog() {
    if (!std::filesystem::exists(logPath_)) { return; }
    const auto bytes = readWholeFile(logPath_, "RaftConsensusRuntime log metadata");
    constexpr size_t fixedHeaderSize = 5 + 8 * 6 + 32 + 32;
    if (bytes.size() < fixedHeaderSize + 12) { throw std::runtime_error("RaftConsensusRuntime: truncated log metadata"); }
    if (std::string_view{reinterpret_cast<const char*>(bytes.data()), 5} != "AKRL2" || readU32(bytes, bytes.size() - 4) != crcBytes(
        std::span<const uint8_t>{bytes.data(), bytes.size() - 4}
    )) { throw std::runtime_error("RaftConsensusRuntime: log metadata CRC or magic mismatch"); }
    if (!std::ranges::equal(groupIdentity_, std::span{bytes}.subspan(53, groupIdentity_.size()))) {
        throw std::runtime_error("RaftConsensusRuntime: log belongs to a different Raft group");
    }
    if (!std::ranges::equal(transactionApplyFingerprint_, std::span{bytes}.subspan(85, transactionApplyFingerprint_.size()))) {
        throw std::runtime_error("RaftConsensusRuntime: log transaction apply policy mismatch");
    }
    commitIndex_ = readU64(bytes, 5);
    lastApplied_ = readU64(bytes, 13);
    lastIncludedIndex_ = readU64(bytes, 21);
    lastIncludedTerm_ = readU64(bytes, 29);
    lastIncludedStateMachineSeq_ = readU64(bytes, 37);
    const auto count = readU64(bytes, 45);
    size_t cursor = fixedHeaderSize;
    if (!readNodeSetField(bytes, cursor, committedMembers_) || !readOptionalNodeSetField(bytes, cursor, jointOldMembers_) || !
        readOptionalNodeSetField(bytes, cursor, jointNewMembers_) || committedMembers_.empty() || cursor + 8 > bytes.size() - 4) {
        throw std::runtime_error("RaftConsensusRuntime: corrupt log membership metadata");
    }
    lastConfigurationGeneration_ = readU64(bytes, cursor);
    cursor += 8;
    if (cursor >= bytes.size() - 4 || bytes[cursor] > 1) { throw std::runtime_error("RaftConsensusRuntime: invalid learner bootstrap state"); }
    learnerBootstrapPending_ = bytes[cursor++] != 0;
    if (!readSnapshotInstallTransactionField(bytes, cursor, durableSnapshotInstall_) || cursor + 32 > bytes.size() - 4) {
        throw std::runtime_error("RaftConsensusRuntime: corrupt snapshot transaction metadata");
    }
    requestJournalGeneration_ = readU64(bytes, cursor);
    requestJournalBytes_ = readU64(bytes, cursor + 8);
    requestJournalRecords_ = readU64(bytes, cursor + 16);
    cursor += 24;
    const auto extentCount = readU64(bytes, cursor);
    cursor += 8;
    if (extentCount > count || extentCount > (bytes.size() - 4 - cursor) / 24 || extentCount * 24 != bytes.size() - 4 - cursor) {
        throw std::runtime_error("RaftConsensusRuntime: invalid log segment metadata");
    }

    uint64_t lastGoodIndex = lastIncludedIndex_;
    uint64_t recoveredStateMachineSeq = lastIncludedStateMachineSeq_;
    bool recoveredSeqHasMutation = false;
    bool truncatedTail = false;
    const auto failOrTruncate = [&](const char* message) {
        if (runtimeOptions_.raftLogRecoveryAction == RaftLogRecoveryAction::TRUNCATE_UNCOMMITTED_TAIL && commitIndex_ <= lastGoodIndex) {
            truncatedTail = true;
            return;
        }
        throw std::runtime_error(std::string{"RaftConsensusRuntime: "} + message);
    };
    for (uint64_t e = 0; e < extentCount && !truncatedTail; ++e) {
        const LogExtent extent{readU64(bytes, cursor), readU64(bytes, cursor + 8), readU64(bytes, cursor + 16)};
        cursor += 24;
        if (extent.segment == 0 || extent.begin >= extent.end) { throw std::runtime_error("RaftConsensusRuntime: invalid segment extent"); }
        if (!std::filesystem::exists(logSegmentPath(extent.segment))) {
            failOrTruncate("missing log segment");
            break;
        }
        const auto segmentBytes = readWholeFile(logSegmentPath(extent.segment), "Raft log segment");
        uint64_t offset = extent.begin;
        while (offset < extent.end && !truncatedTail) {
            const uint64_t begin = offset;
            if (offset > segmentBytes.size() || segmentBytes.size() - offset < 12 || extent.end - offset < 12) {
                failOrTruncate("truncated log entry header");
                break;
            }
            const auto len = readU64(segmentBytes, static_cast<size_t>(offset));
            const auto crc = readU32(segmentBytes, static_cast<size_t>(offset + 8));
            offset += 12;
            if (len > ReplFrameHeader::MAX_RAFT_PAYLOAD_SIZE || len > segmentBytes.size() - offset || len > extent.end - offset) {
                failOrTruncate("truncated log entry payload");
                break;
            }
            const auto payload = std::span<const uint8_t>{segmentBytes.data() + offset, static_cast<size_t>(len)};
            offset += len;
            size_t payloadCursor = 0;
            RaftLogEntry entry;
            if (crc != crcBytes(payload) || !decodeEntryPayload(payload, payloadCursor, entry) || payloadCursor != payload.size() || entry.index !=
                lastGoodIndex + 1 || !isValidStateMachineEntry(entry) || (isStateMachineEntry(entry.kind) && entry.clientSeq <=
                    lastIncludedStateMachineSeq_) || !
                advanceStateMachineSequenceTracker(entry, recoveredStateMachineSeq, recoveredSeqHasMutation)) {
                failOrTruncate("corrupt log entry");
                break;
            }
            lastGoodIndex = entry.index;
            persistedLogLocations_.append(LogLocation{entry.index, entry.term, extent.segment, begin, offset});
            log_.push_back(std::move(entry));
        }
    }
    if (!truncatedTail && log_.size() != count) { failOrTruncate("log entry count mismatch"); }
    if (commitIndex_ > lastGoodIndex || lastIncludedIndex_ > commitIndex_ || lastApplied_ > commitIndex_) {
        throw std::runtime_error("RaftConsensusRuntime: committed log entry is missing or corrupt");
    }
    if (durableSnapshotInstall_ && durableSnapshotInstall_->lastIncludedIndex < lastIncludedIndex_) {
        throw std::runtime_error("RaftConsensusRuntime: snapshot install transaction does not match log metadata");
    }
    if (lastApplied_ < lastIncludedIndex_) { lastApplied_ = lastIncludedIndex_; }
    recoverRequestJournalLocked();
    if (truncatedTail) { persistLog(); }
    collectUnusedLogSegments();
    collectUnusedRequestJournals();
}

void validateRecoveredStateMachine() {
    if (!callbacks_.getLastSeq) { return; }
    const auto durableClientSeq = callbacks_.getLastSeq();
    std::lock_guard lock{mutex_};
    // A durable apply marker cannot replace the state machine it describes.
    // Snapshot intent owns recovery of its replacement image before replay.
    if (!durableSnapshotInstall_) {
        uint64_t requiredSequence = lastIncludedStateMachineSeq_;
        for (const auto& entry : log_) {
            if (entry.index > lastApplied_) { break; }
            if (entry.kind == RaftEntryKind::MUTATION) { requiredSequence = std::max(requiredSequence, entry.clientSeq); }
        }
        if (durableClientSeq < requiredSequence) {
            throw std::runtime_error("RaftConsensusRuntime: recovered state machine is behind its durable applied log; restore storage before reopening");
        }
    }
    // KV sequence alone does not prove that all application side effects
    // (including history) reached stable storage. Only durable Raft metadata
    // advances lastApplied_; start() replays its remaining committed suffix.
}

void recoverDurableSnapshotInstall() {
    std::optional<SnapshotInstallTransaction> transaction;
    {
        std::lock_guard lock{mutex_};
        transaction = durableSnapshotInstall_;
    }
    if (!transaction) { return; }

    try {
        std::lock_guard applyLock{applyMutex_};
        if (callbacks_.recoverSnapshot) { callbacks_.recoverSnapshot(transaction->snapshotSeq); }
        else if (callbacks_.finishSnapshot) { callbacks_.finishSnapshot(transaction->snapshotSeq, transaction->entryCount); }
        if (callbacks_.forceDurable) { callbacks_.forceDurable(); }

        std::lock_guard lock{mutex_};
        if (durableSnapshotInstall_ && durableSnapshotInstall_->snapshotSeq == transaction->snapshotSeq && durableSnapshotInstall_->lastIncludedIndex ==
            transaction->lastIncludedIndex && durableSnapshotInstall_->lastIncludedTerm == transaction->lastIncludedTerm) {
            const auto backup = captureDurableLogStateLocked();
            try {
                if (!finalizeSnapshotInstallMetadataLocked(*transaction)) {
                    throw std::runtime_error("RaftConsensusRuntime: failed to recover snapshot install membership");
                }
                durableSnapshotInstall_.reset();
                persistLog();
                if (resultStore_) { resultStore_->collectGarbage(); }
            }
            catch (...) {
                restoreDurableLogStateLocked(backup);
                throw;
            }
        }
    } catch (...) {
        const auto failure = std::current_exception(); failApplication(failure); std::rethrow_exception(failure);
    }
}


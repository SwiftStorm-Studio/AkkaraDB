/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/vlog/detail/VersionLogApi.hpp
VersionLog::VersionLog() = default;

std::unique_ptr<VersionLog> VersionLog::create(std::filesystem::path logPath, VersionLogOptions opts) {
    if (logPath.empty()) { throw std::invalid_argument("VersionLog: logPath is required"); }
    auto impl = std::make_unique<Impl>();
    impl->logPath_ = std::move(logPath);
    impl->opts_ = std::move(opts);
    impl->validateOptions();

    auto log = std::unique_ptr < VersionLog > (new VersionLog{});
    log->impl_ = std::move(impl);
    try { log->impl_->openOrCreate(); }
    catch (...) {
        log->impl_.reset();
        throw;
    }
    return log;
}

VersionLog::~VersionLog() {
    try { close(); }
    catch (...) {}
}

void VersionLog::append(
    std::span<const uint8_t> key,
    uint64_t seq,
    uint64_t sourceNodeId,
    uint64_t timestampNs,
    uint8_t flags,
    std::span<const uint8_t> value
) {
    appendDeferred(key, seq, sourceNodeId, timestampNs, flags, value);
    markCommitted(seq);
}

void VersionLog::appendDeferred(
    std::span<const uint8_t> key,
    uint64_t seq,
    uint64_t sourceNodeId,
    uint64_t timestampNs,
    uint8_t flags,
    std::span<const uint8_t> value
) {
    if (!impl_) { throw std::runtime_error("VersionLog: append rejected after close"); }
    impl_->waitForRecovery();

    const std::string keyStr(reinterpret_cast<const char*>(key.data()), key.size());
    const auto makeEntry = [&] {
        VersionEntry ve;
        ve.seq = seq;
        ve.sourceNodeId = sourceNodeId;
        ve.timestampNs = timestampNs;
        ve.flags = flags;
        ve.value.assign(value.begin(), value.end());
        return ve;
    };
    const auto persistAndPublish = [&](
        std::vector<uint8_t> bytes,
        VersionEntry entry,
        std::shared_ptr<Impl::AppendCompletion> completion = {}
    ) {
        std::unique_lock lock{impl_->writeMu_};
        if (!impl_->file_) { throw std::runtime_error("VersionLog: append rejected after close"); }
        const bool remainsResident = impl_->persistSerialized(
            lock,
            Impl::PendingWrite{std::move(bytes), keyStr, seq, flags, 0, std::move(completion)}
        );
        impl_->trackEntryStats(flags);
        if (remainsResident) { impl_->publishResident(keyStr, std::move(entry)); }
    };

    if (impl_->usesTrueParallelWrites()) {
        auto bytes = impl_->serializeEntry(key.data(), key.size(), seq, sourceNodeId, timestampNs, flags, value.data(), value.size());
        impl_->publishResident(keyStr, makeEntry());
        try { impl_->appendParallel(Impl::PendingWrite{std::move(bytes), keyStr, seq, flags, 0, {}}); }
        catch (...) {
            impl_->removeResidents(std::deque<Impl::PendingWrite>{Impl::PendingWrite{{}, keyStr, seq, flags, 0, {}}});
            throw;
        }
        return;
    }

    if (impl_->opts_.writeAdmission == VLogWriteAdmissionMode::SERIAL) {
        std::lock_guard admissionLock{impl_->serialAdmissionMu_};
        if (impl_->opts_.serialAppendMode == VLogSerialAppendMode::WAIT_PREVIOUS_APPEND) {
            impl_->waitForAppend(impl_->lastSerialAppendCompletion_);
        }
        auto bytes = impl_->serializeEntry(key.data(), key.size(), seq, sourceNodeId, timestampNs, flags, value.data(), value.size());
        std::shared_ptr<Impl::AppendCompletion> completion;
        if (impl_->opts_.serialAppendMode == VLogSerialAppendMode::WAIT_PREVIOUS_APPEND) {
            completion = std::make_shared<Impl::AppendCompletion>();
        }
        persistAndPublish(std::move(bytes), makeEntry(), completion);
        if (completion) { impl_->lastSerialAppendCompletion_ = std::move(completion); }
    }
    else {
        auto bytes = impl_->serializeEntry(key.data(), key.size(), seq, sourceNodeId, timestampNs, flags, value.data(), value.size());
        persistAndPublish(std::move(bytes), makeEntry());
    }
    impl_->runRequestedRetentionPrune();
}

void VersionLog::markCommitted(uint64_t seq) {
    if (impl_) {
        impl_->waitForRecovery();
        impl_->markCommitted(seq);
    }
}

void VersionLog::waitUntilReady() const { if (impl_) { impl_->waitForRecovery(); } }

std::optional<VersionEntry> VersionLog::getAt(std::span<const uint8_t> key, uint64_t atSeq) const {
    if (!impl_) { return std::nullopt; }

    impl_->waitForRecovery();
    impl_->checkAsyncError();
    const uint64_t visibleSeq = std::min(atSeq, impl_->visibleSeq());
    const std::string_view keySv(reinterpret_cast<const char*>(key.data()), key.size());
    auto resident = impl_->residentForKey(keySv);
    std::optional<VersionEntry> result;
    const auto consider = [&](VersionEntry entry) {
        if (entry.seq <= visibleSeq && (!result || result->seq < entry.seq)) { result = std::move(entry); }
    };
    const uint64_t trailingSegmentId = impl_->trailingReadSegmentId();
    std::shared_lock scanLock{impl_->scanMu_};
    const auto segments = impl_->segmentSnapshot();
    for (const auto& segment : segments) {
        if (segment.hasEntries && segment.firstSeq > visibleSeq) { continue; }
        std::vector<Impl::IndexVersion> versions;
        bool usedIndex = impl_->indexVersionsForSegment(segment, keySv, versions);
        if (usedIndex) {
            try {
                const auto after = std::upper_bound(
                    versions.begin(),
                    versions.end(),
                    visibleSeq,
                    [](uint64_t sequence, const Impl::IndexVersion& version) { return sequence < version.seq; }
                );
                if (after != versions.begin()) {
                    auto selected = std::prev(after);
                    while (selected != versions.begin() && std::prev(selected)->seq == selected->seq) { --selected; }
                    for (auto& entry : impl_->readIndexedEntries(
                             segment,
                             keySv,
                             std::span<const Impl::IndexVersion>{&*selected, 1},
                             visibleSeq
                         )) { consider(std::move(entry)); }
                }
            }
            catch (const Impl::IndexUnavailable&) { usedIndex = false; }
        }
        if (usedIndex) { continue; }
        (void)impl_->scanSegment(
            segment.path,
            impl_->allowTrailingReadForSegment(segment, trailingSegmentId),
            [&](std::string_view entryKey, const AkvlogV5EntryHeader& header, std::span<const uint8_t> entryValue) {
                if (entryKey != keySv || header.seq > visibleSeq) { return; }
                VersionEntry entry;
                entry.seq = header.seq;
                entry.sourceNodeId = header.sourceNodeId;
                entry.timestampNs = header.timestampNs;
                entry.flags = header.flags;
                entry.value.assign(entryValue.begin(), entryValue.end());
                consider(std::move(entry));
            }
        );
    }
    for (auto& entry : resident) { consider(std::move(entry)); }
    return result;
}

std::vector<VersionEntry> VersionLog::history(std::span<const uint8_t> key) const {
    if (!impl_) { return {}; }

    impl_->waitForRecovery();
    impl_->checkAsyncError();
    const uint64_t visibleSeq = impl_->visibleSeq();
    const std::string_view keySv(reinterpret_cast<const char*>(key.data()), key.size());
    std::vector<VersionEntry> entries;
    auto resident = impl_->residentForKey(keySv);
    const uint64_t trailingSegmentId = impl_->trailingReadSegmentId();
    std::shared_lock scanLock{impl_->scanMu_};
    const auto segments = impl_->segmentSnapshot();
    for (const auto& segment : segments) {
        if (segment.hasEntries && segment.firstSeq > visibleSeq) { continue; }
        std::vector<Impl::IndexVersion> versions;
        bool usedIndex = impl_->indexVersionsForSegment(segment, keySv, versions);
        if (usedIndex) {
            try {
                auto indexed = impl_->readIndexedEntries(segment, keySv, versions, visibleSeq);
                entries.insert(entries.end(), std::make_move_iterator(indexed.begin()), std::make_move_iterator(indexed.end()));
            }
            catch (const Impl::IndexUnavailable&) { usedIndex = false; }
        }
        if (usedIndex) { continue; }
        (void)impl_->scanSegment(
            segment.path,
            impl_->allowTrailingReadForSegment(segment, trailingSegmentId),
            [&](std::string_view entryKey, const AkvlogV5EntryHeader& header, std::span<const uint8_t> entryValue) {
                if (entryKey != keySv || header.seq > visibleSeq) { return; }
                VersionEntry entry;
                entry.seq = header.seq;
                entry.sourceNodeId = header.sourceNodeId;
                entry.timestampNs = header.timestampNs;
                entry.flags = header.flags;
                entry.value.assign(entryValue.begin(), entryValue.end());
                entries.push_back(std::move(entry));
            }
        );
    }
    for (auto& entry : resident) { if (entry.seq <= visibleSeq) { entries.push_back(std::move(entry)); } }
    std::sort(entries.begin(), entries.end(), [](const VersionEntry& left, const VersionEntry& right) { return left.seq < right.seq; });
    entries.erase(
        std::unique(
            entries.begin(),
            entries.end(),
            [](const VersionEntry& left, const VersionEntry& right) {
                return left.seq == right.seq && left.sourceNodeId == right.sourceNodeId && left.timestampNs == right.timestampNs && left.
                    flags == right.flags && left.value == right.value;
            }
        ),
        entries.end()
    );
    return entries;
}

std::vector<VersionRecord> VersionLog::collectSince(uint64_t afterSeq) const {
    if (!impl_) { return {}; }

    impl_->waitForRecovery();
    impl_->checkAsyncError();
    const uint64_t visibleSeq = impl_->visibleSeq();
    std::vector<VersionRecord> records;
    (void)impl_->scanLog(
        visibleSeq,
        [&](std::string_view entryKey, const AkvlogV5EntryHeader& header, std::span<const uint8_t> entryValue) {
            if (header.seq <= afterSeq || header.seq > visibleSeq) { return; }
            if ((header.flags & (VLOG_FLAG_ROLLBACK | VLOG_FLAG_RETENTION_BASE)) != 0) { return; }

            VersionRecord record;
            record.key.assign(entryKey.begin(), entryKey.end());
            record.entry.seq = header.seq;
            record.entry.sourceNodeId = header.sourceNodeId;
            record.entry.timestampNs = header.timestampNs;
            record.entry.flags = header.flags;
            record.entry.value.assign(entryValue.begin(), entryValue.end());
            records.push_back(std::move(record));
        }
    );
    auto resident = impl_->residentSnapshot();
    for (auto& [key, entries] : resident) {
        for (auto& entry : entries) {
            if (entry.seq <= afterSeq || entry.seq > visibleSeq) { continue; }
            if ((entry.flags & (VLOG_FLAG_ROLLBACK | VLOG_FLAG_RETENTION_BASE)) != 0) { continue; }

            VersionRecord record;
            record.key.assign(key.begin(), key.end());
            record.entry = std::move(entry);
            records.push_back(std::move(record));
        }
    }
    std::sort(
        records.begin(),
        records.end(),
        [](const VersionRecord& left, const VersionRecord& right) {
            if (left.entry.seq != right.entry.seq) { return left.entry.seq < right.entry.seq; }
            return left.key < right.key;
        }
    );
    records.erase(
        std::unique(
            records.begin(),
            records.end(),
            [](const VersionRecord& left, const VersionRecord& right) {
                return left.entry.seq == right.entry.seq && left.entry.sourceNodeId == right.entry.sourceNodeId && left.entry.timestampNs ==
                    right.entry.timestampNs && left.entry.flags == right.entry.flags && left.key == right.key && left.entry.value == right.
                    entry.value;
            }
        ),
        records.end()
    );
    return records;
}

std::vector<std::pair<std::vector<uint8_t>, std::optional<VersionEntry>>> VersionLog::collectRollbackTargets(uint64_t targetSeq) const {
    if (!impl_) { return {}; }

    impl_->waitForRecovery();
    impl_->checkAsyncError();
    const uint64_t visibleSeq = impl_->visibleSeq();
    const uint64_t effectiveTargetSeq = std::min(targetSeq, visibleSeq);
    std::unordered_map<std::string, std::vector<VersionEntry>> entriesByKey;
    auto resident = impl_->residentSnapshot();
    (void)impl_->scanLog(
        visibleSeq,
        [&](std::string_view entryKey, const AkvlogV5EntryHeader& header, std::span<const uint8_t> entryValue) {
            if (header.seq > visibleSeq) { return; }
            VersionEntry entry;
            entry.seq = header.seq;
            entry.sourceNodeId = header.sourceNodeId;
            entry.timestampNs = header.timestampNs;
            entry.flags = header.flags;
            entry.value.assign(entryValue.begin(), entryValue.end());
            entriesByKey[std::string{entryKey}].push_back(std::move(entry));
        }
    );
    for (auto& [key, entries] : resident) {
        auto& destination = entriesByKey[key];
        for (auto& entry : entries) { if (entry.seq <= visibleSeq) { destination.push_back(std::move(entry)); } }
    }
    std::vector<std::pair<std::vector<uint8_t>, std::optional<VersionEntry>>> result;
    for (auto& [key, versions] : entriesByKey) {
        std::sort(
            versions.begin(),
            versions.end(),
            [](const VersionEntry& left, const VersionEntry& right) { return left.seq < right.seq; }
        );
        versions.erase(
            std::unique(
                versions.begin(),
                versions.end(),
                [](const VersionEntry& left, const VersionEntry& right) {
                    return left.seq == right.seq && left.sourceNodeId == right.sourceNodeId && left.timestampNs == right.timestampNs && left
                       .flags == right.flags && left.value == right.value;
                }
            ),
            versions.end()
        );
        if (versions.empty() || versions.back().seq <= effectiveTargetSeq) { continue; }

        const auto pos = std::upper_bound(
            versions.begin(),
            versions.end(),
            effectiveTargetSeq,
            [](uint64_t seq, const VersionEntry& e) { return seq < e.seq; }
        );

        std::optional<VersionEntry> prev;
        if (pos != versions.begin()) { prev = *std::prev(pos); }

        result.emplace_back(std::vector<uint8_t>(key.begin(), key.end()), std::move(prev));
    }

    return result;
}

void VersionLog::seedCommittedSeq(uint64_t seq) {
    if (impl_) {
        impl_->waitForRecovery();
        impl_->resetCommittedSeq(seq);
    }
}

VersionLogSnapshot VersionLog::snapshot() const noexcept {
    VersionLogSnapshot out;
    if (!impl_) { return out; }

    std::array<VersionLog::Impl::ParallelLane*, 64> lanes{};
    size_t laneCount = 0;
    {
        std::lock_guard writeLock{impl_->writeMu_};
        std::shared_lock residentLock{impl_->residentMu_};
        std::shared_lock segmentLock{impl_->segmentMu_};
        out.syncMode = static_cast<uint8_t>(impl_->opts_.syncMode);
        out.codec = static_cast<uint8_t>(impl_->opts_.codec);
        out.zstdCompressionLevel = impl_->opts_.zstdCompressionLevel;
        out.groupN = impl_->opts_.groupN;
        out.groupMicros = impl_->opts_.groupMicros;
        out.groupBytes = impl_->opts_.groupBytes;
        out.asyncMaxPendingBytes = impl_->opts_.asyncMaxPendingBytes;
        out.indexedKeys = impl_->residentIndex_.size();
        out.indexedEntries = impl_->indexedEntries_.load(std::memory_order_relaxed);
        out.rollbackEntries = impl_->rollbackEntries_.load(std::memory_order_relaxed);
        out.pendingWrites = impl_->pendingWrites_.size();
        out.pendingBytes = impl_->pendingBytes_;
        out.durableBytes = impl_->knownWrittenBytes_;
        out.segmentCount = impl_->segments_.size();
        out.activeSegmentBytes = impl_->activeSegmentBytes_;
        out.retentionDays = impl_->opts_.retentionDays;
        out.retentionMinCommitSeq = impl_->opts_.retentionMinCommitSeq;
        out.flushThreadRunning = impl_->flushThread_.joinable();
        out.recoveryDurationMicros = impl_->recoveryDurationMicros_.load(std::memory_order_relaxed);
        out.recoveredSegmentCount = impl_->recoveredSegmentCount_.load(std::memory_order_relaxed);
        out.recoveredEntryCount = impl_->recoveredEntryCount_.load(std::memory_order_relaxed);
        out.sidecarFallbackCount = impl_->sidecarFallbackCount_.load(std::memory_order_relaxed);
        out.sidecarRebuildFailures = impl_->sidecarRebuildFailures_.load(std::memory_order_relaxed);
        out.retentionPrunedSegments = impl_->retentionPrunedSegments_.load(std::memory_order_relaxed);
        out.retentionBaseEntriesWritten = impl_->retentionBaseEntriesWritten_.load(std::memory_order_relaxed);
        out.parallelQueueRejects = impl_->parallelQueueRejects_.load(std::memory_order_relaxed);
        out.parallelLaneCount = impl_->parallelLanes_.size();
        for (const auto& lane : impl_->parallelLanes_) {
            if (laneCount >= lanes.size()) { break; }
            lanes[laneCount++] = lane.get();
        }
    }
    for (size_t i = 0; i < laneCount; ++i) {
        auto* lane = lanes[i];
        std::lock_guard laneLock{lane->mutex};
        out.parallelPendingWrites += lane->pendingWrites.size();
        if (impl_->opts_.parallelPendingLimitScope != VLogParallelPendingLimitScope::GLOBAL) {
            out.parallelPendingBytes += lane->pendingBytes;
        }
    }
    if (impl_->opts_.parallelPendingLimitScope == VLogParallelPendingLimitScope::GLOBAL) {
        std::lock_guard pendingLock{impl_->parallelQueueMu_};
        out.parallelPendingBytes = impl_->parallelPendingBytes_;
    }
    return out;
}

void VersionLog::forceSync() {
    if (!impl_) { return; }
    impl_->waitForRecovery();
    impl_->joinRecoveryWorker();

    if (impl_->usesTrueParallelWrites()) {
        impl_->checkAsyncError();
        impl_->stopParallelWorkers();
        impl_->forceSyncParallelLanes();
        impl_->startParallelWorkers();
        return;
    }

    if (impl_->opts_.syncMode == VLogSyncMode::SYNC) {
        std::lock_guard lock{impl_->writeMu_};
        impl_->checkAsyncError();
        if (!impl_->file_) { return; }
        flushChecked(impl_->file_);
        fdatasyncChecked(impl_->file_);
        impl_->writeTailFile(impl_->segmentPath(impl_->activeSegmentId_), impl_->activeSegmentBytes_, true);
        return;
    }

    impl_->stopAsyncWorker();

    {
        std::lock_guard lock{impl_->writeMu_};
        impl_->checkAsyncError();
        if (!impl_->file_) { return; }
        flushChecked(impl_->file_);
        fdatasyncChecked(impl_->file_);
        impl_->writeTailFile(impl_->segmentPath(impl_->activeSegmentId_), impl_->activeSegmentBytes_, true);
        impl_->closing_ = false;
    }

    impl_->startAsyncWorkerIfNeeded();
}

void VersionLog::close() {
    if (!impl_) { return; }
    std::exception_ptr recoveryError;
    try { impl_->waitForRecovery(); }
    catch (...) { recoveryError = std::current_exception(); }
    impl_->joinRecoveryWorker();
    impl_->stopAsyncWorker();

    if (impl_->usesTrueParallelWrites()) {
        impl_->stopParallelWorkers();
        const auto asyncError = impl_->asyncError_;
        impl_->closeParallelLanes();
        impl_->pruneClosedSegments();
        if (recoveryError) { std::rethrow_exception(recoveryError); }
        if (asyncError) { std::rethrow_exception(asyncError); }
        return;
    }
    impl_->pruneClosedSegments();

    std::exception_ptr asyncError;
    {
        std::lock_guard lock{impl_->writeMu_};
        asyncError = impl_->asyncError_;
        if (!impl_->file_) {
            if (recoveryError) { std::rethrow_exception(recoveryError); }
            if (asyncError) { std::rethrow_exception(asyncError); }
            return;
        }
        flushChecked(impl_->file_);
        fdatasyncChecked(impl_->file_);
        impl_->writeTailFile(impl_->segmentPath(impl_->activeSegmentId_), impl_->activeSegmentBytes_, true);
        impl_->writeActiveSegmentIndexLocked();
        FILE* file = impl_->file_;
        impl_->file_ = nullptr;
        closeChecked(file);
    }
    if (recoveryError) { std::rethrow_exception(recoveryError); }
    if (asyncError) { std::rethrow_exception(asyncError); }
}

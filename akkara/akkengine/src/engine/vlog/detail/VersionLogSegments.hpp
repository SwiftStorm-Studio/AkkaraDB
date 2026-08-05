/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/vlog/detail/VersionLogSegments.hpp
void recordActiveIndex(const std::string& key, uint64_t seq, uint64_t offset) {
    if (opts_.segmentBytes == 0) { return; }
    std::unique_lock lock{activeIndexMu_};
    auto& versions = activeSegmentIndex_[key];
    const auto pos = std::upper_bound(
        versions.begin(),
        versions.end(),
        IndexVersion{seq, offset},
        [](const IndexVersion& left, const IndexVersion& right) {
            return left.seq == right.seq ? left.offset < right.offset : left.seq < right.seq;
        }
    );
    versions.insert(pos, IndexVersion{seq, offset});
}

void resetActiveIndex(uint64_t segmentId, SegmentKeyIndex index) {
    std::unique_lock lock{activeIndexMu_};
    if (opts_.segmentBytes == 0) {
        activeIndexSegmentId_ = std::numeric_limits<uint64_t>::max();
        activeSegmentIndex_.clear();
        return;
    }
    activeIndexSegmentId_ = segmentId;
    activeSegmentIndex_ = std::move(index);
}

[[nodiscard]] bool activeIndexVersions(uint64_t segmentId, std::string_view key, std::vector<IndexVersion>& out) const {
    if (usesTrueParallelWrites()) {
        for (const auto& lane : parallelLanes_) {
            std::lock_guard laneLock{lane->mutex};
            if (lane->segmentId != segmentId) { continue; }
            const auto it = lane->index.find(key);
            if (it != lane->index.end()) { out = it->second; }
            return true;
        }
        return false;
    }
    std::shared_lock lock{activeIndexMu_};
    if (activeIndexSegmentId_ != segmentId) { return false; }
    const auto it = activeSegmentIndex_.find(key);
    if (it != activeSegmentIndex_.end()) { out = it->second; }
    return true;
}

[[nodiscard]] std::vector<SegmentInfo> discoverSegments() const {
    std::vector<SegmentInfo> discovered;
    if (fs::exists(logPath_)) { discovered.push_back(SegmentInfo{0, logPath_}); }

    const auto parent = logPath_.parent_path().empty() ? fs::path{"."} : logPath_.parent_path();
    const std::string prefix = logPath_.stem().string() + "-seg-";
    const std::string extension = logPath_.extension().string();
    for (const auto& entry : fs::directory_iterator(parent)) {
        if (!entry.is_regular_file()) { continue; }
        const std::string name = entry.path().filename().string();
        if (!name.starts_with(prefix) || !name.ends_with(extension) || name.size() <= prefix.size() + extension.size()) { continue; }
        const std::string_view suffix{name.data() + prefix.size(), name.size() - prefix.size() - extension.size()};
        if (suffix.empty() || !std::ranges::all_of(suffix, [](unsigned char ch) { return ch >= '0' && ch <= '9'; })) { continue; }
        try {
            const uint64_t id = std::stoull(std::string{suffix});
            if (id != 0) { discovered.push_back(SegmentInfo{id, entry.path()}); }
        }
        catch (const std::exception&) {}
    }
    std::sort(discovered.begin(), discovered.end(), [](const SegmentInfo& left, const SegmentInfo& right) { return left.id < right.id; });
    return discovered;
}

[[nodiscard]] std::vector<SegmentInfo> segmentSnapshot() const {
    std::shared_lock lock{segmentMu_};
    return segments_;
}

[[nodiscard]] bool retentionEnabled() const noexcept { return opts_.retentionDays != 0 || opts_.retentionMinCommitSeq != 0; }

[[nodiscard]] bool segmentReachedRetentionBoundary(const SegmentInfo& segment, fs::file_time_type now) const {
    if (opts_.retentionMinCommitSeq != 0 && segment.hasEntries && segment.lastSeq < opts_.retentionMinCommitSeq) { return true; }
    if (opts_.retentionDays == 0) { return false; }

    std::error_code error;
    const auto modified = fs::last_write_time(segment.path, error);
    if (error || modified > now) { return false; }
    const auto retentionAge = std::chrono::hours{24LL * static_cast<int64_t>(opts_.retentionDays)};
    return now - modified >= retentionAge;
}

void pruneClosedSegments() {
    if (!retentionEnabled()) { return; }
    std::unique_lock retentionLock{retentionMu_};
    for (uint32_t attempt = 0; attempt < 2; ++attempt) {
        std::vector<SegmentInfo> segments;
        std::vector<SegmentInfo> expired;
        uint64_t baseSeq = 0;
        uint64_t generation = 0;
        bool needsBase = false;
        {
            std::lock_guard writeLock{writeMu_};
            const auto now = fs::file_time_type::clock::now();
            const uint64_t committedSeq = committedSeq_.load(std::memory_order_acquire);
            segments = segmentSnapshot();
            expired.reserve(segments.size());
            for (const auto& segment : segments) {
                if ((usesTrueParallelWrites() ? parallelActiveSegmentIds_.contains(segment.id) : segment.id == activeSegmentId_) || (segment
                   .hasEntries && segment.lastSeq > committedSeq) || !segmentReachedRetentionBoundary(segment, now)) { continue; }
                expired.push_back(segment);
            }
            if (expired.empty()) { return; }

            bool hasExpiredEntries = false;
            uint64_t latestExpiredSeq = 0;
            for (const auto& segment : expired) {
                if (!segment.hasEntries) { continue; }
                latestExpiredSeq = hasExpiredEntries ? std::max(latestExpiredSeq, segment.lastSeq) : segment.lastSeq;
                hasExpiredEntries = true;
            }
            if (hasExpiredEntries) {
                baseSeq = latestExpiredSeq;
                if (latestExpiredSeq != std::numeric_limits<uint64_t>::max()) { baseSeq = latestExpiredSeq + 1u; }
                if (opts_.retentionMinCommitSeq != 0) { baseSeq = std::max(baseSeq, opts_.retentionMinCommitSeq); }
                if (baseSeq > committedSeq) { return; }
                needsBase = true;
            }
            generation = persistedGeneration_.load(std::memory_order_acquire);
        }

        RetentionStateMap states;
        if (needsBase) {
            std::shared_lock scanLock{scanMu_};
            states = buildRetentionBaseStates(segments, baseSeq);
        }

        std::lock_guard writeLock{writeMu_};
        if (persistedGeneration_.load(std::memory_order_acquire) != generation) { continue; }
        std::unique_lock scanLock{scanMu_};
        const auto expiredIds = retentionSegmentIds(expired);
        if (needsBase) { persistRetentionBasesLocked(states, expiredIds, baseSeq); }
        deleteRetentionSegmentsLocked(expiredIds);
        return;
    }
    // A busy writer prevented a stable snapshot twice. Leave the
    // request pending; the next append or close retries it without
    // making unrelated foreground operations wait for the full scan.
    retentionPrunePending_.store(true, std::memory_order_release);
}

void runRequestedRetentionPrune() { if (retentionPrunePending_.exchange(false, std::memory_order_acq_rel)) { pruneClosedSegments(); } }

void notePersistedLocked(const PendingWrite& write) {
    activeSegmentBytes_ += static_cast<uint64_t>(write.bytes.size());
    {
        std::unique_lock lock{segmentMu_};
        const auto it = std::find_if(
            segments_.begin(),
            segments_.end(),
            [this](const SegmentInfo& segment) { return segment.id == activeSegmentId_; }
        );
        if (it == segments_.end()) { throw std::runtime_error("VersionLog: active segment is missing from the segment index"); }
        if (!it->hasEntries) {
            it->firstSeq = write.seq;
            it->lastSeq = write.seq;
            it->hasEntries = true;
        }
        else {
            it->firstSeq = std::min(it->firstSeq, write.seq);
            it->lastSeq = std::max(it->lastSeq, write.seq);
        }
        it->bytes = activeSegmentBytes_;
        ++it->entryCount;
        if ((write.flags & VLOG_FLAG_ROLLBACK) != 0) { ++it->rollbackCount; }
    }
    recordActiveIndex(write.key, write.seq, write.offset);
    persistedGeneration_.fetch_add(1, std::memory_order_release);
}

// writeMu_ must be held by the caller and all persisted records must
// already have been published to activeSegmentIndex_. A single-file
// log deliberately keeps no unbounded active index, so close rebuilds
// its derived sidecar once from the authoritative file instead.
void writeActiveSegmentIndexLocked() {
    if (opts_.segmentBytes == 0) {
        try {
            SegmentKeyIndex rebuilt;
            const auto summary = scanSegment(
                segmentPath(activeSegmentId_),
                false,
                [&rebuilt](std::string_view key, const AkvlogV5EntryHeader& header, std::span<const uint8_t>, uint64_t offset) {
                    rebuilt[std::string{key}].push_back(IndexVersion{header.seq, offset});
                }
            );
            tryWriteSegmentIndex(segmentPath(activeSegmentId_), summary.durableBytes, rebuilt);
        }
        catch (...) {}
        return;
    }
    SegmentKeyIndex index;
    {
        std::shared_lock lock{activeIndexMu_};
        if (activeIndexSegmentId_ != activeSegmentId_) { return; }
        index = activeSegmentIndex_;
    }
    tryWriteSegmentIndex(segmentPath(activeSegmentId_), activeSegmentBytes_, index);
}

void rotateSegmentIfNeededLocked() {
    if (opts_.segmentBytes == 0 || activeSegmentBytes_ < opts_.segmentBytes) { return; }
    if (!file_) { return; }

    flushChecked(file_);
    fdatasyncChecked(file_);
    writeTailFile(segmentPath(activeSegmentId_), activeSegmentBytes_, true);
    writeActiveSegmentIndexLocked();
    FILE* oldFile = file_;
    file_ = nullptr;
    closeChecked(oldFile);

    uint64_t nextId = activeSegmentId_ + 1;
    {
        std::shared_lock lock{segmentMu_};
        if (!segments_.empty()) { nextId = std::max(nextId, segments_.back().id + 1); }
    }
    const auto path = segmentPath(nextId);
    #ifdef _WIN32
    file_ = _wfopen(path.wstring().c_str(), L"ab");
    #else
    file_ = fopen(path.string().c_str(), "ab");
    #endif
    if (!file_) { throw std::runtime_error("VersionLog: cannot create segment: " + path.string()); }
    writeFileHeader(file_);
    activeSegmentId_ = nextId;
    activeSegmentBytes_ = FILE_HDR_SIZE;
    {
        std::unique_lock lock{segmentMu_};
        segments_.push_back(SegmentInfo{nextId, path, 0, 0, activeSegmentBytes_});
    }
    resetActiveIndex(nextId, {});
    if (!retentionCompacting_ && retentionEnabled()) { retentionPrunePending_.store(true, std::memory_order_release); }
}

[[nodiscard]] uint32_t parallelLaneCount() const noexcept {
    if (opts_.parallelWriteLanes != 0) { return opts_.parallelWriteLanes; }
    const uint32_t hardware = std::thread::hardware_concurrency();
    return std::clamp(hardware == 0 ? 2u : hardware, 2u, 8u);
}

void addParallelLaneSegmentLocked(ParallelLane& lane) {
    const uint64_t id = nextParallelSegmentId_++;
    const auto path = segmentPath(id);
    #ifdef _WIN32
    FILE* file = _wfopen(path.wstring().c_str(), L"wb");
    #else
    FILE* file = fopen(path.string().c_str(), "wb");
    #endif
    if (!file) { throw std::runtime_error("VersionLog: cannot create parallel segment: " + path.string()); }
    try {
        writeFileHeaderRaw(file, opts_.syncMode);
        writeTailFile(path, FILE_HDR_SIZE, true);
    }
    catch (...) {
        fclose(file);
        throw;
    }
    lane.file = file;
    lane.segmentId = id;
    lane.bytes = FILE_HDR_SIZE;
    lane.index.clear();
    parallelActiveSegmentIds_.insert(id);
    activeSegmentId_ = id;
    activeSegmentBytes_ = lane.bytes;
    knownWrittenBytes_ += lane.bytes;
    {
        std::unique_lock segmentLock{segmentMu_};
        segments_.push_back(SegmentInfo{id, path, 0, 0, lane.bytes});
    }
}

void initializeParallelLanesAfterRecovery() {
    if (!usesTrueParallelWrites()) { return; }
    std::lock_guard writeLock{writeMu_};
    if (file_) {
        FILE* file = file_;
        file_ = nullptr;
        closeChecked(file);
    }
    {
        std::shared_lock segmentLock{segmentMu_};
        if (!segments_.empty()) { nextParallelSegmentId_ = segments_.back().id + 1u; }
    }
    parallelLanes_.reserve(parallelLaneCount());
    for (uint32_t i = 0; i < parallelLaneCount(); ++i) {
        auto lane = std::make_unique<ParallelLane>();
        addParallelLaneSegmentLocked(*lane);
        parallelLanes_.push_back(std::move(lane));
    }
    startParallelWorkers();
}

void prepareSerialAppendAfterRecovery() {
    if (usesTrueParallelWrites()) { return; }
    const auto path = segmentPath(activeSegmentId_);
    if (activeSegmentBytes_ == 0) {
        if (file_) {
            FILE* file = file_;
            file_ = nullptr;
            closeChecked(file);
        }
        std::error_code error;
        fs::resize_file(path, 0, error);
        if (error) { throw std::runtime_error("VersionLog: cannot reset empty active segment: " + error.message()); }
        #ifdef _WIN32
        file_ = _wfopen(path.wstring().c_str(), L"ab");
        #else
        file_ = fopen(path.string().c_str(), "ab");
        #endif
        if (!file_) { throw std::runtime_error("VersionLog: cannot reopen empty active segment"); }
        writeFileHeader(file_);
        activeSegmentBytes_ = FILE_HDR_SIZE;
        {
            std::unique_lock lock{segmentMu_};
            const auto it = std::find_if(
                segments_.begin(),
                segments_.end(),
                [this](const SegmentInfo& segment) { return segment.id == activeSegmentId_; }
            );
            if (it != segments_.end()) { it->bytes = activeSegmentBytes_; }
        }
        return;
    }
    const auto tail = readTailFile(path);
    if (!tail.has_value()) { return; }
    if (file_) {
        FILE* file = file_;
        file_ = nullptr;
        closeChecked(file);
    }
    std::error_code error;
    fs::resize_file(path, *tail, error);
    if (error) { throw std::runtime_error("VersionLog: cannot truncate durable tail for serial append: " + error.message()); }
    fs::remove(segmentTailPath(path), error);
    if (error) { throw std::runtime_error("VersionLog: cannot remove durable tail for serial append: " + error.message()); }
    #ifdef _WIN32
    file_ = _wfopen(path.wstring().c_str(), L"ab");
    #else
    file_ = fopen(path.string().c_str(), "ab");
    #endif
    if (!file_) { throw std::runtime_error("VersionLog: cannot reopen serial active segment"); }
    activeSegmentBytes_ = *tail;
}

void truncateSerialActiveSegmentAfterRecovery(uint64_t bytes) {
    if (usesTrueParallelWrites()) { return; }
    const auto path = segmentPath(activeSegmentId_);
    if (file_) {
        FILE* file = file_;
        file_ = nullptr;
        closeChecked(file);
    }
    std::error_code error;
    fs::resize_file(path, bytes, error);
    if (error) { throw std::runtime_error("VersionLog: cannot truncate serial active segment: " + error.message()); }
    #ifdef _WIN32
    file_ = _wfopen(path.wstring().c_str(), L"ab");
    #else
    file_ = fopen(path.string().c_str(), "ab");
    #endif
    if (!file_) { throw std::runtime_error("VersionLog: cannot reopen truncated serial active segment"); }
    activeSegmentBytes_ = bytes;
}

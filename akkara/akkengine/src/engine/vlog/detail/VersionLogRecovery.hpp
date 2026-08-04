/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/vlog/detail/VersionLogRecovery.hpp
template <typename Visitor>
[[nodiscard]] ScanSummary scanLog(bool allowTrailingEntry, uint64_t maxSeq, Visitor&& visitor) const {
    std::shared_lock scanLock{scanMu_};
    ScanSummary total;
    const auto segments = segmentSnapshot();
    for (const auto& segment : segments) {
        if (segment.hasEntries && segment.firstSeq > maxSeq) { continue; }
        mergeScanSummary(total, scanSegment(segment.path, allowTrailingEntry, visitor));
    }
    return total;
}

void applyRecoverySummary(const ScanSummary& summary, std::vector<SegmentInfo> segments) {
    {
        std::lock_guard lock{writeMu_};
        recoveredMaxSeq_ = summary.maxSeq;
        indexedEntries_.store(summary.entryCount, std::memory_order_relaxed);
        rollbackEntries_.store(summary.rollbackCount, std::memory_order_relaxed);
        durableBytes_ = summary.durableBytes;
        activeSegmentId_ = segments.empty() ? 0 : segments.back().id;
        activeSegmentBytes_ = segments.empty() ? 0 : segments.back().bytes;
        std::unique_lock segmentLock{segmentMu_};
        segments_ = std::move(segments);
    }
    resetCommittedSeq(std::max(opts_.initialCommittedSeq, summary.maxSeq));
}

void runRecovery() noexcept {
    const auto started = std::chrono::steady_clock::now();
    std::exception_ptr error;
    uint64_t recoveredSegments = 0;
    uint64_t recoveredEntries = 0;
    try {
        auto segments = discoverSegments();
        ScanSummary total;
        SegmentKeyIndex activeIndex;
        uint64_t recoveredActiveSegmentId = 0;
        std::optional<uint64_t> truncateSerialActiveTo;
        const uint64_t lastSegmentId = segments.empty() ? 0 : segments.back().id;
        for (auto& segment : segments) {
            SegmentKeyIndex segmentIndex;
            const bool serialAsyncActiveSegment = !usesTrueParallelWrites() && opts_.syncMode == VLogSyncMode::ASYNC && segment.id ==
                lastSegmentId;
            const auto summary = scanSegment(
                segment.path,
                serialAsyncActiveSegment,
                [&segmentIndex](std::string_view key, const AkvlogV5EntryHeader& header, std::span<const uint8_t>, uint64_t offset) {
                    auto& versions = segmentIndex[std::string{key}];
                    versions.push_back(IndexVersion{header.seq, offset});
                },
                serialAsyncActiveSegment
            );
            segment.firstSeq = summary.firstSeq;
            segment.lastSeq = summary.maxSeq;
            segment.bytes = summary.durableBytes;
            segment.entryCount = summary.entryCount;
            segment.rollbackCount = summary.rollbackCount;
            segment.hasEntries = summary.hasEntries;
            mergeScanSummary(total, summary);
            if (summary.stoppedAtTrailingEntry) { truncateSerialActiveTo = summary.durableBytes; }
            tryWriteSegmentIndex(segment.path, segment.bytes, segmentIndex);
            recoveredActiveSegmentId = segment.id;
            activeIndex = std::move(segmentIndex);
        }
        recoveredSegments = static_cast<uint64_t>(segments.size());
        recoveredEntries = total.entryCount;
        applyRecoverySummary(total, std::move(segments));
        resetActiveIndex(recoveredActiveSegmentId, std::move(activeIndex));
        if (truncateSerialActiveTo.has_value()) { truncateSerialActiveSegmentAfterRecovery(*truncateSerialActiveTo); }
        prepareSerialAppendAfterRecovery();
        pruneClosedSegments();
        initializeParallelLanesAfterRecovery();
    }
    catch (...) { error = std::current_exception(); }

    const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - started).count();
    recoveryDurationMicros_.store(static_cast<uint64_t>(std::max<int64_t>(0, elapsed)), std::memory_order_relaxed);
    recoveredSegmentCount_.store(recoveredSegments, std::memory_order_relaxed);
    recoveredEntryCount_.store(recoveredEntries, std::memory_order_relaxed);
    {
        std::lock_guard lock{recoveryMu_};
        recoveryError_ = std::move(error);
        recoveryComplete_ = true;
    }
    recoveryCv_.notify_all();
}

void startRecovery() {
    if (opts_.recoveryMode == VLogRecoveryMode::BACKGROUND) {
        recoveryThread_ = std::thread([this] { runRecovery(); });
        return;
    }
    runRecovery();
    waitForRecovery();
}

void waitForRecovery() const {
    std::unique_lock lock{recoveryMu_};
    recoveryCv_.wait(lock, [this] { return recoveryComplete_; });
    const auto error = recoveryError_;
    lock.unlock();
    if (error) { std::rethrow_exception(error); }
}

void joinRecoveryWorker() { if (recoveryThread_.joinable()) { recoveryThread_.join(); } }

void openOrCreate() {
    const auto& path = logPath_;
    const auto parent = path.parent_path();
    if (!parent.empty()) { fs::create_directories(parent); }

    auto segments = discoverSegments();
    const SegmentInfo active = segments.empty() ? SegmentInfo{0, path} : segments.back();
    const bool activeEmpty = !fs::exists(active.path) || fs::file_size(active.path) == 0;
    #ifdef _WIN32
    file_ = _wfopen(active.path.wstring().c_str(), L"ab");
    #else
    file_ = fopen(active.path.string().c_str(), "ab");
    #endif
    if (!file_) { throw std::runtime_error("VersionLog: cannot open active segment: " + active.path.string()); }

    if (activeEmpty) {
        writeFileHeader(file_);
        activeSegmentId_ = active.id;
        activeSegmentBytes_ = FILE_HDR_SIZE;
    }
    else {
        activeSegmentId_ = active.id;
        activeSegmentBytes_ = static_cast<uint64_t>(fs::file_size(active.path));
    }
    try { startRecovery(); }
    catch (...) {
        fclose(file_);
        file_ = nullptr;
        throw;
    }
    if (!usesTrueParallelWrites()) { startAsyncWorkerIfNeeded(); }
}

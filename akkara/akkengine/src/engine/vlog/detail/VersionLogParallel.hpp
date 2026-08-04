/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/vlog/detail/VersionLogParallel.hpp
void rotateParallelLaneIfNeeded(ParallelLane& lane) {
    if (opts_.segmentBytes == 0 || lane.bytes < opts_.segmentBytes) { return; }
    const uint64_t oldId = lane.segmentId;
    const auto oldPath = segmentPath(oldId);
    flushChecked(lane.file);
    fdatasyncChecked(lane.file);
    writeTailFile(oldPath, lane.bytes, true);
    tryWriteSegmentIndex(oldPath, lane.bytes, lane.index);
    fclose(lane.file);
    lane.file = nullptr;

    std::lock_guard writeLock{writeMu_};
    parallelActiveSegmentIds_.erase(oldId);
    addParallelLaneSegmentLocked(lane);
    if (!retentionCompacting_ && retentionEnabled()) { retentionPrunePending_.store(true, std::memory_order_release); }
}

void persistParallelLocked(ParallelLane& lane, PendingWrite write) {
    if (!lane.file) { throw std::runtime_error("VersionLog: parallel lane is closed"); }
    write.offset = lane.bytes;
    writeSerialized(lane.file, write.bytes);
    flushChecked(lane.file);
    const bool sync = opts_.syncMode != VLogSyncMode::ASYNC;
    if (sync) { fdatasyncChecked(lane.file); }
    lane.bytes += static_cast<uint64_t>(write.bytes.size());
    auto& versions = lane.index[write.key];
    const auto pos = std::upper_bound(
        versions.begin(),
        versions.end(),
        IndexVersion{write.seq, write.offset},
        [](const IndexVersion& left, const IndexVersion& right) {
            return left.seq == right.seq ? left.offset < right.offset : left.seq < right.seq;
        }
    );
    versions.insert(pos, IndexVersion{write.seq, write.offset});
    const auto path = segmentPath(lane.segmentId);
    // An ASYNC append can be visible in this process before it is
    // durable. Do not advance its recovery boundary until a later
    // force/rotation/close has synced the segment bytes first.
    if (sync) { writeTailFile(path, lane.bytes, true); }
    {
        std::lock_guard writeLock{writeMu_};
        std::unique_lock segmentLock{segmentMu_};
        const auto it = std::find_if(
            segments_.begin(),
            segments_.end(),
            [&](const SegmentInfo& segment) { return segment.id == lane.segmentId; }
        );
        if (it == segments_.end()) { throw std::runtime_error("VersionLog: parallel active segment is missing"); }
        if (!it->hasEntries) {
            it->firstSeq = write.seq;
            it->lastSeq = write.seq;
            it->hasEntries = true;
        }
        else {
            it->firstSeq = std::min(it->firstSeq, write.seq);
            it->lastSeq = std::max(it->lastSeq, write.seq);
        }
        it->bytes = lane.bytes;
        ++it->entryCount;
        if ((write.flags & VLOG_FLAG_ROLLBACK) != 0) { ++it->rollbackCount; }
        durableBytes_ += static_cast<uint64_t>(write.bytes.size());
        activeSegmentId_ = lane.segmentId;
        activeSegmentBytes_ = lane.bytes;
        indexedEntries_.fetch_add(1, std::memory_order_relaxed);
        if ((write.flags & VLOG_FLAG_ROLLBACK) != 0) { rollbackEntries_.fetch_add(1, std::memory_order_relaxed); }
        persistedGeneration_.fetch_add(1, std::memory_order_release);
    }
    rotateParallelLaneIfNeeded(lane);
}

void parallelFlushLoop(ParallelLane& lane) {
    while (true) {
        PendingWrite write;
        try {
            {
                std::unique_lock laneLock{lane.mutex};
                lane.queueCv.wait(laneLock, [&] { return lane.closing || !lane.pendingWrites.empty(); });
                if (lane.pendingWrites.empty()) {
                    if (lane.closing) { return; }
                    continue;
                }
                write = std::move(lane.pendingWrites.front());
                lane.pendingWrites.pop_front();
                lane.pendingBytes -= static_cast<uint64_t>(write.bytes.size());
                if (opts_.parallelPendingLimitScope == VLogParallelPendingLimitScope::GLOBAL) {
                    std::lock_guard pendingLock{parallelQueueMu_};
                    parallelPendingBytes_ -= static_cast<uint64_t>(write.bytes.size());
                }
                persistParallelLocked(lane, write);
            }
            completeAppend(write.completion);
            removeResidents(std::deque<PendingWrite>{write});
            runRequestedRetentionPrune();
        }
        catch (...) {
            const auto error = std::current_exception();
            completeAppend(write.completion, error);
            {
                std::lock_guard writeLock{writeMu_};
                recordAsyncError(error);
            }
            {
                std::lock_guard laneLock{lane.mutex};
                lane.closing = true;
                for (const auto& pending : lane.pendingWrites) { completeAppend(pending.completion, error); }
                const uint64_t discardedBytes = lane.pendingBytes;
                lane.pendingWrites.clear();
                lane.pendingBytes = 0;
                if (opts_.parallelPendingLimitScope == VLogParallelPendingLimitScope::GLOBAL) {
                    std::lock_guard pendingLock{parallelQueueMu_};
                    parallelPendingBytes_ -= discardedBytes;
                }
            }
            lane.queueCv.notify_all();
            return;
        }
    }
}

void startParallelWorkers() {
    for (const auto& lane : parallelLanes_) {
        std::lock_guard laneLock{lane->mutex};
        if (lane->worker.joinable()) { continue; }
        lane->closing = false;
        lane->worker = std::thread([this, lanePtr = lane.get()] { parallelFlushLoop(*lanePtr); });
    }
}

void stopParallelWorkers() {
    for (const auto& lane : parallelLanes_) {
        {
            std::lock_guard laneLock{lane->mutex};
            lane->closing = true;
        }
        lane->queueCv.notify_all();
    }
    for (const auto& lane : parallelLanes_) { if (lane->worker.joinable()) { lane->worker.join(); } }
}

void appendParallel(PendingWrite write) {
    if (parallelLanes_.empty()) { throw std::runtime_error("VersionLog: parallel lanes are unavailable"); }
    checkAsyncError();
    const uint64_t fingerprint = write.key.empty()
                                     ? 0ULL
                                     : core::computeKeyFp64(reinterpret_cast<const uint8_t*>(write.key.data()), write.key.size());
    auto& lane = *parallelLanes_[fingerprint % parallelLanes_.size()];
    const uint64_t writeBytes = static_cast<uint64_t>(write.bytes.size());
    std::lock_guard laneLock{lane.mutex};
    if (lane.closing) { throw std::runtime_error("VersionLog: parallel lane is stopping"); }
    if (opts_.parallelPendingLimitScope == VLogParallelPendingLimitScope::GLOBAL) {
        std::lock_guard pendingLock{parallelQueueMu_};
        if (parallelPendingBytes_ != 0 && parallelPendingBytes_ + writeBytes > opts_.asyncMaxPendingBytes) {
            parallelQueueRejects_.fetch_add(1, std::memory_order_relaxed);
            throw std::runtime_error("VersionLog: global parallel queue is full");
        }
        lane.pendingWrites.push_back(std::move(write));
        lane.pendingBytes += writeBytes;
        parallelPendingBytes_ += writeBytes;
    }
    else {
        if (!lane.pendingWrites.empty() && lane.pendingBytes + writeBytes > opts_.asyncMaxPendingBytes) {
            parallelQueueRejects_.fetch_add(1, std::memory_order_relaxed);
            throw std::runtime_error("VersionLog: parallel lane queue is full");
        }
        lane.pendingWrites.push_back(std::move(write));
        lane.pendingBytes += writeBytes;
    }
    lane.queueCv.notify_one();
}

void closeParallelLanes() {
    for (const auto& lane : parallelLanes_) {
        std::lock_guard laneLock{lane->mutex};
        if (!lane->file) { continue; }
        const auto path = segmentPath(lane->segmentId);
        flushChecked(lane->file);
        fdatasyncChecked(lane->file);
        writeTailFile(path, lane->bytes, true);
        tryWriteSegmentIndex(path, lane->bytes, lane->index);
        fclose(lane->file);
        lane->file = nullptr;
    }
    std::lock_guard writeLock{writeMu_};
    parallelActiveSegmentIds_.clear();
}

void forceSyncParallelLanes() {
    for (const auto& lane : parallelLanes_) {
        std::lock_guard laneLock{lane->mutex};
        if (!lane->file) { continue; }
        flushChecked(lane->file);
        fdatasyncChecked(lane->file);
        writeTailFile(segmentPath(lane->segmentId), lane->bytes, true);
    }
}

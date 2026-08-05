/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/vlog/detail/VersionLogRetention.hpp
using RetentionStateMap = std::unordered_map<std::string, RetentionBaseState, StringViewHash, std::equal_to<>>;

[[nodiscard]] static std::unordered_set<uint64_t> retentionSegmentIds(const std::vector<SegmentInfo>& segments) {
    std::unordered_set<uint64_t> ids;
    ids.reserve(segments.size());
    for (const auto& segment : segments) { ids.insert(segment.id); }
    return ids;
}

// scanMu_ must be held shared by the caller. The generation check at
// commit guarantees no persisted entry was added while this snapshot
// was being built.
[[nodiscard]] RetentionStateMap buildRetentionBaseStates(const std::vector<SegmentInfo>& segments, uint64_t baseSeq) const {
    RetentionStateMap states;
    for (const auto& segment : segments) {
        if (segment.hasEntries && segment.firstSeq > baseSeq) { continue; }
        (void)scanSegment(
            segment.path,
            false,
            [&](std::string_view key, const AkvlogV5EntryHeader& header, std::span<const uint8_t> value) {
                if (header.seq > baseSeq) { return; }
                const auto it = states.find(key);
                if (it != states.end() && it->second.entry.seq >= header.seq) { return; }
                VersionEntry entry;
                entry.seq = header.seq;
                entry.sourceNodeId = header.sourceNodeId;
                entry.timestampNs = header.timestampNs;
                entry.flags = header.flags;
                entry.value.assign(value.begin(), value.end());
                states[std::string{key}] = RetentionBaseState{std::move(entry), segment.id};
            }
        );
    }
    return states;
}

// writeMu_ and scanMu_ must be held exclusively by the caller.
void persistRetentionBasesLocked(const RetentionStateMap& states, const std::unordered_set<uint64_t>& expiredIds, uint64_t baseSeq) {
    retentionCompacting_ = true;
    uint64_t baseEntriesWritten = 0;
    try {
        if (usesTrueParallelWrites()) {
            ParallelLane base;
            addParallelLaneSegmentLocked(base);
            for (const auto& [key, state] : states) {
                if (!expiredIds.contains(state.segmentId)) { continue; }
                const uint8_t flags = static_cast<uint8_t>(state.entry.flags | VLOG_FLAG_RETENTION_BASE);
                PendingWrite write;
                write.bytes = serializeEntry(
                    reinterpret_cast<const uint8_t*>(key.data()),
                    key.size(),
                    baseSeq,
                    state.entry.sourceNodeId,
                    state.entry.timestampNs,
                    flags,
                    state.entry.value.data(),
                    state.entry.value.size()
                );
                write.key = key;
                write.seq = baseSeq;
                write.flags = flags;
                write.offset = base.bytes;
                writeSerialized(base.file, write.bytes);
                base.bytes += static_cast<uint64_t>(write.bytes.size());
                base.index[key].push_back(IndexVersion{baseSeq, write.offset});
                knownWrittenBytes_ += static_cast<uint64_t>(write.bytes.size());
                indexedEntries_.fetch_add(1, std::memory_order_relaxed);
                if ((flags & VLOG_FLAG_ROLLBACK) != 0) { rollbackEntries_.fetch_add(1, std::memory_order_relaxed); }
                {
                    std::unique_lock segmentLock{segmentMu_};
                    const auto it = std::find_if(
                        segments_.begin(),
                        segments_.end(),
                        [&](const SegmentInfo& segment) { return segment.id == base.segmentId; }
                    );
                    if (it == segments_.end()) { throw std::runtime_error("VersionLog: retention base segment is missing"); }
                    if (!it->hasEntries) {
                        it->firstSeq = baseSeq;
                        it->lastSeq = baseSeq;
                        it->hasEntries = true;
                    }
                    it->bytes = base.bytes;
                    ++it->entryCount;
                    if ((flags & VLOG_FLAG_ROLLBACK) != 0) { ++it->rollbackCount; }
                }
                ++baseEntriesWritten;
            }
            flushChecked(base.file);
            fdatasyncChecked(base.file);
            const auto basePath = segmentPath(base.segmentId);
            writeTailFile(basePath, base.bytes, true);
            tryWriteSegmentIndex(basePath, base.bytes, base.index);
            FILE* baseFile = base.file;
            base.file = nullptr;
            closeChecked(baseFile);
            parallelActiveSegmentIds_.erase(base.segmentId);
            persistedGeneration_.fetch_add(1, std::memory_order_release);
            retentionBaseEntriesWritten_.fetch_add(baseEntriesWritten, std::memory_order_relaxed);
            retentionCompacting_ = false;
            return;
        }
        for (const auto& [key, state] : states) {
            if (!expiredIds.contains(state.segmentId)) { continue; }
            if (!file_) { throw std::runtime_error("VersionLog: active segment is unavailable during retention compaction"); }

            const uint8_t flags = static_cast<uint8_t>(state.entry.flags | VLOG_FLAG_RETENTION_BASE);
            PendingWrite write;
            write.bytes = serializeEntry(
                reinterpret_cast<const uint8_t*>(key.data()),
                key.size(),
                baseSeq,
                state.entry.sourceNodeId,
                state.entry.timestampNs,
                flags,
                state.entry.value.data(),
                state.entry.value.size()
            );
            write.key = key;
            write.seq = baseSeq;
            write.flags = flags;
            write.offset = activeSegmentBytes_;
            writeSerialized(file_, write.bytes);
            knownWrittenBytes_ += static_cast<uint64_t>(write.bytes.size());
            notePersistedLocked(write);
            trackEntryStats(flags);
            rotateSegmentIfNeededLocked();
            ++baseEntriesWritten;
        }
        if (file_) {
            flushChecked(file_);
            fdatasyncChecked(file_);
        }
        retentionBaseEntriesWritten_.fetch_add(baseEntriesWritten, std::memory_order_relaxed);
        retentionCompacting_ = false;
    }
    catch (...) {
        retentionCompacting_ = false;
        throw;
    }
}

// writeMu_ and scanMu_ must be held exclusively by the caller.
void deleteRetentionSegmentsLocked(const std::unordered_set<uint64_t>& expiredIds) {
    std::unique_lock segmentLock{segmentMu_};
    std::vector<SegmentInfo> retained;
    retained.reserve(segments_.size());
    uint64_t prunedSegments = 0;
    for (const auto& segment : segments_) {
        if (!expiredIds.contains(segment.id)) {
            retained.push_back(segment);
            continue;
        }
        std::error_code error;
        if (!fs::remove(segment.path, error) || error) {
            retained.push_back(segment);
            continue;
        }
        std::error_code ignored;
        fs::remove(segmentIndexPath(segment.path), ignored);
        fs::remove(segmentTailPath(segment.path), ignored);
        forgetIndexPayloadValidation(segmentIndexPath(segment.path));
        knownWrittenBytes_ = segment.bytes > knownWrittenBytes_ ? 0 : knownWrittenBytes_ - segment.bytes;
        indexedEntries_.fetch_sub(segment.entryCount, std::memory_order_relaxed);
        rollbackEntries_.fetch_sub(segment.rollbackCount, std::memory_order_relaxed);
        ++prunedSegments;
    }
    segments_.swap(retained);
    retentionPrunedSegments_.fetch_add(prunedSegments, std::memory_order_relaxed);
}

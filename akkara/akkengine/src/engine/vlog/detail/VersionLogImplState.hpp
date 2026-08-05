/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/vlog/detail/VersionLogImplState.hpp
struct AppendCompletion;
fs::path logPath_;
VersionLogOptions opts_;
mutable std::mutex writeMu_;
mutable std::mutex serialAdmissionMu_;
std::shared_ptr<AppendCompletion> lastSerialAppendCompletion_;
mutable std::mutex parallelQueueMu_;
uint64_t parallelPendingBytes_ = 0;
// Readers share this lock only while copying unflushed records.
mutable std::shared_mutex residentMu_;
// Compact directory of immutable/active log segments. It scales with
// the number of files, not historical entries.
mutable std::shared_mutex segmentMu_;
// Readers retain this shared lock for the duration of a file scan so
// retention cannot remove a segment after it has been selected.
mutable std::shared_mutex scanMu_;
// The mutable segment keeps only key-to-offset metadata in memory.
// Closed segments release it after their immutable sidecar is written.
mutable std::shared_mutex activeIndexMu_;
mutable std::mutex indexValidationMu_;
// Serializes retention planning/commit without making normal reads
// or writes wait for the expensive state reconstruction scan.
mutable std::mutex retentionMu_;
std::condition_variable flushCv_;
std::condition_variable queueSpaceCv_;
mutable std::mutex asyncErrorMu_;
mutable std::mutex recoveryMu_;
mutable std::condition_variable recoveryCv_;

struct StringViewHash {
    using is_transparent = void;
    size_t operator()(std::string_view sv) const noexcept { return std::hash<std::string_view>{}(sv); }
    size_t operator()(const std::string& s) const noexcept { return std::hash<std::string_view>{}(s); }
};

struct PendingWrite {
    std::vector<uint8_t> bytes;
    std::string key;
    uint64_t seq = 0;
    uint8_t flags = 0;
    uint64_t offset = 0;
    std::shared_ptr<struct AppendCompletion> completion;
};

struct AppendCompletion {
    std::mutex mutex;
    std::condition_variable cv;
    bool complete = false;
    std::exception_ptr error;
};

struct IndexVersion {
    uint64_t seq = 0;
    uint64_t offset = 0;
};

struct RetentionBaseState {
    VersionEntry entry;
    uint64_t segmentId = 0;
};

struct ValidatedIndexPayload {
    uint32_t crc32c = 0;
    uint64_t bytes = 0;
    fs::file_time_type modified{};
};

using SegmentKeyIndex = std::unordered_map<std::string, std::vector<IndexVersion>, StringViewHash, std::equal_to<>>;

struct ParallelLane {
    std::mutex mutex;
    std::condition_variable queueCv;
    FILE* file = nullptr;
    uint64_t segmentId = 0;
    uint64_t bytes = 0;
    SegmentKeyIndex index;
    std::deque<PendingWrite> pendingWrites;
    uint64_t pendingBytes = 0;
    bool closing = false;
    std::thread worker;
};

struct SegmentInfo {
    uint64_t id = 0;
    fs::path path;
    uint64_t firstSeq = 0;
    uint64_t lastSeq = 0;
    uint64_t bytes = 0;
    uint64_t entryCount = 0;
    uint64_t rollbackCount = 0;
    bool hasEntries = false;
};

// Only entries that have not reached the log file live here. Historical
// reads scan the file on demand after taking this small overlay snapshot.
std::unordered_map<std::string, std::vector<VersionEntry>, StringViewHash, std::equal_to<>> residentIndex_;
SegmentKeyIndex activeSegmentIndex_;
std::vector<std::unique_ptr<ParallelLane>> parallelLanes_;
std::unordered_set<uint64_t> parallelActiveSegmentIds_;
uint64_t nextParallelSegmentId_ = 1;
mutable std::unordered_map<std::string, ValidatedIndexPayload> validatedIndexPayloads_;
uint64_t activeIndexSegmentId_ = 0;
FILE* file_ = nullptr;
std::deque<PendingWrite> pendingWrites_;
std::thread flushThread_;
std::thread recoveryThread_;
std::exception_ptr asyncError_;
std::atomic<bool> asyncFailed_{false};
std::atomic<bool> retentionPrunePending_{false};
std::atomic<uint64_t> persistedGeneration_{0};
std::atomic<uint64_t> recoveryDurationMicros_{0};
std::atomic<uint64_t> recoveredSegmentCount_{0};
std::atomic<uint64_t> recoveredEntryCount_{0};
mutable std::atomic<uint64_t> sidecarFallbackCount_{0};
mutable std::atomic<uint64_t> sidecarRebuildFailures_{0};
std::atomic<uint64_t> retentionPrunedSegments_{0};
std::atomic<uint64_t> retentionBaseEntriesWritten_{0};
std::atomic<uint64_t> parallelQueueRejects_{0};
bool closing_ = false;
bool retentionCompacting_ = false;
bool recoveryComplete_ = false;
std::exception_ptr recoveryError_;
std::atomic<uint64_t> committedSeq_{0};
std::mutex commitMu_;
std::unordered_set<uint64_t> completedSeqs_;
uint64_t recoveredMaxSeq_ = 0;
uint64_t activeSegmentId_ = 0;
uint64_t activeSegmentBytes_ = 0;
std::vector<SegmentInfo> segments_;
uint64_t pendingBytes_ = 0;
std::atomic<uint64_t> indexedEntries_{0};
std::atomic<uint64_t> rollbackEntries_{0};
uint64_t knownWrittenBytes_ = 0;

void trackEntryStats(uint8_t flags) noexcept {
    indexedEntries_.fetch_add(1, std::memory_order_relaxed);
    if ((flags & VLOG_FLAG_ROLLBACK) != 0) { rollbackEntries_.fetch_add(1, std::memory_order_relaxed); }
}

[[nodiscard]] bool usesZstd() const noexcept { return opts_.codec == VLogCodec::ZSTD; }
[[nodiscard]] bool usesTrueParallelWrites() const noexcept { return opts_.writeAdmission == VLogWriteAdmissionMode::PARALLEL; }

void validateOptions() const {
    if (opts_.writeAdmission != VLogWriteAdmissionMode::SERIAL && opts_.writeAdmission != VLogWriteAdmissionMode::PREPARE_PARALLEL && opts_.
        writeAdmission != VLogWriteAdmissionMode::PARALLEL) { throw std::invalid_argument("VersionLog: unsupported write admission mode"); }
    if (opts_.serialAppendMode != VLogSerialAppendMode::PIPELINED && opts_.serialAppendMode != VLogSerialAppendMode::WAIT_PREVIOUS_APPEND) {
        throw std::invalid_argument("VersionLog: unsupported serial append mode");
    }
    if (opts_.parallelPendingLimitScope != VLogParallelPendingLimitScope::PER_LANE && opts_.parallelPendingLimitScope !=
        VLogParallelPendingLimitScope::GLOBAL) { throw std::invalid_argument("VersionLog: unsupported parallel pending limit scope"); }
    if (opts_.parallelWriteLanes > 64) { throw std::invalid_argument("VersionLog: parallelWriteLanes exceeds the supported maximum"); }
    if (usesTrueParallelWrites() && opts_.syncMode != VLogSyncMode::ASYNC) {
        throw std::invalid_argument("VersionLog: PARALLEL write admission requires syncMode=ASYNC");
    }
    if (opts_.codec != VLogCodec::NONE && opts_.codec != VLogCodec::ZSTD) {
        throw std::invalid_argument("VersionLog: unsupported value codec");
    }
    if (usesZstd() && (opts_.zstdCompressionLevel < ZSTD_minCLevel() || opts_.zstdCompressionLevel > ZSTD_maxCLevel())) {
        throw std::invalid_argument("VersionLog: zstdCompressionLevel is outside the supported Zstd range");
    }
}

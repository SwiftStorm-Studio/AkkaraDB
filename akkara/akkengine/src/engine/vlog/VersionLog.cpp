/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/vlog/VersionLog.cpp
#include "akk/engine/vlog/VersionLog.hpp"

#include "akk/cpu/CRC32C.hpp"
#include "akk/core/record/KeyFingerprint.hpp"

#include <zstd.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <exception>
#include <iterator>
#include <chrono>
#include <limits>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif

namespace akkaradb::engine::vlog {
    namespace fs = std::filesystem;

    namespace {
        static constexpr uint32_t AKVLOG_V5_MAGIC = 0x35564B41u; // "AKV5"
        static constexpr uint16_t AKVLOG_V5_VERSION = 0x0001u;
        static constexpr uint32_t AKVLOG_INDEX_MAGIC = 0x49564B41u; // "AKVI"
        static constexpr uint16_t AKVLOG_INDEX_VERSION = 0x0002u;
        static constexpr uint32_t AKVLOG_TAIL_MAGIC = 0x54564B41u; // "AKVT"
        static constexpr uint16_t AKVLOG_TAIL_VERSION = 0x0001u;
        static constexpr uint32_t INDEX_BLOOM_BITS_PER_KEY = 10;
        static constexpr uint32_t INDEX_BLOOM_HASHES = 7;

        #pragma pack(push, 1)
        struct AkvlogV5FileHeader {
            uint32_t magic;
            uint16_t version;
            uint8_t syncModeHint;
            uint8_t reserved0;
            uint64_t createdNs;
            uint64_t reserved1;
            uint32_t crc32c;
            uint32_t reserved2;
        };
        #pragma pack(pop)

        #pragma pack(push, 1)
        struct AkvlogV5EntryHeader {
            uint32_t entryLen;
            uint64_t seq;
            uint64_t sourceNodeId;
            uint64_t timestampNs;
            uint8_t flags;
            uint64_t keyFp64;
            uint16_t keyLen;
            uint32_t valueLen;
        };
        #pragma pack(pop)

        #pragma pack(push, 1)
        struct AkvlogIndexFileHeader {
            uint32_t magic;
            uint16_t version;
            uint16_t bloomHashCount;
            uint64_t logBytes;
            uint64_t keyCount;
            uint64_t versionCount;
            uint32_t bloomBitCount;
            uint32_t payloadCrc32c;
            uint32_t crc32c;
        };
        #pragma pack(pop)

        #pragma pack(push, 1)
        struct AkvlogIndexKeyRecord {
            uint64_t keyFp64;
            uint64_t firstVersion;
            uint32_t versionCount;
            uint32_t reserved;
        };
        #pragma pack(pop)

        #pragma pack(push, 1)
        struct AkvlogIndexVersionRecord {
            uint64_t seq;
            uint64_t offset;
        };
        #pragma pack(pop)

        #pragma pack(push, 1)
        struct AkvlogTailFile {
            uint32_t magic;
            uint16_t version;
            uint16_t reserved0;
            uint64_t committedBytes;
            uint32_t crc32c;
            uint32_t reserved1;
        };
        #pragma pack(pop)

        static_assert(sizeof(AkvlogV5FileHeader) == 32);
        static_assert(sizeof(AkvlogV5EntryHeader) == 43);
        static_assert(sizeof(AkvlogIndexFileHeader) == 44);
        static_assert(sizeof(AkvlogIndexKeyRecord) == 24);
        static_assert(sizeof(AkvlogIndexVersionRecord) == 16);
        static_assert(sizeof(AkvlogTailFile) == 24);

        static constexpr size_t ENTRY_HDR_SIZE = sizeof(AkvlogV5EntryHeader);
        static constexpr size_t CRC_SIZE = sizeof(uint32_t);
        static constexpr size_t MIN_ENTRY_SIZE = ENTRY_HDR_SIZE + CRC_SIZE;
        static constexpr size_t MAX_ENTRY_SIZE = 32u * 1024u * 1024u;
        static constexpr size_t ZSTD_VALUE_PREFIX_SIZE = sizeof(uint32_t);

        [[nodiscard]] static uint64_t nowNsFallback() noexcept { return 0; }

        static void doFdatasync(FILE* f) {
            #ifdef _WIN32
            _commit(_fileno(f));
            #else
            fdatasync(fileno(f));
            #endif
        }

        [[nodiscard]] static uint64_t fileOffset(FILE* file) {
            #ifdef _WIN32
            const auto offset = _ftelli64(file);
            #else
            const auto offset = ftello(file);
            #endif
            if (offset < 0) { throw std::runtime_error("VersionLog: cannot determine file offset"); }
            return static_cast<uint64_t>(offset);
        }

        static void seekFile(FILE* file, uint64_t offset) {
            #ifdef _WIN32
            if (_fseeki64(file, static_cast<__int64>(offset), SEEK_SET) != 0) {
            #else
            if (fseeko(file, static_cast<off_t>(offset), SEEK_SET) != 0) {
                #endif
                throw std::runtime_error("VersionLog: failed to seek file");
            }
        }

        [[nodiscard]] static std::string fileContext(
            const fs::path& path,
            std::optional<uint64_t> offset = std::nullopt,
            std::optional<uint64_t> seq = std::nullopt
        ) {
            std::string out = " path=" + path.string();
            if (offset.has_value()) { out += " offset=" + std::to_string(*offset); }
            if (seq.has_value()) { out += " seq=" + std::to_string(*seq); }
            return out;
        }

        [[noreturn]] static void throwVLogError(
            const std::string& message,
            const fs::path& path,
            std::optional<uint64_t> offset = std::nullopt,
            std::optional<uint64_t> seq = std::nullopt
        ) {
            throw std::runtime_error("VersionLog: " + message + fileContext(path, offset, seq));
        }

        [[nodiscard]] static uint64_t mixIndexHash(uint64_t value) noexcept {
            value ^= value >> 30u;
            value *= 0xbf58476d1ce4e5b9ULL;
            value ^= value >> 27u;
            value *= 0x94d049bb133111ebULL;
            return value ^ (value >> 31u);
        }
    } // namespace

    class VersionLog::Impl {
        public:
            struct AppendCompletion;
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
            uint64_t durableBytes_ = 0;

            void trackEntryStats(uint8_t flags) noexcept {
                indexedEntries_.fetch_add(1, std::memory_order_relaxed);
                if ((flags & VLOG_FLAG_ROLLBACK) != 0) { rollbackEntries_.fetch_add(1, std::memory_order_relaxed); }
            }

            [[nodiscard]] bool usesZstd() const noexcept { return opts_.codec == VLogCodec::ZSTD; }
            [[nodiscard]] bool usesTrueParallelWrites() const noexcept { return opts_.writeAdmission == VLogWriteAdmissionMode::PARALLEL; }

            void validateOptions() const {
                if (opts_.writeAdmission != VLogWriteAdmissionMode::SERIAL && opts_.writeAdmission !=
                    VLogWriteAdmissionMode::PREPARE_PARALLEL && opts_.writeAdmission != VLogWriteAdmissionMode::PARALLEL) {
                    throw std::invalid_argument("VersionLog: unsupported write admission mode");
                }
                if (opts_.serialAppendMode != VLogSerialAppendMode::PIPELINED && opts_.serialAppendMode !=
                    VLogSerialAppendMode::WAIT_PREVIOUS_APPEND) {
                    throw std::invalid_argument("VersionLog: unsupported serial append mode");
                }
                if (opts_.parallelPendingLimitScope != VLogParallelPendingLimitScope::PER_LANE && opts_.parallelPendingLimitScope !=
                    VLogParallelPendingLimitScope::GLOBAL) {
                    throw std::invalid_argument("VersionLog: unsupported parallel pending limit scope");
                }
                if (opts_.parallelWriteLanes > 64) {
                    throw std::invalid_argument("VersionLog: parallelWriteLanes exceeds the supported maximum");
                }
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

            static void completeAppend(const std::shared_ptr<AppendCompletion>& completion, std::exception_ptr error = {}) {
                if (!completion) { return; }
                {
                    std::lock_guard lock{completion->mutex};
                    completion->error = std::move(error);
                    completion->complete = true;
                }
                completion->cv.notify_all();
            }

            static void waitForAppend(const std::shared_ptr<AppendCompletion>& completion) {
                if (!completion) { return; }
                std::unique_lock lock{completion->mutex};
                completion->cv.wait(lock, [&] { return completion->complete; });
                if (completion->error) { std::rethrow_exception(completion->error); }
            }

            [[nodiscard]] std::vector<uint8_t> serializeEntry(
                const uint8_t* keyData,
                size_t keyLen,
                uint64_t seq,
                uint64_t sourceNodeId,
                uint64_t timestampNs,
                uint8_t flags,
                const uint8_t* valueData,
                size_t valueLen
            ) {
                if (keyLen > std::numeric_limits<uint16_t>::max()) { throw std::invalid_argument("VersionLog: key too large"); }
                if (valueLen > std::numeric_limits<uint32_t>::max()) { throw std::invalid_argument("VersionLog: value too large"); }
                if (ENTRY_HDR_SIZE + keyLen + valueLen + CRC_SIZE > MAX_ENTRY_SIZE) {
                    throw std::invalid_argument("VersionLog: entry too large");
                }

                flags &= static_cast<uint8_t>(~VLOG_FLAG_ZSTD);
                std::vector<uint8_t> encodedValue;
                if (usesZstd() && valueLen > 0) {
                    const size_t bound = ZSTD_compressBound(valueLen);
                    encodedValue.resize(ZSTD_VALUE_PREFIX_SIZE + bound);
                    const size_t compressedSize = ZSTD_compress(
                        encodedValue.data() + ZSTD_VALUE_PREFIX_SIZE,
                        bound,
                        valueData,
                        valueLen,
                        opts_.zstdCompressionLevel
                    );
                    if (!ZSTD_isError(compressedSize) && compressedSize + ZSTD_VALUE_PREFIX_SIZE < valueLen) {
                        const uint32_t rawSize = static_cast<uint32_t>(valueLen);
                        std::memcpy(encodedValue.data(), &rawSize, sizeof(rawSize));
                        encodedValue.resize(ZSTD_VALUE_PREFIX_SIZE + compressedSize);
                        flags |= VLOG_FLAG_ZSTD;
                    }
                    else { encodedValue.clear(); }
                }

                const uint8_t* storedValue = encodedValue.empty() ? valueData : encodedValue.data();
                const size_t storedValueLen = encodedValue.empty() ? valueLen : encodedValue.size();
                const size_t total = ENTRY_HDR_SIZE + keyLen + storedValueLen + CRC_SIZE;
                if (total > MAX_ENTRY_SIZE) { throw std::invalid_argument("VersionLog: entry too large"); }

                const uint32_t entryLen = static_cast<uint32_t>(total);
                std::vector<uint8_t> out(entryLen);
                uint8_t* p = out.data();

                auto& hdr = *reinterpret_cast<AkvlogV5EntryHeader*>(p);
                hdr.entryLen = entryLen;
                hdr.seq = seq;
                hdr.sourceNodeId = sourceNodeId;
                hdr.timestampNs = timestampNs;
                hdr.flags = flags;
                hdr.keyFp64 = keyLen == 0 ? 0ULL : core::computeKeyFp64(keyData, keyLen);
                hdr.keyLen = static_cast<uint16_t>(keyLen);
                hdr.valueLen = static_cast<uint32_t>(storedValueLen);
                p += ENTRY_HDR_SIZE;

                if (keyLen > 0) {
                    std::memcpy(p, keyData, keyLen);
                    p += keyLen;
                }
                if (storedValueLen > 0) {
                    std::memcpy(p, storedValue, storedValueLen);
                    p += storedValueLen;
                }

                std::memset(p, 0, CRC_SIZE);
                const uint32_t crc = cpu::CRC32C(reinterpret_cast<const std::byte*>(out.data()), entryLen - CRC_SIZE);
                std::memcpy(p, &crc, CRC_SIZE);
                return out;
            }

            static void writeSerialized(FILE* f, std::span<const uint8_t> bytes) {
                if (fwrite(bytes.data(), 1, bytes.size(), f) != bytes.size()) { throw std::runtime_error("VersionLog: fwrite failed"); }
            }

            void checkAsyncError() const { if (asyncFailed_.load(std::memory_order_acquire)) { std::rethrow_exception(asyncError_); } }

            void recordAsyncError(std::exception_ptr error) noexcept {
                if (asyncFailed_.load(std::memory_order_relaxed)) { return; }
                asyncError_ = std::move(error);
                asyncFailed_.store(true, std::memory_order_release);
            }

            [[nodiscard]] uint64_t visibleSeq() const noexcept {
                return opts_.readVisibility == VLogReadVisibilityMode::COMMIT_ORDER
                           ? committedSeq_.load(std::memory_order_acquire)
                           : std::numeric_limits<uint64_t>::max();
            }

            void resetCommittedSeq(uint64_t seq) {
                std::lock_guard lock{commitMu_};
                completedSeqs_.clear();
                committedSeq_.store(seq, std::memory_order_release);
            }

            void markCommitted(uint64_t seq) {
                std::lock_guard lock{commitMu_};
                uint64_t current = committedSeq_.load(std::memory_order_relaxed);
                if (seq <= current) { return; }

                completedSeqs_.insert(seq);
                while (current != std::numeric_limits<uint64_t>::max() && completedSeqs_.erase(current + 1u) != 0) { ++current; }
                committedSeq_.store(current, std::memory_order_release);
            }

            void publishResident(const std::string& key, VersionEntry entry) {
                std::unique_lock lock{residentMu_};
                insertSorted(key, std::move(entry));
            }

            void removeResidents(const std::deque<PendingWrite>& writes) {
                std::unique_lock lock{residentMu_};
                for (const auto& write : writes) {
                    const auto it = residentIndex_.find(write.key);
                    if (it == residentIndex_.end()) { continue; }
                    auto& versions = it->second;
                    std::erase_if(versions, [&](const VersionEntry& entry) { return entry.seq == write.seq; });
                    if (versions.empty()) { residentIndex_.erase(it); }
                }
            }

            [[nodiscard]] std::vector<VersionEntry> residentForKey(std::string_view key) const {
                std::shared_lock lock{residentMu_};
                const auto it = residentIndex_.find(key);
                return it == residentIndex_.end() ? std::vector<VersionEntry>{} : it->second;
            }

            [[nodiscard]] std::vector<std::pair<std::string, std::vector<VersionEntry>>> residentSnapshot() const {
                std::shared_lock lock{residentMu_};
                std::vector<std::pair<std::string, std::vector<VersionEntry>>> out;
                out.reserve(residentIndex_.size());
                for (const auto& [key, versions] : residentIndex_) { out.emplace_back(key, versions); }
                return out;
            }

            void flushLoop() {
                std::deque<PendingWrite> batch;
                uint64_t batchStartOffset = 0;
                while (true) {
                    {
                        std::unique_lock lock{writeMu_};
                        flushCv_.wait(lock, [this] { return closing_ || !pendingWrites_.empty(); });
                        if (pendingWrites_.empty()) {
                            if (closing_) { break; }
                            continue;
                        }

                        if (!closing_) {
                            const auto maxWait = std::chrono::microseconds(opts_.groupMicros);
                            const auto deadline = std::chrono::steady_clock::now() + maxWait;
                            while (pendingWrites_.size() < opts_.groupN && pendingBytes_ < opts_.groupBytes && !closing_) {
                                if (opts_.groupMicros == 0) { break; }
                                if (flushCv_.wait_until(
                                    lock,
                                    deadline,
                                    [this] {
                                        return closing_ || pendingWrites_.size() >= opts_.groupN || pendingBytes_ >= opts_.groupBytes;
                                    }
                                )) { break; }
                                break;
                            }
                        }

                        batch.swap(pendingWrites_);
                        pendingBytes_ = 0;
                        batchStartOffset = activeSegmentBytes_;
                        queueSpaceCv_.notify_all();
                    }

                    try {
                        uint64_t batchBytes = 0;
                        uint64_t offset = batchStartOffset;
                        for (auto& entry : batch) {
                            entry.offset = offset;
                            writeSerialized(file_, entry.bytes);
                            offset += static_cast<uint64_t>(entry.bytes.size());
                        }
                        for (const auto& entry : batch) { batchBytes += static_cast<uint64_t>(entry.bytes.size()); }
                        fflush(file_);
                        if (opts_.syncMode == VLogSyncMode::BATCHED_SYNC) { doFdatasync(file_); }
                        {
                            std::lock_guard lock{writeMu_};
                            durableBytes_ += batchBytes;
                            for (const auto& entry : batch) { notePersistedLocked(entry); }
                            rotateSegmentIfNeededLocked();
                        }
                        for (const auto& entry : batch) { completeAppend(entry.completion); }
                        runRequestedRetentionPrune();
                        removeResidents(batch);
                    }
                    catch (...) {
                        std::lock_guard lock{writeMu_};
                        const auto error = std::current_exception();
                        for (const auto& entry : batch) { completeAppend(entry.completion, error); }
                        for (const auto& entry : pendingWrites_) { completeAppend(entry.completion, error); }
                        recordAsyncError(error);
                        closing_ = true;
                        pendingWrites_.clear();
                        pendingBytes_ = 0;
                        queueSpaceCv_.notify_all();
                        flushCv_.notify_all();
                        break;
                    }

                    batch.clear();
                }
            }

            void startAsyncWorkerIfNeeded() {
                if (opts_.syncMode == VLogSyncMode::SYNC || flushThread_.joinable()) { return; }
                closing_ = false;
                flushThread_ = std::thread([this] { flushLoop(); });
            }

            void stopAsyncWorker() {
                if (!flushThread_.joinable()) { return; }
                {
                    std::lock_guard lock{writeMu_};
                    closing_ = true;
                    flushCv_.notify_all();
                }
                flushThread_.join();
            }

            // Returns true when the record remains resident until the async flusher publishes it to disk.
            [[nodiscard]] bool persistSerialized(std::unique_lock<std::mutex>& lock, PendingWrite write) {
                checkAsyncError();
                if (!file_) { return false; }

                if (opts_.syncMode == VLogSyncMode::SYNC) {
                    write.offset = activeSegmentBytes_;
                    writeSerialized(file_, write.bytes);
                    fflush(file_);
                    doFdatasync(file_);
                    durableBytes_ += static_cast<uint64_t>(write.bytes.size());
                    notePersistedLocked(write);
                    rotateSegmentIfNeededLocked();
                    completeAppend(write.completion);
                    return false;
                }

                const uint64_t entryBytes = static_cast<uint64_t>(write.bytes.size());
                queueSpaceCv_.wait(
                    lock,
                    [&] {
                        return asyncFailed_.load(std::memory_order_acquire) || closing_ || pendingBytes_ + entryBytes <= opts_.
                            asyncMaxPendingBytes || pendingWrites_.empty();
                    }
                );
                checkAsyncError();
                if (closing_) { throw std::runtime_error("VersionLog: append rejected while flusher is stopping"); }
                pendingWrites_.push_back(std::move(write));
                pendingBytes_ += entryBytes;
                flushCv_.notify_one();
                return true;
            }

            static void writeFileHeaderRaw(FILE* wf, VLogSyncMode syncModeHint) {
                AkvlogV5FileHeader hdr{};
                hdr.magic = AKVLOG_V5_MAGIC;
                hdr.version = AKVLOG_V5_VERSION;
                hdr.syncModeHint = static_cast<uint8_t>(syncModeHint);
                hdr.reserved0 = 0;
                hdr.createdNs = nowNsFallback();
                hdr.reserved1 = 0;
                hdr.crc32c = 0;
                hdr.reserved2 = 0;
                hdr.crc32c = cpu::CRC32C(reinterpret_cast<const std::byte*>(&hdr), sizeof(hdr));

                if (fwrite(&hdr, sizeof(hdr), 1, wf) != 1) { throw std::runtime_error("VersionLog: failed to write header"); }
                fflush(wf);
                doFdatasync(wf);
            }

            void writeFileHeader(FILE* wf) {
                writeFileHeaderRaw(wf, opts_.syncMode);
                durableBytes_ += sizeof(AkvlogV5FileHeader);
            }

            [[nodiscard]] fs::path segmentPath(uint64_t id) const {
                if (id == 0) { return opts_.logPath; }
                const auto name = opts_.logPath.stem().string() + "-seg-" + std::to_string(id) + opts_.logPath.extension().string();
                return opts_.logPath.parent_path() / name;
            }

            [[nodiscard]] static fs::path segmentIndexPath(const fs::path& segmentPath) {
                auto indexPath = segmentPath;
                indexPath.replace_extension(".akvidx");
                return indexPath;
            }

            [[nodiscard]] static fs::path segmentTailPath(const fs::path& segmentPath) {
                auto tailPath = segmentPath;
                tailPath.replace_extension(".akvtail");
                return tailPath;
            }

            static void writeTailFile(const fs::path& segmentPath, uint64_t committedBytes, bool sync) {
                AkvlogTailFile tail{};
                tail.magic = AKVLOG_TAIL_MAGIC;
                tail.version = AKVLOG_TAIL_VERSION;
                tail.committedBytes = committedBytes;
                tail.crc32c = 0;
                tail.crc32c = cpu::CRC32C(reinterpret_cast<const std::byte*>(&tail), sizeof(tail));

                const auto path = segmentTailPath(segmentPath);
                auto temporary = path;
                temporary += ".tmp";
                FILE* file = nullptr;
                try {
                    #ifdef _WIN32
                    file = _wfopen(temporary.wstring().c_str(), L"wb");
                    #else
                    file = fopen(temporary.string().c_str(), "wb");
                    #endif
                    if (!file || fwrite(&tail, sizeof(tail), 1, file) != 1) { throwVLogError("failed to write durable tail", temporary); }
                    fflush(file);
                    if (sync) { doFdatasync(file); }
                    fclose(file);
                    file = nullptr;
                    std::error_code error;
                    fs::remove(path, error);
                    error.clear();
                    fs::rename(temporary, path, error);
                    if (error) { throwVLogError("failed to publish durable tail: " + error.message(), path); }
                }
                catch (...) {
                    if (file) { fclose(file); }
                    std::error_code ignored;
                    fs::remove(temporary, ignored);
                    throw;
                }
            }

            [[nodiscard]] static std::optional<uint64_t> readTailFile(const fs::path& segmentPath) {
                const auto path = segmentTailPath(segmentPath);
                std::error_code sizeError;
                const bool exists = fs::exists(path, sizeError);
                if (sizeError) { throwVLogError("cannot inspect durable tail: " + sizeError.message(), path); }
                if (!exists) { return std::nullopt; }
                const uint64_t bytes = fs::file_size(path, sizeError);
                if (sizeError) { throwVLogError("cannot stat durable tail: " + sizeError.message(), path); }
                if (bytes != sizeof(AkvlogTailFile)) { throwVLogError("invalid durable tail size", path, bytes); }
                #ifdef _WIN32
                FILE* file = _wfopen(path.wstring().c_str(), L"rb");
                #else
                FILE* file = fopen(path.string().c_str(), "rb");
                #endif
                if (!file) { throwVLogError("cannot open durable tail", path); }
                AkvlogTailFile tail{};
                const bool valid = fread(&tail, sizeof(tail), 1, file) == 1;
                fclose(file);
                if (!valid) { throwVLogError("truncated durable tail", path); }
                const uint32_t stored = tail.crc32c;
                tail.crc32c = 0;
                if (tail.magic != AKVLOG_TAIL_MAGIC || tail.version != AKVLOG_TAIL_VERSION || stored != cpu::CRC32C(
                    reinterpret_cast<const std::byte*>(&tail),
                    sizeof(tail)
                )) { throwVLogError("corrupt durable tail", path, tail.committedBytes); }
                return tail.committedBytes;
            }

            void markIndexPayloadValidated(const fs::path& path, uint32_t payloadCrc32c) const {
                std::error_code error;
                const uint64_t bytes = fs::file_size(path, error);
                if (error) { return; }
                const auto modified = fs::last_write_time(path, error);
                if (error) { return; }
                std::lock_guard lock{indexValidationMu_};
                validatedIndexPayloads_[path.string()] = ValidatedIndexPayload{payloadCrc32c, bytes, modified};
            }

            void forgetIndexPayloadValidation(const fs::path& path) const {
                std::lock_guard lock{indexValidationMu_};
                validatedIndexPayloads_.erase(path.string());
            }

            [[nodiscard]] bool verifyIndexPayload(FILE* file, const fs::path& path, const AkvlogIndexFileHeader& header) const {
                std::error_code error;
                const uint64_t totalBytes = fs::file_size(path, error);
                if (error || totalBytes < sizeof(AkvlogIndexFileHeader) || totalBytes - sizeof(AkvlogIndexFileHeader) > std::numeric_limits<
                    size_t>::max()) { return false; }
                const auto modified = fs::last_write_time(path, error);
                if (error) { return false; }
                {
                    std::lock_guard lock{indexValidationMu_};
                    const auto it = validatedIndexPayloads_.find(path.string());
                    if (it != validatedIndexPayloads_.end() && it->second.crc32c == header.payloadCrc32c && it->second.bytes == totalBytes
                        && it->second.modified == modified) { return true; }
                }

                std::vector<uint8_t> payload(static_cast<size_t>(totalBytes - sizeof(AkvlogIndexFileHeader)));
                seekFile(file, sizeof(AkvlogIndexFileHeader));
                if (!payload.empty() && fread(payload.data(), 1, payload.size(), file) != payload.size()) { return false; }
                if (cpu::CRC32C(reinterpret_cast<const std::byte*>(payload.data()), payload.size()) != header.payloadCrc32c) {
                    return false;
                }
                markIndexPayloadValidated(path, header.payloadCrc32c);
                return true;
            }

            static void addBloomFingerprint(std::vector<uint8_t>& bloom, uint64_t bitCount, uint64_t fingerprint) {
                for (uint32_t hash = 0; hash < INDEX_BLOOM_HASHES; ++hash) {
                    const uint64_t bit = mixIndexHash(fingerprint + 0x9e3779b97f4a7c15ULL * hash) % bitCount;
                    bloom[bit / 8u] |= static_cast<uint8_t>(1u << (bit % 8u));
                }
            }

            [[nodiscard]] static bool bloomMayContain(FILE* file, const AkvlogIndexFileHeader& header, uint64_t fingerprint) {
                if (header.bloomBitCount == 0) { return false; }
                for (uint32_t hash = 0; hash < header.bloomHashCount; ++hash) {
                    const uint64_t bit = mixIndexHash(fingerprint + 0x9e3779b97f4a7c15ULL * hash) % header.bloomBitCount;
                    seekFile(file, sizeof(AkvlogIndexFileHeader) + bit / 8u);
                    uint8_t byte = 0;
                    if (fread(&byte, sizeof(byte), 1, file) != 1) {
                        throw std::runtime_error("VersionLog: truncated segment Bloom filter");
                    }
                    if ((byte & static_cast<uint8_t>(1u << (bit % 8u))) == 0) { return false; }
                }
                return true;
            }

            static void writeAll(FILE* file, const void* data, size_t bytes) {
                if (bytes != 0 && fwrite(data, 1, bytes, file) != bytes) {
                    throw std::runtime_error("VersionLog: failed to write segment index");
                }
            }

            void writeSegmentIndex(const fs::path& segmentPath, uint64_t logBytes, const SegmentKeyIndex& index) const {
                struct PreparedKey {
                    uint64_t fingerprint = 0;
                    const std::string* key = nullptr;
                    const std::vector<IndexVersion>* versions = nullptr;
                };

                std::vector<PreparedKey> keys;
                keys.reserve(index.size());
                uint64_t versionCount = 0;
                for (const auto& [key, versions] : index) {
                    if (versions.empty()) { continue; }
                    if (versions.size() > std::numeric_limits<uint32_t>::max() || versionCount > std::numeric_limits<uint64_t>::max() -
                        versions.size()) { throw std::runtime_error("VersionLog: segment index is too large"); }
                    const uint64_t fingerprint = key.empty()
                                                     ? 0ULL
                                                     : core::computeKeyFp64(reinterpret_cast<const uint8_t*>(key.data()), key.size());
                    keys.push_back(PreparedKey{fingerprint, &key, &versions});
                    versionCount += static_cast<uint64_t>(versions.size());
                }
                std::sort(
                    keys.begin(),
                    keys.end(),
                    [](const PreparedKey& left, const PreparedKey& right) {
                        return left.fingerprint == right.fingerprint ? *left.key < *right.key : left.fingerprint < right.fingerprint;
                    }
                );

                const uint64_t requestedBloomBits = std::max<uint64_t>(64, static_cast<uint64_t>(keys.size()) * INDEX_BLOOM_BITS_PER_KEY);
                if (requestedBloomBits > std::numeric_limits<uint32_t>::max()) {
                    throw std::runtime_error("VersionLog: segment Bloom filter is too large");
                }
                const uint32_t bloomBitCount = static_cast<uint32_t>(requestedBloomBits);
                std::vector<uint8_t> bloom((static_cast<size_t>(bloomBitCount) + 7u) / 8u, 0);
                for (const auto& key : keys) { addBloomFingerprint(bloom, bloomBitCount, key.fingerprint); }

                std::vector<AkvlogIndexKeyRecord> directories;
                std::vector<AkvlogIndexVersionRecord> versions;
                directories.reserve(keys.size());
                versions.reserve(static_cast<size_t>(versionCount));
                for (const auto& key : keys) {
                    auto ordered = *key.versions;
                    std::sort(
                        ordered.begin(),
                        ordered.end(),
                        [](const IndexVersion& left, const IndexVersion& right) {
                            return left.seq == right.seq ? left.offset < right.offset : left.seq < right.seq;
                        }
                    );
                    directories.push_back(
                        AkvlogIndexKeyRecord{
                            key.fingerprint,
                            static_cast<uint64_t>(versions.size()),
                            static_cast<uint32_t>(ordered.size()),
                            0,
                        }
                    );
                    for (const auto& version : ordered) { versions.push_back(AkvlogIndexVersionRecord{version.seq, version.offset}); }
                }

                const size_t payloadBytes = bloom.size() + directories.size() * sizeof(AkvlogIndexKeyRecord) + versions.size() * sizeof(
                    AkvlogIndexVersionRecord);
                std::vector<uint8_t> payload(payloadBytes);
                uint8_t* payloadCursor = payload.data();
                if (!bloom.empty()) {
                    std::memcpy(payloadCursor, bloom.data(), bloom.size());
                    payloadCursor += bloom.size();
                }
                if (!directories.empty()) {
                    const size_t bytes = directories.size() * sizeof(AkvlogIndexKeyRecord);
                    std::memcpy(payloadCursor, directories.data(), bytes);
                    payloadCursor += bytes;
                }
                if (!versions.empty()) {
                    const size_t bytes = versions.size() * sizeof(AkvlogIndexVersionRecord);
                    std::memcpy(payloadCursor, versions.data(), bytes);
                }

                AkvlogIndexFileHeader header{};
                header.magic = AKVLOG_INDEX_MAGIC;
                header.version = AKVLOG_INDEX_VERSION;
                header.bloomHashCount = INDEX_BLOOM_HASHES;
                header.logBytes = logBytes;
                header.keyCount = static_cast<uint64_t>(directories.size());
                header.versionCount = static_cast<uint64_t>(versions.size());
                header.bloomBitCount = bloomBitCount;
                header.payloadCrc32c = cpu::CRC32C(reinterpret_cast<const std::byte*>(payload.data()), payload.size());
                header.crc32c = 0;
                header.crc32c = cpu::CRC32C(reinterpret_cast<const std::byte*>(&header), sizeof(header));

                const fs::path path = segmentIndexPath(segmentPath);
                forgetIndexPayloadValidation(path);
                fs::path temporary = path;
                temporary += ".tmp";
                FILE* file = nullptr;
                try {
                    #ifdef _WIN32
                    file = _wfopen(temporary.wstring().c_str(), L"wb");
                    #else
                    file = fopen(temporary.string().c_str(), "wb");
                    #endif
                    if (!file) { throwVLogError("cannot create segment index", temporary); }
                    writeAll(file, &header, sizeof(header));
                    writeAll(file, payload.data(), payload.size());
                    fflush(file);
                    doFdatasync(file);
                    fclose(file);
                    file = nullptr;

                    std::error_code error;
                    fs::remove(path, error);
                    error.clear();
                    fs::rename(temporary, path, error);
                    if (error) { throwVLogError("cannot publish segment index: " + error.message(), path); }
                    markIndexPayloadValidated(path, header.payloadCrc32c);
                }
                catch (...) {
                    if (file) { fclose(file); }
                    std::error_code ignored;
                    fs::remove(temporary, ignored);
                    throw;
                }
            }

            void tryWriteSegmentIndex(const fs::path& segmentPath, uint64_t logBytes, const SegmentKeyIndex& index) const noexcept {
                try { writeSegmentIndex(segmentPath, logBytes, index); }
                catch (...) { sidecarRebuildFailures_.fetch_add(1, std::memory_order_relaxed); }
            }

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
                if (fs::exists(opts_.logPath)) { discovered.push_back(SegmentInfo{0, opts_.logPath}); }

                const auto parent = opts_.logPath.parent_path().empty() ? fs::path{"."} : opts_.logPath.parent_path();
                const std::string prefix = opts_.logPath.stem().string() + "-seg-";
                const std::string extension = opts_.logPath.extension().string();
                for (const auto& entry : fs::directory_iterator(parent)) {
                    if (!entry.is_regular_file()) { continue; }
                    const std::string name = entry.path().filename().string();
                    if (!name.starts_with(prefix) || !name.ends_with(extension) || name.size() <= prefix.size() + extension.size()) {
                        continue;
                    }
                    const std::string_view suffix{name.data() + prefix.size(), name.size() - prefix.size() - extension.size()};
                    if (suffix.empty() || !std::ranges::all_of(suffix, [](unsigned char ch) { return ch >= '0' && ch <= '9'; })) {
                        continue;
                    }
                    try {
                        const uint64_t id = std::stoull(std::string{suffix});
                        if (id != 0) { discovered.push_back(SegmentInfo{id, entry.path()}); }
                    }
                    catch (const std::exception&) {}
                }
                std::sort(
                    discovered.begin(),
                    discovered.end(),
                    [](const SegmentInfo& left, const SegmentInfo& right) { return left.id < right.id; }
                );
                return discovered;
            }

            [[nodiscard]] std::vector<SegmentInfo> segmentSnapshot() const {
                std::shared_lock lock{segmentMu_};
                return segments_;
            }

            [[nodiscard]] bool retentionEnabled() const noexcept { return opts_.retentionDays != 0 || opts_.retentionMinCommitSeq != 0; }

            [[nodiscard]] bool segmentReachedRetentionBoundary(const SegmentInfo& segment, fs::file_time_type now) const {
                if (opts_.retentionMinCommitSeq != 0 && segment.hasEntries && segment.lastSeq < opts_.retentionMinCommitSeq) {
                    return true;
                }
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
                            if ((usesTrueParallelWrites() ? parallelActiveSegmentIds_.contains(segment.id) : segment.id == activeSegmentId_)
                                || (segment.hasEntries && segment.lastSeq > committedSeq) || !
                                segmentReachedRetentionBoundary(segment, now)) { continue; }
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

            void runRequestedRetentionPrune() {
                if (retentionPrunePending_.exchange(false, std::memory_order_acq_rel)) { pruneClosedSegments(); }
            }

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

                fflush(file_);
                doFdatasync(file_);
                writeActiveSegmentIndexLocked();
                fclose(file_);
                file_ = nullptr;

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
                activeSegmentBytes_ = sizeof(AkvlogV5FileHeader);
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
                    writeTailFile(path, sizeof(AkvlogV5FileHeader), true);
                }
                catch (...) {
                    fclose(file);
                    throw;
                }
                lane.file = file;
                lane.segmentId = id;
                lane.bytes = sizeof(AkvlogV5FileHeader);
                lane.index.clear();
                parallelActiveSegmentIds_.insert(id);
                activeSegmentId_ = id;
                activeSegmentBytes_ = lane.bytes;
                durableBytes_ += lane.bytes;
                {
                    std::unique_lock segmentLock{segmentMu_};
                    segments_.push_back(SegmentInfo{id, path, 0, 0, lane.bytes});
                }
            }

            void initializeParallelLanesAfterRecovery() {
                if (!usesTrueParallelWrites()) { return; }
                std::lock_guard writeLock{writeMu_};
                if (file_) {
                    fclose(file_);
                    file_ = nullptr;
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
                const auto tail = readTailFile(path);
                if (!tail.has_value()) { return; }
                if (file_) {
                    fclose(file_);
                    file_ = nullptr;
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

            void rotateParallelLaneIfNeeded(ParallelLane& lane) {
                if (opts_.segmentBytes == 0 || lane.bytes < opts_.segmentBytes) { return; }
                const uint64_t oldId = lane.segmentId;
                const auto oldPath = segmentPath(oldId);
                fflush(lane.file);
                doFdatasync(lane.file);
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
                fflush(lane.file);
                const bool sync = opts_.syncMode != VLogSyncMode::ASYNC;
                if (sync) { doFdatasync(lane.file); }
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
                                                 : core::computeKeyFp64(
                                                     reinterpret_cast<const uint8_t*>(write.key.data()),
                                                     write.key.size()
                                                 );
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
                    fflush(lane->file);
                    doFdatasync(lane->file);
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
                    fflush(lane->file);
                    doFdatasync(lane->file);
                    writeTailFile(segmentPath(lane->segmentId), lane->bytes, true);
                }
            }

            void insertSorted(const std::string& key, VersionEntry ve) {
                auto& versions = residentIndex_[key];
                if (versions.empty() || versions.back().seq <= ve.seq) {
                    versions.push_back(std::move(ve));
                    return;
                }

                const auto pos = std::lower_bound(
                    versions.begin(),
                    versions.end(),
                    ve.seq,
                    [](const VersionEntry& e, uint64_t seq) { return e.seq < seq; }
                );
                versions.insert(pos, std::move(ve));
            }

            struct ScanSummary {
                uint64_t firstSeq = 0;
                uint64_t maxSeq = 0;
                uint64_t entryCount = 0;
                uint64_t rollbackCount = 0;
                uint64_t durableBytes = 0;
                bool hasEntries = false;
            };

            [[nodiscard]] FILE* openReadFile(const fs::path& path) const {
                #ifdef _WIN32
                return _wfopen(path.wstring().c_str(), L"rb");
                #else
                return fopen(path.string().c_str(), "rb");
                #endif
            }

            template <typename Visitor>
            [[nodiscard]] ScanSummary scanFile(
                FILE* rf,
                const fs::path& path,
                bool allowTrailingEntry,
                Visitor&& visitor,
                uint64_t maxBytes = std::numeric_limits<uint64_t>::max()
            ) const {
                ScanSummary summary;
                if (maxBytes < sizeof(AkvlogV5FileHeader)) { throwVLogError("durable tail precedes file header", path, maxBytes); }
                AkvlogV5FileHeader fileHdr{};
                const size_t headerRead = fread(&fileHdr, 1, sizeof(fileHdr), rf);
                if (headerRead == 0 && feof(rf)) { return summary; }
                if (headerRead != sizeof(fileHdr)) { throwVLogError("truncated file header", path, 0); }

                const uint32_t storedHeaderCrc = fileHdr.crc32c;
                fileHdr.crc32c = 0;
                const uint32_t computedHeaderCrc = cpu::CRC32C(reinterpret_cast<const std::byte*>(&fileHdr), sizeof(fileHdr));
                if (fileHdr.magic != AKVLOG_V5_MAGIC || fileHdr.version != AKVLOG_V5_VERSION || storedHeaderCrc != computedHeaderCrc) {
                    throwVLogError("corrupt file header", path, 0);
                }
                summary.durableBytes += sizeof(fileHdr);

                std::vector<uint8_t> buf;
                while (true) {
                    const uint64_t entryOffset = fileOffset(rf);
                    if (entryOffset == maxBytes) { break; }
                    if (entryOffset > maxBytes || maxBytes - entryOffset < sizeof(uint32_t)) {
                        if (allowTrailingEntry) { break; }
                        throwVLogError("durable tail splits entry length", path, entryOffset);
                    }
                    uint32_t entryLen = 0;
                    const size_t prefixRead = fread(&entryLen, 1, sizeof(entryLen), rf);
                    if (prefixRead == 0 && feof(rf)) { break; }
                    if (prefixRead != sizeof(entryLen)) {
                        if (allowTrailingEntry && feof(rf)) { break; }
                        throwVLogError("truncated entry length", path, entryOffset);
                    }
                    if (entryLen < MIN_ENTRY_SIZE || entryLen > MAX_ENTRY_SIZE) {
                        throwVLogError("corrupt entry length", path, entryOffset);
                    }
                    if (static_cast<uint64_t>(entryLen) > maxBytes - entryOffset) {
                        if (allowTrailingEntry) { break; }
                        throwVLogError("durable tail splits entry", path, entryOffset);
                    }

                    buf.resize(entryLen);
                    std::memcpy(buf.data(), &entryLen, sizeof(entryLen));

                    const size_t rest = entryLen - sizeof(entryLen);
                    if (fread(buf.data() + sizeof(entryLen), 1, rest, rf) != rest) {
                        if (allowTrailingEntry && feof(rf)) { break; }
                        throwVLogError("truncated entry", path, entryOffset);
                    }

                    uint32_t storedEntryCrc = 0;
                    std::memcpy(&storedEntryCrc, buf.data() + entryLen - CRC_SIZE, CRC_SIZE);
                    std::memset(buf.data() + entryLen - CRC_SIZE, 0, CRC_SIZE);
                    const uint32_t computedEntryCrc = cpu::CRC32C(reinterpret_cast<const std::byte*>(buf.data()), entryLen - CRC_SIZE);
                    const auto& ehdr = *reinterpret_cast<const AkvlogV5EntryHeader*>(buf.data());
                    if (storedEntryCrc != computedEntryCrc) { throwVLogError("entry CRC mismatch", path, entryOffset, ehdr.seq); }

                    if (buf.size() < ENTRY_HDR_SIZE) { throwVLogError("corrupt entry header", path, entryOffset); }
                    const size_t expectedSize = ENTRY_HDR_SIZE + ehdr.keyLen + ehdr.valueLen + CRC_SIZE;
                    if (expectedSize != entryLen) { throwVLogError("corrupt entry payload", path, entryOffset, ehdr.seq); }

                    const uint8_t* p = buf.data() + ENTRY_HDR_SIZE;
                    const std::string_view key(reinterpret_cast<const char*>(p), ehdr.keyLen);
                    p += ehdr.keyLen;
                    const std::span<const uint8_t> storedValue{p, ehdr.valueLen};
                    std::vector<uint8_t> decodedValue;
                    std::span<const uint8_t> value = storedValue;
                    if ((ehdr.flags & VLOG_FLAG_ZSTD) != 0) {
                        if (storedValue.size() <= ZSTD_VALUE_PREFIX_SIZE) {
                            throwVLogError("invalid compressed value record", path, entryOffset, ehdr.seq);
                        }
                        uint32_t rawSize = 0;
                        std::memcpy(&rawSize, storedValue.data(), sizeof(rawSize));
                        if (rawSize == 0 || rawSize > MAX_ENTRY_SIZE) {
                            throwVLogError("invalid compressed value size", path, entryOffset, ehdr.seq);
                        }
                        decodedValue.resize(rawSize);
                        const size_t decodedSize = ZSTD_decompress(
                            decodedValue.data(),
                            decodedValue.size(),
                            storedValue.data() + ZSTD_VALUE_PREFIX_SIZE,
                            storedValue.size() - ZSTD_VALUE_PREFIX_SIZE
                        );
                        if (ZSTD_isError(decodedSize) || decodedSize != rawSize) {
                            throwVLogError("corrupt Zstd value payload", path, entryOffset, ehdr.seq);
                        }
                        value = std::span<const uint8_t>{decodedValue};
                    }
                    auto logicalHeader = ehdr;
                    logicalHeader.flags &= static_cast<uint8_t>(~VLOG_FLAG_ZSTD);
                    if constexpr (std::is_invocable_v<Visitor&, std::string_view, const AkvlogV5EntryHeader&, std::span<const uint8_t>,
                        uint64_t>) { visitor(key, logicalHeader, value, entryOffset); }
                    else { visitor(key, logicalHeader, value); }
                    if (!summary.hasEntries) {
                        summary.firstSeq = ehdr.seq;
                        summary.maxSeq = ehdr.seq;
                        summary.hasEntries = true;
                    }
                    else {
                        summary.firstSeq = std::min(summary.firstSeq, ehdr.seq);
                        summary.maxSeq = std::max(summary.maxSeq, ehdr.seq);
                    }
                    ++summary.entryCount;
                    if ((ehdr.flags & VLOG_FLAG_ROLLBACK) != 0) { ++summary.rollbackCount; }
                    summary.durableBytes += entryLen;
                }
                return summary;
            }

            static void mergeScanSummary(ScanSummary& total, const ScanSummary& part) {
                if (part.hasEntries) {
                    if (!total.hasEntries) {
                        total.firstSeq = part.firstSeq;
                        total.maxSeq = part.maxSeq;
                        total.hasEntries = true;
                    }
                    else {
                        total.firstSeq = std::min(total.firstSeq, part.firstSeq);
                        total.maxSeq = std::max(total.maxSeq, part.maxSeq);
                    }
                }
                total.entryCount += part.entryCount;
                total.rollbackCount += part.rollbackCount;
                total.durableBytes += part.durableBytes;
            }

            template <typename Visitor>
            [[nodiscard]] ScanSummary scanSegment(const fs::path& path, bool allowTrailingEntry, Visitor&& visitor) const {
                FILE* rf = openReadFile(path);
                if (!rf) { throw std::runtime_error("VersionLog: cannot open segment for reading: " + path.string()); }
                try {
                    uint64_t maxBytes = std::numeric_limits<uint64_t>::max();
                    if (const auto tail = readTailFile(path); tail.has_value()) {
                        std::error_code error;
                        const uint64_t actualBytes = fs::file_size(path, error);
                        if (error) { throwVLogError("cannot stat segment for durable tail validation: " + error.message(), path); }
                        if (*tail > actualBytes) { throwVLogError("invalid durable tail length", path, *tail); }
                        maxBytes = *tail;
                    }
                    auto summary = scanFile(rf, path, allowTrailingEntry, std::forward<Visitor>(visitor), maxBytes);
                    fclose(rf);
                    return summary;
                }
                catch (...) {
                    fclose(rf);
                    throw;
                }
            }

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
                        true,
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
            void persistRetentionBasesLocked(
                const RetentionStateMap& states,
                const std::unordered_set<uint64_t>& expiredIds,
                uint64_t baseSeq
            ) {
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
                            durableBytes_ += static_cast<uint64_t>(write.bytes.size());
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
                        fflush(base.file);
                        doFdatasync(base.file);
                        const auto basePath = segmentPath(base.segmentId);
                        writeTailFile(basePath, base.bytes, true);
                        tryWriteSegmentIndex(basePath, base.bytes, base.index);
                        fclose(base.file);
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
                        durableBytes_ += static_cast<uint64_t>(write.bytes.size());
                        notePersistedLocked(write);
                        trackEntryStats(flags);
                        rotateSegmentIfNeededLocked();
                        ++baseEntriesWritten;
                    }
                    if (file_) {
                        fflush(file_);
                        doFdatasync(file_);
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
                    durableBytes_ = segment.bytes > durableBytes_ ? 0 : durableBytes_ - segment.bytes;
                    indexedEntries_.fetch_sub(segment.entryCount, std::memory_order_relaxed);
                    rollbackEntries_.fetch_sub(segment.rollbackCount, std::memory_order_relaxed);
                    ++prunedSegments;
                }
                segments_.swap(retained);
                retentionPrunedSegments_.fetch_add(prunedSegments, std::memory_order_relaxed);
            }

            class IndexUnavailable final : public std::runtime_error {
                public:
                    using std::runtime_error::runtime_error;
            };

            struct ParsedEntry {
                std::string key;
                VersionEntry entry;
            };

            [[nodiscard]] ParsedEntry readEntryAt(FILE* file, const fs::path& path, uint64_t offset) const {
                seekFile(file, offset);
                uint32_t entryLen = 0;
                if (fread(&entryLen, sizeof(entryLen), 1, file) != 1) {
                    throwVLogError("truncated indexed entry length", path, offset);
                }
                if (entryLen < MIN_ENTRY_SIZE || entryLen > MAX_ENTRY_SIZE) {
                    throwVLogError("corrupt indexed entry length", path, offset);
                }

                std::vector<uint8_t> buffer(entryLen);
                std::memcpy(buffer.data(), &entryLen, sizeof(entryLen));
                const size_t remaining = entryLen - sizeof(entryLen);
                if (fread(buffer.data() + sizeof(entryLen), 1, remaining, file) != remaining) {
                    throwVLogError("truncated indexed entry", path, offset);
                }

                uint32_t storedCrc = 0;
                std::memcpy(&storedCrc, buffer.data() + entryLen - CRC_SIZE, CRC_SIZE);
                std::memset(buffer.data() + entryLen - CRC_SIZE, 0, CRC_SIZE);
                const uint32_t computedCrc = cpu::CRC32C(reinterpret_cast<const std::byte*>(buffer.data()), entryLen - CRC_SIZE);
                const auto& header = *reinterpret_cast<const AkvlogV5EntryHeader*>(buffer.data());
                if (storedCrc != computedCrc) { throwVLogError("indexed entry CRC mismatch", path, offset, header.seq); }

                const size_t expectedSize = ENTRY_HDR_SIZE + header.keyLen + header.valueLen + CRC_SIZE;
                if (expectedSize != entryLen) { throwVLogError("corrupt indexed entry payload", path, offset, header.seq); }

                const uint8_t* data = buffer.data() + ENTRY_HDR_SIZE;
                ParsedEntry parsed;
                parsed.key.assign(reinterpret_cast<const char*>(data), header.keyLen);
                data += header.keyLen;
                const std::span<const uint8_t> storedValue{data, header.valueLen};
                std::span<const uint8_t> value = storedValue;
                std::vector<uint8_t> decodedValue;
                if ((header.flags & VLOG_FLAG_ZSTD) != 0) {
                    if (storedValue.size() <= ZSTD_VALUE_PREFIX_SIZE) {
                        throw std::runtime_error("VersionLog: invalid indexed compressed value record");
                    }
                    uint32_t rawSize = 0;
                    std::memcpy(&rawSize, storedValue.data(), sizeof(rawSize));
                    if (rawSize == 0 || rawSize > MAX_ENTRY_SIZE) {
                        throw std::runtime_error("VersionLog: invalid indexed compressed value size");
                    }
                    decodedValue.resize(rawSize);
                    const size_t decodedSize = ZSTD_decompress(
                        decodedValue.data(),
                        decodedValue.size(),
                        storedValue.data() + ZSTD_VALUE_PREFIX_SIZE,
                        storedValue.size() - ZSTD_VALUE_PREFIX_SIZE
                    );
                    if (ZSTD_isError(decodedSize) || decodedSize != rawSize) {
                        throw std::runtime_error("VersionLog: corrupt indexed Zstd value payload");
                    }
                    value = std::span<const uint8_t>{decodedValue};
                }

                parsed.entry.seq = header.seq;
                parsed.entry.sourceNodeId = header.sourceNodeId;
                parsed.entry.timestampNs = header.timestampNs;
                parsed.entry.flags = header.flags & static_cast<uint8_t>(~VLOG_FLAG_ZSTD);
                parsed.entry.value.assign(value.begin(), value.end());
                return parsed;
            }

            [[nodiscard]] static bool indexLayoutIsValid(const fs::path& path, const AkvlogIndexFileHeader& header) {
                if (header.magic != AKVLOG_INDEX_MAGIC || header.version != AKVLOG_INDEX_VERSION || header.bloomHashCount !=
                    INDEX_BLOOM_HASHES || header.bloomBitCount == 0) { return false; }
                AkvlogIndexFileHeader checksum = header;
                const uint32_t storedCrc = checksum.crc32c;
                checksum.crc32c = 0;
                if (storedCrc != cpu::CRC32C(reinterpret_cast<const std::byte*>(&checksum), sizeof(checksum))) { return false; }

                const uint64_t bloomBytes = (static_cast<uint64_t>(header.bloomBitCount) + 7u) / 8u;
                if (header.keyCount > (std::numeric_limits<uint64_t>::max() - sizeof(AkvlogIndexFileHeader) - bloomBytes) / sizeof(
                    AkvlogIndexKeyRecord)) { return false; }
                uint64_t expectedBytes = sizeof(AkvlogIndexFileHeader) + bloomBytes + header.keyCount * sizeof(AkvlogIndexKeyRecord);
                if (header.versionCount > (std::numeric_limits<uint64_t>::max() - expectedBytes) / sizeof(AkvlogIndexVersionRecord)) {
                    return false;
                }
                expectedBytes += header.versionCount * sizeof(AkvlogIndexVersionRecord);
                std::error_code error;
                const uint64_t actualBytes = fs::file_size(path, error);
                return !error && actualBytes == expectedBytes;
            }

            [[nodiscard]] bool sidecarIndexVersions(
                const SegmentInfo& segment,
                std::string_view key,
                std::vector<IndexVersion>& out
            ) const {
                const fs::path path = segmentIndexPath(segment.path);
                FILE* indexFile = openReadFile(path);
                if (!indexFile) {
                    sidecarFallbackCount_.fetch_add(1, std::memory_order_relaxed);
                    return false;
                }
                try {
                    AkvlogIndexFileHeader header{};
                    if (fread(&header, sizeof(header), 1, indexFile) != 1 || header.logBytes != segment.bytes || !indexLayoutIsValid(
                        path,
                        header
                    ) || !verifyIndexPayload(indexFile, path, header)) {
                        fclose(indexFile);
                        sidecarFallbackCount_.fetch_add(1, std::memory_order_relaxed);
                        return false;
                    }
                    const uint64_t fingerprint = key.empty()
                                                     ? 0ULL
                                                     : core::computeKeyFp64(reinterpret_cast<const uint8_t*>(key.data()), key.size());
                    if (!bloomMayContain(indexFile, header, fingerprint)) {
                        fclose(indexFile);
                        return true;
                    }

                    const uint64_t bloomBytes = (static_cast<uint64_t>(header.bloomBitCount) + 7u) / 8u;
                    const uint64_t directoryOffset = sizeof(AkvlogIndexFileHeader) + bloomBytes;
                    uint64_t first = 0;
                    uint64_t last = header.keyCount;
                    while (first < last) {
                        const uint64_t middle = first + (last - first) / 2u;
                        AkvlogIndexKeyRecord candidate{};
                        seekFile(indexFile, directoryOffset + middle * sizeof(candidate));
                        if (fread(&candidate, sizeof(candidate), 1, indexFile) != 1) {
                            throw IndexUnavailable("VersionLog: truncated segment index directory");
                        }
                        if (candidate.keyFp64 < fingerprint) { first = middle + 1u; }
                        else { last = middle; }
                    }

                    if (first == header.keyCount) {
                        fclose(indexFile);
                        return true;
                    }

                    FILE* logFile = openReadFile(segment.path);
                    if (!logFile) { throw std::runtime_error("VersionLog: cannot open indexed segment: " + segment.path.string()); }
                    try {
                        const uint64_t versionOffset = directoryOffset + header.keyCount * sizeof(AkvlogIndexKeyRecord);
                        for (uint64_t directory = first; directory < header.keyCount; ++directory) {
                            AkvlogIndexKeyRecord candidate{};
                            seekFile(indexFile, directoryOffset + directory * sizeof(candidate));
                            if (fread(&candidate, sizeof(candidate), 1, indexFile) != 1 || candidate.keyFp64 != fingerprint) { break; }
                            if (candidate.versionCount == 0 || candidate.firstVersion > header.versionCount || candidate.versionCount >
                                header.versionCount - candidate.firstVersion) {
                                throw IndexUnavailable("VersionLog: invalid segment index version range");
                            }
                            AkvlogIndexVersionRecord firstVersion{};
                            seekFile(indexFile, versionOffset + candidate.firstVersion * sizeof(firstVersion));
                            if (fread(&firstVersion, sizeof(firstVersion), 1, indexFile) != 1) {
                                throw IndexUnavailable("VersionLog: truncated segment index version record");
                            }
                            if (readEntryAt(logFile, segment.path, firstVersion.offset).key != key) { continue; }

                            std::vector<AkvlogIndexVersionRecord> storedVersions(candidate.versionCount);
                            seekFile(indexFile, versionOffset + candidate.firstVersion * sizeof(AkvlogIndexVersionRecord));
                            if (fread(storedVersions.data(), sizeof(AkvlogIndexVersionRecord), storedVersions.size(), indexFile) !=
                                storedVersions.size()) { throw IndexUnavailable("VersionLog: truncated segment index versions"); }
                            out.reserve(out.size() + storedVersions.size());
                            for (const auto& version : storedVersions) { out.push_back(IndexVersion{version.seq, version.offset}); }
                            fclose(logFile);
                            fclose(indexFile);
                            return true;
                        }
                        fclose(logFile);
                    }
                    catch (...) {
                        fclose(logFile);
                        throw;
                    }
                    fclose(indexFile);
                    return true;
                }
                catch (const IndexUnavailable&) {
                    fclose(indexFile);
                    sidecarFallbackCount_.fetch_add(1, std::memory_order_relaxed);
                    return false;
                }
                catch (...) {
                    fclose(indexFile);
                    sidecarFallbackCount_.fetch_add(1, std::memory_order_relaxed);
                    return false;
                }
            }

            [[nodiscard]] bool indexVersionsForSegment(
                const SegmentInfo& segment,
                std::string_view key,
                std::vector<IndexVersion>& out
            ) const { return activeIndexVersions(segment.id, key, out) || sidecarIndexVersions(segment, key, out); }

            [[nodiscard]] std::vector<VersionEntry> readIndexedEntries(
                const SegmentInfo& segment,
                std::string_view key,
                std::span<const IndexVersion> versions,
                uint64_t visibleSeq
            ) const {
                if (versions.empty()) { return {}; }
                FILE* file = openReadFile(segment.path);
                if (!file) { throw std::runtime_error("VersionLog: cannot open indexed segment: " + segment.path.string()); }
                try {
                    std::vector<VersionEntry> entries;
                    entries.reserve(versions.size());
                    for (const auto& version : versions) {
                        if (version.seq > visibleSeq) { continue; }
                        auto parsed = readEntryAt(file, segment.path, version.offset);
                        if (parsed.key != key || parsed.entry.seq != version.seq) {
                            throw IndexUnavailable("VersionLog: stale segment index entry");
                        }
                        entries.push_back(std::move(parsed.entry));
                    }
                    fclose(file);
                    return entries;
                }
                catch (...) {
                    fclose(file);
                    throw;
                }
            }

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
                    for (auto& segment : segments) {
                        SegmentKeyIndex segmentIndex;
                        const auto summary = scanSegment(
                            segment.path,
                            false,
                            [&segmentIndex](
                            std::string_view key,
                            const AkvlogV5EntryHeader& header,
                            std::span<const uint8_t>,
                            uint64_t offset
                        ) {
                                auto& versions = segmentIndex[std::string{key}];
                                versions.push_back(IndexVersion{header.seq, offset});
                            }
                        );
                        segment.firstSeq = summary.firstSeq;
                        segment.lastSeq = summary.maxSeq;
                        segment.bytes = summary.durableBytes;
                        segment.entryCount = summary.entryCount;
                        segment.rollbackCount = summary.rollbackCount;
                        segment.hasEntries = summary.hasEntries;
                        mergeScanSummary(total, summary);
                        tryWriteSegmentIndex(segment.path, segment.bytes, segmentIndex);
                        recoveredActiveSegmentId = segment.id;
                        activeIndex = std::move(segmentIndex);
                    }
                    recoveredSegments = static_cast<uint64_t>(segments.size());
                    recoveredEntries = total.entryCount;
                    applyRecoverySummary(total, std::move(segments));
                    resetActiveIndex(recoveredActiveSegmentId, std::move(activeIndex));
                    pruneClosedSegments();
                    prepareSerialAppendAfterRecovery();
                    initializeParallelLanesAfterRecovery();
                }
                catch (...) { error = std::current_exception(); }

                const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - started
                ).count();
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
                const auto& path = opts_.logPath;
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
                    activeSegmentBytes_ = sizeof(AkvlogV5FileHeader);
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
    };

    VersionLog::VersionLog() = default;

    std::unique_ptr<VersionLog> VersionLog::create(VersionLogOptions opts) {
        auto impl = std::make_unique<Impl>();
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
        if (!impl_) { return; }
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
            if (!impl_->file_) {
                Impl::completeAppend(completion, std::make_exception_ptr(std::runtime_error("VersionLog: append rejected after close")));
                return;
            }
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
                true,
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
                true,
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
                    return left.seq == right.seq && left.sourceNodeId == right.sourceNodeId && left.timestampNs == right.timestampNs && left
                       .flags == right.flags && left.value == right.value;
                }
            ),
            entries.end()
        );
        return entries;
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
            true,
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
                        return left.seq == right.seq && left.sourceNodeId == right.sourceNodeId && left.timestampNs == right.timestampNs &&
                            left.flags == right.flags && left.value == right.value;
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

    VersionLogSnapshot VersionLog::snapshot() const noexcept {
        VersionLogSnapshot out;
        if (!impl_) { return out; }

        std::vector<VersionLog::Impl::ParallelLane*> lanes;
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
            out.durableBytes = impl_->durableBytes_;
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
            lanes.reserve(impl_->parallelLanes_.size());
            for (const auto& lane : impl_->parallelLanes_) { lanes.push_back(lane.get()); }
        }
        for (const auto& lane : lanes) {
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
            fflush(impl_->file_);
            doFdatasync(impl_->file_);
            return;
        }

        impl_->stopAsyncWorker();

        {
            std::lock_guard lock{impl_->writeMu_};
            impl_->checkAsyncError();
            if (!impl_->file_) { return; }
            fflush(impl_->file_);
            doFdatasync(impl_->file_);
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
            impl_->closeParallelLanes();
            impl_->pruneClosedSegments();
            if (recoveryError) { std::rethrow_exception(recoveryError); }
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
            fflush(impl_->file_);
            doFdatasync(impl_->file_);
            impl_->writeActiveSegmentIndexLocked();
            fclose(impl_->file_);
            impl_->file_ = nullptr;
        }
        if (recoveryError) { std::rethrow_exception(recoveryError); }
        if (asyncError) { std::rethrow_exception(asyncError); }
    }
} // namespace akkaradb::engine::vlog

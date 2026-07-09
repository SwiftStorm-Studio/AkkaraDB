/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/wal/WalWriter.cpp
#include "akk/engine/wal/WalWriter.hpp"

#include "akk/core/record/KeyFingerprint.hpp"
#include "akk/cpu/CRC32C.hpp"
#include "akk/engine/wal/WalFraming.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif

namespace akkaradb::engine::wal {
    namespace fs = std::filesystem;

    namespace {
        static constexpr uint64_t SEGMENT_BYTES = 64ULL * 1024ULL * 1024ULL;

        [[nodiscard]] uint64_t nowUs() noexcept {
            return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch()
            ).count());
        }

        [[nodiscard]] uint16_t resolveAutoShardCount() noexcept {
            const unsigned hw = std::thread::hardware_concurrency();
            return static_cast<uint16_t>(std::clamp<unsigned>(hw == 0 ? 1u : hw, 1u, 16u));
        }

        [[nodiscard]] uint32_t crc32cBytes(const uint8_t* data, size_t size) noexcept {
            return cpu::CRC32C(reinterpret_cast<const std::byte*>(data), size);
        }

        void refreshSegmentCrc(WalSegmentHeader& header) noexcept {
            header.crc32c = 0;
            uint8_t buf[WalSegmentHeader::SIZE]{};
            header.serialize(buf);
            header.crc32c = crc32cBytes(buf, sizeof(buf));
        }

        void doFdatasync(FILE* f) {
            if (f == nullptr) { return; }
            #ifdef _WIN32
            if (_commit(_fileno(f)) != 0) { throw std::runtime_error("WAL fdatasync failed"); }
            #else
            if (::fdatasync(fileno(f)) != 0) { throw std::runtime_error("WAL fdatasync failed"); }
            #endif
        }

        [[nodiscard]] FILE* openRw(const fs::path& path, bool exists) {
            #ifdef _WIN32
            FILE* f = _wfopen(path.wstring().c_str(), exists ? L"r+b" : L"w+b");
            #else
            FILE* f = std::fopen(path.string().c_str(), exists ? "r+b" : "w+b");
            #endif
            if (f == nullptr) { throw std::runtime_error("WAL failed to open segment: " + path.string()); }
            return f;
        }

        void closeFile(FILE*& f) noexcept {
            if (f != nullptr) {
                std::fclose(f);
                f = nullptr;
            }
        }

        void writeAll(FILE* f, const uint8_t* data, size_t size) {
            if (size == 0) { return; }
            if (std::fwrite(data, 1, size, f) != size) { throw std::runtime_error("WAL write failed"); }
        }

        [[nodiscard]] bool readExact(std::ifstream& file, uint8_t* out, size_t len) {
            if (len == 0) { return true; }
            file.read(reinterpret_cast<char*>(out), static_cast<std::streamsize>(len));
            return file.good() || file.gcount() == static_cast<std::streamsize>(len);
        }

        [[nodiscard]] std::string segmentName(uint16_t shardId, uint64_t segmentId) {
            std::ostringstream os;
            os << std::setfill('0') << std::setw(4) << shardId << "_" << std::hex << std::nouppercase << std::setw(16) << segmentId <<
                ".akwal";
            return os.str();
        }

        [[nodiscard]] fs::path segmentPath(const fs::path& walDir, uint16_t shardId, uint64_t segmentId) {
            return walDir / segmentName(shardId, segmentId);
        }

        struct SegmentScanResult {
            bool validHeader = false;
            uint64_t firstSeq = 0;
            uint64_t lastSeq = 0;
        };

        [[nodiscard]] SegmentScanResult scanSegmentSequences(const fs::path& path) {
            SegmentScanResult result{};
            std::ifstream file(path, std::ios::binary);
            if (!file) { return result; }

            uint8_t shdrBuf[WalSegmentHeader::SIZE]{};
            if (!readExact(file, shdrBuf, WalSegmentHeader::SIZE)) { return result; }
            const WalSegmentHeader shdr = WalSegmentHeader::deserialize(shdrBuf);
            if (!shdr.verifyMagic() || !shdr.verifyVersion() || !shdr.verifyChecksum()) { return result; }
            result.validHeader = true;

            while (true) {
                uint8_t ehdrBuf[WalEntryHeader::SIZE]{};
                file.read(reinterpret_cast<char*>(ehdrBuf), WalEntryHeader::SIZE);
                const std::streamsize got = file.gcount();
                if (got == 0) { break; }
                if (got != static_cast<std::streamsize>(WalEntryHeader::SIZE)) { break; }

                const WalEntryHeader ehdr = WalEntryHeader::deserialize(ehdrBuf);
                if (!ehdr.verifyLengths(SEGMENT_BYTES)) { break; }

                std::vector<uint8_t> key(ehdr.keyLen);
                std::vector<uint8_t> value(ehdr.valueLen);
                if (!readExact(file, key.data(), key.size()) || !readExact(file, value.data(), value.size())) { break; }
                if (!ehdr.verifyChecksum(
                    std::span<const uint8_t>{key.data(), key.size()},
                    std::span<const uint8_t>{value.data(), value.size()}
                )) { break; }

                if (result.firstSeq == 0 || ehdr.seq < result.firstSeq) { result.firstSeq = ehdr.seq; }
                if (ehdr.seq > result.lastSeq) { result.lastSeq = ehdr.seq; }
            }
            return result;
        }

        [[nodiscard]] uint64_t findLastSegmentId(const fs::path& walDir, uint16_t shardId) {
            uint64_t maxSegment = 0;
            bool found = false;
            if (!fs::exists(walDir)) { return 0; }

            for (const auto& entry : fs::directory_iterator(walDir)) {
                if (!entry.is_regular_file() || entry.path().extension() != ".akwal") { continue; }
                std::ifstream file(entry.path(), std::ios::binary);
                if (!file) { continue; }
                uint8_t hdrBuf[WalSegmentHeader::SIZE]{};
                if (!readExact(file, hdrBuf, WalSegmentHeader::SIZE)) { continue; }
                const WalSegmentHeader hdr = WalSegmentHeader::deserialize(hdrBuf);
                if (!hdr.verifyMagic() || !hdr.verifyVersion() || !hdr.verifyChecksum() || hdr.shardId != shardId) { continue; }
                maxSegment = found ? std::max(maxSegment, hdr.segmentId) : hdr.segmentId;
                found = true;
            }
            return found ? maxSegment : 0;
        }

        [[nodiscard]] uint16_t shardFor(uint64_t fp64, uint16_t shardCount) noexcept {
            if (shardCount <= 1) { return 0; }
            return static_cast<uint16_t>(fp64 % static_cast<uint64_t>(shardCount));
        }
    } // namespace

    class WalWriter::Impl {
        public:
            struct PendingEntry {
                uint64_t seq;
                std::vector<uint8_t> bytes;
            };

            class ShardWriter {
                public:
                    ShardWriter(WalOptions options, uint16_t shardId)
                        : options_{std::move(options)}, shardId_{shardId}, running_{options_.syncMode == WalSyncMode::ASYNC} {
                        fs::create_directories(options_.walDir);
                        segmentId_ = findLastSegmentId(options_.walDir, shardId_);
                        openSegment(segmentId_);

                        if (options_.syncMode == WalSyncMode::ASYNC) { thread_ = std::thread([this] { runFlusher(); }); }
                    }

                    ~ShardWriter() noexcept {
                        try { close(); }
                        catch (...) {}
                    }

                    ShardWriter(const ShardWriter&) = delete;
                    ShardWriter& operator=(const ShardWriter&) = delete;

                    void append(PendingEntry entry) {
                        checkAsyncError();
                        if (options_.syncMode == WalSyncMode::ASYNC) {
                            const uint64_t entryBytes = static_cast<uint64_t>(entry.bytes.size());
                            {
                                std::unique_lock lock{queueMutex_};
                                queueSpaceCv_.wait(
                                    lock,
                                    [&] {
                                        const uint64_t pendingBytes = queueBytes_ + inFlightBytes_;
                                        return asyncError_ || pendingBytes + entryBytes <= options_.asyncMaxPendingBytes || (queue_.empty()
                                            && inFlightBytes_ == 0);
                                    }
                                );
                                if (asyncError_) { std::rethrow_exception(asyncError_); }
                                queue_.push_back(std::move(entry));
                                queueBytes_ += entryBytes;
                            }
                            queueCv_.notify_one();
                            return;
                        }

                        std::lock_guard fileLock{fileMutex_};
                        writeOneLocked(entry);
                        std::fflush(file_);
                        batchesFlushed_.fetch_add(1, std::memory_order_relaxed);
                        if (options_.syncMode == WalSyncMode::SYNC) {
                            doFdatasync(file_);
                            syncsExecuted_.fetch_add(1, std::memory_order_relaxed);
                        }
                    }

                    void forceSync() {
                        checkAsyncError();
                        if (options_.syncMode == WalSyncMode::ASYNC) { drainAsync(); }

                        std::lock_guard fileLock{fileMutex_};
                        updateHeaderLocked();
                        std::fflush(file_);
                        doFdatasync(file_);
                        syncsExecuted_.fetch_add(1, std::memory_order_relaxed);
                    }

                    void pruneUntil(uint64_t checkpointSeq) {
                        forceSync();
                        std::vector<fs::path> removable;

                        {
                            std::lock_guard fileLock{fileMutex_};
                            for (const auto& entry : fs::directory_iterator(options_.walDir)) {
                                if (!entry.is_regular_file() || entry.path().extension() != ".akwal" || entry.path() == path_) { continue; }

                                std::ifstream file(entry.path(), std::ios::binary);
                                if (!file) { continue; }
                                uint8_t hdrBuf[WalSegmentHeader::SIZE]{};
                                if (!readExact(file, hdrBuf, WalSegmentHeader::SIZE)) { continue; }
                                const WalSegmentHeader hdr = WalSegmentHeader::deserialize(hdrBuf);
                                if (!hdr.verifyMagic() || !hdr.verifyVersion() || !hdr.verifyChecksum() || hdr.shardId != shardId_) {
                                    continue;
                                }

                                const SegmentScanResult scan = scanSegmentSequences(entry.path());
                                if (scan.validHeader && scan.lastSeq != 0 && scan.lastSeq <= checkpointSeq) {
                                    removable.push_back(entry.path());
                                }
                            }
                        }

                        for (const auto& path : removable) { fs::remove(path); }
                    }

                    void close() {
                        if (closed_) { return; }
                        if (options_.syncMode == WalSyncMode::ASYNC) {
                            {
                                std::lock_guard lock{queueMutex_};
                                running_ = false;
                            }
                            queueCv_.notify_one();
                            if (thread_.joinable()) { thread_.join(); }
                            checkAsyncError();
                        }

                        std::lock_guard fileLock{fileMutex_};
                        if (file_ != nullptr) {
                            updateHeaderLocked();
                            std::fflush(file_);
                            if (options_.syncMode != WalSyncMode::OFF) {
                                doFdatasync(file_);
                                syncsExecuted_.fetch_add(1, std::memory_order_relaxed);
                            }
                            closeFile(file_);
                        }
                        closed_ = true;
                    }

                    [[nodiscard]] WalWriterSnapshot snapshot() const noexcept {
                        return {
                            1,
                            entriesWritten_.load(std::memory_order_relaxed),
                            bytesWritten_.load(std::memory_order_relaxed),
                            batchesFlushed_.load(std::memory_order_relaxed),
                            syncsExecuted_.load(std::memory_order_relaxed),
                            segmentRotations_.load(std::memory_order_relaxed)
                        };
                    }

                private:
                    void openSegment(uint64_t segmentId) {
                        path_ = segmentPath(options_.walDir, shardId_, segmentId);
                        const bool exists = fs::exists(path_) && fs::file_size(path_) >= WalSegmentHeader::SIZE;
                        file_ = openRw(path_, exists);

                        if (exists) {
                            uint8_t hdrBuf[WalSegmentHeader::SIZE]{};
                            if (std::fseek(file_, 0, SEEK_SET) != 0 || std::fread(hdrBuf, 1, WalSegmentHeader::SIZE, file_) !=
                                WalSegmentHeader::SIZE) {
                                throw std::runtime_error("WAL failed to read segment header: " + path_.string());
                            }
                            header_ = WalSegmentHeader::deserialize(hdrBuf);
                            if (!header_.verifyMagic() || !header_.verifyVersion() || !header_.verifyChecksum() || header_.shardId !=
                                shardId_) {
                                closeFile(file_);
                                ++segmentId_;
                                openSegment(segmentId_);
                                return;
                            }

                            const SegmentScanResult scan = scanSegmentSequences(path_);
                            if (scan.validHeader) {
                                header_.firstSeq = scan.firstSeq;
                                header_.lastSeq = scan.lastSeq;
                            }
                            currentSize_ = fs::file_size(path_);
                            if (currentSize_ >= SEGMENT_BYTES) {
                                rotateLocked();
                                return;
                            }
                            std::fseek(file_, 0, SEEK_END);
                            return;
                        }

                        header_ = WalSegmentHeader::build(shardId_, segmentId_, nowUs());
                        currentSize_ = 0;
                        updateHeaderLocked();
                        currentSize_ = WalSegmentHeader::SIZE;
                        std::fflush(file_);
                    }

                    void rotateLocked() {
                        updateHeaderLocked();
                        std::fflush(file_);
                        if (options_.syncMode != WalSyncMode::OFF) {
                            doFdatasync(file_);
                            syncsExecuted_.fetch_add(1, std::memory_order_relaxed);
                        }
                        closeFile(file_);
                        ++segmentId_;
                        segmentRotations_.fetch_add(1, std::memory_order_relaxed);
                        openSegment(segmentId_);
                    }

                    void updateHeaderLocked() {
                        if (file_ == nullptr) { return; }
                        refreshSegmentCrc(header_);
                        uint8_t buf[WalSegmentHeader::SIZE]{};
                        header_.serialize(buf);
                        if (std::fseek(file_, 0, SEEK_SET) != 0) { throw std::runtime_error("WAL seek header failed"); }
                        writeAll(file_, buf, sizeof(buf));
                        if (std::fseek(file_, 0, SEEK_END) != 0) { throw std::runtime_error("WAL seek end failed"); }
                    }

                    void writeOneLocked(const PendingEntry& entry) {
                        if (entry.bytes.size() + WalSegmentHeader::SIZE > SEGMENT_BYTES) {
                            throw std::invalid_argument("WAL entry exceeds segment capacity");
                        }
                        if (currentSize_ + entry.bytes.size() > SEGMENT_BYTES && currentSize_ > WalSegmentHeader::SIZE) { rotateLocked(); }

                        if (header_.firstSeq == 0 || entry.seq < header_.firstSeq) { header_.firstSeq = entry.seq; }
                        if (entry.seq > header_.lastSeq) { header_.lastSeq = entry.seq; }
                        writeAll(file_, entry.bytes.data(), entry.bytes.size());
                        currentSize_ += entry.bytes.size();
                        entriesWritten_.fetch_add(1, std::memory_order_relaxed);
                        bytesWritten_.fetch_add(static_cast<uint64_t>(entry.bytes.size()), std::memory_order_relaxed);
                    }

                    void runFlusher() {
                        std::vector<PendingEntry> batch;
                        batch.reserve(options_.groupN == 0 ? 128 : options_.groupN);

                        try {
                            while (true) {
                                {
                                    std::unique_lock lock{queueMutex_};
                                    queueCv_.wait(lock, [this] { return !queue_.empty() || !running_; });
                                    if (!running_ && queue_.empty()) { break; }
                                    if (running_ && queue_.size() < options_.groupN && queueBytes_ < options_.groupBytes) {
                                        queueCv_.wait_for(
                                            lock,
                                            std::chrono::microseconds(options_.groupMicros),
                                            [this] {
                                                return queue_.size() >= options_.groupN || queueBytes_ >= options_.groupBytes || !running_;
                                            }
                                        );
                                    }
                                    inFlight_ = true;
                                    inFlightBytes_ = queueBytes_;
                                    std::swap(batch, queue_);
                                    queueBytes_ = 0;
                                }
                                queueSpaceCv_.notify_all();

                                if (!batch.empty()) {
                                    {
                                        std::lock_guard fileLock{fileMutex_};
                                        for (const PendingEntry& entry : batch) { writeOneLocked(entry); }
                                        std::fflush(file_);
                                        doFdatasync(file_);
                                        batchesFlushed_.fetch_add(1, std::memory_order_relaxed);
                                        syncsExecuted_.fetch_add(1, std::memory_order_relaxed);
                                    }
                                    batch.clear();
                                }

                                {
                                    std::lock_guard lock{queueMutex_};
                                    inFlight_ = false;
                                    inFlightBytes_ = 0;
                                }
                                queueCv_.notify_all();
                                queueSpaceCv_.notify_all();
                            }
                        }
                        catch (...) {
                            {
                                std::lock_guard lock{queueMutex_};
                                asyncError_ = std::current_exception();
                                inFlight_ = false;
                                inFlightBytes_ = 0;
                            }
                            queueCv_.notify_all();
                            queueSpaceCv_.notify_all();
                        }
                    }

                    void drainAsync() {
                        {
                            std::unique_lock lock{queueMutex_};
                            queueCv_.wait(lock, [this] { return queue_.empty() && !inFlight_; });
                        }
                        checkAsyncError();
                    }

                    void checkAsyncError() {
                        std::lock_guard lock{queueMutex_};
                        if (asyncError_) { std::rethrow_exception(asyncError_); }
                    }

                    WalOptions options_;
                    uint16_t shardId_ = 0;
                    uint64_t segmentId_ = 0;
                    fs::path path_;
                    FILE* file_ = nullptr;
                    WalSegmentHeader header_{};
                    uint64_t currentSize_ = 0;
                    bool closed_ = false;

                    mutable std::mutex fileMutex_;
                    std::mutex queueMutex_;
                    std::condition_variable queueCv_;
                    std::condition_variable queueSpaceCv_;
                    std::vector<PendingEntry> queue_;
                    uint64_t queueBytes_ = 0;
                    bool running_ = false;
                    bool inFlight_ = false;
                    uint64_t inFlightBytes_ = 0;
                    std::exception_ptr asyncError_;
                    std::thread thread_;

                    std::atomic<uint64_t> entriesWritten_{0};
                    std::atomic<uint64_t> bytesWritten_{0};
                    std::atomic<uint64_t> batchesFlushed_{0};
                    std::atomic<uint64_t> syncsExecuted_{0};
                    std::atomic<uint64_t> segmentRotations_{0};
            };

            explicit Impl(WalOptions options)
                : options_{std::move(options)} {
                if (options_.walDir.empty()) { throw std::invalid_argument("WAL directory is required"); }
                if (options_.shardCount == 0) { options_.shardCount = resolveAutoShardCount(); }
                if (options_.shardCount > 16) { throw std::invalid_argument("WAL shardCount must be in range 1..16, or 0 for auto"); }
                if (options_.groupN == 0) { options_.groupN = 128; }
                if (options_.groupMicros == 0) { options_.groupMicros = 100; }
                if (options_.groupBytes == 0) { options_.groupBytes = 4ULL * 1024ULL * 1024ULL; }
                if (options_.asyncMaxPendingBytes == 0) { options_.asyncMaxPendingBytes = 64ULL * 1024ULL * 1024ULL; }
                if (options_.asyncMaxPendingBytes < options_.groupBytes) { options_.asyncMaxPendingBytes = options_.groupBytes; }

                shards_.reserve(options_.shardCount);
                for (uint16_t i = 0; i < options_.shardCount; ++i) { shards_.push_back(std::make_unique<ShardWriter>(options_, i)); }
            }

            ~Impl() { close(); }

            void append(
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint64_t seq,
                uint8_t flags,
                uint64_t precomputedFp64
            ) {
                const uint64_t fp64 = precomputedFp64 != 0
                                          ? precomputedFp64
                                          : (key.empty() ? 0 : core::computeKeyFp64(key.data(), key.size()));
                const uint16_t shardId = shardFor(fp64, options_.shardCount);
                PendingEntry entry{seq, serializeEntry(key, value, seq, fp64, flags)};
                shards_[shardId]->append(std::move(entry));
            }

            void forceSync() { for (const auto& shard : shards_) { shard->forceSync(); } }

            void pruneUntil(uint64_t checkpointSeq) { for (const auto& shard : shards_) { shard->pruneUntil(checkpointSeq); } }

            [[nodiscard]] WalWriterSnapshot snapshot() const noexcept {
                WalWriterSnapshot out;
                out.shardCount = static_cast<uint32_t>(shards_.size());
                for (const auto& shard : shards_) {
                    const auto snap = shard->snapshot();
                    out.entriesWritten += snap.entriesWritten;
                    out.bytesWritten += snap.bytesWritten;
                    out.batchesFlushed += snap.batchesFlushed;
                    out.syncsExecuted += snap.syncsExecuted;
                    out.segmentRotations += snap.segmentRotations;
                }
                return out;
            }

            void close() {
                if (closed_) { return; }
                for (const auto& shard : shards_) { shard->close(); }
                closed_ = true;
            }

            WalOptions options_;
            std::vector<std::unique_ptr<ShardWriter>> shards_;
            bool closed_ = false;
    };

    WalWriter::WalWriter() = default;

    std::unique_ptr<WalWriter> WalWriter::create(WalOptions options) {
        auto writer = std::unique_ptr < WalWriter > (new WalWriter{});
        writer->impl_ = std::make_unique<Impl>(std::move(options));
        return writer;
    }

    WalWriter::~WalWriter() {
        try { close(); }
        catch (...) {}
    }

    void WalWriter::append(
        std::span<const uint8_t> key,
        std::span<const uint8_t> value,
        uint64_t seq,
        uint8_t flags,
        uint64_t precomputedFp64
    ) { impl_->append(key, value, seq, flags, precomputedFp64); }

    void WalWriter::forceSync() { impl_->forceSync(); }

    void WalWriter::pruneUntil(uint64_t checkpointSeq) { impl_->pruneUntil(checkpointSeq); }

    WalWriterSnapshot WalWriter::snapshot() const noexcept { return impl_ ? impl_->snapshot() : WalWriterSnapshot{}; }

    void WalWriter::close() { if (impl_) { impl_->close(); } }
} // namespace akkaradb::engine::wal

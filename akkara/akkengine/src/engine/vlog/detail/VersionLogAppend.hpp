/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/vlog/detail/VersionLogAppend.hpp
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
    if (ENTRY_HDR_SIZE + keyLen + valueLen + CRC_SIZE > MAX_ENTRY_SIZE) { throw std::invalid_argument("VersionLog: entry too large"); }

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
            encodedValue[0] = static_cast<uint8_t>(rawSize & 0xffu);
            encodedValue[1] = static_cast<uint8_t>((rawSize >> 8u) & 0xffu);
            encodedValue[2] = static_cast<uint8_t>((rawSize >> 16u) & 0xffu);
            encodedValue[3] = static_cast<uint8_t>((rawSize >> 24u) & 0xffu);
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
    AkvlogV5EntryHeader hdr{};
    hdr.entryLen = entryLen;
    hdr.seq = seq;
    hdr.sourceNodeId = sourceNodeId;
    hdr.timestampNs = timestampNs;
    hdr.flags = flags;
    hdr.keyFp64 = keyLen == 0 ? 0ULL : core::computeKeyFp64(keyData, keyLen);
    hdr.keyLen = static_cast<uint16_t>(keyLen);
    hdr.valueLen = static_cast<uint32_t>(storedValueLen);
    const auto encodedHeader = encodeEntryHeader(hdr);
    std::memcpy(out.data(), encodedHeader.data(), encodedHeader.size());
    uint8_t* p = out.data() + ENTRY_HDR_SIZE;

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
    writeU32LeAt(out, entryLen - CRC_SIZE, crc);
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
                        [this] { return closing_ || pendingWrites_.size() >= opts_.groupN || pendingBytes_ >= opts_.groupBytes; }
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
            flushChecked(file_);
            if (opts_.syncMode == VLogSyncMode::BATCHED_SYNC) { fdatasyncChecked(file_); }
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
    if (!file_) { throw std::runtime_error("VersionLog: append rejected after close"); }

    if (opts_.syncMode == VLogSyncMode::SYNC) {
        write.offset = activeSegmentBytes_;
        writeSerialized(file_, write.bytes);
        flushChecked(file_);
        fdatasyncChecked(file_);
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
            return asyncFailed_.load(std::memory_order_acquire) || closing_ || pendingBytes_ + entryBytes <= opts_.asyncMaxPendingBytes ||
                pendingWrites_.empty();
        }
    );
    checkAsyncError();
    if (closing_) { throw std::runtime_error("VersionLog: append rejected while flusher is stopping"); }
    pendingWrites_.push_back(std::move(write));
    pendingBytes_ += entryBytes;
    flushCv_.notify_one();
    return true;
}

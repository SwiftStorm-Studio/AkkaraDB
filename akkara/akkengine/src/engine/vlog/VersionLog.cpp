/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

// akkengine/src/engine/vlog/VersionLog.cpp
#include "akk/engine/vlog/VersionLog.hpp"

#include "akk/cpu/CRC32C.hpp"
#include "akk/core/record/KeyFingerprint.hpp"

#include <algorithm>
#include <condition_variable>
#include <deque>
#include <exception>
#include <chrono>
#include <limits>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
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

        static_assert(sizeof(AkvlogV5FileHeader) == 32);
        static_assert(sizeof(AkvlogV5EntryHeader) == 43);

        static constexpr size_t ENTRY_HDR_SIZE = sizeof(AkvlogV5EntryHeader);
        static constexpr size_t CRC_SIZE = sizeof(uint32_t);
        static constexpr size_t MIN_ENTRY_SIZE = ENTRY_HDR_SIZE + CRC_SIZE;
        static constexpr size_t MAX_ENTRY_SIZE = 32u * 1024u * 1024u;

        [[nodiscard]] static uint64_t nowNsFallback() noexcept { return 0; }

        static void doFdatasync(FILE* f) {
            #ifdef _WIN32
            _commit(_fileno(f));
            #else
            fdatasync(fileno(f));
            #endif
        }
    } // namespace

    VersionLog::VersionLog() = default;

    class VersionLog::Impl {
        public:
            VersionLogOptions opts_;
            mutable std::mutex mu_;
            std::condition_variable flushCv_;
            std::condition_variable queueSpaceCv_;

            struct StringViewHash {
                using is_transparent = void;
                size_t operator()(std::string_view sv) const noexcept { return std::hash<std::string_view>{}(sv); }
                size_t operator()(const std::string& s) const noexcept { return std::hash<std::string_view>{}(s); }
            };

            std::unordered_map<std::string, std::vector<VersionEntry>, StringViewHash, std::equal_to<>> index_;
            FILE* file_ = nullptr;
            std::deque<std::vector<uint8_t>> pendingWrites_;
            std::thread flushThread_;
            std::exception_ptr asyncError_;
            bool closing_ = false;
            uint64_t pendingBytes_ = 0;

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
                const size_t total = ENTRY_HDR_SIZE + keyLen + valueLen + CRC_SIZE;
                if (total > MAX_ENTRY_SIZE) { throw std::invalid_argument("VersionLog: entry too large"); }
                if (keyLen > std::numeric_limits<uint16_t>::max()) { throw std::invalid_argument("VersionLog: key too large"); }
                if (valueLen > std::numeric_limits<uint32_t>::max()) { throw std::invalid_argument("VersionLog: value too large"); }

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
                hdr.valueLen = static_cast<uint32_t>(valueLen);
                p += ENTRY_HDR_SIZE;

                if (keyLen > 0) {
                    std::memcpy(p, keyData, keyLen);
                    p += keyLen;
                }
                if (valueLen > 0) {
                    std::memcpy(p, valueData, valueLen);
                    p += valueLen;
                }

                std::memset(p, 0, CRC_SIZE);
                const uint32_t crc = cpu::CRC32C(reinterpret_cast<const std::byte*>(out.data()), entryLen - CRC_SIZE);
                std::memcpy(p, &crc, CRC_SIZE);
                return out;
            }

            static void writeSerialized(FILE* f, std::span<const uint8_t> bytes) {
                if (fwrite(bytes.data(), 1, bytes.size(), f) != bytes.size()) { throw std::runtime_error("VersionLog: fwrite failed"); }
            }

            void checkAsyncErrorLocked() const {
                if (asyncError_) { std::rethrow_exception(asyncError_); }
            }

            void flushLoop() {
                std::deque<std::vector<uint8_t>> batch;
                while (true) {
                    {
                        std::unique_lock lock{mu_};
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
                                )) {
                                    break;
                                }
                                break;
                            }
                        }

                        batch.swap(pendingWrites_);
                        pendingBytes_ = 0;
                        queueSpaceCv_.notify_all();
                    }

                    try {
                        for (const auto& entry : batch) { writeSerialized(file_, entry); }
                        fflush(file_);
                        if (opts_.syncMode == VLogSyncMode::BATCHED_SYNC) { doFdatasync(file_); }
                    }
                    catch (...) {
                        std::lock_guard lock{mu_};
                        if (!asyncError_) { asyncError_ = std::current_exception(); }
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
                    std::lock_guard lock{mu_};
                    closing_ = true;
                    flushCv_.notify_all();
                }
                flushThread_.join();
            }

            void writeEntry(
                std::span<const uint8_t> key,
                uint64_t seq,
                uint64_t sourceNodeId,
                uint64_t timestampNs,
                uint8_t flags,
                std::span<const uint8_t> value
            ) {
                auto bytes = serializeEntry(key.data(), key.size(), seq, sourceNodeId, timestampNs, flags, value.data(), value.size());
                if (opts_.syncMode == VLogSyncMode::SYNC) {
                    writeSerialized(file_, bytes);
                    fflush(file_);
                    doFdatasync(file_);
                    return;
                }

                pendingWrites_.push_back(std::move(bytes));
                flushCv_.notify_one();
            }

            void writeFileHeader(FILE* wf) {
                AkvlogV5FileHeader hdr{};
                hdr.magic = AKVLOG_V5_MAGIC;
                hdr.version = AKVLOG_V5_VERSION;
                hdr.syncModeHint = static_cast<uint8_t>(opts_.syncMode);
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

            void openOrCreate() {
                const auto& path = opts_.logPath;
                const auto parent = path.parent_path();
                if (!parent.empty()) { fs::create_directories(parent); }

                const bool existed = fs::exists(path);
                if (existed) {
                    FILE* rf = nullptr;
                    #ifdef _WIN32
                    rf = _wfopen(path.wstring().c_str(), L"rb");
                    #else
                    rf = fopen(path.string().c_str(), "rb");
                    #endif
                    if (rf != nullptr) {
                        try {
                            recover(rf);
                        }
                        catch (...) {
                            fclose(rf);
                            throw;
                        }
                        fclose(rf);
                    }
                }

                #ifdef _WIN32
                file_ = _wfopen(path.wstring().c_str(), L"ab");
                #else
                file_ = fopen(path.string().c_str(), "ab");
                #endif
                if (!file_) { throw std::runtime_error("VersionLog: cannot open file: " + path.string()); }

                if (!existed || fs::file_size(path) == 0) { writeFileHeader(file_); }
                startAsyncWorkerIfNeeded();
            }

            void insertSorted(const std::string& key, VersionEntry ve) {
                auto& versions = index_[key];
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

            void recover(FILE* rf) {
                AkvlogV5FileHeader fileHdr{};
                if (fread(&fileHdr, sizeof(fileHdr), 1, rf) != 1) { return; }

                const uint32_t storedHeaderCrc = fileHdr.crc32c;
                fileHdr.crc32c = 0;
                const uint32_t computedHeaderCrc = cpu::CRC32C(reinterpret_cast<const std::byte*>(&fileHdr), sizeof(fileHdr));
                if (fileHdr.magic != AKVLOG_V5_MAGIC || fileHdr.version != AKVLOG_V5_VERSION || storedHeaderCrc != computedHeaderCrc) {
                    throw std::runtime_error("VersionLog: corrupt file header");
                }

                std::vector<uint8_t> buf;
                while (true) {
                    uint32_t entryLen = 0;
                    if (fread(&entryLen, sizeof(entryLen), 1, rf) != 1) { break; }
                    if (entryLen < MIN_ENTRY_SIZE || entryLen > MAX_ENTRY_SIZE) {
                        throw std::runtime_error("VersionLog: corrupt entry length");
                    }

                    buf.resize(entryLen);
                    std::memcpy(buf.data(), &entryLen, sizeof(entryLen));

                    const size_t rest = entryLen - sizeof(entryLen);
                    if (fread(buf.data() + sizeof(entryLen), 1, rest, rf) != rest) {
                        throw std::runtime_error("VersionLog: truncated entry");
                    }

                    uint32_t storedEntryCrc = 0;
                    std::memcpy(&storedEntryCrc, buf.data() + entryLen - CRC_SIZE, CRC_SIZE);
                    std::memset(buf.data() + entryLen - CRC_SIZE, 0, CRC_SIZE);
                    const uint32_t computedEntryCrc = cpu::CRC32C(reinterpret_cast<const std::byte*>(buf.data()), entryLen - CRC_SIZE);
                    if (storedEntryCrc != computedEntryCrc) { throw std::runtime_error("VersionLog: entry CRC mismatch"); }

                    if (buf.size() < ENTRY_HDR_SIZE) { throw std::runtime_error("VersionLog: corrupt entry header"); }
                    const auto& ehdr = *reinterpret_cast<const AkvlogV5EntryHeader*>(buf.data());
                    const size_t expectedSize = ENTRY_HDR_SIZE + ehdr.keyLen + ehdr.valueLen + CRC_SIZE;
                    if (expectedSize != entryLen) { throw std::runtime_error("VersionLog: corrupt entry payload"); }

                    const uint8_t* p = buf.data() + ENTRY_HDR_SIZE;
                    std::string key(reinterpret_cast<const char*>(p), ehdr.keyLen);
                    p += ehdr.keyLen;

                    VersionEntry ve;
                    ve.seq = ehdr.seq;
                    ve.sourceNodeId = ehdr.sourceNodeId;
                    ve.timestampNs = ehdr.timestampNs;
                    ve.flags = ehdr.flags;
                    ve.value.assign(p, p + ehdr.valueLen);
                    insertSorted(key, std::move(ve));
                }
            }
    };

    std::unique_ptr<VersionLog> VersionLog::create(VersionLogOptions opts) {
        auto log = std::unique_ptr<VersionLog>(new VersionLog{});
        log->impl_ = std::make_unique<Impl>();
        log->impl_->opts_ = std::move(opts);
        log->impl_->openOrCreate();
        return log;
    }

    VersionLog::~VersionLog() {
        try {
            close();
        }
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
        if (!impl_) { return; }

        std::unique_lock lock{impl_->mu_};
        impl_->checkAsyncErrorLocked();
        if (!impl_->file_) { return; }

        if (impl_->opts_.syncMode != VLogSyncMode::SYNC) {
            auto bytes = impl_->serializeEntry(key.data(), key.size(), seq, sourceNodeId, timestampNs, flags, value.data(), value.size());
            const uint64_t entryBytes = static_cast<uint64_t>(bytes.size());
            impl_->queueSpaceCv_.wait(
                lock,
                [&] {
                    return impl_->asyncError_ || impl_->closing_ || impl_->pendingBytes_ + entryBytes <= impl_->opts_.asyncMaxPendingBytes ||
                           impl_->pendingWrites_.empty();
                }
            );
            impl_->checkAsyncErrorLocked();
            if (impl_->closing_) { throw std::runtime_error("VersionLog: append rejected while flusher is stopping"); }
            impl_->pendingWrites_.push_back(std::move(bytes));
            impl_->pendingBytes_ += entryBytes;
            impl_->flushCv_.notify_one();
        }
        else { impl_->writeEntry(key, seq, sourceNodeId, timestampNs, flags, value); }

        const std::string keyStr(reinterpret_cast<const char*>(key.data()), key.size());
        VersionEntry ve;
        ve.seq = seq;
        ve.sourceNodeId = sourceNodeId;
        ve.timestampNs = timestampNs;
        ve.flags = flags;
        ve.value.assign(value.begin(), value.end());
        impl_->insertSorted(keyStr, std::move(ve));
    }

    std::optional<VersionEntry> VersionLog::getAt(std::span<const uint8_t> key, uint64_t atSeq) const {
        if (!impl_) { return std::nullopt; }

        std::lock_guard lock{impl_->mu_};
        impl_->checkAsyncErrorLocked();
        const std::string_view keySv(reinterpret_cast<const char*>(key.data()), key.size());
        const auto it = impl_->index_.find(keySv);
        if (it == impl_->index_.end()) { return std::nullopt; }

        const auto& versions = it->second;
        const auto pos = std::upper_bound(
            versions.begin(),
            versions.end(),
            atSeq,
            [](uint64_t seq, const VersionEntry& e) { return seq < e.seq; }
        );
        if (pos == versions.begin()) { return std::nullopt; }
        return *std::prev(pos);
    }

    std::vector<VersionEntry> VersionLog::history(std::span<const uint8_t> key) const {
        if (!impl_) { return {}; }

        std::lock_guard lock{impl_->mu_};
        impl_->checkAsyncErrorLocked();
        const std::string_view keySv(reinterpret_cast<const char*>(key.data()), key.size());
        const auto it = impl_->index_.find(keySv);
        if (it == impl_->index_.end()) { return {}; }
        return it->second;
    }

    std::vector<std::pair<std::vector<uint8_t>, std::optional<VersionEntry>>> VersionLog::collectRollbackTargets(uint64_t targetSeq) const {
        if (!impl_) { return {}; }

        std::lock_guard lock{impl_->mu_};
        impl_->checkAsyncErrorLocked();
        std::vector<std::pair<std::vector<uint8_t>, std::optional<VersionEntry>>> result;
        for (const auto& [key, versions] : impl_->index_) {
            if (versions.empty() || versions.back().seq <= targetSeq) { continue; }

            const auto pos = std::upper_bound(
                versions.begin(),
                versions.end(),
                targetSeq,
                [](uint64_t seq, const VersionEntry& e) { return seq < e.seq; }
            );

            std::optional<VersionEntry> prev;
            if (pos != versions.begin()) { prev = *std::prev(pos); }

            std::vector<uint8_t> keyBytes(key.begin(), key.end());
            result.emplace_back(std::move(keyBytes), std::move(prev));
        }

        return result;
    }

    void VersionLog::forceSync() {
        if (!impl_) { return; }

        if (impl_->opts_.syncMode == VLogSyncMode::SYNC) {
            std::lock_guard lock{impl_->mu_};
            impl_->checkAsyncErrorLocked();
            if (!impl_->file_) { return; }
            fflush(impl_->file_);
            doFdatasync(impl_->file_);
            return;
        }

        impl_->stopAsyncWorker();

        {
            std::lock_guard lock{impl_->mu_};
            impl_->checkAsyncErrorLocked();
            if (!impl_->file_) { return; }
            fflush(impl_->file_);
            doFdatasync(impl_->file_);
            impl_->closing_ = false;
        }

        impl_->startAsyncWorkerIfNeeded();
    }

    void VersionLog::close() {
        if (!impl_) { return; }
        impl_->stopAsyncWorker();

        std::exception_ptr asyncError;
        {
            std::lock_guard lock{impl_->mu_};
            asyncError = impl_->asyncError_;
            if (!impl_->file_) {
                if (asyncError) { std::rethrow_exception(asyncError); }
                return;
            }
            fflush(impl_->file_);
            fclose(impl_->file_);
            impl_->file_ = nullptr;
        }
        if (asyncError) { std::rethrow_exception(asyncError); }
    }
} // namespace akkaradb::engine::vlog

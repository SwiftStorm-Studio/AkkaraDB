/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/blob/BlobManager.cpp
#include "akk/engine/blob/BlobManager.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <zstd.h>

#ifdef _WIN32
#include <io.h>
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace akkaradb::engine::blob {
    namespace fs = std::filesystem;

    namespace {
        [[nodiscard]] std::string hex2(uint8_t value) {
            static constexpr char lut[] = "0123456789abcdef";
            std::string out(2, '0');
            out[0] = lut[value >> 4u];
            out[1] = lut[value & 0x0fu];
            return out;
        }

        [[nodiscard]] std::string hex16(uint64_t value) {
            static constexpr char lut[] = "0123456789abcdef";
            std::string out(16, '0');
            for (int i = 15; i >= 0; --i) {
                out[static_cast<size_t>(i)] = lut[value & 0x0fu];
                value >>= 4u;
            }
            return out;
        }

        [[nodiscard]] bool parseHex16(std::string_view text, uint64_t& out) noexcept {
            if (text.size() != 16) { return false; }
            uint64_t value = 0;
            const auto* first = text.data();
            const auto* last = text.data() + text.size();
            const auto [ptr, ec] = std::from_chars(first, last, value, 16);
            if (ec != std::errc{} || ptr != last) { return false; }
            out = value;
            return true;
        }

        [[nodiscard]] FILE* openFileWrite(const fs::path& path) {
            #ifdef _WIN32
            FILE* f = _wfopen(path.wstring().c_str(), L"wb");
            #else
            FILE* f = fopen(path.string().c_str(), "wb");
            #endif
            return f;
        }

        [[nodiscard]] FILE* openFileRead(const fs::path& path) {
            #ifdef _WIN32
            FILE* f = _wfopen(path.wstring().c_str(), L"rb");
            #else
            FILE* f = fopen(path.string().c_str(), "rb");
            #endif
            return f;
        }

        void syncFile(FILE* f) {
            fflush(f);
            #ifdef _WIN32
            if (_commit(_fileno(f)) != 0) { throw std::runtime_error("BlobManager: _commit failed"); }
            #else
            if (fsync(fileno(f)) != 0) { throw std::runtime_error("BlobManager: fsync failed"); }
            #endif
        }

        class Crc32cStream {
            public:
                void update(std::span<const uint8_t> bytes) noexcept {
                    for (const auto byte : bytes) { crc_ = (crc_ >> 8u) ^ table()[(crc_ ^ byte) & 0xffu]; }
                }

                [[nodiscard]] uint32_t finish() const noexcept { return ~crc_; }

            private:
                static const std::array<uint32_t, 256>& table() noexcept {
                    static const std::array<uint32_t, 256> values = [] {
                        std::array<uint32_t, 256> out{};
                        for (uint32_t i = 0; i < out.size(); ++i) {
                            uint32_t crc = i;
                            for (uint32_t bit = 0; bit < 8; ++bit) {
                                crc = (crc >> 1u) ^ (0x82F63B78u & (0u - (crc & 1u)));
                            }
                            out[i] = crc;
                        }
                        return out;
                    }();
                    return values;
                }

                uint32_t crc_ = 0xFFFFFFFFu;
        };

        [[nodiscard]] fs::path makeTempPath(const fs::path& path) {
            static std::atomic<uint64_t> sequence{0};
            #ifdef _WIN32
            const auto pid = static_cast<uint64_t>(::GetCurrentProcessId());
            #else
            const auto pid = static_cast<uint64_t>(::getpid());
            #endif
            const auto parent = path.parent_path();
            const auto stem = path.filename().string();
            for (uint32_t attempt = 0; attempt < 1024; ++attempt) {
                auto candidate = parent / (stem + ".tmp." + std::to_string(pid) + "." +
                                           std::to_string(sequence.fetch_add(1, std::memory_order_relaxed)) + "." +
                                           std::to_string(attempt));
                if (!fs::exists(candidate)) { return candidate; }
            }
            throw std::runtime_error("BlobManager: cannot allocate temp file name");
        }

        #ifndef _WIN32
        void syncParentDirectory(const fs::path& path) {
            const auto parent = path.parent_path().empty() ? fs::path{"."} : path.parent_path();
            int flags = O_RDONLY;
            #ifdef O_DIRECTORY
            flags |= O_DIRECTORY;
            #endif
            const int fd = ::open(parent.c_str(), flags);
            if (fd < 0) { throw std::runtime_error("BlobManager: cannot open parent directory for sync"); }
            const int rc = ::fsync(fd);
            const int closeRc = ::close(fd);
            if (rc != 0 || closeRc != 0) { throw std::runtime_error("BlobManager: parent directory sync failed"); }
        }
        #endif

        void replaceFileAtomically(const fs::path& tmp, const fs::path& path) {
            #ifdef _WIN32
            if (!::MoveFileExW(tmp.wstring().c_str(), path.wstring().c_str(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
                throw std::runtime_error("BlobManager: rename tmp to akblob failed: " + path.string());
            }
            #else
            fs::rename(tmp, path);
            syncParentDirectory(path);
            #endif
        }

        void writeAll(FILE* f, const uint8_t* data, size_t size) {
            while (size > 0) {
                const size_t n = fwrite(data, 1, size, f);
                if (n == 0) { throw std::runtime_error("BlobManager: fwrite failed"); }
                data += n;
                size -= n;
            }
        }

        void writeAtomicSplit(const fs::path& path, const uint8_t* header, size_t headerSize, const uint8_t* payload, size_t payloadSize) {
            fs::create_directories(path.parent_path());

            const fs::path tmp = makeTempPath(path);
            {
                FILE* f = openFileWrite(tmp);
                if (!f) { throw std::runtime_error("BlobManager: cannot open tmp file: " + tmp.string()); }
                try {
                    writeAll(f, header, headerSize);
                    if (payloadSize > 0) { writeAll(f, payload, payloadSize); }
                    syncFile(f);
                    fclose(f);
                }
                catch (...) {
                    fclose(f);
                    std::error_code ec;
                    fs::remove(tmp, ec);
                    throw;
                }
            }

            try { replaceFileAtomically(tmp, path); }
            catch (...) {
                std::error_code ec;
                fs::remove(tmp, ec);
                throw;
            }
        }

        [[nodiscard]] AkBlobHeaderV5 readBlobHeaderOnly(const fs::path& path) {
            FILE* f = openFileRead(path);
            if (!f) { throw std::runtime_error("BlobManager: cannot open file: " + path.string()); }
            uint8_t headerBuf[AKBLOB_HEADER_SIZE_V5]{};
            try {
                if (fread(headerBuf, 1, sizeof(headerBuf), f) != sizeof(headerBuf)) {
                    throw std::runtime_error("BlobManager: cannot read header: " + path.string());
                }
                fclose(f);
            }
            catch (...) {
                fclose(f);
                throw;
            }
            auto header = deserializeBlobHeader(headerBuf);
            if (!verifyBlobHeader(header)) { throw std::runtime_error("BlobManager: header corrupt: " + path.string()); }
            return header;
        }

        [[nodiscard]] std::vector<uint8_t> readFile(const fs::path& path) {
            FILE* f = openFileRead(path);
            if (!f) { throw std::runtime_error("BlobManager: cannot open file: " + path.string()); }
            try {
                if (fseek(f, 0, SEEK_END) != 0) { throw std::runtime_error("BlobManager: seek failed"); }
                const long sz = ftell(f);
                if (sz < 0) { throw std::runtime_error("BlobManager: tell failed"); }
                if (fseek(f, 0, SEEK_SET) != 0) { throw std::runtime_error("BlobManager: seek failed"); }

                std::vector<uint8_t> out(static_cast<size_t>(sz));
                size_t offset = 0;
                while (offset < out.size()) {
                    const size_t n = fread(out.data() + offset, 1, out.size() - offset, f);
                    if (n == 0) { throw std::runtime_error("BlobManager: fread failed"); }
                    offset += n;
                }
                fclose(f);
                return out;
            }
            catch (...) {
                fclose(f);
                throw;
            }
        }

        [[nodiscard]] bool removeQuiet(const fs::path& path) noexcept {
            std::error_code ec;
            return fs::remove(path, ec);
        }

        [[nodiscard]] bool renameQuiet(const fs::path& src, const fs::path& dst) noexcept {
            std::error_code ec;
            fs::rename(src, dst, ec);
            if (!ec) { return true; }
            fs::remove(dst, ec);
            ec.clear();
            fs::rename(src, dst, ec);
            return !ec;
        }
    } // namespace

    class BlobManager::Impl {
        public:
            explicit Impl(fs::path blobDirValue, Options optionsValue)
                : blobDir(std::move(blobDirValue)), options(std::move(optionsValue)) {}

            fs::path blobDir;
            Options options;
            mutable std::mutex writeMu;
            std::mutex delMu;
            std::condition_variable delCv;
            std::vector<uint64_t> delQueue;
            std::thread gcThread;
            std::atomic<bool> running{false};
            std::atomic<bool> started{false};
            mutable std::atomic<uint64_t> blobsWritten{0};
            mutable std::atomic<uint64_t> bytesUncompressed{0};
            mutable std::atomic<uint64_t> bytesOnDisk{0};
            mutable std::atomic<uint64_t> blobsDeleted{0};
            mutable std::atomic<uint64_t> gcCycles{0};

            struct PendingStreamingBlob {
                fs::path path;
                fs::path tmp;
                FILE* file = nullptr;
                uint64_t totalSize = 0;
                uint64_t written = 0;
                uint32_t expectedCrc32c = 0;
                Crc32cStream crc;
                bool skipExisting = false;
            };

            std::unordered_map<uint64_t, PendingStreamingBlob> pendingStreamingBlobs;

            [[nodiscard]] fs::path pathFor(uint64_t blobId) const {
                const uint8_t hi = static_cast<uint8_t>(blobId >> 56u);
                return blobDir / hex2(hi) / (hex16(blobId) + ".akblob");
            }

            void createShards() const {
                fs::create_directories(blobDir);
                for (uint32_t i = 0; i < 256; ++i) { fs::create_directories(blobDir / hex2(static_cast<uint8_t>(i))); }
            }

            void startupCleanup() const {
                std::error_code ec;
                if (!fs::exists(blobDir, ec)) { return; }
                for (const auto& entry : fs::recursive_directory_iterator(blobDir, ec)) {
                    if (ec) { break; }
                    if (!entry.is_regular_file(ec)) { continue; }
                    const auto name = entry.path().filename().string();
                    if (name.find(".akblob.tmp.") != std::string::npos || name.ends_with(".akblob.tmp") || name.ends_with(".akblob.del")) {
                        (void)removeQuiet(entry.path());
                    }
                }
            }

            [[nodiscard]] std::vector<uint8_t> maybeCompress(std::span<const uint8_t> content, BlobCodec& actualCodec) const {
                actualCodec = BlobCodec::NONE;
                if (options.codec != BlobCodec::ZSTD || content.empty()) { return {}; }

                const size_t bound = ZSTD_compressBound(content.size());
                std::vector<uint8_t> compressed(bound);
                const size_t n = ZSTD_compress(compressed.data(), compressed.size(), content.data(), content.size(), ZSTD_CLEVEL_DEFAULT);
                if (ZSTD_isError(n) || n >= content.size()) { return {}; }
                compressed.resize(n);
                actualCodec = BlobCodec::ZSTD;
                return compressed;
            }

            void writeBlob(uint64_t blobId, std::span<const uint8_t> content, const fs::path& path) const {
                BlobCodec actualCodec = BlobCodec::NONE;
                std::vector<uint8_t> compressed = maybeCompress(content, actualCodec);

                const uint8_t* payload = content.data();
                size_t payloadSize = content.size();
                if (actualCodec == BlobCodec::ZSTD) {
                    payload = compressed.data();
                    payloadSize = compressed.size();
                }
                if (payloadSize > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
                    throw std::invalid_argument("BlobManager: payload too large");
                }

                const uint32_t contentCrc = crc32c(content);
                const auto header = buildBlobHeader(blobId, content.size(), payloadSize, actualCodec, contentCrc);
                uint8_t headerBuf[AKBLOB_HEADER_SIZE_V5]{};
                serializeBlobHeader(header, headerBuf);
                writeAtomicSplit(path, headerBuf, sizeof(headerBuf), payload, payloadSize);
                if (options.onBlobPut) {
                    options.onBlobPut(
                        blobId,
                        static_cast<uint64_t>(content.size()),
                        static_cast<uint64_t>(payloadSize),
                        contentCrc,
                        static_cast<uint32_t>(actualCodec)
                    );
                }
                blobsWritten.fetch_add(1, std::memory_order_relaxed);
                bytesUncompressed.fetch_add(static_cast<uint64_t>(content.size()), std::memory_order_relaxed);
                bytesOnDisk.fetch_add(
                    static_cast<uint64_t>(sizeof(headerBuf)) + static_cast<uint64_t>(payloadSize),
                    std::memory_order_relaxed
                );
            }

            static void abortPendingNoThrow(PendingStreamingBlob& pending) noexcept {
                if (pending.file != nullptr) {
                    (void)fflush(pending.file);
                    (void)fclose(pending.file);
                    pending.file = nullptr;
                }
                if (!pending.tmp.empty()) {
                    std::error_code ec;
                    fs::remove(pending.tmp, ec);
                }
            }

            void beginStreamingBlob(uint64_t blobId, uint64_t totalSize, uint32_t contentCrc32c) {
                if (pendingStreamingBlobs.contains(blobId)) {
                    throw std::runtime_error("BlobManager: streaming blob write is already active");
                }
                const auto path = pathFor(blobId);
                if (fs::exists(path)) {
                    const auto header = readBlobHeaderOnly(path);
                    if (header.blobId != blobId || header.totalSize != totalSize || header.contentCrc32c != contentCrc32c) {
                        throw std::runtime_error("BlobManager: existing streaming blob metadata mismatch");
                    }
                    PendingStreamingBlob pending;
                    pending.path = path;
                    pending.totalSize = totalSize;
                    pending.written = totalSize;
                    pending.expectedCrc32c = contentCrc32c;
                    pending.skipExisting = true;
                    pendingStreamingBlobs.emplace(blobId, std::move(pending));
                    return;
                }

                fs::create_directories(path.parent_path());
                auto tmp = makeTempPath(path);
                FILE* file = openFileWrite(tmp);
                if (!file) { throw std::runtime_error("BlobManager: cannot open streaming tmp file: " + tmp.string()); }
                PendingStreamingBlob pending;
                pending.path = path;
                pending.tmp = std::move(tmp);
                pending.file = file;
                pending.totalSize = totalSize;
                pending.expectedCrc32c = contentCrc32c;
                try {
                    const auto header = buildBlobHeader(blobId, totalSize, totalSize, BlobCodec::NONE, contentCrc32c);
                    uint8_t headerBuf[AKBLOB_HEADER_SIZE_V5]{};
                    serializeBlobHeader(header, headerBuf);
                    writeAll(pending.file, headerBuf, sizeof(headerBuf));
                    pendingStreamingBlobs.emplace(blobId, std::move(pending));
                }
                catch (...) {
                    abortPendingNoThrow(pending);
                    throw;
                }
            }

            void appendStreamingBlobChunk(uint64_t blobId, uint64_t offset, std::span<const uint8_t> chunk) {
                auto it = pendingStreamingBlobs.find(blobId);
                if (it == pendingStreamingBlobs.end()) { throw std::runtime_error("BlobManager: streaming blob write has not started"); }
                auto& pending = it->second;
                if (pending.skipExisting) { return; }
                if (pending.file == nullptr || offset != pending.written || chunk.size() > pending.totalSize - pending.written) {
                    throw std::runtime_error("BlobManager: invalid streaming blob chunk");
                }
                if (!chunk.empty()) { writeAll(pending.file, chunk.data(), chunk.size()); }
                pending.crc.update(chunk);
                pending.written += static_cast<uint64_t>(chunk.size());
            }

            void finishStreamingBlob(uint64_t blobId) {
                auto it = pendingStreamingBlobs.find(blobId);
                if (it == pendingStreamingBlobs.end()) { throw std::runtime_error("BlobManager: streaming blob write has not started"); }
                auto pending = std::move(it->second);
                pendingStreamingBlobs.erase(it);
                if (pending.skipExisting) { return; }
                try {
                    if (pending.file == nullptr || pending.written != pending.totalSize || pending.crc.finish() != pending.expectedCrc32c) {
                        throw std::runtime_error("BlobManager: streaming blob checksum or size mismatch");
                    }
                    syncFile(pending.file);
                    fclose(pending.file);
                    pending.file = nullptr;
                    replaceFileAtomically(pending.tmp, pending.path);
                    if (options.onBlobPut) {
                        options.onBlobPut(
                            blobId,
                            pending.totalSize,
                            pending.totalSize,
                            pending.expectedCrc32c,
                            static_cast<uint32_t>(BlobCodec::NONE)
                        );
                    }
                    blobsWritten.fetch_add(1, std::memory_order_relaxed);
                    bytesUncompressed.fetch_add(pending.totalSize, std::memory_order_relaxed);
                    bytesOnDisk.fetch_add(static_cast<uint64_t>(AKBLOB_HEADER_SIZE_V5) + pending.totalSize, std::memory_order_relaxed);
                }
                catch (...) {
                    abortPendingNoThrow(pending);
                    throw;
                }
            }

            void abortStreamingBlob(uint64_t blobId) noexcept {
                const auto it = pendingStreamingBlobs.find(blobId);
                if (it == pendingStreamingBlobs.end()) { return; }
                abortPendingNoThrow(it->second);
                pendingStreamingBlobs.erase(it);
            }

            void gcLoop() {
                while (running.load(std::memory_order_acquire)) {
                    std::vector<uint64_t> batch;
                    {
                        std::unique_lock lock(delMu);
                        delCv.wait_for(
                            lock,
                            std::chrono::milliseconds(200),
                            [this] { return !running.load(std::memory_order_acquire) || !delQueue.empty(); }
                        );
                        batch.swap(delQueue);
                    }

                    if (!batch.empty()) { gcCycles.fetch_add(1, std::memory_order_relaxed); }
                    for (uint64_t id : batch) {
                        const auto src = pathFor(id);
                        auto dst = src;
                        dst += ".del";
                        if (renameQuiet(src, dst)) {
                            (void)removeQuiet(dst);
                            if (options.onBlobDelete) { options.onBlobDelete(id); }
                            blobsDeleted.fetch_add(1, std::memory_order_relaxed);
                        }
                    }
                }

                std::vector<uint64_t> finalBatch;
                {
                    std::lock_guard lock(delMu);
                    finalBatch.swap(delQueue);
                }
                if (!finalBatch.empty()) { gcCycles.fetch_add(1, std::memory_order_relaxed); }
                for (uint64_t id : finalBatch) {
                    const auto src = pathFor(id);
                    auto dst = src;
                    dst += ".del";
                    if (renameQuiet(src, dst)) {
                        (void)removeQuiet(dst);
                        if (options.onBlobDelete) { options.onBlobDelete(id); }
                        blobsDeleted.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }

            [[nodiscard]] Snapshot snapshot() const noexcept {
                return {
                    blobsWritten.load(std::memory_order_relaxed),
                    bytesUncompressed.load(std::memory_order_relaxed),
                    bytesOnDisk.load(std::memory_order_relaxed),
                    blobsDeleted.load(std::memory_order_relaxed),
                    gcCycles.load(std::memory_order_relaxed)
                };
            }
    };

    BlobManager::BlobManager() = default;

    std::unique_ptr<BlobManager> BlobManager::create(std::filesystem::path blobDir, Options options) {
        if (blobDir.empty()) { throw std::invalid_argument("BlobManager: blobDir is required"); }
        if (options.thresholdBytes == 0) { throw std::invalid_argument("BlobManager: thresholdBytes must be > 0"); }

        auto manager = std::unique_ptr < BlobManager > (new BlobManager{});
        manager->impl_ = std::make_unique<Impl>(std::move(blobDir), std::move(options));
        return manager;
    }

    BlobManager::~BlobManager() { close(); }

    void BlobManager::start() {
        if (!impl_) { return; }
        bool expected = false;
        if (!impl_->started.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) { return; }

        impl_->createShards();
        impl_->startupCleanup();
        impl_->running.store(true, std::memory_order_release);
        impl_->gcThread = std::thread([this] { impl_->gcLoop(); });
    }

    void BlobManager::close() {
        if (!impl_) { return; }
        if (!impl_->started.load(std::memory_order_acquire)) { return; }
        impl_->running.store(false, std::memory_order_release);
        impl_->delCv.notify_all();
        if (impl_->gcThread.joinable()) { impl_->gcThread.join(); }
        impl_->started.store(false, std::memory_order_release);
    }

    uint64_t BlobManager::threshold() const noexcept { return impl_ ? impl_->options.thresholdBytes : DEFAULT_THRESHOLD_BYTES; }

    fs::path BlobManager::blobPath(uint64_t blobId) const {
        if (!impl_) { return {}; }
        return impl_->pathFor(blobId);
    }

    void BlobManager::write(uint64_t blobId, std::span<const uint8_t> content) {
        if (!impl_) { throw std::runtime_error("BlobManager: not initialized"); }
        const auto path = impl_->pathFor(blobId);
        if (fs::exists(path)) { return; }

        std::lock_guard lock(impl_->writeMu);
        if (fs::exists(path)) { return; }
        impl_->writeBlob(blobId, content, path);
    }

    void BlobManager::beginWrite(uint64_t blobId, uint64_t totalSize, uint32_t contentCrc32c) {
        if (!impl_) { throw std::runtime_error("BlobManager: not initialized"); }
        std::lock_guard lock(impl_->writeMu);
        impl_->beginStreamingBlob(blobId, totalSize, contentCrc32c);
    }

    void BlobManager::appendWriteChunk(uint64_t blobId, uint64_t offset, std::span<const uint8_t> chunk) {
        if (!impl_) { throw std::runtime_error("BlobManager: not initialized"); }
        std::lock_guard lock(impl_->writeMu);
        impl_->appendStreamingBlobChunk(blobId, offset, chunk);
    }

    void BlobManager::finishWrite(uint64_t blobId) {
        if (!impl_) { throw std::runtime_error("BlobManager: not initialized"); }
        std::lock_guard lock(impl_->writeMu);
        impl_->finishStreamingBlob(blobId);
    }

    void BlobManager::abortWrite(uint64_t blobId) noexcept {
        if (!impl_) { return; }
        std::lock_guard lock(impl_->writeMu);
        impl_->abortStreamingBlob(blobId);
    }

    std::vector<uint8_t> BlobManager::read(uint64_t blobId) const {
        if (!impl_) { throw std::runtime_error("BlobManager: not initialized"); }
        const auto path = impl_->pathFor(blobId);
        auto raw = readFile(path);
        if (raw.size() < AKBLOB_HEADER_SIZE_V5) { throw std::runtime_error("BlobManager: file too small: " + path.string()); }

        const auto header = deserializeBlobHeader(raw.data());
        if (!verifyBlobHeader(header)) { throw std::runtime_error("BlobManager: header corrupt: " + path.string()); }
        if (header.blobId != blobId) { throw std::runtime_error("BlobManager: blobId mismatch: " + path.string()); }

        const size_t payloadOffset = AKBLOB_HEADER_SIZE_V5;
        if (header.storedSize > raw.size() - payloadOffset) {
            throw std::runtime_error("BlobManager: payload truncated: " + path.string());
        }

        const auto* payload = raw.data() + payloadOffset;
        std::vector<uint8_t> content;
        if (header.codec == static_cast<uint32_t>(BlobCodec::ZSTD)) {
            content.resize(static_cast<size_t>(header.totalSize));
            const size_t n = ZSTD_decompress(content.data(), content.size(), payload, static_cast<size_t>(header.storedSize));
            if (ZSTD_isError(n) || n != header.totalSize) {
                throw std::runtime_error("BlobManager: Zstd decompress failed: " + path.string());
            }
        }
        else {
            if (header.storedSize != header.totalSize) {
                throw std::runtime_error("BlobManager: uncompressed size mismatch: " + path.string());
            }
            content.assign(payload, payload + static_cast<size_t>(header.storedSize));
        }

        if (crc32c(content) != header.contentCrc32c) { throw std::runtime_error("BlobManager: content crc mismatch: " + path.string()); }
        return content;
    }

    std::vector<uint8_t> BlobManager::read(uint64_t blobId, uint32_t expectedCrc32c) const {
        auto content = read(blobId);
        if (crc32c(content) != expectedCrc32c) { throw std::runtime_error("BlobManager: expected crc mismatch"); }
        return content;
    }

    void BlobManager::scheduleDelete(uint64_t blobId) {
        if (!impl_) { return; }
        {
            std::lock_guard lock(impl_->delMu);
            impl_->delQueue.push_back(blobId);
        }
        impl_->delCv.notify_one();
    }

    void BlobManager::scanOrphans(std::function<bool(uint64_t)> isReferenced) {
        if (!impl_) { return; }
        std::vector<uint64_t> orphans;
        std::error_code ec;
        for (const auto& entry : fs::recursive_directory_iterator(impl_->blobDir, ec)) {
            if (ec) { break; }
            if (!entry.is_regular_file(ec) || entry.path().extension() != ".akblob") { continue; }

            uint64_t blobId = 0;
            if (!parseHex16(entry.path().stem().string(), blobId)) { continue; }
            if (!isReferenced(blobId)) { orphans.push_back(blobId); }
        }
        if (orphans.empty()) { return; }

        {
            std::lock_guard lock(impl_->delMu);
            impl_->delQueue.insert(impl_->delQueue.end(), orphans.begin(), orphans.end());
        }
        impl_->delCv.notify_one();
    }

    BlobManager::Snapshot BlobManager::snapshot() const noexcept { return impl_ ? impl_->snapshot() : Snapshot{}; }
} // namespace akkaradb::engine::blob

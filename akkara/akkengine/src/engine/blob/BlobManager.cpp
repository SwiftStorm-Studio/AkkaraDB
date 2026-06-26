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
#include <vector>

#include <zstd.h>

#ifdef _WIN32
#include <io.h>
#include <windows.h>
#else
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

            fs::path tmp = path;
            tmp += ".tmp";
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

            std::error_code ec;
            fs::rename(tmp, path, ec);
            if (ec) {
                fs::remove(path, ec);
                ec.clear();
                fs::rename(tmp, path, ec);
            }
            if (ec) {
                fs::remove(tmp, ec);
                throw std::runtime_error("BlobManager: rename tmp to akblob failed: " + path.string());
            }
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

    BlobManager::BlobManager() = default;

    class BlobManager::Impl {
        public:
            explicit Impl(Options optionsValue) : options(std::move(optionsValue)) {}

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

            [[nodiscard]] fs::path pathFor(uint64_t blobId) const {
                const uint8_t hi = static_cast<uint8_t>(blobId >> 56u);
                return options.blobDir / hex2(hi) / (hex16(blobId) + ".akblob");
            }

            void createShards() const {
                fs::create_directories(options.blobDir);
                for (uint32_t i = 0; i < 256; ++i) { fs::create_directories(options.blobDir / hex2(static_cast<uint8_t>(i))); }
            }

            void startupCleanup() const {
                std::error_code ec;
                if (!fs::exists(options.blobDir, ec)) { return; }
                for (const auto& entry : fs::recursive_directory_iterator(options.blobDir, ec)) {
                    if (ec) { break; }
                    if (!entry.is_regular_file(ec)) { continue; }
                    const auto name = entry.path().filename().string();
                    if (name.ends_with(".akblob.tmp") || name.ends_with(".akblob.del")) { (void)removeQuiet(entry.path()); }
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
                blobsWritten.fetch_add(1, std::memory_order_relaxed);
                bytesUncompressed.fetch_add(static_cast<uint64_t>(content.size()), std::memory_order_relaxed);
                bytesOnDisk.fetch_add(
                    static_cast<uint64_t>(sizeof(headerBuf)) + static_cast<uint64_t>(payloadSize),
                    std::memory_order_relaxed
                );
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

    std::unique_ptr<BlobManager> BlobManager::create(Options options) {
        if (options.blobDir.empty()) { throw std::invalid_argument("BlobManager: blobDir is required"); }
        if (options.thresholdBytes == 0) { throw std::invalid_argument("BlobManager: thresholdBytes must be > 0"); }

        auto manager = std::unique_ptr < BlobManager > (new BlobManager{});
        manager->impl_ = std::make_unique<Impl>(std::move(options));
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
        for (const auto& entry : fs::recursive_directory_iterator(impl_->options.blobDir, ec)) {
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

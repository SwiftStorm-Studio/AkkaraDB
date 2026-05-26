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

        [[nodiscard]] bool parse_hex16(std::string_view text, uint64_t& out) noexcept {
            if (text.size() != 16) { return false; }
            uint64_t value = 0;
            const auto* first = text.data();
            const auto* last = text.data() + text.size();
            const auto [ptr, ec] = std::from_chars(first, last, value, 16);
            if (ec != std::errc{} || ptr != last) { return false; }
            out = value;
            return true;
        }

        [[nodiscard]] FILE* open_file_write(const fs::path& path) {
            #ifdef _WIN32
            FILE* f = _wfopen(path.wstring().c_str(), L"wb");
            #else
            FILE* f = fopen(path.string().c_str(), "wb");
            #endif
            return f;
        }

        [[nodiscard]] FILE* open_file_read(const fs::path& path) {
            #ifdef _WIN32
            FILE* f = _wfopen(path.wstring().c_str(), L"rb");
            #else
            FILE* f = fopen(path.string().c_str(), "rb");
            #endif
            return f;
        }

        void sync_file(FILE* f) {
            fflush(f);
            #ifdef _WIN32
            if (_commit(_fileno(f)) != 0) { throw std::runtime_error("BlobManager: _commit failed"); }
            #else
            if (fsync(fileno(f)) != 0) { throw std::runtime_error("BlobManager: fsync failed"); }
            #endif
        }

        void write_all(FILE* f, const uint8_t* data, size_t size) {
            while (size > 0) {
                const size_t n = fwrite(data, 1, size, f);
                if (n == 0) { throw std::runtime_error("BlobManager: fwrite failed"); }
                data += n;
                size -= n;
            }
        }

        void write_atomic_split(const fs::path& path, const uint8_t* header, size_t header_size, const uint8_t* payload, size_t payload_size) {
            fs::create_directories(path.parent_path());

            fs::path tmp = path;
            tmp += ".tmp";
            {
                FILE* f = open_file_write(tmp);
                if (!f) { throw std::runtime_error("BlobManager: cannot open tmp file: " + tmp.string()); }
                try {
                    write_all(f, header, header_size);
                    if (payload_size > 0) { write_all(f, payload, payload_size); }
                    sync_file(f);
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

        [[nodiscard]] std::vector<uint8_t> read_file(const fs::path& path) {
            FILE* f = open_file_read(path);
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

        [[nodiscard]] bool remove_quiet(const fs::path& path) noexcept {
            std::error_code ec;
            return fs::remove(path, ec);
        }

        [[nodiscard]] bool rename_quiet(const fs::path& src, const fs::path& dst) noexcept {
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
            explicit Impl(Options options_value) : options(std::move(options_value)) {}

            Options options;
            mutable std::mutex write_mu;
            std::mutex del_mu;
            std::condition_variable del_cv;
            std::vector<uint64_t> del_queue;
            std::thread gc_thread;
            std::atomic<bool> running{false};
            std::atomic<bool> started{false};
            mutable std::atomic<uint64_t> blobs_written{0};
            mutable std::atomic<uint64_t> bytes_uncompressed{0};
            mutable std::atomic<uint64_t> bytes_on_disk{0};
            mutable std::atomic<uint64_t> blobs_deleted{0};
            mutable std::atomic<uint64_t> gc_cycles{0};

            [[nodiscard]] fs::path path_for(uint64_t blob_id) const {
                const uint8_t hi = static_cast<uint8_t>(blob_id >> 56u);
                return options.blob_dir / hex2(hi) / (hex16(blob_id) + ".akblob");
            }

            void create_shards() const {
                fs::create_directories(options.blob_dir);
                for (uint32_t i = 0; i < 256; ++i) { fs::create_directories(options.blob_dir / hex2(static_cast<uint8_t>(i))); }
            }

            void startup_cleanup() const {
                std::error_code ec;
                if (!fs::exists(options.blob_dir, ec)) { return; }
                for (const auto& entry : fs::recursive_directory_iterator(options.blob_dir, ec)) {
                    if (ec) { break; }
                    if (!entry.is_regular_file(ec)) { continue; }
                    const auto name = entry.path().filename().string();
                    if (name.ends_with(".akblob.tmp") || name.ends_with(".akblob.del")) { (void)remove_quiet(entry.path()); }
                }
            }

            [[nodiscard]] std::vector<uint8_t> maybe_compress(std::span<const uint8_t> content, BlobCodec& actual_codec) const {
                actual_codec = BlobCodec::None;
                if (options.codec != BlobCodec::Zstd || content.empty()) { return {}; }

                const size_t bound = ZSTD_compressBound(content.size());
                std::vector<uint8_t> compressed(bound);
                const size_t n = ZSTD_compress(compressed.data(), compressed.size(), content.data(), content.size(), ZSTD_CLEVEL_DEFAULT);
                if (ZSTD_isError(n) || n >= content.size()) { return {}; }
                compressed.resize(n);
                actual_codec = BlobCodec::Zstd;
                return compressed;
            }

            void write_blob(uint64_t blob_id, std::span<const uint8_t> content, const fs::path& path) const {
                BlobCodec actual_codec = BlobCodec::None;
                std::vector<uint8_t> compressed = maybe_compress(content, actual_codec);

                const uint8_t* payload = content.data();
                size_t payload_size = content.size();
                if (actual_codec == BlobCodec::Zstd) {
                    payload = compressed.data();
                    payload_size = compressed.size();
                }
                if (payload_size > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
                    throw std::invalid_argument("BlobManager: payload too large");
                }

                const uint32_t content_crc = crc32c(content);
                const auto header = build_blob_header(blob_id, content.size(), payload_size, actual_codec, content_crc);
                uint8_t header_buf[AKBLOB_HEADER_SIZE_V5]{};
                serialize_blob_header(header, header_buf);
                write_atomic_split(path, header_buf, sizeof(header_buf), payload, payload_size);
                blobs_written.fetch_add(1, std::memory_order_relaxed);
                bytes_uncompressed.fetch_add(static_cast<uint64_t>(content.size()), std::memory_order_relaxed);
                bytes_on_disk.fetch_add(static_cast<uint64_t>(sizeof(header_buf)) + static_cast<uint64_t>(payload_size), std::memory_order_relaxed);
            }

            void gc_loop() {
                while (running.load(std::memory_order_acquire)) {
                    std::vector<uint64_t> batch;
                    {
                        std::unique_lock lock(del_mu);
                        del_cv.wait_for(
                            lock,
                            std::chrono::milliseconds(200),
                            [this] { return !running.load(std::memory_order_acquire) || !del_queue.empty(); }
                        );
                        batch.swap(del_queue);
                    }

                    if (!batch.empty()) { gc_cycles.fetch_add(1, std::memory_order_relaxed); }
                    for (uint64_t id : batch) {
                        const auto src = path_for(id);
                        auto dst = src;
                        dst += ".del";
                        if (rename_quiet(src, dst)) {
                            (void)remove_quiet(dst);
                            blobs_deleted.fetch_add(1, std::memory_order_relaxed);
                        }
                    }
                }

                std::vector<uint64_t> final_batch;
                {
                    std::lock_guard lock(del_mu);
                    final_batch.swap(del_queue);
                }
                if (!final_batch.empty()) { gc_cycles.fetch_add(1, std::memory_order_relaxed); }
                for (uint64_t id : final_batch) {
                    const auto src = path_for(id);
                    auto dst = src;
                    dst += ".del";
                    if (rename_quiet(src, dst)) {
                        (void)remove_quiet(dst);
                        blobs_deleted.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }

            [[nodiscard]] Snapshot snapshot() const noexcept {
                return {
                    blobs_written.load(std::memory_order_relaxed),
                    bytes_uncompressed.load(std::memory_order_relaxed),
                    bytes_on_disk.load(std::memory_order_relaxed),
                    blobs_deleted.load(std::memory_order_relaxed),
                    gc_cycles.load(std::memory_order_relaxed)
                };
            }
    };

    std::unique_ptr<BlobManager> BlobManager::create(Options options) {
        if (options.blob_dir.empty()) { throw std::invalid_argument("BlobManager: blob_dir is required"); }
        if (options.threshold_bytes == 0) { throw std::invalid_argument("BlobManager: threshold_bytes must be > 0"); }

        auto manager = std::unique_ptr<BlobManager>(new BlobManager{});
        manager->impl_ = std::make_unique<Impl>(std::move(options));
        return manager;
    }

    BlobManager::~BlobManager() { close(); }

    void BlobManager::start() {
        if (!impl_) { return; }
        bool expected = false;
        if (!impl_->started.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) { return; }

        impl_->create_shards();
        impl_->startup_cleanup();
        impl_->running.store(true, std::memory_order_release);
        impl_->gc_thread = std::thread([this] { impl_->gc_loop(); });
    }

    void BlobManager::close() {
        if (!impl_) { return; }
        if (!impl_->started.load(std::memory_order_acquire)) { return; }
        impl_->running.store(false, std::memory_order_release);
        impl_->del_cv.notify_all();
        if (impl_->gc_thread.joinable()) { impl_->gc_thread.join(); }
        impl_->started.store(false, std::memory_order_release);
    }

    uint64_t BlobManager::threshold() const noexcept { return impl_ ? impl_->options.threshold_bytes : DEFAULT_THRESHOLD_BYTES; }

    fs::path BlobManager::blob_path(uint64_t blob_id) const {
        if (!impl_) { return {}; }
        return impl_->path_for(blob_id);
    }

    void BlobManager::write(uint64_t blob_id, std::span<const uint8_t> content) {
        if (!impl_) { throw std::runtime_error("BlobManager: not initialized"); }
        const auto path = impl_->path_for(blob_id);
        if (fs::exists(path)) { return; }

        std::lock_guard lock(impl_->write_mu);
        if (fs::exists(path)) { return; }
        impl_->write_blob(blob_id, content, path);
    }

    std::vector<uint8_t> BlobManager::read(uint64_t blob_id) const {
        if (!impl_) { throw std::runtime_error("BlobManager: not initialized"); }
        const auto path = impl_->path_for(blob_id);
        auto raw = read_file(path);
        if (raw.size() < AKBLOB_HEADER_SIZE_V5) { throw std::runtime_error("BlobManager: file too small: " + path.string()); }

        const auto header = deserialize_blob_header(raw.data());
        if (!verify_blob_header(header)) { throw std::runtime_error("BlobManager: header corrupt: " + path.string()); }
        if (header.blob_id != blob_id) { throw std::runtime_error("BlobManager: blob_id mismatch: " + path.string()); }

        const size_t payload_offset = AKBLOB_HEADER_SIZE_V5;
        if (header.stored_size > raw.size() - payload_offset) { throw std::runtime_error("BlobManager: payload truncated: " + path.string()); }

        const auto* payload = raw.data() + payload_offset;
        std::vector<uint8_t> content;
        if (header.codec == static_cast<uint32_t>(BlobCodec::Zstd)) {
            content.resize(static_cast<size_t>(header.total_size));
            const size_t n = ZSTD_decompress(content.data(), content.size(), payload, static_cast<size_t>(header.stored_size));
            if (ZSTD_isError(n) || n != header.total_size) { throw std::runtime_error("BlobManager: Zstd decompress failed: " + path.string()); }
        }
        else {
            if (header.stored_size != header.total_size) { throw std::runtime_error("BlobManager: uncompressed size mismatch: " + path.string()); }
            content.assign(payload, payload + static_cast<size_t>(header.stored_size));
        }

        if (crc32c(content) != header.content_crc32c) { throw std::runtime_error("BlobManager: content crc mismatch: " + path.string()); }
        return content;
    }

    std::vector<uint8_t> BlobManager::read(uint64_t blob_id, uint32_t expected_crc32c) const {
        auto content = read(blob_id);
        if (crc32c(content) != expected_crc32c) { throw std::runtime_error("BlobManager: expected crc mismatch"); }
        return content;
    }

    void BlobManager::schedule_delete(uint64_t blob_id) {
        if (!impl_) { return; }
        {
            std::lock_guard lock(impl_->del_mu);
            impl_->del_queue.push_back(blob_id);
        }
        impl_->del_cv.notify_one();
    }

    void BlobManager::scan_orphans(std::function<bool(uint64_t)> is_referenced) {
        if (!impl_) { return; }
        std::vector<uint64_t> orphans;
        std::error_code ec;
        for (const auto& entry : fs::recursive_directory_iterator(impl_->options.blob_dir, ec)) {
            if (ec) { break; }
            if (!entry.is_regular_file(ec) || entry.path().extension() != ".akblob") { continue; }

            uint64_t blob_id = 0;
            if (!parse_hex16(entry.path().stem().string(), blob_id)) { continue; }
            if (!is_referenced(blob_id)) { orphans.push_back(blob_id); }
        }
        if (orphans.empty()) { return; }

        {
            std::lock_guard lock(impl_->del_mu);
            impl_->del_queue.insert(impl_->del_queue.end(), orphans.begin(), orphans.end());
        }
        impl_->del_cv.notify_one();
    }

    BlobManager::Snapshot BlobManager::snapshot() const noexcept { return impl_ ? impl_->snapshot() : Snapshot{}; }
} // namespace akkaradb::engine::blob

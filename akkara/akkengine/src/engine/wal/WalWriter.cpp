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

        [[nodiscard]] uint64_t now_us() noexcept {
            return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch()
            ).count());
        }

        [[nodiscard]] uint16_t resolve_auto_shard_count() noexcept {
            const unsigned hw = std::thread::hardware_concurrency();
            return static_cast<uint16_t>(std::clamp<unsigned>(hw == 0 ? 1u : hw, 1u, 16u));
        }

        [[nodiscard]] uint32_t crc32c_bytes(const uint8_t* data, size_t size) noexcept {
            return cpu::CRC32C(reinterpret_cast<const std::byte*>(data), size);
        }

        void refresh_segment_crc(WalSegmentHeader& header) noexcept {
            header.crc32c = 0;
            uint8_t buf[WalSegmentHeader::SIZE]{};
            header.serialize(buf);
            header.crc32c = crc32c_bytes(buf, sizeof(buf));
        }

        void do_fdatasync(FILE* f) {
            if (f == nullptr) { return; }
            #ifdef _WIN32
            if (_commit(_fileno(f)) != 0) { throw std::runtime_error("WAL fdatasync failed"); }
            #else
            if (::fdatasync(fileno(f)) != 0) { throw std::runtime_error("WAL fdatasync failed"); }
            #endif
        }

        [[nodiscard]] FILE* open_rw(const fs::path& path, bool exists) {
            #ifdef _WIN32
            FILE* f = _wfopen(path.wstring().c_str(), exists ? L"r+b" : L"w+b");
            #else
            FILE* f = std::fopen(path.string().c_str(), exists ? "r+b" : "w+b");
            #endif
            if (f == nullptr) { throw std::runtime_error("WAL failed to open segment: " + path.string()); }
            return f;
        }

        void close_file(FILE*& f) noexcept {
            if (f != nullptr) {
                std::fclose(f);
                f = nullptr;
            }
        }

        void write_all(FILE* f, const uint8_t* data, size_t size) {
            if (size == 0) { return; }
            if (std::fwrite(data, 1, size, f) != size) { throw std::runtime_error("WAL write failed"); }
        }

        [[nodiscard]] bool read_exact(std::ifstream& file, uint8_t* out, size_t len) {
            if (len == 0) { return true; }
            file.read(reinterpret_cast<char*>(out), static_cast<std::streamsize>(len));
            return file.good() || file.gcount() == static_cast<std::streamsize>(len);
        }

        [[nodiscard]] std::string segment_name(uint16_t shard_id, uint64_t segment_id) {
            std::ostringstream os;
            os << std::setfill('0') << std::setw(4) << shard_id << "_" << std::hex << std::nouppercase << std::setw(16) << segment_id <<
                ".akwal";
            return os.str();
        }

        [[nodiscard]] fs::path segment_path(const fs::path& wal_dir, uint16_t shard_id, uint64_t segment_id) {
            return wal_dir / segment_name(shard_id, segment_id);
        }

        struct SegmentScanResult {
            bool valid_header = false;
            uint64_t first_seq = 0;
            uint64_t last_seq = 0;
        };

        [[nodiscard]] SegmentScanResult scan_segment_sequences(const fs::path& path) {
            SegmentScanResult result{};
            std::ifstream file(path, std::ios::binary);
            if (!file) { return result; }

            uint8_t shdr_buf[WalSegmentHeader::SIZE]{};
            if (!read_exact(file, shdr_buf, WalSegmentHeader::SIZE)) { return result; }
            const WalSegmentHeader shdr = WalSegmentHeader::deserialize(shdr_buf);
            if (!shdr.verify_magic() || !shdr.verify_version() || !shdr.verify_checksum()) { return result; }
            result.valid_header = true;

            while (true) {
                uint8_t ehdr_buf[WalEntryHeader::SIZE]{};
                file.read(reinterpret_cast<char*>(ehdr_buf), WalEntryHeader::SIZE);
                const std::streamsize got = file.gcount();
                if (got == 0) { break; }
                if (got != static_cast<std::streamsize>(WalEntryHeader::SIZE)) { break; }

                const WalEntryHeader ehdr = WalEntryHeader::deserialize(ehdr_buf);
                if (!ehdr.verify_lengths(SEGMENT_BYTES)) { break; }

                std::vector<uint8_t> key(ehdr.key_len);
                std::vector<uint8_t> value(ehdr.value_len);
                if (!read_exact(file, key.data(), key.size()) || !read_exact(file, value.data(), value.size())) { break; }
                if (!ehdr.verify_checksum(
                    std::span<const uint8_t>{key.data(), key.size()},
                    std::span<const uint8_t>{value.data(), value.size()}
                )) { break; }

                if (result.first_seq == 0 || ehdr.seq < result.first_seq) { result.first_seq = ehdr.seq; }
                if (ehdr.seq > result.last_seq) { result.last_seq = ehdr.seq; }
            }
            return result;
        }

        [[nodiscard]] uint64_t find_last_segment_id(const fs::path& wal_dir, uint16_t shard_id) {
            uint64_t max_segment = 0;
            bool found = false;
            if (!fs::exists(wal_dir)) { return 0; }

            for (const auto& entry : fs::directory_iterator(wal_dir)) {
                if (!entry.is_regular_file() || entry.path().extension() != ".akwal") { continue; }
                std::ifstream file(entry.path(), std::ios::binary);
                if (!file) { continue; }
                uint8_t hdr_buf[WalSegmentHeader::SIZE]{};
                if (!read_exact(file, hdr_buf, WalSegmentHeader::SIZE)) { continue; }
                const WalSegmentHeader hdr = WalSegmentHeader::deserialize(hdr_buf);
                if (!hdr.verify_magic() || !hdr.verify_version() || !hdr.verify_checksum() || hdr.shard_id != shard_id) { continue; }
                max_segment = found ? std::max(max_segment, hdr.segment_id) : hdr.segment_id;
                found = true;
            }
            return found ? max_segment : 0;
        }

        [[nodiscard]] uint16_t shard_for(uint64_t fp64, uint16_t shard_count) noexcept {
            if (shard_count <= 1) { return 0; }
            return static_cast<uint16_t>(fp64 % static_cast<uint64_t>(shard_count));
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
                    ShardWriter(WalOptions options, uint16_t shard_id)
                        : options_{std::move(options)}, shard_id_{shard_id}, running_{options_.sync_mode == WalSyncMode::Async} {
                        fs::create_directories(options_.wal_dir);
                        segment_id_ = find_last_segment_id(options_.wal_dir, shard_id_);
                        open_segment(segment_id_);

                        if (options_.sync_mode == WalSyncMode::Async) { thread_ = std::thread([this] { run_flusher(); }); }
                    }

                    ~ShardWriter() noexcept {
                        try { close(); }
                        catch (...) {}
                    }

                    ShardWriter(const ShardWriter&) = delete;
                    ShardWriter& operator=(const ShardWriter&) = delete;

                    void append(PendingEntry entry) {
                        check_async_error();
                        if (options_.sync_mode == WalSyncMode::Async) {
                            const uint64_t entry_bytes = static_cast<uint64_t>(entry.bytes.size());
                            {
                                std::unique_lock lock{queue_mutex_};
                                queue_space_cv_.wait(
                                    lock,
                                    [&] {
                                        const uint64_t pending_bytes = queue_bytes_ + in_flight_bytes_;
                                        return async_error_ || pending_bytes + entry_bytes <= options_.async_max_pending_bytes || (queue_.
                                            empty() && in_flight_bytes_ == 0);
                                    }
                                );
                                if (async_error_) { std::rethrow_exception(async_error_); }
                                queue_.push_back(std::move(entry));
                                queue_bytes_ += entry_bytes;
                            }
                            queue_cv_.notify_one();
                            return;
                        }

                        std::lock_guard file_lock{file_mutex_};
                        write_one_locked(entry);
                        std::fflush(file_);
                        batches_flushed_.fetch_add(1, std::memory_order_relaxed);
                        if (options_.sync_mode == WalSyncMode::Sync) {
                            do_fdatasync(file_);
                            syncs_executed_.fetch_add(1, std::memory_order_relaxed);
                        }
                    }

                    void force_sync() {
                        check_async_error();
                        if (options_.sync_mode == WalSyncMode::Async) { drain_async(); }

                        std::lock_guard file_lock{file_mutex_};
                        update_header_locked();
                        std::fflush(file_);
                        do_fdatasync(file_);
                        syncs_executed_.fetch_add(1, std::memory_order_relaxed);
                    }

                    void prune_until(uint64_t checkpoint_seq) {
                        force_sync();
                        std::vector<fs::path> removable;

                        {
                            std::lock_guard file_lock{file_mutex_};
                            for (const auto& entry : fs::directory_iterator(options_.wal_dir)) {
                                if (!entry.is_regular_file() || entry.path().extension() != ".akwal" || entry.path() == path_) { continue; }

                                std::ifstream file(entry.path(), std::ios::binary);
                                if (!file) { continue; }
                                uint8_t hdr_buf[WalSegmentHeader::SIZE]{};
                                if (!read_exact(file, hdr_buf, WalSegmentHeader::SIZE)) { continue; }
                                const WalSegmentHeader hdr = WalSegmentHeader::deserialize(hdr_buf);
                                if (!hdr.verify_magic() || !hdr.verify_version() || !hdr.verify_checksum() || hdr.shard_id != shard_id_) {
                                    continue;
                                }

                                const SegmentScanResult scan = scan_segment_sequences(entry.path());
                                if (scan.valid_header && scan.last_seq != 0 && scan.last_seq <= checkpoint_seq) {
                                    removable.push_back(entry.path());
                                }
                            }
                        }

                        for (const auto& path : removable) { fs::remove(path); }
                    }

                    void close() {
                        if (closed_) { return; }
                        if (options_.sync_mode == WalSyncMode::Async) {
                            {
                                std::lock_guard lock{queue_mutex_};
                                running_ = false;
                            }
                            queue_cv_.notify_one();
                            if (thread_.joinable()) { thread_.join(); }
                            check_async_error();
                        }

                        std::lock_guard file_lock{file_mutex_};
                        if (file_ != nullptr) {
                            update_header_locked();
                            std::fflush(file_);
                            if (options_.sync_mode != WalSyncMode::Off) {
                                do_fdatasync(file_);
                                syncs_executed_.fetch_add(1, std::memory_order_relaxed);
                            }
                            close_file(file_);
                        }
                        closed_ = true;
                    }

                    [[nodiscard]] WalWriterSnapshot snapshot() const noexcept {
                        return {
                            1,
                            entries_written_.load(std::memory_order_relaxed),
                            bytes_written_.load(std::memory_order_relaxed),
                            batches_flushed_.load(std::memory_order_relaxed),
                            syncs_executed_.load(std::memory_order_relaxed),
                            segment_rotations_.load(std::memory_order_relaxed)
                        };
                    }

                private:
                    void open_segment(uint64_t segment_id) {
                        path_ = segment_path(options_.wal_dir, shard_id_, segment_id);
                        const bool exists = fs::exists(path_) && fs::file_size(path_) >= WalSegmentHeader::SIZE;
                        file_ = open_rw(path_, exists);

                        if (exists) {
                            uint8_t hdr_buf[WalSegmentHeader::SIZE]{};
                            if (std::fseek(file_, 0, SEEK_SET) != 0 || std::fread(hdr_buf, 1, WalSegmentHeader::SIZE, file_) !=
                                WalSegmentHeader::SIZE) {
                                throw std::runtime_error("WAL failed to read segment header: " + path_.string());
                            }
                            header_ = WalSegmentHeader::deserialize(hdr_buf);
                            if (!header_.verify_magic() || !header_.verify_version() || !header_.verify_checksum() || header_.shard_id !=
                                shard_id_) {
                                close_file(file_);
                                ++segment_id_;
                                open_segment(segment_id_);
                                return;
                            }

                            const SegmentScanResult scan = scan_segment_sequences(path_);
                            if (scan.valid_header) {
                                header_.first_seq = scan.first_seq;
                                header_.last_seq = scan.last_seq;
                            }
                            current_size_ = fs::file_size(path_);
                            if (current_size_ >= SEGMENT_BYTES) {
                                rotate_locked();
                                return;
                            }
                            std::fseek(file_, 0, SEEK_END);
                            return;
                        }

                        header_ = WalSegmentHeader::build(shard_id_, segment_id_, now_us());
                        current_size_ = 0;
                        update_header_locked();
                        current_size_ = WalSegmentHeader::SIZE;
                        std::fflush(file_);
                    }

                    void rotate_locked() {
                        update_header_locked();
                        std::fflush(file_);
                        if (options_.sync_mode != WalSyncMode::Off) {
                            do_fdatasync(file_);
                            syncs_executed_.fetch_add(1, std::memory_order_relaxed);
                        }
                        close_file(file_);
                        ++segment_id_;
                        segment_rotations_.fetch_add(1, std::memory_order_relaxed);
                        open_segment(segment_id_);
                    }

                    void update_header_locked() {
                        if (file_ == nullptr) { return; }
                        refresh_segment_crc(header_);
                        uint8_t buf[WalSegmentHeader::SIZE]{};
                        header_.serialize(buf);
                        if (std::fseek(file_, 0, SEEK_SET) != 0) { throw std::runtime_error("WAL seek header failed"); }
                        write_all(file_, buf, sizeof(buf));
                        if (std::fseek(file_, 0, SEEK_END) != 0) { throw std::runtime_error("WAL seek end failed"); }
                    }

                    void write_one_locked(const PendingEntry& entry) {
                        if (entry.bytes.size() + WalSegmentHeader::SIZE > SEGMENT_BYTES) {
                            throw std::invalid_argument("WAL entry exceeds segment capacity");
                        }
                        if (current_size_ + entry.bytes.size() > SEGMENT_BYTES && current_size_ > WalSegmentHeader::SIZE) {
                            rotate_locked();
                        }

                        if (header_.first_seq == 0 || entry.seq < header_.first_seq) { header_.first_seq = entry.seq; }
                        if (entry.seq > header_.last_seq) { header_.last_seq = entry.seq; }
                        write_all(file_, entry.bytes.data(), entry.bytes.size());
                        current_size_ += entry.bytes.size();
                        entries_written_.fetch_add(1, std::memory_order_relaxed);
                        bytes_written_.fetch_add(static_cast<uint64_t>(entry.bytes.size()), std::memory_order_relaxed);
                    }

                    void run_flusher() {
                        std::vector<PendingEntry> batch;
                        batch.reserve(options_.group_n == 0 ? 128 : options_.group_n);

                        try {
                            while (true) {
                                {
                                    std::unique_lock lock{queue_mutex_};
                                    queue_cv_.wait(lock, [this] { return !queue_.empty() || !running_; });
                                    if (!running_ && queue_.empty()) { break; }
                                    if (running_ && queue_.size() < options_.group_n && queue_bytes_ < options_.group_bytes) {
                                        queue_cv_.wait_for(
                                            lock,
                                            std::chrono::microseconds(options_.group_micros),
                                            [this] {
                                                return queue_.size() >= options_.group_n || queue_bytes_ >= options_.group_bytes || !
                                                    running_;
                                            }
                                        );
                                    }
                                    in_flight_ = true;
                                    in_flight_bytes_ = queue_bytes_;
                                    std::swap(batch, queue_);
                                    queue_bytes_ = 0;
                                }
                                queue_space_cv_.notify_all();

                                if (!batch.empty()) {
                                    {
                                        std::lock_guard file_lock{file_mutex_};
                                        for (const PendingEntry& entry : batch) { write_one_locked(entry); }
                                        std::fflush(file_);
                                        do_fdatasync(file_);
                                        batches_flushed_.fetch_add(1, std::memory_order_relaxed);
                                        syncs_executed_.fetch_add(1, std::memory_order_relaxed);
                                    }
                                    batch.clear();
                                }

                                {
                                    std::lock_guard lock{queue_mutex_};
                                    in_flight_ = false;
                                    in_flight_bytes_ = 0;
                                }
                                queue_cv_.notify_all();
                                queue_space_cv_.notify_all();
                            }
                        }
                        catch (...) {
                            {
                                std::lock_guard lock{queue_mutex_};
                                async_error_ = std::current_exception();
                                in_flight_ = false;
                                in_flight_bytes_ = 0;
                            }
                            queue_cv_.notify_all();
                            queue_space_cv_.notify_all();
                        }
                    }

                    void drain_async() {
                        {
                            std::unique_lock lock{queue_mutex_};
                            queue_cv_.wait(lock, [this] { return queue_.empty() && !in_flight_; });
                        }
                        check_async_error();
                    }

                    void check_async_error() {
                        std::lock_guard lock{queue_mutex_};
                        if (async_error_) { std::rethrow_exception(async_error_); }
                    }

                    WalOptions options_;
                    uint16_t shard_id_ = 0;
                    uint64_t segment_id_ = 0;
                    fs::path path_;
                    FILE* file_ = nullptr;
                    WalSegmentHeader header_{};
                    uint64_t current_size_ = 0;
                    bool closed_ = false;

                    mutable std::mutex file_mutex_;
                    std::mutex queue_mutex_;
                    std::condition_variable queue_cv_;
                    std::condition_variable queue_space_cv_;
                    std::vector<PendingEntry> queue_;
                    uint64_t queue_bytes_ = 0;
                    bool running_ = false;
                    bool in_flight_ = false;
                    uint64_t in_flight_bytes_ = 0;
                    std::exception_ptr async_error_;
                    std::thread thread_;

                    std::atomic<uint64_t> entries_written_{0};
                    std::atomic<uint64_t> bytes_written_{0};
                    std::atomic<uint64_t> batches_flushed_{0};
                    std::atomic<uint64_t> syncs_executed_{0};
                    std::atomic<uint64_t> segment_rotations_{0};
            };

            explicit Impl(WalOptions options)
                : options_{std::move(options)} {
                if (options_.wal_dir.empty()) { throw std::invalid_argument("WAL directory is required"); }
                if (options_.shard_count == 0) { options_.shard_count = resolve_auto_shard_count(); }
                if (options_.shard_count > 16) { throw std::invalid_argument("WAL shard_count must be in range 1..16, or 0 for auto"); }
                if (options_.group_n == 0) { options_.group_n = 128; }
                if (options_.group_micros == 0) { options_.group_micros = 100; }
                if (options_.group_bytes == 0) { options_.group_bytes = 4ULL * 1024ULL * 1024ULL; }
                if (options_.async_max_pending_bytes == 0) { options_.async_max_pending_bytes = 64ULL * 1024ULL * 1024ULL; }
                if (options_.async_max_pending_bytes < options_.group_bytes) { options_.async_max_pending_bytes = options_.group_bytes; }

                shards_.reserve(options_.shard_count);
                for (uint16_t i = 0; i < options_.shard_count; ++i) { shards_.push_back(std::make_unique<ShardWriter>(options_, i)); }
            }

            ~Impl() { close(); }

            void append(
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint64_t seq,
                uint8_t flags,
                uint64_t precomputed_fp64
            ) {
                const uint64_t fp64 = precomputed_fp64 != 0
                                          ? precomputed_fp64
                                          : (key.empty() ? 0 : core::compute_key_fp64(key.data(), key.size()));
                const uint16_t shard_id = shard_for(fp64, options_.shard_count);
                PendingEntry entry{seq, serialize_entry(key, value, seq, fp64, flags)};
                shards_[shard_id]->append(std::move(entry));
            }

            void force_sync() { for (const auto& shard : shards_) { shard->force_sync(); } }

            void prune_until(uint64_t checkpoint_seq) { for (const auto& shard : shards_) { shard->prune_until(checkpoint_seq); } }

            [[nodiscard]] WalWriterSnapshot snapshot() const noexcept {
                WalWriterSnapshot out;
                out.shard_count = static_cast<uint32_t>(shards_.size());
                for (const auto& shard : shards_) {
                    const auto snap = shard->snapshot();
                    out.entries_written += snap.entries_written;
                    out.bytes_written += snap.bytes_written;
                    out.batches_flushed += snap.batches_flushed;
                    out.syncs_executed += snap.syncs_executed;
                    out.segment_rotations += snap.segment_rotations;
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

    std::unique_ptr<WalWriter> WalWriter::create(WalOptions options) {
        auto writer = std::unique_ptr<WalWriter>(new WalWriter{});
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
        uint64_t precomputed_fp64
    ) { impl_->append(key, value, seq, flags, precomputed_fp64); }

    void WalWriter::force_sync() { impl_->force_sync(); }

    void WalWriter::prune_until(uint64_t checkpoint_seq) { impl_->prune_until(checkpoint_seq); }

    WalWriterSnapshot WalWriter::snapshot() const noexcept { return impl_ ? impl_->snapshot() : WalWriterSnapshot{}; }

    void WalWriter::close() { if (impl_) { impl_->close(); } }
} // namespace akkaradb::engine::wal

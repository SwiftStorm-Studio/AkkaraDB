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

// akkengine/src/engine/manifest/Manifest.cpp
#include "akk/engine/manifest/Manifest.hpp"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <unordered_set>
#include <utility>
#include "akk/engine/manifest/ManifestFraming.hpp"

#ifdef _WIN32
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#endif

namespace akkaradb::engine::manifest {
    // ============================================================================
    // Internal: platform-specific file handle (fsync-capable)
    // ============================================================================

    namespace {
        class FileHandle {
            public:
                #ifdef _WIN32
                using NativeHandle = HANDLE; inline static const NativeHandle INVALID = INVALID_HANDLE_VALUE;
                #else
                using NativeHandle = int;
                static constexpr NativeHandle INVALID = -1;
                #endif

                FileHandle() : handle_{INVALID} {}
                ~FileHandle() { close(); }

                FileHandle(const FileHandle&) = delete;
                FileHandle& operator=(const FileHandle&) = delete;

                FileHandle(FileHandle&& other) noexcept : handle_{other.handle_} { other.handle_ = INVALID; }

                FileHandle& operator=(FileHandle&& other) noexcept {
                    if (this != &other) {
                        close();
                        handle_ = other.handle_;
                        other.handle_ = INVALID;
                    }
                    return *this;
                }

                [[nodiscard]] static FileHandle open(const std::filesystem::path& path) {
                    FileHandle fh;
                    #ifdef _WIN32
                    fh.handle_ = ::CreateFileW(path.c_str(), GENERIC_WRITE, FILE_SHARE_READ, nullptr, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr); if (fh.
                        handle_ == INVALID) { throw std::runtime_error("Failed to open manifest: " + path.string()); } ::SetFilePointer(
                        fh.handle_,
                        0,
                        nullptr,
                        FILE_END
                    );
                    #else
                    fh.handle_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
                    if (fh.handle_ < 0) { throw std::runtime_error("Failed to open manifest: " + path.string()); }
                    #endif
                    return fh;
                }

                void write(const uint8_t* data, size_t size) {
                    #ifdef _WIN32
                    DWORD written = 0; if (!::WriteFile(handle_, data, static_cast<DWORD>(size), &written, nullptr)) {
                        throw std::runtime_error("Manifest write failed");
                    }
                    #else
                    ssize_t result = ::write(handle_, data, size);
                    if (result < 0 || static_cast<size_t>(result) != size) { throw std::runtime_error("Manifest write failed"); }
                    #endif
                }

                void fsync_data() {
                    #ifdef _WIN32
                    if (!::FlushFileBuffers(handle_)) { throw std::runtime_error("Manifest fsync failed"); }
                    #elif defined(__APPLE__)
                    if (::fcntl(handle_, F_FULLFSYNC) < 0) { throw std::runtime_error("Manifest fsync failed"); }
                    #else
                    if (::fdatasync(handle_) < 0) { throw std::runtime_error("Manifest fsync failed"); }
                    #endif
                }

                void fsync_full() {
                    #ifdef _WIN32
                    if (!::FlushFileBuffers(handle_)) { throw std::runtime_error("Manifest fsync failed"); }
                    #elif defined(__APPLE__)
                    if (::fcntl(handle_, F_FULLFSYNC) < 0) { throw std::runtime_error("Manifest fsync failed"); }
                    #else
                    if (::fsync(handle_) < 0) { throw std::runtime_error("Manifest fsync failed"); }
                    #endif
                }

                void close() noexcept {
                    if (handle_ != INVALID) {
                        #ifdef _WIN32
                        ::CloseHandle(handle_);
                        #else
                        ::close(handle_);
                        #endif
                        handle_ = INVALID;
                    }
                }

                [[nodiscard]] bool is_open() const noexcept { return handle_ != INVALID; }

            private:
                NativeHandle handle_;
        };

        // Returns current time as microseconds since epoch.
        uint64_t now_us() noexcept {
            return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
        }
    } // anonymous namespace

    // ============================================================================
    // Manifest::Impl
    // ============================================================================

    class Manifest::Impl {
        public:
            Impl(std::filesystem::path path, bool fast_mode)
                : path_{std::move(path)}, fast_mode_{fast_mode}, running_{false}, stripes_written_{0}, current_file_size_{0}, rotation_counter_{0} {
                if (path_.has_parent_path()) { std::filesystem::create_directories(path_.parent_path()); }

                replay_internal();

                rotation_counter_ = find_last_rotation_number() + 1;

                current_path_ = make_manifest_path(rotation_counter_);
                file_handle_ = FileHandle::open(current_path_);

                current_file_size_ = std::filesystem::file_size(current_path_);
                const bool is_new_file = (current_file_size_ == 0);

                if (is_new_file) { write_file_header(rotation_counter_); }
            }

            ~Impl() { close(); }

            // ----------------------------------------------------------------
            // start / close
            // ----------------------------------------------------------------

            void start() {
                if (!fast_mode_ || running_) { return; }
                running_ = true;
                last_strong_sync_ = std::chrono::steady_clock::now();
                flusher_thread_ = std::thread([this] { run_flusher(); });
            }

            void close() {
                if (fast_mode_ && running_) {
                    running_ = false;
                    queue_cv_.notify_one();
                    if (flusher_thread_.joinable()) { flusher_thread_.join(); }
                    if (term_write_error_) { std::rethrow_exception(term_write_error_); }
                }
                file_handle_.close();
            }

            // ----------------------------------------------------------------
            // Write API
            // ----------------------------------------------------------------

            void advance(uint64_t new_count) {
                {
                    std::lock_guard lock{advance_mutex_};
                    if (new_count < stripes_written_.load(std::memory_order_relaxed)) {
                        throw std::invalid_argument("Manifest: stripe counter must be monotonic");
                    }
                    stripes_written_.store(new_count, std::memory_order_relaxed);
                }
                append(ManifestRecordType::StripeCommit, encode_stripe_commit(now_us(), new_count));
            }

            void sst_seal(
                int level,
                const std::string& file,
                uint64_t entries,
                const std::optional<std::string>& first_key_hex,
                const std::optional<std::string>& last_key_hex
            ) {
                const uint64_t ts = now_us();
                append(ManifestRecordType::SSTSeal, encode_sst_seal(ts, level, file, entries, first_key_hex, last_key_hex));

                std::lock_guard lock{mutex_};
                sst_seals_.push_back(SSTSealEvent{level, file, entries, first_key_hex, last_key_hex, ts});
                live_sst_.insert(file);
                deleted_sst_.erase(file);
            }

            void checkpoint(const std::optional<std::string>& name, const std::optional<uint64_t>& stripe, const std::optional<uint64_t>& last_seq) {
                const uint64_t ts = now_us();
                append(ManifestRecordType::Checkpoint, encode_checkpoint(ts, name, stripe, last_seq));

                std::lock_guard lock{mutex_};
                last_checkpoint_ = CheckpointEvent{name, stripe, last_seq, ts};

                deleted_sst_.clear();

                std::erase_if(sst_seals_, [this](const SSTSealEvent& e) { return live_sst_.find(e.file) == live_sst_.end(); });
            }

            void compaction_start(int level, const std::vector<std::string>& inputs) {
                append(ManifestRecordType::CompactionStart, encode_compaction_start(now_us(), level, inputs));
            }

            void compaction_end(
                int level,
                const std::string& output,
                const std::vector<std::string>& inputs,
                uint64_t entries,
                const std::optional<std::string>& first_key_hex,
                const std::optional<std::string>& last_key_hex
            ) {
                append(ManifestRecordType::CompactionEnd, encode_compaction_end(now_us(), level, output, inputs, entries, first_key_hex, last_key_hex));

                std::lock_guard lock{mutex_};
                live_sst_.insert(output);
                for (const auto& inp : inputs) {
                    live_sst_.erase(inp);
                    deleted_sst_.insert(inp);
                }
            }

            void sst_delete(const std::string& file) {
                append(ManifestRecordType::SSTDelete, encode_sst_delete(now_us(), file));

                std::lock_guard lock{mutex_};
                live_sst_.erase(file);
                deleted_sst_.insert(file);
            }

            void compaction_commit(const std::vector<std::string>& output_files, const std::vector<std::string>& input_files) {
                // Single append call ↁEsingle CRC-protected record.
                // Either fully applied on replay or entirely absent (CRC mismatch).
                append(ManifestRecordType::CompactionCommit, encode_compaction_commit(now_us(), output_files, input_files));

                std::lock_guard lock{mutex_};
                for (const auto& f : output_files) {
                    live_sst_.insert(f);
                    deleted_sst_.erase(f);
                }
                for (const auto& f : input_files) {
                    live_sst_.erase(f);
                    deleted_sst_.insert(f);
                }
            }

            void truncate(const std::optional<std::string>& reason) { append(ManifestRecordType::Truncate, encode_truncate(now_us(), reason)); }

            // ----------------------------------------------------------------
            // Replay
            // ----------------------------------------------------------------

            void replay() { replay_internal(); }

            // ----------------------------------------------------------------
            // Queries
            // ----------------------------------------------------------------

            uint64_t stripes_written() const noexcept { return stripes_written_; }

            std::optional<CheckpointEvent> last_checkpoint() const noexcept {
                std::lock_guard lock{mutex_};
                return last_checkpoint_;
            }

            std::vector<std::string> live_sst() const {
                std::lock_guard lock{mutex_};
                return {live_sst_.begin(), live_sst_.end()};
            }

            std::vector<std::string> deleted_sst() const {
                std::lock_guard lock{mutex_};
                return {deleted_sst_.begin(), deleted_sst_.end()};
            }

            std::vector<SSTSealEvent> sst_seals() const {
                std::lock_guard lock{mutex_};
                return sst_seals_;
            }

        private:
            static constexpr size_t ROTATION_THRESHOLD = 32 * 1024 * 1024; // 32 MiB

            // ----------------------------------------------------------------
            // Path helpers
            // ----------------------------------------------------------------

            [[nodiscard]] std::filesystem::path make_manifest_path(size_t rotation_number) const {
                if (rotation_number == 0) { return path_; }
                return path_.parent_path() / (path_.filename().string() + "." + std::to_string(rotation_number));
            }

            [[nodiscard]] size_t find_last_rotation_number() const {
                size_t max_rotation = 0;
                if (std::filesystem::exists(path_)) { max_rotation = 0; }
                for (size_t i = 1; i < 10000; ++i) {
                    if (std::filesystem::exists(make_manifest_path(i))) { max_rotation = i; }
                    else { break; }
                }
                return max_rotation;
            }

            // ----------------------------------------------------------------
            // File header
            // ----------------------------------------------------------------

            void write_file_header(size_t rotation_counter) {
                const ManifestFileHeader fhdr = ManifestFileHeader::build(static_cast<uint32_t>(rotation_counter));

                uint8_t buf[ManifestFileHeader::SIZE];
                fhdr.serialize(buf);
                file_handle_.write(buf, ManifestFileHeader::SIZE);
                file_handle_.fsync_data();
                current_file_size_ += ManifestFileHeader::SIZE;
            }

            // ----------------------------------------------------------------
            // Rotation
            // ----------------------------------------------------------------

            void check_rotation() {
                if (current_file_size_ < ROTATION_THRESHOLD) { return; }

                file_handle_.close();
                ++rotation_counter_;
                current_path_ = make_manifest_path(rotation_counter_);
                file_handle_ = FileHandle::open(current_path_);
                current_file_size_ = 0;

                write_file_header(rotation_counter_);
            }

            // ----------------------------------------------------------------
            // Append
            // ----------------------------------------------------------------

            void append(ManifestRecordType type, std::vector<uint8_t> payload) {
                const auto plen = static_cast<uint16_t>(payload.size());
                const ManifestRecordHeader rhdr = ManifestRecordHeader::build(type, payload.data(), plen);
                payload.resize(ManifestRecordHeader::SIZE + plen);
                std::memmove(payload.data() + ManifestRecordHeader::SIZE, payload.data(), plen);
                rhdr.serialize(payload.data());

                if (fast_mode_) {
                    bool was_empty;
                    {
                        std::lock_guard lock{queue_mutex_};
                        was_empty = queue_.empty();
                        queue_.push_back(std::move(payload));
                    }
                    if (was_empty) queue_cv_.notify_one();
                }
                else {
                    std::lock_guard lock{rotation_mutex_};
                    check_rotation();
                    file_handle_.write(payload.data(), payload.size());
                    file_handle_.fsync_data();
                    current_file_size_ += payload.size();
                }
            }

            // ----------------------------------------------------------------
            // Background flusher (fast_mode only)
            // ----------------------------------------------------------------

            void run_flusher() {
                constexpr auto MAX_WAIT = std::chrono::microseconds(500);

                std::vector<std::vector<uint8_t>> batch;
                batch.reserve(64);

                try {
                    while (true) {
                        {
                            std::unique_lock lock{queue_mutex_};
                            queue_cv_.wait_for(lock, MAX_WAIT, [this] { return !queue_.empty() || !running_; });

                            if (!running_ && queue_.empty()) { break; }
                            std::swap(batch, queue_);
                        }

                        if (!batch.empty()) {
                            std::lock_guard rotation_lock{rotation_mutex_};
                            for (const auto& record : batch) {
                                check_rotation();
                                file_handle_.write(record.data(), record.size());
                                current_file_size_ += record.size();
                            }
                            file_handle_.fsync_data();
                            batch.clear();
                        }

                        auto now = std::chrono::steady_clock::now();
                        if (now - last_strong_sync_ > std::chrono::seconds(5)) {
                            std::lock_guard rotation_lock{rotation_mutex_};
                            file_handle_.fsync_full();
                            last_strong_sync_ = now;
                        }
                    }
                }
                catch (...) { term_write_error_ = std::current_exception(); }
            }

            // ----------------------------------------------------------------
            // Replay
            // ----------------------------------------------------------------

            void replay_internal() {
                {
                    std::lock_guard lock{mutex_};
                    sst_seals_.clear();
                    live_sst_.clear();
                    deleted_sst_.clear();
                    last_checkpoint_.reset();
                }
                stripes_written_.store(0, std::memory_order_relaxed);

                std::vector<std::filesystem::path> files;

                if (std::filesystem::exists(path_)) { files.push_back(path_); }
                for (size_t i = 1; i < 10000; ++i) {
                    auto p = make_manifest_path(i);
                    if (std::filesystem::exists(p)) { files.push_back(p); }
                    else { break; }
                }

                for (const auto& f : files) { replay_single_file(f); }
            }

            void replay_single_file(const std::filesystem::path& file_path) {
                if (!std::filesystem::exists(file_path)) { return; }
                const auto file_size = std::filesystem::file_size(file_path);
                if (file_size == 0) { return; }

                std::ifstream file(file_path, std::ios::binary);
                if (!file) { throw std::runtime_error("Failed to open manifest for replay: " + file_path.string()); }

                // --- Read and validate file header ---
                {
                    uint8_t hdr_buf[ManifestFileHeader::SIZE];
                    file.read(reinterpret_cast<char*>(hdr_buf), ManifestFileHeader::SIZE);
                    if (!file || file.gcount() < static_cast<std::streamsize>(ManifestFileHeader::SIZE)) {
                        return; // Too short to have a valid header
                    }

                    ManifestFileHeader fhdr{};
                    // Deserialize manually (same field order as serialize)
                    auto read_u32 = [&](size_t off) -> uint32_t {
                        return static_cast<uint32_t>(hdr_buf[off]) | (static_cast<uint32_t>(hdr_buf[off + 1]) << 8) | (static_cast<uint32_t>(hdr_buf[off + 2])
                            << 16) | (static_cast<uint32_t>(hdr_buf[off + 3]) << 24);
                    };
                    auto read_u16 = [&](size_t off) -> uint16_t {
                        return static_cast<uint16_t>(hdr_buf[off]) | (static_cast<uint16_t>(hdr_buf[off + 1]) << 8);
                    };
                    auto read_u64 = [&](size_t off) -> uint64_t {
                        uint64_t v = 0;
                        for (int i = 7; i >= 0; --i) { v = (v << 8) | hdr_buf[off + i]; }
                        return v;
                    };

                    fhdr.magic = read_u32(0);
                    fhdr.version = read_u16(4);
                    fhdr.flags = read_u16(6);
                    fhdr.file_seq = read_u32(8);
                    fhdr.created_at_us = read_u64(12);
                    fhdr.crc32c = read_u32(20);
                    std::memcpy(fhdr.reserved, hdr_buf + 24, 8);

                    if (!fhdr.verify_magic() || !fhdr.verify_version()) { return; }
                    if (!fhdr.verify_checksum()) { return; }
                }

                // --- Read records ---
                std::vector<uint8_t> payload; // reused across records  Eavoids per-record heap alloc
                while (file) {
                    uint8_t rhdr_buf[ManifestRecordHeader::SIZE];
                    file.read(reinterpret_cast<char*>(rhdr_buf), ManifestRecordHeader::SIZE);
                    if (!file || file.gcount() < static_cast<std::streamsize>(ManifestRecordHeader::SIZE)) { break; }

                    const ManifestRecordHeader rhdr = ManifestRecordHeader::deserialize(rhdr_buf);

                    if (rhdr.payload_len > 0) {
                        payload.resize(rhdr.payload_len);
                        file.read(reinterpret_cast<char*>(payload.data()), rhdr.payload_len);
                        if (!file || file.gcount() < rhdr.payload_len) { break; }

                        if (!rhdr.verify_payload(payload.data(), rhdr.payload_len)) {
                            break; // CRC mismatch  Estop replay
                        }

                        apply_event(static_cast<ManifestRecordType>(rhdr.type), payload.data(), rhdr.payload_len);
                    }
                }
            }

            void apply_event(ManifestRecordType type, const uint8_t* payload, uint16_t len) {
                switch (type) {
                    case ManifestRecordType::StripeCommit: {
                        DecodedStripeCommit d;
                        if (!decode_stripe_commit(payload, len, d)) { return; }
                        if (d.stripe_count >= stripes_written_) { stripes_written_ = d.stripe_count; }
                        break;
                    }
                    case ManifestRecordType::SSTSeal: {
                        DecodedSSTSeal d;
                        if (!decode_sst_seal(payload, len, d)) { return; }
                        std::lock_guard lock{mutex_};
                        sst_seals_.push_back(SSTSealEvent{d.level, d.name, d.entries, d.first_key_hex, d.last_key_hex, d.ts_us});
                        live_sst_.insert(d.name);
                        deleted_sst_.erase(d.name);
                        break;
                    }
                    case ManifestRecordType::SSTDelete: {
                        DecodedSSTDelete d;
                        if (!decode_sst_delete(payload, len, d)) { return; }
                        std::lock_guard lock{mutex_};
                        live_sst_.erase(d.name);
                        deleted_sst_.insert(d.name);
                        break;
                    }
                    case ManifestRecordType::CompactionEnd: {
                        DecodedCompactionEnd d;
                        if (!decode_compaction_end(payload, len, d)) { return; }
                        std::lock_guard lock{mutex_};
                        live_sst_.insert(d.output);
                        for (const auto& inp : d.inputs) {
                            live_sst_.erase(inp);
                            deleted_sst_.insert(inp);
                        }
                        break;
                    }
                    case ManifestRecordType::Checkpoint: {
                        DecodedCheckpoint d;
                        if (!decode_checkpoint(payload, len, d)) { return; }
                        std::lock_guard lock{mutex_};
                        last_checkpoint_ = CheckpointEvent{d.name, d.stripe, d.last_seq, d.ts_us};
                        deleted_sst_.clear();
                        std::erase_if(sst_seals_, [this](const SSTSealEvent& e) { return live_sst_.find(e.file) == live_sst_.end(); });
                        break;
                    }
                    case ManifestRecordType::CompactionCommit: {
                        // Atomic multi-file compaction commit.
                        // Adds all outputs and removes all inputs in one operation.
                        DecodedCompactionCommit d;
                        if (!decode_compaction_commit(payload, len, d)) { return; }
                        std::lock_guard lock{mutex_};
                        for (const auto& f : d.output_files) {
                            live_sst_.insert(f);
                            deleted_sst_.erase(f);
                        }
                        for (const auto& f : d.input_files) {
                            live_sst_.erase(f);
                            deleted_sst_.insert(f);
                        }
                        break;
                    }
                    case ManifestRecordType::CompactionStart:
                    case ManifestRecordType::NodeJoin:
                    case ManifestRecordType::NodeLeave:
                    case ManifestRecordType::PrimaryLease:
                    case ManifestRecordType::Truncate:
                        // Informational only  Eno state change
                        break;
                }
            }

            // ----------------------------------------------------------------
            // Members
            // ----------------------------------------------------------------

            std::filesystem::path path_;
            bool fast_mode_;
            std::atomic<bool> running_;
            FileHandle file_handle_;

            // Durable state
            std::atomic<uint64_t> stripes_written_;
            std::mutex advance_mutex_;
            mutable std::mutex mutex_;
            std::vector<SSTSealEvent> sst_seals_;
            std::unordered_set<std::string> live_sst_;
            std::unordered_set<std::string> deleted_sst_;
            std::optional<CheckpointEvent> last_checkpoint_;

            // Fast-mode flusher
            std::thread flusher_thread_;
            std::mutex queue_mutex_;
            std::condition_variable queue_cv_;
            std::vector<std::vector<uint8_t>> queue_;
            std::chrono::steady_clock::time_point last_strong_sync_;
            std::exception_ptr term_write_error_;

            // Rotation
            std::mutex rotation_mutex_;
            std::filesystem::path current_path_;
            size_t current_file_size_;
            size_t rotation_counter_;
    };

    // ============================================================================
    // Manifest public API  Ethin forwarding layer
    // ============================================================================

    std::unique_ptr<Manifest> Manifest::create(const std::filesystem::path& path, bool fast_mode) {
        return std::unique_ptr<Manifest>(new Manifest(path, fast_mode));
    }

    Manifest::Manifest(const std::filesystem::path& path, bool fast_mode) : impl_{std::make_unique<Impl>(path, fast_mode)} {}

    Manifest::~Manifest() = default;

    void Manifest::start() { impl_->start(); }

    void Manifest::advance(uint64_t new_count) { impl_->advance(new_count); }

    void Manifest::sst_seal(
        int level,
        const std::string& file,
        uint64_t entries,
        const std::optional<std::string>& first_key_hex,
        const std::optional<std::string>& last_key_hex
    ) { impl_->sst_seal(level, file, entries, first_key_hex, last_key_hex); }

    void Manifest::checkpoint(const std::optional<std::string>& name, const std::optional<uint64_t>& stripe, const std::optional<uint64_t>& last_seq) {
        impl_->checkpoint(name, stripe, last_seq);
    }

    void Manifest::compaction_start(int level, const std::vector<std::string>& inputs) { impl_->compaction_start(level, inputs); }

    void Manifest::compaction_end(
        int level,
        const std::string& output,
        const std::vector<std::string>& inputs,
        uint64_t entries,
        const std::optional<std::string>& first_key_hex,
        const std::optional<std::string>& last_key_hex
    ) { impl_->compaction_end(level, output, inputs, entries, first_key_hex, last_key_hex); }

    void Manifest::sst_delete(const std::string& file) { impl_->sst_delete(file); }

    void Manifest::compaction_commit(const std::vector<std::string>& output_files, const std::vector<std::string>& input_files) {
        impl_->compaction_commit(output_files, input_files);
    }

    void Manifest::truncate(const std::optional<std::string>& reason) { impl_->truncate(reason); }

    void Manifest::replay() { impl_->replay(); }

    uint64_t Manifest::stripes_written() const noexcept { return impl_->stripes_written(); }

    std::optional<Manifest::CheckpointEvent> Manifest::last_checkpoint() const noexcept { return impl_->last_checkpoint(); }

    std::vector<std::string> Manifest::live_sst() const { return impl_->live_sst(); }
    std::vector<std::string> Manifest::deleted_sst() const { return impl_->deleted_sst(); }
    std::vector<Manifest::SSTSealEvent> Manifest::sst_seals() const { return impl_->sst_seals(); }

    void Manifest::close() { impl_->close(); }
} // namespace akkaradb::engine::manifest

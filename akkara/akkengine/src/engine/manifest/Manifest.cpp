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
                using NativeHandle = HANDLE;
                inline static const NativeHandle INVALID = INVALID_HANDLE_VALUE;
                #else
                using NativeHandle = int; static constexpr NativeHandle INVALID = -1;
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
                    fh.handle_ = ::CreateFileW(
                        path.c_str(),
                        GENERIC_WRITE,
                        FILE_SHARE_READ,
                        nullptr,
                        OPEN_ALWAYS,
                        FILE_ATTRIBUTE_NORMAL,
                        nullptr
                    );
                    if (fh.handle_ == INVALID) { throw std::runtime_error("Failed to open manifest: " + path.string()); }
                    ::SetFilePointer(fh.handle_, 0, nullptr, FILE_END);
                    #else
                    fh.handle_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644); if (fh.handle_ < 0) {
                        throw std::runtime_error("Failed to open manifest: " + path.string());
                    }
                    #endif
                    return fh;
                }

                void write(const uint8_t* data, size_t size) {
                    #ifdef _WIN32
                    DWORD written = 0;
                    if (!::WriteFile(handle_, data, static_cast<DWORD>(size), &written, nullptr)) {
                        throw std::runtime_error("Manifest write failed");
                    }
                    #else
                    ssize_t result = ::write(handle_, data, size); if (result < 0 || static_cast<size_t>(result) != size) {
                        throw std::runtime_error("Manifest write failed");
                    }
                    #endif
                }

                void fsyncData() {
                    #ifdef _WIN32
                    if (!::FlushFileBuffers(handle_)) { throw std::runtime_error("Manifest fsync failed"); }
                    #elif defined(__APPLE__)
                    if (::fcntl(handle_, F_FULLFSYNC) < 0) { throw std::runtime_error("Manifest fsync failed"); }
                    #else
                    if (::fdatasync(handle_) < 0) { throw std::runtime_error("Manifest fsync failed"); }
                    #endif
                }

                void fsyncFull() {
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

                [[nodiscard]] bool isOpen() const noexcept { return handle_ != INVALID; }

            private:
                NativeHandle handle_;
        };

        // Returns current time as microseconds since epoch.
        uint64_t nowUs() noexcept {
            return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch()
            ).count());
        }
    } // anonymous namespace

    // ============================================================================
    // Manifest::Impl
    // ============================================================================

    class Manifest::Impl {
        public:
            Impl(std::filesystem::path path, bool fastMode)
                : path_{std::move(path)},
                  fastMode_{fastMode},
                  running_{false},
                  stripesWritten_{0},
                  currentFileSize_{0},
                  rotationCounter_{0} {
                if (path_.has_parent_path()) { std::filesystem::create_directories(path_.parent_path()); }

                replayInternal();

                rotationCounter_ = findLastRotationNumber() + 1;

                currentPath_ = makeManifestPath(rotationCounter_);
                fileHandle_ = FileHandle::open(currentPath_);

                currentFileSize_ = std::filesystem::file_size(currentPath_);
                const bool isNewFile = (currentFileSize_ == 0);

                if (isNewFile) { writeFileHeader(rotationCounter_); }
            }

            ~Impl() { close(); }

            // ----------------------------------------------------------------
            // start / close
            // ----------------------------------------------------------------

            void start() {
                if (!fastMode_ || running_) { return; }
                running_ = true;
                lastStrongSync_ = std::chrono::steady_clock::now();
                flusherThread_ = std::thread([this] { runFlusher(); });
            }

            void close() {
                if (fastMode_ && running_) {
                    running_ = false;
                    queueCv_.notify_one();
                    if (flusherThread_.joinable()) { flusherThread_.join(); }
                    if (termWriteError_) { std::rethrow_exception(termWriteError_); }
                }
                fileHandle_.close();
            }

            // ----------------------------------------------------------------
            // Write API
            // ----------------------------------------------------------------

            void advance(uint64_t newCount) {
                {
                    std::lock_guard lock{advanceMutex_};
                    if (newCount < stripesWritten_.load(std::memory_order_relaxed)) {
                        throw std::invalid_argument("Manifest: stripe counter must be monotonic");
                    }
                    stripesWritten_.store(newCount, std::memory_order_relaxed);
                }
                append(ManifestRecordType::STRIPE_COMMIT, encodeStripeCommit(nowUs(), newCount));
            }

            void sstSeal(
                int level,
                const std::string& file,
                uint64_t entries,
                const std::optional<std::string>& firstKeyHex,
                const std::optional<std::string>& lastKeyHex
            ) {
                const uint64_t ts = nowUs();
                append(ManifestRecordType::SST_SEAL, encodeSstSeal(ts, level, file, entries, firstKeyHex, lastKeyHex));

                std::lock_guard lock{mutex_};
                sstSeals_.push_back(SSTSealEvent{level, file, entries, firstKeyHex, lastKeyHex, ts});
                liveSst_.insert(file);
                deletedSst_.erase(file);
            }

            void checkpoint(
                const std::optional<std::string>& name,
                const std::optional<uint64_t>& stripe,
                const std::optional<uint64_t>& lastSeq
            ) {
                const uint64_t ts = nowUs();
                append(ManifestRecordType::CHECKPOINT, encodeCheckpoint(ts, name, stripe, lastSeq));

                std::lock_guard lock{mutex_};
                lastCheckpoint_ = CheckpointEvent{name, stripe, lastSeq, ts};

                deletedSst_.clear();

                std::erase_if(sstSeals_, [this](const SSTSealEvent& e) { return liveSst_.find(e.file) == liveSst_.end(); });
            }

            void compactionStart(int level, const std::vector<std::string>& inputs) {
                append(ManifestRecordType::COMPACTION_START, encodeCompactionStart(nowUs(), level, inputs));
            }

            void compactionEnd(
                int level,
                const std::string& output,
                const std::vector<std::string>& inputs,
                uint64_t entries,
                const std::optional<std::string>& firstKeyHex,
                const std::optional<std::string>& lastKeyHex
            ) {
                append(
                    ManifestRecordType::COMPACTION_END,
                    encodeCompactionEnd(nowUs(), level, output, inputs, entries, firstKeyHex, lastKeyHex)
                );

                std::lock_guard lock{mutex_};
                liveSst_.insert(output);
                for (const auto& inp : inputs) {
                    liveSst_.erase(inp);
                    deletedSst_.insert(inp);
                }
            }

            void sstDelete(const std::string& file) {
                append(ManifestRecordType::SST_DELETE, encodeSstDelete(nowUs(), file));

                std::lock_guard lock{mutex_};
                liveSst_.erase(file);
                deletedSst_.insert(file);
            }

            void compactionCommit(const std::vector<std::string>& outputFiles, const std::vector<std::string>& inputFiles) {
                // Single append call ↁEsingle CRC-protected record.
                // Either fully applied on replay or entirely absent (CRC mismatch).
                append(ManifestRecordType::COMPACTION_COMMIT, encodeCompactionCommit(nowUs(), outputFiles, inputFiles));

                std::lock_guard lock{mutex_};
                for (const auto& f : outputFiles) {
                    liveSst_.insert(f);
                    deletedSst_.erase(f);
                }
                for (const auto& f : inputFiles) {
                    liveSst_.erase(f);
                    deletedSst_.insert(f);
                }
            }

            void truncate(const std::optional<std::string>& reason) {
                append(ManifestRecordType::TRUNCATE, encodeTruncate(nowUs(), reason));
            }

            void nodeJoin(uint64_t nodeId, uint16_t replPort, const std::string& host) {
                const uint64_t ts = nowUs();
                append(ManifestRecordType::NODE_JOIN, encodeNodeJoin(ts, nodeId, replPort, host));

                std::lock_guard lock{mutex_};
                nodeJoins_.push_back(NodeJoinEvent{nodeId, replPort, host, ts});
            }

            void nodeLeave(uint64_t nodeId) {
                const uint64_t ts = nowUs();
                append(ManifestRecordType::NODE_LEAVE, encodeNodeLeave(ts, nodeId));

                std::lock_guard lock{mutex_};
                nodeLeaves_.push_back(NodeLeaveEvent{nodeId, ts});
            }

            void primaryLease(uint64_t nodeId, uint64_t leaseUntilUs) {
                const uint64_t ts = nowUs();
                append(ManifestRecordType::PRIMARY_LEASE, encodePrimaryLease(ts, nodeId, leaseUntilUs));

                std::lock_guard lock{mutex_};
                lastPrimaryLease_ = PrimaryLeaseEvent{nodeId, leaseUntilUs, ts};
            }

            // ----------------------------------------------------------------
            // Replay
            // ----------------------------------------------------------------

            void replay() { replayInternal(); }

            // ----------------------------------------------------------------
            // Queries
            // ----------------------------------------------------------------

            uint64_t stripesWritten() const noexcept { return stripesWritten_; }

            std::optional<CheckpointEvent> lastCheckpoint() const noexcept {
                std::lock_guard lock{mutex_};
                return lastCheckpoint_;
            }

            std::vector<std::string> liveSst() const {
                std::lock_guard lock{mutex_};
                return {liveSst_.begin(), liveSst_.end()};
            }

            std::vector<std::string> deletedSst() const {
                std::lock_guard lock{mutex_};
                return {deletedSst_.begin(), deletedSst_.end()};
            }

            std::vector<SSTSealEvent> sstSeals() const {
                std::lock_guard lock{mutex_};
                return sstSeals_;
            }

            std::vector<NodeJoinEvent> nodeJoins() const {
                std::lock_guard lock{mutex_};
                return nodeJoins_;
            }

            std::vector<NodeLeaveEvent> nodeLeaves() const {
                std::lock_guard lock{mutex_};
                return nodeLeaves_;
            }

            std::optional<PrimaryLeaseEvent> lastPrimaryLease() const noexcept {
                std::lock_guard lock{mutex_};
                return lastPrimaryLease_;
            }

        private:
            static constexpr size_t ROTATION_THRESHOLD = 32 * 1024 * 1024; // 32 MiB

            // ----------------------------------------------------------------
            // Path helpers
            // ----------------------------------------------------------------

            [[nodiscard]] std::filesystem::path makeManifestPath(size_t rotationNumber) const {
                if (rotationNumber == 0) { return path_; }
                return path_.parent_path() / (path_.filename().string() + "." + std::to_string(rotationNumber));
            }

            [[nodiscard]] size_t findLastRotationNumber() const {
                size_t maxRotation = 0;
                if (std::filesystem::exists(path_)) { maxRotation = 0; }
                for (size_t i = 1; i < 10000; ++i) {
                    if (std::filesystem::exists(makeManifestPath(i))) { maxRotation = i; }
                    else { break; }
                }
                return maxRotation;
            }

            // ----------------------------------------------------------------
            // File header
            // ----------------------------------------------------------------

            void writeFileHeader(size_t rotationCounter) {
                const ManifestFileHeader fhdr = ManifestFileHeader::build(static_cast<uint32_t>(rotationCounter));

                uint8_t buf[ManifestFileHeader::SIZE];
                fhdr.serialize(buf);
                fileHandle_.write(buf, ManifestFileHeader::SIZE);
                fileHandle_.fsyncData();
                currentFileSize_ += ManifestFileHeader::SIZE;
            }

            // ----------------------------------------------------------------
            // Rotation
            // ----------------------------------------------------------------

            void checkRotation() {
                if (currentFileSize_ < ROTATION_THRESHOLD) { return; }

                fileHandle_.close();
                ++rotationCounter_;
                currentPath_ = makeManifestPath(rotationCounter_);
                fileHandle_ = FileHandle::open(currentPath_);
                currentFileSize_ = 0;

                writeFileHeader(rotationCounter_);
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

                if (fastMode_) {
                    bool wasEmpty;
                    {
                        std::lock_guard lock{queueMutex_};
                        wasEmpty = queue_.empty();
                        queue_.push_back(std::move(payload));
                    }
                    if (wasEmpty) queueCv_.notify_one();
                }
                else {
                    std::lock_guard lock{rotationMutex_};
                    checkRotation();
                    fileHandle_.write(payload.data(), payload.size());
                    fileHandle_.fsyncData();
                    currentFileSize_ += payload.size();
                }
            }

            // ----------------------------------------------------------------
            // Background flusher (fastMode only)
            // ----------------------------------------------------------------

            void runFlusher() {
                constexpr auto MAX_WAIT = std::chrono::microseconds(500);

                std::vector<std::vector<uint8_t>> batch;
                batch.reserve(64);

                try {
                    while (true) {
                        {
                            std::unique_lock lock{queueMutex_};
                            queueCv_.wait_for(lock, MAX_WAIT, [this] { return !queue_.empty() || !running_; });

                            if (!running_ && queue_.empty()) { break; }
                            std::swap(batch, queue_);
                        }

                        if (!batch.empty()) {
                            std::lock_guard rotationLock{rotationMutex_};
                            for (const auto& record : batch) {
                                checkRotation();
                                fileHandle_.write(record.data(), record.size());
                                currentFileSize_ += record.size();
                            }
                            fileHandle_.fsyncData();
                            batch.clear();
                        }

                        auto now = std::chrono::steady_clock::now();
                        if (now - lastStrongSync_ > std::chrono::seconds(5)) {
                            std::lock_guard rotationLock{rotationMutex_};
                            fileHandle_.fsyncFull();
                            lastStrongSync_ = now;
                        }
                    }
                }
                catch (...) { termWriteError_ = std::current_exception(); }
            }

            // ----------------------------------------------------------------
            // Replay
            // ----------------------------------------------------------------

            void replayInternal() {
                {
                    std::lock_guard lock{mutex_};
                    sstSeals_.clear();
                    liveSst_.clear();
                    deletedSst_.clear();
                    lastCheckpoint_.reset();
                    nodeJoins_.clear();
                    nodeLeaves_.clear();
                    lastPrimaryLease_.reset();
                }
                stripesWritten_.store(0, std::memory_order_relaxed);

                std::vector<std::filesystem::path> files;

                if (std::filesystem::exists(path_)) { files.push_back(path_); }
                for (size_t i = 1; i < 10000; ++i) {
                    auto p = makeManifestPath(i);
                    if (std::filesystem::exists(p)) { files.push_back(p); }
                    else { break; }
                }

                for (const auto& f : files) { replaySingleFile(f); }
            }

            void replaySingleFile(const std::filesystem::path& filePath) {
                if (!std::filesystem::exists(filePath)) { return; }
                const auto file_size = std::filesystem::file_size(filePath);
                if (file_size == 0) { return; }

                std::ifstream file(filePath, std::ios::binary);
                if (!file) { throw std::runtime_error("Failed to open manifest for replay: " + filePath.string()); }

                // --- Read and validate file header ---
                {
                    uint8_t hdrBuf[ManifestFileHeader::SIZE];
                    file.read(reinterpret_cast<char*>(hdrBuf), ManifestFileHeader::SIZE);
                    if (!file || file.gcount() < static_cast<std::streamsize>(ManifestFileHeader::SIZE)) {
                        return; // Too short to have a valid header
                    }

                    ManifestFileHeader fhdr{};
                    // Deserialize manually (same field order as serialize)
                    auto readU32 = [&](size_t off) -> uint32_t {
                        return static_cast<uint32_t>(hdrBuf[off]) | (static_cast<uint32_t>(hdrBuf[off + 1]) << 8) | (static_cast<uint32_t>(
                            hdrBuf[off + 2]) << 16) | (static_cast<uint32_t>(hdrBuf[off + 3]) << 24);
                    };
                    auto readU16 = [&](size_t off) -> uint16_t {
                        return static_cast<uint16_t>(hdrBuf[off]) | (static_cast<uint16_t>(hdrBuf[off + 1]) << 8);
                    };
                    auto readU64 = [&](size_t off) -> uint64_t {
                        uint64_t v = 0;
                        for (int i = 7; i >= 0; --i) { v = (v << 8) | hdrBuf[off + i]; }
                        return v;
                    };

                    fhdr.magic = readU32(0);
                    fhdr.version = readU16(4);
                    fhdr.flags = readU16(6);
                    fhdr.fileSeq = readU32(8);
                    fhdr.createdAtUs = readU64(12);
                    fhdr.crc32c = readU32(20);
                    std::memcpy(fhdr.reserved, hdrBuf + 24, 8);

                    if (!fhdr.verifyMagic() || !fhdr.verifyVersion()) { return; }
                    if (!fhdr.verifyChecksum()) { return; }
                }

                // --- Read records ---
                std::vector<uint8_t> payload; // reused across records  Eavoids per-record heap alloc
                while (file) {
                    uint8_t rhdrBuf[ManifestRecordHeader::SIZE];
                    file.read(reinterpret_cast<char*>(rhdrBuf), ManifestRecordHeader::SIZE);
                    if (!file || file.gcount() < static_cast<std::streamsize>(ManifestRecordHeader::SIZE)) { break; }

                    const ManifestRecordHeader rhdr = ManifestRecordHeader::deserialize(rhdrBuf);

                    if (rhdr.payloadLen > 0) {
                        payload.resize(rhdr.payloadLen);
                        file.read(reinterpret_cast<char*>(payload.data()), rhdr.payloadLen);
                        if (!file || file.gcount() < rhdr.payloadLen) { break; }

                        if (!rhdr.verifyPayload(payload.data(), rhdr.payloadLen)) {
                            break; // CRC mismatch  Estop replay
                        }

                        applyEvent(static_cast<ManifestRecordType>(rhdr.type), payload.data(), rhdr.payloadLen);
                    }
                }
            }

            void applyEvent(ManifestRecordType type, const uint8_t* payload, uint16_t len) {
                switch (type) {
                    case ManifestRecordType::STRIPE_COMMIT: {
                        DecodedStripeCommit d;
                        if (!decodeStripeCommit(payload, len, d)) { return; }
                        if (d.stripeCount >= stripesWritten_) { stripesWritten_ = d.stripeCount; }
                        break;
                    }
                    case ManifestRecordType::SST_SEAL: {
                        DecodedSSTSeal d;
                        if (!decodeSstSeal(payload, len, d)) { return; }
                        std::lock_guard lock{mutex_};
                        sstSeals_.push_back(SSTSealEvent{d.level, d.name, d.entries, d.firstKeyHex, d.lastKeyHex, d.tsUs});
                        liveSst_.insert(d.name);
                        deletedSst_.erase(d.name);
                        break;
                    }
                    case ManifestRecordType::SST_DELETE: {
                        DecodedSSTDelete d;
                        if (!decodeSstDelete(payload, len, d)) { return; }
                        std::lock_guard lock{mutex_};
                        liveSst_.erase(d.name);
                        deletedSst_.insert(d.name);
                        break;
                    }
                    case ManifestRecordType::COMPACTION_END: {
                        DecodedCompactionEnd d;
                        if (!decodeCompactionEnd(payload, len, d)) { return; }
                        std::lock_guard lock{mutex_};
                        liveSst_.insert(d.output);
                        for (const auto& inp : d.inputs) {
                            liveSst_.erase(inp);
                            deletedSst_.insert(inp);
                        }
                        break;
                    }
                    case ManifestRecordType::CHECKPOINT: {
                        DecodedCheckpoint d;
                        if (!decodeCheckpoint(payload, len, d)) { return; }
                        std::lock_guard lock{mutex_};
                        lastCheckpoint_ = CheckpointEvent{d.name, d.stripe, d.lastSeq, d.tsUs};
                        deletedSst_.clear();
                        std::erase_if(sstSeals_, [this](const SSTSealEvent& e) { return liveSst_.find(e.file) == liveSst_.end(); });
                        break;
                    }
                    case ManifestRecordType::COMPACTION_COMMIT: {
                        // Atomic multi-file compaction commit.
                        // Adds all outputs and removes all inputs in one operation.
                        DecodedCompactionCommit d;
                        if (!decodeCompactionCommit(payload, len, d)) { return; }
                        std::lock_guard lock{mutex_};
                        for (const auto& f : d.outputFiles) {
                            liveSst_.insert(f);
                            deletedSst_.erase(f);
                        }
                        for (const auto& f : d.inputFiles) {
                            liveSst_.erase(f);
                            deletedSst_.insert(f);
                        }
                        break;
                    }
                    case ManifestRecordType::COMPACTION_START:
                    case ManifestRecordType::NODE_JOIN:
                    case ManifestRecordType::NODE_LEAVE:
                    case ManifestRecordType::PRIMARY_LEASE:
                    case ManifestRecordType::TRUNCATE:
                        // Informational only  Eno state change
                        break;
                }
            }

            // ----------------------------------------------------------------
            // Members
            // ----------------------------------------------------------------

            std::filesystem::path path_;
            bool fastMode_;
            std::atomic<bool> running_;
            FileHandle fileHandle_;

            // Durable state
            std::atomic<uint64_t> stripesWritten_;
            std::mutex advanceMutex_;
            mutable std::mutex mutex_;
            std::vector<SSTSealEvent> sstSeals_;
            std::unordered_set<std::string> liveSst_;
            std::unordered_set<std::string> deletedSst_;
            std::optional<CheckpointEvent> lastCheckpoint_;
            std::vector<NodeJoinEvent> nodeJoins_;
            std::vector<NodeLeaveEvent> nodeLeaves_;
            std::optional<PrimaryLeaseEvent> lastPrimaryLease_;

            // Fast-mode flusher
            std::thread flusherThread_;
            std::mutex queueMutex_;
            std::condition_variable queueCv_;
            std::vector<std::vector<uint8_t>> queue_;
            std::chrono::steady_clock::time_point lastStrongSync_;
            std::exception_ptr termWriteError_;

            // Rotation
            std::mutex rotationMutex_;
            std::filesystem::path currentPath_;
            size_t currentFileSize_;
            size_t rotationCounter_;
    };

    // ============================================================================
    // Manifest public API  Ethin forwarding layer
    // ============================================================================

    std::unique_ptr<Manifest> Manifest::create(const std::filesystem::path& path, bool fastMode) {
        return std::unique_ptr<Manifest>(new Manifest(path, fastMode));
    }

    Manifest::Manifest(const std::filesystem::path& path, bool fastMode) : impl_{std::make_unique<Impl>(path, fastMode)} {}

    Manifest::~Manifest() = default;

    void Manifest::start() { impl_->start(); }

    void Manifest::advance(uint64_t newCount) { impl_->advance(newCount); }

    void Manifest::sstSeal(
        int level,
        const std::string& file,
        uint64_t entries,
        const std::optional<std::string>& firstKeyHex,
        const std::optional<std::string>& lastKeyHex
    ) { impl_->sstSeal(level, file, entries, firstKeyHex, lastKeyHex); }

    void Manifest::checkpoint(
        const std::optional<std::string>& name,
        const std::optional<uint64_t>& stripe,
        const std::optional<uint64_t>& lastSeq
    ) { impl_->checkpoint(name, stripe, lastSeq); }

    void Manifest::compactionStart(int level, const std::vector<std::string>& inputs) { impl_->compactionStart(level, inputs); }

    void Manifest::compactionEnd(
        int level,
        const std::string& output,
        const std::vector<std::string>& inputs,
        uint64_t entries,
        const std::optional<std::string>& firstKeyHex,
        const std::optional<std::string>& lastKeyHex
    ) { impl_->compactionEnd(level, output, inputs, entries, firstKeyHex, lastKeyHex); }

    void Manifest::sstDelete(const std::string& file) { impl_->sstDelete(file); }

    void Manifest::compactionCommit(const std::vector<std::string>& outputFiles, const std::vector<std::string>& inputFiles) {
        impl_->compactionCommit(outputFiles, inputFiles);
    }

    void Manifest::truncate(const std::optional<std::string>& reason) { impl_->truncate(reason); }

    void Manifest::nodeJoin(uint64_t nodeId, uint16_t replPort, const std::string& host) { impl_->nodeJoin(nodeId, replPort, host); }

    void Manifest::nodeLeave(uint64_t nodeId) { impl_->nodeLeave(nodeId); }

    void Manifest::primaryLease(uint64_t nodeId, uint64_t leaseUntilUs) { impl_->primaryLease(nodeId, leaseUntilUs); }

    void Manifest::replay() { impl_->replay(); }

    uint64_t Manifest::stripesWritten() const noexcept { return impl_->stripesWritten(); }

    std::optional<Manifest::CheckpointEvent> Manifest::lastCheckpoint() const noexcept { return impl_->lastCheckpoint(); }

    std::vector<std::string> Manifest::liveSst() const { return impl_->liveSst(); }
    std::vector<std::string> Manifest::deletedSst() const { return impl_->deletedSst(); }
    std::vector<Manifest::SSTSealEvent> Manifest::sstSeals() const { return impl_->sstSeals(); }
    std::vector<Manifest::NodeJoinEvent> Manifest::nodeJoins() const { return impl_->nodeJoins(); }
    std::vector<Manifest::NodeLeaveEvent> Manifest::nodeLeaves() const { return impl_->nodeLeaves(); }
    std::optional<Manifest::PrimaryLeaseEvent> Manifest::lastPrimaryLease() const noexcept { return impl_->lastPrimaryLease(); }

    void Manifest::close() { impl_->close(); }
} // namespace akkaradb::engine::manifest

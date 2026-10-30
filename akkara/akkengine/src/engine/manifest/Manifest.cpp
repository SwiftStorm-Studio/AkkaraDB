/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/manifest/Manifest.cpp
#include "akk/engine/manifest/Manifest.hpp"
#include "akk/engine/detail/StorageFileLock.hpp"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <charconv>
#include <cerrno>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <map>
#include <mutex>
#include <random>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <unordered_map>
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
        [[nodiscard]] bool pathExists(const std::filesystem::path& path) {
            #ifdef _WIN32
            const DWORD attrs = ::GetFileAttributesW(path.c_str()); return attrs != INVALID_FILE_ATTRIBUTES;
            #else
            return std::filesystem::exists(path);
            #endif
        }

        [[nodiscard]] uint64_t pathFileSize(const std::filesystem::path& path) {
            #ifdef _WIN32
            WIN32_FILE_ATTRIBUTE_DATA data{}; if (!::GetFileAttributesExW(path.c_str(), GetFileExInfoStandard, &data)) {
                throw std::runtime_error("Failed to stat manifest: " + path.string());
            } return (static_cast<uint64_t>(data.nFileSizeHigh) << 32) | static_cast<uint64_t>(data.nFileSizeLow);
            #else
            return std::filesystem::file_size(path);
            #endif
        }

        [[nodiscard]] ManifestFileHeader readFileHeader(const std::filesystem::path& path) {
            std::ifstream file(path, std::ios::binary);
            uint8_t bytes[ManifestFileHeader::SIZE];
            file.read(reinterpret_cast<char*>(bytes), sizeof(bytes));
            if (!file) { throw std::runtime_error("Manifest: missing or truncated file header: " + path.string()); }
            const auto readLe = [&](size_t offset, size_t size) {
                uint64_t value = 0;
                for (size_t i = 0; i < size; ++i) { value |= static_cast<uint64_t>(bytes[offset + i]) << (i * 8); }
                return value;
            };
            ManifestFileHeader header{};
            header.magic = static_cast<uint32_t>(readLe(0, 4));
            header.version = static_cast<uint16_t>(readLe(4, 2));
            header.flags = static_cast<uint16_t>(readLe(6, 2));
            header.fileSeq = static_cast<uint32_t>(readLe(8, 4));
            header.createdAtUs = readLe(12, 8);
            header.crc32c = static_cast<uint32_t>(readLe(20, 4));
            std::memcpy(header.reserved, bytes + 24, sizeof(header.reserved));
            if (!header.verifyMagic() || !header.verifyVersion()) {
                throw std::runtime_error("Manifest: unsupported format; v1 is required (recreate the pre-release database)");
            }
            if (!header.verifyChecksum()) { throw std::runtime_error("Manifest: corrupt file header: " + path.string()); }
            return header;
        }

        void syncDirectory(const std::filesystem::path& path) {
            #ifndef _WIN32
            const auto directory = path.empty() ? std::filesystem::path{"."} : path;
            const int fd = ::open(directory.c_str(), O_RDONLY | O_DIRECTORY);
            if (fd < 0) { throw std::runtime_error("Manifest: cannot open directory for sync"); }
            const int result = ::fsync(fd);
            ::close(fd);
            if (result != 0) { throw std::runtime_error("Manifest: directory sync failed"); }
            #else
            (void)path; // Replacement uses MOVEFILE_WRITE_THROUGH on Windows.
            #endif
        }

        void replaceFile(const std::filesystem::path& source, const std::filesystem::path& destination) {
            #ifdef _WIN32
            if (!::MoveFileExW(source.c_str(), destination.c_str(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
                throw std::runtime_error("Manifest: atomic replacement failed: " + std::to_string(::GetLastError()));
            }
            #else
            std::filesystem::rename(source, destination);
            #endif
        }

        void crashAtTestPoint(const char* point) noexcept {
            const char* requested = std::getenv("AKKARADB_TEST_CRASH_POINT");
            if (requested && std::strcmp(requested, point) == 0) { std::quick_exit(86); }
        }

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
                    fh.handle_ = ::CreateFileW(
                        path.c_str(),
                        GENERIC_WRITE,
                        FILE_SHARE_READ,
                        nullptr,
                        OPEN_ALWAYS,
                        FILE_ATTRIBUTE_NORMAL,
                        nullptr
                    ); if (fh.handle_ == INVALID) { throw std::runtime_error("Failed to open manifest: " + path.string()); }
                    LARGE_INTEGER end{};
                    if (!::SetFilePointerEx(fh.handle_, end, nullptr, FILE_END)) { throw std::runtime_error("Manifest seek failed"); }
                    #else
                    fh.handle_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
                    if (fh.handle_ < 0) { throw std::runtime_error("Failed to open manifest: " + path.string()); }
                    #endif
                    return fh;
                }

                void write(const uint8_t* data, size_t size) {
                    while (size != 0) {
                        #ifdef _WIN32
                        DWORD written = 0;
                        const auto chunk = static_cast<DWORD>(std::min<size_t>(size, UINT32_MAX));
                        if (!::WriteFile(handle_, data, chunk, &written, nullptr) || written == 0) {
                            throw std::runtime_error("Manifest write failed");
                        }
                        const size_t count = written;
                        #else
                        const auto written = ::write(handle_, data, std::min<size_t>(size, 1024u * 1024u));
                        if (written < 0 && errno == EINTR) { continue; }
                        if (written <= 0) { throw std::runtime_error("Manifest write failed"); }
                        const size_t count = static_cast<size_t>(written);
                        #endif
                        data += count;
                        size -= count;
                    }
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
                ownershipLock_ = std::make_unique<detail::StorageFileLock>(path_.parent_path() / (".lock-" + path_.filename().string()));

                const bool manifestExistsNow = pathExists(path_);
                if (!manifestExistsNow && mayHaveManifestHistory()) {
                    throw std::runtime_error("Manifest: base file is missing while manifest history remains");
                }
                if (manifestExistsNow) { replayInternal(true); }

                const auto files = manifestFiles();
                rotationCounter_ = static_cast<size_t>(baseFileSeq_) + files.size() - 1;
                currentPath_ = files.back();
                fileHandle_ = FileHandle::open(currentPath_);

                currentFileSize_ = pathFileSize(currentPath_);
                const bool isNewFile = (currentFileSize_ == 0);

                if (isNewFile) {
                    catalogHeader_ = ManifestFileHeader::build(0);
                    std::random_device random;
                    for (auto& byte : catalogHeader_.reserved) { byte = static_cast<uint8_t>(random()); }
                    writeFileHeader(rotationCounter_);
                }
            }

            // Explicit close reports persistence failures; destruction cannot throw.
            ~Impl() { try { close(); } catch (...) {} }

            // ----------------------------------------------------------------
            // start / close
            // ----------------------------------------------------------------

            void start() {
                std::lock_guard apiLock{apiMutex_};
                if (!ownershipLock_) { throw std::runtime_error("Manifest: already closed"); }
                if (!fastMode_ || running_) { return; }
                running_ = true;
                lastStrongSync_ = std::chrono::steady_clock::now();
                flusherThread_ = std::thread([this] { runFlusher(); });
            }

            void close() {
                std::lock_guard apiLock{apiMutex_};
                if (fastMode_ && running_) {
                    running_ = false;
                    queueCv_.notify_one();
                    if (flusherThread_.joinable()) { flusherThread_.join(); }
                }
                std::lock_guard rotationLock{rotationMutex_};
                fileHandle_.close();
                ownershipLock_.reset();
                if (termWriteError_) { std::rethrow_exception(termWriteError_); }
            }

            // ----------------------------------------------------------------
            // Write API
            // ----------------------------------------------------------------

            void advance(uint64_t newCount) {
                std::lock_guard apiLock{apiMutex_};
                {
                    std::lock_guard lock{advanceMutex_};
                    if (newCount < stripesWritten_.load(std::memory_order_relaxed)) {
                        throw std::invalid_argument("Manifest: stripe counter must be monotonic");
                    }
                }
                append(ManifestRecordType::STRIPE_COMMIT, encodeStripeCommit(nowUs(), newCount));
                stripesWritten_.store(newCount, std::memory_order_relaxed);
            }

            void sstSeal(
                int level,
                const std::string& file,
                uint64_t entries,
                const std::optional<std::string>& firstKeyHex,
                const std::optional<std::string>& lastKeyHex
            ) {
                std::lock_guard apiLock{apiMutex_};
                const uint64_t ts = nowUs();
                append(ManifestRecordType::SST_SEAL, encodeSstSeal(ts, level, file, entries, firstKeyHex, lastKeyHex));
                flushPendingForCompact();

                std::lock_guard lock{mutex_};
                sstSeals_.push_back(SSTSealEvent{level, file, entries, firstKeyHex, lastKeyHex, ts});
                liveSst_.insert(file);
                deletedSst_.erase(file);
            }

            void sstBlobRefs(const std::string& file, const std::vector<SSTBlobRefsEvent::Entry>& entries) {
                std::lock_guard apiLock{apiMutex_};
                const uint64_t ts = nowUs();
                append(ManifestRecordType::SST_BLOB_REFS, encodeSstBlobRefs(ts, file, toFramingEntries(entries)));

                std::lock_guard lock{mutex_};
                auto stored = sortedUniqueEntries(entries);
                sstBlobRefs_.push_back(SSTBlobRefsEvent{file, stored, ts});
                sstBlobRefsByFile_[file] = std::move(stored);
            }

            void checkpoint(
                const std::optional<std::string>& name,
                const std::optional<uint64_t>& stripe,
                const std::optional<uint64_t>& lastSeq
            ) {
                std::lock_guard apiLock{apiMutex_};
                const uint64_t ts = nowUs();
                append(ManifestRecordType::CHECKPOINT, encodeCheckpoint(ts, name, stripe, lastSeq));
                flushPendingForCompact();

                std::lock_guard lock{mutex_};
                lastCheckpoint_ = CheckpointEvent{name, stripe, lastSeq, ts};

                deletedSst_.clear();

                std::erase_if(sstSeals_, [this](const SSTSealEvent& e) { return liveSst_.find(e.file) == liveSst_.end(); });
            }

            void compactionStart(int level, const std::vector<std::string>& inputs) {
                std::lock_guard apiLock{apiMutex_};
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
                std::lock_guard apiLock{apiMutex_};
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
                std::lock_guard apiLock{apiMutex_};
                append(ManifestRecordType::SST_DELETE, encodeSstDelete(nowUs(), file));

                std::lock_guard lock{mutex_};
                liveSst_.erase(file);
                deletedSst_.insert(file);
                sstBlobRefsByFile_.erase(file);
            }

            void compactionCommit(const std::vector<std::string>& outputFiles, const std::vector<std::string>& inputFiles) {
                std::lock_guard apiLock{apiMutex_};
                // Single append call -> single CRC-protected record.
                // Replay applies a complete valid record, repairs an incomplete tail,
                // or rejects corruption without changing the catalog.
                append(ManifestRecordType::COMPACTION_COMMIT, encodeCompactionCommit(nowUs(), outputFiles, inputFiles));
                flushPendingForCompact();

                std::lock_guard lock{mutex_};
                for (const auto& f : outputFiles) {
                    liveSst_.insert(f);
                    deletedSst_.erase(f);
                }
                for (const auto& f : inputFiles) {
                    liveSst_.erase(f);
                    deletedSst_.insert(f);
                    sstBlobRefsByFile_.erase(f);
                }
            }

            void truncate(const std::optional<std::string>& reason) {
                std::lock_guard apiLock{apiMutex_};
                append(ManifestRecordType::TRUNCATE, encodeTruncate(nowUs(), reason));
            }

            void nodeJoin(uint64_t nodeId, uint16_t replPort, const std::string& host) {
                std::lock_guard apiLock{apiMutex_};
                const uint64_t ts = nowUs();
                append(ManifestRecordType::NODE_JOIN, encodeNodeJoin(ts, nodeId, replPort, host));

                std::lock_guard lock{mutex_};
                nodeJoins_.push_back(NodeJoinEvent{nodeId, replPort, host, ts});
            }

            void nodeLeave(uint64_t nodeId) {
                std::lock_guard apiLock{apiMutex_};
                const uint64_t ts = nowUs();
                append(ManifestRecordType::NODE_LEAVE, encodeNodeLeave(ts, nodeId));

                std::lock_guard lock{mutex_};
                nodeLeaves_.push_back(NodeLeaveEvent{nodeId, ts});
            }

            void primaryLease(uint64_t nodeId, uint64_t leaseUntilUs) {
                std::lock_guard apiLock{apiMutex_};
                const uint64_t ts = nowUs();
                append(ManifestRecordType::PRIMARY_LEASE, encodePrimaryLease(ts, nodeId, leaseUntilUs));

                std::lock_guard lock{mutex_};
                lastPrimaryLease_ = PrimaryLeaseEvent{nodeId, leaseUntilUs, ts};
            }

            void blobPut(uint64_t blobId, uint64_t totalSize, uint64_t storedSize, uint32_t contentCrc32c, uint32_t codec) {
                std::lock_guard apiLock{apiMutex_};
                const uint64_t ts = nowUs();
                append(ManifestRecordType::BLOB_PUT, encodeBlobPut(ts, blobId, totalSize, storedSize, contentCrc32c, codec));

                std::lock_guard lock{mutex_};
                blobPuts_.push_back(BlobPutEvent{blobId, totalSize, storedSize, contentCrc32c, codec, ts});
                liveBlobs_.insert(blobId);
                deletedBlobs_.erase(blobId);
            }

            void blobDelete(uint64_t blobId) {
                std::lock_guard apiLock{apiMutex_};
                const uint64_t ts = nowUs();
                append(ManifestRecordType::BLOB_DELETE, encodeBlobDelete(ts, blobId));

                std::lock_guard lock{mutex_};
                blobDeletes_.push_back(BlobDeleteEvent{blobId, ts});
                liveBlobs_.erase(blobId);
                deletedBlobs_.insert(blobId);
            }

            // ----------------------------------------------------------------
            // Replay
            // ----------------------------------------------------------------

            void replay() {
                std::lock_guard apiLock{apiMutex_};
                flushPendingForCompact();
                std::lock_guard rotationLock{rotationMutex_};
                replayInternal(false);
            }

            void compact() {
                std::lock_guard apiLock{apiMutex_};
                flushPendingForCompact();

                uint64_t stripes = 0;
                std::vector<SSTSealEvent> seals;
                std::vector<std::string> live;
                std::vector<std::string> deleted;
                std::optional<CheckpointEvent> checkpoint;
                std::vector<NodeJoinEvent> joins;
                std::vector<NodeLeaveEvent> leaves;
                std::optional<PrimaryLeaseEvent> lease;
                std::vector<BlobPutEvent> blobPuts;
                std::vector<uint64_t> liveBlobs;
                std::vector<uint64_t> deletedBlobs;
                std::vector<SSTBlobRefsEvent> sstBlobRefs;
                {
                    std::lock_guard lock{mutex_};
                    stripes = stripesWritten_.load(std::memory_order_relaxed);
                    seals = sstSeals_;
                    live.assign(liveSst_.begin(), liveSst_.end());
                    deleted.assign(deletedSst_.begin(), deletedSst_.end());
                    checkpoint = lastCheckpoint_;
                    joins = nodeJoins_;
                    leaves = nodeLeaves_;
                    lease = lastPrimaryLease_;
                    blobPuts = blobPuts_;
                    liveBlobs.assign(liveBlobs_.begin(), liveBlobs_.end());
                    deletedBlobs.assign(deletedBlobs_.begin(), deletedBlobs_.end());
                    for (const auto& file : liveSst_) {
                        if (const auto it = sstBlobRefsByFile_.find(file); it != sstBlobRefsByFile_.end()) {
                            sstBlobRefs.push_back(SSTBlobRefsEvent{file, it->second, nowUs()});
                        }
                    }
                }

                std::sort(live.begin(), live.end());
                std::sort(deleted.begin(), deleted.end());
                std::sort(liveBlobs.begin(), liveBlobs.end());
                std::sort(deletedBlobs.begin(), deletedBlobs.end());

                const auto tmpPath = path_.parent_path() / (path_.filename().string() + ".compact.tmp");
                std::filesystem::remove(tmpPath);
                FileHandle compactFile = FileHandle::open(tmpPath);
                uint64_t compactSize = 0;
                {
                    // The base snapshot covers every rotation through this number.
                    // A crash before obsolete-file cleanup must not replay them again.
                    const ManifestFileHeader fhdr = catalogFileHeader(rotationCounter_);
                    uint8_t buf[ManifestFileHeader::SIZE];
                    fhdr.serialize(buf);
                    compactFile.write(buf, ManifestFileHeader::SIZE);
                    compactSize += ManifestFileHeader::SIZE;
                }

                auto writeRecord = [&](ManifestRecordType type, std::vector<uint8_t> payload) {
                    auto record = buildRecord(type, std::move(payload));
                    compactFile.write(record.data(), record.size());
                    compactSize += record.size();
                };

                if (stripes > 0) { writeRecord(ManifestRecordType::STRIPE_COMMIT, encodeStripeCommit(nowUs(), stripes)); }
                for (const auto& seal : seals) {
                    if (!std::binary_search(live.begin(), live.end(), seal.file)) { continue; }
                    writeRecord(
                        ManifestRecordType::SST_SEAL,
                        encodeSstSeal(seal.tsUs, seal.level, seal.file, seal.entries, seal.firstKeyHex, seal.lastKeyHex)
                    );
                }
                if (!live.empty()) { writeRecord(ManifestRecordType::COMPACTION_COMMIT, encodeCompactionCommit(nowUs(), live, {})); }
                for (const auto& refs : sstBlobRefs) {
                    writeRecord(ManifestRecordType::SST_BLOB_REFS, encodeSstBlobRefs(refs.tsUs, refs.file, toFramingEntries(refs.entries)));
                }
                if (checkpoint.has_value()) {
                    writeRecord(
                        ManifestRecordType::CHECKPOINT,
                        encodeCheckpoint(checkpoint->tsUs, checkpoint->name, checkpoint->stripe, checkpoint->lastSeq)
                    );
                }
                for (const auto& file : deleted) { writeRecord(ManifestRecordType::SST_DELETE, encodeSstDelete(nowUs(), file)); }
                for (const auto& join : joins) {
                    writeRecord(ManifestRecordType::NODE_JOIN, encodeNodeJoin(join.tsUs, join.nodeId, join.replPort, join.host));
                }
                for (const auto& leave : leaves) { writeRecord(ManifestRecordType::NODE_LEAVE, encodeNodeLeave(leave.tsUs, leave.nodeId)); }
                if (lease.has_value()) {
                    writeRecord(ManifestRecordType::PRIMARY_LEASE, encodePrimaryLease(lease->tsUs, lease->nodeId, lease->leaseUntilUs));
                }
                for (const auto& put : blobPuts) {
                    if (std::binary_search(liveBlobs.begin(), liveBlobs.end(), put.blobId)) {
                        writeRecord(
                            ManifestRecordType::BLOB_PUT,
                            encodeBlobPut(put.tsUs, put.blobId, put.totalSize, put.storedSize, put.contentCrc32c, put.codec)
                        );
                    }
                }
                for (const auto blobId : deletedBlobs) { writeRecord(ManifestRecordType::BLOB_DELETE, encodeBlobDelete(nowUs(), blobId)); }

                compactFile.fsyncFull();
                compactFile.close();
                crashAtTestPoint("manifest.compact.after_tmp_sync");

                std::lock_guard rotationLock{rotationMutex_};
                fileHandle_.close();
                try { replaceFile(tmpPath, path_); }
                catch (...) {
                    fileHandle_ = FileHandle::open(currentPath_);
                    throw;
                }
                crashAtTestPoint("manifest.compact.after_replace");
                baseFileSeq_ = static_cast<uint32_t>(rotationCounter_);
                currentPath_ = path_;
                currentFileSize_ = compactSize;
                fileHandle_ = FileHandle::open(currentPath_);
                try { syncDirectory(path_.parent_path()); }
                catch (...) { termWriteError_ = std::current_exception(); throw; }
                crashAtTestPoint("manifest.compact.after_directory_sync");
                for (size_t i = 1; i <= baseFileSeq_; ++i) {
                    std::filesystem::remove(makeManifestPath(i));
                    std::filesystem::remove(makeLegacyManifestPath(i));
                    if (i == 1) { crashAtTestPoint("manifest.compact.after_first_rotation_delete"); }
                }
            }

            // ----------------------------------------------------------------
            // Queries
            // ----------------------------------------------------------------

            uint64_t stripesWritten() const noexcept { return stripesWritten_; }

            std::array<uint8_t, 16> catalogIdentity() const {
                std::lock_guard lock{mutex_};
                uint8_t header[ManifestFileHeader::SIZE];
                catalogHeader_.serialize(header);
                std::array<uint8_t, 16> identity;
                std::memcpy(identity.data(), header + 12, 8);
                std::memcpy(identity.data() + 8, header + 24, 8);
                return identity;
            }

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

            std::vector<uint64_t> liveBlobs() const {
                std::lock_guard lock{mutex_};
                return {liveBlobs_.begin(), liveBlobs_.end()};
            }

            std::vector<uint64_t> deletedBlobs() const {
                std::lock_guard lock{mutex_};
                return {deletedBlobs_.begin(), deletedBlobs_.end()};
            }

            std::vector<BlobPutEvent> blobPuts() const {
                std::lock_guard lock{mutex_};
                return blobPuts_;
            }

            std::vector<BlobDeleteEvent> blobDeletes() const {
                std::lock_guard lock{mutex_};
                return blobDeletes_;
            }

            std::vector<SSTBlobRefsEvent> sstBlobRefs() const {
                std::lock_guard lock{mutex_};
                return sstBlobRefs_;
            }

            std::optional<std::vector<SSTBlobRefsEvent::Entry>> liveSstBlobRefs() const {
                std::lock_guard lock{mutex_};
                std::vector<SSTBlobRefsEvent::Entry> refs;
                for (const auto& file : liveSst_) {
                    const auto found = sstBlobRefsByFile_.find(file);
                    if (found == sstBlobRefsByFile_.end()) { return std::nullopt; }
                    refs.insert(refs.end(), found->second.begin(), found->second.end());
                }
                return refs;
            }

            std::vector<uint64_t> sstReferencedBlobs() const {
                std::lock_guard lock{mutex_};
                struct LatestEntry {
                    uint64_t seq = 0;
                    std::optional<uint64_t> blobId;
                };
                std::unordered_map<std::string, LatestEntry> latestByKey;
                for (const auto& file : liveSst_) {
                    const auto it = sstBlobRefsByFile_.find(file);
                    if (it == sstBlobRefsByFile_.end()) { continue; }
                    for (const auto& entry : it->second) {
                        const std::string key{reinterpret_cast<const char*>(entry.key.data()), entry.key.size()};
                        auto& latest = latestByKey[key];
                        if (entry.seq >= latest.seq) { latest = LatestEntry{entry.seq, entry.blobId}; }
                    }
                }
                std::vector<uint64_t> out;
                for (const auto& [_, latest] : latestByKey) { if (latest.blobId.has_value()) { out.push_back(*latest.blobId); } }
                return sortedUniqueBlobIds(out);
            }

            bool sstBlobRefsComplete() const {
                std::lock_guard lock{mutex_};
                for (const auto& file : liveSst_) { if (sstBlobRefsByFile_.find(file) == sstBlobRefsByFile_.end()) { return false; } }
                return true;
            }

        private:
            static constexpr size_t ROTATION_THRESHOLD = 32 * 1024 * 1024; // 32 MiB

            static std::vector<uint64_t> sortedUniqueBlobIds(std::vector<uint64_t> blobIds) {
                std::sort(blobIds.begin(), blobIds.end());
                blobIds.erase(std::unique(blobIds.begin(), blobIds.end()), blobIds.end());
                return blobIds;
            }

            static std::vector<SSTBlobRefEntry> toFramingEntries(const std::vector<SSTBlobRefsEvent::Entry>& entries) {
                std::vector<SSTBlobRefEntry> out;
                out.reserve(entries.size());
                for (const auto& entry : entries) { out.push_back(SSTBlobRefEntry{entry.key, entry.seq, entry.flags, entry.blobId}); }
                return out;
            }

            static std::vector<SSTBlobRefsEvent::Entry> toManifestEntries(std::vector<SSTBlobRefEntry> entries) {
                std::vector<SSTBlobRefsEvent::Entry> out;
                out.reserve(entries.size());
                for (auto& entry : entries) {
                    out.push_back(SSTBlobRefsEvent::Entry{std::move(entry.key), entry.seq, entry.flags, entry.blobId});
                }
                return out;
            }

            static std::vector<SSTBlobRefsEvent::Entry> sortedUniqueEntries(std::vector<SSTBlobRefsEvent::Entry> entries) {
                std::sort(
                    entries.begin(),
                    entries.end(),
                    [](const auto& lhs, const auto& rhs) {
                        if (lhs.key != rhs.key) { return lhs.key < rhs.key; }
                        if (lhs.seq != rhs.seq) { return lhs.seq < rhs.seq; }
                        return lhs.blobId.value_or(0) < rhs.blobId.value_or(0);
                    }
                );
                entries.erase(
                    std::unique(
                        entries.begin(),
                        entries.end(),
                        [](const auto& lhs, const auto& rhs) {
                            return lhs.key == rhs.key && lhs.seq == rhs.seq && lhs.flags == rhs.flags && lhs.blobId == rhs.blobId;
                        }
                    ),
                    entries.end()
                );
                return entries;
            }

            // ----------------------------------------------------------------
            // Path helpers
            // ----------------------------------------------------------------

            [[nodiscard]] std::filesystem::path makeLegacyManifestPath(size_t rotationNumber) const {
                if (rotationNumber == 0) { return path_; }
                return path_.parent_path() / (path_.filename().string() + "." + std::to_string(rotationNumber));
            }

            [[nodiscard]] std::filesystem::path makeManifestPath(size_t rotationNumber) const {
                if (rotationNumber == 0) { return path_; }
                return path_.parent_path() / (path_.stem().string() + "-" + std::to_string(rotationNumber) + path_.extension().string());
            }

            [[nodiscard]] bool mayHaveManifestHistory() const {
                const auto parent = path_.has_parent_path() ? path_.parent_path() : std::filesystem::path{"."};
                if (!std::filesystem::exists(parent)) { return false; }

                const std::string baseName = path_.filename().string();
                const std::string stem = path_.stem().string();

                for (const auto& entry : std::filesystem::directory_iterator(parent)) {
                    const std::string name = entry.path().filename().string();
                    if (name == baseName) { return true; }
                    if (name.rfind(stem + "-", 0) == 0) { return true; }
                    if (name.rfind(baseName + ".", 0) == 0) { return true; }
                }
                return false;
            }

            [[nodiscard]] std::vector<std::filesystem::path> manifestFiles() const {
                std::map<uint32_t, std::filesystem::path> rotations;
                const auto parent = path_.has_parent_path() ? path_.parent_path() : std::filesystem::path{"."};
                const auto modernPrefix = path_.stem().string() + "-";
                const auto extension = path_.extension().string();
                const auto legacyPrefix = path_.filename().string() + ".";
                for (const auto& entry : std::filesystem::directory_iterator(parent)) {
                    const auto name = entry.path().filename().string();
                    std::string_view number;
                    bool modern = false;
                    if (name.starts_with(modernPrefix) && name.ends_with(extension) && name.size() > modernPrefix.size() + extension.size()) {
                        number = std::string_view{name}.substr(modernPrefix.size(), name.size() - modernPrefix.size() - extension.size());
                        modern = true;
                    }
                    else if (name.starts_with(legacyPrefix)) { number = std::string_view{name}.substr(legacyPrefix.size()); }
                    else { continue; }
                    uint32_t sequence = 0;
                    const auto parsed = std::from_chars(number.data(), number.data() + number.size(), sequence);
                    if (parsed.ec != std::errc{} || parsed.ptr != number.data() + number.size() || sequence <= baseFileSeq_) { continue; }
                    if (modern || !rotations.contains(sequence)) { rotations[sequence] = entry.path(); }
                }
                std::vector<std::filesystem::path> files{path_};
                uint64_t expected = static_cast<uint64_t>(baseFileSeq_) + 1;
                for (const auto& [sequence, file] : rotations) {
                    if (sequence != expected) { throw std::runtime_error("Manifest: missing rotation " + std::to_string(expected)); }
                    files.push_back(file);
                    ++expected;
                }
                return files;
            }

            // ----------------------------------------------------------------
            // File header
            // ----------------------------------------------------------------

            [[nodiscard]] ManifestFileHeader catalogFileHeader(size_t rotationCounter) const {
                std::lock_guard lock{mutex_};
                auto header = catalogHeader_;
                header.fileSeq = static_cast<uint32_t>(rotationCounter);
                header.crc32c = 0;
                uint8_t bytes[ManifestFileHeader::SIZE];
                header.serialize(bytes);
                header.crc32c = cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes), sizeof(bytes));
                return header;
            }

            void writeFileHeader(size_t rotationCounter) {
                const ManifestFileHeader fhdr = catalogFileHeader(rotationCounter);

                uint8_t buf[ManifestFileHeader::SIZE];
                fhdr.serialize(buf);
                fileHandle_.write(buf, ManifestFileHeader::SIZE);
                fileHandle_.fsyncData();
                syncDirectory(path_.parent_path());
                currentFileSize_ += ManifestFileHeader::SIZE;
            }

            // ----------------------------------------------------------------
            // Rotation
            // ----------------------------------------------------------------

            void checkRotation() {
                if (currentFileSize_ < ROTATION_THRESHOLD) { return; }
                if (rotationCounter_ == UINT32_MAX) { throw std::runtime_error("Manifest: rotation counter exhausted"); }

                // A durability barrier may span several files in one queued batch.
                // Sync the old file before losing its handle, not just the final file.
                fileHandle_.fsyncData();
                crashAtTestPoint("manifest.rotation.after_sync");
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

            [[nodiscard]] static std::vector<uint8_t> buildRecord(ManifestRecordType type, std::vector<uint8_t> payload) {
                if (payload.size() > UINT32_MAX) { throw std::length_error("Manifest: payload exceeds the v1 u32 limit"); }
                const auto plen = static_cast<uint32_t>(payload.size());
                const ManifestRecordHeader rhdr = ManifestRecordHeader::build(type, payload.data(), plen);
                payload.resize(ManifestRecordHeader::SIZE + plen);
                std::memmove(payload.data() + ManifestRecordHeader::SIZE, payload.data(), plen);
                rhdr.serialize(payload.data());
                return payload;
            }

            void append(ManifestRecordType type, std::vector<uint8_t> payload) {
                payload = buildRecord(type, std::move(payload));
                std::lock_guard lock{rotationMutex_};
                if (!ownershipLock_) { throw std::runtime_error("Manifest: already closed"); }
                if (termWriteError_) { std::rethrow_exception(termWriteError_); }
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
                    try {
                        checkRotation();
                        fileHandle_.write(payload.data(), payload.size());
                        fileHandle_.fsyncData();
                        currentFileSize_ += payload.size();
                    }
                    catch (...) {
                        // A failed write/sync can still have persisted the event.
                        // Only reopening and replaying can resolve that outcome.
                        termWriteError_ = std::current_exception();
                        throw;
                    }
                }
            }

            // All dequeue/write operations use rotationMutex_ before queueMutex_.
            // This keeps an older background batch ahead of a synchronous barrier.
            void writeQueuedRecords(bool forceSync = true) {
                if (termWriteError_) { std::rethrow_exception(termWriteError_); }
                std::vector<std::vector<uint8_t>> batch;
                {
                    std::lock_guard lock{queueMutex_};
                    std::swap(batch, queue_);
                }
                if (batch.empty() && !forceSync) { return; }
                try {
                    for (const auto& record : batch) {
                        checkRotation();
                        fileHandle_.write(record.data(), record.size());
                        currentFileSize_ += record.size();
                    }
                    fileHandle_.fsyncData();
                }
                catch (...) {
                    termWriteError_ = std::current_exception();
                    throw;
                }
            }

            void flushPendingForCompact() {
                std::lock_guard rotationLock{rotationMutex_};
                if (!ownershipLock_) { throw std::runtime_error("Manifest: already closed"); }
                if (termWriteError_) { std::rethrow_exception(termWriteError_); }
                if (fastMode_) { writeQueuedRecords(); }
            }

            // Background batching is used for events without an SST/WAL durability handoff.
            void runFlusher() {
                constexpr auto MAX_WAIT = std::chrono::microseconds(500);
                try {
                    while (true) {
                        {
                            std::unique_lock lock{queueMutex_};
                            queueCv_.wait_for(lock, MAX_WAIT, [this] { return !queue_.empty() || !running_; });
                            if (!running_ && queue_.empty()) { break; }
                        }
                        std::lock_guard rotationLock{rotationMutex_};
                        writeQueuedRecords(false);
                        const auto now = std::chrono::steady_clock::now();
                        if (now - lastStrongSync_ > std::chrono::seconds(5)) {
                            fileHandle_.fsyncFull();
                            lastStrongSync_ = now;
                        }
                    }
                }
                catch (...) {
                    std::lock_guard rotationLock{rotationMutex_};
                    if (!termWriteError_) { termWriteError_ = std::current_exception(); }
                }
            }

            // ----------------------------------------------------------------
            // Replay
            // ----------------------------------------------------------------

            void replayInternal(bool repairTail) {
                {
                    std::lock_guard lock{mutex_};
                    sstSeals_.clear();
                    liveSst_.clear();
                    deletedSst_.clear();
                    lastCheckpoint_.reset();
                    nodeJoins_.clear();
                    nodeLeaves_.clear();
                    lastPrimaryLease_.reset();
                    blobPuts_.clear();
                    blobDeletes_.clear();
                    liveBlobs_.clear();
                    deletedBlobs_.clear();
                    sstBlobRefs_.clear();
                    sstBlobRefsByFile_.clear();
                }
                stripesWritten_.store(0, std::memory_order_relaxed);

                const bool baseExists = pathExists(path_);
                if (!baseExists) { return; }
                const auto header = readFileHeader(path_);
                baseFileSeq_ = header.fileSeq;
                { std::lock_guard lock{mutex_}; catalogHeader_ = header; }

                const auto files = manifestFiles();

                for (size_t i = 0; i < files.size(); ++i) {
                    replaySingleFile(files[i], static_cast<uint32_t>(baseFileSeq_ + i), repairTail && i + 1 == files.size());
                }
            }

            void replaySingleFile(const std::filesystem::path& filePath, uint32_t expectedFileSeq, bool repairTail) {
                const auto file_size = pathFileSize(filePath);
                const auto header = readFileHeader(filePath);
                if (header.fileSeq != expectedFileSeq) { throw std::runtime_error("Manifest: inconsistent rotation sequence"); }

                std::ifstream file(filePath, std::ios::binary);
                if (!file) { throw std::runtime_error("Failed to open manifest for replay: " + filePath.string()); }
                file.seekg(ManifestFileHeader::SIZE);

                uint64_t validEnd = ManifestFileHeader::SIZE;
                // --- Read records ---
                std::vector<uint8_t> payload; // reused across records - avoids per-record heap alloc
                while (validEnd < file_size) {
                    if (file_size - validEnd < ManifestRecordHeader::SIZE) { break; }
                    uint8_t rhdrBuf[ManifestRecordHeader::SIZE];
                    file.read(reinterpret_cast<char*>(rhdrBuf), ManifestRecordHeader::SIZE);
                    if (!file) { throw std::runtime_error("Manifest: record header read failed"); }

                    const ManifestRecordHeader rhdr = ManifestRecordHeader::deserialize(rhdrBuf);
                    if (!rhdr.verifyChecksum()) { throw std::runtime_error("Manifest: corrupt record header"); }
                    const auto position = validEnd + ManifestRecordHeader::SIZE;
                    if (rhdr.payloadLen > file_size - position) { break; }

                    if (rhdr.payloadLen > 0) {
                        payload.resize(rhdr.payloadLen);
                        file.read(reinterpret_cast<char*>(payload.data()), rhdr.payloadLen);
                        if (!file) { throw std::runtime_error("Manifest: record payload read failed"); }

                        if (!rhdr.verifyPayload(payload.data(), rhdr.payloadLen)) {
                            throw std::runtime_error("Manifest: corrupt record payload");
                        }

                        applyEvent(static_cast<ManifestRecordType>(rhdr.type), payload.data(), rhdr.payloadLen);
                    }
                    else { throw std::runtime_error("Manifest: empty v1 event payload"); }
                    validEnd = position + rhdr.payloadLen;
                }
                if (validEnd != file_size) {
                    if (!repairTail) { throw std::runtime_error("Manifest: damaged record before the append boundary"); }
                    // Only a physically incomplete final record is repairable. A full
                    // record with a bad CRC is corruption, even at the end of the log.
                    file.close();
                    std::filesystem::resize_file(filePath, validEnd);
                    auto repaired = FileHandle::open(filePath);
                    repaired.fsyncFull();
                }
            }

            void applyEvent(ManifestRecordType type, const uint8_t* payload, uint32_t len) {
                switch (type) {
                    case ManifestRecordType::STRIPE_COMMIT: {
                        DecodedStripeCommit d;
                        if (!decodeStripeCommit(payload, len, d)) { throw std::runtime_error("Manifest: invalid v1 event payload"); }
                        if (d.stripeCount >= stripesWritten_) { stripesWritten_ = d.stripeCount; }
                        break;
                    }
                    case ManifestRecordType::SST_SEAL: {
                        DecodedSSTSeal d;
                        if (!decodeSstSeal(payload, len, d)) { throw std::runtime_error("Manifest: invalid v1 event payload"); }
                        std::lock_guard lock{mutex_};
                        sstSeals_.push_back(SSTSealEvent{d.level, d.name, d.entries, d.firstKeyHex, d.lastKeyHex, d.tsUs});
                        liveSst_.insert(d.name);
                        deletedSst_.erase(d.name);
                        break;
                    }
                    case ManifestRecordType::SST_DELETE: {
                        DecodedSSTDelete d;
                        if (!decodeSstDelete(payload, len, d)) { throw std::runtime_error("Manifest: invalid v1 event payload"); }
                        std::lock_guard lock{mutex_};
                        liveSst_.erase(d.name);
                        deletedSst_.insert(d.name);
                        sstBlobRefsByFile_.erase(d.name);
                        break;
                    }
                    case ManifestRecordType::COMPACTION_END: {
                        DecodedCompactionEnd d;
                        if (!decodeCompactionEnd(payload, len, d)) { throw std::runtime_error("Manifest: invalid v1 event payload"); }
                        std::lock_guard lock{mutex_};
                        liveSst_.insert(d.output);
                        for (const auto& inp : d.inputs) {
                            liveSst_.erase(inp);
                            deletedSst_.insert(inp);
                            sstBlobRefsByFile_.erase(inp);
                        }
                        break;
                    }
                    case ManifestRecordType::CHECKPOINT: {
                        DecodedCheckpoint d;
                        if (!decodeCheckpoint(payload, len, d)) { throw std::runtime_error("Manifest: invalid v1 event payload"); }
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
                        if (!decodeCompactionCommit(payload, len, d)) { throw std::runtime_error("Manifest: invalid v1 event payload"); }
                        std::lock_guard lock{mutex_};
                        for (const auto& f : d.outputFiles) {
                            liveSst_.insert(f);
                            deletedSst_.erase(f);
                        }
                        for (const auto& f : d.inputFiles) {
                            liveSst_.erase(f);
                            deletedSst_.insert(f);
                            sstBlobRefsByFile_.erase(f);
                        }
                        break;
                    }
                    case ManifestRecordType::SST_BLOB_REFS: {
                        DecodedSSTBlobRefs d;
                        if (!decodeSstBlobRefs(payload, len, d)) { throw std::runtime_error("Manifest: invalid v1 event payload"); }
                        std::lock_guard lock{mutex_};
                        auto entries = sortedUniqueEntries(toManifestEntries(std::move(d.entries)));
                        sstBlobRefs_.push_back(SSTBlobRefsEvent{d.name, entries, d.tsUs});
                        sstBlobRefsByFile_[d.name] = std::move(entries);
                        break;
                    }
                    case ManifestRecordType::NODE_JOIN: {
                        DecodedNodeJoin d;
                        if (!decodeNodeJoin(payload, len, d)) { throw std::runtime_error("Manifest: invalid v1 event payload"); }
                        std::lock_guard lock{mutex_};
                        nodeJoins_.push_back(NodeJoinEvent{d.nodeId, d.replPort, d.host, d.tsUs});
                        break;
                    }
                    case ManifestRecordType::NODE_LEAVE: {
                        DecodedNodeLeave d;
                        if (!decodeNodeLeave(payload, len, d)) { throw std::runtime_error("Manifest: invalid v1 event payload"); }
                        std::lock_guard lock{mutex_};
                        nodeLeaves_.push_back(NodeLeaveEvent{d.nodeId, d.tsUs});
                        break;
                    }
                    case ManifestRecordType::PRIMARY_LEASE: {
                        DecodedPrimaryLease d;
                        if (!decodePrimaryLease(payload, len, d)) { throw std::runtime_error("Manifest: invalid v1 event payload"); }
                        std::lock_guard lock{mutex_};
                        lastPrimaryLease_ = PrimaryLeaseEvent{d.nodeId, d.leaseUntilUs, d.tsUs};
                        break;
                    }
                    case ManifestRecordType::BLOB_PUT: {
                        DecodedBlobPut d;
                        if (!decodeBlobPut(payload, len, d)) { throw std::runtime_error("Manifest: invalid v1 event payload"); }
                        std::lock_guard lock{mutex_};
                        blobPuts_.push_back(BlobPutEvent{d.blobId, d.totalSize, d.storedSize, d.contentCrc32c, d.codec, d.tsUs});
                        liveBlobs_.insert(d.blobId);
                        deletedBlobs_.erase(d.blobId);
                        break;
                    }
                    case ManifestRecordType::BLOB_DELETE: {
                        DecodedBlobDelete d;
                        if (!decodeBlobDelete(payload, len, d)) { throw std::runtime_error("Manifest: invalid v1 event payload"); }
                        std::lock_guard lock{mutex_};
                        blobDeletes_.push_back(BlobDeleteEvent{d.blobId, d.tsUs});
                        liveBlobs_.erase(d.blobId);
                        deletedBlobs_.insert(d.blobId);
                        break;
                    }
                    case ManifestRecordType::COMPACTION_START: {
                        DecodedCompactionStart d;
                        if (!decodeCompactionStart(payload, len, d)) { throw std::runtime_error("Manifest: invalid v1 event payload"); }
                        break;
                    }
                    case ManifestRecordType::TRUNCATE: {
                        DecodedTruncate d;
                        if (!decodeTruncate(payload, len, d)) { throw std::runtime_error("Manifest: invalid v1 event payload"); }
                        break;
                    }
                    default: throw std::runtime_error("Manifest: unknown v1 event type");
                }
            }

            // ----------------------------------------------------------------
            // Members
            // ----------------------------------------------------------------

            std::filesystem::path path_;
            std::unique_ptr<detail::StorageFileLock> ownershipLock_;
            ManifestFileHeader catalogHeader_{};
            bool fastMode_;
            std::atomic<bool> running_;
            FileHandle fileHandle_;
            std::mutex apiMutex_;

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
            std::vector<BlobPutEvent> blobPuts_;
            std::vector<BlobDeleteEvent> blobDeletes_;
            std::unordered_set<uint64_t> liveBlobs_;
            std::unordered_set<uint64_t> deletedBlobs_;
            std::vector<SSTBlobRefsEvent> sstBlobRefs_;
            std::unordered_map<std::string, std::vector<SSTBlobRefsEvent::Entry>> sstBlobRefsByFile_;

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
            uint32_t baseFileSeq_ = 0;
    };

    // ============================================================================
    // Manifest public API - thin forwarding layer
    // ============================================================================

    std::unique_ptr<Manifest> Manifest::create(const std::filesystem::path& path, bool fastMode) {
        return std::unique_ptr<Manifest>(new Manifest(path, fastMode));
    }

    Manifest::Manifest(const std::filesystem::path& path, bool fastMode) : impl_{std::make_unique<Impl>(path, fastMode)} {}

    Manifest::~Manifest() = default;

    void Manifest::start() { impl_->start(); }

    std::array<uint8_t, 16> Manifest::catalogIdentity() const { return impl_->catalogIdentity(); }

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

    void Manifest::sstBlobRefs(const std::string& file, const std::vector<SSTBlobRefsEvent::Entry>& entries) {
        impl_->sstBlobRefs(file, entries);
    }

    void Manifest::truncate(const std::optional<std::string>& reason) { impl_->truncate(reason); }

    void Manifest::nodeJoin(uint64_t nodeId, uint16_t replPort, const std::string& host) { impl_->nodeJoin(nodeId, replPort, host); }

    void Manifest::nodeLeave(uint64_t nodeId) { impl_->nodeLeave(nodeId); }

    void Manifest::primaryLease(uint64_t nodeId, uint64_t leaseUntilUs) { impl_->primaryLease(nodeId, leaseUntilUs); }

    void Manifest::blobPut(uint64_t blobId, uint64_t totalSize, uint64_t storedSize, uint32_t contentCrc32c, uint32_t codec) {
        impl_->blobPut(blobId, totalSize, storedSize, contentCrc32c, codec);
    }

    void Manifest::blobDelete(uint64_t blobId) { impl_->blobDelete(blobId); }

    void Manifest::replay() { impl_->replay(); }
    void Manifest::compact() { impl_->compact(); }

    uint64_t Manifest::stripesWritten() const noexcept { return impl_->stripesWritten(); }

    std::optional<Manifest::CheckpointEvent> Manifest::lastCheckpoint() const noexcept { return impl_->lastCheckpoint(); }

    std::vector<std::string> Manifest::liveSst() const { return impl_->liveSst(); }
    std::vector<std::string> Manifest::deletedSst() const { return impl_->deletedSst(); }
    std::vector<Manifest::SSTSealEvent> Manifest::sstSeals() const { return impl_->sstSeals(); }
    std::vector<Manifest::NodeJoinEvent> Manifest::nodeJoins() const { return impl_->nodeJoins(); }
    std::vector<Manifest::NodeLeaveEvent> Manifest::nodeLeaves() const { return impl_->nodeLeaves(); }
    std::optional<Manifest::PrimaryLeaseEvent> Manifest::lastPrimaryLease() const noexcept { return impl_->lastPrimaryLease(); }
    std::vector<uint64_t> Manifest::liveBlobs() const { return impl_->liveBlobs(); }
    std::vector<uint64_t> Manifest::deletedBlobs() const { return impl_->deletedBlobs(); }
    std::vector<Manifest::BlobPutEvent> Manifest::blobPuts() const { return impl_->blobPuts(); }
    std::vector<Manifest::BlobDeleteEvent> Manifest::blobDeletes() const { return impl_->blobDeletes(); }
    std::vector<Manifest::SSTBlobRefsEvent> Manifest::sstBlobRefs() const { return impl_->sstBlobRefs(); }

    std::optional<std::vector<Manifest::SSTBlobRefsEvent::Entry>> Manifest::liveSstBlobRefs() const { return impl_->liveSstBlobRefs(); }
    std::vector<uint64_t> Manifest::sstReferencedBlobs() const { return impl_->sstReferencedBlobs(); }
    bool Manifest::sstBlobRefsComplete() const { return impl_->sstBlobRefsComplete(); }

    void Manifest::close() { impl_->close(); }
} // namespace akkaradb::engine::manifest

/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/AkkEngine.cpp
#include <akk/engine/cluster/detail/ReconfigurationDeadline.hpp>
#include "akk/engine/AkkEngine.hpp"
#include "akk/engine/detail/ProtocolBulkWriter.hpp"
#include "akk/crypto/Random.hpp"
#include "akk/engine/cluster/detail/ClusterPlacement.hpp"
#include "akk/engine/cluster/detail/KeyedMutex.hpp"
#include "akk/engine/cluster/detail/BoundedExecutor.hpp"
#include "akk/engine/cluster/detail/EngineSnapshot.hpp"
#include "akk/cpu/CRC32C.hpp"

#include "akk/core/record/KeyFingerprint.hpp"
#include "akk/core/record/MemHdr16.hpp"
#include "akk/engine/blob/BlobFraming.hpp"
#include "akk/engine/cluster/ClusterRuntimeProvider.hpp"
#include "akk/engine/erasure/ErasureCodec.hpp"
#include "akk/engine/generation/StorageGeneration.hpp"
#include "akk/engine/manifest/Manifest.hpp"
#include "akk/engine/server/AkkApiServerProvider.hpp"
#include "akk/engine/wal/WalFraming.hpp"
#include "akk/engine/wal/WalRecovery.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <functional>
#include <fstream>
#include <limits>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include "detail/ClusterQuerySpool.hpp"

#ifdef _WIN32
#include <Windows.h>
#include <io.h>
#else
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace akkaradb::engine {
    namespace fs = std::filesystem;

    namespace {
        [[nodiscard]] uint64_t nowNs() noexcept {
            timespec ts{};
            timespec_get(&ts, TIME_UTC);
            return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ULL + static_cast<uint64_t>(ts.tv_nsec);
        }

        void validateRollbackOptions(RollbackOptions options) {
            if (options.execution > RollbackExecutionMode::NEXT_STARTUP) {
                throw std::invalid_argument("AkkEngine: invalid rollback execution mode");
            }
            if (options.conflict > RollbackConflictPolicy::OVERWRITE_LATEST) {
                throw std::invalid_argument("AkkEngine: invalid rollback conflict policy");
            }
        }

        void writeFileAtomicallyDurable(const fs::path& path, std::span<const uint8_t> bytes) {
            if (!path.parent_path().empty()) { fs::create_directories(path.parent_path()); }
            fs::path temporary = path;
            temporary += ".tmp";
            {
                std::ofstream out{temporary, std::ios::binary | std::ios::trunc};
                if (!out || !out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()))) {
                    throw std::runtime_error("AkkEngine: failed to write durable journal");
                }
                out.flush();
                if (!out) { throw std::runtime_error("AkkEngine: failed to flush durable journal"); }
            }
#ifdef _WIN32
            const HANDLE file = ::CreateFileW(
                temporary.c_str(), GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ, nullptr, OPEN_EXISTING,
                FILE_ATTRIBUTE_NORMAL | FILE_FLAG_WRITE_THROUGH, nullptr
            );
            if (file == INVALID_HANDLE_VALUE || !::FlushFileBuffers(file)) {
                if (file != INVALID_HANDLE_VALUE) { ::CloseHandle(file); }
                throw std::runtime_error("AkkEngine: failed to sync durable journal");
            }
            ::CloseHandle(file);
            if (!::MoveFileExW(temporary.c_str(), path.c_str(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
                throw std::runtime_error("AkkEngine: failed to publish durable journal");
            }
#else
            const int fd = ::open(temporary.c_str(), O_RDONLY);
            if (fd < 0 || ::fsync(fd) != 0) {
                if (fd >= 0) { ::close(fd); }
                throw std::runtime_error("AkkEngine: failed to sync durable journal");
            }
            ::close(fd);
            fs::rename(temporary, path);
            const fs::path parentPath = path.parent_path().empty() ? fs::path{"."} : path.parent_path();
            const int parent = ::open(parentPath.c_str(), O_RDONLY | O_DIRECTORY);
            if (parent < 0 || ::fsync(parent) != 0) {
                if (parent >= 0) { ::close(parent); }
                throw std::runtime_error("AkkEngine: failed to sync durable journal directory");
            }
            ::close(parent);
#endif
        }

        using cluster::detail::rendezvousScore;

        // Test-only fault injection used by the recovery smoke test. The process
        // terminates without unwinding, matching the storage guarantees required
        // at each persistence boundary.
        void crashAtTestPoint(const char* point) noexcept {
            const char* requested = std::getenv("AKKARADB_TEST_CRASH_POINT");
            if (requested != nullptr && std::strcmp(requested, point) == 0) { std::quick_exit(86); }
        }

        void corruptSnapshotStagingAtTestPoint(const fs::path& path, const char* point) {
            const char* requested = std::getenv("AKKARADB_TEST_CORRUPT_SNAPSHOT_STAGING_POINT");
            if (requested == nullptr || std::strcmp(requested, point) != 0) { return; }
            #ifdef _WIN32
            (void)_putenv_s("AKKARADB_TEST_CORRUPT_SNAPSHOT_STAGING_POINT", "");
            #else
            (void)::unsetenv("AKKARADB_TEST_CORRUPT_SNAPSHOT_STAGING_POINT");
            #endif

            std::fstream file{path, std::ios::binary | std::ios::in | std::ios::out};
            if (!file) { throw std::runtime_error("AkkEngine: failed to open replication snapshot staging file for test corruption"); }

            constexpr std::streamoff firstRecordHeaderOffset = static_cast<std::streamoff>(5 + sizeof(uint64_t) + sizeof(uint64_t));
            file.seekg(firstRecordHeaderOffset);
            std::array<uint8_t, sizeof(uint32_t)> keyLenBytes{};
            std::array<uint8_t, sizeof(uint64_t)> valueLenBytes{};
            file.read(reinterpret_cast<char*>(keyLenBytes.data()), static_cast<std::streamsize>(keyLenBytes.size()));
            file.read(reinterpret_cast<char*>(valueLenBytes.data()), static_cast<std::streamsize>(valueLenBytes.size()));
            if (!file) { throw std::runtime_error("AkkEngine: failed to read replication snapshot staging header for test corruption"); }
            uint32_t keyLen = 0;
            for (size_t i = 0; i < keyLenBytes.size(); ++i) { keyLen |= static_cast<uint32_t>(keyLenBytes[i]) << (i * 8); }
            uint64_t valueLen = 0;
            for (size_t i = 0; i < valueLenBytes.size(); ++i) { valueLen |= static_cast<uint64_t>(valueLenBytes[i]) << (i * 8); }
            if (valueLen == 0) {
                throw std::runtime_error("AkkEngine: replication snapshot staging value is empty at test corruption point");
            }

            const std::streamoff firstValueByteOffset = firstRecordHeaderOffset + static_cast<std::streamoff>(sizeof(uint32_t) + sizeof(
                uint64_t) + sizeof(uint32_t) + keyLen);
            file.seekg(firstValueByteOffset);
            char byte = 0;
            file.get(byte);
            if (!file) { throw std::runtime_error("AkkEngine: failed to read replication snapshot staging byte for test corruption"); }
            byte = static_cast<char>(static_cast<unsigned char>(byte) ^ 0x5Au);
            file.clear();
            file.seekp(firstValueByteOffset);
            file.put(byte);
            file.flush();
            if (!file) { throw std::runtime_error("AkkEngine: failed to corrupt replication snapshot staging byte"); }

            std::ofstream marker{path.string() + ".corrupted", std::ios::binary | std::ios::trunc};
            marker << "1";
            if (!marker) { throw std::runtime_error("AkkEngine: failed to write replication snapshot staging corruption marker"); }
        }

        [[nodiscard]] uint32_t nextPow2(uint32_t value) noexcept {
            uint32_t out = 1;
            while (out < value) { out <<= 1; }
            return out;
        }

        static constexpr uint64_t BLOB_ID_NAMESPACE_BITS = 2;
        static constexpr uint64_t BLOB_ID_PAYLOAD_BITS = 64 - BLOB_ID_NAMESPACE_BITS;
        static constexpr uint64_t BLOB_ID_NAMESPACE_RAFT_LOG = 1ULL << BLOB_ID_PAYLOAD_BITS;
        static constexpr uint64_t BLOB_ID_NAMESPACE_SNAPSHOT = 2ULL << BLOB_ID_PAYLOAD_BITS;
        static constexpr uint64_t LOCAL_BLOB_ID_LIMIT = 1ULL << BLOB_ID_PAYLOAD_BITS;
        static constexpr size_t SNAPSHOT_STAGING_VALUE_CHUNK_BYTES = 1024ULL * 1024ULL;

        [[nodiscard]] uint64_t raftLogBlobId(uint64_t nodeId, uint64_t seq) {
            constexpr uint64_t SEQ_BITS = 46;
            constexpr uint64_t SEQ_MASK = (1ULL << SEQ_BITS) - 1ULL;
            constexpr uint64_t MAX_NODE_ID = (1ULL << (BLOB_ID_PAYLOAD_BITS - SEQ_BITS)) - 1ULL;
            if (nodeId == 0 || nodeId > MAX_NODE_ID) {
                throw std::invalid_argument("AkkEngine: RAFT_LOG Blob nodeId is outside encodable range");
            }
            if (seq == 0 || seq > SEQ_MASK) { throw std::overflow_error("AkkEngine: RAFT_LOG Blob sequence is outside encodable range"); }
            return BLOB_ID_NAMESPACE_RAFT_LOG | (nodeId << SEQ_BITS) | seq;
        }

        [[nodiscard]] uint64_t snapshotBlobId(uint64_t seq, uint64_t ordinal) {
            constexpr uint64_t ORDINAL_BITS = 22;
            constexpr uint64_t ORDINAL_MASK = (1ULL << ORDINAL_BITS) - 1ULL;
            constexpr uint64_t SEQ_MASK = (1ULL << (BLOB_ID_PAYLOAD_BITS - ORDINAL_BITS)) - 1ULL;
            if (seq == 0 || seq > SEQ_MASK) { throw std::overflow_error("AkkEngine: snapshot Blob sequence is outside encodable range"); }
            if (ordinal == 0 || ordinal > ORDINAL_MASK) {
                throw std::overflow_error("AkkEngine: snapshot Blob ordinal is outside encodable range");
            }
            return BLOB_ID_NAMESPACE_SNAPSHOT | (seq << ORDINAL_BITS) | ordinal;
        }

        [[nodiscard]] uint32_t shardsForThreads(uint32_t writers, uint32_t cap) noexcept {
            if (writers <= 1) { return 1; }
            const uint32_t target = std::max(writers * 4u, 2u);
            return std::min(nextPow2(target), cap);
        }

        [[nodiscard]] bool supportsParallelWriteAdmission(const AkkEngineOptions& options) noexcept {
            return !options.components.blobEnabled && !options.components.clusterEnabled;
        }

        static constexpr size_t COMMIT_COMPLETION_RING_SIZE = 1u << 16u;

        [[nodiscard]] uint32_t resolveCommitWindowSize(uint32_t requested) noexcept {
            if (requested == 0) { return static_cast<uint32_t>(COMMIT_COMPLETION_RING_SIZE); }
            return nextPow2(std::clamp<uint32_t>(requested, 64u, 1u << 24u));
        }

        [[nodiscard]] wal::WalExecutionMode resolveWalExecutionMode(const wal::WalOptions& options) noexcept {
            if (options.execution != wal::WalExecutionMode::AUTO) { return options.execution; }
            return options.syncMode == wal::WalSyncMode::ASYNC ? wal::WalExecutionMode::ASYNC : wal::WalExecutionMode::INLINE;
        }

        [[nodiscard]] wal::WalSyncPolicy resolveWalSyncPolicy(const wal::WalOptions& options) noexcept {
            if (options.syncPolicy != wal::WalSyncPolicy::AUTO) { return options.syncPolicy; }
            switch (options.syncMode) {
                case wal::WalSyncMode::OFF: return wal::WalSyncPolicy::NEVER;
                case wal::WalSyncMode::ASYNC: return wal::WalSyncPolicy::ON_SYNC_ACK;
                case wal::WalSyncMode::SYNC: return wal::WalSyncPolicy::ALWAYS;
            }
            return wal::WalSyncPolicy::ALWAYS;
        }

        void normalizeWalOptions(wal::WalOptions& options) noexcept {
            options.execution = resolveWalExecutionMode(options);
            options.syncPolicy = resolveWalSyncPolicy(options);
            if (options.syncPolicy == wal::WalSyncPolicy::NEVER && options.syncMode == wal::WalSyncMode::SYNC) {
                options.syncMode = wal::WalSyncMode::OFF;
            }
            else if (options.execution == wal::WalExecutionMode::ASYNC) { options.syncMode = wal::WalSyncMode::ASYNC; }
            else if (options.syncPolicy == wal::WalSyncPolicy::NEVER) { options.syncMode = wal::WalSyncMode::OFF; }
            else { options.syncMode = wal::WalSyncMode::SYNC; }
        }

        void normalizeMemTableOptions(memtable::MemTable::Options& options) noexcept {
            if (options.flushMode == memtable::MemTableFlushMode::AUTO) {
                options.flushMode = options.thresholdBytesPerShard > 0
                                        ? memtable::MemTableFlushMode::BYTES_PER_SHARD
                                        : memtable::MemTableFlushMode::MANUAL_ONLY;
            }
        }

        void normalizeSstOptions(sst::SSTManager::Options& options) noexcept {
            if (options.compactionMode == sst::SSTCompactionMode::AUTO) {
                options.compactionMode = options.compactThreads == 0
                                             ? sst::SSTCompactionMode::DISABLED
                                             : sst::SSTCompactionMode::BACKGROUND;
            }
        }

        [[nodiscard]] AkkEngineOptions::WriteVisibilityMode resolveReadVisibility(const AkkEngineOptions& options) noexcept {
            switch (options.runtime.visibility.readVisibility) {
                case AkkEngineOptions::ReadVisibilityMode::AUTO: return options.runtime.writeVisibility;
                case AkkEngineOptions::ReadVisibilityMode::COMMIT_ORDER: return AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER;
                case AkkEngineOptions::ReadVisibilityMode::APPLIED: return AkkEngineOptions::WriteVisibilityMode::APPLIED;
            }
            return options.runtime.writeVisibility;
        }

        void normalizeVisibilityOptions(AkkEngineOptions& options) noexcept {
            const auto effective = resolveReadVisibility(options);
            options.runtime.writeVisibility = effective;
            options.runtime.visibility.readVisibility = effective == AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER
                                                            ? AkkEngineOptions::ReadVisibilityMode::COMMIT_ORDER
                                                            : AkkEngineOptions::ReadVisibilityMode::APPLIED;
        }

        void resolveWritePolicy(AkkEngineOptions& options) {
            switch (options.runtime.writePolicy) {
                case AkkEngineOptions::WritePolicyPreset::CUSTOM: break;
                case AkkEngineOptions::WritePolicyPreset::SAFE: options.runtime.writeDurability =
                        AkkEngineOptions::WriteDurabilityMode::SYNCED;
                    options.runtime.writeVisibility = AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER;
                    break;
                case AkkEngineOptions::WritePolicyPreset::BALANCED: options.runtime.writeDurability =
                        AkkEngineOptions::WriteDurabilityMode::WRITTEN;
                    options.runtime.writeVisibility = AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER;
                    break;
                case AkkEngineOptions::WritePolicyPreset::FAST: options.runtime.writeDurability = options.components.walEnabled
                        ? AkkEngineOptions::WriteDurabilityMode::ENQUEUED
                        : AkkEngineOptions::WriteDurabilityMode::MEMORY;
                    options.runtime.writeVisibility = AkkEngineOptions::WriteVisibilityMode::APPLIED;
                    break;
            }

            if (options.runtime.writeDurability == AkkEngineOptions::WriteDurabilityMode::MEMORY && options.components.walEnabled) {
                throw std::invalid_argument("AkkEngine: writeDurability=MEMORY requires WAL to be disabled");
            }
        }

        void validateRuntimeOptions(const AkkEngineOptions& options) {
            if (options.runtime.sequence.allocation == AkkEngineOptions::SequenceAllocationMode::THREAD_LOCAL_RANGES && options.runtime.
                writeVisibility == AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER) {
                throw std::invalid_argument("AkkEngine: THREAD_LOCAL_RANGES sequence allocation requires readVisibility=APPLIED");
            }
            if (options.runtime.sequence.allocation == AkkEngineOptions::SequenceAllocationMode::THREAD_LOCAL_RANGES && (options.components.
                clusterEnabled || options.components.versionLogEnabled)) {
                throw std::invalid_argument(
                    "AkkEngine: THREAD_LOCAL_RANGES sequence allocation requires cluster and version log to be disabled"
                );
            }
            if (options.runtime.sequence.allocation == AkkEngineOptions::SequenceAllocationMode::THREAD_LOCAL_RANGES && options.runtime.
                sequence.threadLocalRangeSize <= 1) {
                throw std::invalid_argument("AkkEngine: THREAD_LOCAL_RANGES requires sequence.threadLocalRangeSize > 1");
            }
            if (options.runtime.parallelWriteOrder == AkkEngineOptions::ParallelWriteOrderMode::KEY_SEQUENCE && options.runtime.sequence.
                allocation != AkkEngineOptions::SequenceAllocationMode::GLOBAL_ATOMIC) {
                throw std::invalid_argument("AkkEngine: parallelWriteOrder=KEY_SEQUENCE requires sequence.allocation=GLOBAL_ATOMIC");
            }
            if (options.runtime.parallelWriteOrder == AkkEngineOptions::ParallelWriteOrderMode::KEY_SEQUENCE && options.runtime.
                writeVisibility != AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER) {
                throw std::invalid_argument("AkkEngine: parallelWriteOrder=KEY_SEQUENCE requires readVisibility=COMMIT_ORDER");
            }
            if (options.components.sstEnabled && options.runtime.backpressure.maxSstL0Files > 0 && options.runtime.backpressure.
                sstCompactionBacklog == AkkEngineOptions::BackpressureMode::BLOCK && options.sst.compactionMode ==
                sst::SSTCompactionMode::DISABLED) {
                throw std::invalid_argument("AkkEngine: blocking SST backpressure requires background compaction or fail-fast mode");
            }
        }

        void ensureDir(const fs::path& path) { if (!path.empty()) { fs::create_directories(path); } }

        void removeFileIfExists(const fs::path& path) noexcept {
            if (path.empty()) { return; }
            std::error_code ec;
            fs::remove(path, ec);
        }

        void writeU32Le(std::ostream& out, uint32_t value) {
            std::array<uint8_t, 4> bytes{};
            for (size_t i = 0; i < bytes.size(); ++i) { bytes[i] = static_cast<uint8_t>(value >> (i * 8)); }
            out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
            if (!out) { throw std::runtime_error("AkkEngine: failed to write snapshot staging record"); }
        }

        void writeU64Le(std::ostream& out, uint64_t value) {
            std::array<uint8_t, 8> bytes{};
            for (size_t i = 0; i < bytes.size(); ++i) { bytes[i] = static_cast<uint8_t>(value >> (i * 8)); }
            out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
            if (!out) { throw std::runtime_error("AkkEngine: failed to write snapshot staging record"); }
        }

        void readExact(std::istream& in, void* out, size_t bytes, const char* what) {
            if (bytes == 0) { return; }
            in.read(static_cast<char*>(out), static_cast<std::streamsize>(bytes));
            if (!in) { throw std::runtime_error(std::string{"AkkEngine: failed to read snapshot staging "} + what); }
        }

        [[nodiscard]] uint32_t readU32Le(std::istream& in, const char* what) {
            std::array<uint8_t, 4> bytes{};
            readExact(in, bytes.data(), bytes.size(), what);
            uint32_t out = 0;
            for (size_t i = 0; i < bytes.size(); ++i) { out |= static_cast<uint32_t>(bytes[i]) << (i * 8); }
            return out;
        }

        [[nodiscard]] uint64_t readU64Le(std::istream& in, const char* what) {
            std::array<uint8_t, 8> bytes{};
            readExact(in, bytes.data(), bytes.size(), what);
            uint64_t out = 0;
            for (size_t i = 0; i < bytes.size(); ++i) { out |= static_cast<uint64_t>(bytes[i]) << (i * 8); }
            return out;
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
                            for (uint32_t bit = 0; bit < 8; ++bit) { crc = (crc >> 1u) ^ (0x82F63B78u & (0u - (crc & 1u))); }
                            out[i] = crc;
                        }
                        return out;
                    }();
                    return values;
                }

                uint32_t crc_ = 0xFFFFFFFFu;
        };

        void updateCrcU32Le(Crc32cStream& crc, uint32_t value) noexcept {
            std::array<uint8_t, 4> bytes{};
            for (size_t i = 0; i < bytes.size(); ++i) { bytes[i] = static_cast<uint8_t>(value >> (i * 8)); }
            crc.update(bytes);
        }

        void updateCrcU64Le(Crc32cStream& crc, uint64_t value) noexcept {
            std::array<uint8_t, 8> bytes{};
            for (size_t i = 0; i < bytes.size(); ++i) { bytes[i] = static_cast<uint8_t>(value >> (i * 8)); }
            crc.update(bytes);
        }

        struct SnapshotStagingEntryHeader {
            std::vector<uint8_t> key;
            uint64_t valueSize = 0;
            uint32_t valueCrc32c = 0;
            Crc32cStream recordCrc;
        };

        [[nodiscard]] SnapshotStagingEntryHeader readSnapshotStagingEntryHeader(std::istream& in) {
            const uint32_t keyLen = readU32Le(in, "entry key length");
            const uint64_t valueLen = readU64Le(in, "entry value length");
            const uint32_t valueCrc32c = readU32Le(in, "entry value crc");
            SnapshotStagingEntryHeader header;
            header.key.resize(keyLen);
            readExact(in, header.key.data(), header.key.size(), "entry key");
            header.valueSize = valueLen;
            header.valueCrc32c = valueCrc32c;
            updateCrcU32Le(header.recordCrc, keyLen);
            updateCrcU64Le(header.recordCrc, valueLen);
            updateCrcU32Le(header.recordCrc, valueCrc32c);
            header.recordCrc.update(header.key);
            return header;
        }

        class ClusterSnapshotExportFile : public std::enable_shared_from_this<ClusterSnapshotExportFile> {
            public:
                static constexpr std::array<char, 5> MAGIC{'A', 'K', 'S', 'E', '1'};
                static constexpr uint64_t FOLLOW_PUBLISH_BYTES = 1024ull * 1024ull;
                static constexpr size_t REPLAY_CHUNK_BYTES = 1024u * 1024u;
                using EntryVisitor = cluster::ClusterSnapshot::EntryVisitor;
                using Producer = std::function<bool(const EntryVisitor&)>;

                ClusterSnapshotExportFile(fs::path path, uint64_t seq, Producer producer)
                    : path_{std::move(path)}, seq_{seq}, producer_{std::move(producer)} {}
                ~ClusterSnapshotExportFile() { removeFileIfExists(path_); }

                ClusterSnapshotExportFile(const ClusterSnapshotExportFile&) = delete;
                ClusterSnapshotExportFile& operator=(const ClusterSnapshotExportFile&) = delete;

                [[nodiscard]] uint64_t seq() const noexcept { return seq_; }
                [[nodiscard]] const fs::path& path() const noexcept { return path_; }
                [[nodiscard]] bool reusable() const {
                    std::lock_guard lock{mutex_};
                    return state_ != State::FAILED;
                }

                bool forEachEntry(const EntryVisitor& visitor) {
                    if (!visitor) { return false; }
                    Producer producer;
                    uint64_t replayCount = 0;
                    std::exception_ptr failure;
                    bool replayReady = false;
                    bool failed = false;
                    bool followActiveProducer = false;
                    {
                        std::lock_guard lock{mutex_};
                        if (state_ == State::COMPLETE) {
                            replayCount = entryCount_;
                            replayReady = true;
                        }
                        else if (state_ == State::FAILED) {
                            failure = failure_;
                            failed = true;
                        }
                        else if (state_ == State::GENERATING) {
                            ++followers_;
                            followActiveProducer = true;
                        }
                        else {
                            state_ = State::GENERATING;
                            producer = std::move(producer_);
                        }
                    }
                    if (replayReady) { return replay(visitor, replayCount); }
                    if (failure) { std::rethrow_exception(failure); }
                    if (failed) { return false; }
                    if (followActiveProducer) {
                        try {
                            const bool followed = follow(visitor);
                            removeFollower();
                            return followed;
                        }
                        catch (...) {
                            removeFollower();
                            throw;
                        }
                    }
                    if (!producer) {
                        failGeneration(nullptr);
                        return false;
                    }

                    try {
                        uint64_t producedEntries = 0;
                        bool primaryVisitorComplete = true;
                        const bool complete = produce(producer, visitor, producedEntries, primaryVisitorComplete);
                        {
                            std::lock_guard lock{mutex_};
                            entryCount_ = complete ? producedEntries : 0;
                            if (complete) { publishedEntries_ = producedEntries; }
                            state_ = complete ? State::COMPLETE : State::FAILED;
                        }
                        completionCv_.notify_all();
                        return complete && primaryVisitorComplete;
                    }
                    catch (...) {
                        failGeneration(std::current_exception());
                        throw;
                    }
                }

            private:
                enum class State : uint8_t { PENDING, GENERATING, COMPLETE, FAILED };

                bool readEntry(std::istream& in, const EntryVisitor& visitor) const {
                    const auto recordStart = in.tellg();
                    auto header = readSnapshotStagingEntryHeader(in);
                    const bool wireSizeValid = header.key.size() <= cluster::ReplFrameHeader::MAX_PAYLOAD_SIZE - 8ull &&
                        header.valueSize <= UINT32_MAX &&
                        header.valueSize <= cluster::ReplFrameHeader::MAX_PAYLOAD_SIZE - 8ull - header.key.size();
                    const bool fileBacked = visitor.fileEntry && wireSizeValid &&
                        8ull + header.key.size() + header.valueSize > visitor.fileEntryThresholdBytes;
                    Crc32cStream payloadCrc;
                    if (fileBacked) {
                        updateCrcU32Le(payloadCrc, static_cast<uint32_t>(header.key.size()));
                        updateCrcU32Le(payloadCrc, static_cast<uint32_t>(header.valueSize));
                        payloadCrc.update(header.key);
                    }
                    else if (!visitor.beginEntry(header.key, header.valueSize, header.valueCrc32c)) { return false; }
                    std::vector<uint8_t> buffer(REPLAY_CHUNK_BYTES);
                    Crc32cStream valueCrc;
                    uint64_t offset = 0;
                    while (offset < header.valueSize) {
                        const size_t count = static_cast<size_t>(std::min<uint64_t>(buffer.size(), header.valueSize - offset));
                        readExact(in, buffer.data(), count, "export entry value");
                        const auto chunk = std::span<const uint8_t>{buffer}.first(count);
                        valueCrc.update(chunk);
                        header.recordCrc.update(chunk);
                        if (fileBacked) { payloadCrc.update(chunk); }
                        else if (!visitor.appendValueChunk(offset, chunk)) { return false; }
                        offset += count;
                    }
                    if (!fileBacked && header.valueSize == 0 && !visitor.appendValueChunk(0, {})) { return false; }
                    const uint32_t recordCrc = readU32Le(in, "export entry crc");
                    if (valueCrc.finish() != header.valueCrc32c || header.recordCrc.finish() != recordCrc) {
                        throw std::runtime_error("AkkEngine: corrupt cluster snapshot export entry");
                    }
                    if (!fileBacked) { return visitor.finishEntry(); }
                    if (recordStart < 0) { throw std::runtime_error("AkkEngine: invalid cluster snapshot export offset"); }
                    return visitor.fileEntry(cluster::SnapshotFileEntry{
                        .path = path_,
                        .dataOffset = static_cast<uint64_t>(recordStart) + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t),
                        .keySize = static_cast<uint32_t>(header.key.size()),
                        .valueSize = header.valueSize,
                        .payloadCrc32c = payloadCrc.finish(),
                        .storage = shared_from_this(),
                    });
                }

                void readHeader(std::istream& in, std::optional<uint64_t> expectedEntryCount) const {
                    std::array<char, MAGIC.size()> magic{};
                    readExact(in, magic.data(), magic.size(), "export header");
                    const uint64_t fileSeq = readU64Le(in, "export seq");
                    const uint64_t fileEntryCount = readU64Le(in, "export entry count");
                    if (magic != MAGIC || fileSeq != seq_ || (expectedEntryCount && fileEntryCount != *expectedEntryCount)) {
                        throw std::runtime_error("AkkEngine: corrupt cluster snapshot export header");
                    }
                }

                bool replay(const EntryVisitor& visitor, uint64_t expectedEntryCount) const {
                    std::ifstream in{path_, std::ios::binary};
                    if (!in) { throw std::runtime_error("AkkEngine: failed to open cluster snapshot export"); }
                    readHeader(in, expectedEntryCount);
                    for (uint64_t index = 0; index < expectedEntryCount; ++index) {
                        if (!readEntry(in, visitor)) { return false; }
                    }
                    char trailing = 0;
                    if (in.get(trailing)) { throw std::runtime_error("AkkEngine: trailing bytes in cluster snapshot export"); }
                    return true;
                }

                bool follow(const EntryVisitor& visitor) const {
                    std::ifstream in;
                    uint64_t consumedEntries = 0;
                    while (true) {
                        uint64_t availableEntries = 0;
                        uint64_t completedEntryCount = 0;
                        State state;
                        std::exception_ptr failure;
                        {
                            std::unique_lock lock{mutex_};
                            completionCv_.wait(lock, [&] {
                                return publishedEntries_ > consumedEntries || state_ != State::GENERATING;
                            });
                            availableEntries = publishedEntries_;
                            completedEntryCount = entryCount_;
                            state = state_;
                            failure = failure_;
                        }

                        if (!in.is_open() && (availableEntries != 0 || state == State::COMPLETE)) {
                            in.open(path_, std::ios::binary);
                            if (!in) { throw std::runtime_error("AkkEngine: failed to follow cluster snapshot export"); }
                            readHeader(in, std::nullopt);
                        }
                        while (consumedEntries < availableEntries) {
                            if (!readEntry(in, visitor)) { return false; }
                            ++consumedEntries;
                        }
                        if (state == State::COMPLETE) {
                            if (consumedEntries != completedEntryCount) {
                                throw std::runtime_error("AkkEngine: cluster snapshot follower entry count mismatch");
                            }
                            char trailing = 0;
                            if (in.get(trailing)) { throw std::runtime_error("AkkEngine: trailing bytes in cluster snapshot export"); }
                            return true;
                        }
                        if (state == State::FAILED) {
                            if (failure) { std::rethrow_exception(failure); }
                            return false;
                        }
                    }
                }

                bool produce(
                    const Producer& producer,
                    const EntryVisitor& visitor,
                    uint64_t& producedEntries,
                    bool& primaryVisitorComplete
                ) {
                    std::ofstream out{path_, std::ios::binary | std::ios::trunc};
                    if (!out) { throw std::runtime_error("AkkEngine: failed to create cluster snapshot export"); }
                    out.write(MAGIC.data(), MAGIC.size());
                    writeU64Le(out, seq_);
                    writeU64Le(out, 0);
                    uint64_t unpublishedBytes = MAGIC.size() + sizeof(uint64_t) * 2;

                    bool entryActive = false;
                    bool sawChunk = false;
                    uint64_t entryValueSize = 0;
                    uint64_t entryValueOffset = 0;
                    uint32_t entryValueChecksum = 0;
                    uint32_t entryKeySize = 0;
                    uint64_t entryDataOffset = 0;
                    bool entryFileBacked = false;
                    Crc32cStream entryValueCrc;
                    Crc32cStream entryRecordCrc;
                    Crc32cStream entryPayloadCrc;
                    const auto addUnpublished = [&](uint64_t bytes) {
                        unpublishedBytes = bytes > UINT64_MAX - unpublishedBytes ? UINT64_MAX : unpublishedBytes + bytes;
                    };
                    const auto hasConsumer = [&] {
                        if (primaryVisitorComplete) { return true; }
                        std::lock_guard lock{mutex_};
                        return followers_ != 0;
                    };
                    const EntryVisitor spoolVisitor{
                        .beginEntry = [&](std::span<const uint8_t> key, uint64_t valueSize, uint32_t valueCrc32c) {
                            if (entryActive) { throw std::runtime_error("AkkEngine: nested cluster snapshot entry"); }
                            if (producedEntries == UINT64_MAX) {
                                throw std::overflow_error("AkkEngine: cluster snapshot has too many entries");
                            }
                            if (key.size() > UINT32_MAX) { throw std::runtime_error("AkkEngine: cluster snapshot key is too large"); }
                            const uint32_t keySize = static_cast<uint32_t>(key.size());
                            const bool wireSizeValid = key.size() <= cluster::ReplFrameHeader::MAX_PAYLOAD_SIZE - 8ull &&
                                valueSize <= UINT32_MAX &&
                                valueSize <= cluster::ReplFrameHeader::MAX_PAYLOAD_SIZE - 8ull - key.size();
                            const uint64_t wireSize = wireSizeValid ? 8ull + key.size() + valueSize : 0;
                            entryActive = true;
                            sawChunk = false;
                            entryValueSize = valueSize;
                            entryValueOffset = 0;
                            entryValueChecksum = valueCrc32c;
                            entryKeySize = keySize;
                            entryFileBacked = visitor.fileEntry && wireSizeValid &&
                                wireSize > visitor.fileEntryThresholdBytes;
                            entryValueCrc = Crc32cStream{};
                            entryRecordCrc = Crc32cStream{};
                            entryPayloadCrc = Crc32cStream{};
                            updateCrcU32Le(entryRecordCrc, keySize);
                            updateCrcU64Le(entryRecordCrc, valueSize);
                            updateCrcU32Le(entryRecordCrc, valueCrc32c);
                            entryRecordCrc.update(key);
                            writeU32Le(out, keySize);
                            writeU64Le(out, valueSize);
                            writeU32Le(out, valueCrc32c);
                            const auto dataOffset = out.tellp();
                            if (dataOffset < 0) { throw std::runtime_error("AkkEngine: invalid cluster snapshot export offset"); }
                            entryDataOffset = static_cast<uint64_t>(dataOffset);
                            if (!key.empty()) {
                                out.write(reinterpret_cast<const char*>(key.data()), static_cast<std::streamsize>(key.size()));
                            }
                            if (!out) { throw std::runtime_error("AkkEngine: failed to write cluster snapshot export header"); }
                            addUnpublished(sizeof(uint32_t) * 2 + sizeof(uint64_t) + key.size());
                            if (entryFileBacked) {
                                updateCrcU32Le(entryPayloadCrc, keySize);
                                updateCrcU32Le(entryPayloadCrc, static_cast<uint32_t>(valueSize));
                                entryPayloadCrc.update(key);
                            }
                            else if (primaryVisitorComplete && !visitor.beginEntry(key, valueSize, valueCrc32c)) {
                                primaryVisitorComplete = false;
                            }
                            return hasConsumer();
                        },
                        .appendValueChunk = [&](uint64_t offset, std::span<const uint8_t> chunk) {
                            if (!entryActive || offset != entryValueOffset || chunk.size() > entryValueSize - entryValueOffset ||
                                (chunk.empty() && (entryValueSize != 0 || sawChunk))) {
                                throw std::runtime_error("AkkEngine: invalid cluster snapshot value chunk");
                            }
                            sawChunk = true;
                            if (!chunk.empty()) {
                                out.write(reinterpret_cast<const char*>(chunk.data()), static_cast<std::streamsize>(chunk.size()));
                            }
                            if (!out) { throw std::runtime_error("AkkEngine: failed to write cluster snapshot value chunk"); }
                            entryValueCrc.update(chunk);
                            entryRecordCrc.update(chunk);
                            if (entryFileBacked) { entryPayloadCrc.update(chunk); }
                            addUnpublished(chunk.size());
                            if (!entryFileBacked && primaryVisitorComplete && !visitor.appendValueChunk(offset, chunk)) {
                                primaryVisitorComplete = false;
                            }
                            entryValueOffset += static_cast<uint64_t>(chunk.size());
                            return hasConsumer();
                        },
                        .finishEntry = [&] {
                            if (!entryActive || !sawChunk || entryValueOffset != entryValueSize ||
                                entryValueCrc.finish() != entryValueChecksum) {
                                throw std::runtime_error("AkkEngine: incomplete cluster snapshot entry");
                            }
                            writeU32Le(out, entryRecordCrc.finish());
                            if (!out) { throw std::runtime_error("AkkEngine: failed to finish cluster snapshot export entry"); }
                            entryActive = false;
                            ++producedEntries;
                            addUnpublished(sizeof(uint32_t));
                            if (unpublishedBytes >= FOLLOW_PUBLISH_BYTES) {
                                out.flush();
                                if (!out) { throw std::runtime_error("AkkEngine: failed to publish cluster snapshot export entries"); }
                                publishEntries(producedEntries);
                                unpublishedBytes = 0;
                            }
                            if (entryFileBacked && primaryVisitorComplete) {
                                out.flush();
                                if (!out) { throw std::runtime_error("AkkEngine: failed to publish cluster snapshot export entry"); }
                                if (!visitor.fileEntry(cluster::SnapshotFileEntry{
                                        .path = path_,
                                        .dataOffset = entryDataOffset,
                                        .keySize = entryKeySize,
                                        .valueSize = entryValueSize,
                                        .payloadCrc32c = entryPayloadCrc.finish(),
                                        .storage = shared_from_this(),
                                    })) {
                                    primaryVisitorComplete = false;
                                }
                            }
                            else if (!entryFileBacked && primaryVisitorComplete && !visitor.finishEntry()) {
                                primaryVisitorComplete = false;
                            }
                            return hasConsumer();
                        },
                        .fileEntryThresholdBytes = UINT64_MAX,
                        .fileEntry = {},
                    };
                    const bool complete = producer(spoolVisitor);
                    if (complete && entryActive) { throw std::runtime_error("AkkEngine: unterminated cluster snapshot entry"); }
                    if (!complete) { return false; }
                    out.seekp(static_cast<std::streamoff>(MAGIC.size() + sizeof(uint64_t)));
                    writeU64Le(out, producedEntries);
                    out.flush();
                    if (!out) { throw std::runtime_error("AkkEngine: failed to finish cluster snapshot export"); }
                    out.close();
                    publishEntries(producedEntries);
                    return true;
                }

                void publishEntries(uint64_t count) {
                    {
                        std::lock_guard lock{mutex_};
                        publishedEntries_ = count;
                    }
                    completionCv_.notify_all();
                }

                void removeFollower() {
                    std::lock_guard lock{mutex_};
                    if (followers_ != 0) { --followers_; }
                }

                void failGeneration(std::exception_ptr failure) {
                    {
                        std::lock_guard lock{mutex_};
                        failure_ = std::move(failure);
                        state_ = State::FAILED;
                    }
                    completionCv_.notify_all();
                }

                fs::path path_;
                uint64_t seq_;
                mutable std::mutex mutex_;
                mutable std::condition_variable completionCv_;
                State state_ = State::PENDING;
                uint64_t entryCount_ = 0;
                uint64_t publishedEntries_ = 0;
                size_t followers_ = 0;
                Producer producer_;
                std::exception_ptr failure_;
        };

        [[nodiscard]] std::vector<uint8_t> encodeSnapshotCommitValue(uint64_t recordCount) {
            std::vector<uint8_t> out;
            out.insert(out.end(), {'A', 'K', 'S', 'C', '1'});
            for (size_t i = 0; i < sizeof(uint64_t); ++i) { out.push_back(static_cast<uint8_t>(recordCount >> (i * 8))); }
            return out;
        }

        [[nodiscard]] uint64_t loadOrCreateNodeId(const fs::path& path) {
            if (path.empty()) { return 0; }
            ensureDir(path.parent_path());

            {
                std::ifstream in(path, std::ios::binary);
                uint64_t id = 0;
                if (in.read(reinterpret_cast<char*>(&id), sizeof(id)) && id != 0) { return id; }
            }

            std::random_device rd;
            std::mt19937_64 rng(rd());
            uint64_t id = rng();
            if (id == 0) { id = 1; }

            std::ofstream out(path, std::ios::binary | std::ios::trunc);
            if (!out.write(reinterpret_cast<const char*>(&id), sizeof(id))) {
                throw std::runtime_error("AkkEngine: failed to persist node id: " + path.string());
            }
            return id;
        }

        [[nodiscard]] int compareKey(std::span<const uint8_t> a, std::span<const uint8_t> b) {
            const int cmp = std::ranges::lexicographical_compare(a, b) ? -1 : std::ranges::lexicographical_compare(b, a) ? 1 : 0;
            return cmp;
        }

        [[nodiscard]] std::span<const uint8_t> copySpanToArena(std::span<const uint8_t> in, core::BufferArena& arena) {
            if (in.empty()) { return {}; }
            std::byte* raw = arena.allocate(in.size(), alignof(uint8_t));
            auto* bytes = reinterpret_cast<uint8_t*>(raw);
            std::memcpy(bytes, in.data(), in.size());
            return {bytes, in.size()};
        }

        [[nodiscard]] std::optional<uint64_t> decodeBlobIdIfReference(uint8_t flags, std::span<const uint8_t> value) noexcept {
            if ((flags & core::MemHdr16::FLAG_BLOB) == 0 || value.size() < blob::BLOB_REF_SIZE) { return std::nullopt; }
            return blob::decodeBlobRef(value.data()).blobId;
        }
    } // namespace

    [[nodiscard]] std::optional<std::span<const uint8_t>> resolveScanValue(
        uint8_t flags,
        std::span<const uint8_t> value,
        blob::BlobManager* blobManager,
        core::BufferArena& arena
    ) {
        if ((flags & core::MemHdr16::FLAG_BLOB) == 0) { return value; }
        if (!blobManager || value.size() < blob::BLOB_REF_SIZE) { return std::nullopt; }
        const blob::BlobRef ref = blob::decodeBlobRef(value.data());
        auto out = blobManager->read(ref.blobId, ref.contentCrc32c);
        if (out.empty() && ref.totalSize != 0) { return std::nullopt; }
        return copySpanToArena(std::span<const uint8_t>{out.data(), out.size()}, arena);
    }

    struct StoredScanRecordView {
        std::span<const uint8_t> key;
        std::span<const uint8_t> value;
        uint8_t flags = 0;
    };

    [[nodiscard]] core::ArenaGenerator<StoredScanRecordView> storedScanGenerator(
        memtable::MemTable::RangeIterator memtableIter,
        sst::SSTManager::Iterator sstIter,
        std::shared_ptr<void> operationLifetime = {}
    ) {
        // Retains a public operation guard for the lifetime of a returned scan.
        (void)operationLifetime;
        auto memtableCur = memtableIter.hasNext() ? memtableIter.next() : std::optional<core::RecordView>{};
        auto sstCur = sstIter.hasNext() ? sstIter.next() : std::optional<sst::SSTRecord>{};

        while (memtableCur || sstCur) {
            const bool hasMt = memtableCur.has_value();
            const bool hasSst = sstCur.has_value();
            int cmp = 0;
            if (hasMt && hasSst) { cmp = compareKey(memtableCur->key(), sstCur->key); }
            else { cmp = hasMt ? -1 : 1; }

            if (cmp == 0) {
                // Frozen MemTables can overlap newer SSTs after asynchronous
                // flush publication. Resolve versions before hiding tombstones.
                if (memtableCur->seq() >= sstCur->seq) {
                    if (!memtableCur->isTombstone()) { co_yield StoredScanRecordView{memtableCur->key(), memtableCur->value(), memtableCur->flags()}; }
                } else if (!sstCur->isTombstone()) { co_yield StoredScanRecordView{sstCur->key, sstCur->value, sstCur->flags}; }
                memtableCur = memtableIter.hasNext() ? memtableIter.next() : std::optional<core::RecordView>{};
                sstCur = sstIter.hasNext() ? sstIter.next() : std::optional<sst::SSTRecord>{};
            }
            else if (cmp < 0) {
                const auto record = *memtableCur;
                if (!record.isTombstone()) { co_yield StoredScanRecordView{record.key(), record.value(), record.flags()}; }
                memtableCur = memtableIter.hasNext() ? memtableIter.next() : std::optional<core::RecordView>{};
            }
            else {
                const auto record = std::move(*sstCur);
                const bool tombstone = record.isTombstone();
                if (!tombstone) { co_yield StoredScanRecordView{record.key, record.value, record.flags}; }
                sstCur = sstIter.hasNext() ? sstIter.next() : std::optional<sst::SSTRecord>{};
            }
        }
    }

    [[nodiscard]] core::ArenaGenerator<AkkEngine::ScanRecordView> scanGenerator(
        core::BufferArena& arena,
        memtable::MemTable::RangeIterator memtableIter,
        sst::SSTManager::Iterator sstIter,
        blob::BlobManager* blobManager,
        std::shared_ptr<void> operationLifetime = {}
    ) {
        for (const auto& record : storedScanGenerator(
                 std::move(memtableIter), std::move(sstIter), std::move(operationLifetime)
             )) {
            auto value = resolveScanValue(record.flags, record.value, blobManager, arena);
            if (value) { co_yield AkkEngine::ScanRecordView{record.key, *value}; }
        }
    }

    class AkkEngine::Impl {
        public:
            using BatchPutEntry = detail::BulkPutEntry;
            template <typename T>
            class StorageSlot {
                public:
                    explicit StorageSlot(std::unique_ptr<T>* slot) : slot_{slot} {}
                    void bind(std::unique_ptr<T>* slot) noexcept { slot_ = slot; }
                    [[nodiscard]] T* get() const noexcept { return slot_ ? slot_->get() : nullptr; }
                    [[nodiscard]] explicit operator bool() const noexcept { return get() != nullptr; }
                    [[nodiscard]] T* operator->() const noexcept { return get(); }
                    [[nodiscard]] T& operator*() const noexcept { return *get(); }
                    [[nodiscard]] bool operator==(std::nullptr_t) const noexcept { return get() == nullptr; }
                    [[nodiscard]] bool operator!=(std::nullptr_t) const noexcept { return get() != nullptr; }
                    void reset() noexcept { slot_->reset(); }

                    StorageSlot& operator=(std::unique_ptr<T> value) noexcept {
                        *slot_ = std::move(value);
                        return *this;
                    }

                private:
                    std::unique_ptr<T>* slot_;
            };

            struct StorageState {
                std::unique_ptr<manifest::Manifest> manifest;
                std::unique_ptr<sst::SSTManager> sstManager;
                std::unique_ptr<memtable::MemTable> memtable;
                std::unique_ptr<wal::WalWriter> walWriter;
                std::unique_ptr<blob::BlobManager> blobManager;
                std::unique_ptr<vlog::VersionLog> versionLog;
            };

            class OperationGuard {
                public:
                    explicit OperationGuard(Impl& owner, bool rejectClosed = true)
                        : owner_{&owner} {
                        if (!owner_->tryBeginOperation()) {
                            if (rejectClosed) { throw std::runtime_error("AkkEngine: engine is closed"); }
                            return;
                        }
                        active_ = true;
                    }

                    ~OperationGuard() {
                        if (!active_) { return; }
                        owner_->endOperation();
                    }

                    OperationGuard(const OperationGuard&) = delete;
                    OperationGuard& operator=(const OperationGuard&) = delete;

                    [[nodiscard]] explicit operator bool() const noexcept { return active_; }

                private:
                    Impl* owner_;
                    bool active_{false};
            };

            explicit Impl(AkkEngineOptions optionsIn)
                : opts{std::move(optionsIn)},
                  writeBackpressureEnabled
                  {opts.runtime.backpressure.maxMemtableImmutableTables != 0 || opts.runtime.backpressure.maxSstL0Files != 0},
                  commitWindowSize{resolveCommitWindowSize(opts.runtime.sequence.commitWindowSize)},
                  storage{std::make_shared<StorageState>()},
                  manifest{&storage->manifest},
                  sstManager{&storage->sstManager},
                  memtable{&storage->memtable},
                  walWriter{&storage->walWriter},
                  blobManager{&storage->blobManager},
                  versionLog{&storage->versionLog},
                  keySequenceOrderMu{
                      opts.runtime.parallelWriteOrder == AkkEngineOptions::ParallelWriteOrderMode::KEY_SEQUENCE
                          ? std::make_unique<std::array<std::mutex, KEY_SEQUENCE_ORDER_STRIPES>>()
                          : nullptr
                  } {}

            static constexpr size_t KEY_SEQUENCE_ORDER_STRIPES = 256;

            AkkEngineOptions opts;
            const bool writeBackpressureEnabled;
            const uint32_t commitWindowSize;
            std::atomic<bool> closed{false};
            uint64_t nodeId = 0;

            std::shared_ptr<StorageState> storage;
            StorageSlot<manifest::Manifest> manifest;
            StorageSlot<sst::SSTManager> sstManager;
            StorageSlot<memtable::MemTable> memtable;
            StorageSlot<wal::WalWriter> walWriter;
            StorageSlot<blob::BlobManager> blobManager;
            StorageSlot<vlog::VersionLog> versionLog;
            std::unique_ptr<cluster::IClusterRuntime> clusterRuntime;
            std::unique_ptr<server::IAkkApiServer> apiServer;
            cluster::ClusterConfig clusterConfig;
            cluster::ReplicationMode clusterReplicationMode = cluster::ReplicationMode::STANDALONE;
            uint64_t clusterConfiguredNodeCount = 0;
            cluster::AckTimeoutAction primaryAckTimeoutAction = cluster::AckTimeoutAction::ACCEPT_LOCAL;
            cluster::detail::KeyedMutex stripeKeys;
            mutable std::mutex stripeStateMu;
            std::shared_mutex stripeGcEpochMu;
            std::shared_ptr<std::atomic<uint64_t>> stripeQueryPins = std::make_shared<std::atomic<uint64_t>>(0);
            mutable std::mutex stripeRepairStatsMu;
            EngineStats::StripeReadRepairStats stripeRepairStats;
            mutable std::mutex stripeRebuildStatsMu;
            EngineStats::StripeRebuildStats stripeRebuildStats;
            std::exception_ptr stripeFailure;
            std::jthread stripeGcThread;
            std::map<std::vector<uint8_t>, std::vector<uint8_t>> stripePending;
            bool stripeGcLoaded = false;
            std::vector<uint8_t> stripeRebuildCursor;
            std::unique_ptr<cluster::detail::BoundedExecutor> stripeExecutor;

            struct DeferredRollbackTask {
                std::array<uint8_t, 16> taskId{};
                std::array<uint8_t, 16> operationId{};
                std::array<uint8_t, 16> createdStartupId{};
                std::array<uint8_t, 16> clusterId{};
                uint64_t configEpoch = 0;
                cluster::StripeControlAction action = cluster::StripeControlAction::ROLLBACK_KEY;
                bool clusterTask = false;
                uint64_t targetNodeId = 0;
                uint64_t targetSeq = 0;
                uint64_t plannedWatermark = 0;
                RollbackConflictPolicy conflict = RollbackConflictPolicy::FAIL_IF_CHANGED;
                std::vector<uint8_t> key;
            };
            std::array<uint8_t, 16> rollbackStartupId{};
            mutable std::mutex rollbackJournalMu;
            std::vector<DeferredRollbackTask> deferredRollbacks;
            std::jthread rollbackRecoveryThread;

            mutable std::mutex blobGcPlanMu;
            mutable std::mutex writeMu;
            std::mutex partitionSnapshotMu;
            mutable std::shared_mutex mutationEpochMu;
            std::unique_ptr<std::array<std::mutex, KEY_SEQUENCE_ORDER_STRIPES>> keySequenceOrderMu;
            mutable std::shared_mutex storageMu;
            mutable std::mutex lifecycleMu;
            std::condition_variable lifecycleCv;
            static constexpr uint64_t LIFECYCLE_CLOSED = 1ULL << 63u;
            static constexpr uint64_t LIFECYCLE_OPERATION_MASK = ~LIFECYCLE_CLOSED;
            std::atomic<uint64_t> lifecycleState{0};
            bool closing{false};
            std::atomic<uint64_t> putsTotal{0};
            std::atomic<uint64_t> removesTotal{0};
            std::atomic<uint64_t> getsTotal{0};
            std::atomic<uint64_t> getsMemtableHit{0};
            std::atomic<uint64_t> getsSstHit{0};
            std::atomic<uint64_t> getsMiss{0};
            std::atomic<uint64_t> existsTotal{0};
            std::atomic<uint64_t> scansTotal{0};
            std::atomic<uint64_t> blobPutsTotal{0};
            mutable std::atomic<uint64_t> backpressureBlockedWrites{0};
            mutable std::atomic<uint64_t> backpressureRejectedWrites{0};
            mutable std::atomic<uint64_t> backpressureTimedOutWrites{0};
            mutable std::atomic<uint64_t> backpressureMemtableStalls{0};
            mutable std::atomic<uint64_t> backpressureSstStalls{0};
            mutable std::atomic<uint64_t> backpressureWaitMicrosTotal{0};
            mutable std::atomic<uint64_t> backpressureWaitMicrosMax{0};
            std::atomic<uint64_t> committedSeq{0};
            mutable std::mutex commitMu;
            mutable std::condition_variable commitCv;
            // A failed write after it reserved a sequence leaves a permanent
            // prefix gap. KEY_SEQUENCE reads must fail instead of waiting for
            // that gap forever.
            std::exception_ptr keySequenceFailure;
            std::vector<uint64_t> completedSeqRing;
            std::unordered_set<uint64_t> completedSeqOverflow;

            bool snapshotInProgress = false;
            uint64_t pendingSnapshotSeq = 0;
            uint64_t pendingSnapshotEntryCount = 0;
            uint64_t pendingSnapshotAppliedEntries = 0;
            std::unordered_set<std::string> pendingSnapshotKeys;
            std::ofstream pendingSnapshotOut;
            bool pendingSnapshotEntryInProgress = false;
            uint64_t pendingSnapshotEntryValueSize = 0;
            uint64_t pendingSnapshotEntryValueOffset = 0;
            uint32_t pendingSnapshotEntryValueCrc32c = 0;
            Crc32cStream pendingSnapshotEntryRecordCrc;
            Crc32cStream pendingSnapshotEntryValueCrc;
            uint64_t durableReplicaSnapshotSeq = 0;
            std::weak_ptr<ClusterSnapshotExportFile> clusterSnapshotExportCache;

            struct AppliedWrite {
                uint64_t seq = 0;
                std::span<const uint8_t> key;
                std::vector<uint8_t> stored;
                uint8_t flags = MemHdr16::FLAG_NORMAL;
                cluster::ReplOpType op = cluster::ReplOpType::PUT;
                uint64_t sourceNodeId = 0;
            };

            [[nodiscard]] uint64_t snapshotSeq() const noexcept {
                if (opts.runtime.writeVisibility == AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER) {
                    return committedSeq.load(std::memory_order_acquire);
                }
                const uint64_t nextSeq = memtable ? memtable->lastSeq() : 0;
                return nextSeq > 0 ? nextSeq - 1 : 0;
            }

            void markWriteCommitted(uint64_t seq, bool knownContiguous = false) {
                if (opts.runtime.writeVisibility == AkkEngineOptions::WriteVisibilityMode::APPLIED) { return; }
                if (knownContiguous && seq == committedSeq.load(std::memory_order_acquire) + 1u) {
                    committedSeq.store(seq, std::memory_order_release);
                    if (opts.runtime.parallelWriteOrder == AkkEngineOptions::ParallelWriteOrderMode::KEY_SEQUENCE) {
                        commitCv.notify_all();
                    }
                    return;
                }
                bool advanced = false;
                {
                    std::lock_guard lock{commitMu};
                    uint64_t current = committedSeq.load(std::memory_order_relaxed);
                    if (seq <= current) { return; }

                    ensureCompletedSeqRing();
                    const size_t mask = completedSeqRing.size() - 1u;
                    const uint64_t distance = seq - current;
                    if (distance <= completedSeqRing.size()) {
                        const size_t slot = static_cast<size_t>(seq) & mask;
                        if (completedSeqRing[slot] == 0 || completedSeqRing[slot] <= current) { completedSeqRing[slot] = seq; }
                        else if (completedSeqRing[slot] != seq) { completedSeqOverflow.insert(seq); }
                    }
                    else { completedSeqOverflow.insert(seq); }

                    while (true) {
                        const uint64_t next = current + 1u;
                        const size_t slot = static_cast<size_t>(next) & mask;
                        if (completedSeqRing[slot] == next) {
                            completedSeqRing[slot] = 0;
                            current = next;
                            advanced = true;
                            continue;
                        }
                        if (completedSeqOverflow.erase(next) != 0) {
                            current = next;
                            advanced = true;
                            continue;
                        }
                        break;
                    }
                    if (advanced) { committedSeq.store(current, std::memory_order_release); }
                }
                if (advanced) { commitCv.notify_all(); }
            }

            void waitForKeySequenceReadVisibility() const {
                if (opts.runtime.parallelWriteOrder != AkkEngineOptions::ParallelWriteOrderMode::KEY_SEQUENCE || opts.runtime.
                    writeVisibility != AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER || !memtable) { return; }
                const uint64_t nextSeq = memtable->lastSeq();
                const uint64_t target = nextSeq > 0 ? nextSeq - 1 : 0;
                if (committedSeq.load(std::memory_order_acquire) >= target) { return; }

                std::unique_lock lock{commitMu};
                commitCv.wait(
                    lock,
                    [this, target] {
                        return committedSeq.load(std::memory_order_acquire) >= target || keySequenceFailure || closed.load(
                            std::memory_order_acquire
                        );
                    }
                );
                const std::exception_ptr failure = keySequenceFailure;
                lock.unlock();
                if (failure) { std::rethrow_exception(failure); }
            }

            void markKeySequenceFailure(std::exception_ptr failure) {
                {
                    std::lock_guard lock{commitMu};
                    if (!keySequenceFailure) { keySequenceFailure = std::move(failure); }
                }
                commitCv.notify_all();
            }

            void resetCommittedSeq(uint64_t seq) {
                std::lock_guard lock{commitMu};
                committedSeq.store(seq, std::memory_order_release);
                std::ranges::fill(completedSeqRing, 0);
                completedSeqOverflow.clear();
            }

            void ensureCompletedSeqRing() { if (completedSeqRing.empty()) { completedSeqRing.resize(commitWindowSize); } }

            void throwIfBackgroundFailed() const {
                { std::lock_guard lock{transactionFailureMu}; if (transactionFailure) { std::rethrow_exception(transactionFailure); } }
                if (opts.runtime.parallelWriteOrder == AkkEngineOptions::ParallelWriteOrderMode::KEY_SEQUENCE) {
                    std::exception_ptr failure;
                    {
                        std::lock_guard lock{commitMu};
                        failure = keySequenceFailure;
                    }
                    if (failure) { std::rethrow_exception(failure); }
                }
                if (walWriter) { walWriter->throwIfFailed(); }
                if (sstManager) { sstManager->throwIfBackgroundFailed(); }
            }

            [[nodiscard]] wal::WalAppendAck walAckForWriteDurability() const noexcept {
                switch (opts.runtime.writeDurability) {
                    case AkkEngineOptions::WriteDurabilityMode::MEMORY:
                    case AkkEngineOptions::WriteDurabilityMode::ENQUEUED: return wal::WalAppendAck::ENQUEUED;
                    case AkkEngineOptions::WriteDurabilityMode::WRITTEN: return wal::WalAppendAck::WRITTEN;
                    case AkkEngineOptions::WriteDurabilityMode::SYNCED: return wal::WalAppendAck::SYNCED;
                }
                return wal::WalAppendAck::WRITTEN;
            }

            [[nodiscard]] uint64_t reserveWriteSeq(uint64_t count) {
                if (count != 1 || opts.runtime.sequence.allocation != AkkEngineOptions::SequenceAllocationMode::THREAD_LOCAL_RANGES) {
                    return memtable->reserveSeq(count);
                }

                struct SeqRangeCache {
                    const Impl* owner = nullptr;
                    uint64_t next = 0;
                    uint64_t end = 0;
                };
                static thread_local SeqRangeCache cache;

                if (cache.owner != this || cache.next >= cache.end) {
                    const uint32_t rangeSize = std::max<uint32_t>(2, opts.runtime.sequence.threadLocalRangeSize);
                    const uint64_t base = memtable->reserveSeq(rangeSize);
                    cache.owner = this;
                    cache.next = base;
                    cache.end = base + rangeSize;
                }
                return cache.next++;
            }

            [[nodiscard]] std::shared_ptr<StorageState> pinStorage() const {
                std::shared_lock lock{storageMu};
                return storage;
            }

            [[nodiscard]] bool tryBeginOperation() {
                uint64_t state = lifecycleState.load(std::memory_order_acquire);
                while ((state & LIFECYCLE_CLOSED) == 0) {
                    if ((state & LIFECYCLE_OPERATION_MASK) == LIFECYCLE_OPERATION_MASK) {
                        throw std::runtime_error("AkkEngine: too many in-flight operations");
                    }
                    if (lifecycleState.compare_exchange_weak(state, state + 1u, std::memory_order_acq_rel, std::memory_order_acquire)) {
                        return true;
                    }
                }
                return false;
            }

            void endOperation() noexcept {
                const uint64_t previous = lifecycleState.fetch_sub(1u, std::memory_order_acq_rel);
                if ((previous & LIFECYCLE_OPERATION_MASK) == 1u) { lifecycleCv.notify_all(); }
            }

            [[nodiscard]] bool beginClose() {
                std::unique_lock lock{lifecycleMu};
                if (closed.load(std::memory_order_acquire)) {
                    lifecycleCv.wait(lock, [this]() { return !closing; });
                    return false;
                }
                lifecycleState.fetch_or(LIFECYCLE_CLOSED, std::memory_order_acq_rel);
                closed.store(true, std::memory_order_release);
                closing = true;
                lock.unlock();
                commitCv.notify_all();
                if (clusterRuntime) { clusterRuntime->cancelMirrorRecovery(); }
                if (walWriter) { walWriter->requestClose(); }
                lock.lock();
                lifecycleCv.wait(
                    lock,
                    [this]() { return (lifecycleState.load(std::memory_order_acquire) & LIFECYCLE_OPERATION_MASK) == 0; }
                );
                return true;
            }

            void finishClose() noexcept {
                {
                    std::lock_guard lock{lifecycleMu};
                    closing = false;
                }
                lifecycleCv.notify_all();
            }

            void replaceStorage(std::shared_ptr<StorageState> replacement) {
                if (!replacement) { throw std::invalid_argument("AkkEngine: replacement storage is required"); }
                std::unique_lock lock{storageMu};
                storage = std::move(replacement);
                manifest.bind(&storage->manifest);
                sstManager.bind(&storage->sstManager);
                memtable.bind(&storage->memtable);
                walWriter.bind(&storage->walWriter);
                blobManager.bind(&storage->blobManager);
                versionLog.bind(&storage->versionLog);
            }

            [[nodiscard]] bool persistent() const noexcept {
                return opts.components.walEnabled || opts.components.sstEnabled || opts.components.manifestEnabled;
            }

            [[nodiscard]] std::vector<uint8_t> maybeExternalize(uint64_t seq, std::span<const uint8_t> value, uint8_t& flags) {
                if (!blobManager || value.size() < blobManager->threshold()) { return {value.begin(), value.end()}; }
                if (seq >= LOCAL_BLOB_ID_LIMIT) { throw std::overflow_error("AkkEngine: local Blob sequence is outside encodable range"); }

                blobManager->write(seq, value);
                std::vector<uint8_t> ref(blob::BLOB_REF_SIZE);
                blob::encodeBlobRef(ref.data(), blob::BlobRef{seq, static_cast<uint64_t>(value.size()), blob::crc32c(value)});
                flags |= MemHdr16::FLAG_BLOB;
                if (clusterRuntime) { clusterRuntime->shipBlob(seq, seq, value); }
                return ref;
            }

            struct StagedSnapshotRecord {
                std::vector<uint8_t> key;
                std::vector<uint8_t> storedValue;
                uint8_t flags = MemHdr16::FLAG_NORMAL;
                uint64_t fp64 = 0;
                uint64_t miniKey = 0;
                bool history = false;
                uint64_t sequence = 0, source = 0, timestamp = 0;
                uint64_t blobId = 0;
                bool historyPresent = false;
            };

            static bool streamSnapshotValue(const cluster::SnapshotEntryVisitor& visitor, std::span<const uint8_t> key,
                std::span<const uint8_t> value, uint8_t flags, blob::BlobManager* blobs) {
                if ((flags & MemHdr16::FLAG_BLOB) != 0) {
                    if (!blobs || value.size() != blob::BLOB_REF_SIZE) { throw std::runtime_error("AkkEngine: missing snapshot Blob"); }
                    const auto ref = blob::decodeBlobRef(value.data());
                    if (!visitor.beginEntry(key, ref.totalSize, ref.contentCrc32c)) { return false; }
                    if (!blobs->streamRead(ref.blobId, ref.contentCrc32c, ClusterSnapshotExportFile::REPLAY_CHUNK_BYTES,
                        [&](uint64_t offset, auto chunk) { return visitor.appendValueChunk(offset, chunk); })) { return false; }
                    return visitor.finishEntry();
                }
                Crc32cStream crc; crc.update(value);
                if (!visitor.beginEntry(key, value.size(), crc.finish())) { return false; }
                if (value.empty() && !visitor.appendValueChunk(0, {})) { return false; }
                for (size_t offset = 0; offset < value.size();) {
                    const auto chunk = value.subspan(offset, std::min(ClusterSnapshotExportFile::REPLAY_CHUNK_BYTES, value.size() - offset));
                    if (!visitor.appendValueChunk(offset, chunk)) { return false; }
                    offset += chunk.size();
                }
                return visitor.finishEntry();
            }

            static uint64_t snapshotBlobIdentity(uint8_t flags, std::span<const uint8_t> value) {
                if ((flags & MemHdr16::FLAG_BLOB) == 0) { return 0; }
                if (value.size() != blob::BLOB_REF_SIZE) { throw std::runtime_error("AkkEngine: invalid snapshot Blob reference"); }
                return blob::decodeBlobRef(value.data()).blobId;
            }

            [[nodiscard]] StagedSnapshotRecord readStagedSnapshotRecord(std::istream& in, uint64_t seq, uint64_t ordinal, bool writeBlob,
                uint64_t storedHistoryThrough = 0) {
                auto header = readSnapshotStagingEntryHeader(in);
                Crc32cStream valueCrc;
                StagedSnapshotRecord record;
                record.key = std::move(header.key);
                if (clusterConfig.usesDataConsensus()) {
                    const auto decoded = cluster::detail::decodeSnapshotKey(record.key);
                    if (decoded.kind == cluster::detail::SnapshotRecordKind::STATE ||
                        (decoded.kind == cluster::detail::SnapshotRecordKind::HISTORY &&
                         (decoded.sequence == 0 || decoded.sequence > seq))) {
                        throw std::runtime_error("AkkEngine: invalid history snapshot record");
                    }
                    record.history = decoded.kind == cluster::detail::SnapshotRecordKind::HISTORY;
                    record.sequence = decoded.sequence; record.source = decoded.source; record.timestamp = decoded.timestamp;
                    record.blobId = decoded.blobId;
                    record.flags = decoded.flags & static_cast<uint8_t>(~MemHdr16::FLAG_BLOB);
                    record.key = std::vector<uint8_t>{decoded.key.begin(), decoded.key.end()};
                    if (record.history) {
                        record.historyPresent = record.sequence <= snapshotSeq() ||
                            (writeBlob && versionLog && record.sequence <= storedHistoryThrough &&
                             versionLog->containsStoredRecord(record.key, record.sequence, record.source, record.timestamp, record.flags));
                        if (record.historyPresent) { writeBlob = false; }
                    }
                }
                record.fp64 = core::computeKeyFp64(record.key);
                record.miniKey = core::buildMiniKey(record.key);

                const bool externalize = blobManager && header.valueSize >= blobManager->threshold();
                uint64_t blobId = 0;
                bool blobStarted = false;
                try {
                    if (externalize) {
                        blobId = record.blobId != 0 ? record.blobId : snapshotBlobId(seq, ordinal);
                        record.storedValue.resize(blob::BLOB_REF_SIZE);
                        blob::encodeBlobRef(record.storedValue.data(), blob::BlobRef{blobId, header.valueSize, header.valueCrc32c});
                        record.flags |= MemHdr16::FLAG_BLOB;
                        if (writeBlob) {
                            blobManager->abortWrite(blobId);
                            blobManager->beginWrite(blobId, header.valueSize, header.valueCrc32c);
                            blobStarted = true;
                        }

                        std::vector<uint8_t> buffer;
                        buffer.resize(static_cast<size_t>(std::min<uint64_t>(SNAPSHOT_STAGING_VALUE_CHUNK_BYTES, header.valueSize)));
                        uint64_t offset = 0;
                        while (offset < header.valueSize) {
                            const size_t chunkSize = static_cast<size_t>(std::min<uint64_t>(buffer.size(), header.valueSize - offset));
                            readExact(in, buffer.data(), chunkSize, "entry value chunk");
                            const std::span<const uint8_t> chunk{buffer.data(), chunkSize};
                            header.recordCrc.update(chunk);
                            valueCrc.update(chunk);
                            if (writeBlob) { blobManager->appendWriteChunk(blobId, offset, chunk); }
                            offset += static_cast<uint64_t>(chunkSize);
                        }
                    }
                    else {
                        if (header.valueSize > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
                            throw std::runtime_error("AkkEngine: replication snapshot staged value is too large");
                        }
                        record.storedValue.resize(static_cast<size_t>(header.valueSize));
                        readExact(in, record.storedValue.data(), record.storedValue.size(), "entry value");
                        header.recordCrc.update(record.storedValue);
                        valueCrc.update(record.storedValue);
                    }

                    const uint32_t storedRecordCrc = readU32Le(in, "entry crc");
                    if (header.recordCrc.finish() != storedRecordCrc || valueCrc.finish() != header.valueCrc32c) {
                        throw std::runtime_error("AkkEngine: replication snapshot staging entry CRC mismatch");
                    }
                    if (blobStarted) {
                        blobManager->finishWrite(blobId);
                        blobStarted = false;
                    }
                }
                catch (...) {
                    if (blobStarted) { blobManager->abortWrite(blobId); }
                    throw;
                }

                return record;
            }

            struct PreparedRaftValue {
                std::vector<uint8_t> stored;
                std::optional<cluster::ClusterBlobPayload> blob;
            };

            [[nodiscard]] PreparedRaftValue prepareRaftValue(uint64_t seq, std::span<const uint8_t> value, uint8_t& flags) {
                if (!blobManager || value.size() < blobManager->threshold()) {
                    return PreparedRaftValue{.stored = {value.begin(), value.end()}, .blob = std::nullopt};
                }

                const uint64_t blobId = opts.cluster.runtime.raftBlobPolicy == cluster::RaftBlobPolicy::RAFT_LOG
                                            ? raftLogBlobId(nodeId, seq)
                                            : seq;
                std::vector<uint8_t> ref(blob::BLOB_REF_SIZE);
                blob::encodeBlobRef(ref.data(), blob::BlobRef{blobId, static_cast<uint64_t>(value.size()), blob::crc32c(value)});
                flags |= MemHdr16::FLAG_BLOB;

                if (opts.cluster.runtime.raftBlobPolicy == cluster::RaftBlobPolicy::RAFT_LOG) {
                    return PreparedRaftValue{
                        .stored = std::move(ref),
                        .blob = cluster::ClusterBlobPayload{.blobId = blobId, .content = {value.begin(), value.end()}},
                    };
                }

                blobManager->write(blobId, value);
                return PreparedRaftValue{.stored = std::move(ref), .blob = std::nullopt};
            }

            [[nodiscard]] std::optional<std::vector<uint8_t>> resolveValue(uint8_t flags, std::span<const uint8_t> value) const {
                if ((flags & MemHdr16::FLAG_BLOB) == 0) { return std::vector<uint8_t>{value.begin(), value.end()}; }
                if (!blobManager || value.size() < blob::BLOB_REF_SIZE) { return std::nullopt; }
                const auto [blobId, totalSize, contentCrc32c] = blob::decodeBlobRef(value.data());
                auto out = blobManager->read(blobId, contentCrc32c);
                if (out.empty() && totalSize != 0) { return std::nullopt; }
                return out;
            }

            struct StripeMetadata {
                bool tombstone = false;
                bool rollback = false;
                uint64_t version = 0;
                uint64_t ownerNodeId = 0;
                uint64_t authorityNodeId = 0;
                uint64_t fenceToken = 0;
                uint64_t originalSize = 0;
                erasure::ErasureLayout layout;
                uint8_t copiesPerShard = 1;
                std::vector<uint64_t> nodeIds;
                std::vector<uint8_t> shardPresent;
                uint64_t logicalSeq = 0;
                uint64_t originNodeId = 0;
                uint64_t timestampNs = 0;
            };

            static void pushU16(std::vector<uint8_t>& out, uint16_t value) {
                out.push_back(static_cast<uint8_t>(value));
                out.push_back(static_cast<uint8_t>(value >> 8));
            }

            static void pushU32(std::vector<uint8_t>& out, uint32_t value) {
                for (size_t i = 0; i < 4; ++i) { out.push_back(static_cast<uint8_t>(value >> (i * 8))); }
            }

            static void pushU64(std::vector<uint8_t>& out, uint64_t value) {
                for (size_t i = 0; i < 8; ++i) { out.push_back(static_cast<uint8_t>(value >> (i * 8))); }
            }

            static bool pullU16(std::span<const uint8_t> bytes, size_t& cursor, uint16_t& out) {
                if (cursor + 2 > bytes.size()) { return false; }
                out = static_cast<uint16_t>(bytes[cursor]) | static_cast<uint16_t>(bytes[cursor + 1] << 8);
                cursor += 2;
                return true;
            }

            static bool pullU32(std::span<const uint8_t> bytes, size_t& cursor, uint32_t& out) {
                if (cursor + 4 > bytes.size()) { return false; }
                out = 0;
                for (size_t i = 0; i < 4; ++i) { out |= static_cast<uint32_t>(bytes[cursor + i]) << (i * 8); }
                cursor += 4;
                return true;
            }

            static bool pullU64(std::span<const uint8_t> bytes, size_t& cursor, uint64_t& out) {
                if (cursor + 8 > bytes.size()) { return false; }
                out = 0;
                for (size_t i = 0; i < 8; ++i) { out |= static_cast<uint64_t>(bytes[cursor + i]) << (i * 8); }
                cursor += 8;
                return true;
            }

            static bool pullBytes(std::span<const uint8_t> bytes, size_t& cursor, size_t size, std::vector<uint8_t>& out) {
                if (size > bytes.size() - cursor) { return false; }
                out.assign(bytes.begin() + static_cast<std::ptrdiff_t>(cursor), bytes.begin() + static_cast<std::ptrdiff_t>(cursor + size));
                cursor += size;
                return true;
            }

            static std::vector<uint8_t> stripeMetaKey(std::span<const uint8_t> key) {
                // A fixed namespace followed by the raw key preserves public
                // byte order and permits bounded metadata range seeks.
                std::vector<uint8_t> out{0, 'A', 'K', 'S', 'M', '1'};
                out.insert(out.end(), key.begin(), key.end());
                return out;
            }

            static std::vector<uint8_t> stripeHistoryPlacementKey(std::span<const uint8_t> key, uint64_t sequence, uint64_t generation) {
                std::vector<uint8_t> out{0, 'A', 'K', 'S', 'H', '2'};
                pushU64(out, generation); pushU64(out, sequence); out.insert(out.end(), key.begin(), key.end()); return out;
            }
            StripeMetadata effectiveStripeMetadata(std::span<const uint8_t> key, const StripeMetadata& original) {
                const auto active = activePlacementGeneration.load(std::memory_order_acquire);
                const auto value = getValueInternal(stripeHistoryPlacementKey(key, original.logicalSeq, active), false);
                if (!value) { return original; }
                size_t cursor = 0; uint64_t generation = 0;
                if (!pullU64(*value, cursor, generation)) { throw std::runtime_error("AkkEngine: malformed stripe placement alias"); }
                if (generation != active) { throw std::runtime_error("AkkEngine: stripe alias placement generation mismatch"); }
                auto metadata = decodeStripeMetadata(std::span{*value}.subspan(cursor));
                if (!metadata || metadata->logicalSeq != original.logicalSeq || metadata->version != original.version ||
                    metadata->originNodeId != original.originNodeId || metadata->timestampNs != original.timestampNs) {
                    throw std::runtime_error("AkkEngine: invalid stripe placement alias");
                }
                return *metadata;
            }

            static std::vector<uint8_t> stripeShardKey(std::span<const uint8_t> key, uint64_t version, uint16_t shardIndex) {
                std::vector<uint8_t> out{0, 'A', 'K', 'S', 'S', '1'};
                pushU64(out, version);
                pushU16(out, shardIndex);
                pushU32(out, static_cast<uint32_t>(key.size()));
                out.insert(out.end(), key.begin(), key.end());
                return out;
            }

            static bool isStripeInternalKey(std::span<const uint8_t> key) {
                return key.size() >= 6 && key[0] == 0 && key[1] == 'A' && key[2] == 'K' && key[3] == 'S' &&
                       ((key[4] == 'M' && key[5] == '1') ||
                        ((key[4] == 'S' || key[4] == 'T') && key[5] == '1') || (key[4] == 'H' && key[5] == '2'));
            }

            static std::optional<std::vector<uint8_t>> publicKeyFromStripeMetaKey(std::span<const uint8_t> key) {
                if (key.size() < 6 || key[0] != 0 || key[1] != 'A' || key[2] != 'K' || key[3] != 'S' || key[4] != 'M' ||
                    key[5] != '1') {
                    return std::nullopt;
                }
                std::vector<uint8_t> out;
                out.assign(key.begin() + 6, key.end());
                return out;
            }

            static std::vector<uint8_t> encodeStripeMetadata(const StripeMetadata& metadata) {
                std::vector<uint8_t> out{'A', 'K', 'S', 'M', '1'};
                out.push_back(static_cast<uint8_t>((metadata.tombstone ? 1U : 0U) | (metadata.rollback ? 2U : 0U)));
                pushU64(out, metadata.version);
                pushU64(out, metadata.ownerNodeId);
                pushU64(out, metadata.authorityNodeId);
                pushU64(out, metadata.fenceToken);
                pushU64(out, metadata.originalSize);
                pushU16(out, metadata.layout.dataShards);
                pushU16(out, metadata.layout.parityShards);
                pushU16(out, static_cast<uint16_t>(metadata.nodeIds.size()));
                for (const auto nodeId : metadata.nodeIds) { pushU64(out, nodeId); }
                pushU16(out, static_cast<uint16_t>(metadata.shardPresent.size()));
                out.insert(out.end(), metadata.shardPresent.begin(), metadata.shardPresent.end());
                pushU64(out, metadata.logicalSeq);
                pushU64(out, metadata.originNodeId);
                pushU64(out, metadata.timestampNs);
                return out;
            }

            static std::optional<StripeMetadata> decodeStripeMetadata(std::span<const uint8_t> bytes) {
                if (bytes.size() < 62 || bytes[0] != 'A' || bytes[1] != 'K' || bytes[2] != 'S' || bytes[3] != 'M' || bytes[4] != '1') {
                    return std::nullopt;
                }
                size_t cursor = 5;
                StripeMetadata metadata;
                const uint8_t flags = bytes[cursor++];
                if ((flags & ~uint8_t{3}) != 0) { return std::nullopt; }
                metadata.tombstone = (flags & 1U) != 0;
                metadata.rollback = (flags & 2U) != 0;
                if (!pullU64(bytes, cursor, metadata.version) || !pullU64(bytes, cursor, metadata.ownerNodeId) ||
                    !pullU64(bytes, cursor, metadata.authorityNodeId) || !pullU64(bytes, cursor, metadata.fenceToken) ||
                    !pullU64(bytes, cursor, metadata.originalSize) ||
                    !pullU16(bytes, cursor, metadata.layout.dataShards) || !pullU16(bytes, cursor, metadata.layout.parityShards)) {
                    return std::nullopt;
                }
                uint16_t nodeCount = 0;
                const uint16_t logicalShards = metadata.layout.totalShards();
                if (!pullU16(bytes, cursor, nodeCount) || logicalShards == 0 || nodeCount % logicalShards != 0) {
                    return std::nullopt;
                }
                const uint16_t copiesPerShard = nodeCount / logicalShards;
                if (copiesPerShard != 1 && copiesPerShard != 2) { return std::nullopt; }
                metadata.copiesPerShard = static_cast<uint8_t>(copiesPerShard);
                metadata.nodeIds.reserve(nodeCount);
                for (uint16_t i = 0; i < nodeCount; ++i) {
                    uint64_t nodeId = 0;
                    if (!pullU64(bytes, cursor, nodeId)) { return std::nullopt; }
                    metadata.nodeIds.push_back(nodeId);
                }
                uint16_t presenceCount = 0;
                if (!pullU16(bytes, cursor, presenceCount) || presenceCount != nodeCount || presenceCount > bytes.size() - cursor) {
                    return std::nullopt;
                }
                metadata.shardPresent.assign(
                    bytes.begin() + static_cast<std::ptrdiff_t>(cursor),
                    bytes.begin() + static_cast<std::ptrdiff_t>(cursor + presenceCount)
                );
                if (std::ranges::any_of(metadata.shardPresent, [](uint8_t present) { return present > 1; })) { return std::nullopt; }
                cursor += presenceCount;
                if (!pullU64(bytes, cursor, metadata.logicalSeq) || !pullU64(bytes, cursor, metadata.originNodeId) ||
                    !pullU64(bytes, cursor, metadata.timestampNs) || metadata.originNodeId == 0 || metadata.timestampNs == 0) { return std::nullopt; }
                return cursor == bytes.size() ? std::optional<StripeMetadata>{std::move(metadata)} : std::nullopt;
            }

            static std::vector<uint8_t> encodeStripeShard(uint64_t version, const erasure::ErasureShard& shard, erasure::ErasureLayout layout) {
                std::vector<uint8_t> out{'A', 'K', 'S', 'S', '1'};
                pushU64(out, version);
                pushU16(out, shard.index);
                pushU64(out, shard.originalSize);
                pushU16(out, layout.dataShards);
                pushU16(out, layout.parityShards);
                pushU32(out, shard.crc32c);
                pushU32(out, static_cast<uint32_t>(shard.payload.size()));
                out.insert(out.end(), shard.payload.begin(), shard.payload.end());
                return out;
            }

            static std::optional<erasure::ErasureShard> decodeStripeShard(std::span<const uint8_t> bytes, uint64_t version, erasure::ErasureLayout layout) {
                if (bytes.size() < 33 || bytes[0] != 'A' || bytes[1] != 'K' || bytes[2] != 'S' || bytes[3] != 'S' || bytes[4] != '1') {
                    return std::nullopt;
                }
                size_t cursor = 5;
                uint64_t storedVersion = 0;
                uint16_t index = 0;
                uint64_t originalSize = 0;
                erasure::ErasureLayout storedLayout;
                uint32_t crc = 0;
                uint32_t payloadSize = 0;
                if (!pullU64(bytes, cursor, storedVersion) || !pullU16(bytes, cursor, index) || !pullU64(bytes, cursor, originalSize) ||
                    !pullU16(bytes, cursor, storedLayout.dataShards) || !pullU16(bytes, cursor, storedLayout.parityShards) ||
                    !pullU32(bytes, cursor, crc) || !pullU32(bytes, cursor, payloadSize)) {
                    return std::nullopt;
                }
                if (storedVersion != version || storedLayout.dataShards != layout.dataShards || storedLayout.parityShards != layout.parityShards) {
                    return std::nullopt;
                }
                erasure::ErasureShard shard;
                shard.index = index;
                shard.originalSize = originalSize;
                shard.codec = erasure::ErasureCodecKind::RS;
                shard.crc32c = crc;
                if (!pullBytes(bytes, cursor, payloadSize, shard.payload) || cursor != bytes.size() || !shard.verifyCrc()) {
                    return std::nullopt;
                }
                return shard;
            }

            [[nodiscard]] std::optional<std::vector<uint8_t>> getValueInternal(
                std::span<const uint8_t> key,
                bool countStats,
                std::optional<uint64_t> requestedSnapshot = std::nullopt
            ) {
                if (usesPartitionRaft()) {
                    auto* child = localPartition(key);
                    return child ? child->impl_->getValueInternal(key, countStats, requestedSnapshot) : std::nullopt;
                }
                throwIfBackgroundFailed();
                if (!requestedSnapshot.has_value()) { waitForKeySequenceReadVisibility(); }
                if (countStats) { getsTotal.fetch_add(1, std::memory_order_relaxed); }
                const uint64_t seq = requestedSnapshot.value_or(snapshotSeq());

                auto pinned = memtable->getPinned(key, seq);
                if (pinned) {
                    const auto& view = pinned->view;
                    if (view.isTombstone()) {
                        getsMiss.fetch_add(1, std::memory_order_relaxed);
                        return std::nullopt;
                    }
                    auto value = resolveValue(view.flags(), view.value());
                    if (value) { getsMemtableHit.fetch_add(1, std::memory_order_relaxed); }
                    else { getsMiss.fetch_add(1, std::memory_order_relaxed); }
                    return value;
                }

                if (sstManager) {
                    auto record = sstManager->get(key, seq);
                    if (record) {
                        if (record->isTombstone()) {
                            getsMiss.fetch_add(1, std::memory_order_relaxed);
                            return std::nullopt;
                        }
                        if (opts.runtime.sstPromoteReads && memtable) {
                            memtable->put(record->key, record->value, record->seq, record->flags, record->keyFp64, record->miniKey);
                        }
                        auto value = resolveValue(record->flags, record->value);
                        if (value) { getsSstHit.fetch_add(1, std::memory_order_relaxed); }
                        else { getsMiss.fetch_add(1, std::memory_order_relaxed); }
                        return value;
                    }
                }
                getsMiss.fetch_add(1, std::memory_order_relaxed);
                return std::nullopt;
            }

            [[nodiscard]] erasure::ErasureLayout stripeLayoutFor(const cluster::ClusterConfig& config) const {
                return erasure::ErasureLayout{
                    .dataShards = config.stripe().dataShards,
                    .parityShards = config.stripe().parityShards,
                };
            }

            [[nodiscard]] std::vector<uint64_t> stripeNodeIdsFor(
                const cluster::ClusterConfig& config, std::span<const uint8_t> key, uint64_t excludedNodeId = 0
            ) const {
                const uint16_t totalShards = config.stripe().totalShards();
                const uint16_t totalPlacements = config.stripe().totalPlacements();
                if (totalShards == 0) { throw std::runtime_error("AkkEngine: invalid STRIPE shard count"); }
                const auto dataNodes = config.stripePlacementNodes();
                if (dataNodes.size() < totalPlacements) { throw std::runtime_error("AkkEngine: not enough data-bearing nodes for STRIPE"); }

                if (config.stripe().copiesPerShard == 2) {
                    struct MirrorPair {
                        uint64_t score = 0;
                        uint64_t first = 0;
                        uint64_t second = 0;
                    };
                    std::vector<MirrorPair> pairs;
                    pairs.reserve(totalShards);
                    for (uint16_t i = 0; i < totalShards; ++i) {
                        uint64_t first = dataNodes[i * 2].nodeId;
                        uint64_t second = dataNodes[i * 2 + 1].nodeId;
                        const uint64_t firstScore = rendezvousScore(key, first);
                        const uint64_t secondScore = rendezvousScore(key, second);
                        if (secondScore > firstScore || (secondScore == firstScore && second < first)) { std::swap(first, second); }
                        const auto pairIds = std::minmax(first, second);
                        pairs.push_back(MirrorPair{.score = rendezvousScore(key, pairIds.first, pairIds.second), .first = first, .second = second});
                    }
                    std::ranges::sort(pairs, [](const auto& left, const auto& right) {
                        if (left.score != right.score) { return left.score > right.score; }
                        return std::min(left.first, left.second) < std::min(right.first, right.second);
                    });
                    std::vector<uint64_t> ids;
                    ids.reserve(totalPlacements);
                    for (const auto& pair : pairs) {
                        ids.push_back(pair.first);
                        ids.push_back(pair.second);
                    }
                    if (excludedNodeId != 0) {
                        const auto* spare = config.stripeFailoverNode();
                        const auto replaced = std::ranges::find(ids, excludedNodeId);
                        if (spare == nullptr || replaced == ids.end()) {
                            throw std::runtime_error("AkkEngine: RAID.10 hot spare cannot replace the unavailable owner");
                        }
                        *replaced = spare->nodeId;
                    }
                    return ids;
                }

                std::vector<std::pair<uint64_t, uint64_t>> scored;
                scored.reserve(dataNodes.size());
                for (const auto& target : dataNodes) {
                    if (target.nodeId != excludedNodeId) { scored.emplace_back(rendezvousScore(key, target.nodeId), target.nodeId); }
                }
                if (scored.size() < totalShards) { throw std::runtime_error("AkkEngine: no spare STRIPE node for owner failover"); }
                std::ranges::sort(
                    scored,
                    [](const auto& left, const auto& right) {
                        if (left.first != right.first) { return left.first > right.first; }
                        return left.second < right.second;
                    }
                );

                std::vector<uint64_t> ids;
                ids.reserve(totalPlacements);
                for (uint16_t i = 0; i < totalShards; ++i) { ids.push_back(scored[i].second); }
                return ids;
            }

            [[nodiscard]] uint64_t stripeOwnerFor(const cluster::ClusterConfig& config, std::span<const uint8_t> key) const {
                const auto ids = stripeNodeIdsFor(config, key);
                if (ids.empty()) { throw std::runtime_error("AkkEngine: STRIPE key has no owner"); }
                return ids.front();
            }

            void checkStripeHealthy() const {
                std::lock_guard lock{stripeStateMu};
                if (stripeFailure) { std::rethrow_exception(stripeFailure); }
            }

            void failStripe(std::exception_ptr failure) {
                std::lock_guard lock{stripeStateMu};
                if (!stripeFailure) { stripeFailure = std::move(failure); }
            }

            static std::vector<uint8_t> stripeTransactionKey(std::span<const uint8_t> key, uint64_t version) {
                // Intent and shard keys retain their generation-oriented format.
                std::vector<uint8_t> out{0, 'A', 'K', 'S', 'T', '1'};
                pushU32(out, static_cast<uint32_t>(key.size()));
                out.insert(out.end(), key.begin(), key.end());
                pushU64(out, version);
                return out;
            }

            uint64_t writeStripeLocal(
                std::span<const uint8_t> key, std::span<const uint8_t> value,
                cluster::ReplOpType op = cluster::ReplOpType::PUT,
                uint64_t seq = 0,
                uint64_t sourceNodeId = 0,
                uint8_t versionLogFlags = 0xFF
            ) {
                std::lock_guard lock{writeMu};
                try {
                    if (seq == 0) { seq = memtable->reserveSeq(1); }
                    const uint8_t flags = op == cluster::ReplOpType::REMOVE ? MemHdr16::FLAG_TOMBSTONE : MemHdr16::FLAG_NORMAL;
                    appendAll(
                        seq, key, value, flags, sourceNodeId == 0 ? nodeId : sourceNodeId,
                        0, 0, versionLogFlags, true
                    );
                    forceClusterLocalDurable();
                    return seq;
                }
                catch (...) {
                    failStripe(std::current_exception());
                    throw;
                }
            }

            void writeStripeRecordToNode(
                uint64_t targetNodeId, std::span<const uint8_t> key, std::span<const uint8_t> value,
                uint64_t version, cluster::ReplOpType op = cluster::ReplOpType::PUT,
                uint64_t logicalSourceNodeId = 0
            ) {
                cluster::detail::ReconfigurationDeadline::check();
                if (targetNodeId == nodeId) { writeStripeLocal(key, value, op); cluster::detail::ReconfigurationDeadline::check(); return; }
                if (!clusterRuntime) { throw std::runtime_error("AkkEngine: STRIPE write requires cluster runtime"); }
                const uint64_t sourceNodeId = logicalSourceNodeId == 0 ? nodeId : logicalSourceNodeId;
                const auto deadline = cluster::detail::ReconfigurationDeadline::cap(std::chrono::steady_clock::now() +
                    std::chrono::milliseconds{std::max<uint32_t>(1, clusterConfig.consistency().ackTimeoutMs)});
                while (true) {
                    try {
                        clusterRuntime->shipEntryTo(targetNodeId, version, op, key, value, MemHdr16::FLAG_NORMAL, sourceNodeId);
                        return;
                    }
                    catch (...) {
                        cluster::detail::ReconfigurationDeadline::check();
                        if (std::chrono::steady_clock::now() >= deadline) { throw; }
                        std::this_thread::sleep_for(std::chrono::milliseconds{25});
                    }
                }
            }

            [[nodiscard]] std::optional<std::vector<uint8_t>> readStripeRecordFromNode(uint64_t targetNodeId, std::span<const uint8_t> key) {
                if (targetNodeId == nodeId) { return getValueInternal(key, false); }
                if (!clusterRuntime) { throw std::runtime_error("AkkEngine: STRIPE read requires cluster runtime"); }
                const auto response = clusterRuntime->readKeyFromNode(targetNodeId, key, 0);
                if (response.status == cluster::ReadStatus::NOT_FOUND) { return std::nullopt; }
                if (response.status != cluster::ReadStatus::FOUND) {
                    throw std::runtime_error("AkkEngine: STRIPE node read failed: " + std::to_string(targetNodeId) + "/" +
                        std::to_string(static_cast<unsigned>(response.status)));
                }
                return response.value;
            }

            [[nodiscard]] std::optional<StripeMetadata> readStripeMetadata(std::span<const uint8_t> publicKey) {
                checkStripeHealthy();
                const auto owner = stripeOwnerFor(currentPlacement(), publicKey);
                if (!clusterRuntime) { throw std::runtime_error("AkkEngine: STRIPE metadata Raft runtime is unavailable"); }
                const auto value = clusterRuntime->readStripeMetadata(publicKey, owner);
                if (!value) { return std::nullopt; }
                auto metadata = decodeStripeMetadata(*value);
                const uint64_t failoverNode = clusterRuntime ? clusterRuntime->stripeFailoverNodeId() : 0;
                if (!metadata || metadata->version == 0 || metadata->ownerNodeId != owner || metadata->authorityNodeId == 0 ||
                    metadata->fenceToken == 0 || metadata->nodeIds.empty() || metadata->shardPresent.size() != metadata->nodeIds.size() ||
                    metadata->layout.dataShards != currentPlacement().stripe().dataShards ||
                    metadata->layout.parityShards != currentPlacement().stripe().parityShards ||
                    metadata->copiesPerShard != currentPlacement().stripe().copiesPerShard ||
                    (metadata->authorityNodeId != owner && metadata->authorityNodeId != failoverNode)) {
                    throw std::runtime_error("AkkEngine: corrupt STRIPE metadata");
                }
                const auto expectedNodes = stripeNodeIdsFor(
                    currentPlacement(), publicKey, metadata->authorityNodeId == owner ? 0 : owner
                );
                bool validPlacement = metadata->nodeIds == expectedNodes;
                if (!validPlacement && metadata->copiesPerShard == 2 && metadata->authorityNodeId == owner &&
                    metadata->nodeIds.size() == expectedNodes.size()) {
                    const auto placementView = currentPlacement();
                    const auto* spare = placementView.stripeFailoverNode();
                    size_t replacements = 0;
                    validPlacement = spare != nullptr;
                    for (size_t i = 0; validPlacement && i < expectedNodes.size(); ++i) {
                        if (metadata->nodeIds[i] == expectedNodes[i]) { continue; }
                        validPlacement = metadata->nodeIds[i] == spare->nodeId && ++replacements == 1;
                    }
                    validPlacement = validPlacement && replacements == 1;
                }
                if (!validPlacement) { throw std::runtime_error("AkkEngine: invalid STRIPE shard placement metadata"); }
                return metadata;
            }

            void repairStripeShards(
                std::span<const uint8_t> publicKey, const StripeMetadata& metadata,
                const std::vector<erasure::ErasureShard>& available, const std::vector<bool>& present
            ) {
                if (!opts.cluster.runtime.stripeReadRepair ||
                    (metadata.layout.parityShards == 0 && metadata.copiesPerShard == 1) ||
                    metadata.tombstone || metadata.authorityNodeId != nodeId) { return; }
                // Only the owner repairs, under the key lock, so repair cannot
                // resurrect a retired generation after the owner's GC.
                for (uint16_t placement = 0; placement < metadata.nodeIds.size(); ++placement) {
                    if (placement < present.size() && present[placement]) { continue; }
                    const uint16_t shardIndex = placement / metadata.copiesPerShard;
                    {
                        std::lock_guard lock{stripeRepairStatsMu};
                        ++stripeRepairStats.attempts;
                    }
                    try {
                        erasure::ErasureShard repaired;
                        if (metadata.copiesPerShard == 2) {
                            const auto source = std::ranges::find(available, shardIndex, &erasure::ErasureShard::index);
                            if (source == available.end()) { throw std::runtime_error("AkkEngine: missing RAID.10 mirror source"); }
                            repaired = *source;
                        }
                        else { repaired = erasure::RsErasureCodec::repairOne(shardIndex, available, metadata.layout); }
                        writeStripeRecordToNode(
                            metadata.nodeIds[placement], stripeShardKey(publicKey, metadata.version, shardIndex),
                            encodeStripeShard(metadata.version, repaired, metadata.layout), metadata.version
                        );
                        std::lock_guard lock{stripeRepairStatsMu};
                        ++stripeRepairStats.succeeded;
                    }
                    catch (...) {
                        std::lock_guard lock{stripeRepairStatsMu};
                        ++stripeRepairStats.failed;
                        stripeRepairStats.lastFailureNodeId = metadata.nodeIds[placement];
                    }
                }
            }

            struct StripeShardSet {
                std::vector<erasure::ErasureShard> available;
                std::vector<bool> present;
            };

            [[nodiscard]] StripeShardSet readStripeShards(
                std::span<const uint8_t> publicKey, const StripeMetadata& metadata,
                cluster::detail::BoundedExecutor* migrationExecutor = nullptr
            ) {
                StripeShardSet out;
                out.present.assign(metadata.nodeIds.size(), false);
                std::vector<std::optional<erasure::ErasureShard>> decoded(metadata.nodeIds.size());
                const auto budget = cluster::detail::ReconfigurationDeadline::current();
                auto* executor = migrationExecutor ? migrationExecutor : stripeExecutor.get();
                executor->forEach(metadata.nodeIds.size(), [&](size_t placement) {
                    cluster::detail::ReconfigurationDeadline scope{budget};
                    const uint16_t shardIndex = static_cast<uint16_t>(placement / metadata.copiesPerShard);
                    std::optional<std::vector<uint8_t>> stored;
                    try {
                        stored = readStripeRecordFromNode(
                            metadata.nodeIds[placement], stripeShardKey(publicKey, metadata.version, shardIndex));
                    }
                    catch (const std::runtime_error&) { return; }
                    if (!stored) { return; }
                    auto shard = decodeStripeShard(*stored, metadata.version, metadata.layout);
                    if (shard && shard->index == shardIndex && shard->originalSize == metadata.originalSize) {
                        decoded[placement] = std::move(shard);
                    }
                });
                std::vector<bool> logicalPresent(metadata.layout.totalShards(), false);
                for (size_t placement = 0; placement < decoded.size(); ++placement) {
                    if (!decoded[placement]) { continue; }
                    const auto index = decoded[placement]->index;
                    out.present[placement] = true;
                    if (!logicalPresent[index]) {
                        logicalPresent[index] = true;
                        out.available.push_back(std::move(*decoded[placement]));
                    }
                }
                return out;
            }

            [[nodiscard]] std::optional<std::vector<uint8_t>> readStripeValueFromMetadata(
                std::span<const uint8_t> publicKey, const StripeMetadata& metadata, bool repair,
                cluster::detail::BoundedExecutor* migrationExecutor = nullptr
            ) {
                if (metadata.tombstone) { return std::nullopt; }
                if (metadata.nodeIds.size() != metadata.layout.totalShards() * metadata.copiesPerShard) {
                    throw std::runtime_error("AkkEngine: corrupt STRIPE metadata");
                }
                auto shards = readStripeShards(publicKey, metadata, migrationExecutor);
                if (shards.available.size() < metadata.layout.dataShards) {
                    throw std::runtime_error("AkkEngine: not enough STRIPE shards to reconstruct value");
                }
                auto value = erasure::RsErasureCodec::decode(shards.available, metadata.layout);
                if (repair) { repairStripeShards(publicKey, metadata, shards.available, shards.present); }
                return value;
            }

            [[nodiscard]] std::optional<std::vector<uint8_t>> readStripeValue(std::span<const uint8_t> publicKey) {
                auto keyLock = stripeKeys.lock(publicKey);
                checkStripeHealthy();
                const uint64_t originalOwner = stripeOwnerFor(currentPlacement(), publicKey);
                const uint64_t failoverNode = clusterRuntime ? clusterRuntime->stripeFailoverNodeId() : 0;
                const bool coordinatedRead = clusterRuntime &&
                    (nodeId == originalOwner || (nodeId == failoverNode && !clusterRuntime->stripeNodeReachable(originalOwner)));
                auto lease = coordinatedRead
                                 ? clusterRuntime->acquireStripeOperation(publicKey, originalOwner)
                                 : cluster::StripeOperationLease{
                                       .ownerNodeId = originalOwner, .authorityNodeId = originalOwner,
                                       .fenceToken = 0, .coordinated = false, .metadata = {},
                                   };
                if (!lease.coordinated) {
                    for (unsigned attempt = 0; attempt < 8; ++attempt) {
                        const auto metadata = readStripeMetadata(publicKey);
                        if (!metadata || metadata->tombstone) { return std::nullopt; }
                        std::optional<std::vector<uint8_t>> value;
                        std::exception_ptr failure;
                        try { value = readStripeValueFromMetadata(publicKey, *metadata, metadata->authorityNodeId == nodeId); }
                        catch (...) { failure = std::current_exception(); }
                        const auto current = readStripeMetadata(publicKey);
                        if (!current || current->version != metadata->version) { continue; }
                        if (failure) { std::rethrow_exception(failure); }
                        return value;
                    }
                    throw std::runtime_error("AkkEngine: STRIPE generation changed repeatedly during read");
                }
                try {
                    auto metadata = lease.metadata.empty() ? std::optional<StripeMetadata>{} : decodeStripeMetadata(lease.metadata);
                    if (!lease.metadata.empty() && !metadata) {
                        throw std::runtime_error("AkkEngine: corrupt fenced STRIPE metadata");
                    }
                    if (metadata && metadata->ownerNodeId != originalOwner) {
                        throw std::runtime_error("AkkEngine: fenced STRIPE metadata owner mismatch");
                    }
                    if (!metadata || metadata->tombstone) {
                        if (clusterRuntime) { clusterRuntime->releaseStripeOperation(lease, publicKey); }
                        return std::nullopt;
                    }
                    auto value = readStripeValueFromMetadata(publicKey, *metadata, lease.authorityNodeId == nodeId);
                    if (clusterRuntime) { clusterRuntime->releaseStripeOperation(lease, publicKey); }
                    return value;
                }
                catch (...) {
                    if (clusterRuntime) { clusterRuntime->releaseStripeOperation(lease, publicKey); }
                    throw;
                }
            }

            struct StripeTransaction {
                StripeMetadata next;
                std::optional<StripeMetadata> previous;
            };

            static std::vector<uint8_t> encodeStripeTransaction(
                const StripeMetadata& next,
                const std::optional<StripeMetadata>& previous
            ) {
                std::vector<uint8_t> out{'A', 'K', 'S', 'T', '1'};
                const auto nextBytes = encodeStripeMetadata(next);
                const auto previousBytes = previous ? encodeStripeMetadata(*previous) : std::vector<uint8_t>{};
                pushU32(out, static_cast<uint32_t>(nextBytes.size()));
                out.insert(out.end(), nextBytes.begin(), nextBytes.end());
                pushU32(out, static_cast<uint32_t>(previousBytes.size()));
                out.insert(out.end(), previousBytes.begin(), previousBytes.end());
                return out;
            }

            static std::optional<StripeTransaction> decodeStripeTransaction(
                std::span<const uint8_t> bytes, uint64_t expectedVersion, uint64_t expectedAuthority
            ) {
                if (bytes.size() < 13 || std::memcmp(bytes.data(), "AKST1", 5) != 0) { return std::nullopt; }
                size_t cursor = 5;
                StripeTransaction out;
                for (int n = 0; n < 2; ++n) {
                    uint32_t length = 0;
                    if (!pullU32(bytes, cursor, length) || length > bytes.size() - cursor || (n == 0 && length == 0)) {
                        return std::nullopt;
                    }
                    if (length != 0) {
                        auto metadata = decodeStripeMetadata(bytes.subspan(cursor, length));
                        if (!metadata) { return std::nullopt; }
                        if (n == 0) { out.next = std::move(*metadata); }
                        else { out.previous = std::move(*metadata); }
                    }
                    cursor += length;
                }
                if (out.next.nodeIds.empty() || out.next.version != expectedVersion || out.next.authorityNodeId != expectedAuthority ||
                    cursor != bytes.size()) {
                    return std::nullopt;
                }
                return out;
            }

            void writeStripeValue(
                std::span<const uint8_t> publicKey,
                std::span<const uint8_t> value,
                bool tombstone = false,
                bool rollback = false
            ) {
                requireOwnsWriteKey(publicKey);
                auto keyLock = stripeKeys.lock(publicKey);
                checkStripeHealthy();
                const uint64_t originalOwner = stripeOwnerFor(currentPlacement(), publicKey);
                auto lease = clusterRuntime
                                 ? clusterRuntime->acquireStripeOperation(publicKey, originalOwner)
                                 : cluster::StripeOperationLease{
                                       .ownerNodeId = originalOwner, .authorityNodeId = originalOwner,
                                       .fenceToken = 0, .coordinated = false, .metadata = {},
                                   };
                try {
                    auto previous = lease.coordinated && !lease.metadata.empty()
                                        ? decodeStripeMetadata(lease.metadata)
                                        : readStripeMetadata(publicKey);
                    if (lease.coordinated && !lease.metadata.empty() && !previous) {
                        throw std::runtime_error("AkkEngine: corrupt fenced STRIPE metadata");
                    }
                    if (previous && previous->ownerNodeId != originalOwner) {
                        throw std::runtime_error("AkkEngine: fenced STRIPE metadata owner mismatch");
                    }
                    StripeMetadata next;
                    next.tombstone = tombstone;
                    next.rollback = rollback;
                    next.version = lease.coordinated ? lease.fenceToken : 0;
                    next.ownerNodeId = originalOwner;
                    next.authorityNodeId = lease.authorityNodeId;
                    next.originNodeId = lease.authorityNodeId;
                    next.timestampNs = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
                    next.fenceToken = lease.coordinated ? lease.fenceToken : 1;
                    next.originalSize = value.size();
                    next.layout = stripeLayoutFor(currentPlacement());
                    next.copiesPerShard = currentPlacement().stripe().copiesPerShard;
                    const uint64_t excludedOwner = lease.authorityNodeId == originalOwner ? 0 : originalOwner;
                    next.nodeIds = stripeNodeIdsFor(currentPlacement(), publicKey, excludedOwner);
                    if (next.copiesPerShard == 2 && previous && previous->nodeIds.size() == next.nodeIds.size()) {
                        const auto placementView = currentPlacement();
                        const auto* spare = placementView.stripeFailoverNode();
                        if (spare != nullptr && !std::ranges::contains(next.nodeIds, spare->nodeId)) {
                            const auto previousSpare = std::ranges::find(previous->nodeIds, spare->nodeId);
                            if (previousSpare != previous->nodeIds.end()) {
                                const auto position = static_cast<size_t>(std::distance(previous->nodeIds.begin(), previousSpare));
                                next.nodeIds[position] = spare->nodeId;
                            }
                        }
                    }
                    next.shardPresent.assign(next.nodeIds.size(), 0);
                    // The fencing token is also the immutable generation id.
                    // Local WAL ordering continues to use a local storage seq.
                    {
                        std::lock_guard lock{writeMu};
                        try {
                            const uint64_t intentSeq = memtable->reserveSeq(1);
                            if (next.version == 0) { next.version = intentSeq; }
                            const auto intentKey = stripeTransactionKey(publicKey, next.version);
                            const auto intentValue = encodeStripeTransaction(next, previous);
                            appendAll(intentSeq, intentKey, intentValue, MemHdr16::FLAG_NORMAL, nodeId, 0, 0, 0xFF, true);
                            forceClusterLocalDurable();
                            std::lock_guard stateLock{stripeStateMu};
                            stripePending[intentKey] = intentValue;
                        }
                        catch (...) { failStripe(std::current_exception()); throw; }
                    }
                    crashAtTestPoint("stripe.after_intent");
                    if (!tombstone) {
                        uint16_t durablePlacements = 0;
                        std::vector<uint8_t> durableCopies(next.layout.totalShards(), 0);
                        const auto shards = erasure::RsErasureCodec::encode(value, next.layout);
                        stripeExecutor->forEach(next.nodeIds.size(), [&](size_t placement) {
                            const auto shardIndex = static_cast<uint16_t>(placement / next.copiesPerShard);
                            const auto targetNodeId = next.nodeIds[placement];
                            try {
                                if (targetNodeId != nodeId && clusterRuntime && !clusterRuntime->stripeNodeReachable(targetNodeId)) { return; }
                                writeStripeRecordToNode(targetNodeId, stripeShardKey(publicKey, next.version, shardIndex),
                                    encodeStripeShard(next.version, shards[shardIndex], next.layout), next.version);
                                next.shardPresent[placement] = 1;
                                crashAtTestPoint("stripe.after_shard");
                            }
                            catch (const std::runtime_error&) {}
                        });
                        for (size_t placement = 0; placement < next.shardPresent.size(); ++placement) {
                            if (next.shardPresent[placement] != 0) {
                                ++durablePlacements;
                                ++durableCopies[placement / next.copiesPerShard];
                            }
                        }
                        bool sufficient = durablePlacements == next.nodeIds.size();
                        uint16_t requiredPlacements = static_cast<uint16_t>(next.nodeIds.size());
                        if (opts.cluster.runtime.stripeWriteCommitMode == cluster::StripeWriteCommitMode::DATA_SHARDS) {
                            if (next.copiesPerShard == 2) {
                                sufficient = std::ranges::all_of(durableCopies, [](uint8_t copies) { return copies != 0; });
                                requiredPlacements = next.layout.dataShards;
                            }
                            else if (next.layout.parityShards != 0) {
                                sufficient = durablePlacements >= next.layout.dataShards;
                                requiredPlacements = next.layout.dataShards;
                            }
                        }
                        if (!sufficient) {
                            throw std::runtime_error(
                                "AkkEngine: insufficient durable STRIPE placements (durable=" +
                                std::to_string(durablePlacements) + ", required=" + std::to_string(requiredPlacements) + ")"
                            );
                        }
                        if (durablePlacements < next.nodeIds.size()) {
                            std::lock_guard statsLock{stripeRebuildStatsMu};
                            stripeRebuildStats.active = true;
                        }
                    }
                    crashAtTestPoint("stripe.before_publish");
                    const auto encoded = encodeStripeMetadata(next);
                    if (lease.coordinated) { clusterRuntime->commitStripeMetadata(lease, publicKey, encoded); }
                    else { writeStripeLocal(stripeMetaKey(publicKey), encoded); }
                    crashAtTestPoint("stripe.after_publish");
                    if (tombstone) { removesTotal.fetch_add(1, std::memory_order_relaxed); }
                    else { putsTotal.fetch_add(1, std::memory_order_relaxed); }
                }
                catch (...) {
                    if (clusterRuntime) { clusterRuntime->releaseStripeOperation(lease, publicKey); }
                    throw;
                }
            }

            void removeStripeValue(std::span<const uint8_t> publicKey) { writeStripeValue(publicKey, {}, true); }

            bool stripePlacementProtected() const {
                if (!clusterRuntime) { return false; }
                const auto state = clusterRuntime->placementState(false);
                return state.pendingGeneration != 0 ||
                    stripeProtectionGeneration.load(std::memory_order_acquire) > state.lastIssuedGeneration;
            }

            void collectStripeGarbage() {
                if (stripePlacementProtected()) { return; }
                checkStripeHealthy();
                if (!stripeGcLoaded) {
                    std::lock_guard lock{writeMu};
                    core::BufferArena arena;
                    memtable::MemTable::KeyRange range;
                    const auto seq = snapshotSeq();
                    auto mt = memtable->iterator(range, seq);
                    sst::SSTManager::Iterator sst;
                    if (sstManager) { sst = sstManager->scanIter({}, {}, seq); }
                    for (const auto& record : scanGenerator(arena, std::move(mt), std::move(sst), blobManager.get())) {
                        if (isStripeInternalKey(record.key) && record.key[4] == 'T') {
                            std::lock_guard stateLock{stripeStateMu};
                            stripePending[std::vector<uint8_t>{record.key.begin(), record.key.end()}] =
                                std::vector<uint8_t>{record.value.begin(), record.value.end()};
                        }
                    }
                    stripeGcLoaded = true;
                }
                std::vector<uint8_t> previousIntent;
                for (;;) {
                    struct PendingView { std::vector<uint8_t> key; std::vector<uint8_t> value; };
                    PendingView item;
                    {
                        std::lock_guard stateLock{stripeStateMu};
                        const auto it = stripePending.upper_bound(previousIntent);
                        if (it == stripePending.end()) { break; }
                        item = {it->first, it->second};
                    }
                    previousIntent = item.key;
                    size_t cursor = 6;
                    uint32_t keySize = 0;
                    if (!pullU32(item.key, cursor, keySize) || item.key.size() - cursor < 8 ||
                        keySize != item.key.size() - cursor - 8) { throw std::runtime_error("AkkEngine: corrupt STRIPE intent key"); }
                    const auto publicKey = std::span<const uint8_t>{item.key.data() + cursor, keySize};
                    cursor += keySize;
                    uint64_t version = 0;
                    const uint64_t originalOwner = stripeOwnerFor(currentPlacement(), publicKey);
                    const uint64_t failoverNode = clusterRuntime ? clusterRuntime->stripeFailoverNodeId() : 0;
                    if (!pullU64(item.key, cursor, version)) {
                        throw std::runtime_error("AkkEngine: corrupt STRIPE intent version");
                    }
                    auto transaction = decodeStripeTransaction(item.value, version, nodeId);
                    if (!transaction) { throw std::runtime_error("AkkEngine: corrupt STRIPE intent"); }
                    // Placement can retire the writer while its valid intent is
                    // still awaiting cleanup. Its source storage stays retained.
                    if (originalOwner != nodeId && failoverNode != nodeId) { continue; }
                    auto keyLock = stripeKeys.lock(publicKey);
                    const auto current = readStripeMetadata(publicKey);
                    std::unordered_set<uint64_t> retainedGenerations;
                    if (versionLog) {
                        for (const auto& entry : versionLog->history(stripeMetaKey(publicKey))) {
                            if (auto metadata = decodeStripeMetadata(entry.value)) {
                                retainedGenerations.insert(metadata->version);
                            }
                        }
                    }
                    std::vector<StripeMetadata> candidates;
                    candidates.push_back(transaction->next);
                    if (transaction->previous) { candidates.push_back(*transaction->previous); }
                    std::unique_lock reclamationLock{stripeGcEpochMu};
                    if (stripeQueryPins->load(std::memory_order_acquire) != 0) { continue; }
                    bool complete = true;
                    for (const auto& candidate : candidates) {
                        if (candidate.tombstone || (current && candidate.version == current->version) ||
                            retainedGenerations.contains(candidate.version)) { continue; }
                        for (uint16_t placement = 0; placement < candidate.nodeIds.size(); ++placement) {
                            const uint16_t shardIndex = placement / candidate.copiesPerShard;
                            try {
                                writeStripeRecordToNode(
                                    candidate.nodeIds[placement], stripeShardKey(publicKey, candidate.version, shardIndex), {},
                                    candidate.version, cluster::ReplOpType::REMOVE
                                );
                            }
                            catch (...) { complete = false; }
                        }
                    }
                    if (complete) {
                        writeStripeLocal(item.key, {}, cluster::ReplOpType::REMOVE);
                        std::lock_guard stateLock{stripeStateMu};
                        stripePending.erase(item.key);
                    }
                }
            }

            void rebuildStripeShardsBatch() {
                if (stripePlacementProtected()) { return; }
                if (!opts.cluster.runtime.stripeAutoRebuild ||
                    (currentPlacement().stripe().parityShards == 0 && currentPlacement().stripe().copiesPerShard == 1)) { return; }
                struct Candidate {
                    std::vector<uint8_t> key;
                    StripeMetadata metadata;
                };
                std::vector<Candidate> candidates;
                candidates.reserve(opts.cluster.runtime.stripeRebuildBatchKeys);
                {
                    checkStripeHealthy();
                    {
                        std::lock_guard statsLock{stripeRebuildStatsMu};
                        ++stripeRebuildStats.cycles;
                        if (stripeRebuildCursor.empty()) { stripeRebuildStats.active = false; }
                    }
                    core::BufferArena arena;
                    memtable::MemTable::KeyRange range;
                    range.start = stripeRebuildCursor;
                    const uint64_t visibleSeq = snapshotSeq();
                    auto mt = memtable->iterator(range, visibleSeq);
                    sst::SSTManager::Iterator sst;
                    if (sstManager) { sst = sstManager->scanIter(range.start, {}, visibleSeq); }
                    std::vector<uint8_t> lastMetadataKey;
                    uint32_t metadataKeysVisited = 0;
                    for (const auto& record : scanGenerator(arena, std::move(mt), std::move(sst), blobManager.get())) {
                        std::vector<uint8_t> internalKey{record.key.begin(), record.key.end()};
                        if (!stripeRebuildCursor.empty() && internalKey <= stripeRebuildCursor) { continue; }
                        auto publicKey = publicKeyFromStripeMetaKey(record.key);
                        if (!publicKey) { continue; }
                        lastMetadataKey = std::move(internalKey);
                        ++metadataKeysVisited;
                        auto metadata = decodeStripeMetadata(record.value);
                        if (!metadata) { throw std::runtime_error("AkkEngine: corrupt STRIPE rebuild metadata"); }
                        *metadata = effectiveStripeMetadata(*publicKey, *metadata);
                        const bool localMetadataStore = clusterRuntime &&
                            clusterRuntime->stripeMetadataLeaderNodeId() == nodeId;
                        if (!metadata->tombstone &&
                            (metadata->layout.parityShards != 0 || metadata->copiesPerShard == 2) && localMetadataStore) {
                            candidates.push_back(Candidate{.key = std::move(*publicKey), .metadata = std::move(*metadata)});
                        }
                        if (metadataKeysVisited >= opts.cluster.runtime.stripeRebuildBatchKeys) { break; }
                    }
                    stripeRebuildCursor = metadataKeysVisited >= opts.cluster.runtime.stripeRebuildBatchKeys
                                              ? std::move(lastMetadataKey)
                                              : std::vector<uint8_t>{};
                    std::lock_guard statsLock{stripeRebuildStatsMu};
                    stripeRebuildStats.keysScanned += metadataKeysVisited;
                }

                for (auto& candidate : candidates) {
                    auto keyLock = stripeKeys.lock(candidate.key);
                    checkStripeHealthy();
                    const auto latestBytes = getValueInternal(stripeMetaKey(candidate.key), false);
                    auto latest = latestBytes ? decodeStripeMetadata(*latestBytes) : std::nullopt;
                    if (latest) { *latest = effectiveStripeMetadata(candidate.key, *latest); }
                    if (!latest || latest->version != candidate.metadata.version ||
                        latest->authorityNodeId != candidate.metadata.authorityNodeId) { continue; }
                    auto shards = readStripeShards(candidate.key, candidate.metadata);
                    bool missing = false;
                    std::vector<uint8_t> observed(candidate.metadata.nodeIds.size(), 0);
                    for (uint16_t i = 0; i < observed.size(); ++i) {
                        if (i < shards.present.size() && shards.present[i]) { observed[i] = 1; }
                        else { missing = true; }
                    }
                    if (missing) {
                        std::lock_guard statsLock{stripeRebuildStatsMu};
                        stripeRebuildStats.active = true;
                    }
                    for (uint16_t placement = 0; placement < observed.size(); ++placement) {
                        if (observed[placement] != 0) { continue; }
                        const uint16_t shardIndex = placement / candidate.metadata.copiesPerShard;
                        uint64_t targetNodeId = candidate.metadata.nodeIds[placement];
                        if (candidate.metadata.copiesPerShard == 2) {
                            const auto placementView = currentPlacement();
                            const auto* spare = placementView.stripeFailoverNode();
                            const bool targetReachable = targetNodeId == nodeId ||
                                (clusterRuntime && clusterRuntime->stripeNodeReachable(targetNodeId));
                            if (!targetReachable && spare != nullptr &&
                                !std::ranges::contains(candidate.metadata.nodeIds, spare->nodeId) &&
                                (spare->nodeId == nodeId || (clusterRuntime && clusterRuntime->stripeNodeReachable(spare->nodeId)))) {
                                targetNodeId = spare->nodeId;
                                candidate.metadata.nodeIds[placement] = targetNodeId;
                            }
                        }
                        if (targetNodeId != nodeId && clusterRuntime && !clusterRuntime->stripeNodeReachable(targetNodeId)) { continue; }
                        {
                            std::lock_guard statsLock{stripeRebuildStatsMu};
                            ++stripeRebuildStats.attempts;
                        }
                        try {
                            if (shards.available.size() < candidate.metadata.layout.dataShards) {
                                throw std::runtime_error("AkkEngine: insufficient STRIPE shards for rebuild");
                            }
                            erasure::ErasureShard repaired;
                            if (candidate.metadata.copiesPerShard == 2) {
                                const auto source = std::ranges::find(
                                    shards.available, shardIndex, &erasure::ErasureShard::index
                                );
                                if (source == shards.available.end()) {
                                    throw std::runtime_error("AkkEngine: missing RAID.10 mirror source for rebuild");
                                }
                                repaired = *source;
                            }
                            else {
                                repaired = erasure::RsErasureCodec::repairOne(
                                    shardIndex, shards.available, candidate.metadata.layout
                                );
                            }
                            writeStripeRecordToNode(
                                targetNodeId, stripeShardKey(candidate.key, candidate.metadata.version, shardIndex),
                                encodeStripeShard(candidate.metadata.version, repaired, candidate.metadata.layout),
                                candidate.metadata.version, cluster::ReplOpType::PUT, candidate.metadata.authorityNodeId
                            );
                            observed[placement] = 1;
                            shards.present[placement] = true;
                            if (candidate.metadata.copiesPerShard == 1) {
                                shards.available.push_back(std::move(repaired));
                            }
                            std::lock_guard statsLock{stripeRebuildStatsMu};
                            ++stripeRebuildStats.succeeded;
                        }
                        catch (...) {
                            std::lock_guard statsLock{stripeRebuildStatsMu};
                            ++stripeRebuildStats.failed;
                            stripeRebuildStats.lastFailureNodeId = targetNodeId;
                        }
                    }

                    if (observed == candidate.metadata.shardPresent) { continue; }
                    checkStripeHealthy();
                    const auto currentBytes = getValueInternal(stripeMetaKey(candidate.key), false);
                    const auto current = currentBytes ? decodeStripeMetadata(*currentBytes) : std::nullopt;
                    if (!current || current->version != candidate.metadata.version ||
                        current->authorityNodeId != candidate.metadata.authorityNodeId) { continue; }
                    candidate.metadata.shardPresent = std::move(observed);
                    const auto encoded = encodeStripeMetadata(candidate.metadata);
                    (void)clusterRuntime->repairStripeMetadata(candidate.key, candidate.metadata.version, encoded);
                }
            }

            void startStripeGarbageCollector() {
                if (clusterReplicationMode != cluster::ReplicationMode::STRIPE) { return; }
                stripeGcThread = std::jthread([this](std::stop_token stop) {
                    auto nextGarbageCollection = std::chrono::steady_clock::now();
                    auto nextRebuild = std::chrono::steady_clock::now();
                    while (!stop.stop_requested()) {
                        try { collectStripeGarbage(); }
                        catch (const cluster::StripeMetadataUnavailable&) {
                            std::this_thread::sleep_for(std::chrono::milliseconds{50});
                            continue;
                        }
                        catch (...) {
                            failStripe(std::current_exception());
                            return;
                        }
                        const auto now = std::chrono::steady_clock::now();
                        if (now >= nextRebuild) {
                            try { rebuildStripeShardsBatch(); }
                            catch (const cluster::StripeMetadataUnavailable&) {
                                nextRebuild = now + std::chrono::milliseconds{opts.cluster.runtime.stripeRebuildIntervalMs};
                                continue;
                            }
                            catch (...) {
                                failStripe(std::current_exception());
                                return;
                            }
                            nextRebuild = now + std::chrono::milliseconds{opts.cluster.runtime.stripeRebuildIntervalMs};
                        }
                        nextGarbageCollection = now + std::chrono::milliseconds{500};
                        while (!stop.stop_requested() && std::chrono::steady_clock::now() < nextGarbageCollection) {
                            std::this_thread::sleep_for(std::chrono::milliseconds{50});
                        }
                    }
                });
            }

            void reconfigureCluster(cluster::ClusterConfig config) {
                reconfigurePlacement(std::move(config));
            }

            [[nodiscard]] cluster::ReadResponse readLocalForCluster(std::span<const uint8_t> key, uint64_t requestedSnapshot) {
                if (usesPartitionRaft()) {
                    auto* child = localPartition(key);
                    if (!child) {
                        cluster::ReadResponse response;
                        response.status = cluster::ReadStatus::NOT_FOUND;
                        return response;
                    }
                    if (opts.cluster.runtime.readMode == cluster::ClusterReadMode::OWNER_LINEARIZABLE) {
                        return child->impl_->clusterRuntime->linearizableReadKey(key);
                    }
                    return child->impl_->readLocalForCluster(key, requestedSnapshot);
                }
                if (clusterReplicationMode == cluster::ReplicationMode::STRIPE && isStripeInternalKey(key) && key[4] == 'M') {
                    checkStripeHealthy();
                    const auto publicKey = publicKeyFromStripeMetaKey(key);
                    const uint64_t metadataAuthority = clusterRuntime ? clusterRuntime->stripeMetadataLeaderNodeId() : 0;
                    if (!publicKey || metadataAuthority != nodeId) {
                        throw std::runtime_error("AkkEngine: STRIPE metadata must be read from its authority node");
                    }
                }
                if (clusterReplicationMode == cluster::ReplicationMode::STRIPE && !isStripeInternalKey(key)) {
                    cluster::ReadResponse response;
                    const auto value = readStripeValue(key);
                    response.seq = snapshotSeq();
                    if (!value) {
                        response.status = cluster::ReadStatus::NOT_FOUND;
                        return response;
                    }
                    response.status = cluster::ReadStatus::FOUND;
                    response.value = *value;
                    return response;
                }
                const auto snapshot = requestedSnapshot == 0 ? std::optional<uint64_t>{} : std::optional<uint64_t>{requestedSnapshot};
                const auto value = getValueInternal(key, false, snapshot);
                cluster::ReadResponse response;
                response.seq = requestedSnapshot == 0 ? snapshotSeq() : requestedSnapshot;
                if (!value) {
                    response.status = cluster::ReadStatus::NOT_FOUND;
                    return response;
                }
                response.status = cluster::ReadStatus::FOUND;
                response.value = *value;
                return response;
            }

            [[nodiscard]] bool clusterReadMustRoute() const noexcept {
                return clusterRuntime && (clusterReplicationMode == cluster::ReplicationMode::STRIPE ||
                                          opts.cluster.runtime.readMode != cluster::ClusterReadMode::LOCAL_STALE_OK);
            }

            void requireClusterDisabledOperation(std::string_view operation) const {
                if (clusterRuntime) {
                    throw std::runtime_error(
                        "AkkEngine: " + std::string{operation} + " is not supported while cluster runtime is enabled"
                    );
                }
            }

            [[nodiscard]] std::optional<std::vector<uint8_t>> getValueClusterAware(std::span<const uint8_t> key, bool countStats = true) {
                if (!clusterReadMustRoute()) { return getValueInternal(key, countStats); }
                const bool localStripeCoordinator = clusterReplicationMode == cluster::ReplicationMode::STRIPE &&
                    opts.cluster.runtime.stripeReadCoordinatorMode == cluster::StripeReadCoordinatorMode::LOCAL_COORDINATOR &&
                    opts.cluster.runtime.readMode != cluster::ClusterReadMode::OWNER_ONLY;
                if (!localStripeCoordinator && !(clusterReplicationMode == cluster::ReplicationMode::STRIPE && isStripeInternalKey(key))) {
                    if (auto forwarded = tryForwardPoint(cluster::ForwardOperation::GET, key, {}, {},
                        opts.cluster.runtime.readMode != cluster::ClusterReadMode::OWNER_ONLY)) {
                        if (countStats) {
                            getsTotal.fetch_add(1, std::memory_order_relaxed);
                            if (!forwarded->found) { getsMiss.fetch_add(1, std::memory_order_relaxed); }
                        }
                        if (!forwarded->found) { return std::nullopt; }
                        return std::move(forwarded->value);
                    }
                }
                if (countStats) { getsTotal.fetch_add(1, std::memory_order_relaxed); }
                if (clusterReplicationMode == cluster::ReplicationMode::STRIPE) {
                    if (isStripeInternalKey(key)) { return getValueInternal(key, false); }
                    if (opts.cluster.runtime.readMode == cluster::ClusterReadMode::OWNER_ONLY && !clusterRuntime->ownsWriteKey(key)) {
                        throw std::runtime_error("AkkEngine: cluster owner read failed");
                    }
                    std::optional<std::vector<uint8_t>> value;
                    if (opts.cluster.runtime.stripeReadCoordinatorMode == cluster::StripeReadCoordinatorMode::LOCAL_COORDINATOR) {
                        value = readStripeValue(key);
                    }
                    else {
                        const auto response = clusterRuntime->readKey(key, 0);
                        if (response.status == cluster::ReadStatus::FOUND) { value = response.value; }
                        else if (response.status != cluster::ReadStatus::NOT_FOUND) {
                            throw std::runtime_error("AkkEngine: cluster owner read failed");
                        }
                    }
                    if (!value) {
                        if (countStats) { getsMiss.fetch_add(1, std::memory_order_relaxed); }
                        return std::nullopt;
                    }
                    if (countStats) { getsMemtableHit.fetch_add(1, std::memory_order_relaxed); }
                    return value;
                }
                const auto response = clusterRuntime->readKey(key, 0);
                if (response.status == cluster::ReadStatus::NOT_FOUND) {
                    if (countStats) { getsMiss.fetch_add(1, std::memory_order_relaxed); }
                    return std::nullopt;
                }
                if (response.status != cluster::ReadStatus::FOUND) {
                    throw std::runtime_error("AkkEngine: cluster owner read failed");
                }
                if (countStats) { getsMemtableHit.fetch_add(1, std::memory_order_relaxed); }
                return response.value;
            }

            [[nodiscard]] bool getIntoClusterAware(std::span<const uint8_t> key, std::vector<uint8_t>& out) {
                auto value = getValueClusterAware(key);
                if (!value) { return false; }
                out = std::move(*value);
                return true;
            }

            void requireOwnsWriteKey(std::span<const uint8_t> key) const {
                if (clusterRuntime && !clusterRuntime->ownsWriteKey(key)) {
                    throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::NOT_OWNER, clusterRuntime->routeTarget(key));
                }
            }

            template <typename EntryRange>
            [[nodiscard]] std::optional<cluster::ClusterRouteTarget> resolveForwardEntries(const EntryRange& entries, bool allowForward, bool queryRequest = false) {
                if (!clusterRuntime) { return std::nullopt; }
                cluster::ClusterRouteTarget target;
                uint64_t destination = 0;
                const auto inspect = [&](std::span<const uint8_t> key) {
                    const bool local = clusterRuntime->ownsWriteKey(key);
                    auto candidate = clusterRuntime->routeTarget(key);
                    const uint64_t owner = local ? nodeId : candidate.nodeId;
                    if (owner == 0 || (!local && owner == nodeId)) {
                        throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::NO_TARGET, candidate, "No writable owner is currently available");
                    }
                    if (destination != 0 && destination != owner) {
                        throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::CROSS_OWNER_BATCH, {},
                            "Batch spans multiple owners; no entries were applied and the batch was not split");
                    }
                    destination = owner; target = std::move(candidate);
                };
                if (queryRequest) { inspect({}); }
                else { for (const auto& entry : entries) { inspect(entry.key); } }
                if (destination == 0 || destination == nodeId) { return std::nullopt; }
                if (allowForward && opts.cluster.runtime.routingMode == cluster::ClusterRoutingMode::LOCAL_ONLY) {
                    throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::LOCAL_ONLY, {}, "This node accepts local-owner operations only");
                }
                if (!allowForward || opts.cluster.runtime.routingMode != cluster::ClusterRoutingMode::FORWARD) {
                    throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::NOT_OWNER, target);
                }
                return target;
            }

            [[nodiscard]] std::optional<cluster::ClusterRouteTarget> resolveForwardTarget(const cluster::ForwardRequest& request, bool allowForward) {
                return resolveForwardEntries(request.entries, allowForward, request.operation == cluster::ForwardOperation::QUERY_REQUEST && !usesPartitionRaft());
            }

            [[nodiscard]] std::optional<cluster::ForwardResponse> tryForward(cluster::ForwardRequest request, bool allowForward = true) {
                const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(opts.cluster.runtime.forwardingTimeoutMs);
                const auto resolved = resolveForwardTarget(request, allowForward);
                if (!resolved) { return std::nullopt; }
                const auto& target = *resolved;
                if (request.entries.size() > 65536) { throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::PAYLOAD_TOO_LARGE, target); }
                size_t payloadSize = cluster::FORWARD_REQUEST_BASE_SIZE;
                for (const auto& entry : request.entries) {
                    if (entry.key.size() > cluster::MAX_FORWARD_PAYLOAD || entry.value.size() > cluster::MAX_FORWARD_PAYLOAD) {
                        throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::PAYLOAD_TOO_LARGE, target);
                    }
                    payloadSize += 8 + entry.key.size() + entry.value.size();
                    if (payloadSize > cluster::MAX_FORWARD_PAYLOAD) { throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::PAYLOAD_TOO_LARGE, target); }
                }
                const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now()).count();
                if (remaining <= 0) { throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::FORWARD_UNAVAILABLE, target); }
                request.timeoutMs = static_cast<uint32_t>(remaining);
                const std::optional<uint16_t> partition = usesPartitionRaft() && !request.entries.empty()
                    ? std::optional{partitionIndex(request.entries.front().key)} : std::nullopt;
                try {
                    auto response = clusterRuntime->forwardTo(target.nodeId, std::move(request));
                    if (!response.success) { throw cluster::ClusterRoutingError(response.errorCode,
                        response.target.nodeId == 0 ? target : response.target, response.message.empty() ? "Forward operation failed" : response.message); }
                    return response;
                }
                catch (cluster::ClusterRoutingError& error) {
                    if (partition) {
                        auto& hint = partitionLeaderHints[*partition];
                        if (error.code == cluster::ClusterRoutingCode::NOT_OWNER) { hint.store(error.target.nodeId, std::memory_order_release); }
                        else if (error.code == cluster::ClusterRoutingCode::FORWARD_UNAVAILABLE) { hint.store(0, std::memory_order_release); }
                    }
                    if (error.target.nodeId == 0) { error.target = target; }
                    throw;
                }
            }

            [[nodiscard]] std::optional<cluster::ForwardResponse> tryForwardPoint(cluster::ForwardOperation operation,
                std::span<const uint8_t> key, std::span<const uint8_t> value = {},
                const cluster::ClusterRequestId& requestId = {}, bool allowForward = true) {
                if (!clusterRuntime || clusterRuntime->ownsWriteKey(key)) { return std::nullopt; }
                if (usesPartitionRaft() && opts.cluster.runtime.routingMode == cluster::ClusterRoutingMode::FORWARD &&
                    currentPartitionLeader(key) == 0) {
                    const auto index = partitionIndex(key);
                    const auto placement = livePlacement.load();
                    const auto response = partitionRpcLeader(index, partitionAdminHeader(PartitionAdmin::STATUS, index, 0), *placement,
                        std::chrono::steady_clock::now() + std::chrono::milliseconds{opts.cluster.runtime.forwardingTimeoutMs});
                    partitionLeaderHints[index].store(response.target.nodeId, std::memory_order_release);
                }
                const bool forwarding = allowForward && opts.cluster.runtime.routingMode == cluster::ClusterRoutingMode::FORWARD;
                if (forwarding && (key.size() > cluster::MAX_FORWARD_PAYLOAD || value.size() > cluster::MAX_FORWARD_PAYLOAD ||
                    key.size() + value.size() + cluster::FORWARD_REQUEST_BASE_SIZE + 8 > cluster::MAX_FORWARD_PAYLOAD)) {
                    throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::PAYLOAD_TOO_LARGE, clusterRuntime->routeTarget(key));
                }
                cluster::ForwardRequest request;
                request.operation = operation; request.deduplicationId = requestId;
                request.entries.push_back({{key.begin(), key.end()}, {}});
                if (forwarding) { request.entries.back().value.assign(value.begin(), value.end()); }
                return tryForward(std::move(request), allowForward);
            }

            cluster::ForwardResponse receiveForward(const cluster::ForwardRequest& request) {
                OperationGuard operation{*this};
                throwIfBackgroundFailed();
                // Runtime starts before coordinator installation. Startup/close
                // never acknowledges a forwarded operation that it did not run.
                if (!forwardReady.load(std::memory_order_acquire)) { throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::FORWARD_UNAVAILABLE); }
                const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(request.timeoutMs);
                waitForVersionLogRecovery();
                if (std::chrono::steady_clock::now() >= deadline) { throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::FORWARD_UNAVAILABLE); }
                if (request.operation == cluster::ForwardOperation::READ_QUERY) { return receiveReadQuery(request); }
                if (request.operation == cluster::ForwardOperation::CLUSTER_ADMIN) { return receivePartitionAdmin(request); }
                // Destination revalidates every key before taking a fencing grant
                // or mutating storage. It never follows another redirect.
                (void)resolveForwardTarget(request, false);
                cluster::ForwardResponse response;
                response.success = true;
                if (request.operation == cluster::ForwardOperation::QUERY_REQUEST) {
                    response.requestResult = usesPartitionRaft()
                        ? requireLocalPartition(request.entries.front().key).impl_->clusterRuntime->queryRequest(request.deduplicationId)
                        : clusterRuntime->queryRequest(request.deduplicationId);
                }
                else if (request.operation == cluster::ForwardOperation::GET) {
                    const auto result = clusterRuntime->linearizableReadKey(request.entries.front().key);
                    if (result.status == cluster::ReadStatus::ERROR_STATUS) { throw std::runtime_error("Forwarded owner read failed"); }
                    response.found = result.status == cluster::ReadStatus::FOUND; response.value = result.value;
                }
                else {
                    std::shared_lock epochLock{mutationEpochMu};
                    if (std::chrono::steady_clock::now() >= deadline) { throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::FORWARD_UNAVAILABLE); }
                    if (request.operation == cluster::ForwardOperation::PUT_BATCH) {
                        std::vector<BatchPutEntry> entries;
                        for (const auto& entry : request.entries) { entries.push_back({entry.key, entry.value}); }
                        writeCoordinator->putBatch(entries);
                    }
                    else {
                        const auto& entry = request.entries.front();
                        switch (request.operation) {
                            case cluster::ForwardOperation::PUT: writeCoordinator->put(entry.key, entry.value); break;
                            case cluster::ForwardOperation::REMOVE: writeCoordinator->remove(entry.key); break;
                            case cluster::ForwardOperation::PUT_REQUEST:
                            case cluster::ForwardOperation::REMOVE_REQUEST:
                                response.requestResult = writeCoordinator->writeWithRequest(request.deduplicationId,
                                    request.operation == cluster::ForwardOperation::PUT_REQUEST ? cluster::ReplOpType::PUT : cluster::ReplOpType::REMOVE,
                                    entry.key, entry.value); break;
                            default: throw std::invalid_argument("Unsupported forward operation");
                        }
                    }
                }
                return response;
            }

            #include "detail/ClusterQueries.inc"
            #include "detail/Transactions.inc"

            void requireOwnsWriteBatch(std::span<const BatchPutEntry> entries) const {
                for (const auto& entry : entries) { requireOwnsWriteKey(entry.key); }
            }

            [[nodiscard]] bool getIntoInternal(
                std::span<const uint8_t> key,
                std::vector<uint8_t>& out,
                bool countStats,
                std::optional<uint64_t> requestedSnapshot = std::nullopt
            ) {
                if (usesPartitionRaft()) {
                    auto* child = localPartition(key);
                    return child && child->impl_->getIntoInternal(key, out, countStats, requestedSnapshot);
                }
                throwIfBackgroundFailed();
                if (!requestedSnapshot.has_value()) { waitForKeySequenceReadVisibility(); }
                if (blobManager) {
                    auto value = getValueInternal(key, countStats, requestedSnapshot);
                    if (!value) { return false; }
                    out = std::move(*value);
                    return true;
                }

                if (countStats) { getsTotal.fetch_add(1, std::memory_order_relaxed); }
                const uint64_t seq = requestedSnapshot.value_or(snapshotSeq());
                if (const auto mt = memtable->getInto(key, seq, out); mt.has_value()) {
                    if (*mt) { getsMemtableHit.fetch_add(1, std::memory_order_relaxed); }
                    else { getsMiss.fetch_add(1, std::memory_order_relaxed); }
                    return *mt;
                }
                if (sstManager) {
                    if (const auto sst = sstManager->getInto(key, out, seq); sst.has_value()) {
                        if (*sst) {
                            if (opts.runtime.sstPromoteReads) {
                                if (auto record = sstManager->get(key, seq); record && !record->isTombstone()) {
                                    memtable->put(record->key, record->value, record->seq, record->flags, record->keyFp64, record->miniKey);
                                }
                            }
                            getsSstHit.fetch_add(1, std::memory_order_relaxed);
                        }
                        else { getsMiss.fetch_add(1, std::memory_order_relaxed); }
                        return *sst;
                    }
                }
                getsMiss.fetch_add(1, std::memory_order_relaxed);
                return false;
            }

            [[nodiscard]] bool getIntoArenaInternal(
                std::span<const uint8_t> key,
                core::BufferArena& arena,
                std::span<const uint8_t>& out,
                bool countStats
            ) {
                if (usesPartitionRaft()) {
                    auto* child = localPartition(key);
                    out = {};
                    return child && child->impl_->getIntoArenaInternal(key, arena, out, countStats);
                }
                throwIfBackgroundFailed();
                waitForKeySequenceReadVisibility();
                out = {};
                if (blobManager) {
                    auto value = getValueInternal(key, countStats);
                    if (!value) { return false; }
                    out = copySpanToArena(*value, arena);
                    return true;
                }

                if (countStats) { getsTotal.fetch_add(1, std::memory_order_relaxed); }
                const uint64_t seq = snapshotSeq();
                auto pinned = memtable->getPinned(key, seq);
                if (pinned) {
                    const auto& view = pinned->view;
                    if (view.isTombstone()) {
                        getsMiss.fetch_add(1, std::memory_order_relaxed);
                        return false;
                    }
                    out = copySpanToArena(view.value(), arena);
                    getsMemtableHit.fetch_add(1, std::memory_order_relaxed);
                    return true;
                }

                if (sstManager) {
                    auto record = sstManager->get(key, seq);
                    if (record) {
                        if (record->isTombstone()) {
                            getsMiss.fetch_add(1, std::memory_order_relaxed);
                            return false;
                        }
                        if (opts.runtime.sstPromoteReads && memtable) {
                            memtable->put(record->key, record->value, record->seq, record->flags, record->keyFp64, record->miniKey);
                        }
                        out = copySpanToArena(record->value, arena);
                        getsSstHit.fetch_add(1, std::memory_order_relaxed);
                        return true;
                    }
                }
                getsMiss.fetch_add(1, std::memory_order_relaxed);
                return false;
            }

            [[nodiscard]] bool canRunBlobGc() const noexcept { return blobManager != nullptr && versionLog == nullptr && !transactionJournalPending.load(std::memory_order_acquire); }

            void waitForVersionLogRecovery() const { if (versionLog) { versionLog->waitUntilReady(); } }

            [[nodiscard]] bool canUseParallelWriteAdmission() const noexcept {
                const bool supported = !walWriter && !blobManager && !clusterRuntime;
                const bool explicitSupported = !blobManager && !clusterRuntime;
                switch (opts.runtime.writeAdmission) {
                    case AkkEngineOptions::WriteAdmissionMode::SERIAL: return false;
                    case AkkEngineOptions::WriteAdmissionMode::PARALLEL: return explicitSupported;
                    case AkkEngineOptions::WriteAdmissionMode::AUTO: return opts.runtime.relaxedConcurrentWrites && supported;
                }
                return false;
            }

            [[nodiscard]] bool usesKeySequenceOrder() const noexcept {
                return canUseParallelWriteAdmission() && opts.runtime.parallelWriteOrder ==
                    AkkEngineOptions::ParallelWriteOrderMode::KEY_SEQUENCE;
            }

            [[nodiscard]] std::mutex& keySequenceOrderMutex(uint64_t fp64, size_t keySize) noexcept {
                const uint64_t route = fp64 ^ (static_cast<uint64_t>(keySize) * 0x9E3779B97F4A7C15ULL);
                return (*keySequenceOrderMu)[static_cast<size_t>(route) & (KEY_SEQUENCE_ORDER_STRIPES - 1u)];
            }

            [[nodiscard]] std::vector<std::unique_lock<std::mutex>> lockKeySequenceOrderStripes(std::span<const BatchPutEntry> entries) {
                std::vector<size_t> stripes;
                stripes.reserve(entries.size());
                for (const auto& entry : entries) {
                    const uint64_t fp64 = entry.key.empty() ? 0 : core::computeKeyFp64(entry.key.data(), entry.key.size());
                    const uint64_t route = fp64 ^ (static_cast<uint64_t>(entry.key.size()) * 0x9E3779B97F4A7C15ULL);
                    stripes.push_back(static_cast<size_t>(route) & (KEY_SEQUENCE_ORDER_STRIPES - 1u));
                }
                std::ranges::sort(stripes);
                stripes.erase(std::unique(stripes.begin(), stripes.end()), stripes.end());

                std::vector<std::unique_lock<std::mutex>> locks;
                locks.reserve(stripes.size());
                for (const size_t stripe : stripes) { locks.emplace_back((*keySequenceOrderMu)[stripe]); }
                return locks;
            }

            [[nodiscard]] bool memtableBackpressureActive() const noexcept {
                const uint32_t limit = opts.runtime.backpressure.maxMemtableImmutableTables;
                if (limit == 0 || !memtable) { return false; }
                return memtable->snapshot().immutableTables >= limit;
            }

            [[nodiscard]] bool sstBackpressureActive() const {
                const uint32_t limit = opts.runtime.backpressure.maxSstL0Files;
                if (limit == 0 || !sstManager) { return false; }
                const auto levels = sstManager->levelStats();
                for (const auto& level : levels) { if (level.level == 0) { return level.fileCount >= limit; } }
                return false;
            }

            void recordBackpressureWait(std::chrono::steady_clock::duration elapsed) const noexcept {
                const uint64_t micros = static_cast<uint64_t>(std::max<int64_t>(
                    0,
                    std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count()
                ));
                backpressureWaitMicrosTotal.fetch_add(micros, std::memory_order_relaxed);
                uint64_t observed = backpressureWaitMicrosMax.load(std::memory_order_relaxed);
                while (observed < micros && !backpressureWaitMicrosMax.compare_exchange_weak(
                    observed,
                    micros,
                    std::memory_order_relaxed,
                    std::memory_order_relaxed
                )) {}
            }

            [[noreturn]] void rejectBackpressuredWrite(const char* reason, bool timedOut) const {
                backpressureRejectedWrites.fetch_add(1, std::memory_order_relaxed);
                if (timedOut) { backpressureTimedOutWrites.fetch_add(1, std::memory_order_relaxed); }
                std::string message = "AkkEngine: write backpressure ";
                message += timedOut ? "timed out: " : "rejected write: ";
                message += reason;
                throw std::runtime_error(message);
            }

            void applyWriteBackpressure() const {
                if (!writeBackpressureEnabled) {
                    if (closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
                    throwIfBackgroundFailed();
                    if (memtable) { memtable->throwIfFlushFailed(); }
                    return;
                }
                const auto started = std::chrono::steady_clock::now();
                const auto timeout = std::chrono::milliseconds{opts.runtime.backpressure.timeoutMs};
                bool waited = false;
                bool countedMemtable = false;
                bool countedSst = false;
                while (true) {
                    if (closed.load(std::memory_order_acquire)) {
                        if (waited) { recordBackpressureWait(std::chrono::steady_clock::now() - started); }
                        throw std::runtime_error("AkkEngine: engine is closed");
                    }
                    throwIfBackgroundFailed();
                    if (memtable) { memtable->throwIfFlushFailed(); }

                    AkkEngineOptions::BackpressureMode mode;
                    const char* reason = nullptr;
                    if (memtableBackpressureActive()) {
                        mode = opts.runtime.backpressure.memtableFlushBacklog;
                        reason = "memtable immutable-table backlog";
                        if (!countedMemtable) {
                            backpressureMemtableStalls.fetch_add(1, std::memory_order_relaxed);
                            countedMemtable = true;
                        }
                    }
                    else if (sstBackpressureActive()) {
                        mode = opts.runtime.backpressure.sstCompactionBacklog;
                        reason = "sst L0 compaction backlog";
                        if (!countedSst) {
                            backpressureSstStalls.fetch_add(1, std::memory_order_relaxed);
                            countedSst = true;
                        }
                    }
                    else {
                        if (waited) { recordBackpressureWait(std::chrono::steady_clock::now() - started); }
                        return;
                    }

                    if (mode == AkkEngineOptions::BackpressureMode::FAIL_FAST) { rejectBackpressuredWrite(reason, false); }
                    if (!waited) {
                        backpressureBlockedWrites.fetch_add(1, std::memory_order_relaxed);
                        waited = true;
                    }

                    const auto elapsed = std::chrono::steady_clock::now() - started;
                    if (opts.runtime.backpressure.timeoutMs != 0 && elapsed >= timeout) {
                        recordBackpressureWait(elapsed);
                        rejectBackpressuredWrite(reason, true);
                    }

                    auto wait = std::chrono::microseconds{std::max<uint32_t>(1, opts.runtime.backpressure.waitMicros)};
                    if (opts.runtime.backpressure.timeoutMs != 0) {
                        const auto remaining = std::chrono::duration_cast<std::chrono::microseconds>(timeout - elapsed);
                        wait = std::min(wait, std::max(std::chrono::microseconds{1}, remaining));
                    }
                    std::this_thread::sleep_for(wait);
                }
            }

            struct LatestBlobReference {
                uint64_t seq = 0;
                std::optional<uint64_t> blobId;
            };

            static void recordLatestBlobReference(
                std::unordered_map<std::string, LatestBlobReference>& latestByKey,
                std::span<const uint8_t> key,
                uint64_t seq,
                std::optional<uint64_t> blobId
            ) {
                const std::string keyBytes{reinterpret_cast<const char*>(key.data()), key.size()};
                auto& latest = latestByKey[keyBytes];
                if (seq >= latest.seq) { latest = LatestBlobReference{seq, blobId}; }
            }

            [[nodiscard]] std::unordered_set<uint64_t> collectReferencedBlobIdsByScan() const {
                std::unordered_set<uint64_t> live;
                if (!blobManager || !memtable) { return live; }

                const uint64_t snapshotSeq = this->snapshotSeq();
                memtable::MemTable::KeyRange range;
                auto mt = memtable->iterator(range, snapshotSeq);
                sst::SSTManager::Iterator sstIt;
                if (sstManager) { sstIt = sstManager->scanIter({}, {}, snapshotSeq); }

                auto mtCur = mt.hasNext() ? mt.next() : std::optional<core::RecordView>{};
                auto sstCur = sstIt.hasNext() ? sstIt.next() : std::optional<sst::SSTRecord>{};

                while (mtCur || sstCur) {
                    const bool hasMt = mtCur.has_value();
                    const bool hasSst = sstCur.has_value();
                    const int cmp = (hasMt && hasSst) ? compareKey(mtCur->key(), sstCur->key) : (hasMt ? -1 : 1);

                    if (cmp <= 0) {
                        const auto record = *mtCur;
                        if (!record.isTombstone()) {
                            if (const auto blobId = decodeBlobIdIfReference(record.flags(), record.value())) { live.insert(*blobId); }
                        }
                        mtCur = mt.hasNext() ? mt.next() : std::optional<core::RecordView>{};
                        if (cmp == 0) { sstCur = sstIt.hasNext() ? sstIt.next() : std::optional<sst::SSTRecord>{}; }
                    }
                    else {
                        const auto record = std::move(*sstCur);
                        if (!record.isTombstone()) {
                            if (const auto blobId = decodeBlobIdIfReference(record.flags, record.value)) { live.insert(*blobId); }
                        }
                        sstCur = sstIt.hasNext() ? sstIt.next() : std::optional<sst::SSTRecord>{};
                    }
                }

                return live;
            }

            [[nodiscard]] std::optional<std::unordered_set<uint64_t>> collectReferencedBlobIdsFromManifest() const {
                if (!blobManager || !memtable || !manifest || !manifest->sstBlobRefsComplete()) { return std::nullopt; }

                std::unordered_set<std::string> liveSstFiles;
                for (const auto& file : manifest->liveSst()) { liveSstFiles.insert(file); }

                std::unordered_map<std::string, LatestBlobReference> latestByKey;
                for (const auto& refs : manifest->sstBlobRefs()) {
                    if (liveSstFiles.find(refs.file) == liveSstFiles.end()) { continue; }
                    for (const auto& entry : refs.entries) { recordLatestBlobReference(latestByKey, entry.key, entry.seq, entry.blobId); }
                }

                const uint64_t snapshotSeq = this->snapshotSeq();
                memtable::MemTable::KeyRange range;
                auto mt = memtable->iterator(range, snapshotSeq);
                while (mt.hasNext()) {
                    const auto record = mt.next();
                    if (!record.has_value()) { continue; }
                    std::optional<uint64_t> blobId;
                    if (!record->isTombstone()) { blobId = decodeBlobIdIfReference(record->flags(), record->value()); }
                    recordLatestBlobReference(latestByKey, record->key(), record->seq(), blobId);
                }

                std::unordered_set<uint64_t> live;
                for (const auto& [_, latest] : latestByKey) { if (latest.blobId.has_value()) { live.insert(*latest.blobId); } }
                return live;
            }

            [[nodiscard]] std::unordered_set<uint64_t> collectReferencedBlobIds() const {
                if (const auto manifestLive = collectReferencedBlobIdsFromManifest(); manifestLive.has_value()) { return *manifestLive; }
                return collectReferencedBlobIdsByScan();
            }

            void runBlobGcIfSafe() {
                if (!canRunBlobGc()) { return; }
                std::unique_lock planning{blobGcPlanMu, std::try_to_lock};
                if (!planning.owns_lock() || !canRunBlobGc()) { return; }
                const auto live = collectReferencedBlobIds();
                blobManager->scanOrphans([&live](uint64_t blobId) { return live.find(blobId) != live.end(); });
            }

            void appendAll(
                uint64_t seq,
                std::span<const uint8_t> key,
                std::span<const uint8_t> storedValue,
                uint8_t flags,
                uint64_t sourceNodeId,
                uint64_t precomputedFp64 = 0,
                uint64_t precomputedMiniKey = 0,
                uint8_t versionLogFlags = 0xFF,
                bool knownContiguousCommit = false,
                uint64_t timestampNs = 0
            ) {
                throwIfBackgroundFailed();
                waitForVersionLogRecovery();
                memtable->throwIfFlushFailed();
                const uint64_t fp64 = precomputedFp64 != 0 ? precomputedFp64 : core::computeKeyFp64(key);
                const uint64_t mini = precomputedMiniKey != 0 ? precomputedMiniKey : core::buildMiniKey(key);
                if (walWriter) { walWriter->append(key, storedValue, seq, flags, fp64, walAckForWriteDurability()); }
                if (versionLog) {
                    const uint8_t defaultVlogFlags = sourceNodeId == vlog::ROLLBACK_NODE
                                                         ? static_cast<uint8_t>(flags | vlog::VLOG_FLAG_ROLLBACK)
                                                         : flags;
                    const uint8_t vlogFlags = versionLogFlags == 0xFF ? defaultVlogFlags : versionLogFlags;
                    versionLog->appendDeferred(key, seq, sourceNodeId, timestampNs == 0 ? nowNs() : timestampNs, vlogFlags, storedValue);
                }

                if ((flags & MemHdr16::FLAG_TOMBSTONE) != 0) { memtable->remove(key, seq, fp64, mini); }
                else { memtable->put(key, storedValue, seq, flags, fp64, mini); }
                noteTransactionMutation(key);
                markWriteCommitted(seq, knownContiguousCommit);
                if (versionLog) { versionLog->markCommitted(seq); }
            }

            [[nodiscard]] wal::WalAppendAck snapshotCommitWalAck() const noexcept {
                return opts.wal.syncPolicy == wal::WalSyncPolicy::NEVER ? wal::WalAppendAck::WRITTEN : wal::WalAppendAck::SYNCED;
            }

            void appendSnapshotWalRecord(
                uint64_t seq,
                std::span<const uint8_t> key,
                std::span<const uint8_t> storedValue,
                uint8_t flags,
                uint64_t precomputedFp64
            ) {
                if (!walWriter) { return; }
                const uint64_t fp64 = precomputedFp64 != 0 ? precomputedFp64 : core::computeKeyFp64(key);
                walWriter->append(
                    key,
                    storedValue,
                    seq,
                    static_cast<uint16_t>(flags) | wal::WAL_FLAG_SNAPSHOT_RECORD,
                    fp64,
                    wal::WalAppendAck::WRITTEN
                );
            }

            void appendSnapshotWalCommit(uint64_t seq, uint64_t recordCount) {
                if (!walWriter) { return; }
                static constexpr std::array<uint8_t, 5> COMMIT_KEY = {'A', 'K', 'S', 'C', '1'};
                const std::vector<uint8_t> value = encodeSnapshotCommitValue(recordCount);
                walWriter->append(COMMIT_KEY, value, seq, wal::WAL_FLAG_SNAPSHOT_COMMIT, 0, snapshotCommitWalAck());
            }

            void applySnapshotRecordMemory(
                uint64_t seq,
                std::span<const uint8_t> key,
                std::span<const uint8_t> storedValue,
                uint8_t flags,
                uint64_t precomputedFp64 = 0,
                uint64_t precomputedMiniKey = 0
            ) {
                const uint64_t fp64 = precomputedFp64 != 0 ? precomputedFp64 : core::computeKeyFp64(key);
                const uint64_t mini = precomputedMiniKey != 0 ? precomputedMiniKey : core::buildMiniKey(key);
                if ((flags & MemHdr16::FLAG_TOMBSTONE) != 0) { memtable->remove(key, seq, fp64, mini); }
                else { memtable->put(key, storedValue, seq, flags, fp64, mini); }
                if (versionLog && !clusterConfig.usesDataConsensus()) {
                    versionLog->appendDeferred(key, seq, 0, nowNs(), flags, storedValue);
                    versionLog->markCommitted(seq);
                }
            }

            [[nodiscard]] AppliedWrite applyLocalPut(
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint64_t fp64 = 0,
                uint64_t miniKey = 0,
                uint64_t sourceNodeId = 0
            ) {
                AppliedWrite write;
                write.key = key;
                write.op = cluster::ReplOpType::PUT;
                write.sourceNodeId = sourceNodeId == 0 ? nodeId : sourceNodeId;
                applyWriteBackpressure();
                {
                    std::lock_guard lock(writeMu);
                    write.seq = reserveWriteSeq(1);
                    write.stored = maybeExternalize(write.seq, value, write.flags);
                    putsTotal.fetch_add(1, std::memory_order_relaxed);
                    if ((write.flags & MemHdr16::FLAG_BLOB) != 0) { blobPutsTotal.fetch_add(1, std::memory_order_relaxed); }
                    appendAll(write.seq, key, write.stored, write.flags, write.sourceNodeId, fp64, miniKey);
                }
                return write;
            }

            void applyLocalPutUnreplicated(
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint64_t fp64 = 0,
                uint64_t miniKey = 0,
                uint64_t sourceNodeId = 0
            ) {
                uint8_t flags = MemHdr16::FLAG_NORMAL;
                const uint64_t writeSourceNodeId = sourceNodeId == 0 ? nodeId : sourceNodeId;
                applyWriteBackpressure();
                if (canUseParallelWriteAdmission()) {
                    const uint64_t resolvedFp64 = fp64 != 0 ? fp64 : (key.empty() ? 0 : core::computeKeyFp64(key.data(), key.size()));
                    const uint64_t resolvedMiniKey =
                        miniKey != 0 ? miniKey : (key.empty() ? 0 : core::buildMiniKey(key.data(), key.size()));
                    if (usesKeySequenceOrder()) {
                        std::lock_guard lock{keySequenceOrderMutex(resolvedFp64, key.size())};
                        const uint64_t seq = reserveWriteSeq(1);
                        try {
                            putsTotal.fetch_add(1, std::memory_order_relaxed);
                            appendAll(seq, key, value, flags, writeSourceNodeId, resolvedFp64, resolvedMiniKey);
                        }
                        catch (...) {
                            markKeySequenceFailure(std::current_exception());
                            throw;
                        }
                        return;
                    }
                    const uint64_t seq = reserveWriteSeq(1);
                    putsTotal.fetch_add(1, std::memory_order_relaxed);
                    appendAll(seq, key, value, flags, writeSourceNodeId, resolvedFp64, resolvedMiniKey);
                    return;
                }

                std::lock_guard lock(writeMu);
                const uint64_t seq = reserveWriteSeq(1);
                putsTotal.fetch_add(1, std::memory_order_relaxed);

                if (blobManager && value.size() >= blobManager->threshold()) {
                    std::vector<uint8_t> stored = maybeExternalize(seq, value, flags);
                    if ((flags & MemHdr16::FLAG_BLOB) != 0) { blobPutsTotal.fetch_add(1, std::memory_order_relaxed); }
                    appendAll(seq, key, stored, flags, writeSourceNodeId, fp64, miniKey, 0xFF, true);
                    return;
                }

                appendAll(seq, key, value, flags, writeSourceNodeId, fp64, miniKey, 0xFF, true);
            }

            [[nodiscard]] std::vector<AppliedWrite> applyLocalPutBatch(std::span<const BatchPutEntry> entries) {
                std::vector<AppliedWrite> writes;
                writes.reserve(entries.size());
                if (!entries.empty()) { applyWriteBackpressure(); }

                std::lock_guard lock(writeMu);
                const uint64_t baseSeq = reserveWriteSeq(entries.size());
                for (size_t i = 0; i < entries.size(); ++i) {
                    const auto& [key, value] = entries[i];
                    AppliedWrite write;
                    write.seq = baseSeq + i;
                    write.key = key;
                    write.op = cluster::ReplOpType::PUT;
                    write.sourceNodeId = nodeId;
                    write.stored = maybeExternalize(write.seq, value, write.flags);
                    putsTotal.fetch_add(1, std::memory_order_relaxed);
                    if ((write.flags & MemHdr16::FLAG_BLOB) != 0) { blobPutsTotal.fetch_add(1, std::memory_order_relaxed); }
                    appendAll(write.seq, write.key, write.stored, write.flags, nodeId);
                    writes.push_back(std::move(write));
                }
                return writes;
            }

            void applyLocalPutBatchUnreplicated(std::span<const BatchPutEntry> entries) {
                if (!entries.empty()) { applyWriteBackpressure(); }
                if (canUseParallelWriteAdmission()) {
                    std::vector<std::unique_lock<std::mutex>> keyOrderLocks;
                    const bool keySequenceOrder = usesKeySequenceOrder();
                    if (keySequenceOrder) { keyOrderLocks = lockKeySequenceOrderStripes(entries); }
                    const uint64_t baseSeq = reserveWriteSeq(entries.size());
                    try {
                        for (size_t i = 0; i < entries.size(); ++i) {
                            const auto& [key, value] = entries[i];
                            putsTotal.fetch_add(1, std::memory_order_relaxed);
                            appendAll(baseSeq + i, key, value, MemHdr16::FLAG_NORMAL, nodeId);
                        }
                    }
                    catch (...) {
                        if (keySequenceOrder) { markKeySequenceFailure(std::current_exception()); }
                        throw;
                    }
                    return;
                }

                std::lock_guard lock(writeMu);
                const uint64_t baseSeq = reserveWriteSeq(entries.size());
                for (size_t i = 0; i < entries.size(); ++i) {
                    const auto& [key, value] = entries[i];
                    uint8_t flags = MemHdr16::FLAG_NORMAL;
                    putsTotal.fetch_add(1, std::memory_order_relaxed);
                    const uint64_t seq = baseSeq + i;

                    if (blobManager && value.size() >= blobManager->threshold()) {
                        std::vector<uint8_t> stored = maybeExternalize(seq, value, flags);
                        if ((flags & MemHdr16::FLAG_BLOB) != 0) { blobPutsTotal.fetch_add(1, std::memory_order_relaxed); }
                        appendAll(seq, key, stored, flags, nodeId, 0, 0, 0xFF, true);
                    }
                    else { appendAll(seq, key, value, flags, nodeId, 0, 0, 0xFF, true); }
                }
            }

            [[nodiscard]] AppliedWrite applyLocalRemove(
                std::span<const uint8_t> key,
                uint64_t fp64 = 0,
                uint64_t miniKey = 0,
                uint64_t sourceNodeId = 0
            ) {
                AppliedWrite write;
                write.key = key;
                write.op = cluster::ReplOpType::REMOVE;
                write.flags = MemHdr16::FLAG_TOMBSTONE;
                write.sourceNodeId = sourceNodeId == 0 ? nodeId : sourceNodeId;
                applyWriteBackpressure();
                {
                    std::lock_guard lock(writeMu);
                    write.seq = reserveWriteSeq(1);
                    removesTotal.fetch_add(1, std::memory_order_relaxed);
                    appendAll(write.seq, key, {}, write.flags, write.sourceNodeId, fp64, miniKey);
                }
                return write;
            }

            void applyLocalRemoveUnreplicated(
                std::span<const uint8_t> key,
                uint64_t fp64 = 0,
                uint64_t miniKey = 0,
                uint64_t sourceNodeId = 0
            ) {
                const uint64_t writeSourceNodeId = sourceNodeId == 0 ? nodeId : sourceNodeId;
                applyWriteBackpressure();
                if (canUseParallelWriteAdmission()) {
                    const uint64_t resolvedFp64 = fp64 != 0 ? fp64 : (key.empty() ? 0 : core::computeKeyFp64(key.data(), key.size()));
                    const uint64_t resolvedMiniKey =
                        miniKey != 0 ? miniKey : (key.empty() ? 0 : core::buildMiniKey(key.data(), key.size()));
                    if (usesKeySequenceOrder()) {
                        std::lock_guard lock{keySequenceOrderMutex(resolvedFp64, key.size())};
                        const uint64_t seq = reserveWriteSeq(1);
                        try {
                            removesTotal.fetch_add(1, std::memory_order_relaxed);
                            appendAll(seq, key, {}, MemHdr16::FLAG_TOMBSTONE, writeSourceNodeId, resolvedFp64, resolvedMiniKey);
                        }
                        catch (...) {
                            markKeySequenceFailure(std::current_exception());
                            throw;
                        }
                        return;
                    }
                    const uint64_t seq = reserveWriteSeq(1);
                    removesTotal.fetch_add(1, std::memory_order_relaxed);
                    appendAll(seq, key, {}, MemHdr16::FLAG_TOMBSTONE, writeSourceNodeId, resolvedFp64, resolvedMiniKey);
                    return;
                }

                std::lock_guard lock(writeMu);
                const uint64_t seq = reserveWriteSeq(1);
                removesTotal.fetch_add(1, std::memory_order_relaxed);
                appendAll(seq, key, {}, MemHdr16::FLAG_TOMBSTONE, writeSourceNodeId, fp64, miniKey, 0xFF, true);
            }

            void replicateCommitted(const AppliedWrite& write) {
                if (clusterRuntime) {
                    clusterRuntime->shipEntry(write.seq, write.op, write.key, write.stored, write.flags, write.sourceNodeId);
                }
            }

            [[nodiscard]] bool strictPrimaryAckFailWrite() const noexcept {
                return primaryAckTimeoutAction == cluster::AckTimeoutAction::FAIL_WRITE;
            }

            void recordCommittedRaftMutationStats(cluster::ReplOpType op, uint8_t recordFlags) {
                const uint8_t flags = op == cluster::ReplOpType::REMOVE
                                          ? static_cast<uint8_t>(recordFlags | MemHdr16::FLAG_TOMBSTONE)
                                          : recordFlags;
                if (op == cluster::ReplOpType::REMOVE) {
                    removesTotal.fetch_add(1, std::memory_order_relaxed);
                }
                else {
                    putsTotal.fetch_add(1, std::memory_order_relaxed);
                    if ((flags & MemHdr16::FLAG_BLOB) != 0) { blobPutsTotal.fetch_add(1, std::memory_order_relaxed); }
                }
            }

            void forceClusterLocalDurable() {
                if (walWriter) { walWriter->forceSync(); }
                if (versionLog) { versionLog->forceSync(); }
            }

            void applyReplicaRecord(
                uint64_t seq,
                cluster::ReplOpType op,
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint8_t recordFlags,
                uint64_t sourceNodeId,
                uint64_t timestampNs
            ) {
                std::lock_guard lock(writeMu);
                uint8_t flags = recordFlags;
                if (op == cluster::ReplOpType::REMOVE) { flags |= MemHdr16::FLAG_TOMBSTONE; }
                const uint8_t vlogFlags = sourceNodeId == vlog::ROLLBACK_NODE
                                              ? static_cast<uint8_t>(flags | vlog::VLOG_FLAG_ROLLBACK)
                                              : flags;
                const bool partitionedRemote =
                    (clusterReplicationMode == cluster::ReplicationMode::PARTITIONED || clusterReplicationMode == cluster::ReplicationMode::STRIPE) &&
                    sourceNodeId != 0 && sourceNodeId != nodeId;
                if (clusterReplicationMode == cluster::ReplicationMode::STRIPE) {
                    if (!isStripeInternalKey(key) || key[4] != 'S') { throw std::runtime_error("AkkEngine: invalid remote STRIPE key"); }
                    size_t cursor = 6;
                    uint64_t generation = 0;
                    uint16_t index = 0;
                    uint32_t keyLength = 0;
                    if (!pullU64(key, cursor, generation) || !pullU16(key, cursor, index) || !pullU32(key, cursor, keyLength) ||
                        keyLength != key.size() - cursor) { throw std::runtime_error("AkkEngine: invalid remote STRIPE shard key"); }
                    const auto accepts = [&](const cluster::ClusterConfig& placement) {
                        const uint64_t originalOwner = stripeOwnerFor(placement, key.subspan(cursor));
                        const uint64_t failoverNode = clusterRuntime ? clusterRuntime->stripeFailoverNodeId() : 0;
                        const bool fromOwner = sourceNodeId == originalOwner;
                        const bool fromFailover = failoverNode != 0 && sourceNodeId == failoverNode;
                        const auto ownerIds = stripeNodeIdsFor(placement, key.subspan(cursor));
                        const uint8_t copiesPerShard = placement.stripe().copiesPerShard;
                        const auto targetsShard = [&](const std::vector<uint64_t>& placements) {
                            if (index >= placement.stripe().totalShards()) { return false; }
                            const size_t firstPlacement = static_cast<size_t>(index) * copiesPerShard;
                            return std::ranges::any_of(
                                std::span<const uint64_t>{placements}.subspan(firstPlacement, copiesPerShard),
                                [&](uint64_t targetNodeId) { return targetNodeId == nodeId; }
                            );
                        };
                        const bool ownerTarget = targetsShard(ownerIds);
                        const bool hotSpareTarget = copiesPerShard == 2 && failoverNode == nodeId;
                        bool failoverTarget = false;
                        if (fromFailover && !fromOwner) {
                            const auto failoverIds = stripeNodeIdsFor(placement, key.subspan(cursor), originalOwner);
                            failoverTarget = targetsShard(failoverIds);
                        }
                        return (fromOwner || fromFailover) && (!fromOwner || ownerTarget || hotSpareTarget) &&
                            (!fromFailover || ownerTarget || failoverTarget);
                    };
                    bool accepted = accepts(currentPlacement());
                    if (!accepted && clusterRuntime) {
                        const auto state = clusterRuntime->placementState(false);
                        if (state.pendingGeneration != 0) { accepted = accepts(cluster::ClusterConfig::decode(state.pendingConfig)); }
                    }
                    if (!accepted) { throw std::runtime_error("AkkEngine: remote STRIPE write is not from its owner or targets the wrong shard"); }
                }
                const uint64_t storageSeq = clusterReplicationMode == cluster::ReplicationMode::STRIPE ? memtable->reserveSeq(1) :
                    partitionedRemote ? reserveWriteSeq(1) : seq;
                appendAll(storageSeq, key, value, flags, sourceNodeId, 0, 0, vlogFlags, false, timestampNs);
                memtable->advanceSeq(storageSeq);
            }

            [[nodiscard]] fs::path replicaSnapshotStagingPath() const {
                return opts.paths.dataDir.empty()
                           ? fs::path{"replication-snapshot.staging"}
                           : opts.paths.dataDir / "replication-snapshot.staging";
            }

            [[nodiscard]] fs::path clusterSnapshotExportDirectory() const {
                return opts.paths.dataDir.empty()
                           ? fs::path{"cluster-snapshot-exports"}
                           : opts.paths.dataDir / "cluster-snapshot-exports";
            }

            [[nodiscard]] fs::path newClusterSnapshotExportPath() const {
                std::array<uint8_t, 16> random{};
                crypto::secureRandom(random);
                static constexpr char HEX[] = "0123456789abcdef";
                std::string name{"snapshot-"};
                name.reserve(9 + random.size() * 2 + 4);
                for (const uint8_t byte : random) {
                    name.push_back(HEX[byte >> 4]);
                    name.push_back(HEX[byte & 0x0f]);
                }
                name += ".tmp";
                return clusterSnapshotExportDirectory() / name;
            }

            [[nodiscard]] std::pair<uint64_t, uint64_t> readReplicaSnapshotStagingHeader(std::istream& in) const {
                static constexpr char SNAPSHOT_STAGING_MAGIC[] = {'A', 'K', 'S', 'S', '1'};
                std::array<char, sizeof(SNAPSHOT_STAGING_MAGIC)> magic{};
                readExact(in, magic.data(), magic.size(), "header");
                if (!std::equal(magic.begin(), magic.end(), std::begin(SNAPSHOT_STAGING_MAGIC))) {
                    throw std::runtime_error("AkkEngine: corrupt replication snapshot staging file");
                }
                const uint64_t stagedSeq = readU64Le(in, "snapshot seq");
                const uint64_t stagedEntryCount = readU64Le(in, "entry count");
                return {stagedSeq, stagedEntryCount};
            }

            void resetReplicaSnapshotStagingLocked() {
                if (pendingSnapshotOut.is_open()) { pendingSnapshotOut.close(); }
                removeFileIfExists(replicaSnapshotStagingPath());
                snapshotInProgress = false;
                pendingSnapshotSeq = 0;
                pendingSnapshotEntryCount = 0;
                pendingSnapshotAppliedEntries = 0;
                pendingSnapshotKeys.clear();
                pendingSnapshotEntryInProgress = false;
                pendingSnapshotEntryValueSize = 0;
                pendingSnapshotEntryValueOffset = 0;
                pendingSnapshotEntryValueCrc32c = 0;
                pendingSnapshotEntryRecordCrc = Crc32cStream{};
                pendingSnapshotEntryValueCrc = Crc32cStream{};
            }

            void rememberSnapshotHead(std::span<const uint8_t> key) {
                if (clusterConfig.usesDataConsensus()) {
                    const auto record = cluster::detail::decodeSnapshotKey(key);
                    if (record.kind == cluster::detail::SnapshotRecordKind::HISTORY) { return; }
                    if (record.kind != cluster::detail::SnapshotRecordKind::HEAD) {
                        throw std::runtime_error("AkkEngine: invalid data snapshot record kind");
                    }
                    key = record.key;
                }
                const auto [_, inserted] = pendingSnapshotKeys.emplace(reinterpret_cast<const char*>(key.data()), key.size());
                if (!inserted) { throw std::runtime_error("AkkEngine: replication snapshot has duplicate heads"); }
            }

            void loadReplicaSnapshotStagingForRecoveryLocked(uint64_t seq) {
                if (snapshotInProgress && seq == pendingSnapshotSeq && pendingSnapshotAppliedEntries == pendingSnapshotEntryCount && !
                    pendingSnapshotEntryInProgress) {
                    if (pendingSnapshotOut.is_open()) { pendingSnapshotOut.close(); }
                    return;
                }
                if (pendingSnapshotOut.is_open()) { pendingSnapshotOut.close(); }

                const auto path = replicaSnapshotStagingPath();
                std::ifstream staged{path, std::ios::binary};
                if (!staged) { throw std::runtime_error("AkkEngine: missing replication snapshot staging file for recovery"); }

                const auto [stagedSeq, stagedEntryCount] = readReplicaSnapshotStagingHeader(staged);
                if (stagedSeq != seq) { throw std::runtime_error("AkkEngine: replication snapshot recovery sequence mismatch"); }

                pendingSnapshotKeys.clear();
                for (uint64_t i = 0; i < stagedEntryCount; ++i) {
                    const auto entry = readStagedSnapshotRecord(staged, seq, i + 1, false);
                    if (!entry.history) {
                        const auto [_, inserted] = pendingSnapshotKeys.emplace(reinterpret_cast<const char*>(entry.key.data()), entry.key.size());
                        if (!inserted) { throw std::runtime_error("AkkEngine: replication snapshot has duplicate heads"); }
                    }
                }
                char trailing = 0;
                if (staged.get(trailing)) { throw std::runtime_error("AkkEngine: trailing bytes in replication snapshot staging file"); }

                snapshotInProgress = true;
                pendingSnapshotSeq = seq;
                pendingSnapshotEntryCount = stagedEntryCount;
                pendingSnapshotAppliedEntries = stagedEntryCount;
                pendingSnapshotEntryInProgress = false;
                pendingSnapshotEntryValueSize = 0;
                pendingSnapshotEntryValueOffset = 0;
                pendingSnapshotEntryValueCrc32c = 0;
                pendingSnapshotEntryRecordCrc = Crc32cStream{};
                pendingSnapshotEntryValueCrc = Crc32cStream{};
            }

            void beginReplicaSnapshot(uint64_t seq, uint64_t) {
                std::lock_guard lock(writeMu);
                resetReplicaSnapshotStagingLocked();
                const auto path = replicaSnapshotStagingPath();
                ensureDir(path.parent_path());
                pendingSnapshotOut.open(path, std::ios::binary | std::ios::trunc);
                if (!pendingSnapshotOut) { throw std::runtime_error("AkkEngine: failed to open replication snapshot staging file"); }

                static constexpr char SNAPSHOT_STAGING_MAGIC[] = {'A', 'K', 'S', 'S', '1'};
                pendingSnapshotOut.write(SNAPSHOT_STAGING_MAGIC, sizeof(SNAPSHOT_STAGING_MAGIC));
                writeU64Le(pendingSnapshotOut, seq);
                writeU64Le(pendingSnapshotOut, 0); // Patched with the final count on completion.

                snapshotInProgress = true;
                pendingSnapshotSeq = seq;
                pendingSnapshotEntryCount = UINT64_MAX;
                pendingSnapshotAppliedEntries = 0;
            }

            void beginReplicaSnapshotEntry(std::span<const uint8_t> key, uint64_t valueSize, uint32_t valueCrc32c) {
                std::lock_guard lock(writeMu);
                if (!snapshotInProgress || !pendingSnapshotOut.is_open()) {
                    throw std::runtime_error("AkkEngine: received replication snapshot entry without begin");
                }
                if (pendingSnapshotEntryInProgress) { throw std::runtime_error("AkkEngine: replication snapshot entry is already active"); }
                if (pendingSnapshotAppliedEntries == UINT64_MAX) { throw std::runtime_error("AkkEngine: replication snapshot has too many entries"); }
                if (key.size() > UINT32_MAX) { throw std::invalid_argument("AkkEngine: replication snapshot key is too large"); }

                rememberSnapshotHead(key);

                const auto keyLen = static_cast<uint32_t>(key.size());
                writeU32Le(pendingSnapshotOut, keyLen);
                writeU64Le(pendingSnapshotOut, valueSize);
                writeU32Le(pendingSnapshotOut, valueCrc32c);
                if (!key.empty()) {
                    pendingSnapshotOut.write(reinterpret_cast<const char*>(key.data()), static_cast<std::streamsize>(key.size()));
                }
                if (!pendingSnapshotOut) { throw std::runtime_error("AkkEngine: failed to write replication snapshot staging entry"); }

                pendingSnapshotEntryInProgress = true;
                pendingSnapshotEntryValueSize = valueSize;
                pendingSnapshotEntryValueOffset = 0;
                pendingSnapshotEntryValueCrc32c = valueCrc32c;
                pendingSnapshotEntryRecordCrc = Crc32cStream{};
                pendingSnapshotEntryValueCrc = Crc32cStream{};
                updateCrcU32Le(pendingSnapshotEntryRecordCrc, keyLen);
                updateCrcU64Le(pendingSnapshotEntryRecordCrc, valueSize);
                updateCrcU32Le(pendingSnapshotEntryRecordCrc, valueCrc32c);
                pendingSnapshotEntryRecordCrc.update(key);
            }

            void appendReplicaSnapshotEntryChunk(uint64_t offset, std::span<const uint8_t> chunk) {
                std::lock_guard lock(writeMu);
                if (!snapshotInProgress || !pendingSnapshotOut.is_open() || !pendingSnapshotEntryInProgress) {
                    throw std::runtime_error("AkkEngine: received replication snapshot chunk without active entry");
                }
                if (offset != pendingSnapshotEntryValueOffset || chunk.size() > pendingSnapshotEntryValueSize -
                    pendingSnapshotEntryValueOffset) { throw std::runtime_error("AkkEngine: invalid replication snapshot chunk"); }
                if (!chunk.empty()) {
                    pendingSnapshotOut.write(reinterpret_cast<const char*>(chunk.data()), static_cast<std::streamsize>(chunk.size()));
                    if (!pendingSnapshotOut) { throw std::runtime_error("AkkEngine: failed to write replication snapshot chunk"); }
                }
                pendingSnapshotEntryRecordCrc.update(chunk);
                pendingSnapshotEntryValueCrc.update(chunk);
                pendingSnapshotEntryValueOffset += static_cast<uint64_t>(chunk.size());
            }

            void finishReplicaSnapshotEntry() {
                std::lock_guard lock(writeMu);
                if (!snapshotInProgress || !pendingSnapshotOut.is_open() || !pendingSnapshotEntryInProgress) {
                    throw std::runtime_error("AkkEngine: finished replication snapshot entry without active entry");
                }
                if (pendingSnapshotEntryValueOffset != pendingSnapshotEntryValueSize || pendingSnapshotEntryValueCrc.finish() !=
                    pendingSnapshotEntryValueCrc32c) {
                    throw std::runtime_error("AkkEngine: replication snapshot entry checksum or size mismatch");
                }
                writeU32Le(pendingSnapshotOut, pendingSnapshotEntryRecordCrc.finish());
                ++pendingSnapshotAppliedEntries;
                pendingSnapshotEntryInProgress = false;
                pendingSnapshotEntryValueSize = 0;
                pendingSnapshotEntryValueOffset = 0;
                pendingSnapshotEntryValueCrc32c = 0;
                pendingSnapshotEntryRecordCrc = Crc32cStream{};
                pendingSnapshotEntryValueCrc = Crc32cStream{};
            }

            void prepareReplicaSnapshotLocked(uint64_t seq, uint64_t entryCount) {
                if (!snapshotInProgress || seq != pendingSnapshotSeq || pendingSnapshotEntryInProgress ||
                    pendingSnapshotAppliedEntries != entryCount) {
                    throw std::runtime_error("AkkEngine: invalid replication snapshot completion");
                }
                pendingSnapshotEntryCount = entryCount;
                if (pendingSnapshotOut.is_open()) {
                    pendingSnapshotOut.seekp(static_cast<std::streamoff>(5 + sizeof(uint64_t)));
                    writeU64Le(pendingSnapshotOut, entryCount);
                    pendingSnapshotOut.flush();
                    if (!pendingSnapshotOut) { throw std::runtime_error("AkkEngine: failed to flush replication snapshot staging file"); }
                    pendingSnapshotOut.close();
                }
                const auto path = replicaSnapshotStagingPath();
#ifdef _WIN32
                const HANDLE file = ::CreateFileW(path.c_str(), GENERIC_WRITE, FILE_SHARE_READ, nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
                if (file == INVALID_HANDLE_VALUE || !::FlushFileBuffers(file)) {
                    if (file != INVALID_HANDLE_VALUE) { ::CloseHandle(file); }
                    throw std::runtime_error("AkkEngine: failed to sync snapshot staging");
                }
                ::CloseHandle(file);
#else
                const int file = ::open(path.c_str(), O_RDONLY);
                if (file < 0 || ::fsync(file) != 0) {
                    if (file >= 0) { ::close(file); }
                    throw std::runtime_error("AkkEngine: failed to sync snapshot staging");
                }
                ::close(file);
                const int directory = ::open(path.parent_path().c_str(), O_RDONLY | O_DIRECTORY);
                if (directory < 0 || ::fsync(directory) != 0) {
                    if (directory >= 0) { ::close(directory); }
                    throw std::runtime_error("AkkEngine: failed to sync snapshot staging directory");
                }
                ::close(directory);
#endif
            }

            void finishReplicaSnapshotLocked(uint64_t seq, uint64_t entryCount, uint64_t partitionOwner = 0) {
                prepareReplicaSnapshotLocked(seq, entryCount);

                const auto path = replicaSnapshotStagingPath();
                corruptSnapshotStagingAtTestPoint(path, "snapshot.finish.before_staging_apply");
                std::ifstream staged{path, std::ios::binary};
                if (!staged) { throw std::runtime_error("AkkEngine: failed to open replication snapshot staging file for apply"); }

                const auto [stagedSeq, stagedEntryCount] = readReplicaSnapshotStagingHeader(staged);
                if (stagedSeq != seq || stagedEntryCount != pendingSnapshotEntryCount) {
                    throw std::runtime_error("AkkEngine: replication snapshot staging metadata mismatch");
                }

                if (partitionOwner != 0) {
                    // Validate scratch storage before reserving a local commit slot.
                    // A corrupt receive can then reconnect without leaving a sequence gap.
                    const auto entriesOffset = staged.tellg();
                    for (uint64_t i = 0; i < stagedEntryCount; ++i) {
                        (void)readStagedSnapshotRecord(staged, 0, i + 1, false);
                    }
                    char extra = 0;
                    if (staged.get(extra)) { throw std::runtime_error("AkkEngine: trailing bytes in replication snapshot staging file"); }
                    staged.clear(); staged.seekg(entriesOffset);
                    if (!staged) { throw std::runtime_error("AkkEngine: cannot rewind partition snapshot staging file"); }
                }
                const auto inScope = [&](std::span<const uint8_t> key) {
                    if (partitionOwner == 0) { return true; }
                    const auto targets = cluster::detail::partitionTargets(clusterConfig, key);
                    return !targets.empty() && targets.front().nodeId == partitionOwner &&
                        std::ranges::any_of(targets, [&](const auto& target) { return target.nodeId == nodeId; });
                };
                if (partitionOwner != 0 && stagedEntryCount == 0) {
                    core::BufferArena emptyArena;
                    memtable::MemTable::KeyRange emptyRange;
                    const auto visible = snapshotSeq();
                    auto emptyMt = memtable->iterator(emptyRange, visible);
                    sst::SSTManager::Iterator emptySst;
                    if (sstManager) { emptySst = sstManager->scanIter({}, {}, visible); }
                    bool hasOwnedKey = false;
                    for (const auto& record : scanGenerator(emptyArena, std::move(emptyMt), std::move(emptySst), blobManager.get())) {
                        if (inScope(record.key)) { hasOwnedKey = true; break; }
                    }
                    // No storage change: only the sender's peer watermark advances.
                    if (!hasOwnedKey) { staged.close(); resetReplicaSnapshotStagingLocked(); return; }
                }
                const uint64_t storageSeq = partitionOwner != 0 ? reserveWriteSeq(1) : seq;
                uint64_t snapshotWalRecordCount = 0;
                core::BufferArena arena;
                memtable::MemTable::KeyRange range;
                const uint64_t visibleSeq = snapshotSeq();
                uint64_t storedHistoryThrough = 0;
                if (versionLog && clusterConfig.usesDataConsensus()) {
                    versionLog->forceSync(); storedHistoryThrough = versionLog->highestStoredSequence();
                }
                auto mt = memtable->iterator(range, visibleSeq);
                sst::SSTManager::Iterator sst;
                if (sstManager) { sst = sstManager->scanIter({}, {}, visibleSeq); }
                for (const auto& record : scanGenerator(arena, std::move(mt), std::move(sst), blobManager.get())) {
                    const std::string key{reinterpret_cast<const char*>(record.key.data()), record.key.size()};
                    if (inScope(record.key) && pendingSnapshotKeys.find(key) == pendingSnapshotKeys.end()) {
                        appendSnapshotWalRecord(storageSeq, record.key, {}, MemHdr16::FLAG_TOMBSTONE, 0);
                        ++snapshotWalRecordCount;
                    }
                }

                for (uint64_t i = 0; i < stagedEntryCount; ++i) {
                    const auto entry = readStagedSnapshotRecord(staged, storageSeq, i + 1, true, storedHistoryThrough);
                    if (entry.history) {
                        // Applied Raft prefixes are common to all holders. History
                        // reaches disk before the KV snapshot commit advances that prefix.
                        if (versionLog && !entry.historyPresent) {
                            versionLog->appendDeferred(entry.key, entry.sequence, entry.source, entry.timestamp, entry.flags, entry.storedValue);
                        }
                        continue;
                    }
                    appendSnapshotWalRecord(storageSeq, entry.key, entry.storedValue, entry.flags, entry.fp64);
                    ++snapshotWalRecordCount;
                }

                char trailing = 0;
                if (staged.get(trailing)) { throw std::runtime_error("AkkEngine: trailing bytes in replication snapshot staging file"); }
                staged.close();

                if (versionLog && clusterConfig.usesDataConsensus()) { versionLog->forceSync(); }

                if (walWriter) {
                    crashAtTestPoint("snapshot.finish.after_wal_records");
                    appendSnapshotWalCommit(storageSeq, snapshotWalRecordCount);
                    if (partitionOwner == 0) { durableReplicaSnapshotSeq = std::max(durableReplicaSnapshotSeq, seq); }
                    crashAtTestPoint("snapshot.finish.after_wal_commit");
                }

                core::BufferArena applyArena;
                memtable::MemTable::KeyRange applyRange;
                auto applyMt = memtable->iterator(applyRange, visibleSeq);
                sst::SSTManager::Iterator applySst;
                if (sstManager) { applySst = sstManager->scanIter({}, {}, visibleSeq); }
                for (const auto& record : scanGenerator(applyArena, std::move(applyMt), std::move(applySst), blobManager.get())) {
                    const std::string key{reinterpret_cast<const char*>(record.key.data()), record.key.size()};
                    if (inScope(record.key) && pendingSnapshotKeys.find(key) == pendingSnapshotKeys.end()) {
                        applySnapshotRecordMemory(storageSeq, record.key, {}, MemHdr16::FLAG_TOMBSTONE);
                    }
                }
                crashAtTestPoint("snapshot.finish.after_tombstone_apply");
                pendingSnapshotKeys.clear();

                std::ifstream applyStaged{path, std::ios::binary};
                if (!applyStaged) { throw std::runtime_error("AkkEngine: failed to reopen replication snapshot staging file for apply"); }
                const auto [applyStagedSeq, applyStagedEntryCount] = readReplicaSnapshotStagingHeader(applyStaged);
                if (applyStagedSeq != seq || applyStagedEntryCount != stagedEntryCount) {
                    throw std::runtime_error("AkkEngine: replication snapshot staging metadata changed during apply");
                }
                for (uint64_t i = 0; i < stagedEntryCount; ++i) {
                    const auto entry = readStagedSnapshotRecord(applyStaged, storageSeq, i + 1, false);
                    if (entry.history) { continue; }
                    applySnapshotRecordMemory(storageSeq, entry.key, entry.storedValue, entry.flags, entry.fp64, entry.miniKey);
                }
                if (applyStaged.get(trailing)) {
                    throw std::runtime_error("AkkEngine: trailing bytes in replication snapshot staging file");
                }

                memtable->advanceSeq(storageSeq);
                if (partitionOwner != 0) { markWriteCommitted(storageSeq); }
                else {
                    // A whole-database snapshot replaces the entire prefix.
                    resetCommittedSeq(std::max(committedSeq.load(std::memory_order_acquire), seq));
                    if (versionLog && clusterConfig.usesDataConsensus()) { versionLog->seedCommittedSeq(seq); }
                }
                commitCv.notify_all();
                applyStaged.close();
                resetReplicaSnapshotStagingLocked();
            }

            void installPartitionSnapshot(uint64_t owner, const cluster::ClusterSnapshot& snapshot) {
                if (clusterReplicationMode != cluster::ReplicationMode::PARTITIONED || owner == nodeId || !snapshot.forEachEntry) {
                    throw std::runtime_error("AkkEngine: invalid partition snapshot owner");
                }
                // Incoming peers have independent spools; serialize use of the
                // engine's transaction staging file, without holding writeMu over network I/O.
                std::lock_guard installLock{partitionSnapshotMu};
                beginReplicaSnapshot(snapshot.seq, 0);
                uint64_t count = 0;
                try {
                    const cluster::SnapshotEntryVisitor visitor{
                        .beginEntry = [&](auto key, uint64_t size, uint32_t crc) {
                            if (key.size() > UINT16_MAX || (!blobManager && size > UINT16_MAX)) {
                                throw std::runtime_error("AkkEngine: partition snapshot record is too large");
                            }
                            const auto targets = cluster::detail::partitionTargets(clusterConfig, key);
                            if (targets.empty() || targets.front().nodeId != owner ||
                                std::ranges::none_of(targets, [&](const auto& target) { return target.nodeId == nodeId; })) {
                                throw std::runtime_error("AkkEngine: partition snapshot violates placement");
                            }
                            beginReplicaSnapshotEntry(key, size, crc); return true;
                        },
                        .appendValueChunk = [&](uint64_t offset, auto chunk) { appendReplicaSnapshotEntryChunk(offset, chunk); return true; },
                        .finishEntry = [&] { finishReplicaSnapshotEntry(); ++count; return true; },
                        .fileEntry = {},
                    };
                    if (!snapshot.forEachEntry(visitor)) { throw std::runtime_error("AkkEngine: incomplete partition snapshot"); }
                    std::lock_guard lock{writeMu};
                    finishReplicaSnapshotLocked(snapshot.seq, count, owner);
                }
                catch (...) {
                    std::lock_guard lock{writeMu};
                    resetReplicaSnapshotStagingLocked();
                    throw;
                }
            }

            void finishReplicaSnapshot(uint64_t seq, uint64_t entryCount) {
                std::lock_guard lock(writeMu);
                finishReplicaSnapshotLocked(seq, entryCount);
            }

            void recoverReplicaSnapshot(uint64_t seq) {
                std::lock_guard lock(writeMu);
                if (snapshotSeq() >= seq) {
                    durableReplicaSnapshotSeq = std::max(durableReplicaSnapshotSeq, seq);
                    resetReplicaSnapshotStagingLocked();
                    return;
                }
                loadReplicaSnapshotStagingForRecoveryLocked(seq);
                finishReplicaSnapshotLocked(seq, pendingSnapshotEntryCount);
            }

            [[nodiscard]] bool isReplicaSnapshotDurable(uint64_t seq) {
                std::lock_guard lock(writeMu);
                return snapshotSeq() >= seq || durableReplicaSnapshotSeq >= seq;
            }

            class WriteCoordinator {
                public:
                    explicit WriteCoordinator(Impl& engine) : engine_{engine} {}
                    virtual ~WriteCoordinator() = default;
                    WriteCoordinator(const WriteCoordinator&) = delete;
                    WriteCoordinator& operator=(const WriteCoordinator&) = delete;

                    virtual void put(std::span<const uint8_t> key, std::span<const uint8_t> value) = 0;
                    virtual void putHinted(
                        std::span<const uint8_t> key,
                        std::span<const uint8_t> value,
                        uint64_t fp64,
                        uint64_t miniKey
                    ) = 0;
                    virtual void putBatch(std::span<const BatchPutEntry> entries) = 0;
                    virtual void remove(std::span<const uint8_t> key) = 0;
                    virtual void rollback(
                        std::span<const uint8_t> key,
                        std::span<const uint8_t> value,
                        bool tombstone
                    ) = 0;
                    virtual cluster::ClusterRequestResult writeWithRequest(const cluster::ClusterRequestId&, cluster::ReplOpType,
                        std::span<const uint8_t>, std::span<const uint8_t>) {
                        throw std::logic_error("AkkEngine: retry-safe writes require Raft");
                    }
                    virtual void removeHinted(std::span<const uint8_t> key, uint64_t fp64, uint64_t miniKey) = 0;

                protected:
                    Impl& engine_;
            };

            class LocalUnreplicatedWriteCoordinator final : public WriteCoordinator {
                public:
                    using WriteCoordinator::WriteCoordinator;

                    void put(std::span<const uint8_t> key, std::span<const uint8_t> value) override {
                        engine_.applyLocalPutUnreplicated(key, value);
                    }

                    void putHinted(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t fp64, uint64_t miniKey) override {
                        engine_.applyLocalPutUnreplicated(key, value, fp64, miniKey);
                    }

                    void putBatch(std::span<const BatchPutEntry> entries) override { engine_.applyLocalPutBatchUnreplicated(entries); }

                    void remove(std::span<const uint8_t> key) override { engine_.applyLocalRemoveUnreplicated(key); }

                    void rollback(std::span<const uint8_t> key, std::span<const uint8_t> value, bool tombstone) override {
                        if (tombstone) { engine_.applyLocalRemoveUnreplicated(key, 0, 0, vlog::ROLLBACK_NODE); }
                        else { engine_.applyLocalPutUnreplicated(key, value, 0, 0, vlog::ROLLBACK_NODE); }
                    }

                    void removeHinted(std::span<const uint8_t> key, uint64_t fp64, uint64_t miniKey) override {
                        engine_.applyLocalRemoveUnreplicated(key, fp64, miniKey);
                    }
            };

            class LocalReplicationWriteCoordinator final : public WriteCoordinator {
                public:
                    using WriteCoordinator::WriteCoordinator;

                    void put(std::span<const uint8_t> key, std::span<const uint8_t> value) override {
                        engine_.clusterRuntime->executePrimaryWrite([&] {
                            engine_.requireOwnsWriteKey(key);
                            if (engine_.strictPrimaryAckFailWrite()) {
                                strictPut(key, value);
                                return;
                            }
                            const auto write = engine_.applyLocalPut(key, value);
                            engine_.replicateCommitted(write);
                        });
                    }

                    void putHinted(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t fp64, uint64_t miniKey) override {
                        engine_.clusterRuntime->executePrimaryWrite([&] {
                            engine_.requireOwnsWriteKey(key);
                            if (engine_.strictPrimaryAckFailWrite()) {
                                strictPut(key, value, fp64, miniKey);
                                return;
                            }
                            const auto write = engine_.applyLocalPut(key, value, fp64, miniKey);
                            engine_.replicateCommitted(write);
                        });
                    }

                    void putBatch(std::span<const BatchPutEntry> entries) override {
                        engine_.clusterRuntime->executePrimaryWrite([&] {
                            engine_.requireOwnsWriteBatch(entries);
                            if (engine_.strictPrimaryAckFailWrite()) {
                                strictPutBatch(entries);
                                return;
                            }
                            const auto writes = engine_.applyLocalPutBatch(entries);
                            for (const auto& write : writes) { engine_.replicateCommitted(write); }
                        });
                    }

                    void remove(std::span<const uint8_t> key) override {
                        engine_.clusterRuntime->executePrimaryWrite([&] {
                            engine_.requireOwnsWriteKey(key);
                            if (engine_.strictPrimaryAckFailWrite()) {
                                strictRemove(key);
                                return;
                            }
                            const auto write = engine_.applyLocalRemove(key);
                            engine_.replicateCommitted(write);
                        });
                    }

                    void rollback(std::span<const uint8_t> key, std::span<const uint8_t> value, bool tombstone) override {
                        engine_.clusterRuntime->executePrimaryWrite([&] {
                            engine_.requireOwnsWriteKey(key);
                            const auto write = tombstone
                                                   ? engine_.applyLocalRemove(key, 0, 0, vlog::ROLLBACK_NODE)
                                                   : engine_.applyLocalPut(key, value, 0, 0, vlog::ROLLBACK_NODE);
                            if (engine_.strictPrimaryAckFailWrite()) { engine_.forceClusterLocalDurable(); }
                            engine_.replicateCommitted(write);
                        });
                    }

                    void removeHinted(std::span<const uint8_t> key, uint64_t fp64, uint64_t miniKey) override {
                        engine_.clusterRuntime->executePrimaryWrite([&] {
                            engine_.requireOwnsWriteKey(key);
                            if (engine_.strictPrimaryAckFailWrite()) {
                                strictRemove(key, fp64, miniKey);
                                return;
                            }
                            const auto write = engine_.applyLocalRemove(key, fp64, miniKey);
                            engine_.replicateCommitted(write);
                        });
                    }

                private:
                    void strictPut(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t fp64 = 0, uint64_t miniKey = 0) {
                        const auto write = engine_.applyLocalPut(key, value, fp64, miniKey);
                        engine_.forceClusterLocalDurable();
                        engine_.replicateCommitted(write);
                    }

                    void strictPutBatch(std::span<const BatchPutEntry> entries) {
                        if (entries.empty()) { return; }
                        const auto writes = engine_.applyLocalPutBatch(entries);
                        engine_.forceClusterLocalDurable();
                        for (const auto& write : writes) { engine_.replicateCommitted(write); }
                    }

                    void strictRemove(std::span<const uint8_t> key, uint64_t fp64 = 0, uint64_t miniKey = 0) {
                        const auto write = engine_.applyLocalRemove(key, fp64, miniKey);
                        engine_.forceClusterLocalDurable();
                        engine_.replicateCommitted(write);
                    }
            };

            class StripeWriteCoordinator final : public WriteCoordinator {
                public:
                    using WriteCoordinator::WriteCoordinator;

                    void put(std::span<const uint8_t> key, std::span<const uint8_t> value) override {
                        engine_.writeStripeValue(key, value);
                    }

                    void putHinted(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t, uint64_t) override {
                        engine_.writeStripeValue(key, value);
                    }

                    void putBatch(std::span<const BatchPutEntry> entries) override {
                        engine_.requireOwnsWriteBatch(entries);
                        for (const auto& entry : entries) { engine_.writeStripeValue(entry.key, entry.value); }
                    }

                    void remove(std::span<const uint8_t> key) override { engine_.removeStripeValue(key); }

                    void rollback(std::span<const uint8_t> key, std::span<const uint8_t> value, bool tombstone) override {
                        engine_.writeStripeValue(key, value, tombstone, true);
                    }

                    void removeHinted(std::span<const uint8_t> key, uint64_t, uint64_t) override { engine_.removeStripeValue(key); }
            };

            class RaftQuorumWriteCoordinator final : public WriteCoordinator {
                public:
                    using WriteCoordinator::WriteCoordinator;
                    cluster::ClusterRequestResult writeWithRequest(const cluster::ClusterRequestId& id, cluster::ReplOpType op,
                        std::span<const uint8_t> key, std::span<const uint8_t> value) override {
                        engine_.requireOwnsWriteKey(key);
                        engine_.applyWriteBackpressure();
                        const uint8_t operation = static_cast<uint8_t>(op);
                        const std::array<std::span<const uint8_t>, 3> parts{std::span<const uint8_t>{&operation, 1}, key, value};
                        const auto fingerprint = crypto::hash256(parts);
                        std::optional<uint8_t> preparedFlags;
                        auto completion = engine_.clusterRuntime->submitRequest(id, fingerprint, [&](uint64_t sequence) {
                            auto mutation = prepareMutation(
                                sequence, op, key, value,
                                op == cluster::ReplOpType::PUT ? MemHdr16::FLAG_NORMAL : MemHdr16::FLAG_TOMBSTONE
                            );
                            preparedFlags = mutation.recordFlags;
                            return mutation;
                        });
                        const auto result = completion.get();
                        if (preparedFlags && result.status == cluster::ClusterRequestStatus::APPLIED) {
                            engine_.recordCommittedRaftMutationStats(op, *preparedFlags);
                        }
                        return result;
                    }

                    void put(std::span<const uint8_t> key, std::span<const uint8_t> value) override {
                        finish(submit(cluster::ReplOpType::PUT, key, value, MemHdr16::FLAG_NORMAL));
                    }

                    void putHinted(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t fp64, uint64_t miniKey) override {
                        (void)fp64;
                        (void)miniKey;
                        finish(submit(cluster::ReplOpType::PUT, key, value, MemHdr16::FLAG_NORMAL));
                    }

                    void putBatch(std::span<const BatchPutEntry> entries) override {
                        for (size_t offset = 0; offset < entries.size();) {
                            std::vector<Pending> pending;
                            const size_t end = std::min(entries.size(), offset + 256);
                            std::exception_ptr failure;
                            try {
                                for (; offset < end; ++offset) {
                                    const auto& [key, value] = entries[offset];
                                    pending.push_back(submit(cluster::ReplOpType::PUT, key, value, MemHdr16::FLAG_NORMAL));
                                }
                            }
                            catch (...) { failure = std::current_exception(); }
                            for (auto& item : pending) {
                                try { finish(std::move(item)); }
                                catch (...) { if (!failure) { failure = std::current_exception(); } }
                            }
                            if (failure) { std::rethrow_exception(failure); }
                        }
                    }

                    void remove(std::span<const uint8_t> key) override {
                        finish(submit(cluster::ReplOpType::REMOVE, key, {}, MemHdr16::FLAG_TOMBSTONE));
                    }

                    void rollback(std::span<const uint8_t> key, std::span<const uint8_t> value, bool tombstone) override {
                        finish(submit(
                            tombstone ? cluster::ReplOpType::REMOVE : cluster::ReplOpType::PUT,
                            key,
                            value,
                            tombstone ? MemHdr16::FLAG_TOMBSTONE : MemHdr16::FLAG_NORMAL,
                            vlog::ROLLBACK_NODE
                        ));
                    }

                    void removeHinted(std::span<const uint8_t> key, uint64_t fp64, uint64_t miniKey) override {
                        (void)fp64;
                        (void)miniKey;
                        finish(submit(cluster::ReplOpType::REMOVE, key, {}, MemHdr16::FLAG_TOMBSTONE));
                    }

                private:
                    struct Pending {
                        cluster::ReplOpType op = cluster::ReplOpType::PUT;
                        uint8_t recordFlags = MemHdr16::FLAG_NORMAL;
                        std::future<void> completion;
                    };

                    Pending submit(cluster::ReplOpType op, std::span<const uint8_t> key,
                        std::span<const uint8_t> value, uint8_t flags, uint64_t sourceNodeId = 0) {
                        engine_.requireOwnsWriteKey(key);
                        engine_.applyWriteBackpressure();
                        uint8_t preparedFlags = flags;
                        auto submission = engine_.clusterRuntime->submitMutation([&](uint64_t sequence) {
                            auto mutation = prepareMutation(sequence, op, key, value, flags, sourceNodeId);
                            preparedFlags = mutation.recordFlags;
                            return mutation;
                        });
                        return Pending{.op = op, .recordFlags = preparedFlags, .completion = std::move(submission.completion)};
                    }

                    void finish(Pending pending) {
                        pending.completion.get();
                        engine_.recordCommittedRaftMutationStats(pending.op, pending.recordFlags);
                    }

                    [[nodiscard]] cluster::ClusterMutation prepareMutation(
                        uint64_t sequence,
                        cluster::ReplOpType op,
                        std::span<const uint8_t> key,
                        std::span<const uint8_t> value,
                        uint8_t flags,
                        uint64_t sourceNodeId = 0
                    ) {
                        cluster::ClusterMutation mutation{
                            .sourceNodeId = sourceNodeId == 0 ? engine_.nodeId : sourceNodeId,
                            .op = op,
                            .recordFlags = flags,
                            .key = {key.begin(), key.end()},
                            .value = {value.begin(), value.end()},
                            .blob = std::nullopt,
                        };
                        if (op == cluster::ReplOpType::PUT) {
                            auto prepared = engine_.prepareRaftValue(sequence, value, mutation.recordFlags);
                            mutation.value = std::move(prepared.stored);
                            mutation.blob = std::move(prepared.blob);
                        }
                        return mutation;
                    }
            };

            #include "detail/PartitionedRaft.inc"
            #include "detail/StripePlacement.inc"

            [[nodiscard]] std::unique_ptr<WriteCoordinator> createWriteCoordinator() {
                if (usesPartitionRaft()) { return std::make_unique<PartitionedRaftWriteCoordinator>(*this); }
                if (clusterConfig.usesDataConsensus()) { return std::make_unique<RaftQuorumWriteCoordinator>(*this); }
                if (!clusterRuntime) { return std::make_unique<LocalUnreplicatedWriteCoordinator>(*this); }
                if (clusterReplicationMode == cluster::ReplicationMode::STRIPE) { return std::make_unique<StripeWriteCoordinator>(*this); }
                return std::make_unique<LocalReplicationWriteCoordinator>(*this);
            }

            std::unique_ptr<WriteCoordinator> writeCoordinator;
            std::atomic<bool> forwardReady{false};

            [[nodiscard]] uint64_t localRollbackWatermark() {
                if (clusterReplicationMode == cluster::ReplicationMode::STRIPE && clusterRuntime) {
                    return clusterRuntime->stripeMetadataLinearizableWatermark();
                }
                return snapshotSeq();
            }

            [[nodiscard]] uint64_t captureRollbackWatermark() {
                std::unique_lock epochLock{mutationEpochMu};
                versionLog->forceSync();
                return localRollbackWatermark();
            }

            [[nodiscard]] static std::vector<uint8_t> encodeRollbackItems(std::span<const RollbackItemResult> items) {
                std::vector<uint8_t> out{'A', 'K', 'R', 'R', '1'};
                pushU32(out, static_cast<uint32_t>(items.size()));
                for (const auto& item : items) {
                    out.push_back(static_cast<uint8_t>(item.status));
                    pushU32(out, static_cast<uint32_t>(item.key.size()));
                    out.insert(out.end(), item.key.begin(), item.key.end());
                    pushU32(out, static_cast<uint32_t>(item.message.size()));
                    out.insert(out.end(), item.message.begin(), item.message.end());
                }
                return out;
            }

            [[nodiscard]] static std::vector<RollbackItemResult> decodeRollbackItems(std::span<const uint8_t> bytes) {
                if (bytes.size() < 9 || std::memcmp(bytes.data(), "AKRR1", 5) != 0) {
                    throw std::runtime_error("AkkEngine: corrupt rollback control response");
                }
                size_t cursor = 5;
                uint32_t count = 0;
                if (!pullU32(bytes, cursor, count)) { throw std::runtime_error("AkkEngine: truncated rollback control response"); }
                if (count > (bytes.size() - cursor) / 9) {
                    throw std::runtime_error("AkkEngine: invalid rollback control result count");
                }
                std::vector<RollbackItemResult> out;
                out.reserve(count);
                for (uint32_t i = 0; i < count; ++i) {
                    if (cursor >= bytes.size()) { throw std::runtime_error("AkkEngine: truncated rollback result status"); }
                    const auto status = static_cast<RollbackItemStatus>(bytes[cursor++]);
                    if (status > RollbackItemStatus::PERMANENT_FAILURE) {
                        throw std::runtime_error("AkkEngine: invalid rollback result status");
                    }
                    uint32_t keySize = 0;
                    uint32_t messageSize = 0;
                    std::vector<uint8_t> key;
                    std::vector<uint8_t> message;
                    if (!pullU32(bytes, cursor, keySize) || !pullBytes(bytes, cursor, keySize, key) ||
                        !pullU32(bytes, cursor, messageSize) || !pullBytes(bytes, cursor, messageSize, message)) {
                        throw std::runtime_error("AkkEngine: truncated rollback result item");
                    }
                    out.push_back(RollbackItemResult{
                        .key = std::move(key),
                        .status = status,
                        .message = std::string{message.begin(), message.end()},
                    });
                }
                if (cursor != bytes.size()) { throw std::runtime_error("AkkEngine: trailing rollback control response"); }
                return out;
            }

            struct RollbackStreamBatch {
                std::vector<RollbackItemResult> items;
                std::vector<uint8_t> nextCursor;
                bool done = true;
            };

            [[nodiscard]] static std::vector<uint8_t> encodeRollbackStreamBatch(
                std::span<const RollbackItemResult> items,
                std::span<const uint8_t> nextCursor,
                bool done
            ) {
                std::vector<uint8_t> out{'A', 'K', 'R', 'B', '1'};
                out.push_back(done ? 1U : 0U);
                pushU32(out, static_cast<uint32_t>(nextCursor.size()));
                out.insert(out.end(), nextCursor.begin(), nextCursor.end());
                auto encodedItems = encodeRollbackItems(items);
                out.insert(out.end(), encodedItems.begin(), encodedItems.end());
                return out;
            }

            [[nodiscard]] static RollbackStreamBatch decodeRollbackStreamBatch(std::span<const uint8_t> bytes) {
                if (bytes.size() < 19 || std::memcmp(bytes.data(), "AKRB1", 5) != 0 || bytes[5] > 1) {
                    throw std::runtime_error("AkkEngine: corrupt rollback stream response");
                }
                size_t cursor = 6;
                uint32_t cursorSize = 0;
                RollbackStreamBatch out;
                out.done = bytes[5] != 0;
                if (!pullU32(bytes, cursor, cursorSize) || !pullBytes(bytes, cursor, cursorSize, out.nextCursor)) {
                    throw std::runtime_error("AkkEngine: truncated rollback stream cursor");
                }
                out.items = decodeRollbackItems(bytes.subspan(cursor));
                if (out.done != out.nextCursor.empty()) {
                    throw std::runtime_error("AkkEngine: invalid rollback stream continuation");
                }
                return out;
            }

            void persistRollbackJournalLocked() const {
                if (opts.paths.rollbackJournalPath.empty()) {
                    if (!deferredRollbacks.empty()) {
                        throw std::runtime_error("AkkEngine: rollback journal path is required for deferred rollback");
                    }
                    return;
                }
                std::vector<uint8_t> bytes{'A', 'K', 'R', 'J', '1'};
                pushU32(bytes, static_cast<uint32_t>(deferredRollbacks.size()));
                for (const auto& task : deferredRollbacks) {
                    bytes.insert(bytes.end(), task.taskId.begin(), task.taskId.end());
                    bytes.insert(bytes.end(), task.operationId.begin(), task.operationId.end());
                    bytes.insert(bytes.end(), task.createdStartupId.begin(), task.createdStartupId.end());
                    bytes.insert(bytes.end(), task.clusterId.begin(), task.clusterId.end());
                    pushU64(bytes, task.configEpoch);
                    bytes.push_back(static_cast<uint8_t>(task.action));
                    bytes.push_back(task.clusterTask ? 1U : 0U);
                    bytes.push_back(static_cast<uint8_t>(task.conflict));
                    pushU64(bytes, task.targetNodeId);
                    pushU64(bytes, task.targetSeq);
                    pushU64(bytes, task.plannedWatermark);
                    pushU32(bytes, static_cast<uint32_t>(task.key.size()));
                    bytes.insert(bytes.end(), task.key.begin(), task.key.end());
                }
                const uint32_t crc = cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size());
                pushU32(bytes, crc);
                writeFileAtomicallyDurable(opts.paths.rollbackJournalPath, bytes);
            }

            void loadRollbackJournal() {
                crypto::secureRandom(rollbackStartupId);
                if (opts.paths.rollbackJournalPath.empty() || !fs::exists(opts.paths.rollbackJournalPath)) { return; }
                std::ifstream in{opts.paths.rollbackJournalPath, std::ios::binary};
                if (!in) { throw std::runtime_error("AkkEngine: failed to open rollback journal"); }
                std::vector<uint8_t> bytes{
                    std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{}
                };
                if (bytes.size() < 13 || std::memcmp(bytes.data(), "AKRJ1", 5) != 0) {
                    throw std::runtime_error("AkkEngine: corrupt rollback journal header");
                }
                size_t crcCursor = bytes.size() - 4;
                uint32_t storedCrc = 0;
                if (!pullU32(bytes, crcCursor, storedCrc) ||
                    storedCrc != cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size() - 4)) {
                    throw std::runtime_error("AkkEngine: rollback journal checksum mismatch");
                }
                size_t cursor = 5;
                uint32_t count = 0;
                if (!pullU32(bytes, cursor, count)) { throw std::runtime_error("AkkEngine: truncated rollback journal"); }
                if (count > (bytes.size() - 13) / 103) {
                    throw std::runtime_error("AkkEngine: invalid rollback journal task count");
                }
                deferredRollbacks.clear();
                deferredRollbacks.reserve(count);
                for (uint32_t i = 0; i < count; ++i) {
                    DeferredRollbackTask task;
                    if (cursor + 75 > bytes.size() - 4) { throw std::runtime_error("AkkEngine: truncated rollback journal task"); }
                    std::copy_n(bytes.begin() + static_cast<std::ptrdiff_t>(cursor), 16, task.taskId.begin());
                    cursor += 16;
                    std::copy_n(bytes.begin() + static_cast<std::ptrdiff_t>(cursor), 16, task.operationId.begin());
                    cursor += 16;
                    std::copy_n(bytes.begin() + static_cast<std::ptrdiff_t>(cursor), 16, task.createdStartupId.begin());
                    cursor += 16;
                    std::copy_n(bytes.begin() + static_cast<std::ptrdiff_t>(cursor), 16, task.clusterId.begin());
                    cursor += 16;
                    if (!pullU64(bytes, cursor, task.configEpoch)) {
                        throw std::runtime_error("AkkEngine: truncated rollback journal cluster epoch");
                    }
                    task.action = static_cast<cluster::StripeControlAction>(bytes[cursor++]);
                    task.clusterTask = bytes[cursor++] != 0;
                    task.conflict = static_cast<RollbackConflictPolicy>(bytes[cursor++]);
                    uint32_t keySize = 0;
                    if ((task.action != cluster::StripeControlAction::ROLLBACK_KEY &&
                         task.action != cluster::StripeControlAction::ROLLBACK_STREAM) ||
                        task.conflict > RollbackConflictPolicy::OVERWRITE_LATEST ||
                        !pullU64(bytes, cursor, task.targetNodeId) || !pullU64(bytes, cursor, task.targetSeq) ||
                        !pullU64(bytes, cursor, task.plannedWatermark) || !pullU32(bytes, cursor, keySize) ||
                        keySize > bytes.size() - 4 - cursor) {
                        throw std::runtime_error("AkkEngine: invalid rollback journal task");
                    }
                    task.key.assign(
                        bytes.begin() + static_cast<std::ptrdiff_t>(cursor),
                        bytes.begin() + static_cast<std::ptrdiff_t>(cursor + keySize)
                    );
                    cursor += keySize;
                    deferredRollbacks.push_back(std::move(task));
                }
                if (cursor != bytes.size() - 4) { throw std::runtime_error("AkkEngine: trailing rollback journal data"); }
            }

            void enqueueDeferredRollback(DeferredRollbackTask task) {
                if (task.taskId == std::array<uint8_t, 16>{}) { crypto::secureRandom(task.taskId); }
                task.createdStartupId = rollbackStartupId;
                std::lock_guard lock{rollbackJournalMu};
                deferredRollbacks.push_back(std::move(task));
                try { persistRollbackJournalLocked(); }
                catch (...) {
                    deferredRollbacks.pop_back();
                    throw;
                }
            }

            [[nodiscard]] std::array<uint8_t, 16> scheduleDeferredRollback(
                const std::array<uint8_t, 16>& operationId,
                cluster::StripeControlAction action,
                bool clusterTask,
                uint64_t targetNodeId,
                uint64_t targetSeq,
                uint64_t plannedWatermark,
                RollbackConflictPolicy conflict,
                std::span<const uint8_t> key = {}
            ) {
                DeferredRollbackTask task;
                crypto::secureRandom(task.taskId);
                task.operationId = operationId;
                task.action = action;
                task.clusterTask = clusterTask;
                if (clusterTask) {
                    task.clusterId = clusterConfig.clusterId();
                    task.configEpoch = clusterRuntime->configurationEpoch();
                }
                task.targetNodeId = targetNodeId;
                task.targetSeq = targetSeq;
                task.plannedWatermark = plannedWatermark;
                task.conflict = conflict;
                task.key.assign(key.begin(), key.end());
                const auto taskId = task.taskId;
                enqueueDeferredRollback(std::move(task));
                return taskId;
            }

            void completeDeferredRollback(const std::array<uint8_t, 16>& taskId) {
                std::lock_guard lock{rollbackJournalMu};
                const auto previous = deferredRollbacks;
                std::erase_if(deferredRollbacks, [&](const DeferredRollbackTask& task) { return task.taskId == taskId; });
                try { persistRollbackJournalLocked(); }
                catch (...) {
                    deferredRollbacks = previous;
                    throw;
                }
            }

            [[nodiscard]] bool executeDeferredRollback(const DeferredRollbackTask& task, std::stop_token stop) {
                if (task.clusterTask) {
                    if (!clusterRuntime) { return false; }
                    uint64_t controlNode = task.targetNodeId;
                    if (clusterReplicationMode == cluster::ReplicationMode::STRIPE) {
                        controlNode = clusterRuntime->stripeMetadataLeaderNodeId();
                        if (controlNode == 0) { return false; }
                    }
                    else if (clusterReplicationMode != cluster::ReplicationMode::PARTITIONED) {
                        if (clusterConfig.usesDataConsensus()) {
                            controlNode = clusterRuntime->raftStats().leaderNodeId;
                            if (controlNode == 0) { return false; }
                        }
                        else {
                            if (clusterRuntime->role() != cluster::NodeRole::PRIMARY) { return false; }
                            controlNode = nodeId;
                        }
                    }
                    std::vector<uint8_t> cursor = task.key;
                    for (;;) {
                        if (stop.stop_requested()) { return false; }
                        cluster::StripeControlRequest request;
                        request.action = task.action;
                        request.ownerNodeId = controlNode;
                        request.fenceToken = task.targetSeq;
                        request.key = cursor;
                        request.metadata = {
                            static_cast<uint8_t>(RollbackExecutionMode::IMMEDIATE),
                            static_cast<uint8_t>(task.conflict),
                        };
                        pushU64(request.metadata, task.plannedWatermark);
                        const auto response = clusterRuntime->rollbackControl(controlNode, std::move(request));
                        if (response.status != cluster::StripeControlStatus::COMMITTED) { return false; }
                        if (task.action != cluster::StripeControlAction::ROLLBACK_STREAM) { return true; }
                        auto batch = decodeRollbackStreamBatch(response.metadata);
                        if (batch.done) { return true; }
                        cursor = std::move(batch.nextCursor);
                    }
                }
                std::unique_lock epochLock{mutationEpochMu};
                versionLog->forceSync();
                if (stop.stop_requested()) { return false; }
                if (task.action == cluster::StripeControlAction::ROLLBACK_KEY) {
                    (void)rollbackKeyLocal(task.key, task.targetSeq, task.conflict, 0, task.plannedWatermark);
                }
                else {
                    for (const auto& key : rollbackKeysChangedAfter(task.targetSeq)) {
                        if (stop.stop_requested()) { return false; }
                        (void)rollbackKeyLocal(key, task.targetSeq, task.conflict, 0, task.plannedWatermark);
                    }
                }
                return true;
            }

            void startRollbackRecovery() {
                if (deferredRollbacks.empty()) { return; }
                rollbackRecoveryThread = std::jthread([this](std::stop_token stop) {
                    while (!stop.stop_requested()) {
                        std::vector<DeferredRollbackTask> tasks;
                        {
                            std::lock_guard lock{rollbackJournalMu};
                            for (const auto& task : deferredRollbacks) {
                                if (task.createdStartupId != rollbackStartupId) { tasks.push_back(task); }
                            }
                        }
                        for (const auto& task : tasks) {
                            if (stop.stop_requested()) { return; }
                            bool complete = false;
                            try { complete = executeDeferredRollback(task, stop); }
                            catch (...) {}
                            if (!complete) { continue; }
                            std::lock_guard lock{rollbackJournalMu};
                            auto previous = deferredRollbacks;
                            std::erase_if(deferredRollbacks, [&](const DeferredRollbackTask& candidate) {
                                return candidate.taskId == task.taskId;
                            });
                            try { persistRollbackJournalLocked(); }
                            catch (...) { deferredRollbacks = std::move(previous); }
                        }
                        std::unique_lock lock{lifecycleMu};
                        lifecycleCv.wait_for(lock, std::chrono::seconds{1});
                    }
                });
            }

            [[nodiscard]] std::optional<StripeMetadata> stripeMetadataAt(
                std::span<const uint8_t> publicKey,
                uint64_t logicalSeq
            ) {
                std::optional<StripeMetadata> selected;
                if (!versionLog) { return selected; }
                for (const auto& entry : versionLog->history(stripeMetaKey(publicKey))) {
                    auto metadata = decodeStripeMetadata(entry.value);
                    if (metadata) { *metadata = effectiveStripeMetadata(publicKey, *metadata); }
                    if (!metadata || metadata->logicalSeq == 0 || metadata->logicalSeq > logicalSeq) { continue; }
                    if (!selected || selected->logicalSeq <= metadata->logicalSeq) { selected = std::move(metadata); }
                }
                return selected;
            }

            [[nodiscard]] bool stripeHistoryUnavailableBefore(
                std::span<const uint8_t> publicKey,
                uint64_t logicalSeq
            ) const {
                if (!versionLog) { return true; }
                for (const auto& entry : versionLog->history(stripeMetaKey(publicKey))) {
                    const auto metadata = decodeStripeMetadata(entry.value);
                    if (metadata && metadata->logicalSeq > logicalSeq &&
                        (entry.flags & vlog::VLOG_FLAG_RETENTION_BASE) != 0) {
                        return true;
                    }
                }
                return false;
            }

            [[nodiscard]] RollbackItemResult applyMaterializedRollback(
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                bool tombstone,
                uint64_t expectedHead,
                RollbackConflictPolicy conflict
            ) {
                RollbackItemResult result{.key = {key.begin(), key.end()}, .message = {}};
                uint64_t currentHead = 0;
                if (clusterReplicationMode == cluster::ReplicationMode::STRIPE) {
                    if (auto current = readStripeMetadata(key)) { currentHead = current->logicalSeq; }
                }
                else if (versionLog) {
                    if (const auto head = versionLog->getAt(key, UINT64_MAX)) { currentHead = head->seq; }
                }
                if (conflict == RollbackConflictPolicy::FAIL_IF_CHANGED && expectedHead != 0 && currentHead != expectedHead) {
                    result.status = RollbackItemStatus::CONFLICT;
                    result.message = "key changed after rollback planning";
                    return result;
                }
                writeCoordinator->rollback(key, value, tombstone);
                result.status = RollbackItemStatus::APPLIED;
                return result;
            }

            [[nodiscard]] RollbackItemResult rollbackKeyLocal(
                std::span<const uint8_t> key,
                uint64_t targetSeq,
                RollbackConflictPolicy conflict,
                uint64_t expectedHead = 0,
                uint64_t maximumAllowedHead = UINT64_MAX
            ) {
                if (!versionLog) {
                    return RollbackItemResult{
                        .key = {key.begin(), key.end()},
                        .status = RollbackItemStatus::PERMANENT_FAILURE,
                        .message = "version log is disabled",
                    };
                }
                if (clusterReplicationMode == cluster::ReplicationMode::STRIPE) {
                    const auto current = readStripeMetadata(key);
                    const uint64_t currentHead = current ? current->logicalSeq : 0;
                    if (conflict == RollbackConflictPolicy::FAIL_IF_CHANGED && currentHead > maximumAllowedHead) {
                        return RollbackItemResult{
                            .key = {key.begin(), key.end()},
                            .status = RollbackItemStatus::CONFLICT,
                            .message = "key changed after rollback was deferred",
                        };
                    }
                    if (expectedHead == 0) { expectedHead = currentHead; }
                    if (currentHead <= targetSeq) {
                        return RollbackItemResult{
                            .key = {key.begin(), key.end()}, .status = RollbackItemStatus::APPLIED, .message = {}
                        };
                    }
                    const auto target = stripeMetadataAt(key, targetSeq);
                    if (!target && stripeHistoryUnavailableBefore(key, targetSeq)) {
                        return RollbackItemResult{
                            .key = {key.begin(), key.end()},
                            .status = RollbackItemStatus::PERMANENT_FAILURE,
                            .message = "target revision predates retained STRIPE metadata history",
                        };
                    }
                    if (!target || target->tombstone) {
                        return applyMaterializedRollback(key, {}, true, expectedHead, conflict);
                    }
                    try {
                        const auto value = readStripeValueFromMetadata(key, *target, false);
                        if (!value) { throw std::runtime_error("AkkEngine: historical STRIPE value is absent"); }
                        return applyMaterializedRollback(key, *value, false, expectedHead, conflict);
                    }
                    catch (const std::exception& error) {
                        return RollbackItemResult{
                            .key = {key.begin(), key.end()},
                            .status = RollbackItemStatus::PERMANENT_FAILURE,
                            .message = error.what(),
                        };
                    }
                }

                const auto head = versionLog->getAt(key, UINT64_MAX);
                const uint64_t currentHead = head ? head->seq : 0;
                if (conflict == RollbackConflictPolicy::FAIL_IF_CHANGED && currentHead > maximumAllowedHead) {
                    return RollbackItemResult{
                        .key = {key.begin(), key.end()},
                        .status = RollbackItemStatus::CONFLICT,
                        .message = "key changed after rollback was deferred",
                    };
                }
                if (expectedHead == 0) { expectedHead = currentHead; }
                if (currentHead <= targetSeq) {
                    return RollbackItemResult{
                        .key = {key.begin(), key.end()}, .status = RollbackItemStatus::APPLIED, .message = {}
                    };
                }
                const auto target = versionLog->getAt(key, targetSeq);
                if (!target || (target->flags & MemHdr16::FLAG_TOMBSTONE) != 0) {
                    auto history = versionLog->history(key);
                    const auto first = history.begin();
                    if (first != std::default_sentinel && first->seq > targetSeq &&
                        (first->flags & vlog::VLOG_FLAG_RETENTION_BASE) != 0) {
                        return RollbackItemResult{
                            .key = {key.begin(), key.end()},
                            .status = RollbackItemStatus::PERMANENT_FAILURE,
                            .message = "target revision predates retained history",
                        };
                    }
                    return applyMaterializedRollback(key, {}, true, expectedHead, conflict);
                }
                auto value = resolveValue(target->flags, target->value);
                if (!value) {
                    return RollbackItemResult{
                        .key = {key.begin(), key.end()},
                        .status = RollbackItemStatus::PERMANENT_FAILURE,
                        .message = "historical value is unavailable",
                    };
                }
                return applyMaterializedRollback(key, *value, false, expectedHead, conflict);
            }

            [[nodiscard]] RollbackItemResult rollbackStripeCoordinated(
                std::span<const uint8_t> key,
                uint64_t targetSeq,
                RollbackConflictPolicy conflict,
                uint64_t maximumAllowedHead = UINT64_MAX
            ) {
                const auto current = readStripeMetadata(key);
                const uint64_t expectedHead = current ? current->logicalSeq : 0;
                if (conflict == RollbackConflictPolicy::FAIL_IF_CHANGED && expectedHead > maximumAllowedHead) {
                    return RollbackItemResult{
                        .key = {key.begin(), key.end()},
                        .status = RollbackItemStatus::CONFLICT,
                        .message = "key changed after rollback was deferred",
                    };
                }
                if (expectedHead <= targetSeq) {
                    return RollbackItemResult{
                        .key = {key.begin(), key.end()}, .status = RollbackItemStatus::APPLIED, .message = {}
                    };
                }
                bool tombstone = true;
                std::vector<uint8_t> value;
                const auto target = stripeMetadataAt(key, targetSeq);
                if (!target && stripeHistoryUnavailableBefore(key, targetSeq)) {
                    return RollbackItemResult{
                        .key = {key.begin(), key.end()},
                        .status = RollbackItemStatus::PERMANENT_FAILURE,
                        .message = "target revision predates retained STRIPE metadata history",
                    };
                }
                if (target && !target->tombstone) {
                    try {
                        auto historical = readStripeValueFromMetadata(key, *target, false);
                        if (!historical) { throw std::runtime_error("AkkEngine: historical STRIPE value is absent"); }
                        value = std::move(*historical);
                    }
                    catch (const std::exception& error) {
                        return RollbackItemResult{
                            .key = {key.begin(), key.end()},
                            .status = RollbackItemStatus::PERMANENT_FAILURE,
                            .message = error.what(),
                        };
                    }
                    tombstone = false;
                }

                uint64_t authorityNodeId = clusterRuntime->ownerNodeId(key);
                if (!clusterRuntime->stripeNodeReachable(authorityNodeId)) {
                    const uint64_t failover = clusterRuntime->stripeFailoverNodeId();
                    if (failover != 0) { authorityNodeId = failover; }
                }
                cluster::StripeControlRequest request;
                request.action = cluster::StripeControlAction::ROLLBACK_APPLY;
                request.ownerNodeId = authorityNodeId;
                request.key.assign(key.begin(), key.end());
                request.metadata.push_back(static_cast<uint8_t>(conflict));
                pushU64(request.metadata, expectedHead);
                request.metadata.push_back(tombstone ? 1U : 0U);
                request.metadata.insert(request.metadata.end(), value.begin(), value.end());
                if (authorityNodeId == nodeId) {
                    return applyMaterializedRollback(key, value, tombstone, expectedHead, conflict);
                }
                const auto response = clusterRuntime->rollbackControl(authorityNodeId, std::move(request));
                if (response.status != cluster::StripeControlStatus::COMMITTED) {
                    throw std::runtime_error("AkkEngine: rollback authority rejected the mutation");
                }
                auto items = decodeRollbackItems(response.metadata);
                if (items.size() != 1) { throw std::runtime_error("AkkEngine: invalid rollback authority response"); }
                return std::move(items.front());
            }

            [[nodiscard]] std::vector<std::vector<uint8_t>> rollbackKeysChangedAfter(uint64_t targetSeq) {
                std::vector<std::vector<uint8_t>> keys;
                if (clusterReplicationMode == cluster::ReplicationMode::STRIPE) {
                    core::BufferArena arena;
                    memtable::MemTable::KeyRange range;
                    const uint64_t visibleSeq = snapshotSeq();
                    auto mt = memtable->iterator(range, visibleSeq);
                    sst::SSTManager::Iterator sst;
                    if (sstManager) { sst = sstManager->scanIter({}, {}, visibleSeq); }
                    for (const auto& record : scanGenerator(arena, std::move(mt), std::move(sst), blobManager.get())) {
                        auto publicKey = publicKeyFromStripeMetaKey(record.key);
                        if (!publicKey) { continue; }
                        const auto metadata = decodeStripeMetadata(record.value);
                        if (!metadata) { throw std::runtime_error("AkkEngine: corrupt STRIPE metadata history head"); }
                        if (metadata->logicalSeq > targetSeq) { keys.push_back(std::move(*publicKey)); }
                    }
                    std::ranges::sort(keys);
                    return keys;
                }
                for (auto& [key, _] : versionLog->collectRollbackTargets(targetSeq)) {
                    if (isStripeInternalKey(key)) { continue; }
                    if (clusterReplicationMode == cluster::ReplicationMode::PARTITIONED && clusterRuntime &&
                        clusterRuntime->ownerNodeId(key) != nodeId) { continue; }
                    keys.push_back(std::move(key));
                }
                std::ranges::sort(keys);
                return keys;
            }
    };

    AkkEngine::AkkEngine() = default;

    bool RollbackResult::complete() const noexcept {
        return std::ranges::all_of(items, [](const RollbackItemResult& item) {
            return item.status == RollbackItemStatus::APPLIED;
        });
    }

    size_t RollbackResult::appliedCount() const noexcept {
        return static_cast<size_t>(std::ranges::count_if(items, [](const RollbackItemResult& item) {
            return item.status == RollbackItemStatus::APPLIED;
        }));
    }

    size_t RollbackResult::deferredCount() const noexcept {
        return static_cast<size_t>(std::ranges::count_if(items, [](const RollbackItemResult& item) {
            return item.status == RollbackItemStatus::DEFERRED;
        }));
    }

    #include "detail/TransactionApi.inc"

    AkkEngine::~AkkEngine() {
        try { close(); }
        catch (...) {}
    }

    std::unique_ptr<AkkEngine> AkkEngine::open(AkkEngineOptions options) {
        const auto& tx = options.transactions;
        if (tx.maxOpen == 0 || tx.maxOpen > 1024 || tx.maxLifetimeMs == 0 || tx.maxLifetimeMs > 3'600'000 ||
            tx.maxWrites == 0 || tx.maxWrites > 1'048'576 || tx.maxWriteBytes < 1024 || tx.maxWriteBytes > 512ull * 1024 * 1024 ||
            tx.maxReadSetBytes < 1024 || tx.maxReadSetBytes > 512ull * 1024 * 1024 ||
            tx.maxPinnedBytes < 1024 * 1024 || tx.maxPinnedBytes > 1024ull * 1024 * 1024 * 1024 ||
            tx.maxTrackedBytes < 1024 * 1024 || tx.maxTrackedBytes > 512ull * 1024 * 1024 ||
            tx.maxJournalBytes < 1024 || tx.maxJournalBytes > 1024ull * 1024 * 1024 * 1024) {
            throw std::invalid_argument("AkkEngine: invalid transaction runtime limits");
        }
        resolveWritePolicy(options);
        normalizeVisibilityOptions(options);
        if (options.runtime.generationLayoutEnabled && !options.paths.dataDir.empty()) {
            options.paths.dataDir = generation::StorageGeneration::openOrCreate(options.paths.dataDir).activePath();
        }
        auto fillPath = [&](fs::path& target, const char* fallback) {
            if (target.empty() && !options.paths.dataDir.empty()) { target = options.paths.dataDir / fallback; }
        };

        fillPath(options.paths.walDir, "wal");
        fillPath(options.paths.blobDir, "blobs");
        fillPath(options.paths.sstDir, "sstable");
        fillPath(options.paths.manifestPath, "manifest.akmf");
        fillPath(options.paths.versionLogPath, "history.akvlog");
        fillPath(options.paths.clusterConfigPath, "cluster.akcc");
        fillPath(options.paths.nodeIdPath, "node.id");
        fillPath(options.paths.rollbackJournalPath, "rollback-journal.akrb");
        fillPath(options.paths.transactionDir, "transactions");

        if (options.runtime.writerThreads > 0) {
            if (options.memtable.expectedConcurrentWriters == 0) {
                options.memtable.expectedConcurrentWriters = options.runtime.writerThreads;
            }
            if (options.memtable.shardCount == 0) { options.memtable.shardCount = shardsForThreads(options.runtime.writerThreads, 256); }
            if (options.wal.shardCount == 0) {
                options.wal.shardCount = static_cast<uint16_t>(shardsForThreads(options.runtime.writerThreads, 64));
            }
        }
        normalizeWalOptions(options.wal);
        normalizeMemTableOptions(options.memtable);
        normalizeSstOptions(options.sst);
        validateRuntimeOptions(options);

        if (!options.paths.dataDir.empty()) { ensureDir(options.paths.dataDir); }
        if (options.components.walEnabled) { ensureDir(options.paths.walDir); }
        if (options.components.blobEnabled) { ensureDir(options.paths.blobDir); }
        if (options.components.sstEnabled) { ensureDir(options.paths.sstDir); }
        if (options.components.manifestEnabled) { ensureDir(options.paths.manifestPath.parent_path()); }
        if (options.components.versionLogEnabled && options.paths.versionLogPath.empty()) {
            throw std::invalid_argument("AkkEngine: version log path is required when components.versionLogEnabled is true");
        }
        if (options.components.versionLogEnabled) { ensureDir(options.paths.versionLogPath.parent_path()); }
        if (options.components.apiEnabled && options.api.bindHost.empty()) {
            throw std::invalid_argument("AkkEngine: api.bindHost is required when components.apiEnabled is true");
        }
        if (options.runtime.writeAdmission == AkkEngineOptions::WriteAdmissionMode::PARALLEL && !supportsParallelWriteAdmission(options)) {
            throw std::invalid_argument("AkkEngine: runtime.writeAdmission=PARALLEL requires blob and cluster to be disabled");
        }
        auto engine = std::unique_ptr < AkkEngine >
        {
            new AkkEngine()
        };
        engine->impl_ = std::make_unique<Impl>(std::move(options));
        Impl& impl = *engine->impl_;
        impl.loadRollbackJournal();
        if (!impl.deferredRollbacks.empty() && !impl.opts.components.versionLogEnabled) {
            throw std::runtime_error("AkkEngine: deferred rollback journal requires the version log to remain enabled");
        }
        impl.nodeId = loadOrCreateNodeId(impl.opts.paths.nodeIdPath);
        if (impl.opts.components.clusterEnabled) {
            if (!impl.opts.cluster.config) {
                impl.opts.cluster.config = cluster::ClusterConfig::load(impl.opts.paths.clusterConfigPath);
            }
            impl.opts.cluster.config->validateRuntime(impl.nodeId, impl.opts.cluster.runtime);
            if (impl.opts.cluster.runtime.mirrorFencing.mode == cluster::MirrorFencingMode::QUORUM_FENCED &&
                (!impl.opts.components.walEnabled || !impl.opts.runtime.recoverWal)) {
                throw std::invalid_argument("AkkEngine: MIRROR quorum fencing requires WAL durability and WAL recovery");
            }
        }
        for (const auto& task : impl.deferredRollbacks) {
            if (task.clusterTask != impl.opts.components.clusterEnabled) {
                throw std::runtime_error("AkkEngine: deferred rollback journal does not match the current cluster mode");
            }
            if (task.clusterTask) {
                const uint64_t epoch = impl.opts.cluster.runtime.clusterGroupEpoch == 0
                                           ? 1
                                           : impl.opts.cluster.runtime.clusterGroupEpoch;
                if (task.clusterId != impl.opts.cluster.config->clusterId() || task.configEpoch != epoch) {
                    throw std::runtime_error("AkkEngine: deferred rollback journal belongs to a different cluster configuration");
                }
            }
        }
        if (!impl.opts.components.clusterEnabled || !impl.opts.cluster.config || !impl.opts.cluster.config->usesDataConsensus()) {
            removeFileIfExists(impl.replicaSnapshotStagingPath());
        }
        {
            std::error_code ec;
            fs::remove_all(impl.clusterSnapshotExportDirectory(), ec);
        }
        if (impl.opts.components.manifestEnabled && !impl.opts.paths.manifestPath.empty()) {
            impl.manifest = manifest::Manifest::create(impl.opts.paths.manifestPath, impl.opts.manifest.fastMode);
            impl.manifest->start();
        }

        if (impl.opts.components.sstEnabled && !impl.opts.paths.sstDir.empty()) {
            impl.sstManager = sst::SSTManager::create(impl.opts.paths.sstDir, impl.opts.sst, impl.manifest.get());
            if (impl.opts.runtime.recoverSst) { impl.sstManager->recover(); }
        }

        if (impl.sstManager) {
            auto afterFlush = [&impl](uint64_t checkpointSeq) {
                if (checkpointSeq == 0) { return; }
                crashAtTestPoint("engine.flush.after_sst_flush");
                if (impl.walWriter && impl.opts.runtime.pruneWalOnFlush) { impl.walWriter->pruneUntil(checkpointSeq); }
                crashAtTestPoint("engine.flush.after_wal_prune");
                if (impl.manifest) { impl.manifest->checkpoint(std::optional<std::string>{"flush"}, std::nullopt, checkpointSeq); }
                crashAtTestPoint("engine.flush.after_manifest_checkpoint");
                if (impl.blobManager && impl.opts.blob.gcOnFlush) { impl.runBlobGcIfSafe(); }
            };
            if (impl.opts.memtable.flushInputMode == memtable::MemTableFlushInputMode::STREAMING) {
                impl.opts.memtable.onFlushStream = [&, afterFlush](
                    size_t estimatedRecordCount,
                    core::ArenaGenerator<core::RecordView> records
                ) {
                        afterFlush(impl.sstManager->flush(estimatedRecordCount, std::move(records)));
                    };
                impl.opts.memtable.onFlush = {};
            }
            else {
                impl.opts.memtable.onFlush = [&, afterFlush](std::span<const core::RecordView> records) {
                    if (records.empty()) { return; }
                    afterFlush(impl.sstManager->flush(records));
                };
                impl.opts.memtable.onFlushStream = {};
            }
        }
        else {
            impl.opts.memtable.onFlush = {};
            impl.opts.memtable.onFlushStream = {};
        }
        impl.memtable = memtable::MemTable::create(impl.opts.memtable);
        uint64_t manifestCheckpointSeq = 0;
        if (impl.manifest) {
            const auto checkpoint = impl.manifest->lastCheckpoint();
            if (checkpoint.has_value() && checkpoint->lastSeq.has_value()) { manifestCheckpointSeq = *checkpoint->lastSeq; }
        }
        uint64_t walRecoveryCheckpointSeq = 0;
        if (manifestCheckpointSeq > 0 && impl.sstManager && impl.opts.runtime.recoverSst) {
            const uint64_t sstMaxSeq = impl.sstManager->maxSequence();
            if (manifestCheckpointSeq > sstMaxSeq) {
                throw std::runtime_error(
                    "AkkEngine: manifest checkpoint sequence exceeds recovered SST sequence (checkpoint=" + std::to_string(
                        manifestCheckpointSeq
                    ) + ", sstMaxSeq=" + std::to_string(sstMaxSeq) + ")"
                );
            }
            walRecoveryCheckpointSeq = manifestCheckpointSeq;
        }
        uint64_t preWalRecoveredSeq = 0;
        if (impl.sstManager) { preWalRecoveredSeq = std::max(preWalRecoveredSeq, impl.sstManager->maxSequence()); }
        std::exception_ptr versionLogStartupError;
        if (impl.opts.components.versionLogEnabled) {
            impl.opts.vlog.initialCommittedSeq = std::max(impl.opts.vlog.initialCommittedSeq, preWalRecoveredSeq);
            try { impl.versionLog = vlog::VersionLog::create(impl.opts.paths.versionLogPath, impl.opts.vlog); }
            catch (...) { versionLogStartupError = std::current_exception(); }
        }
        bool walRecoveryHadCorruption = false;
        if (impl.opts.components.walEnabled && impl.opts.runtime.recoverWal) {
            const auto recovery = wal::WalRecovery::recoverInto(
                wal::WalRecoveryOptions{
                    .walDir = impl.opts.paths.walDir,
                    .checkpointSeq = walRecoveryCheckpointSeq,
                    .truncateCorruptTail = impl.opts.runtime.truncateCorruptWalOnRecovery
                },
                *impl.memtable
            );
            walRecoveryHadCorruption = recovery.corruptSegments > 0;
            if (walRecoveryHadCorruption && impl.versionLog) {
                try {
                    const uint64_t versionLogSupplementAfterSeq = std::max(walRecoveryCheckpointSeq, recovery.maxSeq);
                    const auto records = impl.versionLog->collectSince(versionLogSupplementAfterSeq);
                    for (const auto& record : records) {
                        const uint8_t flags = static_cast<uint8_t>(record.entry.flags & (MemHdr16::FLAG_TOMBSTONE | MemHdr16::FLAG_BLOB));
                        const std::span<const uint8_t> key{record.key.data(), record.key.size()};
                        const uint64_t fp64 = core::computeKeyFp64(key);
                        const uint64_t mini = core::buildMiniKey(key);
                        if ((flags & MemHdr16::FLAG_TOMBSTONE) != 0) { impl.memtable->remove(key, record.entry.seq, fp64, mini); }
                        else {
                            impl.memtable->put(
                                key,
                                std::span<const uint8_t>{record.entry.value.data(), record.entry.value.size()},
                                record.entry.seq,
                                flags,
                                fp64,
                                mini
                            );
                        }
                    }
                }
                catch (...) {
                    if (!impl.opts.runtime.ignoreVersionLogSupplementErrors) { throw; }
                    impl.versionLog.reset();
                }
            }
        }
        if (versionLogStartupError) {
            if (!walRecoveryHadCorruption || !impl.opts.runtime.ignoreVersionLogSupplementErrors) {
                std::rethrow_exception(versionLogStartupError);
            }
        }
        uint64_t recoveredSeq = 0;
        if (impl.memtable) {
            const uint64_t nextMemtableSeq = impl.memtable->lastSeq();
            recoveredSeq = nextMemtableSeq > 0 ? nextMemtableSeq - 1 : 0;
        }
        if (impl.sstManager) { recoveredSeq = std::max(recoveredSeq, impl.sstManager->maxSequence()); }
        if (impl.memtable && recoveredSeq > 0) { impl.memtable->advanceSeq(recoveredSeq); }
        impl.resetCommittedSeq(recoveredSeq);
        if (impl.versionLog) { impl.versionLog->seedCommittedSeq(recoveredSeq); }

        if (impl.opts.components.walEnabled) { impl.walWriter = wal::WalWriter::create(impl.opts.paths.walDir, impl.opts.wal); }
        if (impl.walWriter && impl.opts.runtime.pruneWalOnFlush && walRecoveryCheckpointSeq > 0) {
            impl.walWriter->pruneUntil(walRecoveryCheckpointSeq);
        }

        if (impl.opts.components.blobEnabled) {
            impl.opts.blob.onBlobPut = [&impl](
                uint64_t blobId,
                uint64_t totalSize,
                uint64_t storedSize,
                uint32_t contentCrc32c,
                uint32_t codec
            ) {
                    if (impl.manifest) { impl.manifest->blobPut(blobId, totalSize, storedSize, contentCrc32c, codec); }
                };
            impl.opts.blob.onBlobDelete = [&impl](uint64_t blobId) {
                if (!impl.manifest) { return; }
                try { impl.manifest->blobDelete(blobId); }
                catch (...) {}
            };
            impl.blobManager = blob::BlobManager::create(impl.opts.paths.blobDir, impl.opts.blob);
            impl.blobManager->start();
        }

        impl.recoverTransactions();

        if (impl.opts.components.clusterEnabled) {
            cluster::ClusterConfig cfg = impl.opts.cluster.config.has_value()
                                             ? *impl.opts.cluster.config
                                             : cluster::ClusterConfig::load(impl.opts.paths.clusterConfigPath);
            impl.clusterConfig = cfg;
            impl.clusterConfiguredNodeCount = cfg.nodes().size();
            impl.clusterReplicationMode = cfg.mode();
            if (cfg.raidPreset() == cluster::RaidPreset::RAID1 && !impl.walWriter) {
                throw std::invalid_argument("AkkEngine: RAID.1 requires WAL durability");
            }
            if ((cfg.mode() == cluster::ReplicationMode::PARTITIONED || cfg.mode() == cluster::ReplicationMode::STRIPE) && !impl.walWriter) {
                throw std::invalid_argument("AkkEngine: PARTITIONED and STRIPE require WAL durability");
            }
            impl.primaryAckTimeoutAction = cfg.consistency().ackTimeoutAction;
            if (!cfg.usesDataConsensus() && impl.opts.components.blobEnabled) {
                throw std::invalid_argument("AkkEngine: non-Raft native cluster requires blob storage to be disabled");
            }
            if (cfg.usesDataConsensus()) {
                const auto raftBlobPolicy = impl.opts.cluster.runtime.raftBlobPolicy;
                if (raftBlobPolicy != cluster::RaftBlobPolicy::REJECT && raftBlobPolicy != cluster::RaftBlobPolicy::PRIMARY_SIDE_ONLY &&
                    raftBlobPolicy != cluster::RaftBlobPolicy::RAFT_LOG) {
                    throw std::invalid_argument("AkkEngine: invalid Raft Blob policy");
                }
                if (impl.opts.components.blobEnabled && raftBlobPolicy == cluster::RaftBlobPolicy::REJECT) {
                    throw std::invalid_argument("AkkEngine: RAFT_QUORUM does not support Blob payload replication");
                }
            }
            cluster::ClusterEngineCallbacks callbacks;
            impl.openPartitionEngines();
            if (impl.usesPartitionRaft()) {
                callbacks.partitionLeader = [&impl](std::span<const uint8_t> key) {
                    return impl.currentPartitionLeader(key);
                };
                callbacks.partitionCandidates = [&impl](std::span<const uint8_t> key) {
                    return cluster::detail::partitionTargets(*impl.livePlacement.load(), key);
                };
            }
            if (impl.usesPartitionRaft() || cfg.mode() == cluster::ReplicationMode::STRIPE) {
                callbacks.placementChanged = [&impl](const cluster::ClusterConfig& config, uint64_t generation) {
                    impl.acceptPlacement(config, generation);
                };
            }
            callbacks.forward = [&impl](const cluster::ForwardRequest& request) { return impl.receiveForward(request); };
            callbacks.getCurrentSeq = [&impl] { return impl.snapshotSeq(); };
            callbacks.getLastSeq = [&impl] { return impl.snapshotSeq(); };
            callbacks.getEntries = [&impl](
                uint64_t afterSeq,
                uint64_t throughSeq
            ) -> std::optional<std::vector<cluster::ClusterHistoryEntry>> {
                    if (!impl.walWriter || throughSeq < afterSeq) { return std::nullopt; }
                    std::vector<cluster::ClusterHistoryEntry> entries;
                    bool hasExternalBlob = false;
                    const auto recovery = wal::WalRecovery::recover(
                        wal::WalRecoveryOptions{.walDir = impl.opts.paths.walDir},
                        [&](const wal::WalRecoveredEntry& item) {
                            if ((item.flags & wal::WAL_FLAG_SNAPSHOT_MASK) != 0) { return; }
                            if (item.seq <= afterSeq || item.seq > throughSeq) { return; }
                            const uint8_t flags = static_cast<uint8_t>(item.flags);
                            if ((flags & MemHdr16::FLAG_BLOB) != 0) {
                                hasExternalBlob = true;
                                return;
                            }
                            entries.push_back(
                                cluster::ClusterHistoryEntry{
                                    .seq = item.seq,
                                    .sourceNodeId = impl.nodeId,
                                    .op = static_cast<uint8_t>((flags & MemHdr16::FLAG_TOMBSTONE) != 0
                                                                   ? cluster::ReplOpType::REMOVE
                                                                   : cluster::ReplOpType::PUT),
                                    .recordFlags = flags,
                                    .key = item.key,
                                    .value = item.value,
                                }
                            );
                        }
                    );
                    (void)recovery;
                    if (hasExternalBlob) { return std::nullopt; }
                    std::sort(entries.begin(), entries.end(), [](const auto& left, const auto& right) { return left.seq < right.seq; });
                    uint64_t expected = afterSeq;
                    for (const auto& entry : entries) {
                        if (expected == UINT64_MAX || entry.seq != expected + 1) { return std::nullopt; }
                        expected = entry.seq;
                    }
                    if (expected != throughSeq) { return std::nullopt; }
                    return entries;
                };
            callbacks.exportSnapshot = [&impl]() -> std::optional<cluster::ClusterSnapshot> {
                std::unique_lock writeLock{impl.writeMu};
                if (!impl.memtable) { return std::nullopt; }
                const uint64_t seq = impl.snapshotSeq();
                const auto makeSnapshot = [](const std::shared_ptr<ClusterSnapshotExportFile>& exported) {
                    cluster::ClusterSnapshot snapshot;
                    snapshot.seq = exported->seq();
                    snapshot.forEachEntry = [exported](const cluster::ClusterSnapshot::EntryVisitor& visitor) {
                        return exported->forEachEntry(visitor);
                    };
                    return snapshot;
                };
                if (auto cached = impl.clusterSnapshotExportCache.lock(); cached && cached->seq() == seq && cached->reusable()) {
                    return makeSnapshot(cached);
                }
                // Blob deletion is pinned from fixed-view capture through the
                // final Blob read, but does not participate in write admission.
                auto blobReadPin = impl.blobManager ? impl.blobManager->pinReads() : blob::BlobManager::ReadPin{};
                memtable::MemTable::KeyRange range;
                std::optional<memtable::MemTable::RangeIterator> mt;
                if (impl.sstManager) { mt.emplace(impl.memtable->sealAndPinIterator(range, seq)); }
                else {
                    const auto& policy = impl.opts.cluster.runtime.memoryOnlySnapshot;
                    mt = impl.memtable->sealAndPinMemoryIterator(
                        range,
                        seq,
                        policy.mode == cluster::MemoryOnlySnapshotMode::COMPLETION_FIRST,
                        policy.maxPinnedBytes,
                        policy.maxPinnedGenerations
                    );
                    if (!mt) { return std::nullopt; }
                }
                sst::SSTManager::Iterator sst;
                if (impl.sstManager) { sst = impl.sstManager->scanIter({}, {}, seq); }

                // Both paths own immutable sources. Memory-only snapshots retain
                // in-memory generations and compact them after the final pin.
                ensureDir(impl.clusterSnapshotExportDirectory());

                struct SnapshotSource {
                    memtable::MemTable::RangeIterator memtable;
                    sst::SSTManager::Iterator sst;
                    blob::BlobManager::ReadPin blobReadPin;
                    blob::BlobManager* blobManager = nullptr;
                    vlog::VersionLog::RecordSnapshot history;
                    bool typed = false;
                };
                auto history = impl.clusterConfig.usesDataConsensus() && impl.versionLog
                    ? impl.versionLog->captureRecords() : vlog::VersionLog::RecordSnapshot{};
                auto source = std::make_shared<SnapshotSource>(SnapshotSource{
                    .memtable = std::move(*mt), .sst = std::move(sst),
                    .blobReadPin = std::move(blobReadPin), .blobManager = impl.blobManager.get(),
                    .history = std::move(history), .typed = impl.clusterConfig.usesDataConsensus(),
                });
                writeLock.unlock();
                auto producer = [source](const cluster::ClusterSnapshot::EntryVisitor& visitor) mutable {
                    if (source->history && !source->history([&](auto key, const vlog::VersionEntry& entry) {
                        const auto encoded = cluster::detail::encodeSnapshotKey({
                            cluster::detail::SnapshotRecordKind::HISTORY, entry.flags, entry.seq, entry.sourceNodeId, entry.timestampNs, key,
                            Impl::snapshotBlobIdentity(entry.flags, entry.value)});
                        return Impl::streamSnapshotValue(visitor, encoded, entry.value, entry.flags, source->blobManager);
                    })) { return false; }
                    source->history = {};
                    for (const auto& record : storedScanGenerator(std::move(source->memtable), std::move(source->sst))) {
                        const auto encoded = source->typed ? cluster::detail::encodeSnapshotKey({
                            cluster::detail::SnapshotRecordKind::HEAD, record.flags, 0, 0, 0, record.key,
                            Impl::snapshotBlobIdentity(record.flags, record.value)}) : std::vector<uint8_t>{};
                        if (!Impl::streamSnapshotValue(visitor, source->typed ? std::span<const uint8_t>{encoded} : record.key,
                            record.value, record.flags, source->blobManager)) { return false; }
                    }
                    return true;
                };
                auto exported = std::make_shared<ClusterSnapshotExportFile>(
                    impl.newClusterSnapshotExportPath(), seq, std::move(producer)
                );
                impl.clusterSnapshotExportCache = exported;
                return makeSnapshot(exported);
            };
            callbacks.installPartitionSnapshot = [&impl](uint64_t owner, const cluster::ClusterSnapshot& snapshot) {
                impl.installPartitionSnapshot(owner, snapshot);
            };
            callbacks.beginSnapshot = [&impl](uint64_t seq, uint64_t entryCount) { impl.beginReplicaSnapshot(seq, entryCount); };
            callbacks.beginSnapshotEntry = [&impl](std::span<const uint8_t> key, uint64_t valueSize, uint32_t valueCrc32c) {
                impl.beginReplicaSnapshotEntry(key, valueSize, valueCrc32c);
            };
            callbacks.appendSnapshotEntryChunk = [&impl](uint64_t offset, std::span<const uint8_t> chunk) {
                impl.appendReplicaSnapshotEntryChunk(offset, chunk);
            };
            callbacks.finishSnapshotEntry = [&impl] { impl.finishReplicaSnapshotEntry(); };
            callbacks.prepareSnapshot = [&impl](uint64_t seq, uint64_t count) {
                std::lock_guard lock{impl.writeMu}; impl.prepareReplicaSnapshotLocked(seq, count);
            };
            callbacks.finishSnapshot = [&impl](uint64_t seq, uint64_t entryCount) { impl.finishReplicaSnapshot(seq, entryCount); };
            callbacks.recoverSnapshot = [&impl](uint64_t seq) { impl.recoverReplicaSnapshot(seq); };
            callbacks.isSnapshotDurable = [&impl](uint64_t seq) { return impl.isReplicaSnapshotDurable(seq); };
            callbacks.forceDurable = [&impl] {
                Impl::OperationGuard operation{impl, false};
                if (!operation) {
                    if (impl.opts.cluster.runtime.mirrorFencing.mode != cluster::MirrorFencingMode::STATIC) {
                        throw std::runtime_error("AkkEngine: closing before fenced write/snapshot durability was confirmed");
                    }
                    return;
                }
                if (impl.walWriter) { impl.walWriter->forceSync(); }
                if (impl.versionLog) { impl.versionLog->forceSync(); }
            };
            callbacks.apply = [&impl](
                uint64_t seq,
                cluster::ReplOpType op,
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint8_t recordFlags,
                uint64_t sourceNodeId,
                uint64_t timestampNs
            ) {
                    Impl::OperationGuard operation{impl, false};
                    if (!operation) { return; }
                    impl.applyReplicaRecord(seq, op, key, value, recordFlags, sourceNodeId, timestampNs);
                };
            callbacks.read = [&impl](std::span<const uint8_t> key, uint64_t snapshotSeq) {
                return impl.readLocalForCluster(key, snapshotSeq);
            };
            callbacks.commitStripeMetadata = [&impl](
                uint64_t logicalSeq,
                std::span<const uint8_t> publicKey,
                std::span<const uint8_t> metadataBytes
            ) {
                if (logicalSeq == UINT64_MAX - 1) {
                    if (metadataBytes.size() < 8) { throw std::runtime_error("AkkEngine: truncated stripe placement alias"); }
                    auto alias = Impl::decodeStripeMetadata(metadataBytes.subspan(8));
                    if (!alias) { throw std::runtime_error("AkkEngine: invalid stripe placement alias"); }
                    size_t cursor = 0; uint64_t placementGeneration = 0;
                    if (!Impl::pullU64(metadataBytes, cursor, placementGeneration)) { throw std::runtime_error("AkkEngine: malformed placement generation"); }
                    impl.writeStripeLocal(Impl::stripeHistoryPlacementKey(publicKey, alias->logicalSeq, placementGeneration), metadataBytes);
                    return;
                }
                auto metadata = Impl::decodeStripeMetadata(metadataBytes);
                if (!metadata) { throw std::runtime_error("AkkEngine: invalid committed STRIPE metadata"); }
                const bool snapshotBase = logicalSeq == UINT64_MAX;
                if (logicalSeq != 0 && !snapshotBase) { metadata->logicalSeq = logicalSeq; }
                const auto encoded = Impl::encodeStripeMetadata(*metadata);
                uint8_t vlogFlags = 0xFF;
                if (snapshotBase) {
                    vlogFlags = static_cast<uint8_t>(vlog::VLOG_FLAG_RETENTION_BASE |
                        (metadata->rollback ? vlog::VLOG_FLAG_ROLLBACK : 0));
                }
                impl.writeStripeLocal(
                    Impl::stripeMetaKey(publicKey), encoded, cluster::ReplOpType::PUT, 0,
                    metadata->rollback ? vlog::ROLLBACK_NODE : impl.nodeId, vlogFlags
                );
            };
            callbacks.readStripeMetadata = [&impl](std::span<const uint8_t> publicKey) {
                const auto value = impl.getValueInternal(Impl::stripeMetaKey(publicKey), false);
                if (!value) { return value; }
                const auto metadata = Impl::decodeStripeMetadata(*value);
                if (!metadata) { throw std::runtime_error("AkkEngine: invalid local stripe metadata"); }
                return std::optional<std::vector<uint8_t>>{Impl::encodeStripeMetadata(impl.effectiveStripeMetadata(publicKey, *metadata))};
            };
            callbacks.rollbackControl = [&impl](
                uint64_t,
                const cluster::StripeControlRequest& request
            ) {
                cluster::StripeControlResponse response;
                response.requestId = request.requestId;
                try {
                    if (request.ownerNodeId != impl.nodeId) {
                        response.status = cluster::StripeControlStatus::REJECTED;
                        return response;
                    }
                    if (!impl.versionLog) {
                        response.status = cluster::StripeControlStatus::ERROR_STATUS;
                        return response;
                    }
                    if (request.action == cluster::StripeControlAction::ROLLBACK_KEY &&
                        impl.clusterReplicationMode == cluster::ReplicationMode::PARTITIONED &&
                        impl.clusterRuntime->ownerNodeId(request.key) != impl.nodeId) {
                        response.status = cluster::StripeControlStatus::REJECTED;
                        return response;
                    }
                    Impl::OperationGuard operation{impl};
                    impl.throwIfBackgroundFailed();
                    impl.waitForVersionLogRecovery();
                    std::unique_lock epochLock{impl.mutationEpochMu};
                    impl.versionLog->forceSync();
                    if (request.action == cluster::StripeControlAction::ROLLBACK_WATERMARK) {
                        response.status = cluster::StripeControlStatus::COMMITTED;
                        response.authorityNodeId = impl.nodeId;
                        response.fenceToken = impl.localRollbackWatermark();
                        return response;
                    }
                    if (request.action == cluster::StripeControlAction::ROLLBACK_APPLY) {
                        if (request.ownerNodeId != impl.nodeId || request.metadata.size() < 10) {
                            response.status = cluster::StripeControlStatus::REJECTED;
                            return response;
                        }
                        size_t cursor = 0;
                        const auto conflict = static_cast<RollbackConflictPolicy>(request.metadata[cursor++]);
                        uint64_t expectedHead = 0;
                        if (conflict > RollbackConflictPolicy::OVERWRITE_LATEST ||
                            !Impl::pullU64(request.metadata, cursor, expectedHead)) {
                            response.status = cluster::StripeControlStatus::REJECTED;
                            return response;
                        }
                        const bool tombstone = request.metadata[cursor++] != 0;
                        const auto value = std::span<const uint8_t>{request.metadata}.subspan(cursor);
                        if (tombstone && !value.empty()) {
                            response.status = cluster::StripeControlStatus::REJECTED;
                            return response;
                        }
                        const auto result = impl.applyMaterializedRollback(
                            request.key, value, tombstone, expectedHead, conflict
                        );
                        const std::array items{result};
                        response.metadata = Impl::encodeRollbackItems(items);
                        response.status = cluster::StripeControlStatus::COMMITTED;
                        return response;
                    }
                    if (request.metadata.size() != 2 && request.metadata.size() != 10) {
                        response.status = cluster::StripeControlStatus::REJECTED;
                        return response;
                    }
                    const auto execution = static_cast<RollbackExecutionMode>(request.metadata[0]);
                    const auto conflict = static_cast<RollbackConflictPolicy>(request.metadata[1]);
                    if (execution != RollbackExecutionMode::IMMEDIATE || conflict > RollbackConflictPolicy::OVERWRITE_LATEST) {
                        response.status = cluster::StripeControlStatus::REJECTED;
                        return response;
                    }
                    uint64_t maximumAllowedHead = UINT64_MAX;
                    if (request.metadata.size() == 10) {
                        size_t cursor = 2;
                        if (!Impl::pullU64(request.metadata, cursor, maximumAllowedHead)) {
                            response.status = cluster::StripeControlStatus::REJECTED;
                            return response;
                        }
                    }
                    std::vector<RollbackItemResult> results;
                    if (request.action == cluster::StripeControlAction::ROLLBACK_KEY) {
                        results.push_back(
                            impl.clusterReplicationMode == cluster::ReplicationMode::STRIPE
                                ? impl.rollbackStripeCoordinated(request.key, request.fenceToken, conflict, maximumAllowedHead)
                                : impl.rollbackKeyLocal(request.key, request.fenceToken, conflict, 0, maximumAllowedHead)
                        );
                    }
                    else if (request.action == cluster::StripeControlAction::ROLLBACK_STREAM) {
                        static constexpr size_t ROLLBACK_STREAM_BATCH_KEYS = 256;
                        const auto keys = impl.rollbackKeysChangedAfter(request.fenceToken);
                        const auto begin = request.key.empty()
                                               ? keys.begin()
                                               : std::ranges::upper_bound(keys, request.key);
                        const auto remaining = static_cast<size_t>(std::distance(begin, keys.end()));
                        const auto batchSize = std::min(remaining, ROLLBACK_STREAM_BATCH_KEYS);
                        const auto end = std::next(begin, static_cast<std::ptrdiff_t>(batchSize));
                        for (auto it = begin; it != end; ++it) {
                            const auto& key = *it;
                            results.push_back(
                                impl.clusterReplicationMode == cluster::ReplicationMode::STRIPE
                                    ? impl.rollbackStripeCoordinated(key, request.fenceToken, conflict, maximumAllowedHead)
                                    : impl.rollbackKeyLocal(key, request.fenceToken, conflict, 0, maximumAllowedHead)
                            );
                        }
                        const bool done = end == keys.end();
                        const std::span<const uint8_t> nextCursor = done || begin == end
                                                                          ? std::span<const uint8_t>{}
                                                                          : std::span<const uint8_t>{*std::prev(end)};
                        response.metadata = Impl::encodeRollbackStreamBatch(results, nextCursor, done);
                        response.status = cluster::StripeControlStatus::COMMITTED;
                        return response;
                    }
                    else {
                        response.status = cluster::StripeControlStatus::REJECTED;
                        return response;
                    }
                    response.metadata = Impl::encodeRollbackItems(results);
                    response.status = cluster::StripeControlStatus::COMMITTED;
                }
                catch (...) { response.status = cluster::StripeControlStatus::ERROR_STATUS; }
                return response;
            };
            callbacks.exportStripeMetadataSnapshot = [&impl](uint64_t metadataSequence)
                -> std::optional<cluster::ClusterSnapshot> {
                std::unique_lock lock{impl.writeMu};
                if (metadataSequence == 0 || !impl.memtable) { return std::nullopt; }
                const auto visible = impl.snapshotSeq();
                memtable::MemTable::KeyRange range;
                std::optional<memtable::MemTable::RangeIterator> mt;
                if (impl.sstManager) { mt.emplace(impl.memtable->sealAndPinIterator(range, visible)); }
                else {
                    const auto& policy = impl.opts.cluster.runtime.memoryOnlySnapshot;
                    mt = impl.memtable->sealAndPinMemoryIterator(range, visible,
                        policy.mode == cluster::MemoryOnlySnapshotMode::COMPLETION_FIRST, policy.maxPinnedBytes, policy.maxPinnedGenerations);
                    if (!mt) { return std::nullopt; }
                }
                sst::SSTManager::Iterator sst;
                if (impl.sstManager) { sst = impl.sstManager->scanIter({}, {}, visible); }
                struct MetadataSource {
                    memtable::MemTable::RangeIterator memtable;
                    sst::SSTManager::Iterator sst;
                    vlog::VersionLog::RecordSnapshot history;
                    blob::BlobManager::ReadPin blobPin;
                    std::shared_ptr<Impl::StorageState> storage;
                };
                auto source = std::make_shared<MetadataSource>(MetadataSource{
                    .memtable = std::move(*mt), .sst = std::move(sst),
                    .history = impl.versionLog ? impl.versionLog->captureRecords() : vlog::VersionLog::RecordSnapshot{},
                    .blobPin = impl.blobManager ? impl.blobManager->pinReads() : blob::BlobManager::ReadPin{}, .storage = impl.storage,
                });
                ensureDir(impl.clusterSnapshotExportDirectory());
                auto exported = std::make_shared<ClusterSnapshotExportFile>(impl.newClusterSnapshotExportPath(), metadataSequence,
                    [source](const cluster::SnapshotEntryVisitor& visitor) mutable {
                        auto* blobs = source->storage->blobManager.get();
                        if (source->history && !source->history([&](auto key, const vlog::VersionEntry& entry) {
                            if (!Impl::publicKeyFromStripeMetaKey(key)) { return true; }
                            const auto encoded = cluster::detail::encodeSnapshotKey({cluster::detail::SnapshotRecordKind::HISTORY,
                                entry.flags, entry.seq, entry.sourceNodeId, entry.timestampNs, key,
                                Impl::snapshotBlobIdentity(entry.flags, entry.value)});
                            return Impl::streamSnapshotValue(visitor, encoded, entry.value, entry.flags, blobs);
                        })) { return false; }
                        source->history = {};
                        for (const auto& record : storedScanGenerator(std::move(source->memtable), std::move(source->sst))) {
                            const bool alias = record.key.size() >= 22 && Impl::isStripeInternalKey(record.key) && record.key[4] == 'H';
                            if (!alias && !Impl::publicKeyFromStripeMetaKey(record.key)) { continue; }
                            const auto encoded = cluster::detail::encodeSnapshotKey({cluster::detail::SnapshotRecordKind::HEAD,
                                record.flags, 0, 0, 0, record.key,
                                Impl::snapshotBlobIdentity(record.flags, record.value)});
                            if (!Impl::streamSnapshotValue(visitor, encoded, record.value, record.flags, blobs)) { return false; }
                        }
                        return true;
                    });
                cluster::ClusterSnapshot snapshot;
                snapshot.seq = metadataSequence;
                snapshot.forEachEntry = [exported](const auto& visitor) { return exported->forEachEntry(visitor); };
                return snapshot;
            };
            callbacks.installStripeMetadataSnapshot = [&impl](const cluster::ClusterSnapshot& snapshot, uint64_t after) {
                std::vector<uint8_t> key, value;
                cluster::detail::SnapshotRecordKind kind{};
                uint8_t flags = 0;
                cluster::SnapshotEntryVisitor visitor;
                visitor.beginEntry = [&](auto encoded, uint64_t size, uint32_t) {
                    if (size > 4ull * 1024 * 1024) { throw std::runtime_error("AkkEngine: oversized metadata snapshot record"); }
                    const auto record = cluster::detail::decodeSnapshotKey(encoded);
                    kind = record.kind; flags = record.flags & static_cast<uint8_t>(~MemHdr16::FLAG_BLOB);
                    key.assign(record.key.begin(), record.key.end()); value.clear(); return true;
                };
                visitor.appendValueChunk = [&](uint64_t offset, auto bytes) {
                    if (offset != value.size()) { throw std::runtime_error("AkkEngine: invalid metadata snapshot offset"); }
                    value.insert(value.end(), bytes.begin(), bytes.end()); return true;
                };
                visitor.finishEntry = [&] {
                    const bool alias = key.size() >= 22 && Impl::isStripeInternalKey(key) && key[4] == 'H';
                    const bool metadataKey = Impl::publicKeyFromStripeMetaKey(key).has_value();
                    if ((!alias && !metadataKey) || kind == cluster::detail::SnapshotRecordKind::STATE ||
                        (alias && kind != cluster::detail::SnapshotRecordKind::HEAD)) {
                        throw std::runtime_error("AkkEngine: invalid metadata snapshot key");
                    }
                    if (alias && value.size() < 8) { throw std::runtime_error("AkkEngine: truncated metadata placement alias"); }
                    auto metadata = Impl::decodeStripeMetadata(alias ? std::span<const uint8_t>{value}.subspan(8) : std::span<const uint8_t>{value});
                    if (!metadata || metadata->logicalSeq == 0 || metadata->logicalSeq > snapshot.seq) {
                        throw std::runtime_error("AkkEngine: invalid metadata snapshot revision");
                    }
                    if (kind == cluster::detail::SnapshotRecordKind::HISTORY && metadata->logicalSeq <= after) { return true; }
                    if (kind == cluster::detail::SnapshotRecordKind::HEAD) {
                        const auto current = impl.getValueInternal(key, false);
                        if (current && *current == value) { return true; }
                        flags = alias ? 0 : metadata->rollback ? vlog::VLOG_FLAG_ROLLBACK : 0;
                    }
                    impl.writeStripeLocal(key, value, cluster::ReplOpType::PUT, 0,
                        metadata->rollback ? vlog::ROLLBACK_NODE : metadata->originNodeId, flags);
                    return true;
                };
                if (!snapshot.forEachEntry(visitor)) { throw std::runtime_error("AkkEngine: incomplete metadata snapshot install"); }
            };
            callbacks.beginBlob = [&impl](uint64_t /*seq*/, uint64_t blobId, uint64_t totalSize, uint32_t contentCrc32c) {
                if (impl.blobManager) {
                    impl.blobManager->abortWrite(blobId);
                    impl.blobManager->beginWrite(blobId, totalSize, contentCrc32c);
                }
            };
            callbacks.appendBlobChunk = [&impl](uint64_t /*seq*/, uint64_t blobId, uint64_t offset, std::span<const uint8_t> chunk) {
                if (impl.blobManager) { impl.blobManager->appendWriteChunk(blobId, offset, chunk); }
            };
            callbacks.finishBlob = [&impl](uint64_t /*seq*/, uint64_t blobId) {
                if (impl.blobManager) { impl.blobManager->finishWrite(blobId); }
            };
            callbacks.abortBlob = [&impl](uint64_t /*seq*/, uint64_t blobId) {
                if (impl.blobManager) { impl.blobManager->abortWrite(blobId); }
            };

            if (!cluster::clusterRuntimeFactoryAvailable() && !cluster::loadClusterRuntimeBackend(impl.opts.cluster.runtimeBackendPath)) {
                const auto detail = cluster::lastClusterRuntimeBackendLoadError();
                throw std::runtime_error(
                    detail.empty()
                        ? "AkkEngine: cluster component is enabled, but the cluster runtime backend library is not available"
                        : "AkkEngine: cluster component is enabled, but the cluster runtime backend library is not available: " + detail
                );
            }
            impl.clusterRuntime = cluster::createClusterRuntime(
                impl.opts.paths.dataDir,
                std::move(cfg),
                impl.nodeId,
                std::move(callbacks),
                impl.opts.cluster.runtime
            );
            impl.writeCoordinator = impl.createWriteCoordinator();
            if (impl.clusterRuntime->mirrorFencingMode() != impl.opts.cluster.runtime.mirrorFencing.mode) {
                throw std::runtime_error("AkkEngine: cluster backend does not implement the requested MIRROR fencing policy");
            }
            if (impl.clusterRuntime->mirrorRecoveryMode() != impl.opts.cluster.runtime.mirrorRecovery.mode) {
                throw std::runtime_error("AkkEngine: cluster backend does not implement the requested MIRROR recovery policy");
            }
            if (impl.clusterReplicationMode == cluster::ReplicationMode::STRIPE) {
                impl.stripeExecutor = std::make_unique<cluster::detail::BoundedExecutor>(8, 64);
            }
            impl.clusterRuntime->start();
            impl.startStripeGarbageCollector();
            impl.startQueryReaper();
        }

        if (!impl.writeCoordinator) { impl.writeCoordinator = impl.createWriteCoordinator(); }
        impl.forwardReady.store(true, std::memory_order_release);
        impl.startPlacementRecovery();
        impl.startRollbackRecovery();

        if (impl.opts.components.apiEnabled) {
            if (!server::akkApiServerFactoryAvailable() && !server::loadAkkApiServerBackend(impl.opts.api.serverBackendPath)) {
                const auto detail = server::lastAkkApiServerBackendLoadError();
                throw std::runtime_error(
                    detail.empty()
                        ? "AkkEngine: API server component is enabled, but the API server backend library is not available"
                        : "AkkEngine: API server component is enabled, but the API server backend library is not available: " + detail
                );
            }
            impl.apiServer = server::createAkkApiServer(*engine, impl.opts.api);
            impl.apiServer->start();
        }

        return engine;
    }

    cluster::ClusterRequestId AkkEngine::newRequestId(uint64_t retentionMs) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        const auto& options = impl_->opts.cluster.runtime.requests;
        if (!impl_->clusterRuntime || !options.enabled) { throw std::logic_error("AkkEngine: retry-safe requests are disabled"); }
        if (retentionMs == 0) { retentionMs = options.maxRetentionMs; }
        if (retentionMs > options.maxRetentionMs) { throw std::invalid_argument("AkkEngine: request retention exceeds configured maximum"); }
        cluster::ClusterRequestId id;
        crypto::secureRandom(id.nonce);
        id.expiresAtUnixMs = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count()) + retentionMs;
        return id;
    }

    cluster::ClusterRequestResult AkkEngine::putWithRequest(const cluster::ClusterRequestId& id,
       std::span<const uint8_t> key, std::span<const uint8_t> value) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        impl_->throwIfBackgroundFailed();
        impl_->waitForVersionLogRecovery();
        if (auto forwarded = impl_->tryForwardPoint(cluster::ForwardOperation::PUT_REQUEST, key, value, id)) { return forwarded->requestResult; }
        std::shared_lock epochLock{impl_->mutationEpochMu};
        return impl_->writeCoordinator->writeWithRequest(id, cluster::ReplOpType::PUT, key, value);
    }

    cluster::ClusterRequestResult AkkEngine::removeWithRequest(const cluster::ClusterRequestId& id, std::span<const uint8_t> key) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        impl_->throwIfBackgroundFailed();
        impl_->waitForVersionLogRecovery();
        if (auto forwarded = impl_->tryForwardPoint(cluster::ForwardOperation::REMOVE_REQUEST, key, {}, id)) { return forwarded->requestResult; }
        std::shared_lock epochLock{impl_->mutationEpochMu};
        return impl_->writeCoordinator->writeWithRequest(id, cluster::ReplOpType::REMOVE, key, {});
    }

    cluster::ClusterRequestResult AkkEngine::queryRequest(const cluster::ClusterRequestId& id) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        if (!impl_->clusterRuntime) { throw std::logic_error("AkkEngine: retry-safe requests require Raft"); }
        if (impl_->usesPartitionRaft()) { throw std::invalid_argument("AkkEngine: PARTITIONED queryRequest requires the original key"); }
        cluster::ForwardRequest request;
        request.operation = cluster::ForwardOperation::QUERY_REQUEST; request.deduplicationId = id;
        request.entries.emplace_back();
        if (auto forwarded = impl_->tryForward(std::move(request))) { return forwarded->requestResult; }
        return impl_->clusterRuntime->queryRequest(id);
    }

    cluster::ClusterRequestResult AkkEngine::queryRequest(const cluster::ClusterRequestId& id, std::span<const uint8_t> key) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        if (!impl_->clusterRuntime) { throw std::logic_error("AkkEngine: retry-safe requests require Raft"); }
        if (auto forwarded = impl_->tryForwardPoint(cluster::ForwardOperation::QUERY_REQUEST, key, {}, id)) { return forwarded->requestResult; }
        return impl_->usesPartitionRaft() ? impl_->requireLocalPartition(key).impl_->clusterRuntime->queryRequest(id) : impl_->clusterRuntime->queryRequest(id);
    }

    void AkkEngine::put(std::span<const uint8_t> key, std::span<const uint8_t> value) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        if (impl_->clusterRuntime) { impl_->throwIfBackgroundFailed(); }
        impl_->waitForVersionLogRecovery();
        if (impl_->tryForwardPoint(cluster::ForwardOperation::PUT, key, value)) { return; }
        std::shared_lock epochLock{impl_->mutationEpochMu};
        impl_->writeCoordinator->put(key, value);
    }

    void AkkEngine::putHinted(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t fp64, uint64_t miniKey) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        if (impl_->clusterRuntime) { impl_->throwIfBackgroundFailed(); }
        impl_->waitForVersionLogRecovery();
        if (impl_->tryForwardPoint(cluster::ForwardOperation::PUT, key, value)) { return; }
        std::shared_lock epochLock{impl_->mutationEpochMu};
        impl_->writeCoordinator->putHinted(key, value, fp64, miniKey);
    }

    void detail::ProtocolBulkWriter::put(AkkEngine& engine, std::span<const BulkPutEntry> entries) { engine.applyProtocolBulk(entries); }

    void AkkEngine::applyProtocolBulk(std::span<const detail::BulkPutEntry> entries) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        if (entries.empty()) {
            impl_->throwIfBackgroundFailed();
            return;
        }
        if (impl_->clusterRuntime) { impl_->throwIfBackgroundFailed(); }
        impl_->waitForVersionLogRecovery();
        if (impl_->clusterRuntime && std::ranges::any_of(entries, [&](const detail::BulkPutEntry& entry) {
                return !impl_->clusterRuntime->ownsWriteKey(entry.key);
            })) {
            if (const auto target = impl_->resolveForwardEntries(entries, true)) {
                size_t payloadSize = cluster::FORWARD_REQUEST_BASE_SIZE;
                if (entries.size() > 65536) { throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::PAYLOAD_TOO_LARGE, *target); }
                for (const auto& entry : entries) {
                    if (entry.key.size() > cluster::MAX_FORWARD_PAYLOAD || entry.value.size() > cluster::MAX_FORWARD_PAYLOAD) {
                        throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::PAYLOAD_TOO_LARGE, *target);
                    }
                    payloadSize += 8 + entry.key.size() + entry.value.size();
                    if (payloadSize > cluster::MAX_FORWARD_PAYLOAD) { throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::PAYLOAD_TOO_LARGE, *target); }
                }
                cluster::ForwardRequest request;
                request.operation = cluster::ForwardOperation::PUT_BATCH;
                request.entries.reserve(entries.size());
                for (const auto& entry : entries) { request.entries.push_back({{entry.key.begin(), entry.key.end()}, {entry.value.begin(), entry.value.end()}}); }
                if (impl_->tryForward(std::move(request))) { return; }
            }
        }
        std::shared_lock epochLock{impl_->mutationEpochMu};
        impl_->writeCoordinator->putBatch(entries);
    }

    void AkkEngine::remove(std::span<const uint8_t> key) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        if (impl_->clusterRuntime) { impl_->throwIfBackgroundFailed(); }
        impl_->waitForVersionLogRecovery();
        if (impl_->tryForwardPoint(cluster::ForwardOperation::REMOVE, key)) { return; }
        std::shared_lock epochLock{impl_->mutationEpochMu};
        impl_->writeCoordinator->remove(key);
    }

    void AkkEngine::removeHinted(std::span<const uint8_t> key, uint64_t fp64, uint64_t miniKey) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        if (impl_->clusterRuntime) { impl_->throwIfBackgroundFailed(); }
        impl_->waitForVersionLogRecovery();
        if (impl_->tryForwardPoint(cluster::ForwardOperation::REMOVE, key)) { return; }
        std::shared_lock epochLock{impl_->mutationEpochMu};
        impl_->writeCoordinator->removeHinted(key, fp64, miniKey);
    }

    std::optional<std::vector<uint8_t>> AkkEngine::get(std::span<const uint8_t> key) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        impl_->throwIfBackgroundFailed();
        impl_->waitForVersionLogRecovery();
        std::shared_lock transactionRead{impl_->transactionVisibilityMu};
        impl_->throwIfBackgroundFailed();
        return impl_->getValueClusterAware(key);
    }

    std::vector<AkkEngine::BatchGetResult> AkkEngine::getBatch(std::span<const std::span<const uint8_t>> keys) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        impl_->throwIfBackgroundFailed();
        impl_->waitForVersionLogRecovery();
        std::shared_lock transactionRead{impl_->transactionVisibilityMu};
        impl_->throwIfBackgroundFailed();
        if (!impl_->clusterReadMustRoute()) { impl_->waitForKeySequenceReadVisibility(); }
        if (impl_->usesPartitionRaft() && !impl_->clusterReadMustRoute()) {
            std::vector<BatchGetResult> results(keys.size());
            std::map<uint16_t, std::vector<size_t>> grouped;
            for (size_t i = 0; i < keys.size(); ++i) { grouped[impl_->partitionIndex(keys[i])].push_back(i); }
            for (const auto& [partition, indexes] : grouped) {
                const auto children = impl_->partitionSnapshot();
                if (!children[partition]) { continue; }
                std::vector<std::span<const uint8_t>> batch;
                for (const auto index : indexes) { batch.push_back(keys[index]); }
                auto values = children[partition]->getBatch(batch);
                for (size_t i = 0; i < indexes.size(); ++i) { results[indexes[i]] = std::move(values[i]); }
            }
            return results;
        }
        const uint64_t snapshot = impl_->snapshotSeq();

        std::vector<BatchGetResult> out;
        out.reserve(keys.size());

        for (const auto& key : keys) {
            BatchGetResult result;
            result.found = impl_->clusterReadMustRoute()
                               ? impl_->getIntoClusterAware(key, result.value)
                               : impl_->getIntoInternal(key, result.value, true, snapshot);
            out.push_back(std::move(result));
        }

        return out;
    }

    bool AkkEngine::exists(std::span<const uint8_t> key) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        impl_->throwIfBackgroundFailed();
        impl_->waitForVersionLogRecovery();
        std::shared_lock transactionRead{impl_->transactionVisibilityMu};
        impl_->throwIfBackgroundFailed();
        if (impl_->clusterReadMustRoute()) {
            impl_->existsTotal.fetch_add(1, std::memory_order_relaxed);
            return impl_->getValueClusterAware(key, false).has_value();
        }
        impl_->waitForKeySequenceReadVisibility();
        impl_->existsTotal.fetch_add(1, std::memory_order_relaxed);
        if (impl_->usesPartitionRaft()) {
            auto* child = impl_->localPartition(key);
            return child && child->exists(key);
        }
        const uint64_t seq = impl_->snapshotSeq();
        if (const auto mt = impl_->memtable->contains(key, seq); mt.has_value()) { return *mt; }
        if (impl_->sstManager) { if (const auto sst = impl_->sstManager->contains(key, seq); sst.has_value()) { return *sst; } }
        return false;
    }

    bool AkkEngine::getInto(std::span<const uint8_t> key, std::vector<uint8_t>& out) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        impl_->throwIfBackgroundFailed();
        impl_->waitForVersionLogRecovery();
        std::shared_lock transactionRead{impl_->transactionVisibilityMu};
        impl_->throwIfBackgroundFailed();
        return impl_->getIntoClusterAware(key, out);
    }

    bool AkkEngine::getIntoArena(std::span<const uint8_t> key, core::BufferArena& arena, std::span<const uint8_t>& out) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        impl_->throwIfBackgroundFailed();
        impl_->waitForVersionLogRecovery();
        std::shared_lock transactionRead{impl_->transactionVisibilityMu};
        impl_->throwIfBackgroundFailed();
        if (impl_->clusterReadMustRoute()) {
            auto value = impl_->getValueClusterAware(key);
            if (!value) {
                out = {};
                return false;
            }
            out = copySpanToArena(*value, arena);
            return true;
        }
        return impl_->getIntoArenaInternal(key, arena, out, true);
    }

    size_t AkkEngine::count(std::span<const uint8_t> startKey, std::span<const uint8_t> endKey) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        impl_->throwIfBackgroundFailed();
        impl_->waitForVersionLogRecovery();
        if (impl_->clusterRuntime) {
            size_t total = 0;
            for (const auto& target : impl_->rangeQueryPlan(startKey)) {
                const auto response = impl_->sendReadQuery(target.node, impl_->openQueryRequest(Impl::QueryAction::COUNT, startKey, endKey, 0, target.partitions));
                size_t cursor = 0; uint64_t count = 0;
                if (!Impl::pullU64(response.value, cursor, count) || cursor != response.value.size() || count > SIZE_MAX - total) {
                    throw std::overflow_error("AkkEngine: invalid or overflowing cluster count");
                }
                total += static_cast<size_t>(count);
            }
            return total;
        }
        std::shared_lock transactionRead{impl_->transactionVisibilityMu};
        impl_->throwIfBackgroundFailed();
        impl_->waitForKeySequenceReadVisibility();

        memtable::MemTable::KeyRange range;
        range.start.assign(startKey.begin(), startKey.end());
        range.end.assign(endKey.begin(), endKey.end());
        uint64_t seq = 0;
        auto mt = [&]() {
            if (impl_->opts.runtime.scanConsistency == AkkEngineOptions::ScanConsistencyMode::PINNED_SNAPSHOT) {
                auto pinned = impl_->memtable->pinnedIterator(range, [this] { return impl_->snapshotSeq(); });
                seq = pinned.snapshotSeq();
                return pinned;
            }
            seq = impl_->snapshotSeq();
            return impl_->memtable->iterator(range, seq);
        }();
        sst::SSTManager::Iterator sstIt;
        if (impl_->sstManager) { sstIt = impl_->sstManager->scanIter(startKey, endKey, seq); }

        auto mtCur = mt.hasNext() ? mt.next() : std::optional<core::RecordView>{};
        auto sstCur = sstIt.hasNext() ? sstIt.next() : std::optional<sst::SSTRecord>{};
        size_t n = 0;

        while (mtCur || sstCur) {
            const bool hasMt = mtCur.has_value();
            const bool hasSst = sstCur.has_value();
            int cmp = 0;
            if (hasMt && hasSst) { cmp = compareKey(mtCur->key(), sstCur->key); }
            else { cmp = hasMt ? -1 : 1; }

            bool tombstone = false;
            if (cmp <= 0) {
                tombstone = mtCur->isTombstone();
                mtCur = mt.hasNext() ? mt.next() : std::optional<core::RecordView>{};
                if (cmp == 0) { sstCur = sstIt.hasNext() ? sstIt.next() : std::optional<sst::SSTRecord>{}; }
            }
            else {
                tombstone = sstCur->isTombstone();
                sstCur = sstIt.hasNext() ? sstIt.next() : std::optional<sst::SSTRecord>{};
            }

            if (!tombstone) { ++n; }
        }

        return n;
    }

    core::ArenaGenerator<AkkEngine::ScanRecordView> AkkEngine::scan(
        core::BufferArena& arena,
        std::span<const uint8_t> startKey,
        std::span<const uint8_t> endKey
    ) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        auto operation = std::make_shared<Impl::OperationGuard>(*impl_);
        impl_->throwIfBackgroundFailed();
        if (impl_->clusterRuntime) {
            impl_->waitForVersionLogRecovery();
            std::vector<std::unique_ptr<Impl::QueryReader>> readers;
            for (const auto& target : impl_->rangeQueryPlan(startKey)) {
                readers.push_back(std::make_unique<Impl::QueryReader>(*impl_, target.node,
                    impl_->openQueryRequest(Impl::QueryAction::SCAN, startKey, endKey, 0, target.partitions)));
                readers.back()->operationLifetime = operation;
            }
            impl_->scansTotal.fetch_add(1, std::memory_order_relaxed);
            return core::ArenaGenerator<ScanRecordView>::withArena(arena, [&] {
                return impl_->clusterScanGenerator(arena, std::move(readers), std::move(operation));
            });
        }
        return scanLocalStorage(arena, startKey, endKey);
    }

    core::ArenaGenerator<AkkEngine::ScanRecordView> AkkEngine::scanLocalStorage(
        core::BufferArena& arena,
        std::span<const uint8_t> startKey,
        std::span<const uint8_t> endKey
    ) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        auto operation = std::make_shared<Impl::OperationGuard>(*impl_);
        impl_->throwIfBackgroundFailed();
        impl_->waitForVersionLogRecovery();
        std::shared_lock transactionRead{impl_->transactionVisibilityMu};
        impl_->throwIfBackgroundFailed();
        impl_->waitForKeySequenceReadVisibility();
        impl_->scansTotal.fetch_add(1, std::memory_order_relaxed);
        if (impl_->usesPartitionRaft()) {
            return core::ArenaGenerator<ScanRecordView>::withArena(arena, [&] {
                return impl_->partitionPublicLocalScan({startKey.begin(), startKey.end()}, {endKey.begin(), endKey.end()}, std::move(operation));
            });
        }

        memtable::MemTable::KeyRange range;
        range.start.assign(startKey.begin(), startKey.end());
        range.end.assign(endKey.begin(), endKey.end());
        uint64_t seq = 0;
        auto mt = [&]() {
            if (impl_->opts.runtime.scanConsistency == AkkEngineOptions::ScanConsistencyMode::PINNED_SNAPSHOT) {
                auto pinned = impl_->memtable->pinnedIterator(range, [this] { return impl_->snapshotSeq(); });
                seq = pinned.snapshotSeq();
                return pinned;
            }
            seq = impl_->snapshotSeq();
            return impl_->memtable->iterator(range, seq);
        }();
        sst::SSTManager::Iterator st;
        if (impl_->sstManager) { st = impl_->sstManager->scanIter(startKey, endKey, seq); }
        return core::ArenaGenerator<ScanRecordView>::withArena(
            arena,
            [&arena, mt = std::move(mt), st = std::move(st), blobManager = impl_->blobManager.get(), operation = std::move(operation)
            ]() mutable {
                return scanGenerator(arena, std::move(mt), std::move(st), blobManager, std::move(operation));
            }
        );
    }

    std::optional<std::vector<uint8_t>> AkkEngine::getAt(std::span<const uint8_t> key, uint64_t atSeq) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        std::shared_lock transactionRead{impl_->transactionVisibilityMu};
        impl_->throwIfBackgroundFailed();
        return impl_->getAtValue(key, atSeq);
    }

    std::optional<std::vector<uint8_t>> AkkEngine::getAt(std::span<const uint8_t> key, Revision revision) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        if (!impl_->clusterRuntime) { throw std::logic_error("AkkEngine: Revision historical read requires cluster mode"); }
        const uint64_t stream = impl_->logicalStreamId(key);
        if (revision.streamId != stream) { throw std::invalid_argument("AkkEngine: revision stream does not own key"); }
        return impl_->getAtValue(key, revision.seq);
    }

    core::ArenaGenerator<VersionEntry> AkkEngine::history(std::span<const uint8_t> key) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        auto operation = std::make_shared<Impl::OperationGuard>(*impl_);
        impl_->throwIfBackgroundFailed();
        impl_->waitForVersionLogRecovery();
        std::shared_lock transactionRead{impl_->transactionVisibilityMu};
        impl_->throwIfBackgroundFailed();
        core::ArenaGenerator<VersionEntry> local;
        std::unique_ptr<Impl::QueryReader> reader;
        if (!impl_->clusterRuntime) { local = impl_->publicHistoryLocal(key); }
        else {
            const auto target = impl_->queryTargets(key, false).front();
            reader = std::make_unique<Impl::QueryReader>(*impl_, target, impl_->openQueryRequest(Impl::QueryAction::HISTORY, key, {}, 0));
        }
        return core::ArenaGenerator<VersionEntry>::withOwnedArena(4096, 65536, [&] {
            return Impl::publicHistoryGenerator(std::move(local), std::move(reader), std::move(operation));
        });
    }

    RollbackResult AkkEngine::rollbackTo(uint64_t targetSeq, RollbackOptions options) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        impl_->throwIfBackgroundFailed();
        impl_->requireClusterDisabledOperation("rollbackTo(uint64_t)");
        validateRollbackOptions(options);
        if (!impl_->versionLog) { throw std::runtime_error("AkkEngine: version log is disabled"); }
        RollbackResult result;
        crypto::secureRandom(result.operationId);
        std::optional<std::array<uint8_t, 16>> deferredTask;
        const uint64_t plannedWatermark = options.execution == RollbackExecutionMode::IMMEDIATE
                                              ? UINT64_MAX
                                              : impl_->captureRollbackWatermark();
        if (options.execution != RollbackExecutionMode::IMMEDIATE) {
            deferredTask = impl_->scheduleDeferredRollback(
                result.operationId, cluster::StripeControlAction::ROLLBACK_STREAM, false, 0, targetSeq,
                plannedWatermark, options.conflict
            );
            if (options.execution == RollbackExecutionMode::NEXT_STARTUP) {
                result.items.push_back(RollbackItemResult{
                    .key = {}, .status = RollbackItemStatus::DEFERRED, .message = "scheduled for the next startup"
                });
                return result;
            }
        }
        try {
            std::unique_lock epochLock{impl_->mutationEpochMu};
            impl_->versionLog->forceSync();
            for (const auto& key : impl_->rollbackKeysChangedAfter(targetSeq)) {
                result.items.push_back(impl_->rollbackKeyLocal(
                    key, targetSeq, options.conflict, 0,
                    deferredTask ? plannedWatermark : UINT64_MAX
                ));
            }
            if (deferredTask) { impl_->completeDeferredRollback(*deferredTask); }
        }
        catch (const std::exception& error) {
            if (!deferredTask) { throw; }
            result.items.push_back(RollbackItemResult{
                .key = {}, .status = RollbackItemStatus::DEFERRED, .message = error.what()
            });
        }
        return result;
    }

    RollbackResult AkkEngine::rollbackKey(std::span<const uint8_t> key, uint64_t targetSeq, RollbackOptions options) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        impl_->throwIfBackgroundFailed();
        impl_->requireClusterDisabledOperation("rollbackKey(uint64_t)");
        validateRollbackOptions(options);
        if (!impl_->versionLog) { throw std::runtime_error("AkkEngine: version log is disabled"); }
        RollbackResult result;
        crypto::secureRandom(result.operationId);
        std::optional<std::array<uint8_t, 16>> deferredTask;
        const uint64_t plannedWatermark = options.execution == RollbackExecutionMode::IMMEDIATE
                                              ? UINT64_MAX
                                              : impl_->captureRollbackWatermark();
        if (options.execution != RollbackExecutionMode::IMMEDIATE) {
            deferredTask = impl_->scheduleDeferredRollback(
                result.operationId, cluster::StripeControlAction::ROLLBACK_KEY, false, 0, targetSeq,
                plannedWatermark, options.conflict, key
            );
            if (options.execution == RollbackExecutionMode::NEXT_STARTUP) {
                result.items.push_back(RollbackItemResult{
                    .key = {key.begin(), key.end()},
                    .status = RollbackItemStatus::DEFERRED,
                    .message = "scheduled for the next startup",
                });
                return result;
            }
        }
        try {
            std::unique_lock epochLock{impl_->mutationEpochMu};
            impl_->versionLog->forceSync();
            result.items.push_back(impl_->rollbackKeyLocal(
                key, targetSeq, options.conflict, 0,
                deferredTask ? plannedWatermark : UINT64_MAX
            ));
            if (deferredTask) { impl_->completeDeferredRollback(*deferredTask); }
        }
        catch (const std::exception& error) {
            if (!deferredTask) { throw; }
            result.items.push_back(RollbackItemResult{
                .key = {key.begin(), key.end()}, .status = RollbackItemStatus::DEFERRED, .message = error.what()
            });
        }
        return result;
    }

    uint64_t AkkEngine::revisionStreamId(std::span<const uint8_t> key) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        if (!impl_->clusterRuntime) { throw std::logic_error("AkkEngine: revision streams require cluster mode"); }
        return impl_->logicalStreamId(key);
    }

    ClusterCheckpoint AkkEngine::createClusterCheckpoint() const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        impl_->throwIfBackgroundFailed();
        impl_->waitForVersionLogRecovery();
        if (!impl_->clusterRuntime) { throw std::logic_error("AkkEngine: cluster checkpoint requires cluster mode"); }
        if (!impl_->versionLog) { throw std::runtime_error("AkkEngine: version log is disabled"); }
        ClusterCheckpoint checkpoint;
        checkpoint.clusterId = impl_->clusterConfig.clusterId();
        checkpoint.configEpoch = impl_->clusterRuntime->configurationEpoch();
        checkpoint.timelineId = 1;
        if (impl_->usesPartitionRaft()) {
            const auto placement = impl_->livePlacement.load();
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{impl_->opts.cluster.runtime.forwardingTimeoutMs};
            for (uint16_t index = 0; index < placement->partition().partitionCount; ++index) {
                const auto response = impl_->partitionRpcLeader(index, Impl::partitionAdminHeader(Impl::PartitionAdmin::WATERMARK, index, 0), *placement, deadline);
                checkpoint.watermarks.push_back(Revision{.streamId = uint64_t{index} + 1, .seq = Impl::partitionAdminNumber(response.value)});
            }
            return checkpoint;
        }
        const auto watermarkFor = [&](uint64_t targetNodeId) {
            cluster::StripeControlRequest request;
            request.action = cluster::StripeControlAction::ROLLBACK_WATERMARK;
            request.ownerNodeId = targetNodeId;
            const auto response = impl_->clusterRuntime->rollbackControl(targetNodeId, std::move(request));
            if (response.status != cluster::StripeControlStatus::COMMITTED) {
                throw std::runtime_error(
                    "AkkEngine: checkpoint watermark request failed with status " +
                    std::to_string(static_cast<unsigned>(response.status))
                );
            }
            return response.fenceToken;
        };
        if (impl_->clusterReplicationMode == cluster::ReplicationMode::PARTITIONED) {
            for (const auto& node : impl_->clusterConfig.dataNodes()) {
                checkpoint.watermarks.push_back(Revision{.streamId = node.nodeId, .seq = watermarkFor(node.nodeId)});
            }
        }
        else if (impl_->clusterReplicationMode == cluster::ReplicationMode::STRIPE) {
            const uint64_t leader = impl_->clusterRuntime->stripeMetadataLeaderNodeId();
            if (leader == 0) { throw std::runtime_error("AkkEngine: STRIPE metadata leader is unavailable"); }
            checkpoint.watermarks.push_back(Revision{.streamId = 0, .seq = watermarkFor(leader)});
        }
        else {
            if (impl_->clusterRuntime->role() != cluster::NodeRole::PRIMARY) {
                throw std::runtime_error("AkkEngine: checkpoint must be created on the cluster leader");
            }
            checkpoint.watermarks.push_back(Revision{.streamId = 0, .seq = impl_->captureRollbackWatermark()});
        }
        return checkpoint;
    }

    RollbackResult AkkEngine::rollbackKey(std::span<const uint8_t> key, Revision target, RollbackOptions options) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        impl_->throwIfBackgroundFailed();
        impl_->waitForVersionLogRecovery();
        validateRollbackOptions(options);
        if (!impl_->clusterRuntime) { throw std::logic_error("AkkEngine: Revision rollback requires cluster mode"); }
        if (!impl_->versionLog) { throw std::runtime_error("AkkEngine: version log is disabled"); }
        if (impl_->usesPartitionRaft()) {
            if (target.streamId != impl_->logicalStreamId(key)) { throw std::invalid_argument("AkkEngine: revision stream does not own key"); }
            return impl_->partitionRollback(impl_->partitionIndex(key), target.seq, options, false, key);
        }
        uint64_t controlNode = impl_->nodeId;
        if (impl_->clusterReplicationMode == cluster::ReplicationMode::PARTITIONED) {
            controlNode = impl_->clusterRuntime->ownerNodeId(key);
            if (target.streamId != controlNode) { throw std::invalid_argument("AkkEngine: revision stream does not own key"); }
        }
        else {
            if (target.streamId != 0) { throw std::invalid_argument("AkkEngine: global revision stream id must be zero"); }
            if (impl_->clusterReplicationMode == cluster::ReplicationMode::STRIPE) {
                controlNode = impl_->clusterRuntime->stripeMetadataLeaderNodeId();
                if (controlNode == 0) { throw std::runtime_error("AkkEngine: STRIPE metadata leader is unavailable"); }
            }
            else if (impl_->clusterRuntime->role() != cluster::NodeRole::PRIMARY) {
                throw std::runtime_error("AkkEngine: rollback must be submitted to the cluster leader");
            }
        }
        cluster::StripeControlRequest watermarkRequest;
        watermarkRequest.action = cluster::StripeControlAction::ROLLBACK_WATERMARK;
        watermarkRequest.ownerNodeId = controlNode;
        const auto watermarkResponse = impl_->clusterRuntime->rollbackControl(controlNode, std::move(watermarkRequest));
        if (watermarkResponse.status != cluster::StripeControlStatus::COMMITTED) {
            throw std::runtime_error(
                "AkkEngine: rollback planning watermark request failed with status " +
                std::to_string(static_cast<unsigned>(watermarkResponse.status))
            );
        }
        const uint64_t plannedWatermark = watermarkResponse.fenceToken;
        if (target.seq > plannedWatermark) {
            throw std::invalid_argument("AkkEngine: revision is ahead of the current stream watermark");
        }
        RollbackResult result;
        crypto::secureRandom(result.operationId);
        std::optional<std::array<uint8_t, 16>> deferredTask;
        if (options.execution != RollbackExecutionMode::IMMEDIATE) {
            deferredTask = impl_->scheduleDeferredRollback(
                result.operationId, cluster::StripeControlAction::ROLLBACK_KEY, true,
                impl_->clusterReplicationMode == cluster::ReplicationMode::STRIPE ? 0 : controlNode,
                target.seq, plannedWatermark, options.conflict, key
            );
            if (options.execution == RollbackExecutionMode::NEXT_STARTUP) {
                result.items.push_back(RollbackItemResult{
                    .key = {key.begin(), key.end()},
                    .status = RollbackItemStatus::DEFERRED,
                    .message = "scheduled for the next startup",
                });
                return result;
            }
        }
        cluster::StripeControlRequest request;
        request.action = cluster::StripeControlAction::ROLLBACK_KEY;
        request.ownerNodeId = controlNode;
        request.fenceToken = target.seq;
        request.key.assign(key.begin(), key.end());
        request.metadata = {static_cast<uint8_t>(RollbackExecutionMode::IMMEDIATE), static_cast<uint8_t>(options.conflict)};
        if (deferredTask) { Impl::pushU64(request.metadata, plannedWatermark); }
        try {
            const auto response = impl_->clusterRuntime->rollbackControl(controlNode, std::move(request));
            if (response.status != cluster::StripeControlStatus::COMMITTED) {
                throw std::runtime_error("AkkEngine: cluster rollback request was rejected");
            }
            result.items = Impl::decodeRollbackItems(response.metadata);
            if (deferredTask) { impl_->completeDeferredRollback(*deferredTask); }
        }
        catch (const std::exception& error) {
            if (!deferredTask) { throw; }
            result.items.push_back(RollbackItemResult{
                .key = {key.begin(), key.end()}, .status = RollbackItemStatus::DEFERRED, .message = error.what()
            });
        }
        return result;
    }

    RollbackResult AkkEngine::rollbackTo(const ClusterCheckpoint& target, RollbackOptions options) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        impl_->throwIfBackgroundFailed();
        impl_->waitForVersionLogRecovery();
        validateRollbackOptions(options);
        if (!impl_->clusterRuntime) { throw std::logic_error("AkkEngine: ClusterCheckpoint rollback requires cluster mode"); }
        if (!impl_->versionLog) { throw std::runtime_error("AkkEngine: version log is disabled"); }
        if (impl_->usesPartitionRaft()) {
            if (target.clusterId != impl_->clusterConfig.clusterId() || target.timelineId != 1 ||
                target.watermarks.size() != impl_->clusterConfig.partition().partitionCount) {
                throw std::invalid_argument("AkkEngine: checkpoint belongs to another partition layout or timeline");
            }
            std::map<uint64_t, uint64_t> streams;
            for (const auto& revision : target.watermarks) {
                if (revision.streamId == 0 || revision.streamId > impl_->clusterConfig.partition().partitionCount ||
                    !streams.emplace(revision.streamId, revision.seq).second) {
                    throw std::invalid_argument("AkkEngine: checkpoint contains unknown or duplicate partition streams");
                }
            }
            const auto current = createClusterCheckpoint();
            for (const auto& revision : current.watermarks) {
                if (streams.at(revision.streamId) > revision.seq) { throw std::invalid_argument("AkkEngine: checkpoint is ahead of a current stream"); }
            }
            RollbackResult result;
            crypto::secureRandom(result.operationId);
            for (const auto& [stream, sequence] : streams) {
                auto part = impl_->partitionRollback(static_cast<uint16_t>(stream - 1), sequence, options, true);
                result.items.insert(result.items.end(), std::make_move_iterator(part.items.begin()), std::make_move_iterator(part.items.end()));
            }
            return result;
        }
        if (target.clusterId != impl_->clusterConfig.clusterId() || (impl_->clusterReplicationMode != cluster::ReplicationMode::STRIPE && target.configEpoch != impl_->clusterRuntime->configurationEpoch()) ||
            target.timelineId != 1) {
            throw std::invalid_argument("AkkEngine: checkpoint belongs to a different cluster configuration or timeline");
        }
        std::unordered_map<uint64_t, uint64_t> watermarks;
        for (const auto& revision : target.watermarks) {
            if (!watermarks.emplace(revision.streamId, revision.seq).second) {
                throw std::invalid_argument("AkkEngine: checkpoint contains duplicate streams");
            }
        }
        std::vector<std::pair<uint64_t, uint64_t>> requests;
        if (impl_->clusterReplicationMode == cluster::ReplicationMode::PARTITIONED) {
            for (const auto& node : impl_->clusterConfig.dataNodes()) {
                const auto it = watermarks.find(node.nodeId);
                if (it == watermarks.end()) { throw std::invalid_argument("AkkEngine: checkpoint is missing a partition stream"); }
                requests.emplace_back(node.nodeId, it->second);
            }
            if (watermarks.size() != requests.size()) { throw std::invalid_argument("AkkEngine: checkpoint contains unknown streams"); }
        }
        else {
            const auto it = watermarks.find(0);
            if (it == watermarks.end() || watermarks.size() != 1) {
                throw std::invalid_argument("AkkEngine: checkpoint must contain exactly the global stream");
            }
            uint64_t controlNode = impl_->nodeId;
            if (impl_->clusterReplicationMode == cluster::ReplicationMode::STRIPE) {
                controlNode = impl_->clusterRuntime->stripeMetadataLeaderNodeId();
                if (controlNode == 0) { throw std::runtime_error("AkkEngine: STRIPE metadata leader is unavailable"); }
            }
            else if (impl_->clusterRuntime->role() != cluster::NodeRole::PRIMARY) {
                throw std::runtime_error("AkkEngine: rollback must be submitted to the cluster leader");
            }
            requests.emplace_back(controlNode, it->second);
        }
        RollbackResult result;
        crypto::secureRandom(result.operationId);
        struct PlannedStreamRollback {
            uint64_t controlNode = 0;
            uint64_t targetWatermark = 0;
            uint64_t plannedWatermark = 0;
        };
        std::vector<PlannedStreamRollback> plannedRequests;
        plannedRequests.reserve(requests.size());
        for (const auto& [controlNode, targetWatermark] : requests) {
            cluster::StripeControlRequest watermarkRequest;
            watermarkRequest.action = cluster::StripeControlAction::ROLLBACK_WATERMARK;
            watermarkRequest.ownerNodeId = controlNode;
            const auto watermarkResponse = impl_->clusterRuntime->rollbackControl(controlNode, std::move(watermarkRequest));
            if (watermarkResponse.status != cluster::StripeControlStatus::COMMITTED) {
                throw std::runtime_error(
                    "AkkEngine: rollback planning watermark request failed with status " +
                    std::to_string(static_cast<unsigned>(watermarkResponse.status))
                );
            }
            if (targetWatermark > watermarkResponse.fenceToken) {
                throw std::invalid_argument("AkkEngine: checkpoint is ahead of a current stream watermark");
            }
            plannedRequests.push_back(PlannedStreamRollback{
                .controlNode = controlNode,
                .targetWatermark = targetWatermark,
                .plannedWatermark = watermarkResponse.fenceToken,
            });
        }
        for (const auto& planned : plannedRequests) {
            std::optional<std::array<uint8_t, 16>> deferredTask;
            if (options.execution != RollbackExecutionMode::IMMEDIATE) {
                deferredTask = impl_->scheduleDeferredRollback(
                    result.operationId, cluster::StripeControlAction::ROLLBACK_STREAM, true,
                    impl_->clusterReplicationMode == cluster::ReplicationMode::STRIPE ? 0 : planned.controlNode,
                    planned.targetWatermark, planned.plannedWatermark, options.conflict
                );
                if (options.execution == RollbackExecutionMode::NEXT_STARTUP) {
                    result.items.push_back(RollbackItemResult{
                        .key = {},
                        .status = RollbackItemStatus::DEFERRED,
                        .message = "stream " + std::to_string(planned.controlNode) + " is scheduled for the next startup",
                    });
                    continue;
                }
            }
            try {
                std::vector<uint8_t> cursor;
                for (;;) {
                    cluster::StripeControlRequest request;
                    request.action = cluster::StripeControlAction::ROLLBACK_STREAM;
                    request.ownerNodeId = planned.controlNode;
                    request.fenceToken = planned.targetWatermark;
                    request.key = cursor;
                    request.metadata = {
                        static_cast<uint8_t>(RollbackExecutionMode::IMMEDIATE),
                        static_cast<uint8_t>(options.conflict),
                    };
                    if (deferredTask) { Impl::pushU64(request.metadata, planned.plannedWatermark); }
                    const auto response = impl_->clusterRuntime->rollbackControl(planned.controlNode, std::move(request));
                    if (response.status != cluster::StripeControlStatus::COMMITTED) {
                        throw std::runtime_error("AkkEngine: cluster rollback stream request was rejected");
                    }
                    auto batch = Impl::decodeRollbackStreamBatch(response.metadata);
                    result.items.insert(
                        result.items.end(), std::make_move_iterator(batch.items.begin()),
                        std::make_move_iterator(batch.items.end())
                    );
                    if (batch.done) { break; }
                    cursor = std::move(batch.nextCursor);
                }
                if (deferredTask) { impl_->completeDeferredRollback(*deferredTask); }
            }
            catch (const std::exception& error) {
                if (!deferredTask) { throw; }
                result.items.push_back(RollbackItemResult{
                    .key = {}, .status = RollbackItemStatus::DEFERRED, .message = error.what()
                });
            }
        }
        return result;
    }

    EngineStats AkkEngine::stats() const noexcept {
        EngineStats out;
        if (!impl_) { return out; }
        Impl::OperationGuard operation{*impl_, false};
        if (!operation) { return out; }

        out.currentSeq = impl_->snapshotSeq();
        out.nodeId = impl_->nodeId;
        {
            std::lock_guard lock{impl_->stripeRepairStatsMu};
            out.stripeReadRepair = impl_->stripeRepairStats;
        }
        {
            std::lock_guard lock{impl_->stripeRebuildStatsMu};
            out.stripeRebuild = impl_->stripeRebuildStats;
        }

        out.putsTotal = impl_->putsTotal.load(std::memory_order_relaxed);
        out.removesTotal = impl_->removesTotal.load(std::memory_order_relaxed);
        out.getsTotal = impl_->getsTotal.load(std::memory_order_relaxed);
        out.getsMemtableHit = impl_->getsMemtableHit.load(std::memory_order_relaxed);
        out.getsSstHit = impl_->getsSstHit.load(std::memory_order_relaxed);
        out.getsMiss = impl_->getsMiss.load(std::memory_order_relaxed);
        out.existsTotal = impl_->existsTotal.load(std::memory_order_relaxed);
        out.scansTotal = impl_->scansTotal.load(std::memory_order_relaxed);
        out.blobPutsTotal = impl_->blobPutsTotal.load(std::memory_order_relaxed);
        out.config.writeAdmission = static_cast<uint32_t>(impl_->opts.runtime.writeAdmission);
        out.config.writeDurability = static_cast<uint32_t>(impl_->opts.runtime.writeDurability);
        out.config.writeVisibility = static_cast<uint32_t>(impl_->opts.runtime.writeVisibility);
        out.config.readVisibility = static_cast<uint32_t>(impl_->opts.runtime.visibility.readVisibility);
        out.config.sequenceAllocation = static_cast<uint32_t>(impl_->opts.runtime.sequence.allocation);
        out.config.sequenceThreadLocalRangeSize = impl_->opts.runtime.sequence.threadLocalRangeSize;
        out.config.commitWindowSize = impl_->commitWindowSize;
        out.config.memtableBackpressureMode = static_cast<uint32_t>(impl_->opts.runtime.backpressure.memtableFlushBacklog);
        out.config.maxMemtableImmutableTables = impl_->opts.runtime.backpressure.maxMemtableImmutableTables;
        out.config.sstBackpressureMode = static_cast<uint32_t>(impl_->opts.runtime.backpressure.sstCompactionBacklog);
        out.config.maxSstL0Files = impl_->opts.runtime.backpressure.maxSstL0Files;
        out.config.backpressureWaitMicros = impl_->opts.runtime.backpressure.waitMicros;
        out.config.backpressureTimeoutMs = impl_->opts.runtime.backpressure.timeoutMs;
        out.config.memtableFlushMode = static_cast<uint32_t>(impl_->opts.memtable.flushMode);
        out.config.walExecution = static_cast<uint32_t>(impl_->opts.wal.execution);
        out.config.walSyncPolicy = static_cast<uint32_t>(impl_->opts.wal.syncPolicy);
        out.config.walBackpressure = static_cast<uint32_t>(impl_->opts.wal.backpressure);
        out.config.sstCompactionMode = static_cast<uint32_t>(impl_->opts.sst.compactionMode);
        out.backpressure.blockedWrites = impl_->backpressureBlockedWrites.load(std::memory_order_relaxed);
        out.backpressure.rejectedWrites = impl_->backpressureRejectedWrites.load(std::memory_order_relaxed);
        out.backpressure.timedOutWrites = impl_->backpressureTimedOutWrites.load(std::memory_order_relaxed);
        out.backpressure.memtableStalls = impl_->backpressureMemtableStalls.load(std::memory_order_relaxed);
        out.backpressure.sstStalls = impl_->backpressureSstStalls.load(std::memory_order_relaxed);
        out.backpressure.waitMicrosTotal = impl_->backpressureWaitMicrosTotal.load(std::memory_order_relaxed);
        out.backpressure.waitMicrosMax = impl_->backpressureWaitMicrosMax.load(std::memory_order_relaxed);
        out.api.enabled = impl_->opts.components.apiEnabled;
        if (impl_->apiServer) { out.api = impl_->apiServer->stats(); }

        if (impl_->memtable) {
            const auto snap = impl_->memtable->snapshot();
            out.memtable.shardCount = snap.shardCount;
            out.memtable.thresholdBytesPerShard = snap.thresholdBytesPerShard;
            out.memtable.approxBytes = snap.approxBytes;
            out.memtable.putsApplied = snap.putsApplied;
            out.memtable.removesApplied = snap.removesApplied;
            out.memtable.flushesCompleted = snap.flushesCompleted;
            out.memtable.immutableTables = snap.immutableTables;
            out.memtable.memorySnapshotCompactionsCompleted = snap.memorySnapshotCompactionsCompleted;
            out.memtable.memorySnapshotCompactionFailures = snap.memorySnapshotCompactionFailures;
            out.memtable.memorySnapshotCompactionLastFailureAtUs = snap.memorySnapshotCompactionLastFailureAtUs;
            out.memtable.memorySnapshotCompactionPending = snap.memorySnapshotCompactionPending;
        }

        out.wal.enabled = impl_->opts.components.walEnabled;
        if (impl_->walWriter) {
            const auto snap = impl_->walWriter->snapshot();
            out.wal.shardCount = snap.shardCount;
            out.wal.entriesWritten = snap.entriesWritten;
            out.wal.bytesWritten = snap.bytesWritten;
            out.wal.batchesFlushed = snap.batchesFlushed;
            out.wal.syncsExecuted = snap.syncsExecuted;
            out.wal.segmentRotations = snap.segmentRotations;
            out.wal.asyncFailures = snap.asyncFailures;
            out.wal.pendingEntries = snap.pendingEntries;
            out.wal.pendingBytes = snap.pendingBytes;
            out.wal.inFlightBytes = snap.inFlightBytes;
            out.wal.healthy = snap.healthy;
        }

        out.blob.enabled = impl_->opts.components.blobEnabled && impl_->blobManager != nullptr;
        out.blob.thresholdBytes = impl_->opts.blob.thresholdBytes;
        if (impl_->blobManager) {
            const auto snap = impl_->blobManager->snapshot();
            out.blob.blobsWritten = snap.blobsWritten;
            out.blob.bytesUncompressed = snap.bytesUncompressed;
            out.blob.bytesOnDisk = snap.bytesOnDisk;
            out.blob.blobsDeleted = snap.blobsDeleted;
            out.blob.gcCycles = snap.gcCycles;
        }

        out.sst.enabled = impl_->sstManager != nullptr;
        if (impl_->sstManager) {
            const auto levels = impl_->sstManager->levelStats();
            out.sst.levels.reserve(levels.size());
            for (const auto& level : levels) {
                out.sst.levels.push_back(LevelStats{level.level, level.fileCount, level.bytes, level.budgetBytes});
                out.sst.fileCount += level.fileCount;
                out.sst.bytes += level.bytes;
                if (level.level == 0) { out.sst.l0FileCount = level.fileCount; }
            }
            out.sst.compactionPending = impl_->sstManager->compactionPending();
            const auto snap = impl_->sstManager->compactionSnapshot();
            out.sst.compactionsCompleted = snap.compactionsCompleted;
            out.sst.filesCompacted = snap.filesCompacted;
            out.sst.bytesCompactedIn = snap.bytesCompactedIn;
            out.sst.bytesCompactedOut = snap.bytesCompactedOut;
            out.sst.compactionFailures = snap.compactionFailures;
        }

        out.manifest.enabled = impl_->manifest != nullptr;
        if (impl_->manifest) {
            const auto checkpoint = impl_->manifest->lastCheckpoint();
            out.manifest.hasCheckpoint = checkpoint.has_value();
            if (checkpoint.has_value()) {
                out.manifest.lastCheckpointSeq = checkpoint->lastSeq.value_or(0);
                out.manifest.lastCheckpointStripe = checkpoint->stripe.value_or(0);
            }
            out.manifest.liveSstCount = impl_->manifest->liveSst().size();
            out.manifest.deletedSstCount = impl_->manifest->deletedSst().size();
            out.manifest.sstSealCount = impl_->manifest->sstSeals().size();
            out.manifest.sstReferencedBlobCount = impl_->manifest->sstReferencedBlobs().size();
            out.manifest.sstBlobRefsComplete = impl_->manifest->sstBlobRefsComplete();
            out.manifest.liveBlobCount = impl_->manifest->liveBlobs().size();
            out.manifest.deletedBlobCount = impl_->manifest->deletedBlobs().size();
            out.manifest.blobPutCount = impl_->manifest->blobPuts().size();
            out.manifest.blobDeleteCount = impl_->manifest->blobDeletes().size();
            if (const auto lease = impl_->manifest->lastPrimaryLease(); lease.has_value()) {
                out.manifest.lastPrimaryLeaseNodeId = lease->nodeId;
                out.manifest.lastPrimaryLeaseUntilUs = lease->leaseUntilUs;
            }
        }

        out.cluster.enabled = impl_->opts.components.clusterEnabled && impl_->clusterRuntime != nullptr;
        out.cluster.configuredNodeCount = impl_->clusterConfiguredNodeCount;
        if (out.cluster.enabled) {
            out.cluster.clusterId = impl_->clusterConfig.clusterId();
            out.cluster.replicationMode = static_cast<uint32_t>(impl_->clusterConfig.mode());
            out.cluster.consistencyMode = static_cast<uint32_t>(impl_->clusterConfig.consistency().mode);
            out.cluster.transportMode = static_cast<uint32_t>(impl_->opts.cluster.runtime.transportMode);
            out.cluster.clusterGroupId = impl_->opts.cluster.runtime.clusterGroupId;
            out.cluster.clusterGroupEpoch = impl_->opts.cluster.runtime.clusterGroupEpoch;
            const auto configured = impl_->usesPartitionRaft() || impl_->clusterReplicationMode == cluster::ReplicationMode::STRIPE
                ? impl_->currentPlacement().nodes() : impl_->clusterConfig.usesDataConsensus()
                ? impl_->clusterRuntime->activeNodes() : impl_->clusterConfig.nodes();
            out.cluster.configuredNodeCount = static_cast<uint32_t>(configured.size());
            out.cluster.configuredNodes.reserve(configured.size());
            for (const auto& node : configured) {
                out.cluster.configuredNodes.push_back({
                    .nodeId = node.nodeId,
                    .host = node.host,
                    .dataPort = node.dataPort,
                    .replPort = node.replPort,
                    .stripeMetadataPort = node.stripeMetadataPort,
                    .capabilities = node.capabilities,
                });
            }
        }
        if (impl_->clusterRuntime) {
            out.cluster.role = static_cast<uint32_t>(impl_->clusterRuntime->role());
            out.cluster.activeNodeCount = impl_->clusterRuntime->activeNodes().size();
            const auto runtime = impl_->clusterRuntime->raftStats();
            const auto metadataRuntime = impl_->clusterRuntime->stripeMetadataRaftStats();
            out.cluster.sampledAtUs = runtime.sampledAtUs;
            out.cluster.runtimeStartedAtUs = runtime.runtimeStartedAtUs;
            out.cluster.clusterGroupId = runtime.clusterGroupId;
            out.cluster.clusterGroupEpoch = runtime.clusterGroupEpoch;
            out.cluster.health = runtime.health;
            if (out.cluster.health != ClusterHealthState::FAILED && out.stripeRebuild.active) {
                out.cluster.health = ClusterHealthState::REBUILDING;
            }
            out.cluster.lastFailure = runtime.lastFailure;
            out.cluster.lastFailureAtUs = runtime.lastFailureAtUs;
            out.cluster.raftEnabled = runtime.enabled;
            if (runtime.enabled) {
                out.cluster.activeNodeCount = 1;
                for (const auto& peer : runtime.peers) { if (peer.connected) { ++out.cluster.activeNodeCount; } }
            }
            out.cluster.raftTerm = runtime.currentTerm;
            out.cluster.leaderNodeId = runtime.leaderNodeId;
            out.cluster.commitIndex = runtime.commitIndex;
            out.cluster.appliedIndex = runtime.appliedIndex;
            out.cluster.lastLogIndex = runtime.lastLogIndex;
            out.cluster.snapshotIndex = runtime.snapshotIndex;
            out.cluster.voterCount = runtime.voterCount;
            out.cluster.learnerCount = runtime.learnerCount;
            out.cluster.localLearner = runtime.localLearner;
            out.cluster.mirrorAuthorityLeaderNodeId = runtime.mirrorAuthorityLeaderNodeId;
            out.cluster.mirrorAuthorityPrimaryNodeId = runtime.mirrorAuthorityPrimaryNodeId;
            out.cluster.mirrorAuthorityEpoch = runtime.mirrorAuthorityEpoch;
            out.cluster.mirrorAuthorityCommitIndex = runtime.mirrorAuthorityCommitIndex;
            out.cluster.mirrorWritePending = runtime.mirrorWritePending;
            out.cluster.stripeMetadataRaftEnabled = metadataRuntime.enabled;
            out.cluster.stripeMetadataRaftTerm = metadataRuntime.currentTerm;
            out.cluster.stripeMetadataLeaderNodeId = metadataRuntime.leaderNodeId;
            out.cluster.stripeMetadataCommitIndex = metadataRuntime.commitIndex;
            out.cluster.stripeMetadataAppliedIndex = metadataRuntime.appliedIndex;
            out.cluster.stripeMetadataSnapshotIndex = metadataRuntime.snapshotIndex;
            out.cluster.outboundConnectionsTotal = runtime.outboundConnections;
            out.cluster.peerWorkers = runtime.peerWorkers;
            out.cluster.proposalBatches = runtime.proposalBatches;
            out.cluster.proposalQueueDepth = runtime.proposalQueueDepth;
            out.cluster.pendingProposals = runtime.pendingProposals;
            out.cluster.retainedRequestResults = runtime.retainedRequestResults;
            out.cluster.pendingRequests = runtime.pendingRequests;
            out.cluster.requestCapacity = runtime.requestCapacity;
            out.cluster.expiredRequestsTotal = runtime.expiredRequests;
            out.cluster.rejectedRequestsTotal = runtime.rejectedRequests;
            out.cluster.requestJournalBytes = runtime.requestJournalBytes;
            out.cluster.requestJournalRecords = runtime.requestJournalRecords;
            out.cluster.requestJournalBytesWrittenTotal = runtime.requestJournalBytesWritten;
            out.cluster.requestJournalCompactionsTotal = runtime.requestJournalCompactions;
            out.cluster.peerPolicyMismatchRejectsTotal = runtime.peerPolicyMismatchRejects;
            out.cluster.foreignClusterRejectsTotal = runtime.foreignClusterRejects;
            out.cluster.leaseRenewFailuresTotal = runtime.leaseRenewFailures;
            out.cluster.endpointStartFailuresTotal = runtime.endpointStartFailures;
            out.cluster.peerReadTimeoutsTotal = runtime.peerReadTimeouts;
            out.cluster.replicationQueueFrames = runtime.replicationQueueFrames;
            out.cluster.replicationQueueBytes = runtime.replicationQueueBytes;
            out.cluster.transferMemoryBytes = runtime.transferMemoryBytes;
            out.cluster.transferSpoolBytes = runtime.transferSpoolBytes;
            out.cluster.activeTransfers = runtime.activeTransfers;
            out.cluster.transferResumeAttemptsTotal = runtime.transferResumeAttempts;
            out.cluster.transferResumedTotal = runtime.transferResumed;
            out.cluster.transferResumedBytesTotal = runtime.transferResumedBytes;
            out.cluster.transferDiscardedPartialsTotal = runtime.transferDiscardedPartials;
            out.cluster.transferRetainedPartials = runtime.transferRetainedPartials;
            out.cluster.peers.reserve(runtime.peers.size());
            for (const auto& peer : runtime.peers) {
                out.cluster.peers.push_back({
                    .nodeId = peer.nodeId,
                    .learner = peer.learner,
                    .matchIndex = peer.matchIndex,
                    .nextIndex = peer.nextIndex,
                    .replicationLag = peer.replicationLag,
                    .connected = peer.connected,
                    .lastSuccessfulContactAtUs = peer.lastSuccessfulContactAtUs,
                    .roundTripsSucceededTotal = peer.roundTripsSucceeded,
                    .roundTripsFailedTotal = peer.roundTripsFailed,
                    .consecutiveRoundTripFailures = peer.consecutiveRoundTripFailures,
                    .lastRoundTripAtUs = peer.lastRoundTripAtUs,
                    .lastRoundTripFailureAtUs = peer.lastRoundTripFailureAtUs,
                    .roundTripLatencyUs = peer.roundTripLatencyUs,
                });
            }
        }

        out.vlog.enabled = impl_->versionLog != nullptr;
        if (impl_->versionLog) {
            const auto snap = impl_->versionLog->snapshot();
            out.vlog.syncMode = snap.syncMode;
            out.vlog.groupN = snap.groupN;
            out.vlog.groupMicros = snap.groupMicros;
            out.vlog.groupBytes = snap.groupBytes;
            out.vlog.asyncMaxPendingBytes = snap.asyncMaxPendingBytes;
            out.vlog.indexedKeys = snap.indexedKeys;
            out.vlog.indexedEntries = snap.indexedEntries;
            out.vlog.rollbackEntries = snap.rollbackEntries;
            out.vlog.pendingWrites = snap.pendingWrites;
            out.vlog.pendingBytes = snap.pendingBytes;
            out.vlog.durableBytes = snap.durableBytes;
            out.vlog.segmentCount = snap.segmentCount;
            out.vlog.activeSegmentBytes = snap.activeSegmentBytes;
            out.vlog.recoveryDurationMicros = snap.recoveryDurationMicros;
            out.vlog.recoveredSegmentCount = snap.recoveredSegmentCount;
            out.vlog.recoveredEntryCount = snap.recoveredEntryCount;
            out.vlog.sidecarFallbackCount = snap.sidecarFallbackCount;
            out.vlog.sidecarRebuildFailures = snap.sidecarRebuildFailures;
            out.vlog.retentionPrunedSegments = snap.retentionPrunedSegments;
            out.vlog.retentionBaseEntriesWritten = snap.retentionBaseEntriesWritten;
            out.vlog.parallelQueueRejects = snap.parallelQueueRejects;
            out.vlog.parallelLaneCount = snap.parallelLaneCount;
            out.vlog.parallelPendingWrites = snap.parallelPendingWrites;
            out.vlog.parallelPendingBytes = snap.parallelPendingBytes;
            out.vlog.flushThreadRunning = snap.flushThreadRunning;
        }
        return out;
    }

    void AkkEngine::addClusterVotingNode(const cluster::NodeInfo& node) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        if (!impl_->clusterRuntime) { throw std::runtime_error("AkkEngine: cluster runtime is not enabled"); }
        impl_->clusterRuntime->addRaftVotingNode(node);
    }

    void AkkEngine::addClusterLearner(const cluster::NodeInfo& node) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        if (!impl_->clusterRuntime) { throw std::runtime_error("AkkEngine: cluster runtime is not enabled"); }
        impl_->clusterRuntime->addRaftLearner(node);
    }

    void AkkEngine::transferMirrorAuthorityLeadership(uint64_t targetNodeId) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        if (!impl_->clusterRuntime) { throw std::runtime_error("AkkEngine: cluster runtime is not enabled"); }
        impl_->clusterRuntime->transferMirrorAuthorityLeadership(targetNodeId);
    }

    bool AkkEngine::recoverMirrorWrite() {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        if (!impl_->clusterRuntime) { throw std::runtime_error("AkkEngine: cluster runtime is not enabled"); }
        return impl_->clusterRuntime->recoverMirrorWrite();
    }

    void AkkEngine::promoteClusterLearner(uint64_t nodeId) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        if (!impl_->clusterRuntime) { throw std::runtime_error("AkkEngine: cluster runtime is not enabled"); }
        impl_->clusterRuntime->promoteRaftLearner(nodeId);
    }

    void AkkEngine::removeClusterLearner(uint64_t nodeId) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        if (!impl_->clusterRuntime) { throw std::runtime_error("AkkEngine: cluster runtime is not enabled"); }
        impl_->clusterRuntime->removeRaftLearner(nodeId);
    }

    void AkkEngine::removeClusterVotingNode(uint64_t nodeId) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        if (!impl_->clusterRuntime) { throw std::runtime_error("AkkEngine: cluster runtime is not enabled"); }
        impl_->clusterRuntime->removeRaftVotingNode(nodeId);
    }

    void AkkEngine::transferClusterLeadership(uint64_t targetNodeId) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        if (!impl_->clusterRuntime) { throw std::runtime_error("AkkEngine: cluster runtime is not enabled"); }
        impl_->clusterRuntime->transferRaftLeadership(targetNodeId);
    }

    void AkkEngine::campaignClusterLeadership(uint64_t streamId) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        if (!impl_->clusterRuntime) { throw std::logic_error("AkkEngine: cluster runtime is not enabled"); }
        if (impl_->usesPartitionRaft()) {
            auto children = impl_->partitionSnapshot();
            if (streamId == 0 || streamId > children.size() || !children[streamId - 1]) {
                throw std::invalid_argument("AkkEngine: campaign requires a locally hosted partition stream");
            }
            children[streamId - 1]->campaignClusterLeadership();
        }
        else {
            if (streamId != 0) { throw std::invalid_argument("AkkEngine: MIRROR uses revision stream zero"); }
            impl_->clusterRuntime->campaignLeadership();
        }
    }

    bool AkkEngine::cancelClusterReconfiguration() {
        if (!impl_) { throw std::logic_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        return impl_->cancelPlacement();
    }

    void AkkEngine::reconfigureCluster(cluster::ClusterConfig config) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        impl_->reconfigureCluster(std::move(config));
    }

    ClusterReconfigurationStatus AkkEngine::clusterReconfigurationStatus() const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        if (!impl_->usesPartitionRaft() && impl_->clusterReplicationMode != cluster::ReplicationMode::STRIPE) { throw std::logic_error("AkkEngine: placement status requires PARTITIONED consensus or STRIPE"); }
        const auto state = impl_->clusterRuntime->placementState(true);
        ClusterReconfigurationStatus result;
        result.authorityNodeId = state.leaderNodeId;
        result.activeGeneration = state.generation;
        result.pendingGeneration = state.pendingGeneration;
        result.stripeWritesBlocked = state.stripeWritesBlocked;
        result.activationStarted = state.activationStarted;
        result.partitionCount = impl_->clusterReplicationMode == cluster::ReplicationMode::STRIPE ? 1 : impl_->clusterConfig.partition().partitionCount;
        { std::lock_guard progress{impl_->placementProgressMu}; result.lastError = impl_->placementLastError; }
        if (state.pendingGeneration == 0) { result.completedPartitions = result.partitionCount; }
        if (impl_->clusterReplicationMode == cluster::ReplicationMode::STRIPE) {
            const auto response = impl_->partitionRpc(state.leaderNodeId, Impl::partitionAdminHeader(Impl::PartitionAdmin::STATUS, 0, 0));
            size_t cursor = 0;
            if (!Impl::pullU64(response.value, cursor, result.transferredBytes) ||
                !Impl::pullU64(response.value, cursor, result.completedRecords) || cursor != response.value.size()) {
                throw std::runtime_error("Cluster placement: malformed stripe progress response");
            }
            return result;
        }
        if (state.pendingGeneration == 0) { return result; }
        const auto active = cluster::ClusterConfig::decode(state.activeConfig);
        const auto target = cluster::ClusterConfig::decode(state.pendingConfig);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{impl_->opts.cluster.runtime.forwardingTimeoutMs};
        for (uint16_t index = 0; index < result.partitionCount; ++index) {
            try {
                const auto response = impl_->partitionRpcLeader(index, Impl::partitionAdminHeader(Impl::PartitionAdmin::STATUS, index, 0), active, deadline);
                if (response.value.size() < 16 || response.value[11] != 0 ||
                    Impl::partitionAdminNumber(std::span{response.value}.first(11)) < state.pendingGeneration) { continue; }
                size_t position = 12;
                uint32_t count = 0;
                if (!Impl::pullU32(response.value, position, count)) { continue; }
                std::vector<uint64_t> members;
                for (uint32_t i = 0; i < count; ++i) {
                    uint64_t id = 0;
                    if (!Impl::pullU64(response.value, position, id)) { throw std::runtime_error("Cluster placement: malformed status membership"); }
                    members.push_back(id);
                }
                std::vector<uint64_t> desired;
                for (const auto& member : impl_->partitionConfig(target, index).dataNodes()) { desired.push_back(member.nodeId); }
                std::ranges::sort(members); std::ranges::sort(desired);
                if (position == response.value.size() && members == desired) { ++result.completedPartitions; }
            }
            catch (const std::exception& error) { result.lastError = error.what(); }
        }
        return result;
    }

    std::vector<ClusterPartitionStats> AkkEngine::clusterPartitionStats() const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        if (!impl_->usesPartitionRaft()) { throw std::logic_error("AkkEngine: partition statistics require PARTITIONED Raft"); }
        std::vector<ClusterPartitionStats> result;
        const auto children = impl_->partitionSnapshot();
        for (size_t index = 0; index < children.size(); ++index) {
            ClusterPartitionStats item;
            item.streamId = index + 1;
            if (children[index]) {
                const auto members = children[index]->impl_->clusterRuntime->activeNodes();
                item.localHolder = std::ranges::any_of(members, [&](const auto& member) { return member.nodeId == impl_->nodeId && !member.raftLearner(); });
            }
            if (children[index]) {
                const auto stats = children[index]->impl_->clusterRuntime->raftStats();
                item.leaderNodeId = stats.leaderNodeId;
                item.currentTerm = stats.currentTerm;
                item.currentSeq = children[index]->impl_->snapshotSeq();
                item.commitIndex = stats.commitIndex;
                item.configurationGeneration = stats.configurationGeneration;
                item.storage = children[index]->stats();
            }
            result.push_back(item);
        }
        return result;
    }

    void AkkEngine::forceSync() {
        if (!impl_) { return; }
        Impl::OperationGuard operation{*impl_, false};
        if (!operation) { return; }
        if (impl_->usesPartitionRaft()) {
            for (const auto& child : impl_->partitionSnapshot()) { if (child) { child->forceSync(); } }
            return;
        }
        if (impl_->walWriter) { impl_->walWriter->forceSync(); }
        if (impl_->versionLog) { impl_->versionLog->forceSync(); }
    }

    void AkkEngine::forceFlush() {
        if (!impl_) { return; }
        Impl::OperationGuard operation{*impl_, false};
        if (operation) {
            std::shared_lock admission{impl_->mutationEpochMu};
            if (impl_->usesPartitionRaft()) {
                for (const auto& child : impl_->partitionSnapshot()) { if (child) { child->forceFlush(); } }
                return;
            }
            impl_->throwIfBackgroundFailed();
            if (impl_->memtable) { impl_->memtable->forceFlush(); }
        }
    }

    void AkkEngine::runBlobGc() {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        if (impl_->usesPartitionRaft()) {
            Impl::OperationGuard operation{*impl_};
            for (const auto& child : impl_->partitionSnapshot()) { if (child) { child->runBlobGc(); } }
            return;
        }
        Impl::OperationGuard operation{*impl_};
        impl_->throwIfBackgroundFailed();
        if (!impl_->blobManager) { return; }
        if (impl_->versionLog) { throw std::runtime_error("AkkEngine: blob GC is disabled while version log is enabled"); }
        impl_->runBlobGcIfSafe();
    }

    void AkkEngine::close() {
        if (!impl_) { return; }
        impl_->rollbackRecoveryThread.request_stop();
        impl_->stripeGcThread.request_stop();
        impl_->queryReaper.request_stop();
        impl_->placementRecoveryThread.request_stop();
        impl_->lifecycleCv.notify_all();
        if (!impl_->beginClose()) { return; }

        std::exception_ptr closeFailure;
        if (impl_->rollbackRecoveryThread.joinable()) { impl_->rollbackRecoveryThread.join(); }
        if (impl_->stripeGcThread.joinable()) { impl_->stripeGcThread.join(); }
        if (impl_->queryReaper.joinable()) { impl_->queryReaper.join(); }
        if (impl_->placementRecoveryThread.joinable()) { impl_->placementRecoveryThread.join(); }
        impl_->partitionQueryExecutor.reset();
        impl_->clearQuerySessions();
        const auto closeStep = [&closeFailure](const auto& operation) {
            try { operation(); }
            catch (...) { if (!closeFailure) { closeFailure = std::current_exception(); } }
        };

        closeStep(
            [&] {
                if (impl_->apiServer) {
                    impl_->apiServer->close();
                    impl_->apiServer.reset();
                }
            }
        );
        closeStep(
            [&] {
                if (impl_->clusterRuntime) {
                    impl_->clusterRuntime->close();
                    impl_->clusterRuntime.reset();
                }
                for (const auto& child : impl_->partitionSnapshot()) { if (child) { child->close(); } }
            }
        );
        closeStep(
            [&] {
                std::lock_guard lock{impl_->writeMu};
                impl_->resetReplicaSnapshotStagingLocked();
            }
        );
        closeStep([&] { impl_->checkpointTransactions(); });
        closeStep([&] { if (impl_->memtable && impl_->opts.runtime.forceFlushOnClose) { impl_->memtable->forceFlush(); } });
        closeStep(
            [&] {
                if (impl_->blobManager) {
                    if (impl_->opts.blob.gcOnClose) { impl_->runBlobGcIfSafe(); }
                    impl_->blobManager->close();
                    impl_->blobManager.reset();
                }
            }
        );
        closeStep(
            [&] {
                if (impl_->sstManager) {
                    impl_->sstManager->shutdown();
                    impl_->sstManager.reset();
                }
            }
        );
        closeStep([&] { if (impl_->walWriter && impl_->opts.runtime.forceSyncOnClose) { impl_->walWriter->forceSync(); } });
        closeStep([&] { if (impl_->versionLog && impl_->opts.runtime.forceSyncOnClose) { impl_->versionLog->forceSync(); } });
        closeStep(
            [&] {
                if (impl_->walWriter) {
                    impl_->walWriter->close();
                    impl_->walWriter.reset();
                }
            }
        );
        closeStep(
            [&] {
                if (impl_->manifest) {
                    impl_->manifest->close();
                    impl_->manifest.reset();
                }
            }
        );
        closeStep(
            [&] {
                if (impl_->versionLog) {
                    impl_->versionLog->close();
                    impl_->versionLog.reset();
                }
            }
        );
        closeStep([&] { impl_->memtable.reset(); });
        impl_->finishClose();
        if (closeFailure) { std::rethrow_exception(closeFailure); }
    }
} // namespace akkaradb::engine

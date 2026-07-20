/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/AkkEngine.cpp
#include "akk/engine/AkkEngine.hpp"

#include "akk/core/record/KeyFingerprint.hpp"
#include "akk/core/record/MemHdr16.hpp"
#include "akk/engine/blob/BlobFraming.hpp"
#include "akk/engine/cluster/ClusterRuntimeProvider.hpp"
#include "akk/engine/generation/StorageGeneration.hpp"
#include "akk/engine/manifest/Manifest.hpp"
#include "akk/engine/server/AkkApiServerProvider.hpp"
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
#include <fstream>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_set>

#ifdef _WIN32
#include <io.h>
#else
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

        // Test-only fault injection used by the recovery smoke test. The process
        // terminates without unwinding, matching the storage guarantees required
        // at each persistence boundary.
        void crashAtTestPoint(const char* point) noexcept {
            const char* requested = std::getenv("AKKARADB_TEST_CRASH_POINT");
            if (requested != nullptr && std::strcmp(requested, point) == 0) { std::quick_exit(86); }
        }

        [[nodiscard]] uint32_t nextPow2(uint32_t value) noexcept {
            uint32_t out = 1;
            while (out < value) { out <<= 1; }
            return out;
        }

        [[nodiscard]] uint32_t shardsForThreads(uint32_t writers, uint32_t cap) noexcept {
            if (writers <= 1) { return 1; }
            const uint32_t target = std::max(writers * 4u, 2u);
            return std::min(nextPow2(target), cap);
        }

        [[nodiscard]] bool supportsParallelWriteAdmission(const AkkEngineOptions& options) noexcept {
            return !options.components.blobEnabled
                   && !options.components.versionLogEnabled
                   && !options.components.clusterEnabled;
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
                case wal::WalSyncMode::OFF:
                    return wal::WalSyncPolicy::NEVER;
                case wal::WalSyncMode::ASYNC:
                    return wal::WalSyncPolicy::ON_SYNC_ACK;
                case wal::WalSyncMode::SYNC:
                    return wal::WalSyncPolicy::ALWAYS;
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
                options.compactionMode = options.compactThreads == 0 ? sst::SSTCompactionMode::DISABLED : sst::SSTCompactionMode::BACKGROUND;
            }
        }

        [[nodiscard]] AkkEngineOptions::WriteVisibilityMode resolveReadVisibility(const AkkEngineOptions& options) noexcept {
            switch (options.runtime.visibility.readVisibility) {
                case AkkEngineOptions::ReadVisibilityMode::AUTO:
                    return options.runtime.writeVisibility;
                case AkkEngineOptions::ReadVisibilityMode::COMMIT_ORDER:
                    return AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER;
                case AkkEngineOptions::ReadVisibilityMode::APPLIED:
                    return AkkEngineOptions::WriteVisibilityMode::APPLIED;
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
                case AkkEngineOptions::WritePolicyPreset::CUSTOM:
                    break;
                case AkkEngineOptions::WritePolicyPreset::SAFE:
                    options.runtime.writeDurability = AkkEngineOptions::WriteDurabilityMode::SYNCED;
                    options.runtime.writeVisibility = AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER;
                    break;
                case AkkEngineOptions::WritePolicyPreset::BALANCED:
                    options.runtime.writeDurability = AkkEngineOptions::WriteDurabilityMode::WRITTEN;
                    options.runtime.writeVisibility = AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER;
                    break;
                case AkkEngineOptions::WritePolicyPreset::FAST:
                    options.runtime.writeDurability = options.components.walEnabled
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
            if (options.runtime.sequence.allocation == AkkEngineOptions::SequenceAllocationMode::THREAD_LOCAL_RANGES &&
                options.runtime.writeVisibility == AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER) {
                throw std::invalid_argument("AkkEngine: THREAD_LOCAL_RANGES sequence allocation requires readVisibility=APPLIED");
            }
            if (options.runtime.sequence.allocation == AkkEngineOptions::SequenceAllocationMode::THREAD_LOCAL_RANGES &&
                (options.components.clusterEnabled || options.components.versionLogEnabled)) {
                throw std::invalid_argument("AkkEngine: THREAD_LOCAL_RANGES sequence allocation requires cluster and version log to be disabled");
            }
            if (options.runtime.sequence.allocation == AkkEngineOptions::SequenceAllocationMode::THREAD_LOCAL_RANGES &&
                options.runtime.sequence.threadLocalRangeSize <= 1) {
                throw std::invalid_argument("AkkEngine: THREAD_LOCAL_RANGES requires sequence.threadLocalRangeSize > 1");
            }
            if (options.runtime.parallelWriteOrder == AkkEngineOptions::ParallelWriteOrderMode::KEY_SEQUENCE &&
                options.runtime.sequence.allocation != AkkEngineOptions::SequenceAllocationMode::GLOBAL_ATOMIC) {
                throw std::invalid_argument("AkkEngine: parallelWriteOrder=KEY_SEQUENCE requires sequence.allocation=GLOBAL_ATOMIC");
            }
            if (options.runtime.parallelWriteOrder == AkkEngineOptions::ParallelWriteOrderMode::KEY_SEQUENCE &&
                options.runtime.writeVisibility != AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER) {
                throw std::invalid_argument("AkkEngine: parallelWriteOrder=KEY_SEQUENCE requires readVisibility=COMMIT_ORDER");
            }
            if (options.components.sstEnabled &&
                options.runtime.backpressure.maxSstL0Files > 0 &&
                options.runtime.backpressure.sstCompactionBacklog == AkkEngineOptions::BackpressureMode::BLOCK &&
                options.sst.compactionMode == sst::SSTCompactionMode::DISABLED) {
                throw std::invalid_argument("AkkEngine: blocking SST backpressure requires background compaction or fail-fast mode");
            }
        }

        void ensureDir(const fs::path& path) { if (!path.empty()) { fs::create_directories(path); } }

        void forceDurable(FILE* file) {
            if (file == nullptr) { return; }
            if (std::fflush(file) != 0) { throw std::runtime_error("AkkEngine: failed to flush Raft log"); }
#ifdef _WIN32
            if (_commit(_fileno(file)) != 0) { throw std::runtime_error("AkkEngine: failed to sync Raft log"); }
#else
            if (::fsync(::fileno(file)) != 0) { throw std::runtime_error("AkkEngine: failed to sync Raft log"); }
#endif
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

    [[nodiscard]] core::ArenaGenerator<AkkEngine::ScanRecordView> scanGenerator(
        core::BufferArena& arena,
        memtable::MemTable::RangeIterator memtableIter,
        sst::SSTManager::Iterator sstIter,
        blob::BlobManager* blobManager,
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

            if (cmp <= 0) {
                const auto record = *memtableCur;
                const bool tombstone = record.isTombstone();
                if (!tombstone) {
                    auto value = resolveScanValue(record.flags(), record.value(), blobManager, arena);
                    if (value) { co_yield AkkEngine::ScanRecordView{record.key(), *value}; }
                }
                memtableCur = memtableIter.hasNext() ? memtableIter.next() : std::optional<core::RecordView>{};
                if (cmp == 0) { sstCur = sstIter.hasNext() ? sstIter.next() : std::optional<sst::SSTRecord>{}; }
            }
            else {
                const auto record = std::move(*sstCur);
                const bool tombstone = record.isTombstone();
                if (!tombstone) {
                    auto value = resolveScanValue(record.flags, record.value, blobManager, arena);
                    if (value) { co_yield AkkEngine::ScanRecordView{record.key, *value}; }
                }
                sstCur = sstIter.hasNext() ? sstIter.next() : std::optional<sst::SSTRecord>{};
            }
        }
    }

    class AkkEngine::Impl {
        public:
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
                    StorageSlot& operator=(std::unique_ptr<T> value) noexcept { *slot_ = std::move(value); return *this; }
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

            class RaftLog;

            class OperationGuard {
                public:
                    explicit OperationGuard(Impl& owner, bool rejectClosed = true) : owner_{&owner} {
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
                  writeBackpressureEnabled{
                      opts.runtime.backpressure.maxMemtableImmutableTables != 0 ||
                      opts.runtime.backpressure.maxSstL0Files != 0
                  },
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
            std::unique_ptr<RaftLog> raftLog;

            mutable std::mutex writeMu;
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
            std::vector<cluster::ClusterHistoryEntry> pendingSnapshotEntries;

            struct AppliedWrite {
                uint64_t seq = 0;
                std::span<const uint8_t> key;
                std::vector<uint8_t> stored;
                uint8_t flags = MemHdr16::FLAG_NORMAL;
                cluster::ReplOpType op = cluster::ReplOpType::PUT;
                uint64_t sourceNodeId = 0;
            };

            struct RaftProposal {
                uint64_t term = 1;
                uint64_t index = 0;
                std::vector<uint8_t> key;
                std::vector<uint8_t> value;
                uint8_t flags = MemHdr16::FLAG_NORMAL;
                cluster::ReplOpType op = cluster::ReplOpType::PUT;
                uint64_t sourceNodeId = 0;
                uint64_t fp64 = 0;
                uint64_t miniKey = 0;
            };

            class RaftLog {
                public:
                    enum class RecordType : uint8_t {
                        PROPOSAL = 1,
                        COMMIT = 2,
                    };

                    [[nodiscard]] static std::unique_ptr<RaftLog> open(const fs::path& path) {
                        if (path.empty()) { throw std::invalid_argument("AkkEngine: Raft log path is required"); }
                        ensureDir(path.parent_path());
                        auto log = std::unique_ptr<RaftLog>{new RaftLog(path)};
                        log->recover();
                        log->file_ = std::fopen(path.string().c_str(), "ab");
                        if (log->file_ == nullptr) { throw std::runtime_error("AkkEngine: failed to open Raft log: " + path.string()); }
                        return log;
                    }

                    ~RaftLog() {
                        if (file_ != nullptr) {
                            try { forceDurable(file_); }
                            catch (...) {}
                            std::fclose(file_);
                        }
                    }

                    RaftLog(const RaftLog&) = delete;
                    RaftLog& operator=(const RaftLog&) = delete;

                    [[nodiscard]] RaftProposal appendProposal(
                        cluster::ReplOpType op,
                        std::span<const uint8_t> key,
                        std::span<const uint8_t> value,
                        uint8_t flags,
                        uint64_t sourceNodeId,
                        uint64_t fp64 = 0,
                        uint64_t miniKey = 0
                    ) {
                        std::lock_guard lock{mutex_};
                        RaftProposal proposal;
                        proposal.term = currentTerm_;
                        proposal.index = ++lastIndex_;
                        proposal.key.assign(key.begin(), key.end());
                        proposal.value.assign(value.begin(), value.end());
                        proposal.flags = flags;
                        proposal.op = op;
                        proposal.sourceNodeId = sourceNodeId;
                        proposal.fp64 = fp64;
                        proposal.miniKey = miniKey;
                        appendRecord(RecordType::PROPOSAL, proposal);
                        return proposal;
                    }

                    void markCommitted(uint64_t term, uint64_t index) {
                        std::lock_guard lock{mutex_};
                        RaftProposal marker;
                        marker.term = term;
                        marker.index = index;
                        appendRecord(RecordType::COMMIT, marker);
                        if (index > commitIndex_) { commitIndex_ = index; }
                    }

                    [[nodiscard]] uint64_t lastIndex() const noexcept { return lastIndex_; }
                    [[nodiscard]] uint64_t commitIndex() const noexcept { return commitIndex_; }

                private:
                    explicit RaftLog(fs::path path) : path_{std::move(path)} {}

                    static constexpr uint32_t MAGIC = 0x31465241; // "ARF1" little-endian.
                    static constexpr uint8_t VERSION = 1;

                    struct Header {
                        uint32_t magic = MAGIC;
                        uint8_t version = VERSION;
                        uint8_t type = 0;
                        uint16_t reserved = 0;
                        uint64_t term = 0;
                        uint64_t index = 0;
                        uint64_t sourceNodeId = 0;
                        uint8_t op = 0;
                        uint8_t flags = 0;
                        uint16_t reserved2 = 0;
                        uint32_t keyLen = 0;
                        uint32_t valueLen = 0;
                    };

                    static bool readExact(FILE* file, void* out, size_t bytes) {
                        return std::fread(out, 1, bytes, file) == bytes;
                    }

                    static void writeExact(FILE* file, const void* data, size_t bytes) {
                        if (bytes == 0) { return; }
                        if (std::fwrite(data, 1, bytes, file) != bytes) {
                            throw std::runtime_error("AkkEngine: failed to write Raft log");
                        }
                    }

                    void recover() {
                        FILE* rf = std::fopen(path_.string().c_str(), "rb");
                        if (rf == nullptr) { return; }
                        while (true) {
                            Header header{};
                            if (!readExact(rf, &header, sizeof(header))) { break; }
                            if (header.magic != MAGIC || header.version != VERSION) { break; }
                            if (header.type != static_cast<uint8_t>(RecordType::PROPOSAL) &&
                                header.type != static_cast<uint8_t>(RecordType::COMMIT)) {
                                break;
                            }
                            if (header.keyLen > 0 && std::fseek(rf, static_cast<long>(header.keyLen), SEEK_CUR) != 0) { break; }
                            if (header.valueLen > 0 && std::fseek(rf, static_cast<long>(header.valueLen), SEEK_CUR) != 0) { break; }
                            if (header.index > lastIndex_) { lastIndex_ = header.index; }
                            if (header.term > currentTerm_) { currentTerm_ = header.term; }
                            if (header.type == static_cast<uint8_t>(RecordType::COMMIT) && header.index > commitIndex_) {
                                commitIndex_ = header.index;
                            }
                        }
                        std::fclose(rf);
                    }

                    void appendRecord(RecordType type, const RaftProposal& proposal) {
                        if (file_ == nullptr) { throw std::runtime_error("AkkEngine: Raft log is closed"); }
                        if (proposal.key.size() > UINT32_MAX || proposal.value.size() > UINT32_MAX) {
                            throw std::invalid_argument("AkkEngine: Raft log entry is too large");
                        }
                        Header header;
                        header.type = static_cast<uint8_t>(type);
                        header.term = proposal.term;
                        header.index = proposal.index;
                        header.sourceNodeId = proposal.sourceNodeId;
                        header.op = static_cast<uint8_t>(proposal.op);
                        header.flags = proposal.flags;
                        header.keyLen = static_cast<uint32_t>(proposal.key.size());
                        header.valueLen = static_cast<uint32_t>(proposal.value.size());
                        writeExact(file_, &header, sizeof(header));
                        writeExact(file_, proposal.key.data(), proposal.key.size());
                        writeExact(file_, proposal.value.data(), proposal.value.size());
                        forceDurable(file_);
                    }

                    fs::path path_;
                    FILE* file_ = nullptr;
                    mutable std::mutex mutex_;
                    uint64_t currentTerm_ = 1;
                    uint64_t lastIndex_ = 0;
                    uint64_t commitIndex_ = 0;
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
                        if (completedSeqRing[slot] == 0 || completedSeqRing[slot] <= current) {
                            completedSeqRing[slot] = seq;
                        }
                        else if (completedSeqRing[slot] != seq) {
                            completedSeqOverflow.insert(seq);
                        }
                    }
                    else {
                        completedSeqOverflow.insert(seq);
                    }

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
                if (opts.runtime.parallelWriteOrder != AkkEngineOptions::ParallelWriteOrderMode::KEY_SEQUENCE ||
                    opts.runtime.writeVisibility != AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER ||
                    !memtable) {
                    return;
                }
                const uint64_t nextSeq = memtable->lastSeq();
                const uint64_t target = nextSeq > 0 ? nextSeq - 1 : 0;
                if (committedSeq.load(std::memory_order_acquire) >= target) { return; }

                std::unique_lock lock{commitMu};
                commitCv.wait(lock, [this, target] {
                    return committedSeq.load(std::memory_order_acquire) >= target ||
                           keySequenceFailure ||
                           closed.load(std::memory_order_acquire);
                });
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

            void ensureCompletedSeqRing() {
                if (completedSeqRing.empty()) { completedSeqRing.resize(commitWindowSize); }
            }

            void throwIfBackgroundFailed() const {
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
                    case AkkEngineOptions::WriteDurabilityMode::ENQUEUED:
                        return wal::WalAppendAck::ENQUEUED;
                    case AkkEngineOptions::WriteDurabilityMode::WRITTEN:
                        return wal::WalAppendAck::WRITTEN;
                    case AkkEngineOptions::WriteDurabilityMode::SYNCED:
                        return wal::WalAppendAck::SYNCED;
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
                    if (lifecycleState.compare_exchange_weak(
                        state,
                        state + 1u,
                        std::memory_order_acq_rel,
                        std::memory_order_acquire
                    )) {
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
                if (walWriter) { walWriter->requestClose(); }
                lock.lock();
                lifecycleCv.wait(lock, [this]() {
                    return (lifecycleState.load(std::memory_order_acquire) & LIFECYCLE_OPERATION_MASK) == 0;
                });
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

                blobManager->write(seq, value);
                std::vector<uint8_t> ref(blob::BLOB_REF_SIZE);
                blob::encodeBlobRef(ref.data(), blob::BlobRef{seq, static_cast<uint64_t>(value.size()), blob::crc32c(value)});
                flags |= MemHdr16::FLAG_BLOB;
                if (clusterRuntime) { clusterRuntime->shipBlob(seq, seq, value); }
                return ref;
            }

            [[nodiscard]] std::optional<std::vector<uint8_t>> resolveValue(uint8_t flags, std::span<const uint8_t> value) const {
                if ((flags & MemHdr16::FLAG_BLOB) == 0) { return std::vector<uint8_t>{value.begin(), value.end()}; }
                if (!blobManager || value.size() < blob::BLOB_REF_SIZE) { return std::nullopt; }
                const auto [blobId, totalSize, contentCrc32c] = blob::decodeBlobRef(value.data());
                auto out = blobManager->read(blobId, contentCrc32c);
                if (out.empty() && totalSize != 0) { return std::nullopt; }
                return out;
            }

            [[nodiscard]] std::optional<std::vector<uint8_t>> getValueInternal(
                std::span<const uint8_t> key,
                bool countStats,
                std::optional<uint64_t> requestedSnapshot = std::nullopt
            ) {
                throwIfBackgroundFailed();
                if (!requestedSnapshot.has_value()) { waitForKeySequenceReadVisibility(); }
                if (countStats) { getsTotal.fetch_add(1, std::memory_order_relaxed); }
                const uint64_t seq = requestedSnapshot.value_or(snapshotSeq());

                RecordView view;
                if (memtable->get(key, seq, &view)) {
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

            [[nodiscard]] bool getIntoInternal(
                std::span<const uint8_t> key,
                std::vector<uint8_t>& out,
                bool countStats,
                std::optional<uint64_t> requestedSnapshot = std::nullopt
            ) {
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
                RecordView view;
                if (memtable->get(key, seq, &view)) {
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

            [[nodiscard]] bool canRunBlobGc() const noexcept { return blobManager != nullptr && versionLog == nullptr; }

            [[nodiscard]] bool canUseParallelWriteAdmission() const noexcept {
                const bool supported = !walWriter && !versionLog && !blobManager && !clusterRuntime;
                const bool explicitSupported = !versionLog && !blobManager && !clusterRuntime;
                switch (opts.runtime.writeAdmission) {
                    case AkkEngineOptions::WriteAdmissionMode::SERIAL:
                        return false;
                    case AkkEngineOptions::WriteAdmissionMode::PARALLEL:
                        return explicitSupported;
                    case AkkEngineOptions::WriteAdmissionMode::AUTO:
                        return opts.runtime.relaxedConcurrentWrites && supported;
                }
                return false;
            }

            [[nodiscard]] bool usesKeySequenceOrder() const noexcept {
                return canUseParallelWriteAdmission() &&
                       opts.runtime.parallelWriteOrder == AkkEngineOptions::ParallelWriteOrderMode::KEY_SEQUENCE;
            }

            [[nodiscard]] std::mutex& keySequenceOrderMutex(uint64_t fp64, size_t keySize) noexcept {
                const uint64_t route = fp64 ^ (static_cast<uint64_t>(keySize) * 0x9E3779B97F4A7C15ULL);
                return (*keySequenceOrderMu)[static_cast<size_t>(route) & (KEY_SEQUENCE_ORDER_STRIPES - 1u)];
            }

            [[nodiscard]] std::vector<std::unique_lock<std::mutex>> lockKeySequenceOrderStripes(
                std::span<const BatchPutEntry> entries
            ) {
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
                for (const auto& level : levels) {
                    if (level.level == 0) { return level.fileCount >= limit; }
                }
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
                    if (closed.load(std::memory_order_acquire)) {
                        throw std::runtime_error("AkkEngine: engine is closed");
                    }
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

            [[nodiscard]] std::unordered_set<uint64_t> collectReferencedBlobIds() const {
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

            void runBlobGcIfSafe() {
                if (!canRunBlobGc()) { return; }
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
                bool knownContiguousCommit = false
            ) {
                throwIfBackgroundFailed();
                memtable->throwIfFlushFailed();
                const uint64_t fp64 = precomputedFp64 != 0 ? precomputedFp64 : core::computeKeyFp64(key);
                const uint64_t mini = precomputedMiniKey != 0 ? precomputedMiniKey : core::buildMiniKey(key);
                if (walWriter) { walWriter->append(key, storedValue, seq, flags, fp64, walAckForWriteDurability()); }
                if (versionLog) {
                    const uint8_t vlogFlags = versionLogFlags == 0xFF ? flags : versionLogFlags;
                    versionLog->append(key, seq, sourceNodeId, nowNs(), vlogFlags, storedValue);
                }

                if ((flags & MemHdr16::FLAG_TOMBSTONE) != 0) { memtable->remove(key, seq, fp64, mini); }
                else { memtable->put(key, storedValue, seq, flags, fp64, mini); }
                markWriteCommitted(seq, knownContiguousCommit);
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
                    const uint64_t resolvedMiniKey = miniKey != 0 ? miniKey : (key.empty() ? 0 : core::buildMiniKey(key.data(), key.size()));
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
                    else {
                        appendAll(seq, key, value, flags, nodeId, 0, 0, 0xFF, true);
                    }
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

            void applyLocalRemoveUnreplicated(std::span<const uint8_t> key, uint64_t fp64 = 0, uint64_t miniKey = 0, uint64_t sourceNodeId = 0) {
                const uint64_t writeSourceNodeId = sourceNodeId == 0 ? nodeId : sourceNodeId;
                applyWriteBackpressure();
                if (canUseParallelWriteAdmission()) {
                    const uint64_t resolvedFp64 = fp64 != 0 ? fp64 : (key.empty() ? 0 : core::computeKeyFp64(key.data(), key.size()));
                    const uint64_t resolvedMiniKey = miniKey != 0 ? miniKey : (key.empty() ? 0 : core::buildMiniKey(key.data(), key.size()));
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

            void applyCommittedRaftProposal(const RaftProposal& proposal) {
                std::lock_guard lock(writeMu);
                const uint8_t flags = proposal.op == cluster::ReplOpType::REMOVE
                                          ? static_cast<uint8_t>(proposal.flags | MemHdr16::FLAG_TOMBSTONE)
                                          : proposal.flags;
                if (proposal.op == cluster::ReplOpType::REMOVE) {
                    removesTotal.fetch_add(1, std::memory_order_relaxed);
                    appendAll(proposal.index, proposal.key, {}, flags, proposal.sourceNodeId, proposal.fp64, proposal.miniKey);
                }
                else {
                    putsTotal.fetch_add(1, std::memory_order_relaxed);
                    appendAll(proposal.index, proposal.key, proposal.value, flags, proposal.sourceNodeId, proposal.fp64, proposal.miniKey);
                }
            }

            void replicateRaftProposal(const RaftProposal& proposal) {
                if (clusterRuntime) {
                    clusterRuntime->shipEntry(
                        proposal.index,
                        proposal.op,
                        proposal.key,
                        proposal.value,
                        proposal.flags,
                        proposal.sourceNodeId
                    );
                }
            }

            void applyReplicaRecord(
                uint64_t seq,
                cluster::ReplOpType op,
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint8_t recordFlags,
                uint64_t sourceNodeId
            ) {
                std::lock_guard lock(writeMu);
                uint8_t flags = recordFlags;
                if (op == cluster::ReplOpType::REMOVE) { flags |= MemHdr16::FLAG_TOMBSTONE; }
                const uint8_t vlogFlags = sourceNodeId == vlog::ROLLBACK_NODE
                                              ? static_cast<uint8_t>(flags | vlog::VLOG_FLAG_ROLLBACK)
                                              : flags;
                appendAll(seq, key, value, flags, sourceNodeId, 0, 0, vlogFlags);
                memtable->advanceSeq(seq);
            }

            void beginReplicaSnapshot(uint64_t seq, uint64_t entryCount) {
                std::lock_guard lock(writeMu);
                if (snapshotInProgress) { throw std::runtime_error("AkkEngine: received nested replication snapshot"); }
                snapshotInProgress = true;
                pendingSnapshotSeq = seq;
                pendingSnapshotEntryCount = entryCount;
                pendingSnapshotEntries.clear();
                pendingSnapshotEntries.reserve(static_cast<size_t>(std::min<uint64_t>(entryCount, SIZE_MAX)));
            }

            void stageReplicaSnapshotEntry(std::span<const uint8_t> key, std::span<const uint8_t> value) {
                std::lock_guard lock(writeMu);
                if (!snapshotInProgress) { throw std::runtime_error("AkkEngine: received replication snapshot entry without begin"); }
                if (pendingSnapshotEntries.size() >= pendingSnapshotEntryCount) {
                    throw std::runtime_error("AkkEngine: replication snapshot has too many entries");
                }
                pendingSnapshotEntries.push_back(cluster::ClusterHistoryEntry{
                    .key = {key.begin(), key.end()},
                    .value = {value.begin(), value.end()},
                });
            }

            void finishReplicaSnapshot(uint64_t seq) {
                std::lock_guard lock(writeMu);
                if (!snapshotInProgress || seq != pendingSnapshotSeq || pendingSnapshotEntries.size() != pendingSnapshotEntryCount) {
                    throw std::runtime_error("AkkEngine: invalid replication snapshot completion");
                }

                std::unordered_set<std::string> incomingKeys;
                incomingKeys.reserve(pendingSnapshotEntries.size());
                for (const auto& entry : pendingSnapshotEntries) {
                    const auto [_, inserted] = incomingKeys.emplace(reinterpret_cast<const char*>(entry.key.data()), entry.key.size());
                    if (!inserted) { throw std::runtime_error("AkkEngine: replication snapshot has duplicate keys"); }
                }

                core::BufferArena arena;
                memtable::MemTable::KeyRange range;
                const uint64_t visibleSeq = snapshotSeq();
                auto mt = memtable->iterator(range, visibleSeq);
                sst::SSTManager::Iterator sst;
                if (sstManager) { sst = sstManager->scanIter({}, {}, visibleSeq); }
                for (const auto& record : scanGenerator(arena, std::move(mt), std::move(sst), blobManager.get())) {
                    const std::string key{reinterpret_cast<const char*>(record.key.data()), record.key.size()};
                    if (incomingKeys.find(key) == incomingKeys.end()) {
                        appendAll(seq, record.key, {}, MemHdr16::FLAG_TOMBSTONE, 0);
                    }
                }
                for (const auto& entry : pendingSnapshotEntries) {
                    appendAll(seq, entry.key, entry.value, MemHdr16::FLAG_NORMAL, 0);
                }
                memtable->advanceSeq(seq);
                pendingSnapshotEntries.clear();
                snapshotInProgress = false;
                pendingSnapshotSeq = 0;
                pendingSnapshotEntryCount = 0;
            }

            class WriteCoordinator {
                public:
                    explicit WriteCoordinator(Impl& engine) : engine_{engine} {}
                    virtual ~WriteCoordinator() = default;
                    WriteCoordinator(const WriteCoordinator&) = delete;
                    WriteCoordinator& operator=(const WriteCoordinator&) = delete;

                    virtual void put(std::span<const uint8_t> key, std::span<const uint8_t> value) = 0;
                    virtual void putHinted(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t fp64, uint64_t miniKey) = 0;
                    virtual void putBatch(std::span<const BatchPutEntry> entries) = 0;
                    virtual void remove(std::span<const uint8_t> key) = 0;
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

                    void removeHinted(std::span<const uint8_t> key, uint64_t fp64, uint64_t miniKey) override {
                        engine_.applyLocalRemoveUnreplicated(key, fp64, miniKey);
                    }
            };

            class LocalReplicationWriteCoordinator final : public WriteCoordinator {
                public:
                    using WriteCoordinator::WriteCoordinator;

                    void put(std::span<const uint8_t> key, std::span<const uint8_t> value) override {
                        const auto write = engine_.applyLocalPut(key, value);
                        engine_.replicateCommitted(write);
                    }

                    void putHinted(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t fp64, uint64_t miniKey) override {
                        const auto write = engine_.applyLocalPut(key, value, fp64, miniKey);
                        engine_.replicateCommitted(write);
                    }

                    void putBatch(std::span<const BatchPutEntry> entries) override {
                        const auto writes = engine_.applyLocalPutBatch(entries);
                        for (const auto& write : writes) { engine_.replicateCommitted(write); }
                    }

                    void remove(std::span<const uint8_t> key) override {
                        const auto write = engine_.applyLocalRemove(key);
                        engine_.replicateCommitted(write);
                    }

                    void removeHinted(std::span<const uint8_t> key, uint64_t fp64, uint64_t miniKey) override {
                        const auto write = engine_.applyLocalRemove(key, fp64, miniKey);
                        engine_.replicateCommitted(write);
                    }
            };

            class RaftQuorumWriteCoordinator final : public WriteCoordinator {
                public:
                    using WriteCoordinator::WriteCoordinator;

                    void put(std::span<const uint8_t> key, std::span<const uint8_t> value) override {
                        commitLocalProposal(propose(cluster::ReplOpType::PUT, key, value, MemHdr16::FLAG_NORMAL));
                    }

                    void putHinted(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t fp64, uint64_t miniKey) override {
                        commitLocalProposal(propose(cluster::ReplOpType::PUT, key, value, MemHdr16::FLAG_NORMAL, fp64, miniKey));
                    }

                    void putBatch(std::span<const BatchPutEntry> entries) override {
                        for (const auto& [key, value] : entries) {
                            commitLocalProposal(propose(cluster::ReplOpType::PUT, key, value, MemHdr16::FLAG_NORMAL));
                        }
                    }

                    void remove(std::span<const uint8_t> key) override {
                        commitLocalProposal(propose(cluster::ReplOpType::REMOVE, key, {}, MemHdr16::FLAG_TOMBSTONE));
                    }

                    void removeHinted(std::span<const uint8_t> key, uint64_t fp64, uint64_t miniKey) override {
                        commitLocalProposal(propose(cluster::ReplOpType::REMOVE, key, {}, MemHdr16::FLAG_TOMBSTONE, fp64, miniKey));
                    }

                private:
                    [[nodiscard]] RaftProposal propose(
                        cluster::ReplOpType op,
                        std::span<const uint8_t> key,
                        std::span<const uint8_t> value,
                        uint8_t flags,
                        uint64_t fp64 = 0,
                        uint64_t miniKey = 0
                    ) {
                        if (!engine_.raftLog) { throw std::runtime_error("AkkEngine: RAFT_QUORUM requires a Raft log"); }
                        return engine_.raftLog->appendProposal(op, key, value, flags, engine_.nodeId, fp64, miniKey);
                    }

                    void commitLocalProposal(const RaftProposal& proposal) {
                        engine_.replicateRaftProposal(proposal);
                        engine_.raftLog->markCommitted(proposal.term, proposal.index);
                        engine_.applyCommittedRaftProposal(proposal);
                    }
            };

            [[nodiscard]] std::unique_ptr<WriteCoordinator> createWriteCoordinator(cluster::ConsistencyMode mode) {
                if (mode == cluster::ConsistencyMode::RAFT_QUORUM) {
                    return std::make_unique<RaftQuorumWriteCoordinator>(*this);
                }
                if (!clusterRuntime) { return std::make_unique<LocalUnreplicatedWriteCoordinator>(*this); }
                return std::make_unique<LocalReplicationWriteCoordinator>(*this);
            }

            std::unique_ptr<WriteCoordinator> writeCoordinator;
    };

    AkkEngine::AkkEngine() = default;
    AkkEngine::~AkkEngine() {
        try { close(); }
        catch (...) {}
    }

    std::unique_ptr<AkkEngine> AkkEngine::open(AkkEngineOptions options) {
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

        if (options.wal.walDir.empty()) { options.wal.walDir = options.paths.walDir; }
        if (options.blob.blobDir.empty()) { options.blob.blobDir = options.paths.blobDir; }
        if (options.sst.sstDir.empty()) { options.sst.sstDir = options.paths.sstDir; }
        if (options.vlog.logPath.empty()) { options.vlog.logPath = options.paths.versionLogPath; }

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
        if (options.components.walEnabled) { ensureDir(options.wal.walDir); }
        if (options.components.blobEnabled) { ensureDir(options.blob.blobDir); }
        if (options.components.sstEnabled) { ensureDir(options.sst.sstDir); }
        if (options.components.manifestEnabled) { ensureDir(options.paths.manifestPath.parent_path()); }
        if (options.components.versionLogEnabled && options.vlog.logPath.empty()) {
            throw std::invalid_argument("AkkEngine: version log path is required when components.versionLogEnabled is true");
        }
        if (options.components.versionLogEnabled) { ensureDir(options.vlog.logPath.parent_path()); }
        if (options.components.apiEnabled && options.api.bindHost.empty()) {
            throw std::invalid_argument("AkkEngine: api.bindHost is required when components.apiEnabled is true");
        }
        if (options.runtime.writeAdmission == AkkEngineOptions::WriteAdmissionMode::PARALLEL && !supportsParallelWriteAdmission(options)) {
            throw std::invalid_argument(
                "AkkEngine: runtime.writeAdmission=PARALLEL currently requires blob, version log, and cluster to be disabled"
            );
        }
        auto engine = std::unique_ptr<AkkEngine>{new AkkEngine()};
        engine->impl_ = std::make_unique<Impl>(std::move(options));
        Impl& impl = *engine->impl_;
        impl.nodeId = loadOrCreateNodeId(impl.opts.paths.nodeIdPath);
        cluster::ConsistencyMode writeCoordinatorMode = cluster::ConsistencyMode::PRIMARY_ACK;
        if (impl.opts.components.manifestEnabled && !impl.opts.paths.manifestPath.empty()) {
            impl.manifest = manifest::Manifest::create(impl.opts.paths.manifestPath, impl.opts.manifest.fastMode);
            impl.manifest->start();
        }

        if (impl.opts.components.sstEnabled && !impl.opts.sst.sstDir.empty()) {
            impl.sstManager = sst::SSTManager::create(impl.opts.sst, impl.manifest.get());
            if (impl.opts.runtime.recoverSst) { impl.sstManager->recover(); }
        }

        if (impl.sstManager) {
            impl.opts.memtable.onFlush = [&impl](std::span<const core::RecordView> records) {
                if (records.empty()) { return; }
                const uint64_t checkpointSeq = impl.sstManager->flush(records);
                crashAtTestPoint("engine.flush.after_sst_flush");
                if (impl.walWriter && impl.opts.runtime.pruneWalOnFlush) { impl.walWriter->pruneUntil(checkpointSeq); }
                crashAtTestPoint("engine.flush.after_wal_prune");
                if (impl.manifest) { impl.manifest->checkpoint(std::optional<std::string>{"flush"}, std::nullopt, checkpointSeq); }
                crashAtTestPoint("engine.flush.after_manifest_checkpoint");
                if (impl.blobManager && impl.opts.blob.gcOnFlush) { impl.runBlobGcIfSafe(); }
            };
        }
        else {
            impl.opts.memtable.onFlush = {};
        }
        impl.memtable = memtable::MemTable::create(impl.opts.memtable);
        if (impl.opts.components.walEnabled && impl.opts.runtime.recoverWal) {
            const auto recovery = wal::WalRecovery::recoverInto(wal::WalRecoveryOptions{.walDir = impl.opts.wal.walDir}, *impl.memtable);
            (void)recovery;
        }
        uint64_t recoveredSeq = 0;
        if (impl.memtable) {
            const uint64_t nextMemtableSeq = impl.memtable->lastSeq();
            recoveredSeq = nextMemtableSeq > 0 ? nextMemtableSeq - 1 : 0;
        }
        if (impl.sstManager) { recoveredSeq = std::max(recoveredSeq, impl.sstManager->maxSequence()); }
        if (impl.memtable && recoveredSeq > 0) { impl.memtable->advanceSeq(recoveredSeq); }
        impl.resetCommittedSeq(recoveredSeq);

        if (impl.opts.components.walEnabled) { impl.walWriter = wal::WalWriter::create(impl.opts.wal); }

        if (impl.opts.components.blobEnabled) {
            impl.blobManager = blob::BlobManager::create(impl.opts.blob);
            impl.blobManager->start();
        }

        if (impl.opts.components.versionLogEnabled) { impl.versionLog = vlog::VersionLog::create(impl.opts.vlog); }

        if (impl.opts.components.clusterEnabled) {
            cluster::ClusterConfig cfg = impl.opts.cluster.config.has_value()
                                             ? *impl.opts.cluster.config
                                             : cluster::ClusterConfig::load(impl.opts.paths.clusterConfigPath);
            writeCoordinatorMode = cfg.consistency().mode;
            cluster::ClusterEngineCallbacks callbacks;
            callbacks.getCurrentSeq = [&impl] { return impl.snapshotSeq(); };
            callbacks.getLastSeq = [&impl] { return impl.snapshotSeq(); };
            callbacks.getEntries = [&impl](uint64_t afterSeq, uint64_t throughSeq) -> std::optional<std::vector<cluster::ClusterHistoryEntry>> {
                if (!impl.walWriter || throughSeq < afterSeq) { return std::nullopt; }
                std::vector<cluster::ClusterHistoryEntry> entries;
                bool hasExternalBlob = false;
                const auto recovery = wal::WalRecovery::recover(
                    wal::WalRecoveryOptions{.walDir = impl.opts.wal.walDir},
                    [&](const wal::WalRecoveredEntry& item) {
                        if (item.seq <= afterSeq || item.seq > throughSeq) { return; }
                        const uint8_t flags = static_cast<uint8_t>(item.flags);
                        if ((flags & MemHdr16::FLAG_BLOB) != 0) { hasExternalBlob = true; return; }
                        entries.push_back(cluster::ClusterHistoryEntry{
                            .seq = item.seq,
                            .sourceNodeId = impl.nodeId,
                            .op = static_cast<uint8_t>((flags & MemHdr16::FLAG_TOMBSTONE) != 0 ? cluster::ReplOpType::REMOVE : cluster::ReplOpType::PUT),
                            .recordFlags = flags,
                            .key = item.key,
                            .value = item.value,
                        });
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
                std::lock_guard lock{impl.writeMu};
                if (!impl.memtable) { return std::nullopt; }
                cluster::ClusterSnapshot snapshot;
                snapshot.seq = impl.snapshotSeq();
                core::BufferArena arena;
                memtable::MemTable::KeyRange range;
                auto mt = impl.memtable->iterator(range, snapshot.seq);
                sst::SSTManager::Iterator sst;
                if (impl.sstManager) { sst = impl.sstManager->scanIter({}, {}, snapshot.seq); }
                for (const auto& record : scanGenerator(arena, std::move(mt), std::move(sst), impl.blobManager.get())) {
                    snapshot.entries.push_back(cluster::ClusterHistoryEntry{
                        .key = {record.key.begin(), record.key.end()},
                        .value = {record.value.begin(), record.value.end()},
                    });
                }
                return snapshot;
            };
            callbacks.beginSnapshot = [&impl](uint64_t seq, uint64_t entryCount) { impl.beginReplicaSnapshot(seq, entryCount); };
            callbacks.applySnapshotEntry = [&impl](std::span<const uint8_t> key, std::span<const uint8_t> value) {
                impl.stageReplicaSnapshotEntry(key, value);
            };
            callbacks.finishSnapshot = [&impl](uint64_t seq) { impl.finishReplicaSnapshot(seq); };
            callbacks.forceDurable = [&impl] {
                if (impl.walWriter) { impl.walWriter->forceSync(); }
                if (impl.versionLog) { impl.versionLog->forceSync(); }
            };
            callbacks.apply = [&impl](
                uint64_t seq,
                cluster::ReplOpType op,
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint8_t recordFlags,
                uint64_t sourceNodeId
            ) {
                    impl.applyReplicaRecord(seq, op, key, value, recordFlags, sourceNodeId);
                };
            callbacks.applyBlob = [&impl](uint64_t /*seq*/, uint64_t blobId, std::span<const uint8_t> content) {
                if (impl.blobManager) { impl.blobManager->write(blobId, content); }
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
            impl.clusterRuntime->start();
        }

        if (writeCoordinatorMode == cluster::ConsistencyMode::RAFT_QUORUM) {
            const fs::path raftLogPath = impl.opts.paths.dataDir.empty()
                                             ? fs::path{"raft.log"}
                                             : impl.opts.paths.dataDir / "raft.log";
            impl.raftLog = Impl::RaftLog::open(raftLogPath);
        }

        impl.writeCoordinator = impl.createWriteCoordinator(writeCoordinatorMode);

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

    void AkkEngine::put(std::span<const uint8_t> key, std::span<const uint8_t> value) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        if (impl_->clusterRuntime) { impl_->throwIfBackgroundFailed(); }
        impl_->writeCoordinator->put(key, value);
    }

    void AkkEngine::putHinted(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t fp64, uint64_t miniKey) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        if (impl_->clusterRuntime) { impl_->throwIfBackgroundFailed(); }
        impl_->writeCoordinator->putHinted(key, value, fp64, miniKey);
    }

    void AkkEngine::putBatch(std::span<const BatchPutEntry> entries) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        if (entries.empty()) {
            impl_->throwIfBackgroundFailed();
            return;
        }
        if (impl_->clusterRuntime) { impl_->throwIfBackgroundFailed(); }
        impl_->writeCoordinator->putBatch(entries);
    }

    void AkkEngine::remove(std::span<const uint8_t> key) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        if (impl_->clusterRuntime) { impl_->throwIfBackgroundFailed(); }
        impl_->writeCoordinator->remove(key);
    }

    void AkkEngine::removeHinted(std::span<const uint8_t> key, uint64_t fp64, uint64_t miniKey) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        if (impl_->clusterRuntime) { impl_->throwIfBackgroundFailed(); }
        impl_->writeCoordinator->removeHinted(key, fp64, miniKey);
    }

    std::optional<std::vector<uint8_t>> AkkEngine::get(std::span<const uint8_t> key) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        impl_->throwIfBackgroundFailed();
        return impl_->getValueInternal(key, true);
    }

    std::vector<AkkEngine::BatchGetResult> AkkEngine::getBatch(std::span<const std::span<const uint8_t>> keys) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        impl_->throwIfBackgroundFailed();
        impl_->waitForKeySequenceReadVisibility();
        const uint64_t snapshot = impl_->snapshotSeq();

        std::vector<BatchGetResult> out;
        out.reserve(keys.size());

        for (const auto& key : keys) {
            BatchGetResult result;
            result.found = impl_->getIntoInternal(key, result.value, true, snapshot);
            out.push_back(std::move(result));
        }

        return out;
    }

    bool AkkEngine::exists(std::span<const uint8_t> key) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        impl_->throwIfBackgroundFailed();
        impl_->waitForKeySequenceReadVisibility();
        impl_->existsTotal.fetch_add(1, std::memory_order_relaxed);
        const uint64_t seq = impl_->snapshotSeq();
        if (const auto mt = impl_->memtable->contains(key, seq); mt.has_value()) { return *mt; }
        if (impl_->sstManager) { if (const auto sst = impl_->sstManager->contains(key, seq); sst.has_value()) { return *sst; } }
        return false;
    }

    bool AkkEngine::getInto(std::span<const uint8_t> key, std::vector<uint8_t>& out) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        impl_->throwIfBackgroundFailed();
        return impl_->getIntoInternal(key, out, true);
    }

    bool AkkEngine::getIntoArena(std::span<const uint8_t> key, core::BufferArena& arena, std::span<const uint8_t>& out) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        impl_->throwIfBackgroundFailed();
        return impl_->getIntoArenaInternal(key, arena, out, true);
    }

    size_t AkkEngine::count(std::span<const uint8_t> startKey, std::span<const uint8_t> endKey) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
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
        impl_->waitForKeySequenceReadVisibility();
        impl_->scansTotal.fetch_add(1, std::memory_order_relaxed);

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
            [&arena, mt = std::move(mt), st = std::move(st), blobManager = impl_->blobManager.get(), operation = std::move(operation)]() mutable {
                return scanGenerator(arena, std::move(mt), std::move(st), blobManager, std::move(operation));
            }
        );
    }

    std::optional<std::vector<uint8_t>> AkkEngine::getAt(std::span<const uint8_t> key, uint64_t atSeq) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        impl_->throwIfBackgroundFailed();
        if (!impl_->versionLog) { return std::nullopt; }
        auto entry = impl_->versionLog->getAt(key, atSeq);
        if (!entry || (entry->flags & core::MemHdr16::FLAG_TOMBSTONE) != 0) { return std::nullopt; }
        return impl_->resolveValue(entry->flags, entry->value);
    }

    std::vector<VersionEntry> AkkEngine::history(std::span<const uint8_t> key) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        impl_->throwIfBackgroundFailed();
        return impl_->versionLog ? impl_->versionLog->history(key) : std::vector<VersionEntry>{};
    }

    void AkkEngine::rollbackTo(uint64_t targetSeq) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        impl_->throwIfBackgroundFailed();
        if (!impl_->versionLog) { throw std::runtime_error("AkkEngine: version log is disabled"); }
        for (const auto& [key, prev] : impl_->versionLog->collectRollbackTargets(targetSeq)) {
            uint64_t seq = 0;
            uint8_t recordFlags = MemHdr16::FLAG_TOMBSTONE;
            std::vector<uint8_t> stored;
            cluster::ReplOpType shipOp = cluster::ReplOpType::REMOVE;

            {
                std::lock_guard lock(impl_->writeMu);
                seq = impl_->memtable->reserveSeq(1);

                if (prev && (prev->flags & core::MemHdr16::FLAG_TOMBSTONE) == 0) {
                    auto value = impl_->resolveValue(prev->flags, prev->value);
                    if (value) {
                        recordFlags = MemHdr16::FLAG_NORMAL;
                        stored = impl_->maybeExternalize(seq, *value, recordFlags);
                        shipOp = cluster::ReplOpType::PUT;
                        impl_->putsTotal.fetch_add(1, std::memory_order_relaxed);
                        if ((recordFlags & MemHdr16::FLAG_BLOB) != 0) { impl_->blobPutsTotal.fetch_add(1, std::memory_order_relaxed); }
                    }
                    else { impl_->removesTotal.fetch_add(1, std::memory_order_relaxed); }
                }
                else { impl_->removesTotal.fetch_add(1, std::memory_order_relaxed); }

                const uint8_t vlogFlags = static_cast<uint8_t>(recordFlags | vlog::VLOG_FLAG_ROLLBACK);
                impl_->appendAll(seq, key, stored, recordFlags, vlog::ROLLBACK_NODE, 0, 0, vlogFlags);
            }

            if (impl_->clusterRuntime) { impl_->clusterRuntime->shipEntry(seq, shipOp, key, stored, recordFlags, vlog::ROLLBACK_NODE); }
        }
    }

    void AkkEngine::rollbackKey(std::span<const uint8_t> key, uint64_t targetSeq) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        impl_->throwIfBackgroundFailed();
        if (!impl_->versionLog) { throw std::runtime_error("AkkEngine: version log is disabled"); }
        const auto prev = impl_->versionLog->getAt(key, targetSeq);
        uint64_t seq = 0;
        uint8_t recordFlags = MemHdr16::FLAG_TOMBSTONE;
        std::vector<uint8_t> stored;
        cluster::ReplOpType shipOp = cluster::ReplOpType::REMOVE;

        {
            std::lock_guard lock(impl_->writeMu);
            seq = impl_->memtable->reserveSeq(1);

            if (prev && (prev->flags & core::MemHdr16::FLAG_TOMBSTONE) == 0) {
                auto value = impl_->resolveValue(prev->flags, prev->value);
                if (value) {
                    recordFlags = MemHdr16::FLAG_NORMAL;
                    stored = impl_->maybeExternalize(seq, *value, recordFlags);
                    shipOp = cluster::ReplOpType::PUT;
                    impl_->putsTotal.fetch_add(1, std::memory_order_relaxed);
                    if ((recordFlags & MemHdr16::FLAG_BLOB) != 0) { impl_->blobPutsTotal.fetch_add(1, std::memory_order_relaxed); }
                }
                else { impl_->removesTotal.fetch_add(1, std::memory_order_relaxed); }
            }
            else { impl_->removesTotal.fetch_add(1, std::memory_order_relaxed); }

            const uint8_t vlogFlags = static_cast<uint8_t>(recordFlags | vlog::VLOG_FLAG_ROLLBACK);
            impl_->appendAll(seq, key, stored, recordFlags, vlog::ROLLBACK_NODE, 0, 0, vlogFlags);
        }

        if (impl_->clusterRuntime) { impl_->clusterRuntime->shipEntry(seq, shipOp, key, stored, recordFlags, vlog::ROLLBACK_NODE); }
    }

    EngineStats AkkEngine::stats() const noexcept {
        EngineStats out;
        if (!impl_) { return out; }
        Impl::OperationGuard operation{*impl_, false};
        if (!operation) { return out; }

        out.currentSeq = impl_->snapshotSeq();
        out.nodeId = impl_->nodeId;

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
            out.vlog.flushThreadRunning = snap.flushThreadRunning;
        }
        return out;
    }

    void AkkEngine::forceSync() {
        if (!impl_) { return; }
        Impl::OperationGuard operation{*impl_, false};
        if (!operation) { return; }
        if (impl_->walWriter) { impl_->walWriter->forceSync(); }
        if (impl_->versionLog) { impl_->versionLog->forceSync(); }
    }

    void AkkEngine::forceFlush() {
        if (!impl_) { return; }
        Impl::OperationGuard operation{*impl_, false};
        if (operation) {
            impl_->throwIfBackgroundFailed();
            if (impl_->memtable) { impl_->memtable->forceFlush(); }
        }
    }

    void AkkEngine::runBlobGc() {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        impl_->throwIfBackgroundFailed();
        if (!impl_->blobManager) { return; }
        if (impl_->versionLog) { throw std::runtime_error("AkkEngine: blob GC is disabled while version log is enabled"); }
        impl_->runBlobGcIfSafe();
    }

    void AkkEngine::close() {
        if (!impl_) { return; }
        if (!impl_->beginClose()) { return; }

        std::exception_ptr closeFailure;
        const auto closeStep = [&closeFailure](const auto& operation) {
            try { operation(); }
            catch (...) {
                if (!closeFailure) { closeFailure = std::current_exception(); }
            }
        };

        closeStep([&] {
            if (impl_->apiServer) {
                impl_->apiServer->close();
                impl_->apiServer.reset();
            }
        });
        closeStep([&] {
            if (impl_->clusterRuntime) {
                impl_->clusterRuntime->close();
                impl_->clusterRuntime.reset();
            }
        });
        closeStep([&] {
            if (impl_->memtable && impl_->opts.runtime.forceFlushOnClose) { impl_->memtable->forceFlush(); }
        });
        closeStep([&] {
            if (impl_->sstManager) {
                impl_->sstManager->shutdown();
                impl_->sstManager.reset();
            }
        });
        closeStep([&] {
            if (impl_->walWriter && impl_->opts.runtime.forceSyncOnClose) { impl_->walWriter->forceSync(); }
        });
        closeStep([&] {
            if (impl_->walWriter) {
                impl_->walWriter->close();
                impl_->walWriter.reset();
            }
        });
        closeStep([&] {
            if (impl_->manifest) {
                impl_->manifest->close();
                impl_->manifest.reset();
            }
        });
        closeStep([&] {
            if (impl_->blobManager) {
                if (impl_->opts.blob.gcOnClose) { impl_->runBlobGcIfSafe(); }
                impl_->blobManager->close();
                impl_->blobManager.reset();
            }
        });
        closeStep([&] {
            if (impl_->versionLog) {
                impl_->versionLog->close();
                impl_->versionLog.reset();
            }
        });
        closeStep([&] { impl_->memtable.reset(); });
        impl_->finishClose();
        if (closeFailure) { std::rethrow_exception(closeFailure); }
    }
} // namespace akkaradb::engine

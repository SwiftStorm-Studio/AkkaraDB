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
#include <atomic>
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
        blob::BlobManager* blobManager
    ) {
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

            explicit Impl(AkkEngineOptions optionsIn)
                : opts{std::move(optionsIn)},
                  storage{std::make_shared<StorageState>()},
                  manifest{&storage->manifest},
                  sstManager{&storage->sstManager},
                  memtable{&storage->memtable},
                  walWriter{&storage->walWriter},
                  blobManager{&storage->blobManager},
                  versionLog{&storage->versionLog} {}

            AkkEngineOptions opts;
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
            mutable std::shared_mutex storageMu;
            std::atomic<uint64_t> putsTotal{0};
            std::atomic<uint64_t> removesTotal{0};
            std::atomic<uint64_t> getsTotal{0};
            std::atomic<uint64_t> getsMemtableHit{0};
            std::atomic<uint64_t> getsSstHit{0};
            std::atomic<uint64_t> getsMiss{0};
            std::atomic<uint64_t> existsTotal{0};
            std::atomic<uint64_t> scansTotal{0};
            std::atomic<uint64_t> blobPutsTotal{0};

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

            [[nodiscard]] uint64_t snapshotSeq() const noexcept { return memtable ? memtable->lastSeq() : 0; }

            [[nodiscard]] std::shared_ptr<StorageState> pinStorage() const {
                std::shared_lock lock{storageMu};
                return storage;
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

            [[nodiscard]] bool canRunBlobGc() const noexcept { return blobManager != nullptr && versionLog == nullptr; }

            [[nodiscard]] std::unordered_set<uint64_t> collectReferencedBlobIds() const {
                std::unordered_set<uint64_t> live;
                if (!blobManager || !memtable) { return live; }

                const uint64_t snapshotSeq = this->snapshotSeq();
                memtable::MemTable::KeyRange range;
                auto mt = memtable->iterator(range, snapshotSeq);
                sst::SSTManager::Iterator sstIt;
                if (sstManager) { sstIt = sstManager->scanIter({}, {}); }

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
                uint8_t versionLogFlags = 0xFF
            ) {
                const uint64_t fp64 = precomputedFp64 != 0 ? precomputedFp64 : core::computeKeyFp64(key);
                const uint64_t mini = precomputedMiniKey != 0 ? precomputedMiniKey : core::buildMiniKey(key);
                if (walWriter) { walWriter->append(key, storedValue, seq, flags, fp64); }
                if (versionLog) {
                    const uint8_t vlogFlags = versionLogFlags == 0xFF ? flags : versionLogFlags;
                    versionLog->append(key, seq, sourceNodeId, nowNs(), vlogFlags, storedValue);
                }

                if ((flags & MemHdr16::FLAG_TOMBSTONE) != 0) { memtable->remove(key, seq, fp64, mini); }
                else { memtable->put(key, storedValue, seq, flags, fp64, mini); }
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
                {
                    std::lock_guard lock(writeMu);
                    write.seq = memtable->reserveSeq(1);
                    write.stored = maybeExternalize(write.seq, value, write.flags);
                    putsTotal.fetch_add(1, std::memory_order_relaxed);
                    if ((write.flags & MemHdr16::FLAG_BLOB) != 0) { blobPutsTotal.fetch_add(1, std::memory_order_relaxed); }
                    appendAll(write.seq, key, write.stored, write.flags, write.sourceNodeId, fp64, miniKey);
                }
                return write;
            }

            [[nodiscard]] std::vector<AppliedWrite> applyLocalPutBatch(std::span<const BatchPutEntry> entries) {
                std::vector<AppliedWrite> writes;
                writes.reserve(entries.size());

                std::lock_guard lock(writeMu);
                const uint64_t baseSeq = memtable->reserveSeq(entries.size());
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
                {
                    std::lock_guard lock(writeMu);
                    write.seq = memtable->reserveSeq(1);
                    removesTotal.fetch_add(1, std::memory_order_relaxed);
                    appendAll(write.seq, key, {}, write.flags, write.sourceNodeId, fp64, miniKey);
                }
                return write;
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
                auto mt = memtable->iterator(range, snapshotSeq());
                sst::SSTManager::Iterator sst;
                if (sstManager) { sst = sstManager->scanIter({}, {}); }
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
                return std::make_unique<LocalReplicationWriteCoordinator>(*this);
            }

            std::unique_ptr<WriteCoordinator> writeCoordinator;
    };

    AkkEngine::AkkEngine() = default;
    AkkEngine::~AkkEngine() { close(); }

    std::unique_ptr<AkkEngine> AkkEngine::open(AkkEngineOptions options) {
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

        impl.opts.memtable.onFlush = [&impl](std::span<const core::RecordView> records) {
            if (!impl.sstManager || records.empty()) { return; }
            const uint64_t checkpointSeq = impl.sstManager->flush(records);
            if (impl.walWriter && impl.opts.runtime.pruneWalOnFlush) { impl.walWriter->pruneUntil(checkpointSeq); }
            if (impl.manifest) { impl.manifest->checkpoint(std::optional<std::string>{"flush"}, std::nullopt, checkpointSeq); }
            if (impl.blobManager && impl.opts.blob.gcOnFlush) { impl.runBlobGcIfSafe(); }
        };
        impl.memtable = memtable::MemTable::create(impl.opts.memtable);
        if (impl.opts.components.walEnabled && impl.opts.runtime.recoverWal) {
            const auto recovery = wal::WalRecovery::recoverInto(wal::WalRecoveryOptions{.walDir = impl.opts.wal.walDir}, *impl.memtable);
            (void)recovery;
        }

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
                if (impl.sstManager) { sst = impl.sstManager->scanIter({}, {}); }
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
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        impl_->writeCoordinator->put(key, value);
    }

    void AkkEngine::putHinted(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t fp64, uint64_t miniKey) {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        impl_->writeCoordinator->putHinted(key, value, fp64, miniKey);
    }

    void AkkEngine::putBatch(std::span<const BatchPutEntry> entries) {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        if (entries.empty()) { return; }
        impl_->writeCoordinator->putBatch(entries);
    }

    void AkkEngine::remove(std::span<const uint8_t> key) {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        impl_->writeCoordinator->remove(key);
    }

    void AkkEngine::removeHinted(std::span<const uint8_t> key, uint64_t fp64, uint64_t miniKey) {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        impl_->writeCoordinator->removeHinted(key, fp64, miniKey);
    }

    std::optional<std::vector<uint8_t>> AkkEngine::get(std::span<const uint8_t> key) const {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        impl_->getsTotal.fetch_add(1, std::memory_order_relaxed);
        const uint64_t seq = impl_->snapshotSeq();

        RecordView view;
        if (impl_->memtable->get(key, seq, &view)) {
            if (view.isTombstone()) {
                impl_->getsMiss.fetch_add(1, std::memory_order_relaxed);
                return std::nullopt;
            }
            auto value = impl_->resolveValue(view.flags(), view.value());
            if (value) { impl_->getsMemtableHit.fetch_add(1, std::memory_order_relaxed); }
            else { impl_->getsMiss.fetch_add(1, std::memory_order_relaxed); }
            return value;
        }

        if (impl_->sstManager) {
            auto record = impl_->sstManager->get(key);
            if (record) {
                if (record->isTombstone()) {
                    impl_->getsMiss.fetch_add(1, std::memory_order_relaxed);
                    return std::nullopt;
                }
                if (impl_->opts.runtime.sstPromoteReads && impl_->memtable) {
                    impl_->memtable->put(record->key, record->value, record->seq, record->flags, record->keyFp64, record->miniKey);
                }
                auto value = impl_->resolveValue(record->flags, record->value);
                if (value) { impl_->getsSstHit.fetch_add(1, std::memory_order_relaxed); }
                else { impl_->getsMiss.fetch_add(1, std::memory_order_relaxed); }
                return value;
            }
        }
        impl_->getsMiss.fetch_add(1, std::memory_order_relaxed);
        return std::nullopt;
    }

    std::vector<AkkEngine::BatchGetResult> AkkEngine::getBatch(std::span<const std::span<const uint8_t>> keys) const {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }

        std::vector<BatchGetResult> out;
        out.reserve(keys.size());

        for (const auto& key : keys) {
            BatchGetResult result;
            result.found = getInto(key, result.value);
            out.push_back(std::move(result));
        }

        return out;
    }

    bool AkkEngine::exists(std::span<const uint8_t> key) const {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        impl_->existsTotal.fetch_add(1, std::memory_order_relaxed);
        const uint64_t seq = impl_->snapshotSeq();
        if (const auto mt = impl_->memtable->contains(key, seq); mt.has_value()) { return *mt; }
        if (impl_->sstManager) { if (const auto sst = impl_->sstManager->contains(key); sst.has_value()) { return *sst; } }
        return false;
    }

    bool AkkEngine::getInto(std::span<const uint8_t> key, std::vector<uint8_t>& out) const {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        if (impl_->blobManager) {
            auto value = get(key);
            if (!value) { return false; }
            out = std::move(*value);
            return true;
        }

        impl_->getsTotal.fetch_add(1, std::memory_order_relaxed);
        const uint64_t seq = impl_->snapshotSeq();
        if (const auto mt = impl_->memtable->getInto(key, seq, out); mt.has_value()) {
            if (*mt) { impl_->getsMemtableHit.fetch_add(1, std::memory_order_relaxed); }
            else { impl_->getsMiss.fetch_add(1, std::memory_order_relaxed); }
            return *mt;
        }
        if (impl_->sstManager) {
            if (const auto sst = impl_->sstManager->getInto(key, out); sst.has_value()) {
                if (*sst) {
                    if (impl_->opts.runtime.sstPromoteReads) {
                        if (auto record = impl_->sstManager->get(key); record && !record->isTombstone()) {
                            impl_->memtable->put(record->key, record->value, record->seq, record->flags, record->keyFp64, record->miniKey);
                        }
                    }
                    impl_->getsSstHit.fetch_add(1, std::memory_order_relaxed);
                }
                else { impl_->getsMiss.fetch_add(1, std::memory_order_relaxed); }
                return *sst;
            }
        }
        impl_->getsMiss.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    bool AkkEngine::getIntoArena(std::span<const uint8_t> key, core::BufferArena& arena, std::span<const uint8_t>& out) const {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        out = {};

        if (impl_->blobManager) {
            auto value = get(key);
            if (!value) { return false; }
            out = copySpanToArena(*value, arena);
            return true;
        }

        impl_->getsTotal.fetch_add(1, std::memory_order_relaxed);
        const uint64_t seq = impl_->snapshotSeq();
        RecordView view;
        if (impl_->memtable->get(key, seq, &view)) {
            if (view.isTombstone()) {
                impl_->getsMiss.fetch_add(1, std::memory_order_relaxed);
                return false;
            }
            out = copySpanToArena(view.value(), arena);
            impl_->getsMemtableHit.fetch_add(1, std::memory_order_relaxed);
            return true;
        }

        if (impl_->sstManager) {
            auto record = impl_->sstManager->get(key);
            if (record) {
                if (record->isTombstone()) {
                    impl_->getsMiss.fetch_add(1, std::memory_order_relaxed);
                    return false;
                }
                if (impl_->opts.runtime.sstPromoteReads && impl_->memtable) {
                    impl_->memtable->put(record->key, record->value, record->seq, record->flags, record->keyFp64, record->miniKey);
                }
                out = copySpanToArena(record->value, arena);
                impl_->getsSstHit.fetch_add(1, std::memory_order_relaxed);
                return true;
            }
        }
        impl_->getsMiss.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    size_t AkkEngine::count(std::span<const uint8_t> startKey, std::span<const uint8_t> endKey) const {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }

        memtable::MemTable::KeyRange range;
        range.start.assign(startKey.begin(), startKey.end());
        range.end.assign(endKey.begin(), endKey.end());
        auto mt = impl_->memtable->iterator(range, impl_->snapshotSeq());
        sst::SSTManager::Iterator sstIt;
        if (impl_->sstManager) { sstIt = impl_->sstManager->scanIter(startKey, endKey); }

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
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        impl_->scansTotal.fetch_add(1, std::memory_order_relaxed);

        memtable::MemTable::KeyRange range;
        range.start.assign(startKey.begin(), startKey.end());
        range.end.assign(endKey.begin(), endKey.end());
        auto mt = impl_->memtable->iterator(range, impl_->snapshotSeq());
        sst::SSTManager::Iterator st;
        if (impl_->sstManager) { st = impl_->sstManager->scanIter(startKey, endKey); }
        return core::ArenaGenerator<ScanRecordView>::withArena(
            arena,
            [&arena, mt = std::move(mt), st = std::move(st), blobManager = impl_->blobManager.get()]() mutable {
                return scanGenerator(arena, std::move(mt), std::move(st), blobManager);
            }
        );
    }

    std::optional<std::vector<uint8_t>> AkkEngine::getAt(std::span<const uint8_t> key, uint64_t atSeq) const {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        if (!impl_->versionLog) { return std::nullopt; }
        auto entry = impl_->versionLog->getAt(key, atSeq);
        if (!entry || (entry->flags & core::MemHdr16::FLAG_TOMBSTONE) != 0) { return std::nullopt; }
        return impl_->resolveValue(entry->flags, entry->value);
    }

    std::vector<VersionEntry> AkkEngine::history(std::span<const uint8_t> key) const {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->versionLog ? impl_->versionLog->history(key) : std::vector<VersionEntry>{};
    }

    void AkkEngine::rollbackTo(uint64_t targetSeq) {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
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
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
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
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { return out; }

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
        if (impl_->walWriter) { impl_->walWriter->forceSync(); }
        if (impl_->versionLog) { impl_->versionLog->forceSync(); }
    }

    void AkkEngine::forceFlush() { if (impl_ && impl_->memtable) { impl_->memtable->forceFlush(); } }

    void AkkEngine::runBlobGc() {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        if (!impl_->blobManager) { return; }
        if (impl_->versionLog) { throw std::runtime_error("AkkEngine: blob GC is disabled while version log is enabled"); }
        impl_->runBlobGcIfSafe();
    }

    void AkkEngine::close() {
        if (!impl_) { return; }
        bool expected = false;
        if (!impl_->closed.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) { return; }

        if (impl_->apiServer) {
            impl_->apiServer->close();
            impl_->apiServer.reset();
        }
        if (impl_->clusterRuntime) {
            impl_->clusterRuntime->close();
            impl_->clusterRuntime.reset();
        }
        if (impl_->memtable && impl_->opts.runtime.forceFlushOnClose) { impl_->memtable->forceFlush(); }
        if (impl_->sstManager) {
            impl_->sstManager->shutdown();
            impl_->sstManager.reset();
        }
        if (impl_->walWriter) {
            if (impl_->opts.runtime.forceSyncOnClose) { impl_->walWriter->forceSync(); }
            impl_->walWriter->close();
            impl_->walWriter.reset();
        }
        if (impl_->manifest) {
            impl_->manifest->close();
            impl_->manifest.reset();
        }
        if (impl_->blobManager) {
            if (impl_->opts.blob.gcOnClose) { impl_->runBlobGcIfSafe(); }
            impl_->blobManager->close();
            impl_->blobManager.reset();
        }
        if (impl_->versionLog) {
            impl_->versionLog->close();
            impl_->versionLog.reset();
        }
        impl_->memtable.reset();
    }
} // namespace akkaradb::engine

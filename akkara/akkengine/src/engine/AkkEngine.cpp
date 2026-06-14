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

// akkengine/src/engine/AkkEngine.cpp
#include "akk/engine/AkkEngine.hpp"

#include "akk/core/record/KeyFingerprint.hpp"
#include "akk/core/record/MemHdr16.hpp"
#include "akk/engine/blob/BlobFraming.hpp"
#include "akk/engine/cluster/ClusterRuntimeProvider.hpp"
#include "akk/engine/manifest/Manifest.hpp"
#include "akk/engine/server/AkkApiServerProvider.hpp"
#include "akk/engine/wal/WalRecovery.hpp"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <random>
#include <stdexcept>
#include <string>

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
            const uint32_t raw = (writers * (writers - 1u) * 9u + 3u) / 4u;
            return std::min(nextPow2(std::max(raw, 2u)), cap);
        }

        void ensureDir(const fs::path& path) { if (!path.empty()) { fs::create_directories(path); } }

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
            const int cmp = std::ranges::lexicographical_compare(a, b)
                                ? -1
                                : std::ranges::lexicographical_compare(b, a)
                                ? 1
                                : 0;
            return cmp;
        }

        [[nodiscard]] std::span<const uint8_t> copySpanToArena(std::span<const uint8_t> in, core::BufferArena& arena) {
            if (in.empty()) { return {}; }
            std::byte* raw = arena.allocate(in.size(), alignof(uint8_t));
            auto* bytes = reinterpret_cast<uint8_t*>(raw);
            std::memcpy(bytes, in.data(), in.size());
            return {bytes, in.size()};
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
            explicit Impl(AkkEngineOptions optionsIn) : opts{std::move(optionsIn)} {}

            AkkEngineOptions opts;
            std::atomic<bool> closed{false};
            uint64_t nodeId = 0;

            std::unique_ptr<manifest::Manifest> manifest;
            std::unique_ptr<sst::SSTManager> sstManager;
            std::unique_ptr<memtable::MemTable> memtable;
            std::unique_ptr<wal::WalWriter> walWriter;
            std::unique_ptr<blob::BlobManager> blobManager;
            std::unique_ptr<vlog::VersionLog> versionLog;
            std::unique_ptr<cluster::IClusterRuntime> clusterRuntime;
            std::unique_ptr<server::IAkkApiServer> apiServer;

            mutable std::mutex writeMu;
            std::atomic<uint64_t> putsTotal{0};
            std::atomic<uint64_t> removesTotal{0};
            std::atomic<uint64_t> getsTotal{0};
            std::atomic<uint64_t> getsMemtableHit{0};
            std::atomic<uint64_t> getsSstHit{0};
            std::atomic<uint64_t> getsMiss{0};
            std::atomic<uint64_t> existsTotal{0};
            std::atomic<uint64_t> scansTotal{0};
            std::atomic<uint64_t> blobPutsTotal{0};

            [[nodiscard]] uint64_t snapshotSeq() const noexcept { return memtable ? memtable->lastSeq() : 0; }

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

            void appendAll(
                uint64_t seq,
                std::span<const uint8_t> key,
                std::span<const uint8_t> storedValue,
                uint8_t flags,
                uint64_t sourceNodeId,
                uint64_t precomputedFp64 = 0,
                uint64_t precomputedMiniKey = 0
            ) {
                const uint64_t fp64 = precomputedFp64 != 0 ? precomputedFp64 : core::computeKeyFp64(key);
                const uint64_t mini = precomputedMiniKey != 0 ? precomputedMiniKey : core::buildMiniKey(key);
                if (walWriter) { walWriter->append(key, storedValue, seq, flags, fp64); }
                if (versionLog) { versionLog->append(key, seq, sourceNodeId, nowNs(), flags, storedValue); }

                if ((flags & MemHdr16::FLAG_TOMBSTONE) != 0) { memtable->remove(key, seq, fp64, mini); }
                else { memtable->put(key, storedValue, seq, flags, fp64, mini); }
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
                appendAll(seq, key, value, flags, sourceNodeId);
                memtable->advanceSeq(seq);
            }
    };

    AkkEngine::AkkEngine() = default;
    AkkEngine::~AkkEngine() { close(); }

    std::unique_ptr<AkkEngine> AkkEngine::open(AkkEngineOptions options) {
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
            cluster::ClusterEngineCallbacks callbacks;
            callbacks.getCurrentSeq = [&impl] { return impl.snapshotSeq(); };
            callbacks.getLastSeq = [&impl] { return impl.snapshotSeq(); };
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
                throw std::runtime_error(
                    "AkkEngine: cluster component is enabled, but the cluster runtime backend library is not available"
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

        if (impl.opts.components.apiEnabled) {
            if (!server::akkApiServerFactoryAvailable() && !server::loadAkkApiServerBackend(impl.opts.api.serverBackendPath)) {
                throw std::runtime_error("AkkEngine: API server component is enabled, but the API server backend library is not available");
            }
            impl.apiServer = server::createAkkApiServer(*engine, impl.opts.api);
            impl.apiServer->start();
        }

        return engine;
    }

    void AkkEngine::put(std::span<const uint8_t> key, std::span<const uint8_t> value) {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }

        uint64_t seq = 0;
        uint8_t flags = MemHdr16::FLAG_NORMAL;
        std::vector<uint8_t> stored;
        {
            std::lock_guard lock(impl_->writeMu);
            seq = impl_->memtable->reserveSeq(1);
            stored = impl_->maybeExternalize(seq, value, flags);
            impl_->putsTotal.fetch_add(1, std::memory_order_relaxed);
            if ((flags & MemHdr16::FLAG_BLOB) != 0) { impl_->blobPutsTotal.fetch_add(1, std::memory_order_relaxed); }
            impl_->appendAll(seq, key, stored, flags, impl_->nodeId);
        }
        if (impl_->clusterRuntime) { impl_->clusterRuntime->shipEntry(seq, cluster::ReplOpType::PUT, key, stored, flags, impl_->nodeId); }
    }

    void AkkEngine::putHinted(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t fp64, uint64_t miniKey) {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }

        uint64_t seq = 0;
        uint8_t flags = MemHdr16::FLAG_NORMAL;
        std::vector<uint8_t> stored;
        {
            std::lock_guard lock(impl_->writeMu);
            seq = impl_->memtable->reserveSeq(1);
            stored = impl_->maybeExternalize(seq, value, flags);
            impl_->putsTotal.fetch_add(1, std::memory_order_relaxed);
            if ((flags & MemHdr16::FLAG_BLOB) != 0) { impl_->blobPutsTotal.fetch_add(1, std::memory_order_relaxed); }
            impl_->appendAll(seq, key, stored, flags, impl_->nodeId, fp64, miniKey);
        }
        if (impl_->clusterRuntime) { impl_->clusterRuntime->shipEntry(seq, cluster::ReplOpType::PUT, key, stored, flags, impl_->nodeId); }
    }

    void AkkEngine::putBatch(std::span<const BatchPutEntry> entries) {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        if (entries.empty()) { return; }

        struct PendingShip {
            uint64_t seq = 0;
            std::span<const uint8_t> key;
            std::vector<uint8_t> stored;
            uint8_t flags = core::MemHdr16::FLAG_NORMAL;
        };

        std::vector<PendingShip> pending;
        pending.reserve(entries.size());

        {
            std::lock_guard lock(impl_->writeMu);
            const uint64_t baseSeq = impl_->memtable->reserveSeq(entries.size());
            for (size_t i = 0; i < entries.size(); ++i) {
                const auto& [key, value] = entries[i];
                PendingShip item;
                item.seq = baseSeq + i;
                item.key = key;
                item.stored = impl_->maybeExternalize(item.seq, value, item.flags);
                impl_->putsTotal.fetch_add(1, std::memory_order_relaxed);
                if ((item.flags & MemHdr16::FLAG_BLOB) != 0) { impl_->blobPutsTotal.fetch_add(1, std::memory_order_relaxed); }
                impl_->appendAll(item.seq, item.key, item.stored, item.flags, impl_->nodeId);
                pending.push_back(std::move(item));
            }
        }

        if (impl_->clusterRuntime) {
            for (const PendingShip& item : pending) {
                impl_->clusterRuntime->shipEntry(item.seq, cluster::ReplOpType::PUT, item.key, item.stored, item.flags, impl_->nodeId);
            }
        }
    }

    void AkkEngine::remove(std::span<const uint8_t> key) {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }

        uint64_t seq = 0;
        constexpr uint8_t flags = MemHdr16::FLAG_TOMBSTONE;
        {
            std::lock_guard lock(impl_->writeMu);
            seq = impl_->memtable->reserveSeq(1);
            impl_->removesTotal.fetch_add(1, std::memory_order_relaxed);
            impl_->appendAll(seq, key, {}, flags, impl_->nodeId);
        }
        if (impl_->clusterRuntime) { impl_->clusterRuntime->shipEntry(seq, cluster::ReplOpType::REMOVE, key, {}, flags, impl_->nodeId); }
    }

    void AkkEngine::removeHinted(std::span<const uint8_t> key, uint64_t fp64, uint64_t miniKey) {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }

        uint64_t seq = 0;
        constexpr uint8_t flags = MemHdr16::FLAG_TOMBSTONE;
        {
            std::lock_guard lock(impl_->writeMu);
            seq = impl_->memtable->reserveSeq(1);
            impl_->removesTotal.fetch_add(1, std::memory_order_relaxed);
            impl_->appendAll(seq, key, {}, flags, impl_->nodeId, fp64, miniKey);
        }
        if (impl_->clusterRuntime) { impl_->clusterRuntime->shipEntry(seq, cluster::ReplOpType::REMOVE, key, {}, flags, impl_->nodeId); }
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
            if (!prev || (prev->flags & core::MemHdr16::FLAG_TOMBSTONE) != 0) { remove(key); }
            else {
                auto value = impl_->resolveValue(prev->flags, prev->value);
                if (value) { put(key, *value); }
                else { remove(key); }
            }
        }
    }

    void AkkEngine::rollbackKey(std::span<const uint8_t> key, uint64_t targetSeq) {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        if (!impl_->versionLog) { throw std::runtime_error("AkkEngine: version log is disabled"); }
        const auto prev = impl_->versionLog->getAt(key, targetSeq);
        if (!prev || (prev->flags & core::MemHdr16::FLAG_TOMBSTONE) != 0) { remove(key); }
        else {
            auto value = impl_->resolveValue(prev->flags, prev->value);
            if (value) { put(key, *value); }
            else { remove(key); }
        }
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
        return out;
    }

    void AkkEngine::forceSync() { if (impl_ && impl_->walWriter) { impl_->walWriter->forceSync(); } }

    void AkkEngine::forceFlush() { if (impl_ && impl_->memtable) { impl_->memtable->forceFlush(); } }

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

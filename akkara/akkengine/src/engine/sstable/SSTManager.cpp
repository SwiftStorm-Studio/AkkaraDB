/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/sstable/SSTManager.cpp
#include "akk/engine/sstable/SSTManager.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <format>
#include <fstream>
#include <functional>
#include <iterator>
#include <list>
#include <mutex>
#include <queue>
#include <ranges>
#include <shared_mutex>
#include <stdexcept>
#include <thread>
#include <unordered_set>

#include <zstd.h>

#include "akk/cpu/CRC32C.hpp"
#include "akk/engine/detail/StorageFileLock.hpp"

#ifdef _WIN32
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace akkaradb::engine::sst {
    namespace {
        [[nodiscard]] int compareBytes(std::span<const uint8_t> a, std::span<const uint8_t> b) noexcept {
            const auto n = std::ranges::min(a.size(), b.size());
            if (n > 0) {
                const auto c = std::memcmp(a.data(), b.data(), n);
                if (c != 0) { return c < 0 ? -1 : 1; }
            }
            if (a.size() < b.size()) { return -1; }
            if (a.size() > b.size()) { return 1; }
            return 0;
        }

        [[nodiscard]] bool keyLess(std::span<const uint8_t> a, std::span<const uint8_t> b) noexcept {
            return compareBytes(a, b) < 0;
        }

        [[nodiscard]] std::string hexKey(std::span<const uint8_t> key) {
            static constexpr char kHex[] = "0123456789abcdef";
            std::string out;
            out.resize(key.size() * 2);
            for (size_t i = 0; i < key.size(); ++i) {
                out[i * 2] = kHex[key[i] >> 4];
                out[i * 2 + 1] = kHex[key[i] & 0x0f];
            }
            return out;
        }

        [[nodiscard]] core::RecordView toView(const SSTRecord& rec) noexcept {
            return core::RecordView{
                rec.key.data(),
                static_cast<uint16_t>(rec.key.size()),
                rec.value.data(),
                static_cast<uint16_t>(rec.value.size()),
                rec.seq,
                rec.flags,
                rec.keyFp64,
                rec.miniKey
            };
        }

        [[nodiscard]] uint64_t recordBytes(const SSTRecord& rec) noexcept { return alignUpU64(32 + rec.key.size() + rec.value.size(), 8); }

        [[nodiscard]] bool levelsOverlap(int aSrc, int aDst, int bSrc, int bDst) noexcept {
            return aSrc == bSrc || aSrc == bDst || aDst == bSrc || aDst == bDst;
        }

        [[nodiscard]] bool compactionEnabled(const SSTManager::Options& options) noexcept {
            switch (options.compactionMode) {
                case SSTCompactionMode::AUTO: return options.compactThreads != 0;
                case SSTCompactionMode::BACKGROUND: return true;
                case SSTCompactionMode::DISABLED: return false;
            }
            return true;
        }

        void validateZstdCompressionLevel(const SSTManager::Options& options) {
            if (options.codec == SSTWriter::Codec::ZSTD && (options.zstdCompressionLevel < ZSTD_minCLevel() || options.zstdCompressionLevel
                > ZSTD_maxCLevel())) {
                throw std::invalid_argument("SSTManager: zstdCompressionLevel is outside the supported Zstd range");
            }
        }

        void validateOptions(const SSTManager::Options& options) {
            if (options.maxL0Files <= 0) { throw std::invalid_argument("SSTManager: maxL0Files must be positive"); }
            if (options.compactThreads < 0) { throw std::invalid_argument("SSTManager: compactThreads must be nonnegative"); }
            if (options.l1MaxBytes == 0 || options.targetFileSize == 0) {
                throw std::invalid_argument("SSTManager: level and target file sizes must be positive");
            }
            if (!std::isfinite(options.levelSizeMultiplier) || options.levelSizeMultiplier < 1.0) {
                throw std::invalid_argument("SSTManager: levelSizeMultiplier must be finite and >= 1");
            }
            if (options.blockSize < 4096 || (options.blockSize & 7u) != 0) {
                throw std::invalid_argument("SSTManager: blockSize must be >=4096 and 8-byte aligned");
            }
            switch (options.compactionMode) {
                case SSTCompactionMode::AUTO:
                case SSTCompactionMode::BACKGROUND:
                case SSTCompactionMode::DISABLED: break;
                default: throw std::invalid_argument("SSTManager: invalid compaction mode");
            }
            if (options.codec != SSTWriter::Codec::NONE && options.codec != SSTWriter::Codec::ZSTD) {
                throw std::invalid_argument("SSTManager: invalid codec");
            }
            validateZstdCompressionLevel(options);
        }

        std::vector<manifest::Manifest::SSTBlobRefsEvent::Entry> toManifestBlobRefs(
            const std::vector<SSTWriter::Result::BlobRefEntry>& refs
        ) {
            return refs | std::views::transform([](const auto& ref) {
                const auto& [key, seq, flags, blobId] = ref;
                return manifest::Manifest::SSTBlobRefsEvent::Entry{key, seq, flags, blobId};
            }) | std::ranges::to<std::vector>();
        }

        void syncFile(const std::filesystem::path& path) {
            #ifdef _WIN32
            const auto h = ::CreateFileW(
                path.wstring().c_str(),
                GENERIC_READ | GENERIC_WRITE,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                nullptr,
                OPEN_EXISTING,
                FILE_ATTRIBUTE_NORMAL,
                nullptr
            ); if (h == INVALID_HANDLE_VALUE) { throw std::runtime_error("SSTManager: failed to open file for sync: " + path.string()); }
            const auto ok = ::FlushFileBuffers(h) != 0; const auto err = ::GetLastError(); ::CloseHandle(h); if (!ok) {
                throw std::runtime_error("SSTManager: file sync failed: " + path.string() + " error=" + std::to_string(err));
            }
            #else
            const auto fd = ::open(path.c_str(), O_RDONLY);
            if (fd < 0) { throw std::runtime_error("SSTManager: failed to open file for sync: " + path.string()); }
            const auto rc = ::fsync(fd);
            const auto saved = errno;
            ::close(fd);
            if (rc != 0) { throw std::runtime_error("SSTManager: file sync failed: " + path.string() + " errno=" + std::to_string(saved)); }
            #endif
        }

        void syncDirectory(const std::filesystem::path& dir) {
            #ifndef _WIN32
            const auto path = dir.empty() ? std::filesystem::path{"."} : dir;
            const auto fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY);
            if (fd < 0) { throw std::runtime_error("SSTManager: cannot open directory for sync: " + path.string()); }
            const auto rc = ::fsync(fd);
            const auto saved = errno;
            ::close(fd);
            if (rc != 0) { throw std::runtime_error("SSTManager: directory sync failed: " + path.string() + " errno=" + std::to_string(saved)); }
            #else
            (void)dir; // Windows publication uses a write-through rename below.
            #endif
        }

        void durableRename(const std::filesystem::path& tmp, const std::filesystem::path& path) {
            syncFile(tmp);
            #ifdef _WIN32
            if (!::MoveFileExW(tmp.c_str(), path.c_str(), MOVEFILE_WRITE_THROUGH)) {
                throw std::runtime_error("SSTManager: durable rename failed: " + std::to_string(::GetLastError()));
            }
            #else
            std::filesystem::rename(tmp, path);
            syncDirectory(path.parent_path());
            #endif
        }

        void removeFileDurable(const std::filesystem::path& path) {
            std::error_code ec;
            if (std::filesystem::remove(path, ec)) { syncDirectory(path.parent_path()); }
        }

        [[nodiscard]] core::ArenaGenerator<core::RecordView> spanRecords(std::span<const core::RecordView> records) {
            for (const auto& record : records) { co_yield record; }
        }

        [[nodiscard]] core::ArenaGenerator<core::RecordView> flushChunk(
            core::ArenaGenerator<core::RecordView>::iterator& position, uint64_t targetBytes
        ) {
            uint64_t bytes = 0;
            std::vector<uint8_t> lastKey;
            bool nonempty = false;
            while (position != std::default_sentinel) {
                const auto& record = *position;
                const auto size = alignUpU64(sizeof(core::SSTHdr32) + record.keySize() + record.valueSize(), 8);
                if (nonempty && record.compareKey(lastKey) != 0 && (bytes >= targetBytes || size > targetBytes - bytes)) {
                    co_return;
                }
                lastKey.assign_range(record.key());
                nonempty = true;
                bytes += size;
                co_yield record;
                ++position;
            }
        }

        // Test-only fault injection. Each point is reached only after the preceding
        // persistence step completed, so recovery tests exercise real crash boundaries.
        void crashAtTestPoint(const char* point) noexcept {
            const auto* requested = std::getenv("AKKARADB_TEST_CRASH_POINT");
            if (requested != nullptr && std::strcmp(requested, point) == 0) { std::quick_exit(86); }
        }
    } // namespace

    struct SSTManager::ReadState {
        public:
            struct Meta {
                std::filesystem::path path;
                std::string filename;
                int level = 0;
                uint64_t entryCount = 0;
                uint64_t file_size = 0;
                uint64_t minSeq = 0;
                uint64_t maxSeq = 0;
                std::vector<uint8_t> firstKey;
                std::vector<uint8_t> lastKey;
                std::shared_ptr<SSTReader> reader;
            };

            using Levels = std::vector<std::vector<Meta>>;
            Levels levels;
            uint64_t maxSeq = 0;
            // Balanced interval tree over overlapping L0 ranges. Each midpoint
            // stores the index of the greatest lastKey in its subtree.
            std::vector<size_t> l0Order;
            std::vector<size_t> l0MaxLast;

            size_t buildRangeIndex(size_t begin, size_t end) {
                const auto mid = begin + (end - begin) / 2;
                auto maximum = l0Order[mid];
                const auto merge = [&](size_t other) {
                    if (keyLess(levels[0][maximum].lastKey, levels[0][other].lastKey)) { maximum = other; }
                };
                if (begin < mid) { merge(buildRangeIndex(begin, mid)); }
                if (mid + 1 < end) { merge(buildRangeIndex(mid + 1, end)); }
                l0MaxLast[mid] = maximum;
                return maximum;
            }

            void indexRanges() {
                if (levels.empty() || levels[0].empty()) { return; }
                for (size_t i = 0; i < levels[0].size(); ++i) { l0Order.push_back(i); }
                std::ranges::sort(l0Order, [this](size_t a, size_t b) { return keyLess(levels[0][a].firstKey, levels[0][b].firstKey); });
                l0MaxLast.resize(l0Order.size());
                (void)buildRangeIndex(0, l0Order.size());
            }

            [[nodiscard]] std::vector<std::shared_ptr<SSTReader>> rangeReaders(
                std::span<const uint8_t> start, std::span<const uint8_t> end, uint64_t seq) const {
                std::vector<std::shared_ptr<SSTReader>> readers;
                if (!end.empty() && !start.empty() && compareBytes(start, end) >= 0) { return readers; }
                const auto consider = [&](const Meta& meta) {
                    if (meta.reader && meta.minSeq <= seq && (start.empty() || compareBytes(meta.lastKey, start) >= 0) &&
                        (end.empty() || compareBytes(meta.firstKey, end) < 0)) { readers.push_back(meta.reader); }
                };
                const auto visit = [&](auto&& self, size_t begin, size_t finish) -> void {
                    if (begin == finish) { return; }
                    const auto mid = begin + (finish - begin) / 2;
                    if (!start.empty() && keyLess(levels[0][l0MaxLast[mid]].lastKey, start)) { return; }
                    self(self, begin, mid);
                    const auto& meta = levels[0][l0Order[mid]];
                    if (!end.empty() && compareBytes(meta.firstKey, end) >= 0) { return; }
                    consider(meta);
                    self(self, mid + 1, finish);
                };
                visit(visit, 0, l0Order.size());
                for (const auto& files : levels | std::views::drop(1)) {
                    const auto first = start.empty() ? files.begin() : std::ranges::lower_bound(files, start, keyLess, &Meta::lastKey);
                    for (auto it = first; it != files.end(); ++it) {
                        if (!end.empty() && compareBytes(it->firstKey, end) >= 0) { break; }
                        consider(*it);
                    }
                }
                return readers;
            }

            [[nodiscard]] std::optional<SSTReader::PinnedRecord> getPinned(std::span<const uint8_t> key, uint64_t snapshotSeq) const {
                std::optional<SSTReader::PinnedRecord> best;
                const auto consider = [&](const Meta& meta) {
                    if (!meta.reader || meta.minSeq > snapshotSeq || (best && meta.maxSeq <= best->view.seq())) { return; }
                    auto candidate = meta.reader->getPinned(key, snapshotSeq);
                    if (candidate && (!best || candidate->view.seq() > best->view.seq())) { best = std::move(candidate); }
                };
                // Flush completion and file ids do not imply sequence precedence.
                if (!levels.empty()) { for (const auto& meta : levels[0]) { consider(meta); } }
                for (const auto& files : levels | std::views::drop(1)) {
                    const auto candidate = std::ranges::lower_bound(files, key, keyLess, &Meta::lastKey);
                    if (candidate != files.end() && compareBytes(candidate->firstKey, key) <= 0 && compareBytes(key, candidate->lastKey) <= 0) {
                        consider(*candidate);
                    }
                }
                return best;
            }
    };

    class SSTManager::Iterator::Impl {
        public:
            Impl(
                std::shared_ptr<const ReadState> state,
                std::span<const uint8_t> startKey,
                std::span<const uint8_t> endKey,
                uint64_t snapshotSeq,
                bool includeTombstones
            )
                : state_{std::move(state)}, snapshotSeq_{snapshotSeq}, includeTombstones_{includeTombstones}, startKey_{std::from_range, startKey}, endKey_{std::from_range, endKey} {
                auto readers = state_->rangeReaders(startKey_, endKey_, snapshotSeq_);
                sources_.reserve(readers.size());
                for (auto& reader : readers) {
                    if (!reader) { continue; }
                    Source source;
                    source.reader = std::move(reader);
                    source.rows = source.reader->scan(startKey_, endKey_, snapshotSeq);
                    source.it = source.rows.begin();
                    const auto sourceIdx = sources_.size();
                    sources_.push_back(std::move(source));
                    pushCurrent(sourceIdx);
                }
                advance();
            }

            [[nodiscard]] bool hasNext() const noexcept { return pending_.has_value(); }

            [[nodiscard]] std::optional<SSTRecord> next() {
                if (!pending_) { return std::nullopt; }
                auto out = std::move(pending_);
                pending_.reset();
                advance();
                return out;
            }

            [[nodiscard]] std::optional<SSTRecord> get(std::span<const uint8_t> key) const {
                const auto result = state_->getPinned(key, snapshotSeq_);
                return result ? std::optional{result->toOwned()} : std::nullopt;
            }
            [[nodiscard]] std::unique_ptr<Impl> fork(std::span<const uint8_t> start, std::span<const uint8_t> end) const {
                return std::make_unique<Impl>(state_, start, end, snapshotSeq_, includeTombstones_);
            }

        private:
            struct Source {
                std::shared_ptr<SSTReader> reader;
                core::ArenaGenerator<SSTRecord> rows;
                core::ArenaGenerator<SSTRecord>::iterator it;
            };

            struct HeapEntry {
                SSTRecord rec;
                size_t sourceIdx = 0;
            };

            struct HeapGreater {
                bool operator()(const HeapEntry& a, const HeapEntry& b) const noexcept {
                    const auto c = compareBytes(a.rec.key, b.rec.key);
                    if (c != 0) { return c > 0; }
                    return a.rec.seq < b.rec.seq;
                }
            };

            void pushCurrent(size_t sourceIdx) {
                auto& source = sources_[sourceIdx];
                if (source.it == std::default_sentinel) { return; }
                heap_.push(HeapEntry{.rec = *source.it, .sourceIdx = sourceIdx});
                ++source.it;
            }

            void advance() {
                pending_.reset();
                while (!heap_.empty()) {
                    auto [best, sourceIdx] = heap_.top();
                    heap_.pop();
                    pushCurrent(sourceIdx);

                    while (!heap_.empty() && compareBytes(heap_.top().rec.key, best.key) == 0) {
                        auto [next, nextSourceIdx] = heap_.top();
                        heap_.pop();
                        pushCurrent(nextSourceIdx);
                        if (next.seq > best.seq) { best = std::move(next); }
                    }

                    if (includeTombstones_ || !best.isTombstone()) {
                        pending_ = std::move(best);
                        return;
                    }
                }
            }

            std::shared_ptr<const ReadState> state_;
            uint64_t snapshotSeq_;
            bool includeTombstones_;
            std::vector<uint8_t> startKey_;
            std::vector<uint8_t> endKey_;
            std::vector<Source> sources_;
            std::priority_queue<HeapEntry, std::vector<HeapEntry>, HeapGreater> heap_;
            std::optional<SSTRecord> pending_;
    };

    std::optional<SSTRecord> SSTManager::ReadSnapshot::get(std::span<const uint8_t> key, uint64_t snapshotSeq) const {
        if (!state_) { return std::nullopt; }
        const auto record = state_->getPinned(key, snapshotSeq);
        return record ? std::optional{record->toOwned()} : std::nullopt;
    }

    std::optional<bool> SSTManager::ReadSnapshot::contains(std::span<const uint8_t> key, uint64_t snapshotSeq) const {
        if (!state_) { return std::nullopt; }
        const auto record = state_->getPinned(key, snapshotSeq);
        return record ? std::optional{!record->view.isTombstone()} : std::nullopt;
    }

    std::optional<bool> SSTManager::ReadSnapshot::getInto(
        std::span<const uint8_t> key, std::vector<uint8_t>& out, uint64_t snapshotSeq) const {
        if (!state_) { return std::nullopt; }
        const auto record = state_->getPinned(key, snapshotSeq);
        if (!record) { return std::nullopt; }
        if (record->view.isTombstone()) { return false; }
        out.assign_range(record->view.value());
        return true;
    }

    uint64_t SSTManager::ReadSnapshot::maxSequence() const noexcept {
        return state_ ? state_->maxSeq : 0;
    }

    SSTManager::Iterator SSTManager::ReadSnapshot::scanIter(
        std::span<const uint8_t> startKey, std::span<const uint8_t> endKey,
        uint64_t snapshotSeq, bool includeTombstones
    ) const {
        if (!state_) { return Iterator{}; }
        return Iterator{std::make_unique<Iterator::Impl>(state_, startKey, endKey, snapshotSeq, includeTombstones)};
    }

    class SSTManager::Impl {
        public:
            using Meta = ReadState::Meta;
            using Levels = ReadState::Levels;

            struct Work {
                int src = 0;
                int dst = 1;
                std::vector<Meta> inputs;
                std::vector<std::vector<uint8_t>> splitBefore{};
            };

            struct BusyWork {
                int src = 0;
                int dst = 0;
            };

            Impl(std::filesystem::path sstDir, const Options& options, manifest::Manifest* manifest,
                std::function<uint64_t()> compactionVisibilitySeq)
                : sstDir_{std::move(sstDir)}, options_{options}, manifest_{manifest},
                  compactionVisibilitySeq_{std::move(compactionVisibilitySeq)} {
                if (options_.maxLevels < 2) { options_.maxLevels = 2; }
                if (sstDir_.empty()) { throw std::invalid_argument("SSTManager: sstDir is required"); }
                validateOptions(options_);
                blockCache_ = std::make_shared<SSTReader::BlockCache>(options_.blockCacheBytes);
                sstDir_ = std::filesystem::absolute(sstDir_);
                std::filesystem::create_directories(sstDir_);
                directoryLock_ = std::make_unique<detail::StorageFileLock>(sstDir_ / ".sst.lock");
                const auto statePath = sstDir_ / "sst-state.akmf";
                if (manifest_ && std::filesystem::exists(statePath)) {
                    throw std::runtime_error("SSTManager: SST state catalog is present; retain standalone catalog mode or recreate the database");
                }
                if (!manifest_) {
                    if (!std::filesystem::exists(statePath)) {
                        for (const auto& entry : std::filesystem::directory_iterator(sstDir_)) {
                            if (entry.path().extension() == ".aksst") {
                                throw std::runtime_error("SSTManager: existing SST files require their SST state catalog; recreate the pre-release database");
                            }
                        }
                    }
                    // Standalone SST storage needs the same atomic replacement protocol
                    // as an engine with an external Manifest. Never infer live files from
                    // directory contents after a partially completed compaction.
                    ownedManifest_ = manifest::Manifest::create(statePath);
                    manifest_ = ownedManifest_.get();
                    syncDirectory(sstDir_);
                }
                validateCatalogBinding();
                levels_.resize(static_cast<size_t>(options_.maxLevels));
                publishLocked();
                if (compactionEnabled(options_)) {
                    const auto n = std::ranges::max(1, options_.compactThreads);
                    compactThreads_.reserve(static_cast<size_t>(n));
                    for (auto i = 0; i < n; ++i) { compactThreads_.emplace_back([this] { compactionLoop(); }); }
                }
            }

            ~Impl() { shutdown(); }

            void recover() {
                std::unique_lock lock{levelsMu_};
                { std::lock_guard retireLock{retiredMu_}; retiredFiles_.clear(); }
                validateCatalogBinding();
                Levels recovered(levels_.size());

                std::vector<std::filesystem::path> abandonedFiles;
                std::unordered_set<std::string> liveManifestFiles;
                std::vector<std::string> allSstFiles;
                for (const auto& entry : std::filesystem::directory_iterator(sstDir_)) {
                    const auto& p = entry.path();
                    if (p.extension() == ".tmp") { abandonedFiles.push_back(p); }
                    else if (p.extension() == ".aksst") {
                        const std::string filename = p.filename().string();
                        allSstFiles.push_back(filename);
                    }
                }

                const auto files = manifest_->liveSst();
                liveManifestFiles.insert_range(files);
                for (const auto& filename : allSstFiles) {
                    if (!liveManifestFiles.contains(filename)) { abandonedFiles.push_back(sstDir_ / filename); }
                }

                for (const auto& file : files) {
                    const auto path = sstDir_ / file;
                    auto reader = SSTReader::open(path, readerOptions());
                    if (!reader) { throw std::runtime_error("SSTManager: manifest references missing or corrupt SST: " + file); }
                    const auto level = reader->header().level;
                    if (level >= static_cast<uint32_t>(options_.maxLevels)) {
                        throw std::runtime_error("SSTManager: live SST " + file + " uses level " + std::to_string(level) +
                            " outside configured maxLevels=" + std::to_string(options_.maxLevels));
                    }
                    auto meta = makeMeta(path, file, std::move(reader));
                    recovered[level].push_back(std::move(meta));
                }
                // Validate the complete live set before cleanup or publishing any recovered state.
                if (!catalogBound_) { writeCatalogBinding(); }
                manifest_->discardUnsealedSstBlobRefs();
                for (const auto& abandoned : abandonedFiles) {
                    if (!SSTReader::isPinned(abandoned)) { removeFileDurable(abandoned); }
                    if (abandoned.extension() == ".aksst") {
                        std::lock_guard retireLock{retiredMu_};
                        retiredFiles_.push_back(abandoned);
                    }
                }
                levels_ = std::move(recovered);
                nextFileId_ = 1;
                for (const auto& file : allSstFiles) { observeFileId(file); }
                for (const auto& file : files) { observeFileId(file); }
                sortAllLevelsLocked();
                publishLocked();
                // A reopened manager can inherit retired readers even with
                // compaction disabled. Start cleanup only if leftovers remain.
                if (!shuttingDown_.load(std::memory_order_relaxed) && compactThreads_.empty() && !cleanupThread_.joinable()) {
                    reclaimRetiredFiles();
                    if (!retiredFiles_.empty()) {
                        cleanupThread_ = std::thread([this] {
                            while (!shuttingDown_.load(std::memory_order_relaxed)) {
                                reclaimRetiredFiles();
                                std::unique_lock cleanupLock{compactMu_};
                                compactCv_.wait_for(cleanupLock, std::chrono::milliseconds{50},
                                    [this] { return shuttingDown_.load(std::memory_order_relaxed); });
                            }
                        });
                    }
                }
                requestCompaction();
            }

            void shutdown() {
                bool expected = false;
                if (!shuttingDown_.compare_exchange_strong(expected, true)) { return; }
                compactCv_.notify_all();
                for (auto& t : compactThreads_) { if (t.joinable()) { t.join(); } }
                compactThreads_.clear();
                if (cleanupThread_.joinable()) { cleanupThread_.join(); }
            }

            uint64_t flush(std::span<const core::RecordView> records) {
                return flush(records.size(), spanRecords(records));
            }

            uint64_t flush(size_t estimatedRecordCount, core::ArenaGenerator<core::RecordView> records) {
                throwIfBackgroundFailed();
                requireCatalogBinding();
                auto position = records.begin();
                if (position == records.end()) { return 0; }

                SSTWriter::Options wopts;
                wopts.level = 0;
                wopts.blockSize = options_.blockSize;
                wopts.bloomBitsPerKey = options_.bloomBitsPerKey;
                wopts.codec = options_.codec;
                wopts.zstdCompressionLevel = options_.zstdCompressionLevel;
                wopts.trackBlobReferences = options_.trackBlobReferences;

                std::vector<Meta> outputs;
                std::vector<SSTWriter::Result> results;
                std::vector<std::pair<std::filesystem::path, bool>> paths;
                uint64_t maxSeq = 0;
                bool publicationAttempted = false;
                try {
                    while (position != records.end()) {
                        if (!outputs.empty() && position->compareKey(outputs.back().lastKey) < 0) {
                            throw std::invalid_argument("SSTManager::flush: records must be sorted by key");
                        }
                        const auto path = makeFilePath(0);
                        paths.emplace_back(path, false);
                        const auto tmp = path.string() + ".tmp";
                        const auto estimate = static_cast<size_t>(std::min<uint64_t>(estimatedRecordCount,
                            std::max<uint64_t>(1, options_.targetFileSize / sizeof(core::SSTHdr32))));
                        auto result = SSTWriter::write(tmp, estimate, flushChunk(position, options_.targetFileSize), wopts);
                        crashAtTestPoint("sst.flush.after_tmp_write");
                        durableRename(tmp, path);
                        paths.back().second = true;
                        crashAtTestPoint("sst.flush.after_rename");
                        auto reader = SSTReader::open(path, readerOptions());
                        if (!reader) { throw std::runtime_error("SSTManager: cannot reopen flushed SST"); }
                        outputs.push_back(makeMeta(path, path.filename().string(), std::move(reader)));
                        maxSeq = std::max(maxSeq, result.maxSeq);
                        results.push_back(std::move(result));
                    }
                    crashAtTestPoint("sst.flush.after_outputs");
                    for (size_t i = 0; i < outputs.size(); ++i) {
                        manifest_->sstBlobRefs(outputs[i].filename, toManifestBlobRefs(results[i].blobRefs));
                    }
                    crashAtTestPoint("sst.flush.after_manifest_refs");
                    // An uncertain commit must be resolved by replay. Never delete
                    // output files after attempting the durable publication record.
                    if (outputs.size() == 1) {
                        const auto& output = outputs.front();
                        publicationAttempted = true;
                        manifest_->sstSeal(0, output.filename, output.entryCount, hexKey(output.firstKey), hexKey(output.lastKey));
                    }
                    else {
                        const auto files = outputs | std::views::transform(&Meta::filename) | std::ranges::to<std::vector>();
                        publicationAttempted = true;
                        manifest_->compactionCommit(files, {});
                    }
                    crashAtTestPoint("sst.flush.after_manifest_seal");
                    std::unique_lock lock{levelsMu_};
                    levels_[0].insert(levels_[0].begin(), std::make_move_iterator(outputs.begin()), std::make_move_iterator(outputs.end()));
                    publishLocked();
                }
                catch (...) {
                    if (!publicationAttempted) {
                        outputs.clear();
                        for (const auto& [path, renamed] : paths) {
                            std::error_code error;
                            if (renamed) { std::filesystem::remove(path, error); }
                            std::filesystem::remove(path.string() + ".tmp", error);
                        }
                    }
                    throw;
                }
                requestCompaction();
                return maxSeq;
            }

            [[nodiscard]] std::optional<SSTRecord> get(std::span<const uint8_t> key, uint64_t snapshotSeq) const {
                return readSnapshot().get(key, snapshotSeq);
            }

            [[nodiscard]] ReadSnapshot readSnapshot() const { return ReadSnapshot{snapshot_.load(std::memory_order_acquire)}; }

            [[nodiscard]] std::optional<bool> contains(std::span<const uint8_t> key, uint64_t snapshotSeq) const {
                return readSnapshot().contains(key, snapshotSeq);
            }

            [[nodiscard]] std::optional<bool> getInto(std::span<const uint8_t> key, std::vector<uint8_t>& out, uint64_t snapshotSeq) const {
                return readSnapshot().getInto(key, out, snapshotSeq);
            }

            [[nodiscard]] Iterator scanIter(
                std::span<const uint8_t> startKey,
                std::span<const uint8_t> endKey,
                uint64_t snapshotSeq,
                bool includeTombstones
            ) const {
                return readSnapshot().scanIter(startKey, endKey, snapshotSeq, includeTombstones);
            }

            [[nodiscard]] std::vector<LevelStats> levelStats() const {
                auto snap = snapshot_.load(std::memory_order_acquire);
                std::vector<LevelStats> out;
                if (!snap) { return out; }
                out.reserve(snap->levels.size());
                for (const auto& [i, level] : std::views::enumerate(snap->levels)) {
                    const auto bytes = std::ranges::fold_left(level | std::views::transform(&Meta::file_size), uint64_t{0}, std::plus{});
                    out.push_back(LevelStats{.level = static_cast<int>(i), .fileCount = level.size(), .bytes = bytes, .budgetBytes = i == 0 ? 0 : levelBudget(static_cast<int>(i))});
                }
                return out;
            }

            [[nodiscard]] uint64_t maxSequence() const noexcept {
                return readSnapshot().maxSequence();
            }

            [[nodiscard]] bool compactionPending() const noexcept { return compactRequested_.load(std::memory_order_relaxed); }

            [[nodiscard]] SSTReader::CacheStats blockCacheStats() const { return blockCache_->stats(); }

            [[nodiscard]] CompactionSnapshot compactionSnapshot() const noexcept {
                return {
                    .compactionsCompleted = compactionsCompleted_.load(std::memory_order_relaxed),
                    .filesCompacted = filesCompacted_.load(std::memory_order_relaxed),
                    .bytesCompactedIn = bytesCompactedIn_.load(std::memory_order_relaxed),
                    .bytesCompactedOut = bytesCompactedOut_.load(std::memory_order_relaxed),
                    .compactionFailures = compactionFailures_.load(std::memory_order_relaxed)
                };
            }

            void throwIfBackgroundFailed() const {
                std::lock_guard lock{errorMu_};
                if (backgroundError_) { std::rethrow_exception(backgroundError_); }
            }

        private:
            [[nodiscard]] std::array<uint8_t, 28> catalogBinding() const {
                std::array<uint8_t, 28> bytes{'A', 'K', 'S', 'C', 1, 0, 0, 0};
                const auto identity = manifest_->catalogIdentity();
                std::ranges::copy(identity, bytes.begin() + 8);
                const auto checksumBytes = std::as_bytes(std::span{bytes}.first(24));
                const auto crc = cpu::CRC32C(checksumBytes.data(), checksumBytes.size());
                for (size_t i = 0; i < 4; ++i) { bytes[24 + i] = static_cast<uint8_t>(crc >> (8 * i)); }
                return bytes;
            }

            void writeCatalogBinding() {
                const auto path = sstDir_ / "sst-catalog.akid";
                const auto tmp = sstDir_ / "sst-catalog.akid.tmp";
                const auto bytes = catalogBinding();
                std::ofstream out{tmp, std::ios::binary | std::ios::trunc};
                out.write(reinterpret_cast<const char*>(bytes.data()), bytes.size());
                out.close();
                if (!out) { throw std::runtime_error("SSTManager: cannot write SST catalog binding"); }
                durableRename(tmp, path);
                catalogBound_ = true;
            }

            void validateCatalogBinding() {
                catalogBound_ = false;
                const auto path = sstDir_ / "sst-catalog.akid";
                if (std::filesystem::exists(path)) {
                    std::array<uint8_t, 28> bytes{};
                    std::ifstream in{path, std::ios::binary};
                    in.read(reinterpret_cast<char*>(bytes.data()), bytes.size());
                    if (!in || in.peek() != std::char_traits<char>::eof() || bytes != catalogBinding()) {
                        throw std::runtime_error("SSTManager: missing, corrupt or mismatched SST catalog binding; restore the original Manifest");
                    }
                    catalogBound_ = true;
                    return;
                }
                for (const auto& entry : std::filesystem::directory_iterator(sstDir_)) {
                    if (entry.path().extension() == ".aksst") {
                        if (manifest_->liveSst().empty()) {
                            throw std::runtime_error("SSTManager: existing SST files cannot be recovered with an empty unbound catalog; restore the original Manifest");
                        }
                        // Older databases acquire their binding only after recovery
                        // validates every catalog-live file, before orphan cleanup.
                        return;
                    }
                }
                // Persist ownership before the first SST can be published, including
                // a crash after rename but before its seal reaches the catalog.
                writeCatalogBinding();
            }

            void requireCatalogBinding() const {
                if (!catalogBound_) { throw std::runtime_error("SSTManager: recover existing SST files before flushing"); }
            }

            [[nodiscard]] SSTReader::Options readerOptions() const noexcept { return {.sharedBlockCache = blockCache_}; }

            [[nodiscard]] uint64_t levelBudget(int level) const {
                if (level <= 0) { return 0; }
                auto budget = static_cast<double>(options_.l1MaxBytes);
                for (auto i = 1; i < level; ++i) { budget *= options_.levelSizeMultiplier; }
                // Large but valid layouts saturate instead of overflowing the
                // floating-point-to-integer conversion.
                if (budget >= static_cast<double>(UINT64_MAX)) { return UINT64_MAX; }
                return static_cast<uint64_t>(budget);
            }

            [[nodiscard]] uint64_t levelBytesLocked(int level) const {
                const auto sizes = levels_[static_cast<size_t>(level)] | std::views::transform(&Meta::file_size);
                return std::ranges::fold_left(sizes, uint64_t{0}, std::plus{});
            }

            [[nodiscard]] std::filesystem::path makeFilePath(int level) {
                const auto id = nextFileId_.fetch_add(1, std::memory_order_relaxed);
                return sstDir_ / std::format("L{}_{}.aksst", level, id);
            }

            [[nodiscard]] static uint64_t parseFileId(const std::string& file) noexcept {
                const auto us = file.find('_');
                const auto dot = file.find('.', us == std::string::npos ? 0 : us);
                if (us == std::string::npos || dot == std::string::npos || dot <= us + 1) { return 0; }
                try { return std::stoull(file.substr(us + 1, dot - us - 1)); }
                catch (...) { return 0; }
            }

            void observeFileId(const std::string& file) noexcept {
                const auto observedNext = parseFileId(file) + 1;
                auto currentNext = nextFileId_.load(std::memory_order_relaxed);
                while (currentNext < observedNext && !nextFileId_.compare_exchange_weak(
                    currentNext,
                    observedNext,
                    std::memory_order_relaxed,
                    std::memory_order_relaxed
                )) {}
            }

            [[nodiscard]] Meta makeMeta(
                const std::filesystem::path& path,
                const std::string& filename,
                std::unique_ptr<SSTReader> reader
            ) const {
                auto shared = std::shared_ptr{std::move(reader)};
                const auto& hdr = shared->header();
                Meta meta;
                meta.path = path;
                meta.filename = filename;
                meta.level = static_cast<int>(hdr.level);
                meta.entryCount = hdr.entryCount;
                meta.file_size = hdr.file_size;
                meta.minSeq = hdr.minSeq;
                meta.maxSeq = hdr.maxSeq;
                meta.firstKey.assign_range(shared->firstKey());
                meta.lastKey.assign_range(shared->lastKey());
                meta.reader = std::move(shared);
                return meta;
            }

            void publishLocked() {
                auto snap = std::make_shared<ReadState>();
                snap->levels = levels_;
                const auto sequences = snap->levels | std::views::join | std::views::transform(&Meta::maxSeq);
                snap->maxSeq = std::ranges::fold_left(sequences, uint64_t{0}, std::ranges::max);
                snap->indexRanges();
                snapshot_.store(std::move(snap), std::memory_order_release);
            }

            void sortAllLevelsLocked() {
                if (!levels_.empty()) {
                    std::ranges::sort(
                        levels_[0],
                        [](const Meta& a, const Meta& b) {
                            const auto aId = parseFileId(a.filename);
                            const auto bId = parseFileId(b.filename);
                            if (aId != bId) { return aId > bId; }
                            return a.filename > b.filename;
                        }
                    );
                }
                for (auto& level : levels_ | std::views::drop(1)) {
                    std::ranges::sort(level, keyLess, &Meta::firstKey);
                }
            }

            void requestCompaction() {
                if (compactThreads_.empty()) { return; }
                compactRequested_.store(true, std::memory_order_relaxed);
                compactCv_.notify_all();
            }

            void compactionLoop() {
                while (!shuttingDown_.load(std::memory_order_relaxed)) {
                    reclaimRetiredFiles();
                    {
                        std::unique_lock lock{compactMu_};
                        compactCv_.wait_for(
                            lock,
                            std::chrono::milliseconds(50),
                            [this] {
                                return compactRequested_.load(std::memory_order_relaxed) || shuttingDown_.load(std::memory_order_relaxed);
                            }
                        );
                    }
                    if (shuttingDown_.load(std::memory_order_relaxed)) { break; }
                    while (!shuttingDown_.load(std::memory_order_relaxed)) {
                        auto work = pickWork();
                        if (!work) {
                            compactRequested_.store(false, std::memory_order_relaxed);
                            break;
                        }
                        try { runCompaction(std::move(*work)); }
                        catch (...) {
                            recordCompactionFailure(*work, std::current_exception());
                            // Publication may already be durable. Replaying on reopen
                            // must resolve it before these inputs can be compacted again.
                            return;
                        }
                    }
                }
            }

            [[nodiscard]] std::optional<Work> pickWork() {
                std::unique_lock levelsLock{levelsMu_};
                std::lock_guard busyLock{busyMu_};
                if (shuttingDown_.load(std::memory_order_relaxed)) { return std::nullopt; }
                if (compactionFailed_.load(std::memory_order_acquire)) { return std::nullopt; }
                if (!levels_[0].empty() && levels_[0].size() >= static_cast<size_t>(options_.maxL0Files) && !isBusyLocked(0, 1)) {
                    Work w{.src = 0, .dst = 1, .inputs = levels_[0]};
                    // L1 ranges are disjoint. Include only files intersecting an
                    // L0 input; compactOutputs keeps disconnected ranges apart.
                    std::unordered_set<std::string> selected;
                    for (const auto& source : levels_[0]) {
                        const auto first = std::ranges::lower_bound(levels_[1], source.firstKey, keyLess, &Meta::lastKey);
                        for (auto it = first; it != levels_[1].end() && !keyLess(source.lastKey, it->firstKey); ++it) {
                            if (selected.insert(it->filename).second) { w.inputs.push_back(*it); }
                        }
                    }
                    std::vector<const Meta*> ordered;
                    for (const auto& input : w.inputs) { ordered.push_back(&input); }
                    std::ranges::sort(ordered, keyLess, [](const Meta* meta) { return std::span{meta->firstKey}; });
                    std::span<const uint8_t> last = ordered.front()->lastKey;
                    for (const auto* input : ordered | std::views::drop(1)) {
                        if (keyLess(last, input->firstKey)) {
                            const auto gap = std::ranges::upper_bound(levels_[1], last, keyLess, &Meta::firstKey);
                            if (gap != levels_[1].end() && keyLess(gap->firstKey, input->firstKey)) {
                                w.splitBefore.push_back(input->firstKey);
                            }
                            last = input->lastKey;
                        }
                        else if (keyLess(last, input->lastKey)) { last = input->lastKey; }
                    }
                    busyWork_.push_back({.src = w.src, .dst = w.dst});
                    return w;
                }

                for (auto level = 1; level + 1 < options_.maxLevels; ++level) {
                    if (isBusyLocked(level, level + 1)) { continue; }
                    if (levelBytesLocked(level) <= levelBudget(level) || levels_[static_cast<size_t>(level)].empty()) { continue; }

                    const auto srcIt = std::ranges::min_element(levels_[static_cast<size_t>(level)], {}, &Meta::maxSeq);
                    Work w{.src = level, .dst = level + 1, .inputs = {*srcIt}};
                    for (const auto& m : levels_[static_cast<size_t>(level + 1)]) {
                        if (compareBytes(m.lastKey, srcIt->firstKey) >= 0 && compareBytes(m.firstKey, srcIt->lastKey) <= 0) {
                            w.inputs.push_back(m);
                        }
                    }
                    busyWork_.push_back({.src = w.src, .dst = w.dst});
                    return w;
                }
                return std::nullopt;
            }

            [[nodiscard]] std::vector<Meta> flushOutputChunk(std::vector<SSTRecord>& records, int dst) {
                if (records.empty()) { return {}; }

                const auto views = records | std::views::transform(toView) | std::ranges::to<std::vector>();

                const auto path = makeFilePath(dst);
                const auto tmp = path.string() + ".tmp";
                SSTWriter::Options wopts;
                wopts.level = dst;
                wopts.blockSize = options_.blockSize;
                wopts.bloomBitsPerKey = options_.bloomBitsPerKey;
                wopts.codec = options_.codec;
                wopts.zstdCompressionLevel = options_.zstdCompressionLevel;
                wopts.trackBlobReferences = options_.trackBlobReferences;
                const auto result = SSTWriter::write(tmp, views, wopts);
                crashAtTestPoint("sst.compaction.after_output_tmp_write");
                durableRename(tmp, path);
                crashAtTestPoint("sst.compaction.after_output_rename");
                auto reader = SSTReader::open(path, readerOptions());
                if (!reader) { throw std::runtime_error("SSTManager: cannot reopen compacted SST"); }

                manifest_->sstBlobRefs(path.filename().string(), toManifestBlobRefs(result.blobRefs));

                std::vector<Meta> outputs;
                outputs.push_back(makeMeta(path, path.filename().string(), std::move(reader)));
                records.clear();
                return outputs;
            }

            [[nodiscard]] std::vector<Meta> compactOutputs(const Work& work) {
                const auto visibilitySeq = compactionVisibilitySeq_ ? compactionVisibilitySeq_() : UINT64_MAX;
                struct Source {
                    core::ArenaGenerator<SSTRecord> rows;
                    core::ArenaGenerator<SSTRecord>::iterator it;
                };
                struct HeapEntry {
                    SSTRecord rec;
                    size_t sourceIdx = 0;
                };
                struct HeapGreater {
                    bool operator()(const HeapEntry& a, const HeapEntry& b) const noexcept {
                        const auto c = compareBytes(a.rec.key, b.rec.key);
                        if (c != 0) { return c > 0; }
                        return a.rec.seq < b.rec.seq;
                    }
                };

                auto pushCurrent = [](
                    std::priority_queue<HeapEntry, std::vector<HeapEntry>, HeapGreater>& heap,
                    std::vector<Source>& sources,
                    size_t idx
                ) {
                    auto& source = sources[idx];
                    if (source.it == std::default_sentinel) { return; }
                    heap.push(HeapEntry{*source.it, idx});
                    ++source.it;
                };

                std::vector<Source> sources;
                sources.reserve(work.inputs.size());
                std::priority_queue<HeapEntry, std::vector<HeapEntry>, HeapGreater> heap;
                for (const auto& m : work.inputs) {
                    if (!m.reader) { continue; }
                    Source source;
                    source.rows = m.reader->scan();
                    source.it = source.rows.begin();
                    const auto sourceIdx = sources.size();
                    sources.push_back(std::move(source));
                    pushCurrent(heap, sources, sourceIdx);
                }

                std::vector<Meta> outputs;
                std::vector<SSTRecord> chunk;
                std::vector<SSTRecord> retained;
                uint64_t chunkBytes = 0;
                size_t boundary = 0;
                // A lower-sequence flush can arrive even after bottom-level compaction.
                // Retain tombstones until a future reclamation policy can prove that
                // no older generation or promotion can be published.

                while (!heap.empty()) {
                    const auto key = heap.top().rec.key;
                    // Preserve untouched destination ranges without producing
                    // tiny outputs for gaps that contain no destination file.
                    if (boundary < work.splitBefore.size() && !keyLess(key, work.splitBefore[boundary])) {
                        auto flushed = flushOutputChunk(chunk, work.dst);
                        outputs.append_range(flushed | std::views::as_rvalue);
                        chunkBytes = 0;
                        do { ++boundary; } while (boundary < work.splitBefore.size() && !keyLess(key, work.splitBefore[boundary]));
                    }
                    retained.clear();
                    std::optional<SSTRecord> visible;
                    do {
                        auto [next, nextSourceIdx] = heap.top();
                        heap.pop();
                        pushCurrent(heap, sources, nextSourceIdx);
                        if (next.seq > visibilitySeq) { retained.push_back(std::move(next)); }
                        else if (!visible || next.seq > visible->seq) { visible = std::move(next); }
                    } while (!heap.empty() && compareBytes(heap.top().rec.key, key) == 0);
                    if (visible) { retained.push_back(std::move(*visible)); }
                    std::ranges::sort(retained, std::greater{}, &SSTRecord::seq);
                    const auto duplicates = std::ranges::unique(retained, {}, &SSTRecord::seq);
                    retained.erase(duplicates.begin(), duplicates.end());

                    const auto bytes = std::ranges::fold_left(retained | std::views::transform(recordBytes), uint64_t{0}, std::plus{});
                    // Higher levels require disjoint key ranges: never split a
                    // retained key's versions across output files, even if oversized.
                    if (!chunk.empty() && chunkBytes + bytes > options_.targetFileSize) {
                        auto flushed = flushOutputChunk(chunk, work.dst);
                        outputs.append_range(flushed | std::views::as_rvalue);
                        chunkBytes = 0;
                    }
                    chunkBytes += bytes;
                    chunk.append_range(retained | std::views::as_rvalue);
                }

                if (!chunk.empty()) {
                    auto flushed = flushOutputChunk(chunk, work.dst);
                    outputs.append_range(flushed | std::views::as_rvalue);
                }
                return outputs;
            }

            void runCompaction(Work work) {
                const auto bytesIn = std::ranges::fold_left(work.inputs | std::views::transform(&Meta::file_size), uint64_t{0}, std::plus{});

                const auto inputFiles = work.inputs | std::views::transform(&Meta::filename) | std::ranges::to<std::vector>();
                manifest_->compactionStart(work.src, inputFiles);

                auto outputs = compactOutputs(work);

                std::vector<std::string> outputFiles;
                uint64_t bytesOut = 0;
                outputFiles.reserve(outputs.size());
                for (const auto& m : outputs) {
                    outputFiles.push_back(m.filename);
                    bytesOut += m.file_size;
                }

                manifest_->compactionCommit(outputFiles, inputFiles);
                crashAtTestPoint("sst.compaction.after_manifest_commit");

                {
                    std::unique_lock lock{levelsMu_};
                    const auto inputSet = std::unordered_set(std::from_range, inputFiles);
                    for (auto& level : levels_) { std::erase_if(level, [&](const Meta& m) { return inputSet.contains(m.filename); }); }
                    auto& dstLevel = levels_[static_cast<size_t>(work.dst)];
                    dstLevel.append_range(outputs | std::views::as_rvalue);
                    sortAllLevelsLocked();
                    publishLocked();
                }

                for (auto&& [i, input] : std::views::enumerate(work.inputs)) {
                    // A cached descriptor can be evicted and reopened. Keep the
                    // path until the last snapshot/iterator releases its reader.
                    { std::lock_guard lock{retiredMu_}; retiredFiles_.push_back(input.path); }
                    input.reader->retire();
                    input.reader.reset();
                    if (i == 0) { crashAtTestPoint("sst.compaction.after_first_input_delete"); }
                }
                syncDirectory(sstDir_);
                crashAtTestPoint("sst.compaction.after_input_delete");

                compactionsCompleted_.fetch_add(1, std::memory_order_relaxed);
                filesCompacted_.fetch_add(work.inputs.size(), std::memory_order_relaxed);
                bytesCompactedIn_.fetch_add(bytesIn, std::memory_order_relaxed);
                bytesCompactedOut_.fetch_add(bytesOut, std::memory_order_relaxed);

                {
                    std::lock_guard lock{busyMu_};
                    std::erase_if(busyWork_, [&](const BusyWork& item) { return item.src == work.src && item.dst == work.dst; });
                }
                requestCompaction();
            }

            void recordCompactionFailure(const Work& work, const std::exception_ptr& failure) noexcept {
                compactionFailures_.fetch_add(1, std::memory_order_relaxed);
                {
                    std::lock_guard lock{errorMu_};
                    if (!backgroundError_) { backgroundError_ = failure; }
                }
                compactionFailed_.store(true, std::memory_order_release);
                {
                    std::lock_guard lock{busyMu_};
                    std::erase_if(busyWork_, [&](const BusyWork& item) { return item.src == work.src && item.dst == work.dst; });
                }
            }

            [[nodiscard]] bool isBusyLocked(int src, int dst) const {
                return std::ranges::any_of(busyWork_, [=](const BusyWork& item) { return levelsOverlap(src, dst, item.src, item.dst); });
            }

            void reclaimRetiredFiles() {
                std::lock_guard lock{retiredMu_};
                const auto attempts = std::min<size_t>(retiredFiles_.size(), 64);
                for (size_t i = 0; i < attempts; ++i) {
                    const auto file = retiredFiles_.begin();
                    std::error_code error;
                    const auto pinned = SSTReader::isPinned(*file);
                    if (!pinned) { std::filesystem::remove(*file, error); }
                    if (pinned || error) { retiredFiles_.splice(retiredFiles_.end(), retiredFiles_, file); }
                    else { retiredFiles_.erase(file); }
                }
            }

            std::filesystem::path sstDir_;
            Options options_;
            std::shared_ptr<SSTReader::BlockCache> blockCache_;
            std::unique_ptr<detail::StorageFileLock> directoryLock_;
            std::unique_ptr<manifest::Manifest> ownedManifest_;
            manifest::Manifest* manifest_;
            std::function<uint64_t()> compactionVisibilitySeq_;
            bool catalogBound_ = false;

            mutable std::shared_mutex levelsMu_;
            Levels levels_;
            std::atomic<std::shared_ptr<const ReadState>> snapshot_;
            std::atomic<uint64_t> nextFileId_{1};

            std::vector<std::thread> compactThreads_;
            std::thread cleanupThread_;
            mutable std::mutex compactMu_;
            std::condition_variable compactCv_;
            std::atomic<bool> compactRequested_{false};
            std::atomic<bool> shuttingDown_{false};
            std::mutex busyMu_;
            std::vector<BusyWork> busyWork_;
            std::mutex retiredMu_;
            std::list<std::filesystem::path> retiredFiles_;

            std::atomic<uint64_t> compactionsCompleted_{0};
            std::atomic<uint64_t> filesCompacted_{0};
            std::atomic<uint64_t> bytesCompactedIn_{0};
            std::atomic<uint64_t> bytesCompactedOut_{0};
            std::atomic<uint64_t> compactionFailures_{0};
            std::atomic<bool> compactionFailed_{false};
            mutable std::mutex errorMu_;
            std::exception_ptr backgroundError_;
    };

    SSTManager::Iterator::Iterator() = default;
    SSTManager::Iterator::Iterator(std::unique_ptr<Impl> impl) : impl_{std::move(impl)} {}
    SSTManager::Iterator::~Iterator() = default;
    SSTManager::Iterator::Iterator(Iterator&&) noexcept = default;
    SSTManager::Iterator& SSTManager::Iterator::operator=(Iterator&&) noexcept = default;
    bool SSTManager::Iterator::hasNext() const noexcept { return impl_ && impl_->hasNext(); }

    std::optional<SSTRecord> SSTManager::Iterator::get(std::span<const uint8_t> key) const {
        return impl_ ? impl_->get(key) : std::nullopt;
    }
    SSTManager::Iterator SSTManager::Iterator::fork(std::span<const uint8_t> start, std::span<const uint8_t> end) const {
        return impl_ ? Iterator{impl_->fork(start, end)} : Iterator{};
    }

    std::optional<SSTRecord> SSTManager::Iterator::next() {
        if (!hasNext()) { return std::nullopt; }
        return impl_->next();
    }

    std::unique_ptr<SSTManager> SSTManager::create(std::filesystem::path sstDir, const Options& options, manifest::Manifest* manifest) {
        return create(std::move(sstDir), options, manifest, {});
    }

    std::unique_ptr<SSTManager> SSTManager::create(std::filesystem::path sstDir, const Options& options,
        manifest::Manifest* manifest, std::function<uint64_t()> compactionVisibilitySeq) {
        return std::unique_ptr<SSTManager>(new SSTManager(std::move(sstDir), options, manifest, std::move(compactionVisibilitySeq)));
    }

    SSTManager::SSTManager(std::filesystem::path sstDir, Options options, manifest::Manifest* manifest,
        std::function<uint64_t()> compactionVisibilitySeq)
        : impl_{std::make_unique<Impl>(std::move(sstDir), options, manifest, std::move(compactionVisibilitySeq))} {}

    SSTManager::~SSTManager() = default;
    void SSTManager::recover() { impl_->recover(); }
    void SSTManager::shutdown() { impl_->shutdown(); }
    uint64_t SSTManager::flush(std::span<const core::RecordView> records) { return impl_->flush(records); }

    uint64_t SSTManager::flush(size_t estimatedRecordCount, core::ArenaGenerator<core::RecordView> records) {
        return impl_->flush(estimatedRecordCount, std::move(records));
    }

    void SSTManager::throwIfBackgroundFailed() const { impl_->throwIfBackgroundFailed(); }

    SSTManager::ReadSnapshot SSTManager::readSnapshot() const { return impl_->readSnapshot(); }

    std::optional<SSTRecord> SSTManager::get(std::span<const uint8_t> key, uint64_t snapshotSeq) const {
        return impl_->get(key, snapshotSeq);
    }

    std::optional<bool> SSTManager::contains(std::span<const uint8_t> key, uint64_t snapshotSeq) const {
        return impl_->contains(key, snapshotSeq);
    }

    std::optional<bool> SSTManager::getInto(std::span<const uint8_t> key, std::vector<uint8_t>& out, uint64_t snapshotSeq) const {
        return impl_->getInto(key, out, snapshotSeq);
    }

    SSTManager::Iterator SSTManager::scanIter(
        std::span<const uint8_t> startKey,
        std::span<const uint8_t> endKey,
        uint64_t snapshotSeq,
        bool includeTombstones
    ) const { return impl_->scanIter(startKey, endKey, snapshotSeq, includeTombstones); }

    std::vector<SSTManager::LevelStats> SSTManager::levelStats() const { return impl_->levelStats(); }
    uint64_t SSTManager::maxSequence() const noexcept { return impl_->maxSequence(); }
    bool SSTManager::compactionPending() const noexcept { return impl_->compactionPending(); }
    SSTManager::CompactionSnapshot SSTManager::compactionSnapshot() const noexcept { return impl_->compactionSnapshot(); }
    SSTReader::CacheStats SSTManager::blockCacheStats() const { return impl_->blockCacheStats(); }
} // namespace akkaradb::engine::sst

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
#include <condition_variable>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <format>
#include <fstream>
#include <functional>
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

        [[nodiscard]] core::ArenaGenerator<core::RecordView> continueRecords(
            core::ArenaGenerator<core::RecordView> records,
            core::ArenaGenerator<core::RecordView>::iterator position
        ) {
            // begin() resumes this single-pass generator; retain its primed cursor
            // and ownership instead of calling begin() again and skipping a record.
            for (; position != records.end(); ++position) { co_yield *position; }
        }

        // Test-only fault injection. Each point is reached only after the preceding
        // persistence step completed, so recovery tests exercise real crash boundaries.
        void crashAtTestPoint(const char* point) noexcept {
            const auto* requested = std::getenv("AKKARADB_TEST_CRASH_POINT");
            if (requested != nullptr && std::strcmp(requested, point) == 0) { std::quick_exit(86); }
        }
    } // namespace

    class SSTManager::Iterator::Impl {
        public:
            Impl(
                std::vector<std::shared_ptr<SSTReader>> readers,
                std::span<const uint8_t> startKey,
                std::span<const uint8_t> endKey,
                uint64_t snapshotSeq,
                bool includeTombstones
            )
                : snapshotSeq_{snapshotSeq}, includeTombstones_{includeTombstones}, startKey_{std::from_range, startKey}, endKey_{std::from_range, endKey} {
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
                std::optional<SSTRecord> result;
                for (const auto& source : sources_) {
                    auto record = source.reader->get(key, snapshotSeq_);
                    if (record && (!result || record->seq > result->seq)) { result = std::move(record); }
                }
                return result;
            }
            [[nodiscard]] std::unique_ptr<Impl> fork(std::span<const uint8_t> start, std::span<const uint8_t> end) const {
                auto readers = sources_ | std::views::transform(&Source::reader) | std::ranges::to<std::vector>();
                return std::make_unique<Impl>(std::move(readers), start, end, snapshotSeq_, includeTombstones_);
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
                heap_.push(HeapEntry{*source.it, sourceIdx});
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

            uint64_t snapshotSeq_;
            bool includeTombstones_;
            std::vector<uint8_t> startKey_;
            std::vector<uint8_t> endKey_;
            std::vector<Source> sources_;
            std::priority_queue<HeapEntry, std::vector<HeapEntry>, HeapGreater> heap_;
            std::optional<SSTRecord> pending_;
    };

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
    };

    std::optional<SSTRecord> SSTManager::ReadSnapshot::get(std::span<const uint8_t> key, uint64_t snapshotSeq) const {
        if (!state_) { return std::nullopt; }
        const auto& levels = state_->levels;
        std::optional<SSTRecord> best;
        const auto consider = [&](const ReadState::Meta& meta) {
            if (!meta.reader || meta.minSeq > snapshotSeq || (best && meta.maxSeq <= best->seq)) { return; }
            auto candidate = meta.reader->get(key, snapshotSeq);
            if (candidate && (!best || candidate->seq > best->seq)) { best = std::move(candidate); }
        };
        // Flush completion and file ids do not imply sequence precedence.
        if (!levels.empty()) { for (const auto& meta : levels[0]) { consider(meta); } }
        for (const auto& files : levels | std::views::drop(1)) {
            const auto candidate = std::ranges::lower_bound(files, key, keyLess, &ReadState::Meta::lastKey);
            if (candidate != files.end() && compareBytes(candidate->firstKey, key) <= 0 && compareBytes(key, candidate->lastKey) <= 0) {
                consider(*candidate);
            }
        }
        return best;
    }

    uint64_t SSTManager::ReadSnapshot::maxSequence() const noexcept {
        if (!state_) { return 0; }
        const auto sequences = state_->levels | std::views::join | std::views::transform(&ReadState::Meta::maxSeq);
        return std::ranges::fold_left(sequences, uint64_t{0}, std::ranges::max);
    }

    class SSTManager::Impl {
        public:
            using Meta = ReadState::Meta;
            using Levels = ReadState::Levels;

            struct Work {
                int src = 0;
                int dst = 1;
                std::vector<Meta> inputs;
            };

            struct BusyWork {
                int src = 0;
                int dst = 0;
            };

            Impl(std::filesystem::path sstDir, const Options& options, manifest::Manifest* manifest)
                : sstDir_{std::move(sstDir)}, options_{options}, manifest_{manifest} {
                if (options_.maxLevels < 2) { options_.maxLevels = 2; }
                if (sstDir_.empty()) { throw std::invalid_argument("SSTManager: sstDir is required"); }
                validateZstdCompressionLevel(options_);
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
                    for (int i = 0; i < n; ++i) { compactThreads_.emplace_back([this] { compactionLoop(); }); }
                }
            }

            ~Impl() { shutdown(); }

            void recover() {
                std::unique_lock lock{levelsMu_};
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
                for (const auto& abandoned : abandonedFiles) { removeFileDurable(abandoned); }
                levels_ = std::move(recovered);
                nextFileId_ = 1;
                for (const auto& file : allSstFiles) { observeFileId(file); }
                for (const auto& file : files) { observeFileId(file); }
                sortAllLevelsLocked();
                publishLocked();
                requestCompaction();
            }

            void shutdown() {
                bool expected = false;
                if (!shuttingDown_.compare_exchange_strong(expected, true)) { return; }
                compactCv_.notify_all();
                for (auto& t : compactThreads_) { if (t.joinable()) { t.join(); } }
                compactThreads_.clear();
            }

            uint64_t flush(std::span<const core::RecordView> records) {
                throwIfBackgroundFailed();
                requireCatalogBinding();
                if (records.empty()) { return 0; }
                const auto path = makeFilePath(0);
                const auto tmp = path.string() + ".tmp";

                SSTWriter::Options wopts;
                wopts.level = 0;
                wopts.blockSize = options_.blockSize;
                wopts.targetFileSize = options_.targetFileSize;
                wopts.bloomBitsPerKey = options_.bloomBitsPerKey;
                wopts.codec = options_.codec;
                wopts.zstdCompressionLevel = options_.zstdCompressionLevel;
                wopts.trackBlobReferences = options_.trackBlobReferences;

                const auto result = SSTWriter::write(tmp, records, wopts);
                crashAtTestPoint("sst.flush.after_tmp_write");
                durableRename(tmp, path);
                crashAtTestPoint("sst.flush.after_rename");
                auto reader = SSTReader::open(path, readerOptions());
                if (!reader) { throw std::runtime_error("SSTManager: cannot reopen flushed SST"); }
                auto meta = makeMeta(path, path.filename().string(), std::move(reader));

                manifest_->sstBlobRefs(meta.filename, toManifestBlobRefs(result.blobRefs));
                manifest_->sstSeal(0, meta.filename, meta.entryCount, hexKey(meta.firstKey), hexKey(meta.lastKey));
                crashAtTestPoint("sst.flush.after_manifest_seal");

                {
                    std::unique_lock lock{levelsMu_};
                    levels_[0].insert(levels_[0].begin(), std::move(meta));
                    publishLocked();
                }
                requestCompaction();
                return result.maxSeq;
            }

            uint64_t flush(size_t estimatedRecordCount, core::ArenaGenerator<core::RecordView> records) {
                throwIfBackgroundFailed();
                requireCatalogBinding();
                const auto first = records.begin();
                if (first == records.end()) { return 0; }
                const auto path = makeFilePath(0);
                const auto tmp = path.string() + ".tmp";

                SSTWriter::Options wopts;
                wopts.level = 0;
                wopts.blockSize = options_.blockSize;
                wopts.targetFileSize = options_.targetFileSize;
                wopts.bloomBitsPerKey = options_.bloomBitsPerKey;
                wopts.codec = options_.codec;
                wopts.zstdCompressionLevel = options_.zstdCompressionLevel;
                wopts.trackBlobReferences = options_.trackBlobReferences;

                const auto result = SSTWriter::write(tmp, estimatedRecordCount, continueRecords(std::move(records), first), wopts);
                crashAtTestPoint("sst.flush.after_tmp_write");
                durableRename(tmp, path);
                crashAtTestPoint("sst.flush.after_rename");
                auto reader = SSTReader::open(path, readerOptions());
                if (!reader) { throw std::runtime_error("SSTManager: cannot reopen flushed SST"); }
                auto meta = makeMeta(path, path.filename().string(), std::move(reader));

                manifest_->sstBlobRefs(meta.filename, toManifestBlobRefs(result.blobRefs));
                manifest_->sstSeal(0, meta.filename, meta.entryCount, hexKey(meta.firstKey), hexKey(meta.lastKey));
                crashAtTestPoint("sst.flush.after_manifest_seal");

                {
                    std::unique_lock lock{levelsMu_};
                    levels_[0].insert(levels_[0].begin(), std::move(meta));
                    publishLocked();
                }
                requestCompaction();
                return result.maxSeq;
            }

            [[nodiscard]] std::optional<SSTRecord> get(std::span<const uint8_t> key, uint64_t snapshotSeq) const {
                return readSnapshot().get(key, snapshotSeq);
            }

            [[nodiscard]] ReadSnapshot readSnapshot() const { return ReadSnapshot{snapshot_.load(std::memory_order_acquire)}; }

            [[nodiscard]] std::optional<bool> contains(std::span<const uint8_t> key, uint64_t snapshotSeq) const {
                auto rec = get(key, snapshotSeq);
                if (!rec) { return std::nullopt; }
                return !rec->isTombstone();
            }

            [[nodiscard]] std::optional<bool> getInto(std::span<const uint8_t> key, std::vector<uint8_t>& out, uint64_t snapshotSeq) const {
                auto record = get(key, snapshotSeq);
                if (!record) { return std::nullopt; }
                if (record->isTombstone()) { return false; }
                out = std::move(record->value);
                return true;
            }

            [[nodiscard]] Iterator scanIter(
                std::span<const uint8_t> startKey,
                std::span<const uint8_t> endKey,
                uint64_t snapshotSeq,
                bool includeTombstones
            ) const {
                auto snap = snapshot_.load(std::memory_order_acquire);
                if (!snap) { return Iterator{}; }

                auto readers = snap->levels | std::views::join
                    | std::views::filter([snapshotSeq](const Meta& meta) { return meta.reader && meta.minSeq <= snapshotSeq; })
                    | std::views::transform(&Meta::reader) | std::ranges::to<std::vector>();
                return Iterator{std::make_unique<Iterator::Impl>(std::move(readers), startKey, endKey, snapshotSeq, includeTombstones)};
            }

            [[nodiscard]] std::vector<LevelStats> levelStats() const {
                auto snap = snapshot_.load(std::memory_order_acquire);
                std::vector<LevelStats> out;
                if (!snap) { return out; }
                out.reserve(snap->levels.size());
                for (const auto& [i, level] : std::views::enumerate(snap->levels)) {
                    const auto bytes = std::ranges::fold_left(level | std::views::transform(&Meta::file_size), uint64_t{0}, std::plus{});
                    out.push_back(LevelStats{static_cast<int>(i), level.size(), bytes, i == 0 ? 0 : levelBudget(static_cast<int>(i))});
                }
                return out;
            }

            [[nodiscard]] uint64_t maxSequence() const noexcept {
                return readSnapshot().maxSequence();
            }

            [[nodiscard]] bool compactionPending() const noexcept { return compactRequested_.load(std::memory_order_relaxed); }

            [[nodiscard]] CompactionSnapshot compactionSnapshot() const noexcept {
                return {
                    compactionsCompleted_.load(std::memory_order_relaxed),
                    filesCompacted_.load(std::memory_order_relaxed),
                    bytesCompactedIn_.load(std::memory_order_relaxed),
                    bytesCompactedOut_.load(std::memory_order_relaxed),
                    compactionFailures_.load(std::memory_order_relaxed)
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
                    std::array<uint8_t, 28> bytes;
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

            [[nodiscard]] SSTReader::Options readerOptions() const noexcept { return SSTReader::Options{options_.blockCacheBytes}; }

            [[nodiscard]] uint64_t levelBudget(int level) const {
                if (level <= 0) { return 0; }
                auto budget = static_cast<double>(options_.l1MaxBytes);
                for (int i = 1; i < level; ++i) { budget *= options_.levelSizeMultiplier; }
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
                auto snap = std::make_shared<ReadState>(ReadState{levels_});
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
                    for (;;) {
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
                if (compactionFailed_.load(std::memory_order_acquire)) { return std::nullopt; }
                if (levels_[0].size() >= static_cast<size_t>(options_.maxL0Files) && !isBusyLocked(0, 1)) {
                    Work w{0, 1, levels_[0]};
                    if (levels_.size() > 1) { w.inputs.append_range(levels_[1]); }
                    busyWork_.push_back({w.src, w.dst});
                    return w;
                }

                for (int level = 1; level + 1 < options_.maxLevels; ++level) {
                    if (isBusyLocked(level, level + 1)) { continue; }
                    if (levelBytesLocked(level) <= levelBudget(level) || levels_[static_cast<size_t>(level)].empty()) { continue; }

                    const auto srcIt = std::ranges::min_element(levels_[static_cast<size_t>(level)], {}, &Meta::maxSeq);
                    Work w{level, level + 1, {*srcIt}};
                    for (const auto& m : levels_[static_cast<size_t>(level + 1)]) {
                        if (compareBytes(m.lastKey, srcIt->firstKey) >= 0 && compareBytes(m.firstKey, srcIt->lastKey) <= 0) {
                            w.inputs.push_back(m);
                        }
                    }
                    busyWork_.push_back({w.src, w.dst});
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
                wopts.targetFileSize = options_.targetFileSize;
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
                uint64_t chunkBytes = 0;
                // A lower-sequence flush can arrive even after bottom-level compaction.
                // Retain tombstones until a future reclamation policy can prove that
                // no older generation or promotion can be published.

                while (!heap.empty()) {
                    auto [best, sourceIdx] = heap.top();
                    heap.pop();
                    pushCurrent(heap, sources, sourceIdx);

                    while (!heap.empty() && compareBytes(heap.top().rec.key, best.key) == 0) {
                        auto [next, nextSourceIdx] = heap.top();
                        heap.pop();
                        pushCurrent(heap, sources, nextSourceIdx);
                        if (next.seq > best.seq) { best = std::move(next); }
                    }

                    const auto bytes = recordBytes(best);
                    if (!chunk.empty() && chunkBytes + bytes > options_.targetFileSize) {
                        auto flushed = flushOutputChunk(chunk, work.dst);
                        outputs.append_range(flushed | std::views::as_rvalue);
                        chunkBytes = 0;
                    }
                    chunkBytes += bytes;
                    chunk.push_back(std::move(best));
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
                    // Release our obsolete reader before unlinking (Windows denies
                    // deletion while an ifstream is open). Pinned readers remain valid;
                    // their leftover files are safely retired by catalog recovery.
                    input.reader.reset();
                    removeFileDurable(input.path);
                    if (i == 0) { crashAtTestPoint("sst.compaction.after_first_input_delete"); }
                }
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

            std::filesystem::path sstDir_;
            Options options_;
            std::unique_ptr<detail::StorageFileLock> directoryLock_;
            std::unique_ptr<manifest::Manifest> ownedManifest_;
            manifest::Manifest* manifest_;
            bool catalogBound_ = false;

            mutable std::shared_mutex levelsMu_;
            Levels levels_;
            std::atomic<std::shared_ptr<const ReadState>> snapshot_;
            std::atomic<uint64_t> nextFileId_{1};

            std::vector<std::thread> compactThreads_;
            mutable std::mutex compactMu_;
            std::condition_variable compactCv_;
            std::atomic<bool> compactRequested_{false};
            std::atomic<bool> shuttingDown_{false};
            std::mutex busyMu_;
            std::vector<BusyWork> busyWork_;

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
        return std::unique_ptr<SSTManager>(new SSTManager(std::move(sstDir), options, manifest));
    }

    SSTManager::SSTManager(std::filesystem::path sstDir, Options options, manifest::Manifest* manifest)
        : impl_{std::make_unique<Impl>(std::move(sstDir), options, manifest)} {}

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
} // namespace akkaradb::engine::sst

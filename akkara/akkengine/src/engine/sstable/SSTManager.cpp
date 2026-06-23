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

// akkengine/src/engine/sstable/SSTManager.cpp
#include "akk/engine/sstable/SSTManager.hpp"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <format>
#include <mutex>
#include <queue>
#include <ranges>
#include <shared_mutex>
#include <stdexcept>
#include <thread>
#include <unordered_set>

namespace akkaradb::engine::sst {
    namespace {
        [[nodiscard]] int compareBytes(std::span<const uint8_t> a, std::span<const uint8_t> b) noexcept {
            const size_t n = std::min(a.size(), b.size());
            if (n > 0) {
                const int c = std::memcmp(a.data(), b.data(), n);
                if (c != 0) { return c < 0 ? -1 : 1; }
            }
            if (a.size() < b.size()) { return -1; }
            if (a.size() > b.size()) { return 1; }
            return 0;
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
    } // namespace

    class SSTManager::Iterator::Impl {
        public:
            Impl(std::vector<std::shared_ptr<SSTReader>> readers, std::span<const uint8_t> startKey, std::span<const uint8_t> endKey)
                : startKey_{startKey.begin(), startKey.end()}, endKey_{endKey.begin(), endKey.end()} {
                sources_.reserve(readers.size());
                for (auto& reader : readers) {
                    if (!reader) { continue; }
                    Source source;
                    source.reader = std::move(reader);
                    source.rows = source.reader->scan(startKey_, endKey_);
                    source.it = source.rows.begin();
                    const size_t sourceIdx = sources_.size();
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
                    const int c = compareBytes(a.rec.key, b.rec.key);
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
                    HeapEntry best = heap_.top();
                    heap_.pop();
                    pushCurrent(best.sourceIdx);

                    while (!heap_.empty() && compareBytes(heap_.top().rec.key, best.rec.key) == 0) {
                        HeapEntry next = heap_.top();
                        heap_.pop();
                        pushCurrent(next.sourceIdx);
                        if (next.rec.seq > best.rec.seq) { best.rec = std::move(next.rec); }
                    }

                    if (!best.rec.isTombstone()) {
                        pending_ = std::move(best.rec);
                        return;
                    }
                }
            }

            std::vector<uint8_t> startKey_;
            std::vector<uint8_t> endKey_;
            std::vector<Source> sources_;
            std::priority_queue<HeapEntry, std::vector<HeapEntry>, HeapGreater> heap_;
            std::optional<SSTRecord> pending_;
    };

    class SSTManager::Impl {
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

            struct Work {
                int src = 0;
                int dst = 1;
                std::vector<Meta> inputs;
            };

            struct BusyWork {
                int src = 0;
                int dst = 0;
            };

            Impl(Options options, manifest::Manifest* manifest)
                : options_{std::move(options)}, manifest_{manifest} {
                if (options_.maxLevels < 2) { options_.maxLevels = 2; }
                if (options_.sstDir.empty()) { throw std::invalid_argument("SSTManager: sstDir is required"); }
                std::filesystem::create_directories(options_.sstDir);
                levels_.resize(static_cast<size_t>(options_.maxLevels));
                publishLocked();
                const int n = std::max(1, options_.compactThreads);
                compactThreads_.reserve(static_cast<size_t>(n));
                for (int i = 0; i < n; ++i) { compactThreads_.emplace_back([this] { compactionLoop(); }); }
            }

            ~Impl() { shutdown(); }

            void recover() {
                std::unique_lock lock{levelsMu_};
                for (auto& level : levels_) { level.clear(); }
                nextFileId_ = 1;

                for (const auto& entry : std::filesystem::directory_iterator(options_.sstDir)) {
                    const auto p = entry.path();
                    if (p.extension() == ".tmp") {
                        std::error_code ec;
                        std::filesystem::remove(p, ec);
                    }
                }

                std::vector<std::string> files;
                if (manifest_) { files = manifest_->liveSst(); }
                else {
                    for (const auto& entry : std::filesystem::directory_iterator(options_.sstDir)) {
                        if (entry.path().extension() == ".aksst") { files.push_back(entry.path().filename().string()); }
                    }
                }

                for (const auto& file : files) {
                    const auto path = options_.sstDir / file;
                    auto reader = SSTReader::open(path, readerOptions());
                    if (!reader) { continue; }
                    Meta meta = makeMeta(path, file, std::move(reader));
                    if (meta.level < 0 || meta.level >= options_.maxLevels) { continue; }
                    levels_[static_cast<size_t>(meta.level)].push_back(std::move(meta));
                    const uint64_t observedNext = parseFileId(file) + 1;
                    uint64_t currentNext = nextFileId_.load(std::memory_order_relaxed);
                    while (currentNext < observedNext && !nextFileId_.compare_exchange_weak(
                        currentNext,
                        observedNext,
                        std::memory_order_relaxed,
                        std::memory_order_relaxed
                    )) {}
                }
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
                if (records.empty()) { return 0; }
                const auto path = makeFilePath(0);
                const auto tmp = path.string() + ".tmp";

                SSTWriter::Options wopts;
                wopts.level = 0;
                wopts.blockSize = options_.blockSize;
                wopts.targetFileSize = options_.targetFileSize;
                wopts.bloomBitsPerKey = options_.bloomBitsPerKey;
                wopts.codec = options_.codec;

                const auto result = SSTWriter::write(tmp, records, wopts);
                std::filesystem::rename(tmp, path);
                auto reader = SSTReader::open(path, readerOptions());
                if (!reader) { throw std::runtime_error("SSTManager: cannot reopen flushed SST"); }
                Meta meta = makeMeta(path, path.filename().string(), std::move(reader));

                if (manifest_) { manifest_->sstSeal(0, meta.filename, meta.entryCount, hexKey(meta.firstKey), hexKey(meta.lastKey)); }

                {
                    std::unique_lock lock{levelsMu_};
                    levels_[0].insert(levels_[0].begin(), std::move(meta));
                    publishLocked();
                }
                requestCompaction();
                return result.maxSeq;
            }

            [[nodiscard]] std::optional<SSTRecord> get(std::span<const uint8_t> key) const {
                auto snap = snapshot_.load(std::memory_order_acquire);
                if (!snap) { return std::nullopt; }

                if (!snap->empty()) {
                    for (const auto& m : (*snap)[0]) {
                        if (m.reader) {
                            auto rec = m.reader->get(key);
                            if (rec) { return rec; }
                        }
                    }
                }

                for (size_t level = 1; level < snap->size(); ++level) {
                    const auto& files = (*snap)[level];
                    size_t lo = 0;
                    size_t hi = files.size();
                    while (lo < hi) {
                        const size_t mid = lo + (hi - lo) / 2;
                        if (compareBytes(files[mid].lastKey, key) < 0) { lo = mid + 1; }
                        else { hi = mid; }
                    }
                    if (lo < files.size() && compareBytes(files[lo].firstKey, key) <= 0 && compareBytes(key, files[lo].lastKey) <= 0 &&
                        files[lo].reader) {
                        auto rec = files[lo].reader->get(key);
                        if (rec) { return rec; }
                    }
                }
                return std::nullopt;
            }

            [[nodiscard]] std::optional<bool> contains(std::span<const uint8_t> key) const {
                auto rec = get(key);
                if (!rec) { return std::nullopt; }
                return !rec->isTombstone();
            }

            [[nodiscard]] std::optional<bool> getInto(std::span<const uint8_t> key, std::vector<uint8_t>& out) const {
                auto snap = snapshot_.load(std::memory_order_acquire);
                if (!snap) { return std::nullopt; }

                if (!snap->empty()) {
                    for (const auto& m : (*snap)[0]) {
                        if (!m.reader) { continue; }
                        auto hit = m.reader->getInto(key, out);
                        if (hit.has_value()) { return hit; }
                    }
                }

                for (size_t level = 1; level < snap->size(); ++level) {
                    const auto& files = (*snap)[level];
                    size_t lo = 0;
                    size_t hi = files.size();
                    while (lo < hi) {
                        const size_t mid = lo + (hi - lo) / 2;
                        if (compareBytes(files[mid].lastKey, key) < 0) { lo = mid + 1; }
                        else { hi = mid; }
                    }
                    if (lo >= files.size()) { continue; }
                    if (compareBytes(files[lo].firstKey, key) > 0 || compareBytes(key, files[lo].lastKey) > 0 || !files[lo].reader) {
                        continue;
                    }
                    auto hit = files[lo].reader->getInto(key, out);
                    if (hit.has_value()) { return hit; }
                }
                return std::nullopt;
            }

            [[nodiscard]] Iterator scanIter(std::span<const uint8_t> startKey, std::span<const uint8_t> endKey) const {
                auto snap = snapshot_.load(std::memory_order_acquire);
                if (!snap) { return Iterator{}; }

                std::vector<std::shared_ptr<SSTReader>> readers;
                for (const auto& level : *snap) { for (const auto& meta : level) { if (meta.reader) { readers.push_back(meta.reader); } } }
                return Iterator{std::make_unique<Iterator::Impl>(std::move(readers), startKey, endKey)};
            }

            [[nodiscard]] std::vector<LevelStats> levelStats() const {
                auto snap = snapshot_.load(std::memory_order_acquire);
                std::vector<LevelStats> out;
                if (!snap) { return out; }
                out.reserve(snap->size());
                for (size_t i = 0; i < snap->size(); ++i) {
                    uint64_t bytes = 0;
                    for (const auto& m : (*snap)[i]) { bytes += m.file_size; }
                    out.push_back(LevelStats{static_cast<int>(i), (*snap)[i].size(), bytes, i == 0 ? 0 : levelBudget(static_cast<int>(i))});
                }
                return out;
            }

            [[nodiscard]] bool compactionPending() const noexcept { return compactRequested_.load(std::memory_order_relaxed); }

            [[nodiscard]] CompactionSnapshot compactionSnapshot() const noexcept {
                return {
                    compactionsCompleted_.load(std::memory_order_relaxed),
                    filesCompacted_.load(std::memory_order_relaxed),
                    bytesCompactedIn_.load(std::memory_order_relaxed),
                    bytesCompactedOut_.load(std::memory_order_relaxed)
                };
            }

        private:
            [[nodiscard]] SSTReader::Options readerOptions() const noexcept { return SSTReader::Options{options_.blockCacheBytes}; }

            [[nodiscard]] uint64_t levelBudget(int level) const {
                if (level <= 0) { return 0; }
                double budget = static_cast<double>(options_.l1MaxBytes);
                for (int i = 1; i < level; ++i) { budget *= options_.levelSizeMultiplier; }
                return static_cast<uint64_t>(budget);
            }

            [[nodiscard]] uint64_t levelBytesLocked(int level) const {
                uint64_t total = 0;
                for (const auto& m : levels_[static_cast<size_t>(level)]) { total += m.file_size; }
                return total;
            }

            [[nodiscard]] std::filesystem::path makeFilePath(int level) {
                const uint64_t id = nextFileId_.fetch_add(1, std::memory_order_relaxed);
                return options_.sstDir / std::format("L{}_{}.aksst", level, id);
            }

            [[nodiscard]] static uint64_t parseFileId(const std::string& file) noexcept {
                const auto us = file.find('_');
                const auto dot = file.find('.', us == std::string::npos ? 0 : us);
                if (us == std::string::npos || dot == std::string::npos || dot <= us + 1) { return 0; }
                try { return std::stoull(file.substr(us + 1, dot - us - 1)); }
                catch (...) { return 0; }
            }

            [[nodiscard]] Meta makeMeta(
                const std::filesystem::path& path,
                const std::string& filename,
                std::unique_ptr<SSTReader> reader
            ) const {
                std::shared_ptr<SSTReader> shared{std::move(reader)};
                const auto& hdr = shared->header();
                Meta meta;
                meta.path = path;
                meta.filename = filename;
                meta.level = static_cast<int>(hdr.level);
                meta.entryCount = hdr.entryCount;
                meta.file_size = hdr.file_size;
                meta.minSeq = hdr.minSeq;
                meta.maxSeq = hdr.maxSeq;
                meta.firstKey.assign(shared->firstKey().begin(), shared->firstKey().end());
                meta.lastKey.assign(shared->lastKey().begin(), shared->lastKey().end());
                meta.reader = std::move(shared);
                return meta;
            }

            void publishLocked() {
                auto snap = std::make_shared<Levels>(levels_);
                snapshot_.store(std::move(snap), std::memory_order_release);
            }

            void sortAllLevelsLocked() {
                if (!levels_.empty()) {
                    std::sort(levels_[0].begin(), levels_[0].end(), [](const Meta& a, const Meta& b) { return a.filename > b.filename; });
                }
                for (size_t i = 1; i < levels_.size(); ++i) {
                    std::sort(
                        levels_[i].begin(),
                        levels_[i].end(),
                        [](const Meta& a, const Meta& b) { return compareBytes(a.firstKey, b.firstKey) < 0; }
                    );
                }
            }

            void requestCompaction() {
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
                        runCompaction(std::move(*work));
                    }
                }
            }

            [[nodiscard]] std::optional<Work> pickWork() {
                std::unique_lock levelsLock{levelsMu_};
                std::lock_guard busyLock{busyMu_};
                if (levels_[0].size() >= static_cast<size_t>(options_.maxL0Files) && !isBusyLocked(0, 1)) {
                    Work w{0, 1, levels_[0]};
                    if (levels_.size() > 1) { w.inputs.insert(w.inputs.end(), levels_[1].begin(), levels_[1].end()); }
                    busyWork_.push_back({w.src, w.dst});
                    return w;
                }

                for (int level = 1; level + 1 < options_.maxLevels; ++level) {
                    if (isBusyLocked(level, level + 1)) { continue; }
                    if (levelBytesLocked(level) <= levelBudget(level) || levels_[static_cast<size_t>(level)].empty()) { continue; }

                    const auto srcIt = std::min_element(
                        levels_[static_cast<size_t>(level)].begin(),
                        levels_[static_cast<size_t>(level)].end(),
                        [](const Meta& a, const Meta& b) { return a.maxSeq < b.maxSeq; }
                    );
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

                std::vector<core::RecordView> views;
                views.reserve(records.size());
                for (const auto& rec : records) { views.push_back(toView(rec)); }

                const auto path = makeFilePath(dst);
                const auto tmp = path.string() + ".tmp";
                SSTWriter::Options wopts;
                wopts.level = dst;
                wopts.blockSize = options_.blockSize;
                wopts.targetFileSize = options_.targetFileSize;
                wopts.bloomBitsPerKey = options_.bloomBitsPerKey;
                wopts.codec = options_.codec;
                (void)SSTWriter::write(tmp, views, wopts);
                std::filesystem::rename(tmp, path);
                auto reader = SSTReader::open(path, readerOptions());
                if (!reader) { throw std::runtime_error("SSTManager: cannot reopen compacted SST"); }

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
                        const int c = compareBytes(a.rec.key, b.rec.key);
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
                    const size_t sourceIdx = sources.size();
                    sources.push_back(std::move(source));
                    pushCurrent(heap, sources, sourceIdx);
                }

                std::vector<Meta> outputs;
                std::vector<SSTRecord> chunk;
                uint64_t chunkBytes = 0;
                const bool dropTombstones = work.dst == options_.maxLevels - 1;

                while (!heap.empty()) {
                    HeapEntry best = heap.top();
                    heap.pop();
                    pushCurrent(heap, sources, best.sourceIdx);

                    while (!heap.empty() && compareBytes(heap.top().rec.key, best.rec.key) == 0) {
                        HeapEntry next = heap.top();
                        heap.pop();
                        pushCurrent(heap, sources, next.sourceIdx);
                        if (next.rec.seq > best.rec.seq) { best.rec = std::move(next.rec); }
                    }

                    if (dropTombstones && best.rec.isTombstone()) { continue; }

                    const uint64_t bytes = recordBytes(best.rec);
                    if (!chunk.empty() && chunkBytes + bytes > options_.targetFileSize) {
                        auto flushed = flushOutputChunk(chunk, work.dst);
                        outputs.insert(outputs.end(), std::make_move_iterator(flushed.begin()), std::make_move_iterator(flushed.end()));
                        chunkBytes = 0;
                    }
                    chunkBytes += bytes;
                    chunk.push_back(std::move(best.rec));
                }

                if (!chunk.empty()) {
                    auto flushed = flushOutputChunk(chunk, work.dst);
                    outputs.insert(outputs.end(), std::make_move_iterator(flushed.begin()), std::make_move_iterator(flushed.end()));
                }
                return outputs;
            }

            void runCompaction(Work work) {
                uint64_t bytesIn = 0;
                for (const auto& m : work.inputs) { bytesIn += m.file_size; }

                std::vector<std::string> inputFiles;
                inputFiles.reserve(work.inputs.size());
                for (const auto& m : work.inputs) { inputFiles.push_back(m.filename); }
                if (manifest_) { manifest_->compactionStart(work.src, inputFiles); }

                std::vector<Meta> outputs = compactOutputs(work);

                std::vector<std::string> outputFiles;
                uint64_t bytesOut = 0;
                outputFiles.reserve(outputs.size());
                for (const auto& m : outputs) {
                    outputFiles.push_back(m.filename);
                    bytesOut += m.file_size;
                }

                if (manifest_) { manifest_->compactionCommit(outputFiles, inputFiles); }

                {
                    std::unique_lock lock{levelsMu_};
                    std::unordered_set<std::string> inputSet(inputFiles.begin(), inputFiles.end());
                    for (auto& level : levels_) { std::erase_if(level, [&](const Meta& m) { return inputSet.count(m.filename) != 0; }); }
                    auto& dstLevel = levels_[static_cast<size_t>(work.dst)];
                    dstLevel.insert(dstLevel.end(), std::make_move_iterator(outputs.begin()), std::make_move_iterator(outputs.end()));
                    sortAllLevelsLocked();
                    publishLocked();
                }

                for (const auto& m : work.inputs) {
                    std::error_code ec;
                    std::filesystem::remove(m.path, ec);
                }

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

            [[nodiscard]] bool isBusyLocked(int src, int dst) const {
                return std::ranges::any_of(busyWork_, [=](const BusyWork& item) { return levelsOverlap(src, dst, item.src, item.dst); });
            }

            Options options_;
            manifest::Manifest* manifest_;

            mutable std::shared_mutex levelsMu_;
            Levels levels_;
            std::atomic<std::shared_ptr<const Levels>> snapshot_;
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
    };

    SSTManager::Iterator::Iterator() = default;
    SSTManager::Iterator::Iterator(std::unique_ptr<Impl> impl) : impl_{std::move(impl)} {}
    SSTManager::Iterator::~Iterator() = default;
    SSTManager::Iterator::Iterator(Iterator&&) noexcept = default;
    SSTManager::Iterator& SSTManager::Iterator::operator=(Iterator&&) noexcept = default;
    bool SSTManager::Iterator::hasNext() const noexcept { return impl_ && impl_->hasNext(); }

    std::optional<SSTRecord> SSTManager::Iterator::next() {
        if (!hasNext()) { return std::nullopt; }
        return impl_->next();
    }

    std::unique_ptr<SSTManager> SSTManager::create(Options options, manifest::Manifest* manifest) {
        return std::unique_ptr<SSTManager>(new SSTManager(std::move(options), manifest));
    }

    SSTManager::SSTManager(Options options, manifest::Manifest* manifest) : impl_{std::make_unique<Impl>(std::move(options), manifest)} {}

    SSTManager::~SSTManager() = default;
    void SSTManager::recover() { impl_->recover(); }
    void SSTManager::shutdown() { impl_->shutdown(); }
    uint64_t SSTManager::flush(std::span<const core::RecordView> records) { return impl_->flush(records); }
    std::optional<SSTRecord> SSTManager::get(std::span<const uint8_t> key) const { return impl_->get(key); }
    std::optional<bool> SSTManager::contains(std::span<const uint8_t> key) const { return impl_->contains(key); }

    std::optional<bool> SSTManager::getInto(std::span<const uint8_t> key, std::vector<uint8_t>& out) const {
        return impl_->getInto(key, out);
    }

    SSTManager::Iterator SSTManager::scanIter(std::span<const uint8_t> startKey, std::span<const uint8_t> endKey) const {
        return impl_->scanIter(startKey, endKey);
    }

    std::vector<SSTManager::LevelStats> SSTManager::levelStats() const { return impl_->levelStats(); }
    bool SSTManager::compactionPending() const noexcept { return impl_->compactionPending(); }
    SSTManager::CompactionSnapshot SSTManager::compactionSnapshot() const noexcept { return impl_->compactionSnapshot(); }
} // namespace akkaradb::engine::sst

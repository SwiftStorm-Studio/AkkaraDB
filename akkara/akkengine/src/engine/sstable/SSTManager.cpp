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
#include <set>
#include <shared_mutex>
#include <stdexcept>
#include <thread>
#include <unordered_set>

namespace akkaradb::engine::sst {
    namespace {
        [[nodiscard]] int compare_bytes(std::span<const uint8_t> a, std::span<const uint8_t> b) noexcept {
            const size_t n = std::min(a.size(), b.size());
            if (n > 0) {
                const int c = std::memcmp(a.data(), b.data(), n);
                if (c != 0) { return c < 0 ? -1 : 1; }
            }
            if (a.size() < b.size()) { return -1; }
            if (a.size() > b.size()) { return 1; }
            return 0;
        }

        [[nodiscard]] std::string hex_key(std::span<const uint8_t> key) {
            static constexpr char kHex[] = "0123456789abcdef";
            std::string out;
            out.resize(key.size() * 2);
            for (size_t i = 0; i < key.size(); ++i) {
                out[i * 2] = kHex[key[i] >> 4];
                out[i * 2 + 1] = kHex[key[i] & 0x0f];
            }
            return out;
        }

        [[nodiscard]] core::RecordView to_view(const SSTRecord& rec) noexcept {
            return core::RecordView{
                rec.key.data(),
                static_cast<uint16_t>(rec.key.size()),
                rec.value.data(),
                static_cast<uint16_t>(rec.value.size()),
                rec.seq,
                rec.flags,
                rec.key_fp64,
                rec.mini_key
            };
        }

        [[nodiscard]] uint64_t record_bytes(const SSTRecord& rec) noexcept {
            return align_up_u64(32 + rec.key.size() + rec.value.size(), 8);
        }
    } // namespace

    class SSTManager::Iterator::Impl {
        public:
            Impl(std::vector<std::shared_ptr<SSTReader>> readers, std::span<const uint8_t> start_key, std::span<const uint8_t> end_key)
                : start_key_{start_key.begin(), start_key.end()}, end_key_{end_key.begin(), end_key.end()} {
                sources_.reserve(readers.size());
                for (auto& reader : readers) {
                    if (!reader) { continue; }
                    Source source;
                    source.reader = std::move(reader);
                    source.rows = source.reader->scan(start_key_, end_key_);
                    source.it = source.rows.begin();
                    const size_t source_idx = sources_.size();
                    sources_.push_back(std::move(source));
                    push_current(source_idx);
                }
                advance();
            }

            [[nodiscard]] bool has_next() const noexcept { return pending_.has_value(); }

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
                size_t source_idx = 0;
            };

            struct HeapGreater {
                bool operator()(const HeapEntry& a, const HeapEntry& b) const noexcept {
                    const int c = compare_bytes(a.rec.key, b.rec.key);
                    if (c != 0) { return c > 0; }
                    return a.rec.seq < b.rec.seq;
                }
            };

            void push_current(size_t source_idx) {
                auto& source = sources_[source_idx];
                if (source.it == std::default_sentinel) { return; }
                heap_.push(HeapEntry{*source.it, source_idx});
                ++source.it;
            }

            void advance() {
                pending_.reset();
                while (!heap_.empty()) {
                    HeapEntry best = heap_.top();
                    heap_.pop();
                    push_current(best.source_idx);

                    while (!heap_.empty() && compare_bytes(heap_.top().rec.key, best.rec.key) == 0) {
                        HeapEntry next = heap_.top();
                        heap_.pop();
                        push_current(next.source_idx);
                        if (next.rec.seq > best.rec.seq) { best.rec = std::move(next.rec); }
                    }

                    if (!best.rec.is_tombstone()) {
                        pending_ = std::move(best.rec);
                        return;
                    }
                }
            }

            std::vector<uint8_t> start_key_;
            std::vector<uint8_t> end_key_;
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
                uint64_t entry_count = 0;
                uint64_t file_size = 0;
                uint64_t min_seq = 0;
                uint64_t max_seq = 0;
                std::vector<uint8_t> first_key;
                std::vector<uint8_t> last_key;
                std::shared_ptr<SSTReader> reader;
            };

            using Levels = std::vector<std::vector<Meta>>;

            struct Work {
                int src = 0;
                int dst = 1;
                std::vector<Meta> inputs;
            };

            Impl(Options options, manifest::Manifest* manifest)
                : options_{std::move(options)}, manifest_{manifest} {
                if (options_.max_levels < 2) { options_.max_levels = 2; }
                if (options_.sst_dir.empty()) { throw std::invalid_argument("SSTManager: sst_dir is required"); }
                std::filesystem::create_directories(options_.sst_dir);
                levels_.resize(static_cast<size_t>(options_.max_levels));
                publish_locked();
                const int n = std::max(1, options_.compact_threads);
                compact_threads_.reserve(static_cast<size_t>(n));
                for (int i = 0; i < n; ++i) { compact_threads_.emplace_back([this] { compaction_loop(); }); }
            }

            ~Impl() { shutdown(); }

            void recover() {
                std::unique_lock lock{levels_mu_};
                for (auto& level : levels_) { level.clear(); }
                next_file_id_ = 1;

                for (const auto& entry : std::filesystem::directory_iterator(options_.sst_dir)) {
                    const auto p = entry.path();
                    if (p.extension() == ".tmp") {
                        std::error_code ec;
                        std::filesystem::remove(p, ec);
                    }
                }

                std::vector<std::string> files;
                if (manifest_) { files = manifest_->live_sst(); }
                else {
                    for (const auto& entry : std::filesystem::directory_iterator(options_.sst_dir)) {
                        if (entry.path().extension() == ".aksst") { files.push_back(entry.path().filename().string()); }
                    }
                }

                for (const auto& file : files) {
                    const auto path = options_.sst_dir / file;
                    auto reader = SSTReader::open(path, reader_options());
                    if (!reader) { continue; }
                    Meta meta = make_meta(path, file, std::move(reader));
                    if (meta.level < 0 || meta.level >= options_.max_levels) { continue; }
                    levels_[static_cast<size_t>(meta.level)].push_back(std::move(meta));
                    const uint64_t observed_next = parse_file_id(file) + 1;
                    uint64_t current_next = next_file_id_.load(std::memory_order_relaxed);
                    while (current_next < observed_next && !next_file_id_.compare_exchange_weak(
                        current_next,
                        observed_next,
                        std::memory_order_relaxed,
                        std::memory_order_relaxed
                    )) {}
                }
                sort_all_levels_locked();
                publish_locked();
                request_compaction();
            }

            void shutdown() {
                bool expected = false;
                if (!shutting_down_.compare_exchange_strong(expected, true)) { return; }
                compact_cv_.notify_all();
                for (auto& t : compact_threads_) { if (t.joinable()) { t.join(); } }
                compact_threads_.clear();
            }

            uint64_t flush(std::span<const core::RecordView> records) {
                if (records.empty()) { return 0; }
                const auto path = make_file_path(0);
                const auto tmp = path.string() + ".tmp";

                SSTWriter::Options wopts;
                wopts.level = 0;
                wopts.block_size = options_.block_size;
                wopts.target_file_size = options_.target_file_size;
                wopts.bloom_bits_per_key = options_.bloom_bits_per_key;
                wopts.codec = options_.codec;

                const auto result = SSTWriter::write(tmp, records, wopts);
                std::filesystem::rename(tmp, path);
                auto reader = SSTReader::open(path, reader_options());
                if (!reader) { throw std::runtime_error("SSTManager: cannot reopen flushed SST"); }
                Meta meta = make_meta(path, path.filename().string(), std::move(reader));

                if (manifest_) { manifest_->sst_seal(0, meta.filename, meta.entry_count, hex_key(meta.first_key), hex_key(meta.last_key)); }

                {
                    std::unique_lock lock{levels_mu_};
                    levels_[0].insert(levels_[0].begin(), std::move(meta));
                    publish_locked();
                }
                request_compaction();
                return result.max_seq;
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
                        if (compare_bytes(files[mid].last_key, key) < 0) { lo = mid + 1; }
                        else { hi = mid; }
                    }
                    if (lo < files.size() && compare_bytes(files[lo].first_key, key) <= 0 && compare_bytes(key, files[lo].last_key) <= 0 &&
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
                return !rec->is_tombstone();
            }

            [[nodiscard]] std::optional<bool> get_into(std::span<const uint8_t> key, std::vector<uint8_t>& out) const {
                auto rec = get(key);
                if (!rec) { return std::nullopt; }
                if (rec->is_tombstone()) { return false; }
                out = std::move(rec->value);
                return true;
            }

            [[nodiscard]] Iterator scan_iter(std::span<const uint8_t> start_key, std::span<const uint8_t> end_key) const {
                auto snap = snapshot_.load(std::memory_order_acquire);
                if (!snap) { return Iterator{}; }

                std::vector<std::shared_ptr<SSTReader>> readers;
                for (const auto& level : *snap) { for (const auto& meta : level) { if (meta.reader) { readers.push_back(meta.reader); } } }
                return Iterator{std::make_unique<Iterator::Impl>(std::move(readers), start_key, end_key)};
            }

            [[nodiscard]] std::vector<LevelStats> level_stats() const {
                auto snap = snapshot_.load(std::memory_order_acquire);
                std::vector<LevelStats> out;
                if (!snap) { return out; }
                out.reserve(snap->size());
                for (size_t i = 0; i < snap->size(); ++i) {
                    uint64_t bytes = 0;
                    for (const auto& m : (*snap)[i]) { bytes += m.file_size; }
                    out.push_back(
                        LevelStats{static_cast<int>(i), (*snap)[i].size(), bytes, i == 0 ? 0 : level_budget(static_cast<int>(i))}
                    );
                }
                return out;
            }

            [[nodiscard]] bool compaction_pending() const noexcept { return compact_requested_.load(std::memory_order_relaxed); }

            [[nodiscard]] CompactionSnapshot compaction_snapshot() const noexcept {
                return {
                    compactions_completed_.load(std::memory_order_relaxed),
                    files_compacted_.load(std::memory_order_relaxed),
                    bytes_compacted_in_.load(std::memory_order_relaxed),
                    bytes_compacted_out_.load(std::memory_order_relaxed)
                };
            }

        private:
            [[nodiscard]] SSTReader::Options reader_options() const noexcept { return SSTReader::Options{options_.block_cache_bytes}; }

            [[nodiscard]] uint64_t level_budget(int level) const {
                if (level <= 0) { return 0; }
                double budget = static_cast<double>(options_.l1_max_bytes);
                for (int i = 1; i < level; ++i) { budget *= options_.level_size_multiplier; }
                return static_cast<uint64_t>(budget);
            }

            [[nodiscard]] uint64_t level_bytes_locked(int level) const {
                uint64_t total = 0;
                for (const auto& m : levels_[static_cast<size_t>(level)]) { total += m.file_size; }
                return total;
            }

            [[nodiscard]] std::filesystem::path make_file_path(int level) {
                const uint64_t id = next_file_id_.fetch_add(1, std::memory_order_relaxed);
                return options_.sst_dir / std::format("L{}_{}.aksst", level, id);
            }

            [[nodiscard]] static uint64_t parse_file_id(const std::string& file) noexcept {
                const auto us = file.find('_');
                const auto dot = file.find('.', us == std::string::npos ? 0 : us);
                if (us == std::string::npos || dot == std::string::npos || dot <= us + 1) { return 0; }
                try { return std::stoull(file.substr(us + 1, dot - us - 1)); }
                catch (...) { return 0; }
            }

            [[nodiscard]] Meta make_meta(
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
                meta.entry_count = hdr.entry_count;
                meta.file_size = hdr.file_size;
                meta.min_seq = hdr.min_seq;
                meta.max_seq = hdr.max_seq;
                meta.first_key.assign(shared->first_key().begin(), shared->first_key().end());
                meta.last_key.assign(shared->last_key().begin(), shared->last_key().end());
                meta.reader = std::move(shared);
                return meta;
            }

            void publish_locked() {
                auto snap = std::make_shared<Levels>(levels_);
                snapshot_.store(std::move(snap), std::memory_order_release);
            }

            void sort_all_levels_locked() {
                if (!levels_.empty()) {
                    std::sort(levels_[0].begin(), levels_[0].end(), [](const Meta& a, const Meta& b) { return a.filename > b.filename; });
                }
                for (size_t i = 1; i < levels_.size(); ++i) {
                    std::sort(
                        levels_[i].begin(),
                        levels_[i].end(),
                        [](const Meta& a, const Meta& b) { return compare_bytes(a.first_key, b.first_key) < 0; }
                    );
                }
            }

            void request_compaction() {
                compact_requested_.store(true, std::memory_order_relaxed);
                compact_cv_.notify_all();
            }

            void compaction_loop() {
                while (!shutting_down_.load(std::memory_order_relaxed)) {
                    {
                        std::unique_lock lock{compact_mu_};
                        compact_cv_.wait_for(
                            lock,
                            std::chrono::milliseconds(50),
                            [this] {
                                return compact_requested_.load(std::memory_order_relaxed) || shutting_down_.load(std::memory_order_relaxed);
                            }
                        );
                    }
                    if (shutting_down_.load(std::memory_order_relaxed)) { break; }
                    for (;;) {
                        auto work = pick_work();
                        if (!work) {
                            compact_requested_.store(false, std::memory_order_relaxed);
                            break;
                        }
                        run_compaction(std::move(*work));
                    }
                }
            }

            [[nodiscard]] std::optional<Work> pick_work() {
                std::unique_lock levels_lock{levels_mu_};
                std::lock_guard busy_lock{busy_mu_};
                if (!busy_src_.empty()) { return std::nullopt; }

                if (levels_[0].size() >= static_cast<size_t>(options_.max_l0_files) && busy_src_.count(0) == 0) {
                    Work w{0, 1, levels_[0]};
                    if (levels_.size() > 1) { w.inputs.insert(w.inputs.end(), levels_[1].begin(), levels_[1].end()); }
                    busy_src_.insert(0);
                    return w;
                }

                for (int level = 1; level + 1 < options_.max_levels; ++level) {
                    if (busy_src_.count(level) != 0) { continue; }
                    if (level_bytes_locked(level) <= level_budget(level) || levels_[static_cast<size_t>(level)].empty()) { continue; }

                    const auto src_it = std::min_element(
                        levels_[static_cast<size_t>(level)].begin(),
                        levels_[static_cast<size_t>(level)].end(),
                        [](const Meta& a, const Meta& b) { return a.max_seq < b.max_seq; }
                    );
                    Work w{level, level + 1, {*src_it}};
                    for (const auto& m : levels_[static_cast<size_t>(level + 1)]) {
                        if (compare_bytes(m.last_key, src_it->first_key) >= 0 && compare_bytes(m.first_key, src_it->last_key) <= 0) {
                            w.inputs.push_back(m);
                        }
                    }
                    busy_src_.insert(level);
                    return w;
                }
                return std::nullopt;
            }

            static void dedupe_in_place(std::vector<SSTRecord>& records, bool drop_tombstones) {
                std::sort(
                    records.begin(),
                    records.end(),
                    [](const SSTRecord& a, const SSTRecord& b) {
                        const int c = compare_bytes(a.key, b.key);
                        if (c != 0) { return c < 0; }
                        return a.seq > b.seq;
                    }
                );
                std::vector<SSTRecord> out;
                out.reserve(records.size());
                for (size_t i = 0; i < records.size();) {
                    SSTRecord best = std::move(records[i]);
                    ++i;
                    while (i < records.size() && compare_bytes(best.key, records[i].key) == 0) { ++i; }
                    if (!(drop_tombstones && best.is_tombstone())) { out.push_back(std::move(best)); }
                }
                records = std::move(out);
            }

            [[nodiscard]] std::vector<Meta> write_outputs(std::vector<SSTRecord>& records, int dst) {
                std::vector<Meta> outputs;
                size_t start = 0;
                while (start < records.size()) {
                    uint64_t bytes = 0;
                    size_t end = start;
                    while (end < records.size() && (end == start || bytes + record_bytes(records[end]) <= options_.target_file_size)) {
                        bytes += record_bytes(records[end]);
                        ++end;
                    }

                    std::vector<core::RecordView> views;
                    views.reserve(end - start);
                    for (size_t i = start; i < end; ++i) { views.push_back(to_view(records[i])); }

                    const auto path = make_file_path(dst);
                    const auto tmp = path.string() + ".tmp";
                    SSTWriter::Options wopts;
                    wopts.level = dst;
                    wopts.block_size = options_.block_size;
                    wopts.target_file_size = options_.target_file_size;
                    wopts.bloom_bits_per_key = options_.bloom_bits_per_key;
                    wopts.codec = options_.codec;
                    (void)SSTWriter::write(tmp, views, wopts);
                    std::filesystem::rename(tmp, path);
                    auto reader = SSTReader::open(path, reader_options());
                    if (!reader) { throw std::runtime_error("SSTManager: cannot reopen compacted SST"); }
                    outputs.push_back(make_meta(path, path.filename().string(), std::move(reader)));
                    start = end;
                }
                return outputs;
            }

            void run_compaction(Work work) {
                uint64_t bytes_in = 0;
                for (const auto& m : work.inputs) { bytes_in += m.file_size; }

                std::vector<std::string> input_files;
                input_files.reserve(work.inputs.size());
                for (const auto& m : work.inputs) { input_files.push_back(m.filename); }
                if (manifest_) { manifest_->compaction_start(work.src, input_files); }

                std::vector<SSTRecord> merged;
                for (const auto& m : work.inputs) {
                    if (!m.reader) { continue; }
                    auto rows = m.reader->scan();
                    for (auto&& row : rows) { merged.push_back(std::move(row)); }
                }
                dedupe_in_place(merged, work.dst == options_.max_levels - 1);

                std::vector<Meta> outputs;
                if (!merged.empty()) { outputs = write_outputs(merged, work.dst); }

                std::vector<std::string> output_files;
                uint64_t bytes_out = 0;
                output_files.reserve(outputs.size());
                for (const auto& m : outputs) {
                    output_files.push_back(m.filename);
                    bytes_out += m.file_size;
                }

                if (manifest_) { manifest_->compaction_commit(output_files, input_files); }

                {
                    std::unique_lock lock{levels_mu_};
                    std::unordered_set<std::string> input_set(input_files.begin(), input_files.end());
                    for (auto& level : levels_) { std::erase_if(level, [&](const Meta& m) { return input_set.count(m.filename) != 0; }); }
                    auto& dst_level = levels_[static_cast<size_t>(work.dst)];
                    dst_level.insert(dst_level.end(), std::make_move_iterator(outputs.begin()), std::make_move_iterator(outputs.end()));
                    sort_all_levels_locked();
                    publish_locked();
                }

                for (const auto& m : work.inputs) {
                    std::error_code ec;
                    std::filesystem::remove(m.path, ec);
                }

                compactions_completed_.fetch_add(1, std::memory_order_relaxed);
                files_compacted_.fetch_add(work.inputs.size(), std::memory_order_relaxed);
                bytes_compacted_in_.fetch_add(bytes_in, std::memory_order_relaxed);
                bytes_compacted_out_.fetch_add(bytes_out, std::memory_order_relaxed);

                {
                    std::lock_guard lock{busy_mu_};
                    busy_src_.erase(work.src);
                }
                request_compaction();
            }

            Options options_;
            manifest::Manifest* manifest_;

            mutable std::shared_mutex levels_mu_;
            Levels levels_;
            std::atomic<std::shared_ptr<const Levels>> snapshot_;
            std::atomic<uint64_t> next_file_id_{1};

            std::vector<std::thread> compact_threads_;
            mutable std::mutex compact_mu_;
            std::condition_variable compact_cv_;
            std::atomic<bool> compact_requested_{false};
            std::atomic<bool> shutting_down_{false};
            std::mutex busy_mu_;
            std::set<int> busy_src_;

            std::atomic<uint64_t> compactions_completed_{0};
            std::atomic<uint64_t> files_compacted_{0};
            std::atomic<uint64_t> bytes_compacted_in_{0};
            std::atomic<uint64_t> bytes_compacted_out_{0};
    };

    SSTManager::Iterator::Iterator() = default;
    SSTManager::Iterator::Iterator(std::unique_ptr<Impl> impl) : impl_{std::move(impl)} {}
    SSTManager::Iterator::~Iterator() = default;
    SSTManager::Iterator::Iterator(Iterator&&) noexcept = default;
    SSTManager::Iterator& SSTManager::Iterator::operator=(Iterator&&) noexcept = default;
    bool SSTManager::Iterator::has_next() const noexcept { return impl_ && impl_->has_next(); }

    std::optional<SSTRecord> SSTManager::Iterator::next() {
        if (!has_next()) { return std::nullopt; }
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

    std::optional<bool> SSTManager::get_into(std::span<const uint8_t> key, std::vector<uint8_t>& out) const {
        return impl_->get_into(key, out);
    }

    SSTManager::Iterator SSTManager::scan_iter(std::span<const uint8_t> start_key, std::span<const uint8_t> end_key) const {
        return impl_->scan_iter(start_key, end_key);
    }

    std::vector<SSTManager::LevelStats> SSTManager::level_stats() const { return impl_->level_stats(); }
    bool SSTManager::compaction_pending() const noexcept { return impl_->compaction_pending(); }
    SSTManager::CompactionSnapshot SSTManager::compaction_snapshot() const noexcept { return impl_->compaction_snapshot(); }
} // namespace akkaradb::engine::sst

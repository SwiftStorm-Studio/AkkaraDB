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

// akkengine/src/engine/memtable/MemTable.cpp
#include "akk/engine/memtable/MemTable.hpp"

#include <algorithm>
#include <atomic>
#include <cmath>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "akk/core/record/KeyFingerprint.hpp"
#include "akk/engine/memtable/SkipListMemTable.hpp"

namespace akkaradb::engine::memtable {
    namespace {
        [[nodiscard]] core::ByteView to_byte_view(std::span<const uint8_t> bytes) noexcept {
            return {reinterpret_cast<const std::byte*>(bytes.data()), bytes.size()};
        }

        [[nodiscard]] uint64_t compute_fp64(std::span<const uint8_t> key, uint64_t precomputed_fp64) noexcept {
            if (precomputed_fp64 != 0) { return precomputed_fp64; }
            if (key.empty()) { return 0; }
            return core::compute_key_fp64(key.data(), key.size());
        }

        [[nodiscard]] uint64_t compute_mini(std::span<const uint8_t> key, uint64_t precomputed_mk) noexcept {
            if (precomputed_mk != 0) { return precomputed_mk; }
            if (key.empty()) { return 0; }
            return core::build_mini_key(key.data(), key.size());
        }

        [[nodiscard]] uint64_t load_u64_le(const uint8_t* p) noexcept {
            uint64_t v = 0;
            std::memcpy(&v, p, sizeof(v));
            return v;
        }

        [[nodiscard]] uint64_t avalanche64(uint64_t x) noexcept {
            x ^= x >> 33;
            x *= 0xff51afd7ed558ccdULL;
            x ^= x >> 33;
            x *= 0xc4ceb9fe1a85ec53ULL;
            x ^= x >> 33;
            return x;
        }

        [[nodiscard]] uint64_t compute_shard_hash(std::span<const uint8_t> key) noexcept {
            const auto* p = key.data();
            size_t n = key.size();
            uint64_t h = 0x9e3779b97f4a7c15ULL ^ (static_cast<uint64_t>(n) * 0xbf58476d1ce4e5b9ULL);

            while (n >= 8) {
                uint64_t lane = load_u64_le(p);
                lane *= 0x9ddfea08eb382d69ULL;
                lane ^= lane >> 32;
                h ^= lane;
                h = (h << 27) | (h >> 37);
                h = h * 5 + 0x52dce729;
                p += 8;
                n -= 8;
            }

            uint64_t tail = 0;
            for (size_t i = 0; i < n; ++i) { tail |= static_cast<uint64_t>(p[i]) << (i * 8); }
            h ^= tail * 0x9ddfea08eb382d69ULL;
            return avalanche64(h);
        }

        [[nodiscard]] uint32_t next_pow2_clamped(uint64_t n, uint32_t min_value, uint32_t max_value) noexcept {
            uint32_t p = 1;
            while (p < n && p < max_value) { p <<= 1; }
            if (p < min_value) { p = min_value; }
            if (p > max_value) { p = max_value; }
            return p;
        }

        [[nodiscard]] uint32_t resolve_shard_count(size_t requested, size_t expected_concurrent_writers, size_t auto_cap) {
            if (requested == 1) { return 1; }

            if (requested > 1) { return next_pow2_clamped(static_cast<uint64_t>(requested), 2, 256); }

            const uint32_t effective_cap = next_pow2_clamped(static_cast<uint64_t>(auto_cap == 0 ? 128 : auto_cap), 2, 256);

            const size_t n = expected_concurrent_writers > 0
                                 ? expected_concurrent_writers
                                 : std::max<size_t>(2, std::thread::hardware_concurrency());

            const uint64_t target = n <= 1 ? 1ULL : static_cast<uint64_t>(n) * 4ULL;
            return next_pow2_clamped(target, 2, effective_cap);
        }

        [[nodiscard]] uint32_t shard_for_hash(uint64_t hash, uint32_t shard_count) noexcept {
            if (shard_count <= 1) { return 0; }
            return static_cast<uint32_t>(hash & static_cast<uint64_t>(shard_count - 1));
        }

        [[noreturn]] void throw_status_error(const char* op, const core::Status& st) {
            std::string msg = op;
            msg += " failed";
            if (!st.message().empty()) {
                msg += ": ";
                msg.append(st.message().begin(), st.message().end());
            }
            throw std::runtime_error(msg);
        }
    } // namespace

    class MemTable::RangeIterator::Impl {
        public:
            explicit Impl(std::vector<RecordView> records, std::vector<std::shared_ptr<const IMemTable>> sources)
                : records_{std::move(records)}, sources_{std::move(sources)} {}

            [[nodiscard]] bool has_next() const noexcept { return index_ < records_.size(); }

            [[nodiscard]] std::optional<RecordView> next() noexcept {
                if (!has_next()) { return std::nullopt; }
                return records_[index_++];
            }

        private:
            std::vector<RecordView> records_;
            std::vector<std::shared_ptr<const IMemTable>> sources_;
            size_t index_{0};
    };

    class MemTable::Impl {
        public:
            struct Shard {
                mutable std::shared_mutex mutex;
                std::shared_ptr<IMemTable> active;
                std::atomic<IMemTable*> active_raw{nullptr};
                std::atomic<uint32_t> immutable_count{0};

                struct Immutable {
                    uint64_t id;
                    std::shared_ptr<IMemTable> table;
                    size_t bytes;
                };

                struct PublishedTables {
                    std::shared_ptr<const IMemTable> active;
                    std::vector<std::shared_ptr<const IMemTable>> immutables;
                };

                std::deque<Immutable> immutables;
                std::atomic<std::shared_ptr<const PublishedTables>> published;
                std::atomic<size_t> approx_bytes{0};
                std::atomic<uint64_t> puts_applied{0};
                std::atomic<uint64_t> removes_applied{0};
                size_t active_bytes{0};
                uint64_t next_immutable_id{1};
            };

            class Flusher {
                public:
                    struct Item {
                        uint64_t id;
                        std::shared_ptr<IMemTable> table;
                    };

                    using FlushDone = std::function<void(uint64_t)>;

                    Flusher(FlushCallback callback, FlushDone done)
                        : callback_{std::move(callback)}, on_done_{std::move(done)}, running_{true}, thread_{[this]() { run(); }} {}

                    ~Flusher() {
                        {
                            std::lock_guard<std::mutex> lock{mutex_};
                            running_ = false;
                        }
                        cv_.notify_all();
                        if (thread_.joinable()) { thread_.join(); }
                    }

                    Flusher(const Flusher&) = delete;
                    Flusher& operator=(const Flusher&) = delete;

                    void enqueue(Item item) {
                        {
                            std::lock_guard<std::mutex> lock{mutex_};
                            queue_.push(std::move(item));
                        }
                        cv_.notify_one();
                    }

                    void drain() {
                        std::unique_lock<std::mutex> lock{mutex_};
                        cv_.wait(lock, [this]() { return queue_.empty() && in_flight_ == 0; });
                    }

                private:
                    void run() {
                        for (;;) {
                            Item item;
                            {
                                std::unique_lock<std::mutex> lock{mutex_};
                                cv_.wait(lock, [this]() { return !queue_.empty() || !running_; });
                                if (!running_ && queue_.empty()) { break; }
                                item = std::move(queue_.front());
                                queue_.pop();
                                ++in_flight_;
                            }

                            std::vector<RecordView> records;
                            records.reserve(item.table->entryCount());
                            for (const RecordView& rec : item.table->iterator(
                                     core::ByteView{},
                                     core::ByteView{},
                                     std::numeric_limits<uint64_t>::max()
                                 )) { records.push_back(rec); }
                            if (callback_) { callback_(std::span<const RecordView>{records}); }
                            if (on_done_) { on_done_(item.id); }

                            {
                                std::lock_guard<std::mutex> lock{mutex_};
                                --in_flight_;
                            }
                            cv_.notify_all();
                        }
                    }

                    FlushCallback callback_;
                    FlushDone on_done_;
                    std::mutex mutex_;
                    std::condition_variable cv_;
                    std::queue<Item> queue_;
                    bool running_;
                    size_t in_flight_{0};
                    std::thread thread_;
            };

            explicit Impl(Options options)
                : options_{std::move(options)},
                  shard_count_{
                      resolve_shard_count(options_.shard_count, options_.expected_concurrent_writers, options_.auto_shard_count_cap)
                  },
                  threshold_bytes_per_shard_{options_.threshold_bytes_per_shard},
                  seq_gen_{1} {
                if (!options_.backend_factory) { options_.backend_factory = []() { return std::make_unique<SkipListMemTable>(); }; }

                shards_.resize(shard_count_);
                for (uint32_t i = 0; i < shard_count_; ++i) {
                    shards_[i] = std::make_unique<Shard>();
                    auto table = options_.backend_factory();
                    if (!table) { throw std::invalid_argument("MemTable backend_factory returned null"); }
                    shards_[i]->active = std::shared_ptr<IMemTable>{std::move(table)};
                    shards_[i]->active_raw.store(shards_[i]->active.get(), std::memory_order_relaxed);
                    shards_[i]->active_bytes = shards_[i]->active->sizeBytes();
                    shards_[i]->approx_bytes.store(shards_[i]->active_bytes, std::memory_order_relaxed);
                    publish_tables_locked(*shards_[i]);
                }

                if (options_.on_flush) { set_flush_callback(options_.on_flush); }
            }

            void put(
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint64_t seq,
                uint8_t flags,
                uint64_t precomputed_fp64,
                uint64_t precomputed_mk
            ) {
                const uint64_t fp64 = compute_fp64(key, precomputed_fp64);
                const uint64_t mini = compute_mini(key, precomputed_mk);
                const uint32_t shard_index = shard_for_hash(compute_shard_hash(key), shard_count_);

                auto& shard = *shards_[shard_index];
                bool should_flush = false;

                {
                    std::unique_lock<std::shared_mutex> lock{shard.mutex};
                    const core::Status st = shard.active->put(to_byte_view(key), to_byte_view(value), seq, flags, fp64, mini);
                    if (!st.ok()) { throw_status_error("MemTable::put", st); }
                    const size_t new_active_bytes = shard.active->sizeBytes();
                    const size_t previous_active_bytes = shard.active_bytes;
                    shard.active_bytes = new_active_bytes;

                    size_t total_bytes = shard.approx_bytes.load(std::memory_order_relaxed);
                    total_bytes = total_bytes - previous_active_bytes + new_active_bytes;
                    shard.approx_bytes.store(total_bytes, std::memory_order_relaxed);
                    should_flush = threshold_bytes_per_shard_ > 0 && new_active_bytes > threshold_bytes_per_shard_;
                }

                shard.puts_applied.fetch_add(1, std::memory_order_relaxed);
                advance_seq(seq);
                if (should_flush) { trigger_flush(shard_index); }
            }

            void remove(std::span<const uint8_t> key, uint64_t seq, uint64_t precomputed_fp64, uint64_t precomputed_mk) {
                const uint64_t fp64 = compute_fp64(key, precomputed_fp64);
                const uint64_t mini = compute_mini(key, precomputed_mk);
                const uint32_t shard_index = shard_for_hash(compute_shard_hash(key), shard_count_);

                put(key, {}, seq, RecordView::FLAG_TOMBSTONE, fp64, mini);
                shards_[shard_index]->puts_applied.fetch_sub(1, std::memory_order_relaxed);
                shards_[shard_index]->removes_applied.fetch_add(1, std::memory_order_relaxed);
            }

            [[nodiscard]] bool get(std::span<const uint8_t> key, uint64_t snapshot_seq, RecordView* out, uint64_t precomputed_fp64) const {
                if (out == nullptr) { return false; }

                const core::ByteView key_view = to_byte_view(key);
                (void)precomputed_fp64;
                const uint32_t shard_index = shard_for_hash(compute_shard_hash(key), shard_count_);
                const auto& shard = *shards_[shard_index];

                if (raw_active_get_enabled_.load(std::memory_order_acquire) && shard.immutable_count.load(std::memory_order_acquire) == 0) {
                    const IMemTable* active = shard.active_raw.load(std::memory_order_acquire);
                    return active != nullptr && active->get(key_view, snapshot_seq, out);
                }

                const auto published = shard.published.load(std::memory_order_acquire);
                if (!published) { return false; }

                if (published->active && published->active->get(key_view, snapshot_seq, out)) { return true; }

                for (const auto& immutable : published->immutables) {
                    if (immutable && immutable->get(key_view, snapshot_seq, out)) { return true; }
                }
                return false;
            }

            [[nodiscard]] std::optional<bool> get_into(
                std::span<const uint8_t> key,
                uint64_t snapshot_seq,
                std::vector<uint8_t>& out
            ) const {
                RecordView view;
                if (!get(key, snapshot_seq, &view, 0)) { return std::nullopt; }
                if (view.is_tombstone()) { return false; }
                const auto value = view.value();
                out.assign(value.begin(), value.end());
                return true;
            }

            [[nodiscard]] std::optional<bool> contains(std::span<const uint8_t> key, uint64_t snapshot_seq) const {
                RecordView view;
                if (!get(key, snapshot_seq, &view, 0)) { return std::nullopt; }
                return !view.is_tombstone();
            }

            [[nodiscard]] RangeIterator iterator(const KeyRange& range, uint64_t snapshot_seq) const {
                std::vector<std::shared_ptr<const IMemTable>> sources;
                sources.reserve(shard_count_ * 2);

                for (const auto& shard_ptr : shards_) {
                    const auto published = shard_ptr->published.load(std::memory_order_acquire);
                    if (!published) { continue; }
                    if (published->active) { sources.push_back(published->active); }
                    for (const auto& immutable : published->immutables) { if (immutable) { sources.push_back(immutable); } }
                }

                const core::ByteView start_view = to_byte_view(std::span<const uint8_t>{range.start.data(), range.start.size()});
                const core::ByteView end_view = to_byte_view(std::span<const uint8_t>{range.end.data(), range.end.size()});

                std::vector<RecordView> deduped;

                struct SourceCursor {
                    core::ArenaGenerator<RecordView> generator;
                    core::ArenaGenerator<RecordView>::iterator it{};
                    RecordView current{};
                };

                auto advance_filtered = [&](SourceCursor& cursor, bool consume_current) -> bool {
                    if (consume_current) { ++cursor.it; }
                    if (cursor.it == cursor.generator.end()) { return false; }
                    cursor.current = *cursor.it;
                    return true;
                };

                std::vector<SourceCursor> cursors;
                cursors.reserve(sources.size());

                for (const auto& table : sources) {
                    SourceCursor cursor;
                    cursor.generator = table->iterator(start_view, end_view, snapshot_seq);
                    cursor.it = cursor.generator.begin();
                    if (cursor.it == cursor.generator.end()) { continue; }
                    if (!advance_filtered(cursor, false)) { continue; }
                    cursors.push_back(std::move(cursor));
                }

                if (!cursors.empty()) {
                    auto min_key_cmp = [&](size_t lhs, size_t rhs) {
                        const RecordView& a = cursors[lhs].current;
                        const RecordView& b = cursors[rhs].current;
                        const int key_cmp = a.compare_key(b);
                        if (key_cmp != 0) { return key_cmp > 0; }
                        return a.seq() < b.seq();
                    };

                    std::priority_queue<size_t, std::vector<size_t>, decltype(min_key_cmp)> heap(min_key_cmp);
                    for (size_t i = 0; i < cursors.size(); ++i) { heap.push(i); }

                    std::vector<size_t> same_key_indices;
                    same_key_indices.reserve(cursors.size());

                    while (!heap.empty()) {
                        same_key_indices.clear();

                        const size_t first_idx = heap.top();
                        heap.pop();

                        RecordView best = cursors[first_idx].current;
                        same_key_indices.push_back(first_idx);

                        while (!heap.empty()) {
                            const size_t idx = heap.top();
                            if (cursors[idx].current.compare_key(best) != 0) { break; }
                            heap.pop();
                            const RecordView candidate = cursors[idx].current;
                            if (candidate.seq() > best.seq()) { best = candidate; }
                            same_key_indices.push_back(idx);
                        }

                        deduped.push_back(best);

                        for (const size_t idx : same_key_indices) { if (advance_filtered(cursors[idx], true)) { heap.push(idx); } }
                    }
                }

                return RangeIterator{std::make_unique<RangeIterator::Impl>(std::move(deduped), std::move(sources))};
            }

            [[nodiscard]] uint64_t next_seq() noexcept { return seq_gen_.fetch_add(1, std::memory_order_relaxed); }

            [[nodiscard]] uint64_t reserve_seq(uint64_t count) {
                if (count == 0) { return seq_gen_.load(std::memory_order_relaxed); }
                return seq_gen_.fetch_add(count, std::memory_order_relaxed);
            }

            [[nodiscard]] uint64_t last_seq() const noexcept { return seq_gen_.load(std::memory_order_relaxed); }

            void advance_seq(uint64_t observed_seq) noexcept {
                uint64_t current = seq_gen_.load(std::memory_order_relaxed);
                while (current <= observed_seq) {
                    if (seq_gen_.compare_exchange_weak(current, observed_seq + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
                        break;
                    }
                }
            }

            void flush_hint() {
                for (uint32_t i = 0; i < shard_count_; ++i) {
                    bool over_threshold = false;
                    {
                        std::shared_lock<std::shared_mutex> lock{shards_[i]->mutex};
                        over_threshold = shards_[i]->active_bytes > threshold_bytes_per_shard_;
                    }
                    if (over_threshold) { trigger_flush(i); }
                }
            }

            void force_flush() {
                for (uint32_t i = 0; i < shard_count_; ++i) { trigger_flush(i); }
                for (auto& worker : flushers_) { if (worker) { worker->drain(); } }
            }

            void set_flush_callback(const FlushCallback& cb) {
                raw_active_get_enabled_.store(false, std::memory_order_release);
                for (auto& worker : flushers_) { if (worker) { worker->drain(); } }

                flushers_.clear();
                flushers_.resize(shard_count_);

                if (!cb) {
                    raw_active_get_enabled_.store(true, std::memory_order_release);
                    return;
                }

                for (uint32_t i = 0; i < shard_count_; ++i) {
                    flushers_[i] = std::make_unique<Flusher>(cb, [this, i](uint64_t immutable_id) { on_flushed(i, immutable_id); });
                }
            }

            [[nodiscard]] size_t approx_size() const noexcept {
                size_t total = 0;
                for (const auto& shard : shards_) { total += shard->approx_bytes.load(std::memory_order_relaxed); }
                return total;
            }

            [[nodiscard]] MemTableSnapshot snapshot() const noexcept {
                uint64_t puts = 0;
                uint64_t removes = 0;
                for (const auto& shard : shards_) {
                    puts += shard->puts_applied.load(std::memory_order_relaxed);
                    removes += shard->removes_applied.load(std::memory_order_relaxed);
                }
                return {
                    shard_count_,
                    static_cast<uint64_t>(threshold_bytes_per_shard_),
                    static_cast<uint64_t>(approx_size()),
                    puts,
                    removes,
                    flushes_completed_.load(std::memory_order_relaxed),
                };
            }

        private:
            static void publish_tables_locked(Shard& shard) {
                auto published = std::make_shared<Shard::PublishedTables>();
                published->active = std::const_pointer_cast<const IMemTable>(shard.active);
                published->immutables.reserve(shard.immutables.size());
                for (auto it = shard.immutables.rbegin(); it != shard.immutables.rend(); ++it) {
                    published->immutables.push_back(std::const_pointer_cast<const IMemTable>(it->table));
                }
                shard.immutable_count.store(static_cast<uint32_t>(published->immutables.size()), std::memory_order_release);
                std::shared_ptr<const Shard::PublishedTables> published_const = std::move(published);
                shard.published.store(std::move(published_const), std::memory_order_release);
            }

            void trigger_flush(uint32_t shard_index) {
                if (shard_index >= flushers_.size() || !flushers_[shard_index]) { return; }

                auto& shard = *shards_[shard_index];
                std::shared_ptr<IMemTable> sealed;
                uint64_t immutable_id = 0;
                size_t sealed_bytes = 0;

                {
                    std::unique_lock<std::shared_mutex> lock{shard.mutex};
                    if (shard.active->entryCount() == 0) { return; }

                    shard.active->freeze();
                    sealed = shard.active;
                    sealed_bytes = shard.active_bytes;

                    auto new_active = options_.backend_factory();
                    if (!new_active) { throw std::invalid_argument("MemTable backend_factory returned null"); }
                    immutable_id = shard.next_immutable_id++;
                    shard.immutables.emplace_back(Shard::Immutable{immutable_id, sealed, sealed_bytes});
                    shard.immutable_count.store(static_cast<uint32_t>(shard.immutables.size()), std::memory_order_release);

                    shard.active = std::shared_ptr<IMemTable>{std::move(new_active)};
                    shard.active_raw.store(shard.active.get(), std::memory_order_release);
                    shard.active_bytes = shard.active->sizeBytes();

                    size_t total_bytes = shard.approx_bytes.load(std::memory_order_relaxed);
                    total_bytes = total_bytes - sealed_bytes + shard.active_bytes + sealed_bytes;
                    shard.approx_bytes.store(total_bytes, std::memory_order_relaxed);

                    publish_tables_locked(shard);
                }

                flushers_[shard_index]->enqueue(Flusher::Item{immutable_id, std::move(sealed)});
            }

            void on_flushed(uint32_t shard_index, uint64_t immutable_id) {
                auto& shard = *shards_[shard_index];
                std::unique_lock<std::shared_mutex> lock{shard.mutex};
                std::erase_if(
                    shard.immutables,
                    [&](const Shard::Immutable& item) {
                        if (item.id != immutable_id) { return false; }
                        const size_t total = shard.approx_bytes.load(std::memory_order_relaxed);
                        shard.approx_bytes.store(total >= item.bytes ? total - item.bytes : 0, std::memory_order_relaxed);
                        return true;
                    }
                );
                publish_tables_locked(shard);
                flushes_completed_.fetch_add(1, std::memory_order_relaxed);
            }

            Options options_;
            uint32_t shard_count_;
            size_t threshold_bytes_per_shard_;
            std::vector<std::unique_ptr<Shard>> shards_;
            std::vector<std::unique_ptr<Flusher>> flushers_;

            std::atomic<uint64_t> seq_gen_;
            std::atomic<uint64_t> flushes_completed_{0};
            std::atomic<bool> raw_active_get_enabled_{true};
    };

    MemTable::RangeIterator::RangeIterator(std::unique_ptr<Impl> impl) : impl_{std::move(impl)} {}

    MemTable::RangeIterator::~RangeIterator() = default;
    MemTable::RangeIterator::RangeIterator(RangeIterator&&) noexcept = default;
    MemTable::RangeIterator& MemTable::RangeIterator::operator=(RangeIterator&&) noexcept = default;

    bool MemTable::RangeIterator::has_next() const noexcept { return impl_ && impl_->has_next(); }

    std::optional<RecordView> MemTable::RangeIterator::next() noexcept {
        if (!impl_) { return std::nullopt; }
        return impl_->next();
    }

    std::unique_ptr<MemTable> MemTable::create() { return create(Options{}); }

    std::unique_ptr<MemTable> MemTable::create(const Options& options) { return std::unique_ptr<MemTable>{new MemTable(options)}; }

    MemTable::MemTable(const Options& options) : impl_{std::make_unique<Impl>(options)} {}

    MemTable::~MemTable() = default;

    void MemTable::put(
        std::span<const uint8_t> key,
        std::span<const uint8_t> value,
        uint64_t seq,
        uint8_t flags,
        uint64_t precomputed_fp64,
        uint64_t precomputed_mk
    ) { impl_->put(key, value, seq, flags, precomputed_fp64, precomputed_mk); }

    void MemTable::remove(std::span<const uint8_t> key, uint64_t seq, uint64_t precomputed_fp64, uint64_t precomputed_mk) {
        impl_->remove(key, seq, precomputed_fp64, precomputed_mk);
    }

    void MemTable::advance_seq(uint64_t seq) noexcept { impl_->advance_seq(seq); }

    bool MemTable::get(std::span<const uint8_t> key, uint64_t snapshot_seq, RecordView* out) const {
        return impl_->get(key, snapshot_seq, out, 0);
    }

    bool MemTable::get(std::span<const uint8_t> key, uint64_t snapshot_seq, RecordView* out, uint64_t precomputed_fp64) const {
        return impl_->get(key, snapshot_seq, out, precomputed_fp64);
    }

    std::optional<bool> MemTable::get_into(std::span<const uint8_t> key, uint64_t snapshot_seq, std::vector<uint8_t>& out) const {
        return impl_->get_into(key, snapshot_seq, out);
    }

    std::optional<bool> MemTable::contains(std::span<const uint8_t> key, uint64_t snapshot_seq) const {
        return impl_->contains(key, snapshot_seq);
    }

    MemTable::RangeIterator MemTable::iterator(const KeyRange& range, uint64_t snapshot_seq) const {
        return impl_->iterator(range, snapshot_seq);
    }

    uint64_t MemTable::next_seq() noexcept { return impl_->next_seq(); }

    uint64_t MemTable::reserve_seq(uint64_t count) { return impl_->reserve_seq(count); }

    uint64_t MemTable::last_seq() const noexcept { return impl_->last_seq(); }

    void MemTable::flush_hint() { impl_->flush_hint(); }

    void MemTable::force_flush() { impl_->force_flush(); }

    void MemTable::set_flush_callback(const FlushCallback& cb) { impl_->set_flush_callback(cb); }

    size_t MemTable::approx_size() const noexcept { return impl_->approx_size(); }

    MemTable::MemTableSnapshot MemTable::snapshot() const noexcept { return impl_->snapshot(); }
} // namespace akkaradb::engine::memtable

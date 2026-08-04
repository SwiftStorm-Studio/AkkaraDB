/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
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
#include <exception>
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
        [[nodiscard]] core::ByteView toByteView(std::span<const uint8_t> bytes) noexcept {
            return {reinterpret_cast<const std::byte*>(bytes.data()), bytes.size()};
        }

        [[nodiscard]] uint64_t computeFp64(std::span<const uint8_t> key, uint64_t precomputedFp64) noexcept {
            if (precomputedFp64 != 0) { return precomputedFp64; }
            if (key.empty()) { return 0; }
            return core::computeKeyFp64(key.data(), key.size());
        }

        [[nodiscard]] uint64_t computeMini(std::span<const uint8_t> key, uint64_t precomputedMk) noexcept {
            if (precomputedMk != 0) { return precomputedMk; }
            if (key.empty()) { return 0; }
            return core::buildMiniKey(key.data(), key.size());
        }

        [[nodiscard]] uint64_t avalanche64(uint64_t x) noexcept {
            x ^= x >> 33;
            x *= 0xff51afd7ed558ccdULL;
            x ^= x >> 33;
            x *= 0xc4ceb9fe1a85ec53ULL;
            x ^= x >> 33;
            return x;
        }

        [[nodiscard]] uint64_t shardRouteHash(uint64_t fp64, size_t keySize) noexcept {
            return avalanche64(fp64 ^ (static_cast<uint64_t>(keySize) * 0xbf58476d1ce4e5b9ULL));
        }

        [[nodiscard]] uint32_t nextPow2Clamped(uint64_t n, uint32_t minValue, uint32_t maxValue) noexcept {
            uint32_t p = 1;
            while (p < n && p < maxValue) { p <<= 1; }
            if (p < minValue) { p = minValue; }
            if (p > maxValue) { p = maxValue; }
            return p;
        }

        [[nodiscard]] uint32_t resolveShardCount(size_t requested, size_t expectedConcurrentWriters, size_t autoCap) {
            if (requested == 1) { return 1; }

            if (requested > 1) { return nextPow2Clamped(static_cast<uint64_t>(requested), 2, 256); }

            const uint32_t effectiveCap = nextPow2Clamped(static_cast<uint64_t>(autoCap == 0 ? 128 : autoCap), 2, 256);

            const size_t n = expectedConcurrentWriters > 0
                                 ? expectedConcurrentWriters
                                 : std::max<size_t>(2, std::thread::hardware_concurrency());

            const uint64_t target = n <= 1 ? 1ULL : static_cast<uint64_t>(n) * 4ULL;
            return nextPow2Clamped(target, 2, effectiveCap);
        }

        [[nodiscard]] uint32_t resolveFlushWorkerCount(uint32_t shardCount) noexcept {
            const uint32_t hardware = std::max(1u, std::thread::hardware_concurrency());
            const uint32_t target = std::max(1u, hardware / 2u);
            return std::min(std::max(1u, shardCount), target);
        }

        [[nodiscard]] uint32_t shardForHash(uint64_t hash, uint32_t shardCount) noexcept {
            if (shardCount <= 1) { return 0; }
            return static_cast<uint32_t>(hash & static_cast<uint64_t>(shardCount - 1));
        }

        [[nodiscard]] MemTableFlushMode resolveFlushMode(MemTableFlushMode mode, size_t thresholdBytesPerShard) noexcept {
            if (mode != MemTableFlushMode::AUTO) { return mode; }
            return thresholdBytesPerShard > 0 ? MemTableFlushMode::BYTES_PER_SHARD : MemTableFlushMode::MANUAL_ONLY;
        }

        [[noreturn]] void throwStatusError(const char* op, const core::Status& st) {
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
            explicit Impl(
                std::vector<std::shared_lock<std::shared_mutex>> scanLocks,
                std::vector<std::shared_ptr<const IMemTable>> sources,
                std::vector<uint8_t> start,
                std::vector<uint8_t> end,
                uint64_t snapshotSeq
            )
                : scanLocks_{std::move(scanLocks)},
                  sources_{std::move(sources)},
                  start_{std::move(start)},
                  end_{std::move(end)},
                  snapshotSeq_{snapshotSeq},
                  heap_{CursorCompare{&cursors_}} {
                const core::ByteView startView{reinterpret_cast<const std::byte*>(start_.data()), start_.size()};
                const core::ByteView endView{reinterpret_cast<const std::byte*>(end_.data()), end_.size()};

                cursors_.reserve(sources_.size());
                for (const auto& table : sources_) {
                    cursors_.emplace_back();
                    SourceCursor& cursor = cursors_.back();
                    cursor.generator = table->iterator(startView, endView, snapshotSeq);
                    cursor.it = cursor.generator.begin();
                    if (!advance(cursor, false)) {
                        cursors_.pop_back();
                        continue;
                    }
                    heap_.push(cursors_.size() - 1);
                }

                sameKeyIndices_.reserve(cursors_.size());
            }

            [[nodiscard]] bool hasNext() const noexcept { return const_cast<Impl*>(this)->fillPending(); }
            [[nodiscard]] uint64_t snapshotSeq() const noexcept { return snapshotSeq_; }

            [[nodiscard]] std::optional<RecordView> next() noexcept {
                if (!fillPending()) { return std::nullopt; }
                const RecordView out = *pending_;
                pending_.reset();
                return out;
            }

        private:
            struct SourceCursor {
                core::ArenaGenerator<RecordView> generator;
                core::ArenaGenerator<RecordView>::iterator it{};
                RecordView current{};
            };

            struct CursorCompare {
                const std::vector<SourceCursor>* cursors{nullptr};

                [[nodiscard]] bool operator()(size_t lhs, size_t rhs) const noexcept {
                    const RecordView& a = (*cursors)[lhs].current;
                    const RecordView& b = (*cursors)[rhs].current;
                    const int keyCmp = a.compareKey(b);
                    if (keyCmp != 0) { return keyCmp > 0; }
                    return a.seq() < b.seq();
                }
            };

            [[nodiscard]] bool advance(SourceCursor& cursor, bool consumeCurrent) {
                if (consumeCurrent) { ++cursor.it; }
                if (cursor.it == cursor.generator.end()) { return false; }
                cursor.current = *cursor.it;
                return true;
            }

            [[nodiscard]] bool fillPending() {
                if (pending_.has_value()) { return true; }
                if (heap_.empty()) { return false; }

                sameKeyIndices_.clear();

                const size_t firstIdx = heap_.top();
                heap_.pop();

                RecordView best = cursors_[firstIdx].current;
                sameKeyIndices_.push_back(firstIdx);

                while (!heap_.empty()) {
                    const size_t idx = heap_.top();
                    if (cursors_[idx].current.compareKey(best) != 0) { break; }
                    heap_.pop();
                    const RecordView candidate = cursors_[idx].current;
                    if (candidate.seq() > best.seq()) { best = candidate; }
                    sameKeyIndices_.push_back(idx);
                }

                for (const size_t idx : sameKeyIndices_) { if (advance(cursors_[idx], true)) { heap_.push(idx); } }

                pending_ = best;
                return true;
            }

            // Declare locks first so all source generators are destroyed before
            // the scan releases its shard read locks.
            std::vector<std::shared_lock<std::shared_mutex>> scanLocks_;
            std::vector<std::shared_ptr<const IMemTable>> sources_;
            std::vector<uint8_t> start_;
            std::vector<uint8_t> end_;
            uint64_t snapshotSeq_ = 0;
            std::vector<SourceCursor> cursors_;
            std::priority_queue<size_t, std::vector<size_t>, CursorCompare> heap_;
            std::vector<size_t> sameKeyIndices_;
            std::optional<RecordView> pending_;
    };

    class MemTable::Impl {
        public:
            struct Shard {
                mutable std::shared_mutex mutex;
                std::shared_ptr<IMemTable> active;
                std::atomic<IMemTable*> activeRaw{nullptr};
                std::atomic<uint32_t> immutableCount{0};

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
                std::atomic<size_t> approxBytes{0};
                std::atomic<uint64_t> putsApplied{0};
                std::atomic<uint64_t> removesApplied{0};
                size_t activeBytes{0};
                uint64_t nextImmutableId{1};
            };

            class FlushPool {
                public:
                    struct Item {
                        uint32_t shardIndex;
                        uint64_t id;
                        std::shared_ptr<IMemTable> table;
                    };

                    using FlushDone = std::function<void(uint32_t, uint64_t)>;

                    FlushPool(
                        uint32_t workerCount,
                        MemTableFlushInputMode inputMode,
                        FlushCallback callback,
                        StreamingFlushCallback streamingCallback,
                        FlushDone done
                    )
                        : inputMode_{inputMode},
                          callback_{std::move(callback)},
                          streamingCallback_{std::move(streamingCallback)},
                          onDone_{std::move(done)},
                          running_{true} {
                        workers_.reserve(workerCount);
                        for (uint32_t i = 0; i < workerCount; ++i) { workers_.emplace_back([this]() { run(); }); }
                    }

                    ~FlushPool() {
                        {
                            std::lock_guard<std::mutex> lock{mutex_};
                            running_ = false;
                        }
                        cv_.notify_all();
                        for (auto& worker : workers_) { if (worker.joinable()) { worker.join(); } }
                    }

                    FlushPool(const FlushPool&) = delete;
                    FlushPool& operator=(const FlushPool&) = delete;

                    void enqueue(Item item) {
                        {
                            std::lock_guard<std::mutex> lock{mutex_};
                            queue_.push(std::move(item));
                        }
                        cv_.notify_one();
                    }

                    void drain() {
                        std::unique_lock<std::mutex> lock{mutex_};
                        cv_.wait(lock, [this]() { return queue_.empty() && inFlight_ == 0; });
                        rethrowFailureLocked();
                    }

                    void throwIfFailed() {
                        std::lock_guard<std::mutex> lock{mutex_};
                        rethrowFailureLocked();
                    }

                private:
                    void recordFailure(std::exception_ptr failure) noexcept {
                        std::lock_guard<std::mutex> lock{mutex_};
                        if (!failure_) { failure_ = std::move(failure); }
                    }

                    void rethrowFailureLocked() const { if (failure_) { std::rethrow_exception(failure_); } }

                    void run() {
                        for (;;) {
                            Item item;
                            {
                                std::unique_lock<std::mutex> lock{mutex_};
                                cv_.wait(lock, [this]() { return !queue_.empty() || !running_; });
                                if (!running_ && queue_.empty()) { break; }
                                item = std::move(queue_.front());
                                queue_.pop();
                                ++inFlight_;
                            }

                            try {
                                auto records = item.table->iterator(
                                    core::ByteView{},
                                    core::ByteView{},
                                    std::numeric_limits<uint64_t>::max()
                                );
                                if (inputMode_ == MemTableFlushInputMode::STREAMING && streamingCallback_) {
                                    streamingCallback_(item.table->entryCount(), std::move(records));
                                }
                                else if (callback_) {
                                    std::vector<RecordView> materialized;
                                    materialized.reserve(item.table->entryCount());
                                    for (const RecordView& rec : records) { materialized.push_back(rec); }
                                    callback_(std::span<const RecordView>{materialized});
                                }
                                if (onDone_) { onDone_(item.shardIndex, item.id); }
                            }
                            catch (...) {
                                // Keep the immutable table published: callers can still read it and
                                // recovery can replay its WAL record instead of silently losing data.
                                recordFailure(std::current_exception());
                            }

                            {
                                std::lock_guard<std::mutex> lock{mutex_};
                                --inFlight_;
                            }
                            cv_.notify_all();
                        }
                    }

                    MemTableFlushInputMode inputMode_;
                    FlushCallback callback_;
                    StreamingFlushCallback streamingCallback_;
                    FlushDone onDone_;
                    std::mutex mutex_;
                    std::condition_variable cv_;
                    std::queue<Item> queue_;
                    bool running_;
                    size_t inFlight_{0};
                    std::exception_ptr failure_;
                    std::vector<std::thread> workers_;
            };

            explicit Impl(Options options)
                : options_{std::move(options)},
                  shardCount_{resolveShardCount(options_.shardCount, options_.expectedConcurrentWriters, options_.autoShardCountCap)},
                  flushMode_{resolveFlushMode(options_.flushMode, options_.thresholdBytesPerShard)},
                  thresholdBytesPerShard_{options_.thresholdBytesPerShard},
                  seqGen_{1} {
                if (flushMode_ == MemTableFlushMode::BYTES_PER_SHARD && thresholdBytesPerShard_ == 0) {
                    throw std::invalid_argument("MemTable: BYTES_PER_SHARD flush mode requires thresholdBytesPerShard > 0");
                }

                shards_.resize(shardCount_);
                for (uint32_t i = 0; i < shardCount_; ++i) {
                    shards_[i] = std::make_unique<Shard>();
                    auto table = makeBackend();
                    if (!table) { throw std::invalid_argument("MemTable backend factory returned null"); }
                    shards_[i]->active = std::shared_ptr<IMemTable>{std::move(table)};
                    shards_[i]->activeRaw.store(shards_[i]->active.get(), std::memory_order_relaxed);
                    shards_[i]->activeBytes = shards_[i]->active->sizeBytes();
                    shards_[i]->approxBytes.store(shards_[i]->activeBytes, std::memory_order_relaxed);
                    publishTablesLocked(*shards_[i]);
                }

                if (options_.onFlush || options_.onFlushStream) {
                    setFlushCallbacks(options_.onFlush, options_.onFlushStream, options_.flushInputMode);
                }
            }

            [[nodiscard]] std::unique_ptr<IMemTable> makeBackend() const {
                if (options_.backendFactoryWithOptions) { return options_.backendFactoryWithOptions(options_.backendOptions); }
                if (options_.backendFactory) { return options_.backendFactory(); }
                return std::make_unique<SkipListMemTable>(
                    core::BufferArena::DEFAULT_INITIAL_BLOCK_SIZE,
                    core::BufferArena::DEFAULT_MAX_BLOCK_SIZE,
                    64 * 1024,
                    2 * 1024 * 1024,
                    options_.backendOptions
                );
            }

            void put(
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint64_t seq,
                uint8_t flags,
                uint64_t precomputedFp64,
                uint64_t precomputedMk
            ) {
                throwIfFlushFailed();
                const uint64_t fp64 = computeFp64(key, precomputedFp64);
                const uint64_t mini = computeMini(key, precomputedMk);
                const uint32_t shardIndex = shardForHash(shardRouteHash(fp64, key.size()), shardCount_);

                auto& shard = *shards_[shardIndex];
                bool shouldFlush = false;

                {
                    std::unique_lock<std::shared_mutex> lock{shard.mutex};
                    const core::Status st = shard.active->put(toByteView(key), toByteView(value), seq, flags, fp64, mini);
                    if (!st.ok()) { throwStatusError("MemTable::put", st); }
                    const size_t newActiveBytes = shard.active->sizeBytes();
                    const size_t previousActiveBytes = shard.activeBytes;
                    shard.activeBytes = newActiveBytes;

                    size_t totalBytes = shard.approxBytes.load(std::memory_order_relaxed);
                    totalBytes = totalBytes - previousActiveBytes + newActiveBytes;
                    shard.approxBytes.store(totalBytes, std::memory_order_relaxed);
                    shouldFlush = flushMode_ == MemTableFlushMode::BYTES_PER_SHARD && newActiveBytes > thresholdBytesPerShard_;
                }

                shard.putsApplied.fetch_add(1, std::memory_order_relaxed);
                advanceSeq(seq);
                if (shouldFlush) { triggerFlush(shardIndex); }
            }

            void remove(std::span<const uint8_t> key, uint64_t seq, uint64_t precomputedFp64, uint64_t precomputedMk) {
                const uint64_t fp64 = computeFp64(key, precomputedFp64);
                const uint64_t mini = computeMini(key, precomputedMk);
                const uint32_t shardIndex = shardForHash(shardRouteHash(fp64, key.size()), shardCount_);

                put(key, {}, seq, RecordView::FLAG_TOMBSTONE, fp64, mini);
                shards_[shardIndex]->putsApplied.fetch_sub(1, std::memory_order_relaxed);
                shards_[shardIndex]->removesApplied.fetch_add(1, std::memory_order_relaxed);
            }

            [[nodiscard]] bool get(std::span<const uint8_t> key, uint64_t snapshotSeq, RecordView* out, uint64_t precomputedFp64) const {
                if (out == nullptr) { return false; }

                const core::ByteView keyView = toByteView(key);
                const uint64_t fp64 = computeFp64(key, precomputedFp64);
                const uint32_t shardIndex = shardForHash(shardRouteHash(fp64, key.size()), shardCount_);
                const auto& shard = *shards_[shardIndex];

                if (rawActiveGetEnabled_.load(std::memory_order_acquire) && shard.immutableCount.load(std::memory_order_acquire) == 0) {
                    const IMemTable* active = shard.activeRaw.load(std::memory_order_acquire);
                    return active != nullptr && active->get(keyView, snapshotSeq, out);
                }

                const auto published = shard.published.load(std::memory_order_acquire);
                if (!published) { return false; }

                if (published->active && published->active->get(keyView, snapshotSeq, out)) { return true; }

                for (const auto& immutable : published->immutables) {
                    if (immutable && immutable->get(keyView, snapshotSeq, out)) { return true; }
                }
                return false;
            }

            [[nodiscard]] std::optional<bool> getInto(std::span<const uint8_t> key, uint64_t snapshotSeq, std::vector<uint8_t>& out) const {
                RecordView view;
                if (!get(key, snapshotSeq, &view, 0)) { return std::nullopt; }
                if (view.isTombstone()) { return false; }
                const auto value = view.value();
                out.assign(value.begin(), value.end());
                return true;
            }

            [[nodiscard]] std::optional<bool> contains(std::span<const uint8_t> key, uint64_t snapshotSeq) const {
                RecordView view;
                if (!get(key, snapshotSeq, &view, 0)) { return std::nullopt; }
                return !view.isTombstone();
            }

            [[nodiscard]] RangeIterator makeIterator(
                const KeyRange& range,
                uint64_t snapshotSeq,
                std::vector<std::shared_lock<std::shared_mutex>> scanLocks = {}
            ) const {
                std::vector<std::shared_ptr<const IMemTable>> sources;
                sources.reserve(shardCount_ * 2);

                for (const auto& shardPtr : shards_) {
                    const auto published = shardPtr->published.load(std::memory_order_acquire);
                    if (!published) { continue; }
                    if (published->active) { sources.push_back(published->active); }
                    for (const auto& immutable : published->immutables) { if (immutable) { sources.push_back(immutable); } }
                }

                return RangeIterator{
                    std::make_unique<RangeIterator::Impl>(std::move(scanLocks), std::move(sources), range.start, range.end, snapshotSeq)
                };
            }

            [[nodiscard]] RangeIterator iterator(const KeyRange& range, uint64_t snapshotSeq) const {
                return makeIterator(range, snapshotSeq);
            }

            [[nodiscard]] RangeIterator pinnedIterator(const KeyRange& range, const std::function<uint64_t()>& snapshotSeqProvider) const {
                std::vector<std::shared_lock<std::shared_mutex>> scanLocks;
                scanLocks.reserve(shards_.size());
                for (const auto& shard : shards_) { scanLocks.emplace_back(shard->mutex); }
                return makeIterator(range, snapshotSeqProvider(), std::move(scanLocks));
            }

            [[nodiscard]] uint64_t nextSeq() noexcept { return seqGen_.fetch_add(1, std::memory_order_relaxed); }

            [[nodiscard]] uint64_t reserveSeq(uint64_t count) {
                if (count == 0) { return seqGen_.load(std::memory_order_relaxed); }
                return seqGen_.fetch_add(count, std::memory_order_relaxed);
            }

            [[nodiscard]] uint64_t lastSeq() const noexcept { return seqGen_.load(std::memory_order_relaxed); }

            void advanceSeq(uint64_t observedSeq) noexcept {
                uint64_t current = seqGen_.load(std::memory_order_relaxed);
                while (current <= observedSeq) {
                    if (seqGen_.compare_exchange_weak(current, observedSeq + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
                        break;
                    }
                }
            }

            void flushHint() {
                throwIfFlushFailed();
                if (flushMode_ != MemTableFlushMode::BYTES_PER_SHARD) { return; }
                for (uint32_t i = 0; i < shardCount_; ++i) {
                    bool overThreshold = false;
                    {
                        std::shared_lock<std::shared_mutex> lock{shards_[i]->mutex};
                        overThreshold = shards_[i]->activeBytes > thresholdBytesPerShard_;
                    }
                    if (overThreshold) { triggerFlush(i); }
                }
            }

            void forceFlush() {
                throwIfFlushFailed();
                for (uint32_t i = 0; i < shardCount_; ++i) { triggerFlush(i); }
                if (flushPool_) { flushPool_->drain(); }
            }

            void throwIfFlushFailed() const { if (flushPool_) { flushPool_->throwIfFailed(); } }

            void setFlushCallback(const FlushCallback& cb) { setFlushCallbacks(cb, nullptr, MemTableFlushInputMode::MATERIALIZE_VECTOR); }

            void setStreamingFlushCallback(const StreamingFlushCallback& cb) {
                setFlushCallbacks(nullptr, cb, MemTableFlushInputMode::STREAMING);
            }

            void setFlushCallbacks(FlushCallback callback, StreamingFlushCallback streamingCallback, MemTableFlushInputMode inputMode) {
                rawActiveGetEnabled_.store(false, std::memory_order_release);
                if (flushPool_) { flushPool_->drain(); }

                flushPool_.reset();

                if (!callback && !streamingCallback) {
                    rawActiveGetEnabled_.store(true, std::memory_order_release);
                    return;
                }

                flushPool_ = std::make_unique<FlushPool>(
                    resolveFlushWorkerCount(shardCount_),
                    inputMode,
                    std::move(callback),
                    std::move(streamingCallback),
                    [this](uint32_t shardIndex, uint64_t immutableId) { onFlushed(shardIndex, immutableId); }
                );
            }

            [[nodiscard]] size_t approxSize() const noexcept {
                size_t total = 0;
                for (const auto& shard : shards_) { total += shard->approxBytes.load(std::memory_order_relaxed); }
                return total;
            }

            [[nodiscard]] MemTableSnapshot snapshot() const noexcept {
                uint64_t puts = 0;
                uint64_t removes = 0;
                uint64_t immutables = 0;
                for (const auto& shard : shards_) {
                    puts += shard->putsApplied.load(std::memory_order_relaxed);
                    removes += shard->removesApplied.load(std::memory_order_relaxed);
                    immutables += shard->immutableCount.load(std::memory_order_relaxed);
                }
                return {
                    shardCount_,
                    static_cast<uint64_t>(thresholdBytesPerShard_),
                    static_cast<uint64_t>(approxSize()),
                    puts,
                    removes,
                    flushesCompleted_.load(std::memory_order_relaxed),
                    immutables,
                };
            }

        private:
            static void publishTablesLocked(Shard& shard) {
                auto published = std::make_shared<Shard::PublishedTables>();
                published->active = std::const_pointer_cast<const IMemTable>(shard.active);
                published->immutables.reserve(shard.immutables.size());
                for (auto it = shard.immutables.rbegin(); it != shard.immutables.rend(); ++it) {
                    published->immutables.push_back(std::const_pointer_cast<const IMemTable>(it->table));
                }
                shard.immutableCount.store(static_cast<uint32_t>(published->immutables.size()), std::memory_order_release);
                std::shared_ptr<const Shard::PublishedTables> publishedConst = std::move(published);
                shard.published.store(std::move(publishedConst), std::memory_order_release);
            }

            void triggerFlush(uint32_t shardIndex) {
                if (!flushPool_) { return; }

                auto& shard = *shards_[shardIndex];
                std::shared_ptr<IMemTable> sealed;
                uint64_t immutableId = 0;
                size_t sealedBytes = 0;

                {
                    std::unique_lock<std::shared_mutex> lock{shard.mutex};
                    if (shard.active->entryCount() == 0) { return; }

                    shard.active->freeze();
                    sealed = shard.active;
                    sealedBytes = shard.activeBytes;

                    auto newActive = makeBackend();
                    if (!newActive) { throw std::invalid_argument("MemTable backend factory returned null"); }
                    immutableId = shard.nextImmutableId++;
                    shard.immutables.emplace_back(Shard::Immutable{immutableId, sealed, sealedBytes});
                    shard.immutableCount.store(static_cast<uint32_t>(shard.immutables.size()), std::memory_order_release);

                    shard.active = std::shared_ptr<IMemTable>{std::move(newActive)};
                    shard.activeRaw.store(shard.active.get(), std::memory_order_release);
                    shard.activeBytes = shard.active->sizeBytes();

                    size_t totalBytes = shard.approxBytes.load(std::memory_order_relaxed);
                    totalBytes = totalBytes - sealedBytes + shard.activeBytes + sealedBytes;
                    shard.approxBytes.store(totalBytes, std::memory_order_relaxed);

                    publishTablesLocked(shard);
                }

                flushPool_->enqueue(FlushPool::Item{shardIndex, immutableId, std::move(sealed)});
            }

            void onFlushed(uint32_t shardIndex, uint64_t immutableId) {
                auto& shard = *shards_[shardIndex];
                std::unique_lock<std::shared_mutex> lock{shard.mutex};
                std::erase_if(
                    shard.immutables,
                    [&](const Shard::Immutable& item) {
                        if (item.id != immutableId) { return false; }
                        const size_t total = shard.approxBytes.load(std::memory_order_relaxed);
                        shard.approxBytes.store(total >= item.bytes ? total - item.bytes : 0, std::memory_order_relaxed);
                        return true;
                    }
                );
                publishTablesLocked(shard);
                flushesCompleted_.fetch_add(1, std::memory_order_relaxed);
            }

            Options options_;
            uint32_t shardCount_;
            MemTableFlushMode flushMode_;
            size_t thresholdBytesPerShard_;
            std::vector<std::unique_ptr<Shard>> shards_;
            std::unique_ptr<FlushPool> flushPool_;

            std::atomic<uint64_t> seqGen_;
            std::atomic<uint64_t> flushesCompleted_{0};
            std::atomic<bool> rawActiveGetEnabled_{true};
    };

    MemTable::RangeIterator::RangeIterator(std::unique_ptr<Impl> impl) : impl_{std::move(impl)} {}

    MemTable::RangeIterator::~RangeIterator() = default;
    MemTable::RangeIterator::RangeIterator(RangeIterator&&) noexcept = default;
    MemTable::RangeIterator& MemTable::RangeIterator::operator=(RangeIterator&&) noexcept = default;

    bool MemTable::RangeIterator::hasNext() const noexcept { return impl_ && impl_->hasNext(); }

    std::optional<RecordView> MemTable::RangeIterator::next() noexcept {
        if (!impl_) { return std::nullopt; }
        return impl_->next();
    }

    uint64_t MemTable::RangeIterator::snapshotSeq() const noexcept { return impl_ ? impl_->snapshotSeq() : 0; }

    std::unique_ptr<MemTable> MemTable::create() { return create(Options{}); }

    std::unique_ptr<MemTable> MemTable::create(const Options& options) { return std::unique_ptr<MemTable>{new MemTable(options)}; }

    MemTable::MemTable(const Options& options) : impl_{std::make_unique<Impl>(options)} {}

    MemTable::~MemTable() = default;

    void MemTable::put(
        std::span<const uint8_t> key,
        std::span<const uint8_t> value,
        uint64_t seq,
        uint8_t flags,
        uint64_t precomputedFp64,
        uint64_t precomputedMk
    ) { impl_->put(key, value, seq, flags, precomputedFp64, precomputedMk); }

    void MemTable::remove(std::span<const uint8_t> key, uint64_t seq, uint64_t precomputedFp64, uint64_t precomputedMk) {
        impl_->remove(key, seq, precomputedFp64, precomputedMk);
    }

    void MemTable::advanceSeq(uint64_t seq) noexcept { impl_->advanceSeq(seq); }

    bool MemTable::get(std::span<const uint8_t> key, uint64_t snapshotSeq, RecordView* out) const {
        return impl_->get(key, snapshotSeq, out, 0);
    }

    bool MemTable::get(std::span<const uint8_t> key, uint64_t snapshotSeq, RecordView* out, uint64_t precomputedFp64) const {
        return impl_->get(key, snapshotSeq, out, precomputedFp64);
    }

    std::optional<bool> MemTable::getInto(std::span<const uint8_t> key, uint64_t snapshotSeq, std::vector<uint8_t>& out) const {
        return impl_->getInto(key, snapshotSeq, out);
    }

    std::optional<bool> MemTable::contains(std::span<const uint8_t> key, uint64_t snapshotSeq) const {
        return impl_->contains(key, snapshotSeq);
    }

    MemTable::RangeIterator MemTable::iterator(const KeyRange& range, uint64_t snapshotSeq) const {
        return impl_->iterator(range, snapshotSeq);
    }

    MemTable::RangeIterator MemTable::pinnedIterator(const KeyRange& range, const std::function<uint64_t()>& snapshotSeqProvider) const {
        return impl_->pinnedIterator(range, snapshotSeqProvider);
    }

    uint64_t MemTable::nextSeq() noexcept { return impl_->nextSeq(); }

    uint64_t MemTable::reserveSeq(uint64_t count) { return impl_->reserveSeq(count); }

    uint64_t MemTable::lastSeq() const noexcept { return impl_->lastSeq(); }

    void MemTable::flushHint() { impl_->flushHint(); }

    void MemTable::forceFlush() { impl_->forceFlush(); }

    void MemTable::throwIfFlushFailed() const { impl_->throwIfFlushFailed(); }

    void MemTable::setFlushCallback(const FlushCallback& cb) { impl_->setFlushCallback(cb); }

    void MemTable::setStreamingFlushCallback(const StreamingFlushCallback& cb) { impl_->setStreamingFlushCallback(cb); }

    size_t MemTable::approxSize() const noexcept { return impl_->approxSize(); }

    MemTable::MemTableSnapshot MemTable::snapshot() const noexcept { return impl_->snapshot(); }
} // namespace akkaradb::engine::memtable

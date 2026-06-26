/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// benchmarks/smoke/memtable_smoke_test.cpp
#include "TestErrorHandlers.hpp"

#include "akk/core/record/RecordView.hpp"
#include "akk/core/types/ByteView.hpp"
#include "akk/core/utils/ArenaGenerator.hpp"
#include "akk/engine/memtable/ARTMemTable.hpp"
#include "akk/engine/memtable/BPTreeMemTable.hpp"
#include "akk/engine/memtable/MemTable.hpp"
#include "akk/engine/memtable/SkipListMemTable.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

using namespace akkaradb::core;
using namespace akkaradb::engine;
using namespace akkaradb::engine::memtable;

namespace {
    static std::span<const uint8_t> asU8(std::string_view sv) {
        return {reinterpret_cast<const uint8_t*>(sv.data()), sv.size()};
    }

    static ByteView asBv(std::string_view sv) {
        return {reinterpret_cast<const std::byte*>(sv.data()), sv.size()};
    }

    static std::string to_string(std::span<const uint8_t> value) {
        return {reinterpret_cast<const char*>(value.data()), value.size()};
    }

    using BackendFactory = std::function<std::unique_ptr<IMemTable>()>;

    static void runContractTestsForBackend(const BackendFactory& makeBackend) {
        {
            auto memtable = makeBackend();
            RecordView out;

            AKK_TEST_CHECK(memtable->put(asBv("k"), asBv("v1"), 1, 0).ok());
            AKK_TEST_CHECK(memtable->put(asBv("k"), asBv("v2"), 2, 0).ok());

            AKK_TEST_CHECK(memtable->get(asBv("k"), 1, &out));
            AKK_TEST_CHECK(to_string(out.value()) == "v1");

            AKK_TEST_CHECK(memtable->get(asBv("k"), 2, &out));
            AKK_TEST_CHECK(to_string(out.value()) == "v2");
        }

        {
            auto memtable = makeBackend();
            RecordView out;

            AKK_TEST_CHECK(memtable->put(asBv("key"), asBv("v1"), 1, 0).ok());
            AKK_TEST_CHECK(memtable->put(asBv("key"), asBv("v2"), 2, 0).ok());
            AKK_TEST_CHECK(memtable->put(asBv("key"), asBv("v3"), 3, 0).ok());
            AKK_TEST_CHECK(memtable->put(asBv("key"), asBv("v4"), 4, 0).ok());
            AKK_TEST_CHECK(memtable->put(asBv("key"), asBv("v5"), 5, 0).ok());

            AKK_TEST_CHECK(!memtable->get(asBv("key"), 1, &out));
            AKK_TEST_CHECK(memtable->get(asBv("key"), 2, &out));
            AKK_TEST_CHECK(to_string(out.value()) == "v2");
            AKK_TEST_CHECK(memtable->get(asBv("key"), 5, &out));
            AKK_TEST_CHECK(to_string(out.value()) == "v5");
            AKK_TEST_CHECK(memtable->entryCount() == 4);
        }

        {
            auto memtable = makeBackend();
            RecordView out;

            AKK_TEST_CHECK(memtable->put(asBv("dead"), asBv("alive"), 1, 0).ok());
            AKK_TEST_CHECK(memtable->put(asBv("dead"), ByteView{}, 2, RecordView::FLAG_TOMBSTONE).ok());

            AKK_TEST_CHECK(memtable->get(asBv("dead"), 2, &out));
            AKK_TEST_CHECK(out.isTombstone());
        }

        {
            auto memtable = makeBackend();
            memtable->freeze();
            const Status st = memtable->put(asBv("k"), asBv("v"), 1, 0);
            AKK_TEST_CHECK(!st.ok());
        }

        {
            auto memtable = makeBackend();
            AKK_TEST_CHECK(memtable->put(asBv("b"), asBv("1"), 1, 0).ok());
            AKK_TEST_CHECK(memtable->put(asBv("a"), asBv("2"), 2, 0).ok());
            AKK_TEST_CHECK(memtable->put(asBv("c"), asBv("3"), 3, 0).ok());

            std::vector<std::string> keys;
            for (const RecordView& rec : memtable->iterator(ByteView{}, ByteView{}, 3)) {
                keys.emplace_back(reinterpret_cast<const char*>(rec.key().data()), rec.key().size());
            }

            AKK_TEST_CHECK(keys.size() == 3);
            AKK_TEST_CHECK(keys[0] == "a");
            AKK_TEST_CHECK(keys[1] == "b");
            AKK_TEST_CHECK(keys[2] == "c");
        }
    }

    static void runSingleWriterMultiReaderGetStress(const BackendFactory& makeBackend) {
        auto memtable = makeBackend();
        std::atomic<bool> stop{false};
        std::atomic<uint64_t> latestSeq{0};
        std::atomic<uint64_t> readHits{0};

        std::thread writer([&]() {
            for (uint64_t seq = 1; seq <= 2000; ++seq) {
                std::string value = "v" + std::to_string(seq);
                const Status st = memtable->put(asBv("shared"), asBv(value), seq, 0);
                AKK_TEST_CHECK(st.ok());
                latestSeq.store(seq, std::memory_order_release);
            }
            stop.store(true, std::memory_order_release);
        });

        std::vector<std::thread> readers;
        readers.reserve(2);
        for (int i = 0; i < 2; ++i) {
            readers.emplace_back([&]() {
                RecordView out;
                size_t spin = 0;
                while (!stop.load(std::memory_order_acquire)) {
                    const uint64_t snapshot = latestSeq.load(std::memory_order_acquire);
                    if (snapshot == 0) {
                        continue;
                    }
                    if (memtable->get(asBv("shared"), snapshot, &out)) {
                        readHits.fetch_add(1, std::memory_order_relaxed);
                    }
                    if ((++spin & 0x3FF) == 0) {
                        std::this_thread::yield();
                    }
                }
            });
        }

        writer.join();
        for (auto& th : readers) {
            th.join();
        }

        AKK_TEST_CHECK(readHits.load(std::memory_order_relaxed) > 0);
    }

    static void runWriterIteratorStress(const BackendFactory& makeBackend) {
        auto memtable = makeBackend();
        std::atomic<bool> stop{false};
        std::atomic<uint64_t> latestSeq{0};
        std::atomic<uint64_t> iterSteps{0};

        std::thread writer([&]() {
            for (uint64_t seq = 1; seq <= 2000; ++seq) {
                const std::string key = "k" + std::to_string(seq % 128);
                const std::string value = "v" + std::to_string(seq);
                const Status st = memtable->put(asBv(key), asBv(value), seq, 0);
                AKK_TEST_CHECK(st.ok());
                latestSeq.store(seq, std::memory_order_release);
            }
            stop.store(true, std::memory_order_release);
        });

        std::thread iterReader([&]() {
            while (!stop.load(std::memory_order_acquire)) {
                const uint64_t snapshot = latestSeq.load(std::memory_order_acquire);
                if (snapshot == 0) {
                    continue;
                }
                std::string prevKey;
                bool first = true;
                for (const RecordView& rec : memtable->iterator(ByteView{}, ByteView{}, snapshot)) {
                    const std::string key{
                        reinterpret_cast<const char*>(rec.key().data()),
                        rec.key().size()
                    };
                    if (!first) {
                        AKK_TEST_CHECK(prevKey <= key);
                    }
                    AKK_TEST_CHECK(rec.seq() <= snapshot);
                    prevKey = key;
                    first = false;
                    iterSteps.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });

        writer.join();
        iterReader.join();
        AKK_TEST_CHECK(iterSteps.load(std::memory_order_relaxed) > 0);
    }

    static std::vector<std::string> collectRangeKeys(
        IMemTable& memtable,
        std::string_view start,
        std::string_view end,
        uint64_t snapshotSeq
    ) {
        std::vector<std::string> keys;
        for (const RecordView& rec : memtable.iterator(asBv(start), asBv(end), snapshotSeq)) {
            keys.emplace_back(reinterpret_cast<const char*>(rec.key().data()), rec.key().size());
        }
        return keys;
    }

    static void runRangeBoundaryTestsForBackend(const BackendFactory& makeBackend) {
        auto memtable = makeBackend();
        AKK_TEST_CHECK(memtable->put(asBv("a"), asBv("v1"), 1, 0).ok());
        AKK_TEST_CHECK(memtable->put(asBv("ab"), asBv("v2"), 2, 0).ok());
        AKK_TEST_CHECK(memtable->put(asBv("ac"), asBv("v3"), 3, 0).ok());
        AKK_TEST_CHECK(memtable->put(asBv("b"), asBv("v4"), 4, 0).ok());

        {
            const auto keys = collectRangeKeys(*memtable, "", "", 4);
            AKK_TEST_CHECK((keys == std::vector<std::string>{"a", "ab", "ac", "b"}));
        }
        {
            const auto keys = collectRangeKeys(*memtable, "ab", "", 4);
            AKK_TEST_CHECK((keys == std::vector<std::string>{"ab", "ac", "b"}));
        }
        {
            const auto keys = collectRangeKeys(*memtable, "", "ac", 4);
            AKK_TEST_CHECK((keys == std::vector<std::string>{"a", "ab"}));
        }
        {
            const auto keys = collectRangeKeys(*memtable, "ab", "ab", 4);
            AKK_TEST_CHECK(keys.empty());
        }
        {
            const auto keys = collectRangeKeys(*memtable, "ac", "ab", 4);
            AKK_TEST_CHECK(keys.empty());
        }
        {
            const auto keys = collectRangeKeys(*memtable, "ab", "ac", 4);
            AKK_TEST_CHECK((keys == std::vector<std::string>{"ab"}));
        }
    }

    void testGeneratorYieldAll() {
        BufferArena arena{16 * 1024, 128 * 1024};

        auto gen1 = ArenaGenerator<int>::withArena(arena, []() -> ArenaGenerator<int> {
            co_yield 1;
            co_yield 2;
        });
        auto gen2 = ArenaGenerator<int>::withArena(arena, []() -> ArenaGenerator<int> {
            co_yield 3;
            co_yield 4;
        });
        auto gen3 = ArenaGenerator<int>::withArena(arena, []() -> ArenaGenerator<int> {
            co_yield 5;
        });
        auto gen4 = ArenaGenerator<int>::withArena(arena, []() -> ArenaGenerator<int> {
            co_yield 6;
            co_yield 7;
        });

        auto merged = ArenaGenerator<int>::yieldAll(
            arena,
            std::move(gen1),
            std::move(gen2),
            std::move(gen3),
            std::move(gen4)
        );
        std::vector<int> values;
        for (int value : merged) {
            values.push_back(value);
        }

        AKK_TEST_CHECK(values.size() == 7);
        AKK_TEST_CHECK(values[0] == 1);
        AKK_TEST_CHECK(values[1] == 2);
        AKK_TEST_CHECK(values[2] == 3);
        AKK_TEST_CHECK(values[3] == 4);
        AKK_TEST_CHECK(values[4] == 5);
        AKK_TEST_CHECK(values[5] == 6);
        AKK_TEST_CHECK(values[6] == 7);
    }

    void testBackendContractsSkiplist() {
        runContractTestsForBackend([]() { return std::make_unique<SkipListMemTable>(); });
    }

    void testBackendContractsBptree() {
        runContractTestsForBackend([]() { return std::make_unique<BPTreeMemTable>(); });
    }

    void testBackendContractsArt() {
        runContractTestsForBackend([]() { return std::make_unique<ARTMemTable>(); });
    }

    void testBackendSingleWriterMultiReaderStress() {
        runSingleWriterMultiReaderGetStress([]() { return std::make_unique<SkipListMemTable>(); });
        runSingleWriterMultiReaderGetStress([]() { return std::make_unique<BPTreeMemTable>(); });
        runSingleWriterMultiReaderGetStress([]() { return std::make_unique<ARTMemTable>(); });
    }

    void testBackendWriterIteratorStress() {
        runWriterIteratorStress([]() { return std::make_unique<BPTreeMemTable>(); });
        runWriterIteratorStress([]() { return std::make_unique<ARTMemTable>(); });
    }

    void testBackendRangeBoundaries() {
        runRangeBoundaryTestsForBackend([]() { return std::make_unique<BPTreeMemTable>(); });
        runRangeBoundaryTestsForBackend([]() { return std::make_unique<ARTMemTable>(); });
    }

    void testShardedMemtableGetIntoAndContains() {
        memtable::MemTable::Options opts;
        opts.shardCount = 4;
        auto table = memtable::MemTable::create(opts);

        const uint64_t s1 = table->nextSeq();
        table->put(asU8("alpha"), asU8("v1"), s1);
        const uint64_t s2 = table->reserveSeq(2);
        table->remove(asU8("alpha"), s2);
        const uint64_t s3 = s2 + 1;
        table->put(asU8("beta"), asU8("v2"), s3);
        AKK_TEST_CHECK(table->nextSeq() == s3 + 1);

        std::vector<uint8_t> out;
        auto r1 = table->getInto(asU8("beta"), s3, out);
        AKK_TEST_CHECK(r1.has_value() && *r1);
        AKK_TEST_CHECK(to_string(out) == "v2");

        auto r2 = table->getInto(asU8("alpha"), s3, out);
        AKK_TEST_CHECK(r2.has_value() && !*r2);

        auto r3 = table->contains(asU8("gamma"), s3);
        AKK_TEST_CHECK(!r3.has_value());
    }

    void testShardedMemtableForceFlushAndCallback() {
        std::mutex mu;
        std::condition_variable cv;
        std::vector<std::string> flushedKeys;
        bool flushed = false;

        memtable::MemTable::Options opts;
        opts.shardCount = 2;
        opts.thresholdBytesPerShard = 1;
        opts.backendFactory = []() { return std::make_unique<BPTreeMemTable>(); };
        opts.onFlush = [&](std::span<const RecordView> batch) {
            std::lock_guard<std::mutex> lock{mu};
            for (const RecordView& rec : batch) {
                flushedKeys.emplace_back(
                    reinterpret_cast<const char*>(rec.key().data()),
                    rec.key().size()
                );
            }
            flushed = true;
            cv.notify_one();
        };

        auto table = memtable::MemTable::create(opts);
        table->put(asU8("k1"), asU8("v1"), table->nextSeq());
        table->flushHint();
        table->forceFlush();

        {
            std::unique_lock<std::mutex> lock{mu};
            cv.wait_for(lock, std::chrono::seconds(1), [&]() { return flushed; });
        }
        AKK_TEST_CHECK(flushed);
        AKK_TEST_CHECK(!flushedKeys.empty());
    }

    void testShardedMemtableRangeMerge() {
        memtable::MemTable::Options opts;
        opts.shardCount = 1;
        opts.backendFactory = []() { return std::make_unique<BPTreeMemTable>(); };
        auto table = memtable::MemTable::create(opts);

        table->put(asU8("b"), asU8("v1"), table->nextSeq());
        table->put(asU8("a"), asU8("v2"), table->nextSeq());
        table->put(asU8("c"), asU8("v3"), table->nextSeq());
        table->put(asU8("b"), asU8("v4"), table->nextSeq());

        memtable::MemTable::KeyRange range;
        range.start = std::vector<uint8_t>{'a'};
        range.end = std::vector<uint8_t>{'z'};

        auto it = table->iterator(range, table->lastSeq());
        std::vector<std::string> pairs;
        while (it.hasNext()) {
            const auto rec = it.next();
            AKK_TEST_CHECK(rec.has_value());
            pairs.emplace_back(
                std::string(reinterpret_cast<const char*>(rec->key().data()), rec->key().size()) + ":" +
                std::string(reinterpret_cast<const char*>(rec->value().data()), rec->value().size())
            );
        }

        AKK_TEST_CHECK(pairs.size() == 3);
        AKK_TEST_CHECK(pairs[0] == "a:v2");
        AKK_TEST_CHECK(pairs[1] == "b:v4");
        AKK_TEST_CHECK(pairs[2] == "c:v3");
    }

    void testShardedMemtableBptreeFlushReaderIteratorSafety() {
        memtable::MemTable::Options opts;
        opts.shardCount = 4;
        opts.thresholdBytesPerShard = 1024;
        opts.backendFactory = []() { return std::make_unique<BPTreeMemTable>(); };
        opts.onFlush = [](std::span<const RecordView>) {};
        auto table = memtable::MemTable::create(opts);

        std::atomic<bool> stop{false};
        std::atomic<uint64_t> latestSeq{0};
        std::atomic<uint64_t> reads{0};
        std::atomic<uint64_t> iters{0};

        std::thread writer([&]() {
            for (uint64_t i = 1; i <= 2500; ++i) {
                const uint64_t seq = table->nextSeq();
                const std::string key = "key" + std::to_string(i % 256);
                const std::string value = "value" + std::to_string(i);
                table->put(asU8(key), asU8(value), seq);
                latestSeq.store(seq, std::memory_order_release);
                if ((i % 128) == 0) {
                    table->flushHint();
                }
            }
            stop.store(true, std::memory_order_release);
        });

        std::thread reader([&]() {
            RecordView out;
            memtable::MemTable::KeyRange range;
            while (!stop.load(std::memory_order_acquire)) {
                const uint64_t snapshot = latestSeq.load(std::memory_order_acquire);
                if (snapshot == 0) {
                    continue;
                }
                if (table->get(asU8("key1"), snapshot, &out)) {
                    reads.fetch_add(1, std::memory_order_relaxed);
                }
                auto it = table->iterator(range, snapshot);
                while (it.hasNext()) {
                    const auto rec = it.next();
                    AKK_TEST_CHECK(rec.has_value());
                    AKK_TEST_CHECK(rec->seq() <= snapshot);
                    iters.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });

        writer.join();
        table->forceFlush();
        reader.join();

        AKK_TEST_CHECK(reads.load(std::memory_order_relaxed) > 0);
        AKK_TEST_CHECK(iters.load(std::memory_order_relaxed) > 0);
    }

    void testShardedMemtableConcurrencySmoke() {
        memtable::MemTable::Options opts;
        opts.shardCount = 4;
        auto table = memtable::MemTable::create(opts);

        std::atomic<bool> stop{false};
        std::atomic<uint64_t> latestSeq{0};
        std::atomic<uint64_t> reads{0};

        std::thread writer([&]() {
            for (uint64_t i = 0; i < 2000; ++i) {
                const uint64_t seq = table->nextSeq();
                const std::string value = "v" + std::to_string(seq);
                table->put(asU8("shared"), asU8(value), seq);
                latestSeq.store(seq, std::memory_order_release);
            }
            stop.store(true, std::memory_order_release);
        });

        std::thread reader([&]() {
            RecordView out;
            while (!stop.load(std::memory_order_acquire)) {
                const uint64_t snap = latestSeq.load(std::memory_order_acquire);
                if (snap == 0) {
                    continue;
                }
                if (table->get(asU8("shared"), snap, &out)) {
                    reads.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });

        writer.join();
        reader.join();
        AKK_TEST_CHECK(reads.load(std::memory_order_relaxed) > 0);
    }

    void testShardedMemtableAutoShardDerivation() {
        memtable::MemTable::Options opts;
        opts.shardCount = 0;
        opts.expectedConcurrentWriters = 4;

        auto table = memtable::MemTable::create(opts);
        const auto snap = table->snapshot();
        // writers * 4 = 16, already power-of-two
        AKK_TEST_CHECK(snap.shardCount == 16);
    }
} // namespace

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();

    std::puts("[1/13] backend contracts (skiplist)");
    std::fflush(stdout);
    testBackendContractsSkiplist();

    std::puts("[2/13] backend contracts (bptree)");
    std::fflush(stdout);
    testBackendContractsBptree();

    std::puts("[3/13] backend contracts (art)");
    std::fflush(stdout);
    testBackendContractsArt();

    std::puts("[4/13] ArenaGenerator::yieldAll");
    std::fflush(stdout);
    testGeneratorYieldAll();

    std::puts("[5/13] backend single-writer/multi-reader get stress");
    std::fflush(stdout);
    testBackendSingleWriterMultiReaderStress();

    std::puts("[6/13] backend writer+iterator stress");
    std::fflush(stdout);
    testBackendWriterIteratorStress();

    std::puts("[7/13] backend range boundary iterator");
    std::fflush(stdout);
    testBackendRangeBoundaries();

    std::puts("[8/13] sharded memtable getInto/contains");
    std::fflush(stdout);
    testShardedMemtableGetIntoAndContains();

    std::puts("[9/13] sharded memtable forceFlush (bptree)");
    std::fflush(stdout);
    testShardedMemtableForceFlushAndCallback();

    std::puts("[10/13] sharded memtable range merge (bptree)");
    std::fflush(stdout);
    testShardedMemtableRangeMerge();

    std::puts("[11/13] sharded memtable bptree flush/read/iterator safety");
    std::fflush(stdout);
    testShardedMemtableBptreeFlushReaderIteratorSafety();

    std::puts("[12/13] sharded memtable concurrency");
    std::fflush(stdout);
    testShardedMemtableConcurrencySmoke();

    std::puts("[13/13] sharded memtable auto shard derivation");
    std::fflush(stdout);
    testShardedMemtableAutoShardDerivation();

    std::puts("memtable smoke test passed");
    return 0;
}
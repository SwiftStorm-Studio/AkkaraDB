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
#include <cassert>
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

namespace {
    static std::span<const uint8_t> as_u8(std::string_view sv) {
        return {reinterpret_cast<const uint8_t*>(sv.data()), sv.size()};
    }

    static ByteView as_bv(std::string_view sv) {
        return {reinterpret_cast<const std::byte*>(sv.data()), sv.size()};
    }

    static std::string to_string(std::span<const uint8_t> value) {
        return {reinterpret_cast<const char*>(value.data()), value.size()};
    }

    using BackendFactory = std::function<std::unique_ptr<IMemTable>()>;

    static void run_contract_tests_for_backend(const BackendFactory& make_backend) {
        {
            auto memtable = make_backend();
            RecordView out;

            assert(memtable->put(as_bv("k"), as_bv("v1"), 1, 0).ok());
            assert(memtable->put(as_bv("k"), as_bv("v2"), 2, 0).ok());

            assert(memtable->get(as_bv("k"), 1, &out));
            assert(to_string(out.value()) == "v1");

            assert(memtable->get(as_bv("k"), 2, &out));
            assert(to_string(out.value()) == "v2");
        }

        {
            auto memtable = make_backend();
            RecordView out;

            assert(memtable->put(as_bv("key"), as_bv("v1"), 1, 0).ok());
            assert(memtable->put(as_bv("key"), as_bv("v2"), 2, 0).ok());
            assert(memtable->put(as_bv("key"), as_bv("v3"), 3, 0).ok());
            assert(memtable->put(as_bv("key"), as_bv("v4"), 4, 0).ok());
            assert(memtable->put(as_bv("key"), as_bv("v5"), 5, 0).ok());

            assert(!memtable->get(as_bv("key"), 1, &out));
            assert(memtable->get(as_bv("key"), 2, &out));
            assert(to_string(out.value()) == "v2");
            assert(memtable->get(as_bv("key"), 5, &out));
            assert(to_string(out.value()) == "v5");
            assert(memtable->entryCount() == 4);
        }

        {
            auto memtable = make_backend();
            RecordView out;

            assert(memtable->put(as_bv("dead"), as_bv("alive"), 1, 0).ok());
            assert(memtable->put(as_bv("dead"), ByteView{}, 2, RecordView::FLAG_TOMBSTONE).ok());

            assert(memtable->get(as_bv("dead"), 2, &out));
            assert(out.is_tombstone());
        }

        {
            auto memtable = make_backend();
            memtable->freeze();
            const Status st = memtable->put(as_bv("k"), as_bv("v"), 1, 0);
            assert(!st.ok());
        }

        {
            auto memtable = make_backend();
            assert(memtable->put(as_bv("b"), as_bv("1"), 1, 0).ok());
            assert(memtable->put(as_bv("a"), as_bv("2"), 2, 0).ok());
            assert(memtable->put(as_bv("c"), as_bv("3"), 3, 0).ok());

            std::vector<std::string> keys;
            for (const RecordView& rec : memtable->iterator(ByteView{}, ByteView{}, 3)) {
                keys.emplace_back(reinterpret_cast<const char*>(rec.key().data()), rec.key().size());
            }

            assert(keys.size() == 3);
            assert(keys[0] == "a");
            assert(keys[1] == "b");
            assert(keys[2] == "c");
        }
    }

    static void run_single_writer_multi_reader_get_stress(const BackendFactory& make_backend) {
        auto memtable = make_backend();
        std::atomic<bool> stop{false};
        std::atomic<uint64_t> latest_seq{0};
        std::atomic<uint64_t> read_hits{0};

        std::thread writer([&]() {
            for (uint64_t seq = 1; seq <= 2000; ++seq) {
                std::string value = "v" + std::to_string(seq);
                const Status st = memtable->put(as_bv("shared"), as_bv(value), seq, 0);
                assert(st.ok());
                latest_seq.store(seq, std::memory_order_release);
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
                    const uint64_t snapshot = latest_seq.load(std::memory_order_acquire);
                    if (snapshot == 0) {
                        continue;
                    }
                    if (memtable->get(as_bv("shared"), snapshot, &out)) {
                        read_hits.fetch_add(1, std::memory_order_relaxed);
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

        assert(read_hits.load(std::memory_order_relaxed) > 0);
    }

    static void run_writer_iterator_stress(const BackendFactory& make_backend) {
        auto memtable = make_backend();
        std::atomic<bool> stop{false};
        std::atomic<uint64_t> latest_seq{0};
        std::atomic<uint64_t> iter_steps{0};

        std::thread writer([&]() {
            for (uint64_t seq = 1; seq <= 2000; ++seq) {
                const std::string key = "k" + std::to_string(seq % 128);
                const std::string value = "v" + std::to_string(seq);
                const Status st = memtable->put(as_bv(key), as_bv(value), seq, 0);
                assert(st.ok());
                latest_seq.store(seq, std::memory_order_release);
            }
            stop.store(true, std::memory_order_release);
        });

        std::thread iter_reader([&]() {
            while (!stop.load(std::memory_order_acquire)) {
                const uint64_t snapshot = latest_seq.load(std::memory_order_acquire);
                if (snapshot == 0) {
                    continue;
                }
                std::string prev_key;
                bool first = true;
                for (const RecordView& rec : memtable->iterator(ByteView{}, ByteView{}, snapshot)) {
                    const std::string key{
                        reinterpret_cast<const char*>(rec.key().data()),
                        rec.key().size()
                    };
                    if (!first) {
                        assert(prev_key <= key);
                    }
                    assert(rec.seq() <= snapshot);
                    prev_key = key;
                    first = false;
                    iter_steps.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });

        writer.join();
        iter_reader.join();
        assert(iter_steps.load(std::memory_order_relaxed) > 0);
    }

    static std::vector<std::string> collect_range_keys(
        IMemTable& memtable,
        std::string_view start,
        std::string_view end,
        uint64_t snapshot_seq
    ) {
        std::vector<std::string> keys;
        for (const RecordView& rec : memtable.iterator(as_bv(start), as_bv(end), snapshot_seq)) {
            keys.emplace_back(reinterpret_cast<const char*>(rec.key().data()), rec.key().size());
        }
        return keys;
    }

    static void run_range_boundary_tests_for_backend(const BackendFactory& make_backend) {
        auto memtable = make_backend();
        assert(memtable->put(as_bv("a"), as_bv("v1"), 1, 0).ok());
        assert(memtable->put(as_bv("ab"), as_bv("v2"), 2, 0).ok());
        assert(memtable->put(as_bv("ac"), as_bv("v3"), 3, 0).ok());
        assert(memtable->put(as_bv("b"), as_bv("v4"), 4, 0).ok());

        {
            const auto keys = collect_range_keys(*memtable, "", "", 4);
            assert((keys == std::vector<std::string>{"a", "ab", "ac", "b"}));
        }
        {
            const auto keys = collect_range_keys(*memtable, "ab", "", 4);
            assert((keys == std::vector<std::string>{"ab", "ac", "b"}));
        }
        {
            const auto keys = collect_range_keys(*memtable, "", "ac", 4);
            assert((keys == std::vector<std::string>{"a", "ab"}));
        }
        {
            const auto keys = collect_range_keys(*memtable, "ab", "ab", 4);
            assert(keys.empty());
        }
        {
            const auto keys = collect_range_keys(*memtable, "ac", "ab", 4);
            assert(keys.empty());
        }
        {
            const auto keys = collect_range_keys(*memtable, "ab", "ac", 4);
            assert((keys == std::vector<std::string>{"ab"}));
        }
    }

    void test_generator_yield_all() {
        BufferArena arena{16 * 1024, 128 * 1024};

        auto gen1 = ArenaGenerator<int>::with_arena(arena, []() -> ArenaGenerator<int> {
            co_yield 1;
            co_yield 2;
        });
        auto gen2 = ArenaGenerator<int>::with_arena(arena, []() -> ArenaGenerator<int> {
            co_yield 3;
            co_yield 4;
        });
        auto gen3 = ArenaGenerator<int>::with_arena(arena, []() -> ArenaGenerator<int> {
            co_yield 5;
        });
        auto gen4 = ArenaGenerator<int>::with_arena(arena, []() -> ArenaGenerator<int> {
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

        assert(values.size() == 7);
        assert(values[0] == 1);
        assert(values[1] == 2);
        assert(values[2] == 3);
        assert(values[3] == 4);
        assert(values[4] == 5);
        assert(values[5] == 6);
        assert(values[6] == 7);
    }

    void test_backend_contracts_skiplist() {
        run_contract_tests_for_backend([]() { return std::make_unique<SkipListMemTable>(); });
    }

    void test_backend_contracts_bptree() {
        run_contract_tests_for_backend([]() { return std::make_unique<BPTreeMemTable>(); });
    }

    void test_backend_contracts_art() {
        run_contract_tests_for_backend([]() { return std::make_unique<ARTMemTable>(); });
    }

    void test_backend_single_writer_multi_reader_stress() {
        run_single_writer_multi_reader_get_stress([]() { return std::make_unique<SkipListMemTable>(); });
        run_single_writer_multi_reader_get_stress([]() { return std::make_unique<BPTreeMemTable>(); });
        run_single_writer_multi_reader_get_stress([]() { return std::make_unique<ARTMemTable>(); });
    }

    void test_backend_writer_iterator_stress() {
        run_writer_iterator_stress([]() { return std::make_unique<BPTreeMemTable>(); });
        run_writer_iterator_stress([]() { return std::make_unique<ARTMemTable>(); });
    }

    void test_backend_range_boundaries() {
        run_range_boundary_tests_for_backend([]() { return std::make_unique<BPTreeMemTable>(); });
        run_range_boundary_tests_for_backend([]() { return std::make_unique<ARTMemTable>(); });
    }

    void test_sharded_memtable_get_into_and_contains() {
        memtable::MemTable::Options opts;
        opts.shard_count = 4;
        auto table = memtable::MemTable::create(opts);

        const uint64_t s1 = table->next_seq();
        table->put(as_u8("alpha"), as_u8("v1"), s1);
        const uint64_t s2 = table->reserve_seq(2);
        table->remove(as_u8("alpha"), s2);
        const uint64_t s3 = s2 + 1;
        table->put(as_u8("beta"), as_u8("v2"), s3);
        assert(table->next_seq() == s3 + 1);

        std::vector<uint8_t> out;
        auto r1 = table->get_into(as_u8("beta"), s3, out);
        assert(r1.has_value() && *r1);
        assert(to_string(out) == "v2");

        auto r2 = table->get_into(as_u8("alpha"), s3, out);
        assert(r2.has_value() && !*r2);

        auto r3 = table->contains(as_u8("gamma"), s3);
        assert(!r3.has_value());
    }

    void test_sharded_memtable_force_flush_and_callback() {
        std::mutex mu;
        std::condition_variable cv;
        std::vector<std::string> flushed_keys;
        bool flushed = false;

        memtable::MemTable::Options opts;
        opts.shard_count = 2;
        opts.threshold_bytes_per_shard = 1;
        opts.backend_factory = []() { return std::make_unique<BPTreeMemTable>(); };
        opts.on_flush = [&](std::span<const RecordView> batch) {
            std::lock_guard<std::mutex> lock{mu};
            for (const RecordView& rec : batch) {
                flushed_keys.emplace_back(
                    reinterpret_cast<const char*>(rec.key().data()),
                    rec.key().size()
                );
            }
            flushed = true;
            cv.notify_one();
        };

        auto table = memtable::MemTable::create(opts);
        table->put(as_u8("k1"), as_u8("v1"), table->next_seq());
        table->flush_hint();
        table->force_flush();

        {
            std::unique_lock<std::mutex> lock{mu};
            cv.wait_for(lock, std::chrono::seconds(1), [&]() { return flushed; });
        }
        assert(flushed);
        assert(!flushed_keys.empty());
    }

    void test_sharded_memtable_range_merge() {
        memtable::MemTable::Options opts;
        opts.shard_count = 1;
        opts.backend_factory = []() { return std::make_unique<BPTreeMemTable>(); };
        auto table = memtable::MemTable::create(opts);

        table->put(as_u8("b"), as_u8("v1"), table->next_seq());
        table->put(as_u8("a"), as_u8("v2"), table->next_seq());
        table->put(as_u8("c"), as_u8("v3"), table->next_seq());
        table->put(as_u8("b"), as_u8("v4"), table->next_seq());

        memtable::MemTable::KeyRange range;
        range.start = std::vector<uint8_t>{'a'};
        range.end = std::vector<uint8_t>{'z'};

        auto it = table->iterator(range, table->last_seq());
        std::vector<std::string> pairs;
        while (it.has_next()) {
            const auto rec = it.next();
            assert(rec.has_value());
            pairs.emplace_back(
                std::string(reinterpret_cast<const char*>(rec->key().data()), rec->key().size()) + ":" +
                std::string(reinterpret_cast<const char*>(rec->value().data()), rec->value().size())
            );
        }

        assert(pairs.size() == 3);
        assert(pairs[0] == "a:v2");
        assert(pairs[1] == "b:v4");
        assert(pairs[2] == "c:v3");
    }

    void test_sharded_memtable_bptree_flush_reader_iterator_safety() {
        memtable::MemTable::Options opts;
        opts.shard_count = 4;
        opts.threshold_bytes_per_shard = 1024;
        opts.backend_factory = []() { return std::make_unique<BPTreeMemTable>(); };
        opts.on_flush = [](std::span<const RecordView>) {};
        auto table = memtable::MemTable::create(opts);

        std::atomic<bool> stop{false};
        std::atomic<uint64_t> latest_seq{0};
        std::atomic<uint64_t> reads{0};
        std::atomic<uint64_t> iters{0};

        std::thread writer([&]() {
            for (uint64_t i = 1; i <= 2500; ++i) {
                const uint64_t seq = table->next_seq();
                const std::string key = "key" + std::to_string(i % 256);
                const std::string value = "value" + std::to_string(i);
                table->put(as_u8(key), as_u8(value), seq);
                latest_seq.store(seq, std::memory_order_release);
                if ((i % 128) == 0) {
                    table->flush_hint();
                }
            }
            stop.store(true, std::memory_order_release);
        });

        std::thread reader([&]() {
            RecordView out;
            memtable::MemTable::KeyRange range;
            while (!stop.load(std::memory_order_acquire)) {
                const uint64_t snapshot = latest_seq.load(std::memory_order_acquire);
                if (snapshot == 0) {
                    continue;
                }
                if (table->get(as_u8("key1"), snapshot, &out)) {
                    reads.fetch_add(1, std::memory_order_relaxed);
                }
                auto it = table->iterator(range, snapshot);
                while (it.has_next()) {
                    const auto rec = it.next();
                    assert(rec.has_value());
                    assert(rec->seq() <= snapshot);
                    iters.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });

        writer.join();
        table->force_flush();
        reader.join();

        assert(reads.load(std::memory_order_relaxed) > 0);
        assert(iters.load(std::memory_order_relaxed) > 0);
    }

    void test_sharded_memtable_concurrency_smoke() {
        memtable::MemTable::Options opts;
        opts.shard_count = 4;
        auto table = memtable::MemTable::create(opts);

        std::atomic<bool> stop{false};
        std::atomic<uint64_t> latest_seq{0};
        std::atomic<uint64_t> reads{0};

        std::thread writer([&]() {
            for (uint64_t i = 0; i < 2000; ++i) {
                const uint64_t seq = table->next_seq();
                const std::string value = "v" + std::to_string(seq);
                table->put(as_u8("shared"), as_u8(value), seq);
                latest_seq.store(seq, std::memory_order_release);
            }
            stop.store(true, std::memory_order_release);
        });

        std::thread reader([&]() {
            RecordView out;
            while (!stop.load(std::memory_order_acquire)) {
                const uint64_t snap = latest_seq.load(std::memory_order_acquire);
                if (snap == 0) {
                    continue;
                }
                if (table->get(as_u8("shared"), snap, &out)) {
                    reads.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });

        writer.join();
        reader.join();
        assert(reads.load(std::memory_order_relaxed) > 0);
    }

    void test_sharded_memtable_auto_shard_derivation() {
        memtable::MemTable::Options opts;
        opts.shard_count = 0;
        opts.expected_concurrent_writers = 4;

        auto table = memtable::MemTable::create(opts);
        const auto snap = table->snapshot();
        // writers * 2 = 8, already power-of-two
        assert(snap.shard_count == 8);
    }
} // namespace

int main() {
    akkara::test::install_msvc_test_error_handlers();

    std::puts("[1/13] backend contracts (skiplist)");
    std::fflush(stdout);
    test_backend_contracts_skiplist();

    std::puts("[2/13] backend contracts (bptree)");
    std::fflush(stdout);
    test_backend_contracts_bptree();

    std::puts("[3/13] backend contracts (art)");
    std::fflush(stdout);
    test_backend_contracts_art();

    std::puts("[4/13] ArenaGenerator::yield_all");
    std::fflush(stdout);
    test_generator_yield_all();

    std::puts("[5/13] backend single-writer/multi-reader get stress");
    std::fflush(stdout);
    test_backend_single_writer_multi_reader_stress();

    std::puts("[6/13] backend writer+iterator stress");
    std::fflush(stdout);
    test_backend_writer_iterator_stress();

    std::puts("[7/13] backend range boundary iterator");
    std::fflush(stdout);
    test_backend_range_boundaries();

    std::puts("[8/13] sharded memtable get_into/contains");
    std::fflush(stdout);
    test_sharded_memtable_get_into_and_contains();

    std::puts("[9/13] sharded memtable force_flush (bptree)");
    std::fflush(stdout);
    test_sharded_memtable_force_flush_and_callback();

    std::puts("[10/13] sharded memtable range merge (bptree)");
    std::fflush(stdout);
    test_sharded_memtable_range_merge();

    std::puts("[11/13] sharded memtable bptree flush/read/iterator safety");
    std::fflush(stdout);
    test_sharded_memtable_bptree_flush_reader_iterator_safety();

    std::puts("[12/13] sharded memtable concurrency");
    std::fflush(stdout);
    test_sharded_memtable_concurrency_smoke();

    std::puts("[13/13] sharded memtable auto shard derivation");
    std::fflush(stdout);
    test_sharded_memtable_auto_shard_derivation();

    std::puts("memtable smoke test passed");
    return 0;
}

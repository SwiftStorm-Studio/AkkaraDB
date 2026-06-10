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

// benchmarks/suite/benchmark.cpp
#include "TestErrorHandlers.hpp"

#include "akkaradb/AkkaraDB.hpp"
#include "akk/engine/AkkEngine.hpp"

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <span>
#include <string>
#include <vector>

using Clock = std::chrono::steady_clock;

namespace {
    struct TrivialRecord {
        uint64_t id;
        int64_t data;
    };

    struct BinPackRecord {
        uint64_t id;
        std::string value;
        uint32_t group;
    };

    [[nodiscard]] double elapsed_ms(Clock::time_point start) {
        return std::chrono::duration<double, std::milli>(Clock::now() - start).count();
    }

    void write_le64(uint64_t v, uint8_t* out) {
        for (size_t i = 0; i < 8; ++i) { out[i] = static_cast<uint8_t>(v >> (8 * i)); }
    }

    [[nodiscard]] akkaradb::engine::AkkEngineOptions memory_options() {
        akkaradb::engine::AkkEngineOptions opts;
        opts.components.wal_enabled = false;
        opts.components.blob_enabled = false;
        opts.components.manifest_enabled = false;
        opts.components.sst_enabled = false;
        opts.components.version_log_enabled = false;
        opts.runtime.force_flush_on_close = false;
        opts.runtime.force_sync_on_close = false;
        opts.memtable.threshold_bytes_per_shard = 512ULL * 1024ULL * 1024ULL;
        return opts;
    }

    void bench_api_comparison() {
        constexpr int N = 1'000'000;
        constexpr size_t K8 = 8;
        constexpr size_t K16 = 16;

        std::vector<uint8_t> key8(static_cast<size_t>(N) * K8);
        std::vector<uint8_t> val8(static_cast<size_t>(N) * K8);
        std::vector<uint8_t> key16(static_cast<size_t>(N) * K16);
        std::vector<uint8_t> val16(static_cast<size_t>(N) * K16);
        std::vector<TrivialRecord> trivial(static_cast<size_t>(N));

        for (int i = 0; i < N; ++i) {
            const auto u = static_cast<uint64_t>(i);
            uint8_t* k8 = key8.data() + static_cast<size_t>(i) * K8;
            uint8_t* v8 = val8.data() + static_cast<size_t>(i) * K8;
            uint8_t* k16 = key16.data() + static_cast<size_t>(i) * K16;
            uint8_t* v16 = val16.data() + static_cast<size_t>(i) * K16;

            write_le64(u, k8);
            write_le64(u, v8);
            std::memset(k16, 0, 8);
            std::memcpy(k16 + 8, k8, 8);
            std::memcpy(v16, v8, 8);
            std::memset(v16 + 8, 0, 8);
            trivial[static_cast<size_t>(i)] = {u, static_cast<int64_t>(u)};
        }

        auto s_k8 = [&](int i) { return std::span<const uint8_t>{key8.data() + static_cast<size_t>(i) * K8, K8}; };
        auto s_v8 = [&](int i) { return std::span<const uint8_t>{val8.data() + static_cast<size_t>(i) * K8, K8}; };
        auto s_k16 = [&](int i) { return std::span<const uint8_t>{key16.data() + static_cast<size_t>(i) * K16, K16}; };
        auto s_v16 = [&](int i) { return std::span<const uint8_t>{val16.data() + static_cast<size_t>(i) * K16, K16}; };

        double raw8_w = 0.0;
        double raw8_r = 0.0;
        {
            auto eng = akkaradb::engine::AkkEngine::open(memory_options());
            auto t0 = Clock::now();
            for (int i = 0; i < N; ++i) { eng->put(s_k8(i), s_v8(i)); }
            raw8_w = static_cast<double>(N) / (elapsed_ms(t0) / 1000.0);

            std::vector<uint8_t> out;
            out.reserve(K8);
            int found = 0;
            t0 = Clock::now();
            for (int i = 0; i < N; ++i) { if (eng->get_into(s_k8(i), out)) { ++found; } }
            raw8_r = static_cast<double>(N) / (elapsed_ms(t0) / 1000.0);
            assert(found == N);
        }

        double raw16_w = 0.0;
        double raw16_r = 0.0;
        {
            auto eng = akkaradb::engine::AkkEngine::open(memory_options());
            auto t0 = Clock::now();
            for (int i = 0; i < N; ++i) { eng->put(s_k16(i), s_v16(i)); }
            raw16_w = static_cast<double>(N) / (elapsed_ms(t0) / 1000.0);

            std::vector<uint8_t> out;
            out.reserve(K16);
            int found = 0;
            t0 = Clock::now();
            for (int i = 0; i < N; ++i) { if (eng->get_into(s_k16(i), out)) { ++found; } }
            raw16_r = static_cast<double>(N) / (elapsed_ms(t0) / 1000.0);
            assert(found == N);
        }

        double typed_w = 0.0;
        double typed_r = 0.0;
        {
            auto db = akkaradb::AkkaraDB::open({}, akkaradb::StartupMode::ULTRA_FAST);
            auto table = db->table<&TrivialRecord::id>("bench_trivial");

            auto t0 = Clock::now();
            for (int i = 0; i < N; ++i) { table.put(trivial[static_cast<size_t>(i)]); }
            typed_w = static_cast<double>(N) / (elapsed_ms(t0) / 1000.0);

            TrivialRecord out{};
            int found = 0;
            t0 = Clock::now();
            for (int i = 0; i < N; ++i) { if (table.get_into(static_cast<uint64_t>(i), out)) { ++found; } }
            typed_r = static_cast<double>(N) / (elapsed_ms(t0) / 1000.0);
            assert(found == N);
        }

        std::printf("AkkaraDB SPECv5 typed API benchmark\n");
        std::printf("  [raw-inline     ] k= 8B v= 8B total=16B  write %10.0f read %10.0f ops/s\n", raw8_w, raw8_r);
        std::printf("  [raw-heap       ] k=16B v=16B total=32B  write %10.0f read %10.0f ops/s\n", raw16_w, raw16_r);
        std::printf("  [typed-trivial  ] k=16B v=%2zuB           write %10.0f read %10.0f ops/s\n", sizeof(TrivialRecord), typed_w, typed_r);
        std::printf("  typed vs raw-heap: write %.2fx read %.2fx slower (target <= 1.25x)\n", raw16_w / typed_w, raw16_r / typed_r);
    }

    void bench_binpack_and_index() {
        constexpr int N = 100'000;
        std::vector<BinPackRecord> records;
        records.reserve(N);
        for (int i = 0; i < N; ++i) {
            records.push_back({static_cast<uint64_t>(i), "value-" + std::to_string(i), static_cast<uint32_t>(i % 100)});
        }

        auto db = akkaradb::AkkaraDB::open({}, akkaradb::StartupMode::ULTRA_FAST);
        auto table = db->table<&BinPackRecord::id>("bench_binpack");

        auto t0 = Clock::now();
        for (const auto& record : records) { table.put(record); }
        const double no_index_w = static_cast<double>(N) / (elapsed_ms(t0) / 1000.0);

        auto indexed = db->table<&BinPackRecord::id>("bench_indexed");
        auto group = indexed.index<&BinPackRecord::group>();

        t0 = Clock::now();
        for (const auto& record : records) { indexed.put(record); }
        const double index_w = static_cast<double>(N) / (elapsed_ms(t0) / 1000.0);

        size_t hits = 0;
        t0 = Clock::now();
        auto range = group.find(42U);
        while (range.has_next()) {
            auto entry = range.next();
            assert(entry.value.group == 42U);
            ++hits;
        }
        const double index_lookup = static_cast<double>(hits) / (elapsed_ms(t0) / 1000.0);

        std::printf("  [typed-binpack  ] string entity           write %10.0f ops/s\n", no_index_w);
        std::printf("  [typed-index-put] one non-unique index    write %10.0f ops/s\n", index_w);
        std::printf("  [index-lookup   ] exact non-unique hits=%zu read %10.0f rows/s\n", hits, index_lookup);
    }
} // namespace

int main() {
    akkara::test::install_msvc_test_error_handlers();

    bench_api_comparison();
    bench_binpack_and_index();
    return 0;
}

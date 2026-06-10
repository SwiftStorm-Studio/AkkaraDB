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

    [[nodiscard]] double elapsedMs(Clock::time_point start) {
        return std::chrono::duration<double, std::milli>(Clock::now() - start).count();
    }

    void writeLe64(uint64_t v, uint8_t* out) {
        for (size_t i = 0; i < 8; ++i) { out[i] = static_cast<uint8_t>(v >> (8 * i)); }
    }

    [[nodiscard]] akkaradb::engine::AkkEngineOptions memoryOptions() {
        akkaradb::engine::AkkEngineOptions opts;
        opts.components.walEnabled = false;
        opts.components.blobEnabled = false;
        opts.components.manifestEnabled = false;
        opts.components.sstEnabled = false;
        opts.components.versionLogEnabled = false;
        opts.runtime.forceFlushOnClose = false;
        opts.runtime.forceSyncOnClose = false;
        opts.memtable.thresholdBytesPerShard = 512ULL * 1024ULL * 1024ULL;
        return opts;
    }

    void benchApiComparison() {
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

            writeLe64(u, k8);
            writeLe64(u, v8);
            std::memset(k16, 0, 8);
            std::memcpy(k16 + 8, k8, 8);
            std::memcpy(v16, v8, 8);
            std::memset(v16 + 8, 0, 8);
            trivial[static_cast<size_t>(i)] = {u, static_cast<int64_t>(u)};
        }

        auto sK8 = [&](int i) { return std::span<const uint8_t>{key8.data() + static_cast<size_t>(i) * K8, K8}; };
        auto sV8 = [&](int i) { return std::span<const uint8_t>{val8.data() + static_cast<size_t>(i) * K8, K8}; };
        auto sK16 = [&](int i) { return std::span<const uint8_t>{key16.data() + static_cast<size_t>(i) * K16, K16}; };
        auto sV16 = [&](int i) { return std::span<const uint8_t>{val16.data() + static_cast<size_t>(i) * K16, K16}; };

        double raw8W = 0.0;
        double raw8R = 0.0;
        {
            auto eng = akkaradb::engine::AkkEngine::open(memoryOptions());
            auto t0 = Clock::now();
            for (int i = 0; i < N; ++i) { eng->put(sK8(i), sV8(i)); }
            raw8W = static_cast<double>(N) / (elapsedMs(t0) / 1000.0);

            std::vector<uint8_t> out;
            out.reserve(K8);
            int found = 0;
            t0 = Clock::now();
            for (int i = 0; i < N; ++i) { if (eng->getInto(sK8(i), out)) { ++found; } }
            raw8R = static_cast<double>(N) / (elapsedMs(t0) / 1000.0);
            AKK_TEST_CHECK(found == N);
        }

        double raw16W = 0.0;
        double raw16R = 0.0;
        {
            auto eng = akkaradb::engine::AkkEngine::open(memoryOptions());
            auto t0 = Clock::now();
            for (int i = 0; i < N; ++i) { eng->put(sK16(i), sV16(i)); }
            raw16W = static_cast<double>(N) / (elapsedMs(t0) / 1000.0);

            std::vector<uint8_t> out;
            out.reserve(K16);
            int found = 0;
            t0 = Clock::now();
            for (int i = 0; i < N; ++i) { if (eng->getInto(sK16(i), out)) { ++found; } }
            raw16R = static_cast<double>(N) / (elapsedMs(t0) / 1000.0);
            AKK_TEST_CHECK(found == N);
        }

        double typedW = 0.0;
        double typedR = 0.0;
        {
            auto db = akkaradb::AkkaraDB::open({}, akkaradb::StartupMode::ULTRA_FAST);
            auto table = db->table<&TrivialRecord::id>("benchTrivial");

            auto t0 = Clock::now();
            for (int i = 0; i < N; ++i) { table.put(trivial[static_cast<size_t>(i)]); }
            typedW = static_cast<double>(N) / (elapsedMs(t0) / 1000.0);

            TrivialRecord out{};
            int found = 0;
            t0 = Clock::now();
            for (int i = 0; i < N; ++i) { if (table.getInto(static_cast<uint64_t>(i), out)) { ++found; } }
            typedR = static_cast<double>(N) / (elapsedMs(t0) / 1000.0);
            AKK_TEST_CHECK(found == N);
        }

        std::printf("AkkaraDB SPECv5 typed API benchmark\n");
        std::printf("  [raw-inline     ] k= 8B v= 8B total=16B  write %10.0f read %10.0f ops/s\n", raw8W, raw8R);
        std::printf("  [raw-heap       ] k=16B v=16B total=32B  write %10.0f read %10.0f ops/s\n", raw16W, raw16R);
        std::printf("  [typed-trivial  ] k=16B v=%2zuB           write %10.0f read %10.0f ops/s\n", sizeof(TrivialRecord), typedW, typedR);
        std::printf("  typed vs raw-heap: write %.2fx read %.2fx slower (target <= 1.25x)\n", raw16W / typedW, raw16R / typedR);
    }

    void benchBinpackAndIndex() {
        constexpr int N = 100'000;
        std::vector<BinPackRecord> records;
        records.reserve(N);
        for (int i = 0; i < N; ++i) {
            records.push_back({static_cast<uint64_t>(i), "value-" + std::to_string(i), static_cast<uint32_t>(i % 100)});
        }

        auto db = akkaradb::AkkaraDB::open({}, akkaradb::StartupMode::ULTRA_FAST);
        auto table = db->table<&BinPackRecord::id>("benchBinpack");

        auto t0 = Clock::now();
        for (const auto& record : records) { table.put(record); }
        const double noIndexW = static_cast<double>(N) / (elapsedMs(t0) / 1000.0);

        auto indexed = db->table<&BinPackRecord::id>("benchIndexed");
        auto group = indexed.index<&BinPackRecord::group>();

        t0 = Clock::now();
        for (const auto& record : records) { indexed.put(record); }
        const double indexW = static_cast<double>(N) / (elapsedMs(t0) / 1000.0);

        size_t hits = 0;
        t0 = Clock::now();
        auto range = group.find(42U);
        while (range.hasNext()) {
            auto entry = range.next();
            AKK_TEST_CHECK(entry.value.group == 42U);
            ++hits;
        }
        const double indexLookup = static_cast<double>(hits) / (elapsedMs(t0) / 1000.0);

        std::printf("  [typed-binpack  ] string entity           write %10.0f ops/s\n", noIndexW);
        std::printf("  [typed-index-put] one non-unique index    write %10.0f ops/s\n", indexW);
        std::printf("  [index-lookup   ] exact non-unique hits=%zu read %10.0f rows/s\n", hits, indexLookup);
    }
} // namespace

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();

    benchApiComparison();
    benchBinpackAndIndex();
    return 0;
}

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

// benchmarks/throughput/memtableThroughputBenchmark.cpp
#include "TestErrorHandlers.hpp"

/*
 * Sharded MemTable throughput benchmark (size sweep).
 *
 * Measures:
 *  - PUT throughput (unique keys)
 *  - GET throughput (existing keys)
 *
 * Runs one backend with configurable shard/flush settings.
 *
 * Usage:
 *   akkaradbMemtableThroughputBenchmark [opsPerCase]
 *       [--writers=N] [--shards=N] [--auto-cap=N]
 *       [--threshold-bytes=N|NK|NM|NG|NKiB|NMiB|NGiB]
 *       [--flush-after-scan] [--prehash] [--backend=skiplist|bptree|art]
 *
 * Default:
 *   opsPerCase = 200000
 *   writers      = 16
 *   shards       = 0 (auto)
 */

#include "akk/engine/memtable/MemTable.hpp"
#include "akk/engine/memtable/ARTMemTable.hpp"
#include "akk/engine/memtable/BPTreeMemTable.hpp"
#include "akk/engine/memtable/SkipListMemTable.hpp"
#include "akk/core/record/KeyFingerprint.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <format>
#include <limits>
#include <string>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;
using namespace akkaradb::engine;
using namespace akkaradb::engine::memtable;
using namespace akkaradb::core;

namespace {
    constexpr uint32_t kLatencySampleMask = 0x3F; // sample 1 / 64 ops to reduce benchmark perturbation
    constexpr size_t kScanSampleWindow = 32; // amortize clock resolution/overhead for iterator next()
    constexpr uint64_t kAutoFlushDisabledThreshold = (1ULL << 62);

    struct CaseSpec {
        int keySize;
        int valueSize;
    };

    struct LatencyPercentiles {
        double p50Us = 0.0;
        double p90Us = 0.0;
        double p99Us = 0.0;
        double p999Us = 0.0;
        uint32_t sampleCount = 0;
    };

    enum class BackendKind {
        SkipList,
        BPTree,
        ART
    };

    struct ThroughputResult {
        double putOpsPerSec;
        double getOpsPerSec;
        double scanOpsPerSec;
        double putMs;
        double getMs;
        double scanMs;
        double flushMs;
        uint64_t approxBytes;
        uint64_t flushesCompleted;
        uint64_t flushRecordsSeen;
        LatencyPercentiles putLatency;
        LatencyPercentiles getLatency;
        LatencyPercentiles scanLatency;
    };

    struct BenchConfig {
        int opsPerCase = 500000;
        int writerThreads = 16;
        uint32_t requestedShards = 0;
        uint32_t autoShardCountCap = 128;
        uint64_t thresholdBytesPerShard = kAutoFlushDisabledThreshold;
        bool flushAfterScan = false;
        bool usePrehash = false;
        BackendKind backend = BackendKind::ART;
    };

    [[nodiscard]] static uint32_t nextPow2Clamped(uint64_t n, uint32_t minValue, uint32_t maxValue) {
        uint32_t p = 1;
        while (p < n && p < maxValue) {
            p <<= 1;
        }
        if (p < minValue) {
            p = minValue;
        }
        if (p > maxValue) {
            p = maxValue;
        }
        return p;
    }

    [[nodiscard]] static uint32_t resolveShardCount(
        size_t requested,
        size_t expectedConcurrentWriters,
        size_t autoCap
    ) {
        if (requested == 1) {
            return 1;
        }
        if (requested > 1) {
            return nextPow2Clamped(static_cast<uint64_t>(requested), 2, 256);
        }

        const uint32_t effectiveCap = nextPow2Clamped(static_cast<uint64_t>(autoCap == 0 ? 128 : autoCap), 2, 256);
        const size_t n = expectedConcurrentWriters > 0
            ? expectedConcurrentWriters
            : std::max<size_t>(2, std::thread::hardware_concurrency());
        const uint64_t target = n <= 1 ? 1ULL : static_cast<uint64_t>(n) * 4ULL;
        return nextPow2Clamped(target, 2, effectiveCap);
    }

    [[nodiscard]] static int resolveWriterThreads(int requested) {
        if (requested > 0) {
            return requested;
        }
        const unsigned hw = std::thread::hardware_concurrency();
        return static_cast<int>(hw == 0 ? 1u : hw);
    }

    static std::span<const uint8_t> asU8(const std::string& s) {
        return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
    }

    [[nodiscard]] static std::string lowerAscii(std::string s) {
        for (char& ch : s) {
            if (ch >= 'A' && ch <= 'Z') {
                ch = static_cast<char>(ch - 'A' + 'a');
            }
        }
        return s;
    }

    [[nodiscard]] static bool parseU64WithSuffix(const std::string& text, uint64_t* out) {
        if (text.empty() || out == nullptr) {
            return false;
        }

        char* end = nullptr;
        const unsigned long long base = std::strtoull(text.c_str(), &end, 10);
        if (end == text.c_str()) {
            return false;
        }

        const std::string suffix = lowerAscii(std::string{end});
        uint64_t multiplier = 1;
        if (suffix.empty() || suffix == "b") {
            multiplier = 1;
        } else if (suffix == "k" || suffix == "kb" || suffix == "kib") {
            multiplier = 1024ULL;
        } else if (suffix == "m" || suffix == "mb" || suffix == "mib") {
            multiplier = 1024ULL * 1024ULL;
        } else if (suffix == "g" || suffix == "gb" || suffix == "gib") {
            multiplier = 1024ULL * 1024ULL * 1024ULL;
        } else if (suffix == "t" || suffix == "tb" || suffix == "tib") {
            multiplier = 1024ULL * 1024ULL * 1024ULL * 1024ULL;
        } else {
            return false;
        }

        if (base > std::numeric_limits<uint64_t>::max() / multiplier) {
            return false;
        }
        *out = static_cast<uint64_t>(base) * multiplier;
        return true;
    }

    [[nodiscard]] static std::string formatBytes(uint64_t bytes) {
        constexpr double kKiB = 1024.0;
        constexpr double kMiB = 1024.0 * 1024.0;
        constexpr double kGiB = 1024.0 * 1024.0 * 1024.0;
        constexpr double kTiB = 1024.0 * 1024.0 * 1024.0 * 1024.0;

        if (bytes >= static_cast<uint64_t>(kTiB)) {
            return std::format("{:.2f} TiB", static_cast<double>(bytes) / kTiB);
        }
        if (bytes >= static_cast<uint64_t>(kGiB)) {
            return std::format("{:.2f} GiB", static_cast<double>(bytes) / kGiB);
        }
        if (bytes >= static_cast<uint64_t>(kMiB)) {
            return std::format("{:.2f} MiB", static_cast<double>(bytes) / kMiB);
        }
        if (bytes >= static_cast<uint64_t>(kKiB)) {
            return std::format("{:.2f} KiB", static_cast<double>(bytes) / kKiB);
        }
        return std::format("{} B", bytes);
    }

    [[nodiscard]] static double payloadMibPerSec(size_t bytesPerOp, int ops, double ms) {
        if (ms <= 0.0) {
            return 0.0;
        }
        const double totalMib = static_cast<double>(bytesPerOp) * static_cast<double>(ops) / (1024.0 * 1024.0);
        return totalMib * 1000.0 / ms;
    }

    [[nodiscard]] static double quantileFromSorted(
        const std::vector<uint32_t>& sortedNs,
        double q
    ) {
        if (sortedNs.empty()) {
            return 0.0;
        }
        const double qClamped = std::clamp(q, 0.0, 1.0);
        const size_t idx = static_cast<size_t>(
            qClamped * static_cast<double>(sortedNs.size() - 1)
        );
        return static_cast<double>(sortedNs[idx]) / 1000.0;
    }

    [[nodiscard]] static LatencyPercentiles buildPercentiles(std::vector<uint32_t>& samplesNs) {
        LatencyPercentiles out{};
        if (samplesNs.empty()) {
            return out;
        }

        std::sort(samplesNs.begin(), samplesNs.end());
        out.sampleCount = static_cast<uint32_t>(samplesNs.size());
        out.p50Us = quantileFromSorted(samplesNs, 0.50);
        out.p90Us = quantileFromSorted(samplesNs, 0.90);
        out.p99Us = quantileFromSorted(samplesNs, 0.99);
        out.p999Us = quantileFromSorted(samplesNs, 0.999);
        return out;
    }

    static std::string makeFixedBytes(int size, uint64_t seed) {
        std::string out;
        out.resize(static_cast<size_t>(size));
        uint64_t x = seed ^ 0x9e3779b97f4a7c15ULL;
        for (int i = 0; i < size; ++i) {
            x ^= (x << 13);
            x ^= (x >> 7);
            x ^= (x << 17);
            out[static_cast<size_t>(i)] = static_cast<char>('a' + (x % 26));
        }
        return out;
    }

    static memtable::MemTable::Options makeOptions(
        const BenchConfig& config,
        int effectiveWriters,
        std::atomic<uint64_t>* flushRecordsSeen
    ) {
        memtable::MemTable::Options opts;
        opts.shardCount = config.requestedShards;
        opts.expectedConcurrentWriters = static_cast<size_t>(effectiveWriters);
        opts.autoShardCountCap = config.autoShardCountCap;
        opts.thresholdBytesPerShard = config.thresholdBytesPerShard;
        if (config.flushAfterScan) {
            opts.onFlush = [flushRecordsSeen](std::span<const memtable::MemTable::RecordView> records) {
                if (flushRecordsSeen) {
                    flushRecordsSeen->fetch_add(static_cast<uint64_t>(records.size()), std::memory_order_relaxed);
                }
            };
        }
        if (config.backend == BackendKind::BPTree) {
            opts.backendFactory = []() {
                return std::make_unique<BPTreeMemTable>();
            };
        } else if (config.backend == BackendKind::ART) {
            opts.backendFactory = []() {
                return std::make_unique<ARTMemTable>();
            };
        } else {
            opts.backendFactory = []() {
                return std::make_unique<SkipListMemTable>();
            };
        }
        return opts;
    }

    static void runPutParallel(
        memtable::MemTable& memtable,
        const std::vector<std::string>& keys,
        const std::string& value,
        const std::vector<uint64_t>* keyFp64,
        const std::vector<uint64_t>* keyMk,
        int writerThreads,
        std::vector<uint32_t>* latencySamplesNs
    ) {
        if (latencySamplesNs) {
            latencySamplesNs->clear();
        }

        if (writerThreads <= 1) {
            if (latencySamplesNs) {
                latencySamplesNs->reserve((keys.size() + kLatencySampleMask) / (kLatencySampleMask + 1));
            }
            for (size_t i = 0; i < keys.size(); ++i) {
                const bool doSample = ((static_cast<uint32_t>(i) & kLatencySampleMask) == 0);
                const auto t0 = doSample ? Clock::now() : Clock::time_point{};
                const uint64_t seq = memtable.nextSeq();
                memtable.put(
                    asU8(keys[i]),
                    asU8(value),
                    seq,
                    0,
                    keyFp64 ? (*keyFp64)[i] : 0ULL,
                    keyMk ? (*keyMk)[i] : 0ULL
                );
                if (doSample && latencySamplesNs) {
                    const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count();
                    latencySamplesNs->push_back(static_cast<uint32_t>(std::min<int64_t>(dt, INT32_MAX)));
                }
            }
            return;
        }

        std::vector<std::thread> threads;
        threads.reserve(static_cast<size_t>(writerThreads));
        std::vector<std::vector<uint32_t>> localSamples(static_cast<size_t>(writerThreads));
        for (auto& v : localSamples) {
            v.reserve((keys.size() / static_cast<size_t>(writerThreads) + kLatencySampleMask) / (kLatencySampleMask + 1));
        }

        for (int tid = 0; tid < writerThreads; ++tid) {
            threads.emplace_back([&, tid]() {
                auto& samples = localSamples[static_cast<size_t>(tid)];
                const size_t stride = static_cast<size_t>(writerThreads);
                const size_t first = static_cast<size_t>(tid);
                const size_t opCount = first < keys.size() ? ((keys.size() - first + stride - 1) / stride) : 0;
                const uint64_t seqBase = memtable.reserveSeq(static_cast<uint64_t>(opCount));
                uint64_t seqOffset = 0;
                for (size_t i = first; i < keys.size(); i += stride) {
                    const bool doSample = ((static_cast<uint32_t>(i) & kLatencySampleMask) == 0);
                    const auto t0 = doSample ? Clock::now() : Clock::time_point{};
                    const uint64_t seq = seqBase + seqOffset++;
                    memtable.put(
                        asU8(keys[i]),
                        asU8(value),
                        seq,
                        0,
                        keyFp64 ? (*keyFp64)[i] : 0ULL,
                        keyMk ? (*keyMk)[i] : 0ULL
                    );
                    if (doSample) {
                        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count();
                        samples.push_back(static_cast<uint32_t>(std::min<int64_t>(dt, INT32_MAX)));
                    }
                }
            });
        }

        for (auto& th : threads) {
            th.join();
        }

        if (latencySamplesNs) {
            size_t total = 0;
            for (const auto& v : localSamples) {
                total += v.size();
            }
            latencySamplesNs->reserve(total);
            for (auto& v : localSamples) {
                latencySamplesNs->insert(latencySamplesNs->end(), v.begin(), v.end());
            }
        }
    }

    static void runGetParallel(
        memtable::MemTable& memtable,
        const std::vector<std::string>& keys,
        const std::vector<uint64_t>* keyFp64,
        uint64_t snapshot,
        int readerThreads,
        std::vector<uint32_t>* latencySamplesNs
    ) {
        if (latencySamplesNs) {
            latencySamplesNs->clear();
        }

        if (readerThreads <= 1) {
            RecordView out;
            if (latencySamplesNs) {
                latencySamplesNs->reserve((keys.size() + kLatencySampleMask) / (kLatencySampleMask + 1));
            }
            for (size_t i = 0; i < keys.size(); ++i) {
                const bool doSample = ((static_cast<uint32_t>(i) & kLatencySampleMask) == 0);
                const auto t0 = doSample ? Clock::now() : Clock::time_point{};
                if (!memtable.get(asU8(keys[i]), snapshot, &out, keyFp64 ? (*keyFp64)[i] : 0ULL)) {
                    std::fprintf(stderr, "GET miss at i=%zu\n", i);
                    std::exit(3);
                }
                if (doSample && latencySamplesNs) {
                    const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count();
                    latencySamplesNs->push_back(static_cast<uint32_t>(std::min<int64_t>(dt, INT32_MAX)));
                }
            }
            return;
        }

        std::vector<std::thread> threads;
        threads.reserve(static_cast<size_t>(readerThreads));
        std::vector<std::vector<uint32_t>> localSamples(static_cast<size_t>(readerThreads));
        for (auto& v : localSamples) {
            v.reserve((keys.size() / static_cast<size_t>(readerThreads) + kLatencySampleMask) / (kLatencySampleMask + 1));
        }

        for (int tid = 0; tid < readerThreads; ++tid) {
            threads.emplace_back([&, tid]() {
                RecordView out;
                auto& samples = localSamples[static_cast<size_t>(tid)];
                for (size_t i = static_cast<size_t>(tid); i < keys.size(); i += static_cast<size_t>(readerThreads)) {
                    const bool doSample = ((static_cast<uint32_t>(i) & kLatencySampleMask) == 0);
                    const auto t0 = doSample ? Clock::now() : Clock::time_point{};
                    if (!memtable.get(asU8(keys[i]), snapshot, &out, keyFp64 ? (*keyFp64)[i] : 0ULL)) {
                        std::fprintf(stderr, "GET miss at i=%zu (tid=%d)\n", i, tid);
                        std::exit(3);
                    }
                    if (doSample) {
                        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count();
                        samples.push_back(static_cast<uint32_t>(std::min<int64_t>(dt, INT32_MAX)));
                    }
                }
            });
        }

        for (auto& th : threads) {
            th.join();
        }

        if (latencySamplesNs) {
            size_t total = 0;
            for (const auto& v : localSamples) {
                total += v.size();
            }
            latencySamplesNs->reserve(total);
            for (auto& v : localSamples) {
                latencySamplesNs->insert(latencySamplesNs->end(), v.begin(), v.end());
            }
        }
    }

    static double runScanSingle(
        memtable::MemTable& memtable,
        uint64_t snapshot,
        size_t expectedRecords,
        std::vector<uint32_t>* latencySamplesNs
    ) {
        if (latencySamplesNs) {
            latencySamplesNs->clear();
            latencySamplesNs->reserve((expectedRecords + kLatencySampleMask) / (kLatencySampleMask + 1));
        }

        memtable::MemTable::KeyRange fullRange{};
        size_t scanned = 0;
        const auto scanT0 = Clock::now();
        auto it = memtable.iterator(fullRange, snapshot);
        while (it.hasNext()) {
            const bool doSample = ((static_cast<uint32_t>(scanned) & kLatencySampleMask) == 0);
            if (doSample && latencySamplesNs) {
                const auto t0 = Clock::now();
                size_t windowCount = 0;
                while (windowCount < kScanSampleWindow && it.hasNext()) {
                    const auto rec = it.next();
                    if (!rec.has_value()) {
                        std::fprintf(stderr, "SCAN iterator returned nullopt before end (scanned=%zu)\n", scanned);
                        std::exit(4);
                    }
                    ++windowCount;
                    ++scanned;
                }
                const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count();
                const int64_t perRecordNs = windowCount > 0 ? (dt / static_cast<int64_t>(windowCount)) : 0;
                latencySamplesNs->push_back(static_cast<uint32_t>(std::min<int64_t>(perRecordNs, INT32_MAX)));
                continue;
            }

            const auto rec = it.next();
            if (!rec.has_value()) {
                std::fprintf(stderr, "SCAN iterator returned nullopt before end (scanned=%zu)\n", scanned);
                std::exit(4);
            }
            ++scanned;
        }
        const auto scanMs = std::chrono::duration<double, std::milli>(Clock::now() - scanT0).count();

        if (scanned != expectedRecords) {
            std::fprintf(stderr, "SCAN count mismatch: expected=%zu actual=%zu\n", expectedRecords, scanned);
            std::exit(5);
        }
        if (scanMs <= 0.0) {
            return 0.0;
        }
        return static_cast<double>(scanned) * 1000.0 / scanMs;
    }

    static ThroughputResult runCase(
        const CaseSpec spec,
        int opsPerCase,
        int writerThreads,
        const BenchConfig& config
    ) {
        const int warmupOps = std::min(opsPerCase, 100000);

        std::vector<std::string> keys;
        keys.reserve(static_cast<size_t>(opsPerCase));
        for (int i = 0; i < opsPerCase; ++i) {
            std::string key = makeFixedBytes(spec.keySize, static_cast<uint64_t>(i) + 1);
            if (spec.keySize >= 10) {
                const auto tail = std::format("{:010d}", i);
                std::memcpy(key.data() + (spec.keySize - 10), tail.data(), 10);
            }
            keys.emplace_back(std::move(key));
        }

        const std::string value = makeFixedBytes(spec.valueSize, 0xA11CEULL);

        std::vector<uint64_t> keyFp64;
        std::vector<uint64_t> keyMk;
        const std::vector<uint64_t>* fpPtr = nullptr;
        const std::vector<uint64_t>* mkPtr = nullptr;

        if (config.usePrehash) {
            keyFp64.resize(static_cast<size_t>(opsPerCase));
            keyMk.resize(static_cast<size_t>(opsPerCase));
            for (int i = 0; i < opsPerCase; ++i) {
                const auto& key = keys[static_cast<size_t>(i)];
                const uint8_t* ptr = reinterpret_cast<const uint8_t*>(key.data());
                const size_t len = key.size();
                keyFp64[static_cast<size_t>(i)] = computeKeyFp64(ptr, len);
                keyMk[static_cast<size_t>(i)] = buildMiniKey(ptr, len);
            }
            fpPtr = &keyFp64;
            mkPtr = &keyMk;
        }

        {
            std::atomic<uint64_t> warmupFlushRecordsSeen{0};
            auto warmup = memtable::MemTable::create(makeOptions(config, writerThreads, &warmupFlushRecordsSeen));
            const std::vector<std::string> warmupKeys(keys.begin(), keys.begin() + warmupOps);
            runPutParallel(*warmup, warmupKeys, value, fpPtr, mkPtr, writerThreads, nullptr);

            const uint64_t snapshot = warmup->lastSeq();
            RecordView out;
            for (int i = 0; i < warmupOps; ++i) {
                if (!warmup->get(asU8(keys[static_cast<size_t>(i)]), snapshot, &out)) {
                    std::fprintf(stderr, "WARMUP GET miss at i=%d\n", i);
                    std::exit(1);
                }
            }
        }

        std::atomic<uint64_t> flushRecordsSeen{0};
        auto memtable = memtable::MemTable::create(makeOptions(config, writerThreads, &flushRecordsSeen));
        std::vector<uint32_t> putLatencyNs;
        std::vector<uint32_t> getLatencyNs;
        std::vector<uint32_t> scanLatencyNs;

        const auto putT0 = Clock::now();
        runPutParallel(*memtable, keys, value, fpPtr, mkPtr, writerThreads, &putLatencyNs);
        const auto putMs = std::chrono::duration<double, std::milli>(Clock::now() - putT0).count();

        const uint64_t snapshot = memtable->lastSeq();
        const auto getT0 = Clock::now();
        runGetParallel(*memtable, keys, fpPtr, snapshot, writerThreads, &getLatencyNs);
        const auto getMs = std::chrono::duration<double, std::milli>(Clock::now() - getT0).count();
        const auto scanT0 = Clock::now();
        const double scanOpsPerSec = runScanSingle(*memtable, snapshot, keys.size(), &scanLatencyNs);
        const auto scanMs = std::chrono::duration<double, std::milli>(Clock::now() - scanT0).count();

        double flushMs = 0.0;
        if (config.flushAfterScan) {
            const auto flushT0 = Clock::now();
            memtable->forceFlush();
            flushMs = std::chrono::duration<double, std::milli>(Clock::now() - flushT0).count();
        }
        const auto snapshotAfter = memtable->snapshot();

        return {
            .putOpsPerSec = static_cast<double>(opsPerCase) * 1000.0 / putMs,
            .getOpsPerSec = static_cast<double>(opsPerCase) * 1000.0 / getMs,
            .scanOpsPerSec = scanOpsPerSec,
            .putMs = putMs,
            .getMs = getMs,
            .scanMs = scanMs,
            .flushMs = flushMs,
            .approxBytes = snapshotAfter.approxBytes,
            .flushesCompleted = snapshotAfter.flushesCompleted,
            .flushRecordsSeen = flushRecordsSeen.load(std::memory_order_relaxed),
            .putLatency = buildPercentiles(putLatencyNs),
            .getLatency = buildPercentiles(getLatencyNs),
            .scanLatency = buildPercentiles(scanLatencyNs)
        };
    }
} // namespace

int main(int argc, char** argv) {
    akkaradb::test::installMsvcTestErrorHandlers();

    BenchConfig config;

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--prehash") {
            config.usePrehash = true;
            continue;
        }
        if (arg == "--flush-after-scan" || arg == "--flush-callback") {
            config.flushAfterScan = true;
            continue;
        }
        if (arg.rfind("--backend=", 0) == 0) {
            const std::string kind = arg.substr(10);
            if (kind == "skiplist") {
                config.backend = BackendKind::SkipList;
                continue;
            }
            if (kind == "bptree") {
                config.backend = BackendKind::BPTree;
                continue;
            }
            if (kind == "art") {
                config.backend = BackendKind::ART;
                continue;
            }
            std::fprintf(stderr, "Unknown backend: %s (use skiplist|bptree|art)\n", kind.c_str());
            return 2;
        }
        if (arg.rfind("--writers=", 0) == 0) {
            config.writerThreads = std::max(0, std::atoi(arg.substr(10).c_str()));
            continue;
        }
        if (arg.rfind("--shards=", 0) == 0) {
            config.requestedShards = static_cast<uint32_t>(std::max(0, std::atoi(arg.substr(9).c_str())));
            continue;
        }
        if (arg.rfind("--auto-cap=", 0) == 0) {
            config.autoShardCountCap = static_cast<uint32_t>(std::max(0, std::atoi(arg.substr(11).c_str())));
            continue;
        }
        if (arg.rfind("--threshold-bytes=", 0) == 0) {
            uint64_t parsed = 0;
            if (!parseU64WithSuffix(arg.substr(18), &parsed)) {
                std::fprintf(stderr, "Invalid --threshold-bytes value: %s\n", arg.substr(18).c_str());
                return 2;
            }
            config.thresholdBytesPerShard = parsed;
            continue;
        }
        config.opsPerCase = std::max(1000, std::atoi(arg.c_str()));
    }

    const std::array<CaseSpec, 6> cases{{
        {8, 16},
        {16, 64},
        {16, 256},
        {32, 1024},
        {32, 4096},
        {64, 16384}
    }};

    const int effectiveWriters = resolveWriterThreads(config.writerThreads);
    if (config.flushAfterScan && config.thresholdBytesPerShard != kAutoFlushDisabledThreshold) {
        std::fprintf(
            stderr,
            "--flush-after-scan expects auto flush to stay disabled; omit --threshold-bytes for this benchmark mode.\n"
        );
        return 2;
    }
    const uint32_t shardCount = resolveShardCount(
        config.requestedShards,
        static_cast<size_t>(effectiveWriters),
        config.autoShardCountCap
    );

    std::printf("Sharded MemTable throughput benchmark\n");
    std::printf("opsPerCase = %d\n", config.opsPerCase);
    const char* backendName = "skiplist";
    if (config.backend == BackendKind::BPTree) {
        backendName = "bptree";
    } else if (config.backend == BackendKind::ART) {
        backendName = "art";
    }
    std::printf("backend = %s\n", backendName);
    if (config.writerThreads == 0) {
        std::printf("writerThreads = auto (%d from hwThreads)\n", effectiveWriters);
    } else {
        std::printf("writerThreads = %d\n", config.writerThreads);
    }
    if (config.requestedShards == 0) {
        std::printf("resolvedShards = %u (auto, writers*4 locality heuristic, cap %u)\n", shardCount, config.autoShardCountCap);
    } else if (config.requestedShards == shardCount) {
        std::printf("resolvedShards = %u (explicit)\n", shardCount);
    } else {
        std::printf("resolvedShards = %u (explicit %u rounded to power-of-two)\n", shardCount, config.requestedShards);
    }
    if (config.thresholdBytesPerShard == kAutoFlushDisabledThreshold) {
        std::printf("thresholdBytesPerShard = disabled\n");
    } else if (config.flushAfterScan) {
        std::printf("thresholdBytesPerShard = %s\n", formatBytes(config.thresholdBytesPerShard).c_str());
    } else {
        std::printf(
            "thresholdBytesPerShard = %s (no flush worker)\n",
            formatBytes(config.thresholdBytesPerShard).c_str()
        );
    }
    std::printf("flushAfterScan = %s\n\n", config.flushAfterScan ? "ON" : "OFF");
    std::printf("warmupOps   = %d\n\n", std::min(config.opsPerCase, 50000));
    std::printf("prehashMode = %s\n\n", config.usePrehash ? "ON (fp64/mk precomputed)" : "OFF (hash inside put)");
    std::printf("%-10s %-12s %-8s %-8s %-14s %-14s %-14s %-14s %-8s %-8s %-8s\n",
                "key", "value", "shards", "writers", "put(ops/s)", "get(ops/s)", "scan(ops/s)", "memBytes", "putSmp", "getSmp", "scanSmp");
    std::printf("%-10s %-12s %-8s %-8s %-14s %-14s %-14s %-14s %-8s %-8s %-8s\n",
                "", "", "", "", "", "", "", "", "P50/P90/P99/P999(us)", "P50/P90/P99/P999(us)", "P50/P90/P99/P999(us)");
    std::printf("------------------------------------------------------------------------------------------------\n");

    for (const auto& spec : cases) {
        const ThroughputResult result = runCase(spec, config.opsPerCase, effectiveWriters, config);
        std::printf("%-10d %-12d %-8u %-8d %-14.0f %-14.0f %-14.0f %-14llu %-8u %-8u %-8u\n",
                    spec.keySize,
                    spec.valueSize,
                    shardCount,
                    effectiveWriters,
                    result.putOpsPerSec,
                    result.getOpsPerSec,
                    result.scanOpsPerSec,
                    static_cast<unsigned long long>(result.approxBytes),
                    result.putLatency.sampleCount,
                    result.getLatency.sampleCount,
                    result.scanLatency.sampleCount);
        std::printf("%-10s %-12s %-8s %-8s %-14s %-14s %-14s %-14s %4.2f/%4.2f/%4.2f/%4.2f %4.2f/%4.2f/%4.2f/%4.2f %4.2f/%4.2f/%4.2f/%4.2f\n",
                    "", "", "", "", "", "", "", "",
                    result.putLatency.p50Us,
                    result.putLatency.p90Us,
                    result.putLatency.p99Us,
                    result.putLatency.p999Us,
                    result.getLatency.p50Us,
                    result.getLatency.p90Us,
                    result.getLatency.p99Us,
                    result.getLatency.p999Us,
                    result.scanLatency.p50Us,
                    result.scanLatency.p90Us,
                    result.scanLatency.p99Us,
                    result.scanLatency.p999Us);
        std::printf("  timings(ms): put=%8.2f get=%8.2f scan=%8.2f flush=%8.2f   payload(MiB/s): put=%8.2f get=%8.2f scan=%8.2f   flushes=%llu flushRecords=%llu\n",
                    result.putMs,
                    result.getMs,
                    result.scanMs,
                    result.flushMs,
                    payloadMibPerSec(static_cast<size_t>(spec.keySize + spec.valueSize), config.opsPerCase, result.putMs),
                    payloadMibPerSec(static_cast<size_t>(spec.keySize + spec.valueSize), config.opsPerCase, result.getMs),
                    payloadMibPerSec(static_cast<size_t>(spec.keySize + spec.valueSize), config.opsPerCase, result.scanMs),
                    static_cast<unsigned long long>(result.flushesCompleted),
                    static_cast<unsigned long long>(result.flushRecordsSeen));
        std::printf("------------------------------------------------------------------------------------------------\n");
        std::fflush(stdout);
    }

    return 0;
}

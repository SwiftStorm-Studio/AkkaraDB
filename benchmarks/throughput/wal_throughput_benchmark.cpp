/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// benchmarks/throughput/wal_throughput_benchmark.cpp
#include "TestErrorHandlers.hpp"

/*
 * Sharded WAL throughput benchmark (size sweep).
 *
 * Measures:
 *  - append throughput
 *  - finish throughput, including close() drain/header update/sync semantics
 *  - recovery throughput
 *
 * Usage:
 *   akkaradbWalThroughputBenchmark [opsPerCase] [--writers=N] [--prehash]
 *                                     [--sync=off|async|sync] [--shards=N]
 *                                     [--group-n=N] [--group-micros=N]
 *                                     [--group-bytes=N] [--max-pending-bytes=N]
 *
 * Default:
 *   opsPerCase = 200000
 *   writers      = 16
 *   sync         = async
 *   shards       = 0 (auto, clamped to WAL's 16-shard limit)
 */

#include "akk/core/record/KeyFingerprint.hpp"
#include "akk/core/record/MemHdr16.hpp"
#include "akk/engine/wal/WalRecovery.hpp"
#include "akk/engine/wal/WalWriter.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <format>
#include <span>
#include <string>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;
using namespace akkaradb::core;
using namespace akkaradb::engine::wal;

namespace {
    namespace fs = std::filesystem;

    constexpr uint32_t kLatencySampleMask = 0x3F; // sample 1 / 64 ops to reduce benchmark perturbation

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

    struct ThroughputResult {
        double appendOpsPerSec = 0.0;
        double finishOpsPerSec = 0.0;
        double recoverOpsPerSec = 0.0;
        double appendMs = 0.0;
        double closeMs = 0.0;
        double totalMs = 0.0;
        double recoverMs = 0.0;
        LatencyPercentiles appendLatency;
        uint64_t recoveredEntries = 0;
        uint64_t walBytes = 0;
    };

    [[nodiscard]] static uint32_t resolveAutoShardCount(size_t expectedConcurrentWriters) {
        const size_t n = expectedConcurrentWriters > 0
            ? expectedConcurrentWriters
            : std::max<size_t>(1, std::thread::hardware_concurrency());
        return static_cast<uint32_t>(std::clamp<size_t>(n, 1, 16));
    }

    [[nodiscard]] static int resolveWriterThreads(int requested) {
        if (requested > 0) {
            return requested;
        }
        const unsigned hw = std::thread::hardware_concurrency();
        return static_cast<int>(hw == 0 ? 1u : hw);
    }

    [[nodiscard]] static bool isValidWalShardCount(uint32_t n) {
        return n >= 1 && n <= 16;
    }

    [[nodiscard]] static std::span<const uint8_t> asU8(const std::string& s) {
        return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
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

    [[nodiscard]] static std::string makeFixedBytes(int size, uint64_t seed) {
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

    [[nodiscard]] static const char* syncModeName(WalSyncMode mode) {
        switch (mode) {
            case WalSyncMode::OFF:
                return "off";
            case WalSyncMode::ASYNC:
                return "async";
            case WalSyncMode::SYNC:
                return "sync";
        }
        return "unknown";
    }

    [[nodiscard]] static uint64_t parseByteCount(const std::string& text) {
        if (text.empty()) {
            return 0;
        }

        char* end = nullptr;
        const unsigned long long base = std::strtoull(text.c_str(), &end, 10);
        uint64_t multiplier = 1;
        if (end && *end != '\0') {
            std::string suffix{end};
            std::transform(suffix.begin(), suffix.end(), suffix.begin(), [](unsigned char c) {
                return static_cast<char>(std::tolower(c));
            });
            if (suffix == "k" || suffix == "kb" || suffix == "kib") {
                multiplier = 1024ULL;
            } else if (suffix == "m" || suffix == "mb" || suffix == "mib") {
                multiplier = 1024ULL * 1024ULL;
            } else if (suffix == "g" || suffix == "gb" || suffix == "gib") {
                multiplier = 1024ULL * 1024ULL * 1024ULL;
            } else {
                std::fprintf(stderr, "Unknown byte suffix: %s (use raw bytes, KiB, MiB, or GiB)\n", suffix.c_str());
                std::exit(2);
            }
        }
        return static_cast<uint64_t>(base) * multiplier;
    }

    [[nodiscard]] static double bytesToMib(uint64_t bytes) {
        return static_cast<double>(bytes) / (1024.0 * 1024.0);
    }

    [[nodiscard]] static double mibPerSec(uint64_t bytes, double ms) {
        if (ms <= 0.0) {
            return 0.0;
        }
        return bytesToMib(bytes) * 1000.0 / ms;
    }

    [[nodiscard]] static fs::path makeTempDir(const std::string& suffix) {
        auto dir = fs::temp_directory_path() / ("akkaradbWalBench_" + suffix);
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir, ec);
        if (ec) {
            std::fprintf(stderr, "failed to create temp dir: %s\n", dir.string().c_str());
            std::exit(6);
        }
        return dir;
    }

    [[nodiscard]] static uint64_t sumRegularFileBytes(const fs::path& dir) {
        uint64_t total = 0;
        if (!fs::exists(dir)) {
            return total;
        }
        for (const auto& entry : fs::recursive_directory_iterator(dir)) {
            if (!entry.is_regular_file()) {
                continue;
            }
            std::error_code ec;
            const auto size = entry.file_size(ec);
            if (!ec) {
                total += static_cast<uint64_t>(size);
            }
        }
        return total;
    }

    static void removeTempDir(const fs::path& dir) {
        std::error_code ec;
        fs::remove_all(dir, ec);
    }

    static void runAppendParallel(
        WalWriter& writer,
        const std::vector<std::string>& keys,
        const std::string& value,
        const std::vector<uint64_t>* keyFp64,
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
                writer.append(
                    asU8(keys[i]),
                    asU8(value),
                    static_cast<uint64_t>(i + 1),
                    MemHdr16::FLAG_NORMAL,
                    keyFp64 ? (*keyFp64)[i] : 0ULL
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
                for (size_t i = static_cast<size_t>(tid); i < keys.size(); i += static_cast<size_t>(writerThreads)) {
                    const bool doSample = ((static_cast<uint32_t>(i) & kLatencySampleMask) == 0);
                    const auto t0 = doSample ? Clock::now() : Clock::time_point{};
                    writer.append(
                        asU8(keys[i]),
                        asU8(value),
                        static_cast<uint64_t>(i + 1),
                        MemHdr16::FLAG_NORMAL,
                        keyFp64 ? (*keyFp64)[i] : 0ULL
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

    [[nodiscard]] static WalOptions makeOptions(
        const fs::path& walDir,
        WalSyncMode syncMode,
        uint32_t shardCount,
        uint32_t groupN,
        uint32_t groupMicros,
        uint64_t groupBytes,
        uint64_t asyncMaxPendingBytes
    ) {
        WalOptions opts;
        opts.walDir = walDir;
        opts.syncMode = syncMode;
        opts.shardCount = static_cast<uint16_t>(shardCount);
        opts.groupN = groupN;
        opts.groupMicros = groupMicros;
        opts.groupBytes = groupBytes;
        opts.asyncMaxPendingBytes = asyncMaxPendingBytes;
        return opts;
    }

    [[nodiscard]] static ThroughputResult runCase(
        const CaseSpec spec,
        int opsPerCase,
        bool usePrehash,
        uint32_t shardCount,
        int writerThreads,
        WalSyncMode syncMode,
        uint32_t groupN,
        uint32_t groupMicros,
        uint64_t groupBytes,
        uint64_t asyncMaxPendingBytes
    ) {
        const int warmupOps = std::min(opsPerCase, 50000);

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
        const std::vector<uint64_t>* fpPtr = nullptr;

        if (usePrehash) {
            keyFp64.resize(static_cast<size_t>(opsPerCase));
            for (int i = 0; i < opsPerCase; ++i) {
                const auto& key = keys[static_cast<size_t>(i)];
                keyFp64[static_cast<size_t>(i)] = computeKeyFp64(
                    reinterpret_cast<const uint8_t*>(key.data()),
                    key.size()
                );
            }
            fpPtr = &keyFp64;
        }

        {
            const auto warmupDir = makeTempDir(std::format(
                "warmupK{}_v{}_{}",
                spec.keySize,
                spec.valueSize,
                syncModeName(syncMode)
            ));
            auto warmup = WalWriter::create(makeOptions(
                warmupDir,
                syncMode,
                shardCount,
                groupN,
                groupMicros,
                groupBytes,
                asyncMaxPendingBytes
            ));
            const std::vector<std::string> warmupKeys(keys.begin(), keys.begin() + warmupOps);
            runAppendParallel(*warmup, warmupKeys, value, fpPtr, writerThreads, nullptr);
            warmup->close();
            removeTempDir(warmupDir);
        }

        const auto dir = makeTempDir(std::format(
            "k{}_v{}_{}_w{}_s{}",
            spec.keySize,
            spec.valueSize,
            syncModeName(syncMode),
            writerThreads,
            shardCount
        ));

        auto writer = WalWriter::create(makeOptions(
            dir,
            syncMode,
            shardCount,
            groupN,
            groupMicros,
            groupBytes,
            asyncMaxPendingBytes
        ));
        std::vector<uint32_t> appendLatencyNs;

        const auto writeT0 = Clock::now();
        runAppendParallel(*writer, keys, value, fpPtr, writerThreads, &appendLatencyNs);
        const auto appendMs = std::chrono::duration<double, std::milli>(Clock::now() - writeT0).count();
        const auto closeT0 = Clock::now();
        writer->close();
        const auto closeMs = std::chrono::duration<double, std::milli>(Clock::now() - closeT0).count();
        const auto finishMs = std::chrono::duration<double, std::milli>(Clock::now() - writeT0).count();

        const uint64_t walBytes = sumRegularFileBytes(dir);
        uint64_t recovered = 0;
        const auto recoverT0 = Clock::now();
        const auto recovery = WalRecovery::recover(WalRecoveryOptions{.walDir = dir}, [&](const WalRecoveredEntry&) {
            ++recovered;
        });
        const auto recoverMs = std::chrono::duration<double, std::milli>(Clock::now() - recoverT0).count();

        if (recovered != static_cast<uint64_t>(opsPerCase) || recovery.entriesReplayed != static_cast<uint64_t>(opsPerCase)) {
            std::fprintf(
                stderr,
                "RECOVER count mismatch: expected=%d callback=%llu result=%llu\n",
                opsPerCase,
                static_cast<unsigned long long>(recovered),
                static_cast<unsigned long long>(recovery.entriesReplayed)
            );
            std::exit(7);
        }

        removeTempDir(dir);

        return {
            .appendOpsPerSec = static_cast<double>(opsPerCase) * 1000.0 / appendMs,
            .finishOpsPerSec = static_cast<double>(opsPerCase) * 1000.0 / finishMs,
            .recoverOpsPerSec = static_cast<double>(opsPerCase) * 1000.0 / recoverMs,
            .appendMs = appendMs,
            .closeMs = closeMs,
            .totalMs = finishMs,
            .recoverMs = recoverMs,
            .appendLatency = buildPercentiles(appendLatencyNs),
            .recoveredEntries = recovered,
            .walBytes = walBytes
        };
    }
} // namespace

int main(int argc, char** argv) {
    akkaradb::test::installMsvcTestErrorHandlers();

    int opsPerCase = 200000;
    int writerThreads = 16;
    bool usePrehash = false;
    WalSyncMode syncMode = WalSyncMode::ASYNC;
    uint32_t requestedShards = 0;
    uint32_t groupN = 2048;
    uint32_t groupMicros = 2000;
    uint64_t groupBytes = 4ULL * 1024ULL * 1024ULL;
    uint64_t asyncMaxPendingBytes = 64ULL * 1024ULL * 1024ULL;

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--prehash") {
            usePrehash = true;
            continue;
        }
        if (arg.rfind("--writers=", 0) == 0) {
            writerThreads = std::max(0, std::atoi(arg.substr(10).c_str()));
            continue;
        }
        if (arg.rfind("--sync=", 0) == 0) {
            const std::string mode = arg.substr(7);
            if (mode == "off") {
                syncMode = WalSyncMode::OFF;
                continue;
            }
            if (mode == "async") {
                syncMode = WalSyncMode::ASYNC;
                continue;
            }
            if (mode == "sync") {
                syncMode = WalSyncMode::SYNC;
                continue;
            }
            std::fprintf(stderr, "Unknown sync mode: %s (use off|async|sync)\n", mode.c_str());
            return 2;
        }
        if (arg.rfind("--shards=", 0) == 0) {
            requestedShards = static_cast<uint32_t>(std::max(0, std::atoi(arg.substr(9).c_str())));
            if (requestedShards != 0 && !isValidWalShardCount(requestedShards)) {
                std::fprintf(stderr, "Invalid WAL shard count: %u (use 1..16, or 0 for auto)\n", requestedShards);
                return 2;
            }
            continue;
        }
        if (arg.rfind("--group-n=", 0) == 0) {
            groupN = static_cast<uint32_t>(std::max(1, std::atoi(arg.substr(10).c_str())));
            continue;
        }
        if (arg.rfind("--group-micros=", 0) == 0) {
            groupMicros = static_cast<uint32_t>(std::max(1, std::atoi(arg.substr(15).c_str())));
            continue;
        }
        if (arg.rfind("--group-bytes=", 0) == 0) {
            groupBytes = std::max<uint64_t>(1, parseByteCount(arg.substr(14)));
            continue;
        }
        if (arg.rfind("--max-pending-bytes=", 0) == 0) {
            asyncMaxPendingBytes = std::max<uint64_t>(1, parseByteCount(arg.substr(20)));
            continue;
        }
        opsPerCase = std::max(1, std::atoi(arg.c_str()));
    }

    const std::array<CaseSpec, 6> cases{{
        {8, 16},
        {16, 64},
        {16, 256},
        {32, 1024},
        {32, 4096},
        {64, 16384}
    }};

    const int effectiveWriters = resolveWriterThreads(writerThreads);
    const uint32_t shardCount = requestedShards == 0
        ? resolveAutoShardCount(static_cast<size_t>(effectiveWriters))
        : requestedShards;

    std::printf("Sharded WAL throughput benchmark\n");
    std::printf("opsPerCase = %d\n", opsPerCase);
    std::printf("syncMode = %s\n", syncModeName(syncMode));
    if (writerThreads == 0) {
        std::printf("writerThreads = auto (%d from hwThreads)\n", effectiveWriters);
    } else {
        std::printf("writerThreads = %d\n", writerThreads);
    }
    if (requestedShards == 0) {
        std::printf("resolvedShards = %u (auto, one shard per writer up to WAL limit 16)\n", shardCount);
    } else {
        std::printf("resolvedShards = %u (requested)\n", shardCount);
    }
    std::printf("groupN = %u\n", groupN);
    std::printf("groupMicros = %u\n", groupMicros);
    std::printf("groupBytes = %.2f MiB\n", bytesToMib(groupBytes));
    std::printf("maxPendingBytes = %.2f MiB\n\n", bytesToMib(asyncMaxPendingBytes));
    std::printf("warmupOps   = %d\n\n", std::min(opsPerCase, 50000));
    std::printf("prehashMode = %s\n\n", usePrehash ? "ON (fp64 precomputed)" : "OFF (hash inside append)");
    std::printf("%-10s %-12s %-8s %-8s %-14s %-14s %-14s %-12s %-8s\n",
                "key", "value", "shards", "writers", "append(ops/s)", "finish(ops/s)", "recover(ops/s)", "walBytes", "appSmp");
    std::printf("%-10s %-12s %-8s %-8s %-14s %-14s %-14s %-12s %-8s\n",
                "", "", "", "", "", "", "", "", "P50/P90/P99/P999(us)");
    std::printf("------------------------------------------------------------------------------------------------\n");

    for (const auto& spec : cases) {
        const ThroughputResult result = runCase(
            spec,
            opsPerCase,
            usePrehash,
            shardCount,
            effectiveWriters,
            syncMode,
            groupN,
            groupMicros,
            groupBytes,
            asyncMaxPendingBytes
        );
        std::printf("%-10d %-12d %-8u %-8d %-14.0f %-14.0f %-14.0f %-12llu %-8u\n",
                    spec.keySize,
                    spec.valueSize,
                    shardCount,
                    effectiveWriters,
                    result.appendOpsPerSec,
                    result.finishOpsPerSec,
                    result.recoverOpsPerSec,
                    static_cast<unsigned long long>(result.walBytes),
                    result.appendLatency.sampleCount);
        std::printf("%-10s %-12s %-8s %-8s %-14s %-14s %-14s %-12s %4.2f/%4.2f/%4.2f/%4.2f\n",
                    "", "", "", "", "", "", "", "",
                    result.appendLatency.p50Us,
                    result.appendLatency.p90Us,
                    result.appendLatency.p99Us,
                    result.appendLatency.p999Us);
        std::printf("  timings(ms): append=%8.2f close=%8.2f total=%8.2f recover=%8.2f   throughput(MiB/s): append=%8.2f finish=%8.2f recover=%8.2f\n",
                    result.appendMs,
                    result.closeMs,
                    result.totalMs,
                    result.recoverMs,
                    mibPerSec(result.walBytes, result.appendMs),
                    mibPerSec(result.walBytes, result.totalMs),
                    mibPerSec(result.walBytes, result.recoverMs));
        std::printf("------------------------------------------------------------------------------------------------\n");
        std::fflush(stdout);
    }

    return 0;
}
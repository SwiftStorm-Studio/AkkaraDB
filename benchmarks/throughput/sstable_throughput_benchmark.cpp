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

// benchmarks/throughput/sstableThroughputBenchmark.cpp
#include "TestErrorHandlers.hpp"

/*
 * SSTable throughput benchmark (size sweep).
 *
 * Measures:
 *  - SSTWriter write throughput
 *  - SSTReader point-read throughput
 *  - SSTReader scan throughput
 *
 * Usage:
 *   akkaradbSstableThroughputBenchmark [opsPerCase]
 *       [--readers=N|--writers=N] [--codec=none|zstd]
 *       [--block-size=N] [--cache-bytes=N|NKiB|NMiB|NGiB]
 *       [--max-case-bytes=N|NKiB|NMiB|NGiB] [--fixed-ops|--same-ops]
 *
 * Default:
 *   opsPerCase = 500000
 *   readers      = 16
 *   codec        = zstd
 *   max payload   = 512 MiB per case unless --fixed-ops is set
 */

#include "akk/core/record/KeyFingerprint.hpp"
#include "akk/core/record/SSTHdr32.hpp"
#include "akk/engine/sstable/SSTReader.hpp"
#include "akk/engine/sstable/SSTWriter.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <format>
#include <limits>
#include <span>
#include <string>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;
using namespace akkaradb::core;
namespace sst = akkaradb::engine::sst;

namespace {
    namespace fs = std::filesystem;

    constexpr uint32_t kLatencySampleMask = 0x3F; // sample 1 / 64 ops to reduce benchmark perturbation
    constexpr size_t kScanSampleWindow = 32; // amortize clock resolution/overhead for iterator scan()

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

    struct BenchConfig {
        int opsPerCase = 500000;
        int readerThreads = 16;
        uint32_t blockSize = sst::SST_DEFAULT_BLOCK_SIZE;
        uint64_t blockCacheBytes = 64ULL * 1024ULL * 1024ULL;
        uint64_t maxCasePayloadBytes = 512ULL * 1024ULL * 1024ULL;
        bool fixedOps = false;
        sst::SSTWriter::Codec codec = sst::SSTWriter::Codec::ZSTD;
    };

    struct ThroughputResult {
        int ops = 0;
        double writeOpsPerSec = 0.0;
        double openMs = 0.0;
        double getOpsPerSec = 0.0;
        double scanOpsPerSec = 0.0;
        double writeMs = 0.0;
        double getMs = 0.0;
        double scanMs = 0.0;
        uint64_t fileBytes = 0;
        uint64_t scanRecords = 0;
        LatencyPercentiles getLatency;
        LatencyPercentiles scanLatency;
    };

    [[nodiscard]] static std::span<const uint8_t> asU8(const std::string& s) {
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
        } else {
            return false;
        }

        if (base > std::numeric_limits<uint64_t>::max() / multiplier) {
            return false;
        }
        *out = static_cast<uint64_t>(base) * multiplier;
        return true;
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

    [[nodiscard]] static double payloadMibPerSec(size_t bytesPerOp, int ops, double ms) {
        if (ms <= 0.0) {
            return 0.0;
        }
        const double totalMib = static_cast<double>(bytesPerOp) * static_cast<double>(ops) / (1024.0 * 1024.0);
        return totalMib * 1000.0 / ms;
    }

    [[nodiscard]] static double quantileFromSorted(const std::vector<uint32_t>& sortedNs, double q) {
        if (sortedNs.empty()) {
            return 0.0;
        }
        const double qClamped = std::clamp(q, 0.0, 1.0);
        const size_t idx = static_cast<size_t>(qClamped * static_cast<double>(sortedNs.size() - 1));
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

    [[nodiscard]] static fs::path makeTempDir(const std::string& suffix) {
        auto dir = fs::temp_directory_path() / ("akkaradbSstableBench_" + suffix);
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir, ec);
        if (ec) {
            std::fprintf(stderr, "failed to create temp dir: %s\n", dir.string().c_str());
            std::exit(6);
        }
        return dir;
    }

    static void removeTempDir(const fs::path& dir) {
        std::error_code ec;
        fs::remove_all(dir, ec);
    }

    [[nodiscard]] static int resolveReaderThreads(int requested) {
        if (requested > 0) {
            return requested;
        }
        const unsigned hw = std::thread::hardware_concurrency();
        return static_cast<int>(hw == 0 ? 1u : hw);
    }

    struct Records {
        std::vector<std::string> keys;
        std::string value;
        std::vector<RecordView> views;
    };

    [[nodiscard]] static Records makeRecords(CaseSpec spec, int count) {
        Records records;
        records.keys.reserve(static_cast<size_t>(count));
        records.views.reserve(static_cast<size_t>(count));
        records.value = makeFixedBytes(spec.valueSize, 0xA11CEULL);

        for (int i = 0; i < count; ++i) {
            std::string key = makeFixedBytes(spec.keySize, static_cast<uint64_t>(i) + 1);
            if (spec.keySize >= 10) {
                const auto tail = std::format("{:010d}", i);
                std::memcpy(key.data() + (spec.keySize - 10), tail.data(), 10);
            }
            records.keys.emplace_back(std::move(key));
        }

        std::sort(records.keys.begin(), records.keys.end());

        for (int i = 0; i < count; ++i) {
            const auto& key = records.keys[static_cast<size_t>(i)];
            const auto keySpan = asU8(key);
            const uint64_t fp = computeKeyFp64(keySpan.data(), keySpan.size());
            const uint64_t mk = buildMiniKey(keySpan.data(), keySpan.size());
            records.views.emplace_back(
                keySpan.data(),
                static_cast<uint16_t>(keySpan.size()),
                reinterpret_cast<const uint8_t*>(records.value.data()),
                static_cast<uint16_t>(records.value.size()),
                static_cast<uint64_t>(i + 1),
                SSTHdr32::FLAG_NORMAL,
                fp,
                mk
            );
        }
        return records;
    }

    static void runGetParallel(
        const sst::SSTReader& reader,
        const std::vector<std::string>& keys,
        int readerThreads,
        std::vector<uint32_t>* latencySamplesNs
    ) {
        if (latencySamplesNs) {
            latencySamplesNs->clear();
        }

        if (readerThreads <= 1) {
            std::vector<uint8_t> out;
            if (latencySamplesNs) {
                latencySamplesNs->reserve((keys.size() + kLatencySampleMask) / (kLatencySampleMask + 1));
            }
            for (size_t i = 0; i < keys.size(); ++i) {
                const bool doSample = ((static_cast<uint32_t>(i) & kLatencySampleMask) == 0);
                const auto t0 = doSample ? Clock::now() : Clock::time_point{};
                const auto found = reader.getInto(asU8(keys[i]), out);
                if (!found.has_value() || !*found) {
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
                std::vector<uint8_t> out;
                auto& samples = localSamples[static_cast<size_t>(tid)];
                for (size_t i = static_cast<size_t>(tid); i < keys.size(); i += static_cast<size_t>(readerThreads)) {
                    const bool doSample = ((static_cast<uint32_t>(i) & kLatencySampleMask) == 0);
                    const auto t0 = doSample ? Clock::now() : Clock::time_point{};
                    const auto found = reader.getInto(asU8(keys[i]), out);
                    if (!found.has_value() || !*found) {
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

    [[nodiscard]] static uint64_t runScan(
        const sst::SSTReader& reader,
        size_t expectedRecords,
        std::vector<uint32_t>* latencySamplesNs
    ) {
        if (latencySamplesNs) {
            latencySamplesNs->clear();
            latencySamplesNs->reserve((expectedRecords + kLatencySampleMask) / (kLatencySampleMask + 1));
        }

        uint64_t scanned = 0;
        auto rows = reader.scan();
        auto it = rows.begin();
        const auto end = rows.end();
        while (it != end) {
            const bool doSample = ((static_cast<uint32_t>(scanned) & kLatencySampleMask) == 0);
            if (doSample && latencySamplesNs) {
                const auto t0 = Clock::now();
                size_t windowCount = 0;
                while (windowCount < kScanSampleWindow && it != end) {
                    if (it->key.empty()) {
                        std::fprintf(stderr, "SCAN returned an empty key at i=%llu\n", static_cast<unsigned long long>(scanned));
                        std::exit(5);
                    }
                    ++it;
                    ++windowCount;
                    ++scanned;
                }
                const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count();
                const int64_t perRecordNs = windowCount > 0 ? (dt / static_cast<int64_t>(windowCount)) : 0;
                latencySamplesNs->push_back(static_cast<uint32_t>(std::min<int64_t>(perRecordNs, INT32_MAX)));
                continue;
            }

            if (it->key.empty()) {
                std::fprintf(stderr, "SCAN returned an empty key at i=%llu\n", static_cast<unsigned long long>(scanned));
                std::exit(5);
            }
            ++it;
            ++scanned;
        }
        if (scanned != expectedRecords) {
            std::fprintf(
                stderr,
                "SCAN count mismatch: expected=%zu actual=%llu\n",
                expectedRecords,
                static_cast<unsigned long long>(scanned)
            );
            std::exit(4);
        }
        return scanned;
    }

    [[nodiscard]] static sst::SSTWriter::Options writerOptions(const BenchConfig& config) {
        sst::SSTWriter::Options opts;
        opts.blockSize = config.blockSize;
        opts.codec = config.codec;
        return opts;
    }

    [[nodiscard]] static int resolveCaseOps(CaseSpec spec, const BenchConfig& config) {
        if (config.fixedOps || config.maxCasePayloadBytes == 0) {
            return config.opsPerCase;
        }
        const uint64_t bytesPerOp = static_cast<uint64_t>(spec.keySize) + static_cast<uint64_t>(spec.valueSize);
        if (bytesPerOp == 0) {
            return config.opsPerCase;
        }
        const uint64_t capped = std::max<uint64_t>(1000, config.maxCasePayloadBytes / bytesPerOp);
        return static_cast<int>(std::min<uint64_t>(static_cast<uint64_t>(config.opsPerCase), capped));
    }

    [[nodiscard]] static ThroughputResult runCase(CaseSpec spec, int opsPerCase, const BenchConfig& config, int readerThreads) {
        const int warmupOps = std::min(opsPerCase, 50000);
        const auto warmupDir = makeTempDir(std::format("warmupK{}_v{}", spec.keySize, spec.valueSize));
        {
            auto warmupRecords = makeRecords(spec, warmupOps);
            const auto warmupPath = warmupDir / "warmup.aksst";
            (void)sst::SSTWriter::write(warmupPath, warmupRecords.views, writerOptions(config));
            auto warmupReader = sst::SSTReader::open(warmupPath, sst::SSTReader::Options{config.blockCacheBytes});
            if (!warmupReader) {
                std::fprintf(stderr, "failed to open warmup SST\n");
                std::exit(7);
            }
            runGetParallel(*warmupReader, warmupRecords.keys, readerThreads, nullptr);
            auto warmupScan = warmupReader->scan();
            for (auto&& row : warmupScan) {
                (void)row;
            }
            warmupReader.reset();
        }
        removeTempDir(warmupDir);

        auto records = makeRecords(spec, opsPerCase);
        const auto dir = makeTempDir(std::format("k{}_v{}_r{}", spec.keySize, spec.valueSize, readerThreads));
        const auto path = dir / "bench.aksst";

        const auto writeT0 = Clock::now();
        const auto writeResult = sst::SSTWriter::write(path, records.views, writerOptions(config));
        const auto writeMs = std::chrono::duration<double, std::milli>(Clock::now() - writeT0).count();

        const auto openT0 = Clock::now();
        auto reader = sst::SSTReader::open(path, sst::SSTReader::Options{config.blockCacheBytes});
        const auto openMs = std::chrono::duration<double, std::milli>(Clock::now() - openT0).count();
        if (!reader) {
            std::fprintf(stderr, "failed to open SST: %s\n", path.string().c_str());
            std::exit(8);
        }

        std::vector<uint32_t> getLatencyNs;
        const auto getT0 = Clock::now();
        runGetParallel(*reader, records.keys, readerThreads, &getLatencyNs);
        const auto getMs = std::chrono::duration<double, std::milli>(Clock::now() - getT0).count();

        std::vector<uint32_t> scanLatencyNs;
        const auto scanT0 = Clock::now();
        const uint64_t scanned = runScan(*reader, records.keys.size(), &scanLatencyNs);
        const auto scanMs = std::chrono::duration<double, std::milli>(Clock::now() - scanT0).count();

        reader.reset();
        removeTempDir(dir);

        return {
            .ops = opsPerCase,
            .writeOpsPerSec = static_cast<double>(opsPerCase) * 1000.0 / writeMs,
            .openMs = openMs,
            .getOpsPerSec = static_cast<double>(opsPerCase) * 1000.0 / getMs,
            .scanOpsPerSec = static_cast<double>(scanned) * 1000.0 / scanMs,
            .writeMs = writeMs,
            .getMs = getMs,
            .scanMs = scanMs,
            .fileBytes = writeResult.fileSizeBytes,
            .scanRecords = scanned,
            .getLatency = buildPercentiles(getLatencyNs),
            .scanLatency = buildPercentiles(scanLatencyNs)
        };
    }

    [[nodiscard]] static const char* codecName(sst::SSTWriter::Codec codec) {
        switch (codec) {
            case sst::SSTWriter::Codec::NONE:
                return "none";
            case sst::SSTWriter::Codec::ZSTD:
                return "zstd";
        }
        return "unknown";
    }
} // namespace

int main(int argc, char** argv) {
    akkaradb::test::installMsvcTestErrorHandlers();

    BenchConfig config;

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg.rfind("--readers=", 0) == 0 || arg.rfind("--writers=", 0) == 0) {
            config.readerThreads = std::max(0, std::atoi(arg.substr(10).c_str()));
            continue;
        }
        if (arg.rfind("--codec=", 0) == 0) {
            const std::string codec = arg.substr(8);
            if (codec == "none") {
                config.codec = sst::SSTWriter::Codec::NONE;
                continue;
            }
            if (codec == "zstd") {
                config.codec = sst::SSTWriter::Codec::ZSTD;
                continue;
            }
            std::fprintf(stderr, "Unknown codec: %s (use none|zstd)\n", codec.c_str());
            return 2;
        }
        if (arg.rfind("--block-size=", 0) == 0) {
            uint64_t parsed = 0;
            if (!parseU64WithSuffix(arg.substr(13), &parsed) || parsed > std::numeric_limits<uint32_t>::max()) {
                std::fprintf(stderr, "Invalid --block-size value: %s\n", arg.substr(13).c_str());
                return 2;
            }
            config.blockSize = static_cast<uint32_t>(parsed);
            continue;
        }
        if (arg.rfind("--cache-bytes=", 0) == 0) {
            uint64_t parsed = 0;
            if (!parseU64WithSuffix(arg.substr(14), &parsed)) {
                std::fprintf(stderr, "Invalid --cache-bytes value: %s\n", arg.substr(14).c_str());
                return 2;
            }
            config.blockCacheBytes = parsed;
            continue;
        }
        if (arg.rfind("--max-case-bytes=", 0) == 0) {
            uint64_t parsed = 0;
            if (!parseU64WithSuffix(arg.substr(17), &parsed)) {
                std::fprintf(stderr, "Invalid --max-case-bytes value: %s\n", arg.substr(17).c_str());
                return 2;
            }
            config.maxCasePayloadBytes = parsed;
            continue;
        }
        if (arg == "--fixed-ops" || arg == "--same-ops") {
            config.fixedOps = true;
            continue;
        }
        config.opsPerCase = std::max(1, std::atoi(arg.c_str()));
    }

    const std::array<CaseSpec, 6> cases{{
        {8, 16},
        {16, 64},
        {16, 256},
        {32, 1024},
        {32, 4096},
        {64, 16384}
    }};

    const int effectiveReaders = resolveReaderThreads(config.readerThreads);

    std::printf("SSTable throughput benchmark\n");
    std::printf("opsPerCase = %d\n", config.opsPerCase);
    std::printf("codec = %s\n", codecName(config.codec));
    std::printf("blockSize = %u\n", config.blockSize);
    std::printf("blockCacheBytes = %.2f MiB\n", bytesToMib(config.blockCacheBytes));
    if (config.fixedOps || config.maxCasePayloadBytes == 0) {
        std::printf("maxCasePayloadBytes = disabled (--fixed-ops/--same-ops)\n");
    } else {
        std::printf("maxCasePayloadBytes = %.2f MiB\n", bytesToMib(config.maxCasePayloadBytes));
    }
    if (config.readerThreads == 0) {
        std::printf("readerThreads = auto (%d from hwThreads)\n", effectiveReaders);
    } else {
        std::printf("readerThreads = %d\n", config.readerThreads);
    }
    std::printf("warmupOps = %d\n\n", std::min(config.opsPerCase, 50000));
    std::printf("%-10s %-12s %-10s %-8s %-14s %-14s %-14s %-12s %-8s %-8s\n",
                "key", "value", "ops", "readers", "write(ops/s)", "get(ops/s)", "scan(ops/s)", "sstBytes", "getSmp", "scanSmp");
    std::printf("%-10s %-12s %-10s %-8s %-14s %-14s %-14s %-12s %-8s %-8s\n",
                "", "", "", "", "", "", "", "", "P50/P90/P99/P999(us)", "P50/P90/P99/P999(us)");
    std::printf("------------------------------------------------------------------------------------------------\n");

    for (const auto& spec : cases) {
        const int caseOps = resolveCaseOps(spec, config);
        const ThroughputResult result = runCase(spec, caseOps, config, effectiveReaders);
        std::printf("%-10d %-12d %-10d %-8d %-14.0f %-14.0f %-14.0f %-12llu %-8u %-8u\n",
                    spec.keySize,
                    spec.valueSize,
                    result.ops,
                    effectiveReaders,
                    result.writeOpsPerSec,
                    result.getOpsPerSec,
                    result.scanOpsPerSec,
                    static_cast<unsigned long long>(result.fileBytes),
                    result.getLatency.sampleCount,
                    result.scanLatency.sampleCount);
        std::printf("%-10s %-12s %-10s %-8s %-14s %-14s %-14s %-12s %4.2f/%4.2f/%4.2f/%4.2f %4.2f/%4.2f/%4.2f/%4.2f\n",
                    "", "", "", "", "", "", "", "",
                    result.getLatency.p50Us,
                    result.getLatency.p90Us,
                    result.getLatency.p99Us,
                    result.getLatency.p999Us,
                    result.scanLatency.p50Us,
                    result.scanLatency.p90Us,
                    result.scanLatency.p99Us,
                    result.scanLatency.p999Us);
        std::printf("  timings(ms): write=%8.2f open=%8.2f get=%8.2f scan=%8.2f   file(MiB/s): write=%8.2f get=%8.2f scan=%8.2f   payload(MiB/s): write=%8.2f get=%8.2f scan=%8.2f\n",
                    result.writeMs,
                    result.openMs,
                    result.getMs,
                    result.scanMs,
                    mibPerSec(result.fileBytes, result.writeMs),
                    mibPerSec(result.fileBytes, result.getMs),
                    mibPerSec(result.fileBytes, result.scanMs),
                    payloadMibPerSec(static_cast<size_t>(spec.keySize + spec.valueSize), result.ops, result.writeMs),
                    payloadMibPerSec(static_cast<size_t>(spec.keySize + spec.valueSize), result.ops, result.getMs),
                    payloadMibPerSec(static_cast<size_t>(spec.keySize + spec.valueSize), result.ops, result.scanMs));
        std::printf("------------------------------------------------------------------------------------------------\n");
        std::fflush(stdout);
    }

    return 0;
}

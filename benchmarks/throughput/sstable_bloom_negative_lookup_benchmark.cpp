/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// benchmarks/throughput/sstable_bloom_negative_lookup_benchmark.cpp
#include "TestErrorHandlers.hpp"

/*
 * SSTable Bloom negative lookup benchmark.
 *
 * Measures how fast SSTReader::contains() rejects absent keys that are inside
 * the SST key range. That path exercises key fingerprinting and the SST bloom
 * filter instead of being short-circuited by first/last key range checks.
 *
 * Layout:
 *   existing : "bfkeyXXXXXXXXXX" where X = i * 2
 *   negative : "bfkeyXXXXXXXXXX" where X = i * 2 + 1
 *
 * Usage:
 *   akkaradbSstableBloomNegativeLookupBenchmark [keys] [probes]
 *       [--readers=N|--writers=N] [--bits-per-key=N]
 *       [--codec=none|zstd] [--block-size=N|NKiB|NMiB]
 *       [--cache-bytes=N|NKiB|NMiB|NGiB] [--value-size=N]
 *
 * Default:
 *   keys         = 1000000
 *   probes       = 5000000
 *   readers      = 16
 *   bitsPerKey = 10
 *   codec        = zstd
 */

#include "akk/core/record/KeyFingerprint.hpp"
#include "akk/core/record/SSTHdr32.hpp"
#include "akk/engine/sstable/SSTReader.hpp"
#include "akk/engine/sstable/SSTWriter.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <format>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;
using namespace akkaradb::core;
namespace sst = akkaradb::engine::sst;

namespace {
    namespace fs = std::filesystem;

    constexpr uint32_t kLatencySampleMask = 0x3F; // sample 1 / 64 probes

    struct LatencyPercentiles {
        double p50Us = 0.0;
        double p90Us = 0.0;
        double p99Us = 0.0;
        double p999Us = 0.0;
        uint32_t sampleCount = 0;
    };

    struct BenchConfig {
        int keys = 1'000'000;
        int probes = 5'000'000;
        int readerThreads = 16;
        uint32_t bitsPerKey = sst::SST_DEFAULT_BLOOM_BITS_PER_KEY;
        uint32_t blockSize = sst::SST_DEFAULT_BLOCK_SIZE;
        uint64_t blockCacheBytes = 64ULL * 1024ULL * 1024ULL;
        int valueSize = 1;
        sst::SSTWriter::Codec codec = sst::SSTWriter::Codec::ZSTD;
    };

    struct Records {
        std::vector<std::string> existingKeys;
        std::vector<std::string> negativeKeys;
        std::string value;
        std::vector<RecordView> views;
    };

    struct Result {
        double opsPerSec = 0.0;
        double totalMs = 0.0;
        uint64_t expectedAbsent = 0;
        uint64_t wrongPresent = 0;
        LatencyPercentiles latency;
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

    [[nodiscard]] static std::string makeValue(int size) {
        std::string out;
        out.resize(static_cast<size_t>(std::max(1, size)));
        for (size_t i = 0; i < out.size(); ++i) {
            out[i] = static_cast<char>('a' + (i % 26));
        }
        return out;
    }

    [[nodiscard]] static fs::path makeTempDir(const std::string& suffix) {
        auto dir = fs::temp_directory_path() / ("akkaradbSstableBloomNegativeBench_" + suffix);
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

    [[nodiscard]] static uint64_t regularFileBytes(const fs::path& path) {
        std::error_code ec;
        const auto size = fs::file_size(path, ec);
        return ec ? 0ULL : static_cast<uint64_t>(size);
    }

    [[nodiscard]] static int resolveReaderThreads(int requested) {
        if (requested > 0) {
            return requested;
        }
        const unsigned hw = std::thread::hardware_concurrency();
        return static_cast<int>(hw == 0 ? 1u : hw);
    }

    [[nodiscard]] static Records makeRecords(int keyCount, int valueSize) {
        Records records;
        records.existingKeys.reserve(static_cast<size_t>(keyCount));
        records.negativeKeys.reserve(static_cast<size_t>(std::max(0, keyCount - 1)));
        records.views.reserve(static_cast<size_t>(keyCount));
        records.value = makeValue(valueSize);

        for (int i = 0; i < keyCount; ++i) {
            records.existingKeys.emplace_back(std::format("bfkey_{:010d}", i * 2));
            if (i + 1 < keyCount) {
                records.negativeKeys.emplace_back(std::format("bfkey_{:010d}", i * 2 + 1));
            }
        }

        for (int i = 0; i < keyCount; ++i) {
            const auto& key = records.existingKeys[static_cast<size_t>(i)];
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

    [[nodiscard]] static sst::SSTWriter::Options writerOptions(const BenchConfig& config) {
        sst::SSTWriter::Options opts;
        opts.blockSize = config.blockSize;
        opts.bloomBitsPerKey = config.bitsPerKey;
        opts.codec = config.codec;
        return opts;
    }

    static void warmupReader(const sst::SSTReader& reader, const std::vector<std::string>& negativeKeys) {
        volatile uint64_t observed = 0;
        const size_t warmup = std::min<size_t>(negativeKeys.size(), 50'000);
        const size_t start = negativeKeys.size() - warmup;
        for (size_t i = start; i < negativeKeys.size(); ++i) {
            const auto result = reader.contains(asU8(negativeKeys[i]));
            if (result.has_value()) {
                observed += *result ? 1u : 0u;
            }
        }
        (void)observed;
    }

    [[nodiscard]] static Result runNegativeLookup(
        const sst::SSTReader& reader,
        const std::vector<std::string>& negativeKeys,
        int probes,
        int readerThreads
    ) {
        if (negativeKeys.empty()) {
            std::fprintf(stderr, "need at least two keys to generate in-range negative probes\n");
            std::exit(2);
        }

        std::atomic<uint64_t> expectedAbsent{0};
        std::atomic<uint64_t> wrongPresent{0};
        std::vector<std::vector<uint32_t>> localSamples(static_cast<size_t>(std::max(1, readerThreads)));
        for (auto& samples : localSamples) {
            samples.reserve((static_cast<size_t>(probes) / static_cast<size_t>(std::max(1, readerThreads)) + kLatencySampleMask) / (kLatencySampleMask + 1));
        }

        const auto t0 = Clock::now();
        std::vector<std::thread> threads;
        threads.reserve(static_cast<size_t>(std::max(1, readerThreads)));

        for (int tid = 0; tid < std::max(1, readerThreads); ++tid) {
            threads.emplace_back([&, tid]() {
                uint64_t localAbsent = 0;
                uint64_t localWrong = 0;
                auto& samples = localSamples[static_cast<size_t>(tid)];
                for (int i = tid; i < probes; i += std::max(1, readerThreads)) {
                    const auto& key = negativeKeys[static_cast<size_t>(i) % negativeKeys.size()];
                    const bool doSample = ((static_cast<uint32_t>(i) & kLatencySampleMask) == 0);
                    const auto opT0 = doSample ? Clock::now() : Clock::time_point{};
                    const std::optional<bool> found = reader.contains(asU8(key));
                    if (doSample) {
                        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - opT0).count();
                        samples.push_back(static_cast<uint32_t>(std::min<int64_t>(dt, INT32_MAX)));
                    }

                    if (!found.has_value()) {
                        ++localAbsent;
                    } else if (*found) {
                        ++localWrong;
                    } else {
                        ++localAbsent;
                    }
                }
                expectedAbsent.fetch_add(localAbsent, std::memory_order_relaxed);
                wrongPresent.fetch_add(localWrong, std::memory_order_relaxed);
            });
        }

        for (auto& th : threads) {
            th.join();
        }
        const auto totalMs = std::chrono::duration<double, std::milli>(Clock::now() - t0).count();

        std::vector<uint32_t> samples;
        size_t sampleCount = 0;
        for (const auto& local : localSamples) {
            sampleCount += local.size();
        }
        samples.reserve(sampleCount);
        for (auto& local : localSamples) {
            samples.insert(samples.end(), local.begin(), local.end());
        }

        return {
            .opsPerSec = static_cast<double>(probes) * 1000.0 / totalMs,
            .totalMs = totalMs,
            .expectedAbsent = expectedAbsent.load(std::memory_order_relaxed),
            .wrongPresent = wrongPresent.load(std::memory_order_relaxed),
            .latency = buildPercentiles(samples)
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
    int positional = 0;

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg.rfind("--readers=", 0) == 0 || arg.rfind("--writers=", 0) == 0) {
            config.readerThreads = std::max(0, std::atoi(arg.substr(10).c_str()));
            continue;
        }
        if (arg.rfind("--bits-per-key=", 0) == 0) {
            config.bitsPerKey = static_cast<uint32_t>(std::max(1, std::atoi(arg.substr(15).c_str())));
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
        if (arg.rfind("--value-size=", 0) == 0) {
            config.valueSize = std::max(1, std::atoi(arg.substr(13).c_str()));
            if (config.valueSize > UINT16_MAX) {
                std::fprintf(stderr, "Invalid --value-size: %d (max %u)\n", config.valueSize, UINT16_MAX);
                return 2;
            }
            continue;
        }

        if (positional == 0) {
            config.keys = std::max(2, std::atoi(arg.c_str()));
        } else if (positional == 1) {
            config.probes = std::max(1, std::atoi(arg.c_str()));
        } else {
            std::fprintf(stderr, "Unexpected argument: %s\n", arg.c_str());
            return 2;
        }
        ++positional;
    }

    const int effectiveReaders = resolveReaderThreads(config.readerThreads);
    const auto dir = makeTempDir(std::format(
        "k{}_p{}_bpk{}_r{}",
        config.keys,
        config.probes,
        config.bitsPerKey,
        effectiveReaders
    ));
    const auto path = dir / "bench.aksst";

    std::printf("SSTable Bloom negative lookup benchmark\n");
    std::printf("keys = %d\n", config.keys);
    std::printf("negativeKeys = %d\n", config.keys - 1);
    std::printf("probes = %d\n", config.probes);
    std::printf("codec = %s\n", codecName(config.codec));
    std::printf("bitsPerKey = %u\n", config.bitsPerKey);
    std::printf("blockSize = %u\n", config.blockSize);
    std::printf("blockCacheBytes = %.2f MiB\n", bytesToMib(config.blockCacheBytes));
    std::printf("valueSize = %d\n", config.valueSize);
    if (config.readerThreads == 0) {
        std::printf("readerThreads = auto (%d from hwThreads)\n", effectiveReaders);
    } else {
        std::printf("readerThreads = %d\n", config.readerThreads);
    }

    const auto records = makeRecords(config.keys, config.valueSize);
    const auto writeT0 = Clock::now();
    const auto writeResult = sst::SSTWriter::write(path, records.views, writerOptions(config));
    const auto writeMs = std::chrono::duration<double, std::milli>(Clock::now() - writeT0).count();
    const uint64_t fileBytes = writeResult.fileSizeBytes == 0 ? regularFileBytes(path) : writeResult.fileSizeBytes;

    const auto openT0 = Clock::now();
    auto reader = sst::SSTReader::open(path, sst::SSTReader::Options{config.blockCacheBytes});
    const auto openMs = std::chrono::duration<double, std::milli>(Clock::now() - openT0).count();
    if (!reader) {
        std::fprintf(stderr, "failed to open SST: %s\n", path.string().c_str());
        removeTempDir(dir);
        return 8;
    }

    warmupReader(*reader, records.negativeKeys);
    const Result result = runNegativeLookup(*reader, records.negativeKeys, config.probes, effectiveReaders);

    std::printf("writeMs = %.2f\n", writeMs);
    std::printf("openMs = %.2f\n", openMs);
    std::printf("sstBytes = %llu\n", static_cast<unsigned long long>(fileBytes));
    std::printf("writeFileMibPerSec = %.2f\n\n", mibPerSec(fileBytes, writeMs));

    std::printf("%-12s %-12s %-8s %-14s %-14s %-8s %-14s\n",
                "keys", "probes", "readers", "negative(ops/s)", "totalMs", "samples", "wrongPresent");
    std::printf("%-12d %-12d %-8d %-14.0f %-14.2f %-8u %-14llu\n",
                config.keys,
                config.probes,
                effectiveReaders,
                result.opsPerSec,
                result.totalMs,
                result.latency.sampleCount,
                static_cast<unsigned long long>(result.wrongPresent));
    std::printf("latency(us): p50=%.2f p90=%.2f p99=%.2f p999=%.2f\n",
                result.latency.p50Us,
                result.latency.p90Us,
                result.latency.p99Us,
                result.latency.p999Us);
    std::printf("expectedAbsent = %llu\n", static_cast<unsigned long long>(result.expectedAbsent));

    reader.reset();
    removeTempDir(dir);
    return result.wrongPresent == 0 ? 0 : 9;
}

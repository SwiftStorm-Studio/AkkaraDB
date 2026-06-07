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
 *   existing : "bfkey_XXXXXXXXXX" where X = i * 2
 *   negative : "bfkey_XXXXXXXXXX" where X = i * 2 + 1
 *
 * Usage:
 *   akkaradb_sstable_bloom_negative_lookup_benchmark [keys] [probes]
 *       [--readers=N|--writers=N] [--bits-per-key=N]
 *       [--codec=none|zstd] [--block-size=N|NKiB|NMiB]
 *       [--cache-bytes=N|NKiB|NMiB|NGiB] [--value-size=N]
 *
 * Default:
 *   keys         = 1000000
 *   probes       = 5000000
 *   readers      = 16
 *   bits_per_key = 10
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
        double p50_us = 0.0;
        double p90_us = 0.0;
        double p99_us = 0.0;
        double p999_us = 0.0;
        uint32_t sample_count = 0;
    };

    struct BenchConfig {
        int keys = 1'000'000;
        int probes = 5'000'000;
        int reader_threads = 16;
        uint32_t bits_per_key = sst::SST_DEFAULT_BLOOM_BITS_PER_KEY;
        uint32_t block_size = sst::SST_DEFAULT_BLOCK_SIZE;
        uint64_t block_cache_bytes = 64ULL * 1024ULL * 1024ULL;
        int value_size = 1;
        sst::SSTWriter::Codec codec = sst::SSTWriter::Codec::Zstd;
    };

    struct Records {
        std::vector<std::string> existing_keys;
        std::vector<std::string> negative_keys;
        std::string value;
        std::vector<RecordView> views;
    };

    struct Result {
        double ops_per_sec = 0.0;
        double total_ms = 0.0;
        uint64_t expected_absent = 0;
        uint64_t wrong_present = 0;
        LatencyPercentiles latency;
    };

    [[nodiscard]] static std::span<const uint8_t> as_u8(const std::string& s) {
        return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
    }

    [[nodiscard]] static std::string lower_ascii(std::string s) {
        for (char& ch : s) {
            if (ch >= 'A' && ch <= 'Z') {
                ch = static_cast<char>(ch - 'A' + 'a');
            }
        }
        return s;
    }

    [[nodiscard]] static bool parse_u64_with_suffix(const std::string& text, uint64_t* out) {
        if (text.empty() || out == nullptr) {
            return false;
        }

        char* end = nullptr;
        const unsigned long long base = std::strtoull(text.c_str(), &end, 10);
        if (end == text.c_str()) {
            return false;
        }

        const std::string suffix = lower_ascii(std::string{end});
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

    [[nodiscard]] static double bytes_to_mib(uint64_t bytes) {
        return static_cast<double>(bytes) / (1024.0 * 1024.0);
    }

    [[nodiscard]] static double mib_per_sec(uint64_t bytes, double ms) {
        if (ms <= 0.0) {
            return 0.0;
        }
        return bytes_to_mib(bytes) * 1000.0 / ms;
    }

    [[nodiscard]] static double quantile_from_sorted(const std::vector<uint32_t>& sorted_ns, double q) {
        if (sorted_ns.empty()) {
            return 0.0;
        }
        const double q_clamped = std::clamp(q, 0.0, 1.0);
        const size_t idx = static_cast<size_t>(q_clamped * static_cast<double>(sorted_ns.size() - 1));
        return static_cast<double>(sorted_ns[idx]) / 1000.0;
    }

    [[nodiscard]] static LatencyPercentiles build_percentiles(std::vector<uint32_t>& samples_ns) {
        LatencyPercentiles out{};
        if (samples_ns.empty()) {
            return out;
        }

        std::sort(samples_ns.begin(), samples_ns.end());
        out.sample_count = static_cast<uint32_t>(samples_ns.size());
        out.p50_us = quantile_from_sorted(samples_ns, 0.50);
        out.p90_us = quantile_from_sorted(samples_ns, 0.90);
        out.p99_us = quantile_from_sorted(samples_ns, 0.99);
        out.p999_us = quantile_from_sorted(samples_ns, 0.999);
        return out;
    }

    [[nodiscard]] static std::string make_value(int size) {
        std::string out;
        out.resize(static_cast<size_t>(std::max(1, size)));
        for (size_t i = 0; i < out.size(); ++i) {
            out[i] = static_cast<char>('a' + (i % 26));
        }
        return out;
    }

    [[nodiscard]] static fs::path make_temp_dir(const std::string& suffix) {
        auto dir = fs::temp_directory_path() / ("akkaradb_sstable_bloom_negative_bench_" + suffix);
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir, ec);
        if (ec) {
            std::fprintf(stderr, "failed to create temp dir: %s\n", dir.string().c_str());
            std::exit(6);
        }
        return dir;
    }

    static void remove_temp_dir(const fs::path& dir) {
        std::error_code ec;
        fs::remove_all(dir, ec);
    }

    [[nodiscard]] static uint64_t regular_file_bytes(const fs::path& path) {
        std::error_code ec;
        const auto size = fs::file_size(path, ec);
        return ec ? 0ULL : static_cast<uint64_t>(size);
    }

    [[nodiscard]] static int resolve_reader_threads(int requested) {
        if (requested > 0) {
            return requested;
        }
        const unsigned hw = std::thread::hardware_concurrency();
        return static_cast<int>(hw == 0 ? 1u : hw);
    }

    [[nodiscard]] static Records make_records(int key_count, int value_size) {
        Records records;
        records.existing_keys.reserve(static_cast<size_t>(key_count));
        records.negative_keys.reserve(static_cast<size_t>(std::max(0, key_count - 1)));
        records.views.reserve(static_cast<size_t>(key_count));
        records.value = make_value(value_size);

        for (int i = 0; i < key_count; ++i) {
            records.existing_keys.emplace_back(std::format("bfkey_{:010d}", i * 2));
            if (i + 1 < key_count) {
                records.negative_keys.emplace_back(std::format("bfkey_{:010d}", i * 2 + 1));
            }
        }

        for (int i = 0; i < key_count; ++i) {
            const auto& key = records.existing_keys[static_cast<size_t>(i)];
            const auto key_span = as_u8(key);
            const uint64_t fp = compute_key_fp64(key_span.data(), key_span.size());
            const uint64_t mk = build_mini_key(key_span.data(), key_span.size());
            records.views.emplace_back(
                key_span.data(),
                static_cast<uint16_t>(key_span.size()),
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

    [[nodiscard]] static sst::SSTWriter::Options writer_options(const BenchConfig& config) {
        sst::SSTWriter::Options opts;
        opts.block_size = config.block_size;
        opts.bloom_bits_per_key = config.bits_per_key;
        opts.codec = config.codec;
        return opts;
    }

    static void warmup_reader(const sst::SSTReader& reader, const std::vector<std::string>& negative_keys) {
        volatile uint64_t observed = 0;
        const size_t warmup = std::min<size_t>(negative_keys.size(), 50'000);
        const size_t start = negative_keys.size() - warmup;
        for (size_t i = start; i < negative_keys.size(); ++i) {
            const auto result = reader.contains(as_u8(negative_keys[i]));
            if (result.has_value()) {
                observed += *result ? 1u : 0u;
            }
        }
        (void)observed;
    }

    [[nodiscard]] static Result run_negative_lookup(
        const sst::SSTReader& reader,
        const std::vector<std::string>& negative_keys,
        int probes,
        int reader_threads
    ) {
        if (negative_keys.empty()) {
            std::fprintf(stderr, "need at least two keys to generate in-range negative probes\n");
            std::exit(2);
        }

        std::atomic<uint64_t> expected_absent{0};
        std::atomic<uint64_t> wrong_present{0};
        std::vector<std::vector<uint32_t>> local_samples(static_cast<size_t>(std::max(1, reader_threads)));
        for (auto& samples : local_samples) {
            samples.reserve((static_cast<size_t>(probes) / static_cast<size_t>(std::max(1, reader_threads)) + kLatencySampleMask) / (kLatencySampleMask + 1));
        }

        const auto t0 = Clock::now();
        std::vector<std::thread> threads;
        threads.reserve(static_cast<size_t>(std::max(1, reader_threads)));

        for (int tid = 0; tid < std::max(1, reader_threads); ++tid) {
            threads.emplace_back([&, tid]() {
                uint64_t local_absent = 0;
                uint64_t local_wrong = 0;
                auto& samples = local_samples[static_cast<size_t>(tid)];
                for (int i = tid; i < probes; i += std::max(1, reader_threads)) {
                    const auto& key = negative_keys[static_cast<size_t>(i) % negative_keys.size()];
                    const bool do_sample = ((static_cast<uint32_t>(i) & kLatencySampleMask) == 0);
                    const auto op_t0 = do_sample ? Clock::now() : Clock::time_point{};
                    const std::optional<bool> found = reader.contains(as_u8(key));
                    if (do_sample) {
                        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - op_t0).count();
                        samples.push_back(static_cast<uint32_t>(std::min<int64_t>(dt, INT32_MAX)));
                    }

                    if (!found.has_value()) {
                        ++local_absent;
                    } else if (*found) {
                        ++local_wrong;
                    } else {
                        ++local_absent;
                    }
                }
                expected_absent.fetch_add(local_absent, std::memory_order_relaxed);
                wrong_present.fetch_add(local_wrong, std::memory_order_relaxed);
            });
        }

        for (auto& th : threads) {
            th.join();
        }
        const auto total_ms = std::chrono::duration<double, std::milli>(Clock::now() - t0).count();

        std::vector<uint32_t> samples;
        size_t sample_count = 0;
        for (const auto& local : local_samples) {
            sample_count += local.size();
        }
        samples.reserve(sample_count);
        for (auto& local : local_samples) {
            samples.insert(samples.end(), local.begin(), local.end());
        }

        return {
            .ops_per_sec = static_cast<double>(probes) * 1000.0 / total_ms,
            .total_ms = total_ms,
            .expected_absent = expected_absent.load(std::memory_order_relaxed),
            .wrong_present = wrong_present.load(std::memory_order_relaxed),
            .latency = build_percentiles(samples)
        };
    }

    [[nodiscard]] static const char* codec_name(sst::SSTWriter::Codec codec) {
        switch (codec) {
            case sst::SSTWriter::Codec::None:
                return "none";
            case sst::SSTWriter::Codec::Zstd:
                return "zstd";
        }
        return "unknown";
    }
} // namespace

int main(int argc, char** argv) {
    akkara::test::install_msvc_test_error_handlers();

    BenchConfig config;
    int positional = 0;

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg.rfind("--readers=", 0) == 0 || arg.rfind("--writers=", 0) == 0) {
            config.reader_threads = std::max(0, std::atoi(arg.substr(10).c_str()));
            continue;
        }
        if (arg.rfind("--bits-per-key=", 0) == 0) {
            config.bits_per_key = static_cast<uint32_t>(std::max(1, std::atoi(arg.substr(15).c_str())));
            continue;
        }
        if (arg.rfind("--codec=", 0) == 0) {
            const std::string codec = arg.substr(8);
            if (codec == "none") {
                config.codec = sst::SSTWriter::Codec::None;
                continue;
            }
            if (codec == "zstd") {
                config.codec = sst::SSTWriter::Codec::Zstd;
                continue;
            }
            std::fprintf(stderr, "Unknown codec: %s (use none|zstd)\n", codec.c_str());
            return 2;
        }
        if (arg.rfind("--block-size=", 0) == 0) {
            uint64_t parsed = 0;
            if (!parse_u64_with_suffix(arg.substr(13), &parsed) || parsed > std::numeric_limits<uint32_t>::max()) {
                std::fprintf(stderr, "Invalid --block-size value: %s\n", arg.substr(13).c_str());
                return 2;
            }
            config.block_size = static_cast<uint32_t>(parsed);
            continue;
        }
        if (arg.rfind("--cache-bytes=", 0) == 0) {
            uint64_t parsed = 0;
            if (!parse_u64_with_suffix(arg.substr(14), &parsed)) {
                std::fprintf(stderr, "Invalid --cache-bytes value: %s\n", arg.substr(14).c_str());
                return 2;
            }
            config.block_cache_bytes = parsed;
            continue;
        }
        if (arg.rfind("--value-size=", 0) == 0) {
            config.value_size = std::max(1, std::atoi(arg.substr(13).c_str()));
            if (config.value_size > UINT16_MAX) {
                std::fprintf(stderr, "Invalid --value-size: %d (max %u)\n", config.value_size, UINT16_MAX);
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

    const int effective_readers = resolve_reader_threads(config.reader_threads);
    const auto dir = make_temp_dir(std::format(
        "k{}_p{}_bpk{}_r{}",
        config.keys,
        config.probes,
        config.bits_per_key,
        effective_readers
    ));
    const auto path = dir / "bench.aksst";

    std::printf("SSTable Bloom negative lookup benchmark\n");
    std::printf("keys = %d\n", config.keys);
    std::printf("negative_keys = %d\n", config.keys - 1);
    std::printf("probes = %d\n", config.probes);
    std::printf("codec = %s\n", codec_name(config.codec));
    std::printf("bits_per_key = %u\n", config.bits_per_key);
    std::printf("block_size = %u\n", config.block_size);
    std::printf("block_cache_bytes = %.2f MiB\n", bytes_to_mib(config.block_cache_bytes));
    std::printf("value_size = %d\n", config.value_size);
    if (config.reader_threads == 0) {
        std::printf("reader_threads = auto (%d from hw_threads)\n", effective_readers);
    } else {
        std::printf("reader_threads = %d\n", config.reader_threads);
    }

    const auto records = make_records(config.keys, config.value_size);
    const auto write_t0 = Clock::now();
    const auto write_result = sst::SSTWriter::write(path, records.views, writer_options(config));
    const auto write_ms = std::chrono::duration<double, std::milli>(Clock::now() - write_t0).count();
    const uint64_t file_bytes = write_result.file_size_bytes == 0 ? regular_file_bytes(path) : write_result.file_size_bytes;

    const auto open_t0 = Clock::now();
    auto reader = sst::SSTReader::open(path, sst::SSTReader::Options{config.block_cache_bytes});
    const auto open_ms = std::chrono::duration<double, std::milli>(Clock::now() - open_t0).count();
    if (!reader) {
        std::fprintf(stderr, "failed to open SST: %s\n", path.string().c_str());
        remove_temp_dir(dir);
        return 8;
    }

    warmup_reader(*reader, records.negative_keys);
    const Result result = run_negative_lookup(*reader, records.negative_keys, config.probes, effective_readers);

    std::printf("write_ms = %.2f\n", write_ms);
    std::printf("open_ms = %.2f\n", open_ms);
    std::printf("sst_bytes = %llu\n", static_cast<unsigned long long>(file_bytes));
    std::printf("write_file_mib_per_sec = %.2f\n\n", mib_per_sec(file_bytes, write_ms));

    std::printf("%-12s %-12s %-8s %-14s %-14s %-8s %-14s\n",
                "keys", "probes", "readers", "negative(ops/s)", "total_ms", "samples", "wrong_present");
    std::printf("%-12d %-12d %-8d %-14.0f %-14.2f %-8u %-14llu\n",
                config.keys,
                config.probes,
                effective_readers,
                result.ops_per_sec,
                result.total_ms,
                result.latency.sample_count,
                static_cast<unsigned long long>(result.wrong_present));
    std::printf("latency(us): p50=%.2f p90=%.2f p99=%.2f p999=%.2f\n",
                result.latency.p50_us,
                result.latency.p90_us,
                result.latency.p99_us,
                result.latency.p999_us);
    std::printf("expected_absent = %llu\n", static_cast<unsigned long long>(result.expected_absent));

    reader.reset();
    remove_temp_dir(dir);
    return result.wrong_present == 0 ? 0 : 9;
}

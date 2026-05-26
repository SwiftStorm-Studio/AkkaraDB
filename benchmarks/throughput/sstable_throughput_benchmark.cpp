/*
 * SSTable throughput benchmark (size sweep).
 *
 * Measures:
 *  - SSTWriter write throughput
 *  - SSTReader point-read throughput
 *  - SSTReader scan throughput
 *
 * Usage:
 *   akkaradb_sstable_throughput_benchmark [ops_per_case]
 *       [--readers=N|--writers=N] [--codec=none|zstd]
 *       [--block-size=N] [--cache-bytes=N|NKiB|NMiB|NGiB]
 *       [--max-case-bytes=N|NKiB|NMiB|NGiB] [--fixed-ops|--same-ops]
 *
 * Default:
 *   ops_per_case = 500000
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
        int key_size;
        int value_size;
    };

    struct LatencyPercentiles {
        double p50_us = 0.0;
        double p90_us = 0.0;
        double p99_us = 0.0;
        double p999_us = 0.0;
        uint32_t sample_count = 0;
    };

    struct BenchConfig {
        int ops_per_case = 500000;
        int reader_threads = 16;
        uint32_t block_size = sst::SST_DEFAULT_BLOCK_SIZE;
        uint64_t block_cache_bytes = 64ULL * 1024ULL * 1024ULL;
        uint64_t max_case_payload_bytes = 512ULL * 1024ULL * 1024ULL;
        bool fixed_ops = false;
        sst::SSTWriter::Codec codec = sst::SSTWriter::Codec::Zstd;
    };

    struct ThroughputResult {
        int ops = 0;
        double write_ops_per_sec = 0.0;
        double open_ms = 0.0;
        double get_ops_per_sec = 0.0;
        double scan_ops_per_sec = 0.0;
        double write_ms = 0.0;
        double get_ms = 0.0;
        double scan_ms = 0.0;
        uint64_t file_bytes = 0;
        uint64_t scan_records = 0;
        LatencyPercentiles get_latency;
        LatencyPercentiles scan_latency;
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

    [[nodiscard]] static double payload_mib_per_sec(size_t bytes_per_op, int ops, double ms) {
        if (ms <= 0.0) {
            return 0.0;
        }
        const double total_mib = static_cast<double>(bytes_per_op) * static_cast<double>(ops) / (1024.0 * 1024.0);
        return total_mib * 1000.0 / ms;
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

    [[nodiscard]] static std::string make_fixed_bytes(int size, uint64_t seed) {
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

    [[nodiscard]] static fs::path make_temp_dir(const std::string& suffix) {
        auto dir = fs::temp_directory_path() / ("akkaradb_sstable_bench_" + suffix);
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

    [[nodiscard]] static int resolve_reader_threads(int requested) {
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

    [[nodiscard]] static Records make_records(CaseSpec spec, int count) {
        Records records;
        records.keys.reserve(static_cast<size_t>(count));
        records.views.reserve(static_cast<size_t>(count));
        records.value = make_fixed_bytes(spec.value_size, 0xA11CEULL);

        for (int i = 0; i < count; ++i) {
            std::string key = make_fixed_bytes(spec.key_size, static_cast<uint64_t>(i) + 1);
            if (spec.key_size >= 10) {
                const auto tail = std::format("{:010d}", i);
                std::memcpy(key.data() + (spec.key_size - 10), tail.data(), 10);
            }
            records.keys.emplace_back(std::move(key));
        }

        std::sort(records.keys.begin(), records.keys.end());

        for (int i = 0; i < count; ++i) {
            const auto& key = records.keys[static_cast<size_t>(i)];
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

    static void run_get_parallel(
        const sst::SSTReader& reader,
        const std::vector<std::string>& keys,
        int reader_threads,
        std::vector<uint32_t>* latency_samples_ns
    ) {
        if (latency_samples_ns) {
            latency_samples_ns->clear();
        }

        if (reader_threads <= 1) {
            std::vector<uint8_t> out;
            if (latency_samples_ns) {
                latency_samples_ns->reserve((keys.size() + kLatencySampleMask) / (kLatencySampleMask + 1));
            }
            for (size_t i = 0; i < keys.size(); ++i) {
                const bool do_sample = ((static_cast<uint32_t>(i) & kLatencySampleMask) == 0);
                const auto t0 = do_sample ? Clock::now() : Clock::time_point{};
                const auto found = reader.get_into(as_u8(keys[i]), out);
                if (!found.has_value() || !*found) {
                    std::fprintf(stderr, "GET miss at i=%zu\n", i);
                    std::exit(3);
                }
                if (do_sample && latency_samples_ns) {
                    const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count();
                    latency_samples_ns->push_back(static_cast<uint32_t>(std::min<int64_t>(dt, INT32_MAX)));
                }
            }
            return;
        }

        std::vector<std::thread> threads;
        threads.reserve(static_cast<size_t>(reader_threads));
        std::vector<std::vector<uint32_t>> local_samples(static_cast<size_t>(reader_threads));
        for (auto& v : local_samples) {
            v.reserve((keys.size() / static_cast<size_t>(reader_threads) + kLatencySampleMask) / (kLatencySampleMask + 1));
        }

        for (int tid = 0; tid < reader_threads; ++tid) {
            threads.emplace_back([&, tid]() {
                std::vector<uint8_t> out;
                auto& samples = local_samples[static_cast<size_t>(tid)];
                for (size_t i = static_cast<size_t>(tid); i < keys.size(); i += static_cast<size_t>(reader_threads)) {
                    const bool do_sample = ((static_cast<uint32_t>(i) & kLatencySampleMask) == 0);
                    const auto t0 = do_sample ? Clock::now() : Clock::time_point{};
                    const auto found = reader.get_into(as_u8(keys[i]), out);
                    if (!found.has_value() || !*found) {
                        std::fprintf(stderr, "GET miss at i=%zu (tid=%d)\n", i, tid);
                        std::exit(3);
                    }
                    if (do_sample) {
                        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count();
                        samples.push_back(static_cast<uint32_t>(std::min<int64_t>(dt, INT32_MAX)));
                    }
                }
            });
        }

        for (auto& th : threads) {
            th.join();
        }

        if (latency_samples_ns) {
            size_t total = 0;
            for (const auto& v : local_samples) {
                total += v.size();
            }
            latency_samples_ns->reserve(total);
            for (auto& v : local_samples) {
                latency_samples_ns->insert(latency_samples_ns->end(), v.begin(), v.end());
            }
        }
    }

    [[nodiscard]] static uint64_t run_scan(
        const sst::SSTReader& reader,
        size_t expected_records,
        std::vector<uint32_t>* latency_samples_ns
    ) {
        if (latency_samples_ns) {
            latency_samples_ns->clear();
            latency_samples_ns->reserve((expected_records + kLatencySampleMask) / (kLatencySampleMask + 1));
        }

        uint64_t scanned = 0;
        auto rows = reader.scan();
        auto it = rows.begin();
        const auto end = rows.end();
        while (it != end) {
            const bool do_sample = ((static_cast<uint32_t>(scanned) & kLatencySampleMask) == 0);
            if (do_sample && latency_samples_ns) {
                const auto t0 = Clock::now();
                size_t window_count = 0;
                while (window_count < kScanSampleWindow && it != end) {
                    if (it->key.empty()) {
                        std::fprintf(stderr, "SCAN returned an empty key at i=%llu\n", static_cast<unsigned long long>(scanned));
                        std::exit(5);
                    }
                    ++it;
                    ++window_count;
                    ++scanned;
                }
                const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count();
                const int64_t per_record_ns = window_count > 0 ? (dt / static_cast<int64_t>(window_count)) : 0;
                latency_samples_ns->push_back(static_cast<uint32_t>(std::min<int64_t>(per_record_ns, INT32_MAX)));
                continue;
            }

            if (it->key.empty()) {
                std::fprintf(stderr, "SCAN returned an empty key at i=%llu\n", static_cast<unsigned long long>(scanned));
                std::exit(5);
            }
            ++it;
            ++scanned;
        }
        if (scanned != expected_records) {
            std::fprintf(
                stderr,
                "SCAN count mismatch: expected=%zu actual=%llu\n",
                expected_records,
                static_cast<unsigned long long>(scanned)
            );
            std::exit(4);
        }
        return scanned;
    }

    [[nodiscard]] static sst::SSTWriter::Options writer_options(const BenchConfig& config) {
        sst::SSTWriter::Options opts;
        opts.block_size = config.block_size;
        opts.codec = config.codec;
        return opts;
    }

    [[nodiscard]] static int resolve_case_ops(CaseSpec spec, const BenchConfig& config) {
        if (config.fixed_ops || config.max_case_payload_bytes == 0) {
            return config.ops_per_case;
        }
        const uint64_t bytes_per_op = static_cast<uint64_t>(spec.key_size) + static_cast<uint64_t>(spec.value_size);
        if (bytes_per_op == 0) {
            return config.ops_per_case;
        }
        const uint64_t capped = std::max<uint64_t>(1000, config.max_case_payload_bytes / bytes_per_op);
        return static_cast<int>(std::min<uint64_t>(static_cast<uint64_t>(config.ops_per_case), capped));
    }

    [[nodiscard]] static ThroughputResult run_case(CaseSpec spec, int ops_per_case, const BenchConfig& config, int reader_threads) {
        const int warmup_ops = std::min(ops_per_case, 50000);
        const auto warmup_dir = make_temp_dir(std::format("warmup_k{}_v{}", spec.key_size, spec.value_size));
        {
            auto warmup_records = make_records(spec, warmup_ops);
            const auto warmup_path = warmup_dir / "warmup.aksst";
            (void)sst::SSTWriter::write(warmup_path, warmup_records.views, writer_options(config));
            auto warmup_reader = sst::SSTReader::open(warmup_path, sst::SSTReader::Options{config.block_cache_bytes});
            if (!warmup_reader) {
                std::fprintf(stderr, "failed to open warmup SST\n");
                std::exit(7);
            }
            run_get_parallel(*warmup_reader, warmup_records.keys, reader_threads, nullptr);
            auto warmup_scan = warmup_reader->scan();
            for (auto&& row : warmup_scan) {
                (void)row;
            }
            warmup_reader.reset();
        }
        remove_temp_dir(warmup_dir);

        auto records = make_records(spec, ops_per_case);
        const auto dir = make_temp_dir(std::format("k{}_v{}_r{}", spec.key_size, spec.value_size, reader_threads));
        const auto path = dir / "bench.aksst";

        const auto write_t0 = Clock::now();
        const auto write_result = sst::SSTWriter::write(path, records.views, writer_options(config));
        const auto write_ms = std::chrono::duration<double, std::milli>(Clock::now() - write_t0).count();

        const auto open_t0 = Clock::now();
        auto reader = sst::SSTReader::open(path, sst::SSTReader::Options{config.block_cache_bytes});
        const auto open_ms = std::chrono::duration<double, std::milli>(Clock::now() - open_t0).count();
        if (!reader) {
            std::fprintf(stderr, "failed to open SST: %s\n", path.string().c_str());
            std::exit(8);
        }

        std::vector<uint32_t> get_latency_ns;
        const auto get_t0 = Clock::now();
        run_get_parallel(*reader, records.keys, reader_threads, &get_latency_ns);
        const auto get_ms = std::chrono::duration<double, std::milli>(Clock::now() - get_t0).count();

        std::vector<uint32_t> scan_latency_ns;
        const auto scan_t0 = Clock::now();
        const uint64_t scanned = run_scan(*reader, records.keys.size(), &scan_latency_ns);
        const auto scan_ms = std::chrono::duration<double, std::milli>(Clock::now() - scan_t0).count();

        reader.reset();
        remove_temp_dir(dir);

        return {
            .ops = ops_per_case,
            .write_ops_per_sec = static_cast<double>(ops_per_case) * 1000.0 / write_ms,
            .open_ms = open_ms,
            .get_ops_per_sec = static_cast<double>(ops_per_case) * 1000.0 / get_ms,
            .scan_ops_per_sec = static_cast<double>(scanned) * 1000.0 / scan_ms,
            .write_ms = write_ms,
            .get_ms = get_ms,
            .scan_ms = scan_ms,
            .file_bytes = write_result.file_size_bytes,
            .scan_records = scanned,
            .get_latency = build_percentiles(get_latency_ns),
            .scan_latency = build_percentiles(scan_latency_ns)
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
    BenchConfig config;

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg.rfind("--readers=", 0) == 0 || arg.rfind("--writers=", 0) == 0) {
            config.reader_threads = std::max(0, std::atoi(arg.substr(10).c_str()));
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
        if (arg.rfind("--max-case-bytes=", 0) == 0) {
            uint64_t parsed = 0;
            if (!parse_u64_with_suffix(arg.substr(17), &parsed)) {
                std::fprintf(stderr, "Invalid --max-case-bytes value: %s\n", arg.substr(17).c_str());
                return 2;
            }
            config.max_case_payload_bytes = parsed;
            continue;
        }
        if (arg == "--fixed-ops" || arg == "--same-ops") {
            config.fixed_ops = true;
            continue;
        }
        config.ops_per_case = std::max(1, std::atoi(arg.c_str()));
    }

    const std::array<CaseSpec, 6> cases{{
        {8, 16},
        {16, 64},
        {16, 256},
        {32, 1024},
        {32, 4096},
        {64, 16384}
    }};

    const int effective_readers = resolve_reader_threads(config.reader_threads);

    std::printf("SSTable throughput benchmark\n");
    std::printf("ops_per_case = %d\n", config.ops_per_case);
    std::printf("codec = %s\n", codec_name(config.codec));
    std::printf("block_size = %u\n", config.block_size);
    std::printf("block_cache_bytes = %.2f MiB\n", bytes_to_mib(config.block_cache_bytes));
    if (config.fixed_ops || config.max_case_payload_bytes == 0) {
        std::printf("max_case_payload_bytes = disabled (--fixed-ops/--same-ops)\n");
    } else {
        std::printf("max_case_payload_bytes = %.2f MiB\n", bytes_to_mib(config.max_case_payload_bytes));
    }
    if (config.reader_threads == 0) {
        std::printf("reader_threads = auto (%d from hw_threads)\n", effective_readers);
    } else {
        std::printf("reader_threads = %d\n", config.reader_threads);
    }
    std::printf("warmup_ops = %d\n\n", std::min(config.ops_per_case, 50000));
    std::printf("%-10s %-12s %-10s %-8s %-14s %-14s %-14s %-12s %-8s %-8s\n",
                "key", "value", "ops", "readers", "write(ops/s)", "get(ops/s)", "scan(ops/s)", "sst_bytes", "get_smp", "scan_smp");
    std::printf("%-10s %-12s %-10s %-8s %-14s %-14s %-14s %-12s %-8s %-8s\n",
                "", "", "", "", "", "", "", "", "P50/P90/P99/P999(us)", "P50/P90/P99/P999(us)");
    std::printf("------------------------------------------------------------------------------------------------\n");

    for (const auto& spec : cases) {
        const int case_ops = resolve_case_ops(spec, config);
        const ThroughputResult result = run_case(spec, case_ops, config, effective_readers);
        std::printf("%-10d %-12d %-10d %-8d %-14.0f %-14.0f %-14.0f %-12llu %-8u %-8u\n",
                    spec.key_size,
                    spec.value_size,
                    result.ops,
                    effective_readers,
                    result.write_ops_per_sec,
                    result.get_ops_per_sec,
                    result.scan_ops_per_sec,
                    static_cast<unsigned long long>(result.file_bytes),
                    result.get_latency.sample_count,
                    result.scan_latency.sample_count);
        std::printf("%-10s %-12s %-10s %-8s %-14s %-14s %-14s %-12s %4.2f/%4.2f/%4.2f/%4.2f %4.2f/%4.2f/%4.2f/%4.2f\n",
                    "", "", "", "", "", "", "", "",
                    result.get_latency.p50_us,
                    result.get_latency.p90_us,
                    result.get_latency.p99_us,
                    result.get_latency.p999_us,
                    result.scan_latency.p50_us,
                    result.scan_latency.p90_us,
                    result.scan_latency.p99_us,
                    result.scan_latency.p999_us);
        std::printf("  timings(ms): write=%8.2f open=%8.2f get=%8.2f scan=%8.2f   file(MiB/s): write=%8.2f get=%8.2f scan=%8.2f   payload(MiB/s): write=%8.2f get=%8.2f scan=%8.2f\n",
                    result.write_ms,
                    result.open_ms,
                    result.get_ms,
                    result.scan_ms,
                    mib_per_sec(result.file_bytes, result.write_ms),
                    mib_per_sec(result.file_bytes, result.get_ms),
                    mib_per_sec(result.file_bytes, result.scan_ms),
                    payload_mib_per_sec(static_cast<size_t>(spec.key_size + spec.value_size), result.ops, result.write_ms),
                    payload_mib_per_sec(static_cast<size_t>(spec.key_size + spec.value_size), result.ops, result.get_ms),
                    payload_mib_per_sec(static_cast<size_t>(spec.key_size + spec.value_size), result.ops, result.scan_ms));
        std::printf("------------------------------------------------------------------------------------------------\n");
        std::fflush(stdout);
    }

    return 0;
}

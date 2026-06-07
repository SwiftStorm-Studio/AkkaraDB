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
 *   akkaradb_wal_throughput_benchmark [ops_per_case] [--writers=N] [--prehash]
 *                                     [--sync=off|async|sync] [--shards=N]
 *                                     [--group-n=N] [--group-micros=N]
 *                                     [--group-bytes=N] [--max-pending-bytes=N]
 *
 * Default:
 *   ops_per_case = 200000
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

    struct ThroughputResult {
        double append_ops_per_sec = 0.0;
        double finish_ops_per_sec = 0.0;
        double recover_ops_per_sec = 0.0;
        double append_ms = 0.0;
        double close_ms = 0.0;
        double total_ms = 0.0;
        double recover_ms = 0.0;
        LatencyPercentiles append_latency;
        uint64_t recovered_entries = 0;
        uint64_t wal_bytes = 0;
    };

    [[nodiscard]] static uint32_t resolve_auto_shard_count(size_t expected_concurrent_writers) {
        const size_t n = expected_concurrent_writers > 0
            ? expected_concurrent_writers
            : std::max<size_t>(1, std::thread::hardware_concurrency());
        return static_cast<uint32_t>(std::clamp<size_t>(n, 1, 16));
    }

    [[nodiscard]] static int resolve_writer_threads(int requested) {
        if (requested > 0) {
            return requested;
        }
        const unsigned hw = std::thread::hardware_concurrency();
        return static_cast<int>(hw == 0 ? 1u : hw);
    }

    [[nodiscard]] static bool is_valid_wal_shard_count(uint32_t n) {
        return n >= 1 && n <= 16;
    }

    [[nodiscard]] static std::span<const uint8_t> as_u8(const std::string& s) {
        return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
    }

    [[nodiscard]] static double quantile_from_sorted(
        const std::vector<uint32_t>& sorted_ns,
        double q
    ) {
        if (sorted_ns.empty()) {
            return 0.0;
        }
        const double q_clamped = std::clamp(q, 0.0, 1.0);
        const size_t idx = static_cast<size_t>(
            q_clamped * static_cast<double>(sorted_ns.size() - 1)
        );
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

    [[nodiscard]] static const char* sync_mode_name(WalSyncMode mode) {
        switch (mode) {
            case WalSyncMode::Off:
                return "off";
            case WalSyncMode::Async:
                return "async";
            case WalSyncMode::Sync:
                return "sync";
        }
        return "unknown";
    }

    [[nodiscard]] static uint64_t parse_byte_count(const std::string& text) {
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

    [[nodiscard]] static double bytes_to_mib(uint64_t bytes) {
        return static_cast<double>(bytes) / (1024.0 * 1024.0);
    }

    [[nodiscard]] static double mib_per_sec(uint64_t bytes, double ms) {
        if (ms <= 0.0) {
            return 0.0;
        }
        return bytes_to_mib(bytes) * 1000.0 / ms;
    }

    [[nodiscard]] static fs::path make_temp_dir(const std::string& suffix) {
        auto dir = fs::temp_directory_path() / ("akkaradb_wal_bench_" + suffix);
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir, ec);
        if (ec) {
            std::fprintf(stderr, "failed to create temp dir: %s\n", dir.string().c_str());
            std::exit(6);
        }
        return dir;
    }

    [[nodiscard]] static uint64_t sum_regular_file_bytes(const fs::path& dir) {
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

    static void remove_temp_dir(const fs::path& dir) {
        std::error_code ec;
        fs::remove_all(dir, ec);
    }

    static void run_append_parallel(
        WalWriter& writer,
        const std::vector<std::string>& keys,
        const std::string& value,
        const std::vector<uint64_t>* key_fp64,
        int writer_threads,
        std::vector<uint32_t>* latency_samples_ns
    ) {
        if (latency_samples_ns) {
            latency_samples_ns->clear();
        }

        if (writer_threads <= 1) {
            if (latency_samples_ns) {
                latency_samples_ns->reserve((keys.size() + kLatencySampleMask) / (kLatencySampleMask + 1));
            }
            for (size_t i = 0; i < keys.size(); ++i) {
                const bool do_sample = ((static_cast<uint32_t>(i) & kLatencySampleMask) == 0);
                const auto t0 = do_sample ? Clock::now() : Clock::time_point{};
                writer.append(
                    as_u8(keys[i]),
                    as_u8(value),
                    static_cast<uint64_t>(i + 1),
                    MemHdr16::FLAG_NORMAL,
                    key_fp64 ? (*key_fp64)[i] : 0ULL
                );
                if (do_sample && latency_samples_ns) {
                    const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count();
                    latency_samples_ns->push_back(static_cast<uint32_t>(std::min<int64_t>(dt, INT32_MAX)));
                }
            }
            return;
        }

        std::vector<std::thread> threads;
        threads.reserve(static_cast<size_t>(writer_threads));
        std::vector<std::vector<uint32_t>> local_samples(static_cast<size_t>(writer_threads));
        for (auto& v : local_samples) {
            v.reserve((keys.size() / static_cast<size_t>(writer_threads) + kLatencySampleMask) / (kLatencySampleMask + 1));
        }

        for (int tid = 0; tid < writer_threads; ++tid) {
            threads.emplace_back([&, tid]() {
                auto& samples = local_samples[static_cast<size_t>(tid)];
                for (size_t i = static_cast<size_t>(tid); i < keys.size(); i += static_cast<size_t>(writer_threads)) {
                    const bool do_sample = ((static_cast<uint32_t>(i) & kLatencySampleMask) == 0);
                    const auto t0 = do_sample ? Clock::now() : Clock::time_point{};
                    writer.append(
                        as_u8(keys[i]),
                        as_u8(value),
                        static_cast<uint64_t>(i + 1),
                        MemHdr16::FLAG_NORMAL,
                        key_fp64 ? (*key_fp64)[i] : 0ULL
                    );
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

    [[nodiscard]] static WalOptions make_options(
        const fs::path& wal_dir,
        WalSyncMode sync_mode,
        uint32_t shard_count,
        uint32_t group_n,
        uint32_t group_micros,
        uint64_t group_bytes,
        uint64_t async_max_pending_bytes
    ) {
        WalOptions opts;
        opts.wal_dir = wal_dir;
        opts.sync_mode = sync_mode;
        opts.shard_count = static_cast<uint16_t>(shard_count);
        opts.group_n = group_n;
        opts.group_micros = group_micros;
        opts.group_bytes = group_bytes;
        opts.async_max_pending_bytes = async_max_pending_bytes;
        return opts;
    }

    [[nodiscard]] static ThroughputResult run_case(
        const CaseSpec spec,
        int ops_per_case,
        bool use_prehash,
        uint32_t shard_count,
        int writer_threads,
        WalSyncMode sync_mode,
        uint32_t group_n,
        uint32_t group_micros,
        uint64_t group_bytes,
        uint64_t async_max_pending_bytes
    ) {
        const int warmup_ops = std::min(ops_per_case, 50000);

        std::vector<std::string> keys;
        keys.reserve(static_cast<size_t>(ops_per_case));
        for (int i = 0; i < ops_per_case; ++i) {
            std::string key = make_fixed_bytes(spec.key_size, static_cast<uint64_t>(i) + 1);
            if (spec.key_size >= 10) {
                const auto tail = std::format("{:010d}", i);
                std::memcpy(key.data() + (spec.key_size - 10), tail.data(), 10);
            }
            keys.emplace_back(std::move(key));
        }

        const std::string value = make_fixed_bytes(spec.value_size, 0xA11CEULL);

        std::vector<uint64_t> key_fp64;
        const std::vector<uint64_t>* fp_ptr = nullptr;

        if (use_prehash) {
            key_fp64.resize(static_cast<size_t>(ops_per_case));
            for (int i = 0; i < ops_per_case; ++i) {
                const auto& key = keys[static_cast<size_t>(i)];
                key_fp64[static_cast<size_t>(i)] = compute_key_fp64(
                    reinterpret_cast<const uint8_t*>(key.data()),
                    key.size()
                );
            }
            fp_ptr = &key_fp64;
        }

        {
            const auto warmup_dir = make_temp_dir(std::format(
                "warmup_k{}_v{}_{}",
                spec.key_size,
                spec.value_size,
                sync_mode_name(sync_mode)
            ));
            auto warmup = WalWriter::create(make_options(
                warmup_dir,
                sync_mode,
                shard_count,
                group_n,
                group_micros,
                group_bytes,
                async_max_pending_bytes
            ));
            const std::vector<std::string> warmup_keys(keys.begin(), keys.begin() + warmup_ops);
            run_append_parallel(*warmup, warmup_keys, value, fp_ptr, writer_threads, nullptr);
            warmup->close();
            remove_temp_dir(warmup_dir);
        }

        const auto dir = make_temp_dir(std::format(
            "k{}_v{}_{}_w{}_s{}",
            spec.key_size,
            spec.value_size,
            sync_mode_name(sync_mode),
            writer_threads,
            shard_count
        ));

        auto writer = WalWriter::create(make_options(
            dir,
            sync_mode,
            shard_count,
            group_n,
            group_micros,
            group_bytes,
            async_max_pending_bytes
        ));
        std::vector<uint32_t> append_latency_ns;

        const auto write_t0 = Clock::now();
        run_append_parallel(*writer, keys, value, fp_ptr, writer_threads, &append_latency_ns);
        const auto append_ms = std::chrono::duration<double, std::milli>(Clock::now() - write_t0).count();
        const auto close_t0 = Clock::now();
        writer->close();
        const auto close_ms = std::chrono::duration<double, std::milli>(Clock::now() - close_t0).count();
        const auto finish_ms = std::chrono::duration<double, std::milli>(Clock::now() - write_t0).count();

        const uint64_t wal_bytes = sum_regular_file_bytes(dir);
        uint64_t recovered = 0;
        const auto recover_t0 = Clock::now();
        const auto recovery = WalRecovery::recover(WalRecoveryOptions{.wal_dir = dir}, [&](const WalRecoveredEntry&) {
            ++recovered;
        });
        const auto recover_ms = std::chrono::duration<double, std::milli>(Clock::now() - recover_t0).count();

        if (recovered != static_cast<uint64_t>(ops_per_case) || recovery.entries_replayed != static_cast<uint64_t>(ops_per_case)) {
            std::fprintf(
                stderr,
                "RECOVER count mismatch: expected=%d callback=%llu result=%llu\n",
                ops_per_case,
                static_cast<unsigned long long>(recovered),
                static_cast<unsigned long long>(recovery.entries_replayed)
            );
            std::exit(7);
        }

        remove_temp_dir(dir);

        return {
            .append_ops_per_sec = static_cast<double>(ops_per_case) * 1000.0 / append_ms,
            .finish_ops_per_sec = static_cast<double>(ops_per_case) * 1000.0 / finish_ms,
            .recover_ops_per_sec = static_cast<double>(ops_per_case) * 1000.0 / recover_ms,
            .append_ms = append_ms,
            .close_ms = close_ms,
            .total_ms = finish_ms,
            .recover_ms = recover_ms,
            .append_latency = build_percentiles(append_latency_ns),
            .recovered_entries = recovered,
            .wal_bytes = wal_bytes
        };
    }
} // namespace

int main(int argc, char** argv) {
    akkara::test::install_msvc_test_error_handlers();

    int ops_per_case = 200000;
    int writer_threads = 16;
    bool use_prehash = false;
    WalSyncMode sync_mode = WalSyncMode::Async;
    uint32_t requested_shards = 0;
    uint32_t group_n = 2048;
    uint32_t group_micros = 2000;
    uint64_t group_bytes = 4ULL * 1024ULL * 1024ULL;
    uint64_t async_max_pending_bytes = 64ULL * 1024ULL * 1024ULL;

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--prehash") {
            use_prehash = true;
            continue;
        }
        if (arg.rfind("--writers=", 0) == 0) {
            writer_threads = std::max(0, std::atoi(arg.substr(10).c_str()));
            continue;
        }
        if (arg.rfind("--sync=", 0) == 0) {
            const std::string mode = arg.substr(7);
            if (mode == "off") {
                sync_mode = WalSyncMode::Off;
                continue;
            }
            if (mode == "async") {
                sync_mode = WalSyncMode::Async;
                continue;
            }
            if (mode == "sync") {
                sync_mode = WalSyncMode::Sync;
                continue;
            }
            std::fprintf(stderr, "Unknown sync mode: %s (use off|async|sync)\n", mode.c_str());
            return 2;
        }
        if (arg.rfind("--shards=", 0) == 0) {
            requested_shards = static_cast<uint32_t>(std::max(0, std::atoi(arg.substr(9).c_str())));
            if (requested_shards != 0 && !is_valid_wal_shard_count(requested_shards)) {
                std::fprintf(stderr, "Invalid WAL shard count: %u (use 1..16, or 0 for auto)\n", requested_shards);
                return 2;
            }
            continue;
        }
        if (arg.rfind("--group-n=", 0) == 0) {
            group_n = static_cast<uint32_t>(std::max(1, std::atoi(arg.substr(10).c_str())));
            continue;
        }
        if (arg.rfind("--group-micros=", 0) == 0) {
            group_micros = static_cast<uint32_t>(std::max(1, std::atoi(arg.substr(15).c_str())));
            continue;
        }
        if (arg.rfind("--group-bytes=", 0) == 0) {
            group_bytes = std::max<uint64_t>(1, parse_byte_count(arg.substr(14)));
            continue;
        }
        if (arg.rfind("--max-pending-bytes=", 0) == 0) {
            async_max_pending_bytes = std::max<uint64_t>(1, parse_byte_count(arg.substr(20)));
            continue;
        }
        ops_per_case = std::max(1, std::atoi(arg.c_str()));
    }

    const std::array<CaseSpec, 6> cases{{
        {8, 16},
        {16, 64},
        {16, 256},
        {32, 1024},
        {32, 4096},
        {64, 16384}
    }};

    const int effective_writers = resolve_writer_threads(writer_threads);
    const uint32_t shard_count = requested_shards == 0
        ? resolve_auto_shard_count(static_cast<size_t>(effective_writers))
        : requested_shards;

    std::printf("Sharded WAL throughput benchmark\n");
    std::printf("ops_per_case = %d\n", ops_per_case);
    std::printf("sync_mode = %s\n", sync_mode_name(sync_mode));
    if (writer_threads == 0) {
        std::printf("writer_threads = auto (%d from hw_threads)\n", effective_writers);
    } else {
        std::printf("writer_threads = %d\n", writer_threads);
    }
    if (requested_shards == 0) {
        std::printf("resolved_shards = %u (auto, one shard per writer up to WAL limit 16)\n", shard_count);
    } else {
        std::printf("resolved_shards = %u (requested)\n", shard_count);
    }
    std::printf("group_n = %u\n", group_n);
    std::printf("group_micros = %u\n", group_micros);
    std::printf("group_bytes = %.2f MiB\n", bytes_to_mib(group_bytes));
    std::printf("max_pending_bytes = %.2f MiB\n\n", bytes_to_mib(async_max_pending_bytes));
    std::printf("warmup_ops   = %d\n\n", std::min(ops_per_case, 50000));
    std::printf("prehash_mode = %s\n\n", use_prehash ? "ON (fp64 precomputed)" : "OFF (hash inside append)");
    std::printf("%-10s %-12s %-8s %-8s %-14s %-14s %-14s %-12s %-8s\n",
                "key", "value", "shards", "writers", "append(ops/s)", "finish(ops/s)", "recover(ops/s)", "wal_bytes", "app_smp");
    std::printf("%-10s %-12s %-8s %-8s %-14s %-14s %-14s %-12s %-8s\n",
                "", "", "", "", "", "", "", "", "P50/P90/P99/P999(us)");
    std::printf("------------------------------------------------------------------------------------------------\n");

    for (const auto& spec : cases) {
        const ThroughputResult result = run_case(
            spec,
            ops_per_case,
            use_prehash,
            shard_count,
            effective_writers,
            sync_mode,
            group_n,
            group_micros,
            group_bytes,
            async_max_pending_bytes
        );
        std::printf("%-10d %-12d %-8u %-8d %-14.0f %-14.0f %-14.0f %-12llu %-8u\n",
                    spec.key_size,
                    spec.value_size,
                    shard_count,
                    effective_writers,
                    result.append_ops_per_sec,
                    result.finish_ops_per_sec,
                    result.recover_ops_per_sec,
                    static_cast<unsigned long long>(result.wal_bytes),
                    result.append_latency.sample_count);
        std::printf("%-10s %-12s %-8s %-8s %-14s %-14s %-14s %-12s %4.2f/%4.2f/%4.2f/%4.2f\n",
                    "", "", "", "", "", "", "", "",
                    result.append_latency.p50_us,
                    result.append_latency.p90_us,
                    result.append_latency.p99_us,
                    result.append_latency.p999_us);
        std::printf("  timings(ms): append=%8.2f close=%8.2f total=%8.2f recover=%8.2f   throughput(MiB/s): append=%8.2f finish=%8.2f recover=%8.2f\n",
                    result.append_ms,
                    result.close_ms,
                    result.total_ms,
                    result.recover_ms,
                    mib_per_sec(result.wal_bytes, result.append_ms),
                    mib_per_sec(result.wal_bytes, result.total_ms),
                    mib_per_sec(result.wal_bytes, result.recover_ms));
        std::printf("------------------------------------------------------------------------------------------------\n");
        std::fflush(stdout);
    }

    return 0;
}

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

// benchmarks/throughput/memtable_throughput_benchmark.cpp
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
 *   akkaradb_memtable_throughput_benchmark [ops_per_case]
 *       [--writers=N] [--shards=N] [--auto-cap=N]
 *       [--threshold-bytes=N|NK|NM|NG|NKiB|NMiB|NGiB]
 *       [--flush-after-scan] [--prehash] [--backend=skiplist|bptree|art]
 *
 * Default:
 *   ops_per_case = 200000
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
using namespace akkaradb::core;

namespace {
    constexpr uint32_t kLatencySampleMask = 0x3F; // sample 1 / 64 ops to reduce benchmark perturbation
    constexpr size_t kScanSampleWindow = 32; // amortize clock resolution/overhead for iterator next()
    constexpr uint64_t kAutoFlushDisabledThreshold = (1ULL << 62);

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

    enum class BackendKind {
        SkipList,
        BPTree,
        ART
    };

    struct ThroughputResult {
        double put_ops_per_sec;
        double get_ops_per_sec;
        double scan_ops_per_sec;
        double put_ms;
        double get_ms;
        double scan_ms;
        double flush_ms;
        uint64_t approx_bytes;
        uint64_t flushes_completed;
        uint64_t flush_records_seen;
        LatencyPercentiles put_latency;
        LatencyPercentiles get_latency;
        LatencyPercentiles scan_latency;
    };

    struct BenchConfig {
        int ops_per_case = 500000;
        int writer_threads = 16;
        uint32_t requested_shards = 0;
        uint32_t auto_shard_count_cap = 128;
        uint64_t threshold_bytes_per_shard = kAutoFlushDisabledThreshold;
        bool flush_after_scan = false;
        bool use_prehash = false;
        BackendKind backend = BackendKind::ART;
    };

    [[nodiscard]] static uint32_t next_pow2_clamped(uint64_t n, uint32_t min_value, uint32_t max_value) {
        uint32_t p = 1;
        while (p < n && p < max_value) {
            p <<= 1;
        }
        if (p < min_value) {
            p = min_value;
        }
        if (p > max_value) {
            p = max_value;
        }
        return p;
    }

    [[nodiscard]] static uint32_t resolve_shard_count(
        size_t requested,
        size_t expected_concurrent_writers,
        size_t auto_cap
    ) {
        if (requested == 1) {
            return 1;
        }
        if (requested > 1) {
            return next_pow2_clamped(static_cast<uint64_t>(requested), 2, 256);
        }

        const uint32_t effective_cap = next_pow2_clamped(static_cast<uint64_t>(auto_cap == 0 ? 128 : auto_cap), 2, 256);
        const size_t n = expected_concurrent_writers > 0
            ? expected_concurrent_writers
            : std::max<size_t>(2, std::thread::hardware_concurrency());
        const uint64_t target = n <= 1 ? 1ULL : static_cast<uint64_t>(n) * 4ULL;
        return next_pow2_clamped(target, 2, effective_cap);
    }

    [[nodiscard]] static int resolve_writer_threads(int requested) {
        if (requested > 0) {
            return requested;
        }
        const unsigned hw = std::thread::hardware_concurrency();
        return static_cast<int>(hw == 0 ? 1u : hw);
    }

    static std::span<const uint8_t> as_u8(const std::string& s) {
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

    [[nodiscard]] static std::string format_bytes(uint64_t bytes) {
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

    [[nodiscard]] static double payload_mib_per_sec(size_t bytes_per_op, int ops, double ms) {
        if (ms <= 0.0) {
            return 0.0;
        }
        const double total_mib = static_cast<double>(bytes_per_op) * static_cast<double>(ops) / (1024.0 * 1024.0);
        return total_mib * 1000.0 / ms;
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

    static std::string make_fixed_bytes(int size, uint64_t seed) {
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

    static memtable::MemTable::Options make_options(
        const BenchConfig& config,
        int effective_writers,
        std::atomic<uint64_t>* flush_records_seen
    ) {
        memtable::MemTable::Options opts;
        opts.shard_count = config.requested_shards;
        opts.expected_concurrent_writers = static_cast<size_t>(effective_writers);
        opts.auto_shard_count_cap = config.auto_shard_count_cap;
        opts.threshold_bytes_per_shard = config.threshold_bytes_per_shard;
        if (config.flush_after_scan) {
            opts.on_flush = [flush_records_seen](std::span<const memtable::MemTable::RecordView> records) {
                if (flush_records_seen) {
                    flush_records_seen->fetch_add(static_cast<uint64_t>(records.size()), std::memory_order_relaxed);
                }
            };
        }
        if (config.backend == BackendKind::BPTree) {
            opts.backend_factory = []() {
                return std::make_unique<BPTreeMemTable>();
            };
        } else if (config.backend == BackendKind::ART) {
            opts.backend_factory = []() {
                return std::make_unique<ARTMemTable>();
            };
        } else {
            opts.backend_factory = []() {
                return std::make_unique<SkipListMemTable>();
            };
        }
        return opts;
    }

    static void run_put_parallel(
        memtable::MemTable& memtable,
        const std::vector<std::string>& keys,
        const std::string& value,
        const std::vector<uint64_t>* key_fp64,
        const std::vector<uint64_t>* key_mk,
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
                const uint64_t seq = memtable.next_seq();
                memtable.put(
                    as_u8(keys[i]),
                    as_u8(value),
                    seq,
                    0,
                    key_fp64 ? (*key_fp64)[i] : 0ULL,
                    key_mk ? (*key_mk)[i] : 0ULL
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
                const size_t stride = static_cast<size_t>(writer_threads);
                const size_t first = static_cast<size_t>(tid);
                const size_t op_count = first < keys.size() ? ((keys.size() - first + stride - 1) / stride) : 0;
                const uint64_t seq_base = memtable.reserve_seq(static_cast<uint64_t>(op_count));
                uint64_t seq_offset = 0;
                for (size_t i = first; i < keys.size(); i += stride) {
                    const bool do_sample = ((static_cast<uint32_t>(i) & kLatencySampleMask) == 0);
                    const auto t0 = do_sample ? Clock::now() : Clock::time_point{};
                    const uint64_t seq = seq_base + seq_offset++;
                    memtable.put(
                        as_u8(keys[i]),
                        as_u8(value),
                        seq,
                        0,
                        key_fp64 ? (*key_fp64)[i] : 0ULL,
                        key_mk ? (*key_mk)[i] : 0ULL
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

    static void run_get_parallel(
        memtable::MemTable& memtable,
        const std::vector<std::string>& keys,
        const std::vector<uint64_t>* key_fp64,
        uint64_t snapshot,
        int reader_threads,
        std::vector<uint32_t>* latency_samples_ns
    ) {
        if (latency_samples_ns) {
            latency_samples_ns->clear();
        }

        if (reader_threads <= 1) {
            RecordView out;
            if (latency_samples_ns) {
                latency_samples_ns->reserve((keys.size() + kLatencySampleMask) / (kLatencySampleMask + 1));
            }
            for (size_t i = 0; i < keys.size(); ++i) {
                const bool do_sample = ((static_cast<uint32_t>(i) & kLatencySampleMask) == 0);
                const auto t0 = do_sample ? Clock::now() : Clock::time_point{};
                if (!memtable.get(as_u8(keys[i]), snapshot, &out, key_fp64 ? (*key_fp64)[i] : 0ULL)) {
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
                RecordView out;
                auto& samples = local_samples[static_cast<size_t>(tid)];
                for (size_t i = static_cast<size_t>(tid); i < keys.size(); i += static_cast<size_t>(reader_threads)) {
                    const bool do_sample = ((static_cast<uint32_t>(i) & kLatencySampleMask) == 0);
                    const auto t0 = do_sample ? Clock::now() : Clock::time_point{};
                    if (!memtable.get(as_u8(keys[i]), snapshot, &out, key_fp64 ? (*key_fp64)[i] : 0ULL)) {
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

    static double run_scan_single(
        memtable::MemTable& memtable,
        uint64_t snapshot,
        size_t expected_records,
        std::vector<uint32_t>* latency_samples_ns
    ) {
        if (latency_samples_ns) {
            latency_samples_ns->clear();
            latency_samples_ns->reserve((expected_records + kLatencySampleMask) / (kLatencySampleMask + 1));
        }

        memtable::MemTable::KeyRange full_range{};
        size_t scanned = 0;
        const auto scan_t0 = Clock::now();
        auto it = memtable.iterator(full_range, snapshot);
        while (it.has_next()) {
            const bool do_sample = ((static_cast<uint32_t>(scanned) & kLatencySampleMask) == 0);
            if (do_sample && latency_samples_ns) {
                const auto t0 = Clock::now();
                size_t window_count = 0;
                while (window_count < kScanSampleWindow && it.has_next()) {
                    const auto rec = it.next();
                    if (!rec.has_value()) {
                        std::fprintf(stderr, "SCAN iterator returned nullopt before end (scanned=%zu)\n", scanned);
                        std::exit(4);
                    }
                    ++window_count;
                    ++scanned;
                }
                const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count();
                const int64_t per_record_ns = window_count > 0 ? (dt / static_cast<int64_t>(window_count)) : 0;
                latency_samples_ns->push_back(static_cast<uint32_t>(std::min<int64_t>(per_record_ns, INT32_MAX)));
                continue;
            }

            const auto rec = it.next();
            if (!rec.has_value()) {
                std::fprintf(stderr, "SCAN iterator returned nullopt before end (scanned=%zu)\n", scanned);
                std::exit(4);
            }
            ++scanned;
        }
        const auto scan_ms = std::chrono::duration<double, std::milli>(Clock::now() - scan_t0).count();

        if (scanned != expected_records) {
            std::fprintf(stderr, "SCAN count mismatch: expected=%zu actual=%zu\n", expected_records, scanned);
            std::exit(5);
        }
        if (scan_ms <= 0.0) {
            return 0.0;
        }
        return static_cast<double>(scanned) * 1000.0 / scan_ms;
    }

    static ThroughputResult run_case(
        const CaseSpec spec,
        int ops_per_case,
        int writer_threads,
        const BenchConfig& config
    ) {
        const int warmup_ops = std::min(ops_per_case, 100000);

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
        std::vector<uint64_t> key_mk;
        const std::vector<uint64_t>* fp_ptr = nullptr;
        const std::vector<uint64_t>* mk_ptr = nullptr;

        if (config.use_prehash) {
            key_fp64.resize(static_cast<size_t>(ops_per_case));
            key_mk.resize(static_cast<size_t>(ops_per_case));
            for (int i = 0; i < ops_per_case; ++i) {
                const auto& key = keys[static_cast<size_t>(i)];
                const uint8_t* ptr = reinterpret_cast<const uint8_t*>(key.data());
                const size_t len = key.size();
                key_fp64[static_cast<size_t>(i)] = compute_key_fp64(ptr, len);
                key_mk[static_cast<size_t>(i)] = build_mini_key(ptr, len);
            }
            fp_ptr = &key_fp64;
            mk_ptr = &key_mk;
        }

        {
            std::atomic<uint64_t> warmup_flush_records_seen{0};
            auto warmup = memtable::MemTable::create(make_options(config, writer_threads, &warmup_flush_records_seen));
            const std::vector<std::string> warmup_keys(keys.begin(), keys.begin() + warmup_ops);
            run_put_parallel(*warmup, warmup_keys, value, fp_ptr, mk_ptr, writer_threads, nullptr);

            const uint64_t snapshot = warmup->last_seq();
            RecordView out;
            for (int i = 0; i < warmup_ops; ++i) {
                if (!warmup->get(as_u8(keys[static_cast<size_t>(i)]), snapshot, &out)) {
                    std::fprintf(stderr, "WARMUP GET miss at i=%d\n", i);
                    std::exit(1);
                }
            }
        }

        std::atomic<uint64_t> flush_records_seen{0};
        auto memtable = memtable::MemTable::create(make_options(config, writer_threads, &flush_records_seen));
        std::vector<uint32_t> put_latency_ns;
        std::vector<uint32_t> get_latency_ns;
        std::vector<uint32_t> scan_latency_ns;

        const auto put_t0 = Clock::now();
        run_put_parallel(*memtable, keys, value, fp_ptr, mk_ptr, writer_threads, &put_latency_ns);
        const auto put_ms = std::chrono::duration<double, std::milli>(Clock::now() - put_t0).count();

        const uint64_t snapshot = memtable->last_seq();
        const auto get_t0 = Clock::now();
        run_get_parallel(*memtable, keys, fp_ptr, snapshot, writer_threads, &get_latency_ns);
        const auto get_ms = std::chrono::duration<double, std::milli>(Clock::now() - get_t0).count();
        const auto scan_t0 = Clock::now();
        const double scan_ops_per_sec = run_scan_single(*memtable, snapshot, keys.size(), &scan_latency_ns);
        const auto scan_ms = std::chrono::duration<double, std::milli>(Clock::now() - scan_t0).count();

        double flush_ms = 0.0;
        if (config.flush_after_scan) {
            const auto flush_t0 = Clock::now();
            memtable->force_flush();
            flush_ms = std::chrono::duration<double, std::milli>(Clock::now() - flush_t0).count();
        }
        const auto snapshot_after = memtable->snapshot();

        return {
            .put_ops_per_sec = static_cast<double>(ops_per_case) * 1000.0 / put_ms,
            .get_ops_per_sec = static_cast<double>(ops_per_case) * 1000.0 / get_ms,
            .scan_ops_per_sec = scan_ops_per_sec,
            .put_ms = put_ms,
            .get_ms = get_ms,
            .scan_ms = scan_ms,
            .flush_ms = flush_ms,
            .approx_bytes = snapshot_after.approx_bytes,
            .flushes_completed = snapshot_after.flushes_completed,
            .flush_records_seen = flush_records_seen.load(std::memory_order_relaxed),
            .put_latency = build_percentiles(put_latency_ns),
            .get_latency = build_percentiles(get_latency_ns),
            .scan_latency = build_percentiles(scan_latency_ns)
        };
    }
} // namespace

int main(int argc, char** argv) {
    akkara::test::install_msvc_test_error_handlers();

    BenchConfig config;

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--prehash") {
            config.use_prehash = true;
            continue;
        }
        if (arg == "--flush-after-scan" || arg == "--flush-callback") {
            config.flush_after_scan = true;
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
            config.writer_threads = std::max(0, std::atoi(arg.substr(10).c_str()));
            continue;
        }
        if (arg.rfind("--shards=", 0) == 0) {
            config.requested_shards = static_cast<uint32_t>(std::max(0, std::atoi(arg.substr(9).c_str())));
            continue;
        }
        if (arg.rfind("--auto-cap=", 0) == 0) {
            config.auto_shard_count_cap = static_cast<uint32_t>(std::max(0, std::atoi(arg.substr(11).c_str())));
            continue;
        }
        if (arg.rfind("--threshold-bytes=", 0) == 0) {
            uint64_t parsed = 0;
            if (!parse_u64_with_suffix(arg.substr(18), &parsed)) {
                std::fprintf(stderr, "Invalid --threshold-bytes value: %s\n", arg.substr(18).c_str());
                return 2;
            }
            config.threshold_bytes_per_shard = parsed;
            continue;
        }
        config.ops_per_case = std::max(1000, std::atoi(arg.c_str()));
    }

    const std::array<CaseSpec, 6> cases{{
        {8, 16},
        {16, 64},
        {16, 256},
        {32, 1024},
        {32, 4096},
        {64, 16384}
    }};

    const int effective_writers = resolve_writer_threads(config.writer_threads);
    if (config.flush_after_scan && config.threshold_bytes_per_shard != kAutoFlushDisabledThreshold) {
        std::fprintf(
            stderr,
            "--flush-after-scan expects auto flush to stay disabled; omit --threshold-bytes for this benchmark mode.\n"
        );
        return 2;
    }
    const uint32_t shard_count = resolve_shard_count(
        config.requested_shards,
        static_cast<size_t>(effective_writers),
        config.auto_shard_count_cap
    );

    std::printf("Sharded MemTable throughput benchmark\n");
    std::printf("ops_per_case = %d\n", config.ops_per_case);
    const char* backend_name = "skiplist";
    if (config.backend == BackendKind::BPTree) {
        backend_name = "bptree";
    } else if (config.backend == BackendKind::ART) {
        backend_name = "art";
    }
    std::printf("backend = %s\n", backend_name);
    if (config.writer_threads == 0) {
        std::printf("writer_threads = auto (%d from hw_threads)\n", effective_writers);
    } else {
        std::printf("writer_threads = %d\n", config.writer_threads);
    }
    if (config.requested_shards == 0) {
        std::printf("resolved_shards = %u (auto, writers*4 locality heuristic, cap %u)\n", shard_count, config.auto_shard_count_cap);
    } else if (config.requested_shards == shard_count) {
        std::printf("resolved_shards = %u (explicit)\n", shard_count);
    } else {
        std::printf("resolved_shards = %u (explicit %u rounded to power-of-two)\n", shard_count, config.requested_shards);
    }
    if (config.threshold_bytes_per_shard == kAutoFlushDisabledThreshold) {
        std::printf("threshold_bytes_per_shard = disabled\n");
    } else if (config.flush_after_scan) {
        std::printf("threshold_bytes_per_shard = %s\n", format_bytes(config.threshold_bytes_per_shard).c_str());
    } else {
        std::printf(
            "threshold_bytes_per_shard = %s (no flush worker)\n",
            format_bytes(config.threshold_bytes_per_shard).c_str()
        );
    }
    std::printf("flush_after_scan = %s\n\n", config.flush_after_scan ? "ON" : "OFF");
    std::printf("warmup_ops   = %d\n\n", std::min(config.ops_per_case, 50000));
    std::printf("prehash_mode = %s\n\n", config.use_prehash ? "ON (fp64/mk precomputed)" : "OFF (hash inside put)");
    std::printf("%-10s %-12s %-8s %-8s %-14s %-14s %-14s %-14s %-8s %-8s %-8s\n",
                "key", "value", "shards", "writers", "put(ops/s)", "get(ops/s)", "scan(ops/s)", "mem_bytes", "put_smp", "get_smp", "scan_smp");
    std::printf("%-10s %-12s %-8s %-8s %-14s %-14s %-14s %-14s %-8s %-8s %-8s\n",
                "", "", "", "", "", "", "", "", "P50/P90/P99/P999(us)", "P50/P90/P99/P999(us)", "P50/P90/P99/P999(us)");
    std::printf("------------------------------------------------------------------------------------------------\n");

    for (const auto& spec : cases) {
        const ThroughputResult result = run_case(spec, config.ops_per_case, effective_writers, config);
        std::printf("%-10d %-12d %-8u %-8d %-14.0f %-14.0f %-14.0f %-14llu %-8u %-8u %-8u\n",
                    spec.key_size,
                    spec.value_size,
                    shard_count,
                    effective_writers,
                    result.put_ops_per_sec,
                    result.get_ops_per_sec,
                    result.scan_ops_per_sec,
                    static_cast<unsigned long long>(result.approx_bytes),
                    result.put_latency.sample_count,
                    result.get_latency.sample_count,
                    result.scan_latency.sample_count);
        std::printf("%-10s %-12s %-8s %-8s %-14s %-14s %-14s %-14s %4.2f/%4.2f/%4.2f/%4.2f %4.2f/%4.2f/%4.2f/%4.2f %4.2f/%4.2f/%4.2f/%4.2f\n",
                    "", "", "", "", "", "", "", "",
                    result.put_latency.p50_us,
                    result.put_latency.p90_us,
                    result.put_latency.p99_us,
                    result.put_latency.p999_us,
                    result.get_latency.p50_us,
                    result.get_latency.p90_us,
                    result.get_latency.p99_us,
                    result.get_latency.p999_us,
                    result.scan_latency.p50_us,
                    result.scan_latency.p90_us,
                    result.scan_latency.p99_us,
                    result.scan_latency.p999_us);
        std::printf("  timings(ms): put=%8.2f get=%8.2f scan=%8.2f flush=%8.2f   payload(MiB/s): put=%8.2f get=%8.2f scan=%8.2f   flushes=%llu flush_records=%llu\n",
                    result.put_ms,
                    result.get_ms,
                    result.scan_ms,
                    result.flush_ms,
                    payload_mib_per_sec(static_cast<size_t>(spec.key_size + spec.value_size), config.ops_per_case, result.put_ms),
                    payload_mib_per_sec(static_cast<size_t>(spec.key_size + spec.value_size), config.ops_per_case, result.get_ms),
                    payload_mib_per_sec(static_cast<size_t>(spec.key_size + spec.value_size), config.ops_per_case, result.scan_ms),
                    static_cast<unsigned long long>(result.flushes_completed),
                    static_cast<unsigned long long>(result.flush_records_seen));
        std::printf("------------------------------------------------------------------------------------------------\n");
        std::fflush(stdout);
    }

    return 0;
}

/*
 * Copyright (C) 2026 Swift Storm Studio
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
#include "TestErrorHandlers.hpp"
#include "LatencyMeasurement.hpp"
#include "akk/engine/AkkEngine.hpp"
#include "akk/engine/memtable/ARTMemTable.hpp"
#include "akk/engine/memtable/BPTreeMemTable.hpp"
#include "akk/engine/memtable/SkipListMemTable.hpp"

#include <array>
#include <bit>
#include <charconv>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <latch>
#include <locale>
#include <numeric>
#include <random>
#include <string>
#include <thread>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#endif

namespace {
    using namespace akkaradb::engine;
    using Clock = std::chrono::steady_clock;
    using Key = std::array<uint8_t, 16>;
    namespace fs = std::filesystem;

    struct Config {
        int keys = 1'000'000, iterations = 1'000'000, warmup = 100, rounds = 3;
        int latencySamples = 100'000, shards = 1, valueBytes = 128;
        std::vector<int> threads{1, 2, 4, 8, 12, 16}, cpus;
        std::string backend = "bptree";
        std::string keyPattern = "prefix";
        fs::path output = "benchmark-results", profileControl;
    };

    int integer(std::string_view text, int minimum = 1) {
        int value = 0;
        const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
        if (error != std::errc{} || end != text.data() + text.size() || value < minimum) {
            throw std::invalid_argument("invalid integer: " + std::string{text});
        }
        return value;
    }

    std::vector<int> numbers(std::string_view text, int minimum) {
        std::vector<int> result;
        do {
            const auto comma = text.find(',');
            const auto value = integer(text.substr(0, comma), minimum);
            if (std::ranges::find(result, value) != result.end()) { throw std::invalid_argument("duplicate list value"); }
            result.push_back(value);
            if (comma == std::string_view::npos) { return result; }
            text.remove_prefix(comma + 1);
        } while (true);
    }

    auto factory(std::string_view name) -> memtable::MemTable::MemTableFactory {
        if (name == "bptree") { return [] { return std::make_unique<memtable::BPTreeMemTable>(); }; }
        if (name == "art") { return [] { return std::make_unique<memtable::ARTMemTable>(); }; }
        if (name == "skiplist") { return [] { return std::make_unique<memtable::SkipListMemTable>(); }; }
        throw std::invalid_argument("backend must be bptree, art, or skiplist");
    }

    Config parse(int argc, char** argv) {
        Config c;
        for (int i = 1; i < argc; ++i) {
            const std::string_view name = argv[i];
            if (++i == argc) { throw std::invalid_argument("missing option value"); }
            const std::string_view value = argv[i];
            if (name == "--keys") { c.keys = integer(value); }
            else if (name == "--iterations") { c.iterations = integer(value); }
            else if (name == "--warmup") { c.warmup = integer(value, 0); }
            else if (name == "--rounds") { c.rounds = integer(value); }
            else if (name == "--latency-samples") { c.latencySamples = integer(value, 0); }
            else if (name == "--shards") { c.shards = integer(value); }
            else if (name == "--value-bytes") { c.valueBytes = integer(value, 8); }
            else if (name == "--threads") { c.threads = numbers(value, 1); }
            else if (name == "--cpus") { c.cpus = numbers(value, 0); }
            else if (name == "--backend") { c.backend = value; }
            else if (name == "--key-pattern") { c.keyPattern = value; }
            else if (name == "--csv-dir") { c.output = value; }
            else if (name == "--profile-control") { c.profileControl = value; }
            else { throw std::invalid_argument("unknown option: " + std::string{name}); }
        }
        (void)factory(c.backend);
        if (c.keyPattern != "prefix" && c.keyPattern != "mixed") { throw std::invalid_argument("--key-pattern must be prefix or mixed"); }
        if (!std::has_single_bit(static_cast<unsigned>(c.shards))) { throw std::invalid_argument("--shards must be a power of two"); }
        for (int count : c.threads) {
            if (count > c.iterations || (c.latencySamples && count > c.latencySamples)) {
                throw std::invalid_argument("each worker needs at least one operation in each enabled pass");
            }
            if (!c.cpus.empty() && static_cast<size_t>(count) > c.cpus.size()) {
                throw std::invalid_argument("--cpus needs a different CPU for every worker");
            }
        }
        if (c.output.empty()) { throw std::invalid_argument("empty output directory"); }
        if (!c.profileControl.empty() && (c.threads != std::vector<int>{1} || c.rounds != 1 || c.latencySamples != 0)) {
            throw std::invalid_argument("profiling requires --threads 1 --rounds 1 --latency-samples 0");
        }
        return c;
    }

    uint64_t mix(uint64_t value) {
        value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
        value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
        return value ^ (value >> 31);
    }

    Key keyFor(uint64_t i, std::string_view pattern) {
        Key key{};
        if (pattern == "prefix") {
            key[0] = 'k';
            for (size_t j = 0; j < 8; ++j) { key[8 + j] = static_cast<uint8_t>(i >> (56 - j * 8)); }
        } else {
            // Each half is a bijection of the row number, so the 16-byte keys remain distinct.
            const uint64_t high = mix(i ^ 0xA44A5EEDULL), low = mix(i + 0x9e3779b97f4a7c15ULL);
            for (size_t j = 0; j < 8; ++j) {
                key[j] = static_cast<uint8_t>(high >> (56 - j * 8));
                key[8 + j] = static_cast<uint8_t>(low >> (56 - j * 8));
            }
        }
        return key;
    }

    void pin(const Config& c, size_t worker) {
        if (c.cpus.empty()) { return; }
#ifdef _WIN32
        const auto cpu = static_cast<unsigned>(c.cpus.at(worker));
        if (cpu >= sizeof(DWORD_PTR) * 8 || SetThreadAffinityMask(GetCurrentThread(), DWORD_PTR{1} << cpu) == 0) {
            throw std::runtime_error("could not pin worker to CPU " + std::to_string(cpu));
        }
#else
        throw std::invalid_argument("--cpus currently requires Windows");
#endif
    }

    double ns(Clock::time_point a, Clock::time_point b) {
        return std::chrono::duration<double, std::nano>(b - a).count();
    }

    uint64_t threadCpuNs() {
#ifdef _WIN32
        FILETIME created{}, exited{}, kernel{}, user{};
        if (!GetThreadTimes(GetCurrentThread(), &created, &exited, &kernel, &user)) {
            throw std::runtime_error("GetThreadTimes failed");
        }
        const auto ticks = [](FILETIME v) { return (uint64_t{v.dwHighDateTime} << 32) | v.dwLowDateTime; };
        return (ticks(kernel) + ticks(user)) * 100;
#else
        return 0;
#endif
    }

    void profileGate(const fs::path& control) {
        if (control.empty()) { return; }
#ifdef _WIN32
        if (fs::exists(control.string() + ".ready") || fs::exists(control.string() + ".go") || fs::exists(control.string() + ".done")) {
            throw std::runtime_error("profile control files already exist; use a new prefix");
        }
        std::ofstream ready{control.string() + ".ready"};
        ready.exceptions(std::ios::badbit | std::ios::failbit);
        ready << GetCurrentProcessId() << ' ' << GetCurrentThreadId() << '\n';
        ready.close();
        std::cout << "Profile ready: " << control << '\n' << std::flush;
        const auto deadline = Clock::now() + std::chrono::seconds(60);
        while (!fs::exists(control.string() + ".go")) {
            if (Clock::now() > deadline) { throw std::runtime_error("profile sampler did not attach within 60s"); }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
#else
        throw std::invalid_argument("profiling currently requires Windows");
#endif
    }

    struct alignas(64) Worker {
        uint64_t checksum = 0, cpuNs = 0;
        Clock::time_point begin{}, end{};
        std::exception_ptr error;
        std::vector<double> samples;
    };
    struct Pass {
        double wallNs = 0;
        uint64_t checksum = 0, cpuNs = 0;
        std::vector<Worker> workers;
    };

    Pass pass(AkkEngine& engine, const Config& c, const std::vector<Key>& keys,
        const std::vector<size_t>& order, int threads, bool latency) {
        const size_t total = static_cast<size_t>(latency ? c.latencySamples : c.iterations);
        Pass result;
        result.workers.resize(static_cast<size_t>(threads));
        std::latch ready{threads}, start{1};
        const auto run = [&](int tid) {
            auto& worker = result.workers[static_cast<size_t>(tid)];
            const size_t begin = total * static_cast<size_t>(tid) / threads;
            const size_t end = total * static_cast<size_t>(tid + 1) / threads;
            const auto read = [&](size_t pos) {
                const auto value = engine.get(keys[order[pos % order.size()]]);
                if (!value || value->size() != static_cast<size_t>(c.valueBytes) || value->front() != 0 ||
                    value->back() != (c.valueBytes == 8 ? 0 : 0x5a)) {
                    throw std::runtime_error("read returned unexpected data");
                }
                return value->size() + value->front() + value->back();
            };
            try {
                pin(c, static_cast<size_t>(tid));
                if (latency) { worker.samples.resize(end - begin, 0.0); }
                for (int i = 0; i < c.warmup; ++i) { worker.checksum += read(begin + static_cast<size_t>(i)); }
            } catch (...) { worker.error = std::current_exception(); }
            ready.count_down();
            if (threads == 1 && !worker.error) { profileGate(c.profileControl); }
            start.wait();
            if (!worker.error) {
                try {
                    const auto cpuStart = threadCpuNs();
                    std::atomic_signal_fence(std::memory_order_seq_cst);
                    worker.begin = Clock::now();
                    for (size_t i = begin; i < end; ++i) {
                        if (latency) {
                            std::atomic_signal_fence(std::memory_order_seq_cst);
                            const auto t0 = Clock::now();
                            const auto observed = read(i);
                            std::atomic_signal_fence(std::memory_order_seq_cst);
                            const auto t1 = Clock::now();
                            std::atomic_signal_fence(std::memory_order_seq_cst);
                            worker.samples[i - begin] = ns(t0, t1);
                            worker.checksum += observed;
                        } else { worker.checksum += read(i); }
                    }
                    std::atomic_signal_fence(std::memory_order_seq_cst);
                    worker.end = Clock::now();
                    worker.cpuNs = threadCpuNs() - cpuStart;
                } catch (...) { worker.error = std::current_exception(); }
            }
        };
        std::vector<std::jthread> pool;
        Clock::time_point t0;
        if (threads == 1) {
            // A direct caller preserves the original single-thread baseline and gives the sampler a stable thread ID.
            start.count_down();
            run(0);
            t0 = result.workers[0].begin;
        } else {
            try {
                for (int tid = 0; tid < threads; ++tid) { pool.emplace_back(run, tid); }
            } catch (...) {
                start.count_down();
                throw;
            }
            ready.wait();
            t0 = Clock::now();
            start.count_down();
            for (auto& thread : pool) { thread.join(); }
        }
        auto end = t0;
        for (auto& worker : result.workers) {
            if (worker.error) { std::rethrow_exception(worker.error); }
            end = std::max(end, worker.end);
            result.checksum += worker.checksum;
            result.cpuNs += worker.cpuNs;
        }
        result.wallNs = ns(t0, end);
        if (result.wallNs <= 0) { throw std::runtime_error("measurement below timer resolution"); }
        if (!c.profileControl.empty()) {
            std::ofstream done{c.profileControl.string() + ".done"};
            done.exceptions(std::ios::badbit | std::ios::failbit);
            done << "done\n";
        }
        return result;
    }

    fs::path createRun(const fs::path& parent) {
        fs::create_directories(parent);
        std::mt19937_64 random{std::random_device{}()};
        for (int i = 0; i < 32; ++i) {
            const auto path = parent / ("run-" + std::to_string(Clock::now().time_since_epoch().count()) + '-' + std::to_string(random()));
            if (fs::create_directory(path)) { return path; }
        }
        throw std::runtime_error("could not create unique result directory");
    }

    void run(const Config& c, const fs::path& output) {
        auto openCsv = [&](std::string_view name) {
            std::ofstream file{output / name};
            file.exceptions(std::ios::badbit | std::ios::failbit);
            file.imbue(std::locale::classic());
            file << std::setprecision(17);
            return file;
        };
        auto config = openCsv("config.csv");
        config << "key,value\nmode,read_scaling\nmeasurement_contract_version,1\nbackend," << c.backend
            << "\nkey_pattern," << c.keyPattern
            << "\nkeys," << c.keys << "\nkey_bytes,16\nvalue_bytes," << c.valueBytes
            << "\niterations_total_per_bulk_pass," << c.iterations << "\nlatency_samples_total_per_pass," << c.latencySamples
            << "\nwarmup_per_worker_per_pass," << c.warmup << "\nrounds," << c.rounds << "\nshards," << c.shards
            << "\nshuffle_seed," << 0xA44A5EEDu << "\nengine_instances,1\napi,get\ncopy_and_disposal,included\n"
            << "WAL,off\nSST,off\nvLog,off\nBlob,off\nCluster,off\nAPI_server,off\nread_visibility,COMMIT_ORDER\ncpus,\"";
        for (int cpu : c.cpus) { config << cpu << ' '; }
        config << "\"\nprofiled," << (!c.profileControl.empty() ? "true" : "false") << '\n' << std::flush;
        try {
            pin(c, 0);
            AkkEngineOptions options;
            options.components.walEnabled = options.components.sstEnabled = options.components.manifestEnabled = false;
            options.components.versionLogEnabled = options.components.blobEnabled = false;
            options.components.clusterEnabled = options.components.apiEnabled = false;
            options.runtime.writeDurability = AkkEngineOptions::WriteDurabilityMode::MEMORY;
            options.memtable.flushMode = memtable::MemTableFlushMode::MANUAL_ONLY;
            options.memtable.shardCount = static_cast<size_t>(c.shards);
            options.memtable.backendFactory = factory(c.backend);
            auto engine = AkkEngine::open(options);
            std::vector<Key> keys(static_cast<size_t>(c.keys));
            std::vector<size_t> order(keys.size());
            std::iota(order.begin(), order.end(), size_t{0});
            std::mt19937 random{0xA44A5EEDu};
            std::shuffle(order.begin(), order.end(), random);
            std::vector<uint8_t> value(static_cast<size_t>(c.valueBytes), 0x5a);
            std::fill_n(value.begin(), 8, uint8_t{0});
            for (size_t i = 0; i < keys.size(); ++i) { keys[i] = keyFor(i, c.keyPattern); }
            std::cout << "Seeding " << c.keys << " keys; backend=" << c.backend << "; shards=" << c.shards << '\n' << std::flush;
            const auto seedStart = Clock::now();
            for (const auto& key : keys) { engine->put(key, value); }
            const auto seedNs = ns(seedStart, Clock::now());
            config << "seed_insert_total_ns," << seedNs << "\nseed_insert_ns_per_op," << seedNs / c.keys
                << "\nengine_memtable_bytes," << engine->stats().memtable.approxBytes << '\n';
            if (engine->count() != keys.size()) { throw std::runtime_error("distinct key count mismatch"); }
            for (const auto& key : keys) {
                if (engine->get(key) != std::optional{value}) { throw std::runtime_error("seed verification failed"); }
            }
            config << "verified_distinct_keys," << keys.size() << '\n';
            auto summary = openCsv("summary.csv"), samples = openCsv("samples.csv");
            summary << "round,threads,backend,shards,bulk_operations,bulk_total_ns,aggregate_ops_per_sec,wall_ns_per_op,worker_cpu_ns,cpu_cores_used,latency_samples,latency_mean_ns,p50_ns,p90_ns,p99_ns,p999_ns,verified_read_calls,checksum\n";
            samples << "round,threads,worker,sample_index,latency_ns\n";
            uint64_t verifiedReads = 0;
            std::cout << "round threads aggregate_ops/s wall_ns/op P50_ns P99_ns CPU_cores\n";
            for (int round = 0; round < c.rounds; ++round) {
                for (size_t j = 0; j < c.threads.size(); ++j) {
                    const int count = c.threads[(j + static_cast<size_t>(round)) % c.threads.size()];
                    const auto before = engine->stats().getsTotal;
                    auto bulk = pass(*engine, c, keys, order, count, false);
                    Pass latency;
                    if (c.latencySamples) { latency = pass(*engine, c, keys, order, count, true); }
                    const uint64_t expected = uint64_t(c.iterations) + c.latencySamples +
                        uint64_t(c.warmup) * count * (c.latencySamples ? 2 : 1);
                    const auto actual = engine->stats().getsTotal - before;
                    const auto expectedChecksum = expected * (uint64_t(c.valueBytes) + (c.valueBytes == 8 ? 0 : 0x5a));
                    if (actual != expected || bulk.checksum + latency.checksum != expectedChecksum) {
                        throw std::runtime_error("measured read count/checksum mismatch");
                    }
                    verifiedReads += actual;
                    std::vector<double> pooled;
                    pooled.reserve(static_cast<size_t>(c.latencySamples));
                    for (size_t t = 0; t < latency.workers.size(); ++t) {
                        const auto& values = latency.workers[t].samples;
                        for (size_t i = 0; i < values.size(); ++i) {
                            samples << round + 1 << ',' << count << ',' << t << ',' << i << ',' << values[i] << '\n';
                        }
                        pooled.insert(pooled.end(), values.begin(), values.end());
                    }
                    akkaradb::benchmark::Metrics metrics{};
                    if (!pooled.empty()) { metrics = akkaradb::benchmark::summarize(pooled, latency.wallNs); }
                    const double rate = double(c.iterations) * 1e9 / bulk.wallNs;
                    const double mean = bulk.wallNs / c.iterations;
                    summary << round + 1 << ',' << count << ',' << c.backend << ',' << c.shards << ',' << c.iterations << ','
                        << bulk.wallNs << ',' << rate << ',' << mean << ',' << bulk.cpuNs << ',' << double(bulk.cpuNs) / bulk.wallNs
                        << ',' << c.latencySamples << ',';
                    if (c.latencySamples) {
                        summary << metrics.latencyMean << ',' << metrics.p50 << ',' << metrics.p90 << ',' << metrics.p99 << ',' << metrics.p999;
                    } else { summary << ",,,,"; }
                    summary << ',' << actual << ',' << expectedChecksum << '\n' << std::flush;
                    samples.flush();
                    std::cout << round + 1 << ' ' << count << ' ' << std::fixed << std::setprecision(1) << rate << ' ' << mean
                        << ' ' << metrics.p50 << ' ' << metrics.p99 << ' ' << std::setprecision(2) << double(bulk.cpuNs) / bulk.wallNs
                        << '\n' << std::flush;
                }
            }
            engine->close();
            samples.close();
            summary.close();
            config << "verified_measured_and_warmup_read_calls," << verifiedReads << "\nverification,passed\n";
            config.close();
            std::cout << "Verification: passed\n";
        } catch (...) {
            config << "verification,failed\n" << std::flush;
            throw;
        }
    }
}

int main(int argc, char** argv) {
    akkaradb::test::installMsvcTestErrorHandlers();
    try {
        if (argc == 2 && std::string_view{argv[1]} == "--help") {
            std::cout << "AkkEngine shared-instance read scaling (memory-only, copying get)\n"
                "--keys INT --iterations INT (TOTAL per bulk pass) --latency-samples INT (0 disables)\n"
                "--threads 1,2,4,8,12,16 --shards INT --rounds INT --warmup INT (per worker per pass)\n"
                "--backend bptree|art|skiplist --value-bytes INT --cpus 0,2,4,6,8,9,... --csv-dir PATH\n"
                "--key-pattern prefix|mixed (default prefix; mixed uses distinct scrambled 16-byte keys)\n"
                "--profile-control PATH (Windows sampler handshake; single thread/round, no latency pass)\n"
                "Throughput uses one wall-clock interval; latency percentiles use a separate pass.\n"
                "wall_ns_per_op is reciprocal aggregate throughput, not per-request latency.\n";
            return 0;
        }
        const auto c = parse(argc, argv);
        const auto output = createRun(c.output);
        std::cout << "CSV directory: " << fs::absolute(output) << '\n' << std::flush;
        run(c, output);
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "Read scaling benchmark failed: " << error.what() << '\n';
        return 2;
    }
}

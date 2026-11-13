/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
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

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <locale>
#include <numeric>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace {
    using namespace akkaradb::engine;
    using Clock = std::chrono::steady_clock;
    using akkaradb::benchmark::Metrics;
    using akkaradb::benchmark::percentile;
    using Key = std::array<uint8_t, 16>;
    namespace fs = std::filesystem;

    struct Config {
        bool sst = false;
        bool vlog = false;
        bool wal = false;
        bool vlogSync = false;
        bool readComparison = false;
        int comparisonRounds = 3;
        int iterations = 10'000;
        int warmup = 100;
        int keys = 0; // Resolve to iterations unless --keys is supplied.
        int valueBytes = 128;
        int scanRows = 0; // Resolve to min(128, keys) unless explicitly supplied.
        int historyDepth = 8;
        std::string backend = "skiplist";
        fs::path csvDir = "benchmark-results";
        TransactionIsolation isolation = TransactionIsolation::SERIALIZABLE;
    };

    memtable::MemTable::MemTableFactory backendFactory(std::string_view backend) {
        if (backend == "skiplist") { return [] { return std::make_unique<memtable::SkipListMemTable>(); }; }
        if (backend == "bptree") { return [] { return std::make_unique<memtable::BPTreeMemTable>(); }; }
        if (backend == "art") { return [] { return std::make_unique<memtable::ARTMemTable>(); }; }
        throw std::invalid_argument("--backend must be skiplist, bptree, or art");
    }

    int integer(std::string_view text, std::string_view name, int minimum = 1) {
        int value = 0;
        const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
        if (error != std::errc{} || end != text.data() + text.size() || value < minimum) {
            throw std::invalid_argument(std::string{name} + " must be an int >= " + std::to_string(minimum));
        }
        return value;
    }

    bool boolean(std::string_view text, std::string_view name) {
        if (text == "t") { return true; }
        if (text == "f") { return false; }
        throw std::invalid_argument(std::string{name} + " must be t or f");
    }

    std::string prompt(const char* label) {
        std::cout << label << std::flush;
        std::string value;
        if (!std::getline(std::cin, value)) { throw std::invalid_argument("input ended before configuration was complete"); }
        const auto first = value.find_first_not_of(" \t\r");
        if (first == std::string::npos) { return {}; }
        return value.substr(first, value.find_last_not_of(" \t\r") - first + 1);
    }

    Config configuration(int argc, char** argv) {
        Config config;
        if (argc == 1) {
            std::cout << "MemTable is always enabled. WAL/Blob/Cluster/API are off by default.\n";
            const bool only = boolean(prompt("MemTable only [t/f]: "), "MemTable only");
            if (!only) {
                config.sst = boolean(prompt("SST [t/f]: "), "SST");
                config.vlog = boolean(prompt("vLog [t/f]: "), "vLog");
            }
            const auto backend = prompt("MemTable backend [skiplist/bptree/art, Enter=skiplist]: ");
            if (!backend.empty()) { config.backend = backend; }
            config.iterations = integer(prompt("Iterations per operation per pass [int > 0]: "), "iterations");
        }
        for (int i = 1; i < argc; ++i) {
            std::string_view name{argv[i]};
            const auto equal = name.find('=');
            std::string_view value;
            if (equal != std::string_view::npos) {
                value = name.substr(equal + 1);
                name = name.substr(0, equal);
            } else {
                if (++i == argc) { throw std::invalid_argument("missing value for " + std::string{name}); }
                value = argv[i];
            }
            if (name == "--sst") { config.sst = boolean(value, name); }
            else if (name == "--vlog") { config.vlog = boolean(value, name); }
            else if (name == "--wal") { config.wal = boolean(value, name); }
            else if (name == "--vlog-sync") { config.vlogSync = boolean(value, name); }
            else if (name == "--read-comparison") { config.readComparison = boolean(value, name); }
            else if (name == "--comparison-rounds") { config.comparisonRounds = integer(value, name); }
            else if (name == "--iterations") { config.iterations = integer(value, name); }
            else if (name == "--warmup") { config.warmup = integer(value, name, 0); }
            else if (name == "--keys") { config.keys = integer(value, name); }
            else if (name == "--value-bytes") { config.valueBytes = integer(value, name, 8); }
            else if (name == "--scan-rows") { config.scanRows = integer(value, name); }
            else if (name == "--history-depth") { config.historyDepth = integer(value, name, 3); }
            else if (name == "--backend") { config.backend = value; }
            else if (name == "--csv-dir") {
                if (value.empty()) { throw std::invalid_argument("--csv-dir must not be empty"); }
                config.csvDir = value;
            }
            else if (name == "--isolation") {
                if (value == "serializable") { config.isolation = TransactionIsolation::SERIALIZABLE; }
                else if (value == "snapshot") { config.isolation = TransactionIsolation::SNAPSHOT_ISOLATION; }
                else { throw std::invalid_argument("--isolation must be serializable or snapshot"); }
            } else { throw std::invalid_argument("unknown option: " + std::string{name}); }
        }
        if (config.keys == 0) { config.keys = config.iterations; }
        if (config.scanRows == 0) { config.scanRows = std::min(128, config.keys); }
        if (config.scanRows > config.keys) { throw std::invalid_argument("--scan-rows must not exceed --keys"); }
        if (config.vlogSync && !config.vlog) { throw std::invalid_argument("--vlog-sync t requires --vlog t"); }
        if (config.readComparison && (config.sst || config.vlog || config.wal)) {
            throw std::invalid_argument("--read-comparison requires --sst f --vlog f --wal f");
        }
        (void)backendFactory(config.backend);
        return config;
    }

    void help() {
        std::cout << "AkkEngine get/put/scan/getAt/transaction latency benchmark\n"
            "No arguments: interactive t/f storage selection and integer iteration count.\n"
            "Options (also accept --name=value):\n"
            "  --sst t|f             Add SST + manifest (default f)\n"
            "  --vlog t|f            Add VersionLog (default f)\n"
            "  --wal t|f             Add synced WAL (default f)\n"
            "  --vlog-sync t|f       Sync each VersionLog append (default f: async)\n"
            "  --iterations INT      Calls per operation in EACH of two passes (default 10000)\n"
            "  --warmup INT          Untimed calls before EACH pass (default 100; 0 allowed)\n"
            "  --keys INT            Distinct existing key count (default: iterations)\n"
            "  --value-bytes INT     Value size, at least 8 (default 128)\n"
            "  --scan-rows INT       Rows consumed per scan, <= keys (default min(128, keys))\n"
            "  --history-depth INT   Initial versions per key, at least 3 (default 8)\n"
            "  --backend skiplist|bptree|art (default skiplist)\n"
            "  --read-comparison t|f  Compare backend/MemTable/engine reads only (default f)\n"
            "  --comparison-rounds INT  Rotating-order comparison rounds (default 3)\n"
            "  --csv-dir PATH        Parent for a unique run folder (default benchmark-results)\n"
            "  --isolation serializable|snapshot (default serializable)\n"
            "MemTable is mandatory; SST=f, vLog=f, WAL=f selects memory-only.\n"
            "Mean: one timer pair for the whole loop. P50/P90/P99/P999: individual calls.\n"
            "P999 means P99.9. Percentiles are RAW ns, including timer overhead; no subtraction.\n"
            "Scan costs are per fully consumed scan; transactions include begin/end/destruction.\n"
            "CSV: config.csv, summary.csv, samples.csv (individual samples in execution order).\n";
    }

    class TemporaryDirectory {
        fs::path path_;
    public:
        TemporaryDirectory() {
            std::mt19937_64 random{std::random_device{}()};
            for (int attempt = 0; attempt < 32; ++attempt) {
                auto candidate = fs::temp_directory_path() / ("akkengine-latency-" + std::to_string(random()));
                if (fs::create_directory(candidate)) { path_ = std::move(candidate); return; }
            }
            throw std::runtime_error("could not create a unique temporary benchmark directory");
        }
        TemporaryDirectory(const TemporaryDirectory&) = delete;
        TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;
        ~TemporaryDirectory() {
            std::error_code error;
            fs::remove_all(path_, error);
            if (error) { std::cerr << "Could not remove benchmark directory " << path_ << ": " << error.message() << '\n'; }
        }
        const fs::path& path() const noexcept { return path_; }
    };

    AkkEngineOptions engineOptions(const Config& config, const fs::path& directory) {
        AkkEngineOptions options;
        // Leave paths empty for the memory-only case so transaction journals are not created.
        if (config.sst || config.vlog || config.wal) { options.paths.dataDir = directory; }
        options.components.sstEnabled = config.sst;
        options.components.manifestEnabled = config.sst;
        options.components.versionLogEnabled = config.vlog;
        options.components.walEnabled = config.wal;
        options.components.blobEnabled = false;
        options.components.clusterEnabled = false;
        options.components.apiEnabled = false;
        options.runtime.writeDurability = config.wal ? AkkEngineOptions::WriteDurabilityMode::SYNCED
                                                    : AkkEngineOptions::WriteDurabilityMode::MEMORY;
        options.runtime.forceFlushOnClose = config.sst;
        options.memtable.shardCount = 1;
        options.memtable.backendFactory = backendFactory(config.backend);
        options.memtable.flushMode = config.sst ? memtable::MemTableFlushMode::BYTES_PER_SHARD
                                              : memtable::MemTableFlushMode::MANUAL_ONLY;
        options.memtable.thresholdBytesPerShard = config.sst ? 4ULL * 1024 * 1024 : 0;
        options.vlog.syncMode = config.vlogSync ? vlog::VLogSyncMode::SYNC : vlog::VLogSyncMode::ASYNC;
        return options;
    }

    Key makeKey(uint64_t index) {
        Key key{};
        key[0] = 'k';
        for (size_t i = 0; i < 8; ++i) { key[8 + i] = static_cast<uint8_t>(index >> (56 - i * 8)); }
        return key;
    }

    std::vector<uint8_t> makeValue(int size, uint64_t version) {
        std::vector<uint8_t> value(static_cast<size_t>(size), 0x5a);
        for (size_t i = 0; i < 8; ++i) { value[i] = static_cast<uint8_t>(version >> (i * 8)); }
        return value;
    }

    double elapsedNs(Clock::time_point start, Clock::time_point end) {
        return std::chrono::duration<double, std::nano>(end - start).count();
    }

    struct TimerBaseline {
        double median, mean, p90, smallestNonzero;
    };

    TimerBaseline timerBaseline() {
        std::vector<double> samples(10'000);
        for (auto& sample : samples) {
            std::atomic_signal_fence(std::memory_order_seq_cst);
            const auto start = Clock::now();
            std::atomic_signal_fence(std::memory_order_seq_cst);
            const auto end = Clock::now();
            std::atomic_signal_fence(std::memory_order_seq_cst);
            sample = elapsedNs(start, end);
        }
        std::sort(samples.begin(), samples.end());
        const auto nonzero = std::find_if(samples.begin(), samples.end(), [](double sample) { return sample > 0; });
        return {percentile(samples, 500),
            std::accumulate(samples.begin(), samples.end(), 0.0) / static_cast<double>(samples.size()),
            percentile(samples, 900), nonzero == samples.end() ? 0.0 : *nonzero};
    }

    std::string csvString(std::string_view value) {
        std::string result = "\"";
        for (char ch : value) {
            if (ch == '"') { result += '"'; }
            result += ch;
        }
        result += '"';
        return result;
    }

    class CsvReport {
        const Config& config_;
        fs::path directory_;
        std::ofstream settings_, summary_, samples_;
        std::string stage_ = "setup";
    public:
        explicit CsvReport(const Config& config) : config_{config} {
            const auto parent = fs::absolute(config.csvDir);
            fs::create_directories(parent);
            const auto started = std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            std::mt19937_64 random{std::random_device{}()};
            for (int attempt = 0; attempt < 32; ++attempt) {
                auto candidate = parent / ("run-" + std::to_string(started) + "-" + std::to_string(random()));
                if (fs::create_directory(candidate)) { directory_ = std::move(candidate); break; }
            }
            if (directory_.empty()) { throw std::runtime_error("could not create a unique CSV run directory"); }
            const auto open = [&](std::ofstream& stream, const char* name) {
                stream.exceptions(std::ios::failbit | std::ios::badbit);
                stream.imbue(std::locale::classic());
                stream.open(directory_ / name, std::ios::out | std::ios::binary);
                stream << std::setprecision(17);
            };
            open(settings_, "config.csv");
            open(summary_, "summary.csv");
            open(samples_, "samples.csv");
            settings_ << "setting,value\r\n";
            summary_ << "operation,unit,backend,sst,vlog,wal,vlog_sync,isolation,iterations,keys,key_bytes,value_bytes,"
                "scan_rows,history_depth,warmup,bulk_total_ns,mean_ns,ops_per_sec,latency_mean_ns,latency_stddev_ns,"
                "latency_min_ns,p50_ns,p90_ns,p99_ns,p999_ns,latency_max_ns,revision,checksum\r\n";
            samples_ << "operation,sample_index,latency_ns\r\n";
            setting("schema_version", 1);
            setting("measurement_contract_version", 2);
            setting("timing_boundary", "operation + result consumption + result disposal");
            setting("started_unix_ns", started);
            setting("backend", config.backend);
            setting("mode", config.readComparison ? "read_comparison" : "full");
            setting("comparison_rounds", config.readComparison ? config.comparisonRounds : 0);
            setting("sst", config.sst);
            setting("vlog", config.vlog);
            setting("wal", config.wal);
            setting("vlog_sync", config.vlogSync);
            setting("isolation", config.isolation == TransactionIsolation::SERIALIZABLE ? "serializable" : "snapshot");
            setting("iterations_per_pass", config.iterations);
            setting("measured_passes", 2);
            setting("warmup_per_pass", config.warmup);
            setting("keys", config.keys);
            setting("key_bytes", 16);
            setting("value_bytes", config.valueBytes);
            setting("scan_rows", config.scanRows);
            setting("initial_history_depth", config.vlog ? config.historyDepth : 1);
            setting("memtable_shards", 1);
            setting("memtable_flush_threshold_bytes", config.sst ? 4 * 1024 * 1024 : 0);
            setting("shuffle_seed", 0xA44A5EEDu);
            setting("scan_shuffle_seed", 0xA44A5EEEu);
            setting("clock", "steady_clock");
            setting("percentile_method", "nearest_rank");
            setting("percentile_timer_correction", "none");
            setting("point_workload", "existing-key updates and hit reads; shuffled without replacement until wrap");
            setting("scan_workload", "varying bounded ranges, fully consumed");
            const auto path = directory_.generic_u8string();
            setting("csv_directory", std::string{path.begin(), path.end()});
#ifdef NDEBUG
            setting("assertions_enabled", 0);
#else
            setting("assertions_enabled", 1);
#endif
#ifdef __clang_version__
            setting("compiler", __clang_version__);
#endif
            std::cout << "CSV directory: " << directory_ << '\n' << std::flush;
        }

        template<class Value>
        void setting(std::string_view name, const Value& value) {
            std::ostringstream text;
            text.imbue(std::locale::classic());
            text << std::setprecision(17) << value;
            settings_ << csvString(name) << ',' << csvString(text.str()) << "\r\n";
        }

        void recordBaseline(const TimerBaseline& baseline) {
            setting("timer_pair_median_ns", baseline.median);
            setting("timer_pair_mean_ns", baseline.mean);
            setting("timer_pair_p90_ns", baseline.p90);
            setting("timer_pair_smallest_nonzero_ns", baseline.smallestNonzero);
        }

        void stage(std::string_view name) { stage_ = name; }

        void fail(std::string_view error) noexcept {
            try {
                setting("failed_stage", stage_);
                setting("error", error);
                setting("verification", "failed");
                settings_.flush();
            } catch (const std::exception& failure) {
                // Preserve the original exception if failure diagnostics also hit an I/O error.
                std::cerr << "Could not write CSV failure details: " << failure.what() << '\n';
            }
        }

        void recordSamples(std::string_view name, const std::vector<double>& values) {
            const auto operation = csvString(name);
            for (size_t i = 0; i < values.size(); ++i) {
                samples_ << operation << ',' << i << ',' << values[i] << "\r\n";
            }
            samples_.flush();
        }

        void recordSummary(std::string_view name, const Metrics& metrics, uint64_t checksum,
            std::optional<uint64_t> revision) {
            const auto& config = config_;
            const auto unit = name.starts_with("tx.") ? "transaction" : name == "scan" ? "scan" : "operation";
            summary_ << csvString(name) << ',' << csvString(unit) << ',' << csvString(config.backend)
                << ',' << config.sst << ',' << config.vlog << ',' << config.wal << ',' << config.vlogSync
                << ',' << csvString(config.isolation == TransactionIsolation::SERIALIZABLE ? "serializable" : "snapshot")
                << ',' << config.iterations << ',' << config.keys << ",16," << config.valueBytes << ',' << config.scanRows
                << ',' << (config.vlog ? config.historyDepth : 1) << ',' << config.warmup
                << ',' << metrics.bulkNs << ',' << metrics.meanNs << ',' << metrics.opsPerSecond
                << ',' << metrics.latencyMean << ',' << metrics.latencyStddev << ',' << metrics.min
                << ',' << metrics.p50 << ',' << metrics.p90 << ',' << metrics.p99 << ',' << metrics.p999 << ',' << metrics.max << ',';
            if (revision) { summary_ << *revision; }
            summary_ << ',' << checksum << "\r\n";
            summary_.flush();
        }

        void finish() {
            samples_.close();
            summary_.close();
            setting("verification", "passed");
            settings_.close();
        }
    };

    volatile uint64_t observedChecksum = 0;

    // Consume returned data rather than an unused result, including in optimized/static builds.
    uint64_t observe(const std::optional<std::vector<uint8_t>>& value) {
        if (!value || value->empty()) { throw std::runtime_error("benchmark read unexpectedly missed"); }
        return value->size() + value->front() + value->back();
    }
    uint64_t observe(const std::optional<akkaradb::core::RecordView>& value) {
        if (!value || value->value().empty()) { throw std::runtime_error("benchmark view unexpectedly missed"); }
        const auto bytes = value->value();
        return bytes.size() + bytes.front() + bytes.back();
    }
    uint64_t observe(const std::optional<memtable::MemTable::PinnedRecord>& value) {
        return observe(value ? std::optional{value->view} : std::nullopt);
    }
    uint64_t observe(const std::optional<std::span<const uint8_t>>& value) {
        if (!value || value->empty()) { throw std::runtime_error("benchmark buffer read unexpectedly missed"); }
        return value->size() + value->front() + value->back();
    }
    uint64_t observe(uint64_t value) { return value; }

    template<class Operation, class Prepare>
    void measure(std::string_view name, const Config& config, Operation operation, Prepare prepare,
        CsvReport& report, std::optional<uint64_t> revision = {}) {
        report.stage(name);
        auto measurements = akkaradb::benchmark::collect(config.iterations, config.warmup, operation,
            [](const auto& value) { return observe(value); }, prepare);
        observedChecksum = measurements.checksum;
        // Export before sorting so sample indices retain execution order, outside both timed passes.
        report.recordSamples(name, measurements.samples);
        const auto metrics = akkaradb::benchmark::summarize(measurements.samples, measurements.bulkNs);
        report.recordSummary(name, metrics, measurements.checksum, revision);
        std::cout << std::left << std::setw(24) << name << std::right << std::fixed << std::setprecision(1)
            << std::setw(16) << metrics.meanNs << std::setw(16) << metrics.opsPerSecond
            << std::setw(16) << metrics.p50 << std::setw(16) << metrics.p90
            << std::setw(16) << metrics.p99 << std::setw(16) << metrics.p999 << '\n' << std::flush;
    }

    uint64_t consumeScan(auto& reader, akkaradb::core::BufferArena& arena, const Key& start, const Key& end, int expectedRows) {
        arena.reset();
        uint64_t rows = 0, checksum = 0;
        for (const auto& row : reader.scan(arena, start, end)) {
            ++rows;
            checksum += row.key.back() + row.value.front() + row.value.size();
        }
        if (rows != static_cast<uint64_t>(expectedRows)) { throw std::runtime_error("scan returned an unexpected number of rows"); }
        return checksum;
    }

    void printTableHeader() {
        std::cout << std::left << std::setw(24) << "Operation" << std::right << std::setw(16) << "Mean_ns"
            << std::setw(16) << "Ops/s" << std::setw(16) << "P50_ns" << std::setw(16) << "P90_ns"
            << std::setw(16) << "P99_ns" << std::setw(16) << "P999_ns" << '\n';
    }

    void runReadComparison(const Config& config, AkkEngine& engine, const std::vector<Key>& keys,
        const std::vector<size_t>& order, CsvReport& report) {
        const auto seq = static_cast<uint64_t>(keys.size());
        auto backend = backendFactory(config.backend)();
        // Match the engine's COMMIT_ORDER retention policy, even with one version per key.
        backend->retainAllVersions();
        const std::function<uint64_t()> snapshot = [seq] { return seq; };
        auto memory = memtable::MemTable::create(engineOptions(config, {}).memtable, snapshot);
        const auto value = makeValue(config.valueBytes, 0);
        std::cout << "Read comparison: backend=" << config.backend << "; keys=" << keys.size()
            << "; iterations=" << config.iterations << "; rounds=" << config.comparisonRounds
            << "; value_bytes=" << config.valueBytes << "; copies and views are labeled separately\n"
            << "Seeding three independent stores with identical key/value/sequence data...\n" << std::flush;
        report.stage("seeding comparison stores");
        for (size_t i = 0; i < keys.size(); ++i) {
            const auto status = backend->put(std::as_bytes(std::span{keys[i]}), std::as_bytes(std::span{value}),
                i + 1, akkaradb::core::MemHdr16::FLAG_NORMAL);
            if (!status.ok()) { throw std::runtime_error("comparison backend seed failed"); }
            memory->put(keys[i], value, i + 1);
            engine.put(keys[i], value);
        }
        const auto backendRead = [&](size_t i) -> std::optional<akkaradb::core::RecordView> {
            akkaradb::core::RecordView record;
            return backend->get(std::as_bytes(std::span{keys[order[i % order.size()]]}), seq, &record)
                ? std::optional{record} : std::nullopt;
        };
        const auto memoryRead = [&](size_t i) -> std::optional<akkaradb::core::RecordView> {
            akkaradb::core::RecordView record;
            return memory->get(keys[order[i % order.size()]], seq, &record) ? std::optional{record} : std::nullopt;
        };
        std::vector<uint8_t> memoryBuffer, engineBuffer;
        memoryBuffer.reserve(value.size());
        engineBuffer.reserve(value.size());
        const auto memoryInto = [&](size_t i) -> std::optional<std::span<const uint8_t>> {
            const auto found = memory->getInto(keys[order[i % order.size()]], seq, memoryBuffer);
            return found.value_or(false) ? std::optional<std::span<const uint8_t>>{memoryBuffer} : std::nullopt;
        };
        const auto engineInto = [&](size_t i) -> std::optional<std::span<const uint8_t>> {
            return engine.getInto(keys[order[i % order.size()]], engineBuffer)
                ? std::optional<std::span<const uint8_t>>{engineBuffer} : std::nullopt;
        };
        const auto matches = [&](const auto& bytes) { return std::ranges::equal(bytes, value); };
        report.stage("verifying comparison stores");
        for (size_t i = 0; i < keys.size(); ++i) {
            const auto direct = backendRead(i), wrapped = memoryRead(i);
            const auto pinned = memory->getPinnedWithSnapshot(keys[order[i]], snapshot);
            const auto copied = engine.get(keys[order[i]]);
            const auto memoryBytes = memoryInto(i), engineBytes = engineInto(i);
            if (!direct || !wrapped || !pinned || !copied || !memoryBytes || !engineBytes ||
                !matches(direct->value()) || !matches(wrapped->value()) || !matches(pinned->view.value()) ||
                !matches(*copied) || !matches(*memoryBytes) || !matches(*engineBytes)) {
                throw std::runtime_error("comparison read verification failed");
            }
        }
        if (backend->entryCount() != keys.size() || engine.count() != keys.size()) {
            throw std::runtime_error("comparison distinct key count verification failed");
        }
        report.setting("verified_distinct_keys", keys.size());
        report.setting("backend_bytes", backend->sizeBytes());
        report.setting("memtable_bytes", memory->approxSize());
        report.setting("engine_memtable_bytes", engine.stats().memtable.approxBytes);
        report.setting("comparison_order", "rotate by two positions per round");
        const auto baseline = timerBaseline();
        report.recordBaseline(baseline);
        std::cout << "Verified distinct keys=" << keys.size() << "; all read paths verified\n"
            << "Clock=steady_clock; empty timer pair median_ns=" << baseline.median << "; mean_ns=" << baseline.mean << '\n'
            << "Mean/ops/s use bulk timing; percentiles use raw individual-call timing.\n";
        printTableHeader();
        constexpr std::array<std::string_view, 7> names{
            "backend.view", "backend.copy", "memtable.view", "memtable.pinned", "memtable.into", "engine.get", "engine.into"};
        const auto readsBefore = engine.stats().getsTotal;
        for (int round = 0; round < config.comparisonRounds; ++round) {
            for (size_t position = 0; position < names.size(); ++position) {
                const size_t which = (position + (static_cast<size_t>(round) * 2) % names.size()) % names.size();
                const auto name = std::string{names[which]} + ".r" + std::to_string(round + 1);
                const auto bench = [&](auto operation) { measure(name, config, operation, [] {}, report); };
                switch (which) {
                    case 0: bench(backendRead); break;
                    case 1: bench([&](size_t i) -> std::optional<std::vector<uint8_t>> {
                        const auto record = backendRead(i);
                        if (!record) { return std::nullopt; }
                        const auto bytes = record->value();
                        return std::vector<uint8_t>{bytes.begin(), bytes.end()};
                    }); break;
                    case 2: bench(memoryRead); break;
                    case 3: bench([&](size_t i) { return memory->getPinnedWithSnapshot(keys[order[i % order.size()]], snapshot); }); break;
                    case 4: bench(memoryInto); break;
                    case 5: bench([&](size_t i) { return engine.get(keys[order[i % order.size()]]); }); break;
                    case 6: bench(engineInto); break;
                }
            }
        }
        const uint64_t expectedReads = 4ULL * (static_cast<uint64_t>(config.iterations) + config.warmup) * config.comparisonRounds;
        const auto actualReads = engine.stats().getsTotal - readsBefore;
        if (actualReads != expectedReads) { throw std::runtime_error("comparison engine read count verification failed"); }
        report.setting("verified_engine_read_calls", actualReads);
        report.stage("closing engine");
        engine.close();
        report.finish();
        std::cout << "Verification: passed; read comparison complete\n";
    }

    void run(const Config& config, CsvReport& report) {
        TemporaryDirectory directory;
        auto engine = AkkEngine::open(engineOptions(config, directory.path()));
        std::vector<Key> keys;
        keys.reserve(static_cast<size_t>(config.keys));
        for (int i = 0; i < config.keys; ++i) { keys.push_back(makeKey(static_cast<uint64_t>(i))); }
        std::vector<size_t> order(keys.size());
        std::iota(order.begin(), order.end(), 0);
        std::mt19937 random{0xA44A5EED};
        std::shuffle(order.begin(), order.end(), random);
        if (config.readComparison) {
            runReadComparison(config, *engine, keys, order, report);
            return;
        }
        const auto keyAt = [&](size_t i) -> const Key& { return keys[order[i % order.size()]]; };
        std::vector<Key> scanEnds;
        const size_t scanRanges = keys.size() - static_cast<size_t>(config.scanRows) + 1;
        scanEnds.reserve(scanRanges);
        for (size_t i = 0; i < scanRanges; ++i) {
            scanEnds.push_back(makeKey(i + static_cast<size_t>(config.scanRows)));
        }
        std::vector<size_t> scanOrder(scanRanges);
        std::iota(scanOrder.begin(), scanOrder.end(), 0);
        std::mt19937 scanRandom{0xA44A5EEE};
        std::shuffle(scanOrder.begin(), scanOrder.end(), scanRandom);
        const int depth = config.vlog ? config.historyDepth : 1;
        const std::array<int, 3> versions{0, depth / 2, depth - 1};
        std::array<uint64_t, 3> revisions{};
        std::cout << "MemTable=t backend=" << config.backend << " SST=" << (config.sst ? 't' : 'f') << " vLog=" << (config.vlog ? 't' : 'f')
            << " WAL=" << (config.wal ? "synced" : "off") << " Blob=off Cluster=off API=off\n"
            << "vLog sync=" << (config.vlogSync ? "sync" : "async")
            << "; persistent transaction commits include the engine's durable journal cost\n"
            << "iterations=" << config.iterations << " per pass; passes=2; warmup=" << config.warmup
            << " per pass; keys=" << config.keys << "; key_bytes=16; value_bytes=" << config.valueBytes
            << "; scan_rows=" << config.scanRows << "; initial_history_depth=" << depth
            << "; isolation=" << (config.isolation == TransactionIsolation::SERIALIZABLE ? "serializable" : "snapshot") << '\n'
            << "Seeding outside measurement...\n" << std::flush;
        report.stage("seeding engine");
        for (int version = 0; version < depth; ++version) {
            const auto value = makeValue(config.valueBytes, static_cast<uint64_t>(version));
            for (const auto& key : keys) { engine->put(key, value); }
            if (config.vlog && std::find(versions.begin(), versions.end(), version) != versions.end()) {
                engine->forceSync();
                uint64_t latest = 0;
                for (const auto& entry : engine->history(keys.back())) { latest = std::max(latest, entry.seq); }
                if (latest == 0) { throw std::runtime_error("seed history has no revision"); }
                for (size_t i = 0; i < versions.size(); ++i) {
                    if (versions[i] == version) { revisions[i] = latest; }
                }
            }
        }
        const auto prepare = [&] {
            engine->forceSync();
            if (config.sst) { engine->forceFlush(); }
        };
        prepare();
        if (config.sst && engine->stats().sst.fileCount == 0) { throw std::runtime_error("SST mode did not produce any SST files"); }
        const auto seededKeys = engine->count();
        if (seededKeys != keys.size()) { throw std::runtime_error("seeded distinct key count verification failed"); }
        std::cout << "Verified distinct keys=" << seededKeys << '\n' << std::flush;
        report.setting("verified_distinct_keys", seededKeys);
        report.stage("verifying seed values");
        const auto lastValue = makeValue(config.valueBytes, static_cast<uint64_t>(depth - 1));
        for (const auto& key : keys) {
            if (engine->get(key) != std::optional{lastValue}) { throw std::runtime_error("seed value verification failed"); }
        }
        if (config.vlog) {
            for (size_t i = 0; i < versions.size(); ++i) {
                const auto expected = makeValue(config.valueBytes, static_cast<uint64_t>(versions[i]));
                for (const auto& key : keys) {
                    if (engine->getAt(key, revisions[i]) != std::optional{expected}) {
                        throw std::runtime_error("historical version verification failed");
                    }
                }
            }
        }
        const auto baseline = timerBaseline();
        report.recordBaseline(baseline);
        std::cout << "Clock=steady_clock; empty timer pair median_ns=" << baseline.median
            << "; mean_ns=" << baseline.mean << "; p90_ns=" << baseline.p90
            << "; smallest_observed_nonzero_ns=" << baseline.smallestNonzero << '\n';
        std::cout << "Mean_ns: bulk loop, only one timer pair. P columns: raw individual-call ns, no subtraction.\n"
            << "Ops/s uses the bulk pass. P999=P99.9; scan/tx.scan unit=one fully consumed scan; tx.* unit=one complete transaction.\n"
            << "Point keys are shuffled without replacement, wrapping only after all keys are visited.\n"
            << "Scan ranges vary across the dataset. Reads follow seeding/warmup; SST reads follow forceFlush.\n"
            << "put updates existing keys; this does not measure new-key insertion.\n";
        if (config.iterations < 10'000) { std::cout << "Note: fewer than 10000 samples; P999 has limited tail resolution.\n"; }
        printTableHeader();
        const auto benchmark = [&](std::string_view name, auto operation, std::optional<uint64_t> revision = {}) {
            measure(name, config, operation, prepare, report, revision);
        };
        benchmark("get", [&](size_t i) { return engine->get(keyAt(i)); });
        akkaradb::core::BufferArena arena;
        const auto scanAt = [&](auto& reader, size_t i) {
            const size_t first = scanOrder[i % scanOrder.size()];
            return consumeScan(reader, arena, keys[first], scanEnds[first], config.scanRows);
        };
        benchmark("scan", [&](size_t i) { return scanAt(*engine, i); });
        if (config.vlog) {
            constexpr std::array<std::string_view, 3> names{"getAt.oldest", "getAt.middle", "getAt.latest"};
            for (size_t version = 0; version < revisions.size(); ++version) {
                benchmark(names[version], [&](size_t i) { return engine->getAt(keyAt(i), revisions[version]); }, revisions[version]);
            }
        } else { std::cout << "getAt: skipped (vLog=f)\n"; }
        const TransactionOptions transactionOptions{config.isolation};
        benchmark("tx.empty", [&](size_t) {
            auto tx = engine->begin(transactionOptions);
            tx.end();
            return uint64_t{1};
        });
        benchmark("tx.get", [&](size_t i) {
            auto tx = engine->begin(transactionOptions);
            auto value = tx.get(keyAt(i));
            tx.end();
            return value;
        });
        benchmark("tx.scan", [&](size_t i) {
            auto tx = engine->begin(transactionOptions);
            const auto result = scanAt(tx, i);
            tx.end();
            return result;
        });
        const auto update = makeValue(config.valueBytes, static_cast<uint64_t>(depth));
        const auto verifyWritten = [&](const std::vector<uint8_t>& expected) {
            report.stage("verifying written values");
            const size_t written = std::min(keys.size(), static_cast<size_t>(std::max(config.iterations, config.warmup)));
            for (size_t i = 0; i < written; ++i) {
                if (engine->get(keyAt(i)) != std::optional{expected}) { throw std::runtime_error("committed write verification failed"); }
            }
        };
        benchmark("put", [&](size_t i) {
            engine->put(keyAt(i), update);
            return uint64_t{1};
        });
        verifyWritten(update);
        benchmark("tx.put", [&](size_t i) {
            auto tx = engine->begin(transactionOptions);
            tx.put(keyAt(i), lastValue);
            tx.end();
            return uint64_t{1};
        });
        verifyWritten(lastValue);
        benchmark("tx.get_put", [&](size_t i) {
            auto tx = engine->begin(transactionOptions);
            auto value = tx.get(keyAt(i));
            tx.put(keyAt(i), update);
            tx.end();
            return value;
        });
        // Verify the writes covered by each pass, including N < key count, outside all timing intervals.
        verifyWritten(update);
        report.stage("closing engine");
        engine->close();
        report.finish();
        std::cout << "Verification: passed; checksum=" << observedChecksum << "\nCSV files: config.csv, summary.csv, samples.csv\n";
    }
}

int main(int argc, char** argv) {
    akkaradb::test::installMsvcTestErrorHandlers();
    try {
        if (argc == 2 && std::string_view{argv[1]} == "--help") { help(); return 0; }
        const auto config = configuration(argc, argv);
        CsvReport report{config};
        try {
            run(config, report);
        } catch (const std::exception& error) {
            report.fail(error.what());
            throw;
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "AkkEngine latency benchmark failed: " << error.what() << '\n';
        return 2;
    }
}

/*
 * Copyright (C) 2026 Swift Storm Studio
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. See https://mozilla.org/MPL/2.0/.
 */
#include "TestErrorHandlers.hpp"
#include "akk/engine/memtable/ARTMemTable.hpp"
#include "akk/engine/memtable/BPTreeMemTable.hpp"
#include "akk/engine/memtable/MemTable.hpp"
#include "akk/engine/memtable/SkipListMemTable.hpp"
#include "akk/engine/memtable/WormholeMemTable.hpp"

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <locale>
#include <numeric>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#endif

namespace {
    namespace mt = akkaradb::engine::memtable;
    namespace fs = std::filesystem;
    using Clock = std::chrono::steady_clock;
    using Bytes = std::span<const uint8_t>;
    volatile uint64_t observedChecksum;

    struct Config {
        size_t keys = 100000, keyBytes = 16, valueBytes = 128;
        size_t iterations = 100000, scanIterations = 10000, warmup = 100, rounds = 3;
        std::vector<size_t> scanRows{1, 8, 32, 128, 1024};
        std::string backend = "wormhole";
        fs::path output;
        int cpu = -1;
    };

    size_t integer(std::string_view text, size_t minimum = 1) {
        size_t result{};
        const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), result);
        if (error != std::errc{} || end != text.data() + text.size() || result < minimum) {
            throw std::invalid_argument("invalid integer: " + std::string{text});
        }
        return result;
    }
    std::vector<size_t> numbers(std::string_view text) {
        std::vector<size_t> result;
        for (;;) {
            const auto comma = text.find(',');
            const size_t n = integer(text.substr(0, comma));
            if (std::find(result.begin(), result.end(), n) != result.end()) { throw std::invalid_argument("duplicate scan row count"); }
            result.push_back(n);
            if (comma == std::string_view::npos) { return result; }
            text.remove_prefix(comma + 1);
        }
    }
    mt::MemTable::MemTableFactory factory(std::string_view backend) {
        if (backend == "skiplist") { return [] { return std::make_unique<mt::SkipListMemTable>(); }; }
        if (backend == "bptree") { return [] { return std::make_unique<mt::BPTreeMemTable>(); }; }
        if (backend == "art") { return [] { return std::make_unique<mt::ARTMemTable>(); }; }
        if (backend == "wormhole") { return [] { return std::make_unique<mt::WormholeMemTable>(); }; }
        throw std::invalid_argument("backend must be skiplist, bptree, art, or wormhole");
    }
    Config parse(int argc, char** argv) {
        Config c;
        for (int i = 1; i < argc; ++i) {
            const std::string_view name = argv[i];
            if (++i == argc) { throw std::invalid_argument("missing option value"); }
            const std::string_view value = argv[i];
            if (name == "--keys") { c.keys = integer(value); }
            else if (name == "--key-bytes") { c.keyBytes = integer(value, 16); }
            else if (name == "--value-bytes") { c.valueBytes = integer(value, 8); }
            else if (name == "--iterations") { c.iterations = integer(value); }
            else if (name == "--scan-iterations") { c.scanIterations = integer(value); }
            else if (name == "--warmup") { c.warmup = integer(value, 0); }
            else if (name == "--rounds") { c.rounds = integer(value); }
            else if (name == "--scan-rows") { c.scanRows = numbers(value); }
            else if (name == "--backend") { c.backend = value; }
            else if (name == "--output") { c.output = value; }
            else if (name == "--cpu") {
                const auto cpu = integer(value, 0);
                if (cpu >= 64) { throw std::invalid_argument("CPU must be in 0..63"); }
                c.cpu = static_cast<int>(cpu);
            }
            else { throw std::invalid_argument("unknown option: " + std::string{name}); }
        }
        (void)factory(c.backend);
        if (c.keys > UINT32_MAX || c.keys == std::numeric_limits<size_t>::max() ||
            c.keys + 1 > std::numeric_limits<size_t>::max() / c.keyBytes ||
            c.iterations > std::numeric_limits<size_t>::max() / c.keyBytes) {
            throw std::invalid_argument("key slab is too large");
        }
        if (c.keyBytes > UINT16_MAX || c.valueBytes > UINT16_MAX || c.keyBytes + c.valueBytes > UINT16_MAX) {
            throw std::invalid_argument("key/value exceed MemHdr16");
        }
        if (*std::max_element(c.scanRows.begin(), c.scanRows.end()) > c.keys) { throw std::invalid_argument("scan rows exceed keys"); }
        const auto add = [](uint64_t a, uint64_t b) {
            if (a > UINT64_MAX - b) { throw std::invalid_argument("work accounting overflow"); }
            return a + b;
        };
        const auto multiply = [](uint64_t a, uint64_t b) {
            if (b != 0 && a > UINT64_MAX / b) { throw std::invalid_argument("work accounting overflow"); }
            return a * b;
        };
        uint64_t sumRows = 0;
        for (auto n : c.scanRows) { sumRows = add(sumRows, n); }
        const auto scans = add(add(c.scanIterations, c.warmup), 2);
        (void)add(c.keys, multiply(multiply(c.rounds, sumRows), scans));
        (void)add(c.keys, multiply(multiply(c.rounds, 2), add(c.iterations, c.warmup)));
        (void)multiply(multiply(c.scanIterations, *std::max_element(c.scanRows.begin(), c.scanRows.end())), c.valueBytes + 510);
        (void)multiply(c.iterations, c.valueBytes + 510);
        if (c.output.empty()) { throw std::invalid_argument("--output is required"); }
        #ifndef _WIN32
        if (c.cpu >= 0) { throw std::invalid_argument("--cpu currently requires Windows"); }
        #endif
        return c;
    }
    void pin(int cpu) {
        #ifdef _WIN32
        if (cpu >= 0) {
            const auto mask = DWORD_PTR{1} << static_cast<unsigned>(cpu);
            DWORD_PTR allowed{}, system{};
            if (!GetProcessAffinityMask(GetCurrentProcess(), &allowed, &system) || (allowed & mask) == 0 ||
                !SetProcessAffinityMask(GetCurrentProcess(), mask)) { throw std::runtime_error("CPU affinity failed"); }
        }
        #else
        (void)cpu;
        #endif
    }
    double ns(Clock::time_point start) { return std::chrono::duration<double, std::nano>(Clock::now() - start).count(); }
    void encode(uint8_t* key, size_t length, uint64_t id) {
        std::fill_n(key, length, uint8_t{0});
        key[0] = 'k';
        for (size_t j = 0; j < 8; ++j) { key[length - 8 + j] = static_cast<uint8_t>(id >> (56 - 8 * j)); }
    }
    uint64_t observe(Bytes value) { return value.empty() ? 0 : value.size() + value.front() + value.back(); }

    class Store {
        const Config& c_;
        std::unique_ptr<mt::MemTable> memory_;
        public:
            std::vector<uint8_t> keys, value;
            uint64_t getCalls = 0, scanCalls = 0, scannedRows = 0;
            explicit Store(const Config& c) : c_{c}, keys((c.keys + 1) * c.keyBytes), value(c.valueBytes, 0x5a) {
                mt::MemTable::Options options;
                options.shardCount = 1;
                options.sequenceOrderedVersions = true;
                options.flushMode = mt::MemTableFlushMode::MANUAL_ONLY;
                options.backendFactory = factory(c.backend);
                memory_ = mt::MemTable::create(options);
                for (size_t i = 0; i < 8; ++i) { value[i] = i == 0 ? 1 : 0; }
                for (size_t i = 0; i <= c.keys; ++i) { encode(keys.data() + i * c.keyBytes, c.keyBytes, 2 * uint64_t{i}); }
            }
            Bytes key(size_t i) const { return {keys.data() + i * c_.keyBytes, c_.keyBytes}; }
            double seed() {
                auto start = Clock::now();
                double total = 0;
                for (size_t i = 0; i < c_.keys; ++i) {
                    memory_->put(key(i), value, i + 1);
                    if ((i + 1) % 1000000 == 0) {
                        total += ns(start);
                        std::cout << "Seeded " << i + 1 << '\n' << std::flush;
                        start = Clock::now();
                    }
                }
                return total + ns(start);
            }
            // Keep the owning result observable across a call boundary, so the
            // compiler cannot replace its allocation/copy with two byte loads.
            #if defined(_MSC_VER)
            __declspec(noinline)
            #elif defined(__GNUC__)
            __attribute__((noinline))
            #endif
            std::optional<std::vector<uint8_t>> get(Bytes key) {
                ++getCalls;
                auto record = memory_->getPinnedWithSnapshot(key, [] { return UINT64_MAX; });
                if (!record || record->view.isTombstone()) { return std::nullopt; }
                const auto bytes = record->view.value();
                return std::vector<uint8_t>{bytes.begin(), bytes.end()};
            }
            void verify() {
                for (size_t i = 0; i < c_.keys; ++i) {
                    ++getCalls;
                    auto record = memory_->getPinnedWithSnapshot(key(i), [] { return UINT64_MAX; });
                    if (!record || record->view.flags() != 0 || record->view.seq() != i + 1 || record->view.keySize() != c_.keyBytes ||
                        record->view.valueSize() != value.size() ||
                        std::memcmp(record->view.key().data(), key(i).data(), c_.keyBytes) != 0 ||
                        std::memcmp(record->view.value().data(), value.data(), value.size()) != 0) {
                        throw std::runtime_error("seed verification failed");
                    }
                    if ((i + 1) % 1000000 == 0) { std::cout << "Verified " << i + 1 << '\n' << std::flush; }
                }
                // Normalize lazy key-order indexes before comparing seek/range
                // lengths. Bounded chunks avoid materializing the entire large
                // dataset while verifying every record's order and contents.
                for (size_t first = 0; first < c_.keys;) {
                    const size_t count = std::min<size_t>(65536, c_.keys - first);
                    (void)scan(first, count, true);
                    first += count;
                }
                std::cout << "Ordered range verification complete\n" << std::flush;
            }
            uint64_t scan(size_t first, size_t count, bool verify) {
                ++scanCalls;
                const auto a = key(first), b = key(first + count);
                mt::MemTable::KeyRange range{{a.begin(), a.end()}, {b.begin(), b.end()}};
                auto cursor = memory_->iteratorWithSnapshot(range, [] { return UINT64_MAX; });
                size_t seen = 0;
                uint64_t checksum = 0;
                while (const auto row = cursor.next()) {
                    if (seen == count) { throw std::runtime_error("scan crossed upper bound"); }
                    if (row->flags() != 0) { throw std::runtime_error("scan returned unexpected flags"); }
                    if (verify && (row->seq() != first + seen + 1 || row->keySize() != c_.keyBytes ||
                        row->valueSize() != value.size() ||
                        std::memcmp(row->key().data(), key(first + seen).data(), c_.keyBytes) != 0 ||
                        std::memcmp(row->value().data(), value.data(), value.size()) != 0)) {
                        throw std::runtime_error("scan oracle differs");
                    }
                    checksum += observe(row->value());
                    ++seen;
                }
                if (seen != count) { throw std::runtime_error("scan ended early"); }
                scannedRows += seen;
                return checksum;
            }
            size_t memoryBytes() const { return memory_->approxSize(); }
            uint64_t puts() const { return memory_->snapshot().putsApplied; }
    };

    void run(const Config& c) {
        pin(c.cpu);
        if (c.output.has_parent_path()) { fs::create_directories(c.output.parent_path()); }
        if (!fs::create_directory(c.output)) { throw std::invalid_argument("output already exists"); }
        std::ofstream config{c.output / "config.csv"}, summary{c.output / "summary.csv"};
        config.exceptions(std::ios::badbit | std::ios::failbit);
        summary.exceptions(std::ios::badbit | std::ios::failbit);
        config.imbue(std::locale::classic()); summary.imbue(std::locale::classic());
        config << std::setprecision(17) << "key,value\n";
        summary << std::setprecision(17) << "round,backend,keys,key_bytes,value_bytes,operation,scan_rows,iterations,total_ns,mean_ns,ops_per_sec,rows_per_sec,checksum\n";
        const auto setting = [&](std::string_view name, const auto& value) { config << name << ',' << value << '\n'; };
        setting("contract", "memtable-scaling-v2"); setting("backend", c.backend);
        setting("keys", c.keys); setting("key_bytes", c.keyBytes); setting("value_bytes", c.valueBytes);
        setting("iterations", c.iterations); setting("scan_iterations", c.scanIterations);
        setting("rounds", c.rounds); setting("warmup", c.warmup); setting("cpu", c.cpu);
        setting("sst", 0); setting("wal", 0); setting("shards", 1); setting("max_versions", 4);
        setting("seed_order", "ascending"); setting("key_pattern", "k-zero-prefix-big-endian-even-tail");
        setting("get_result", "owning-value-copy"); setting("scan_result", "record-view-full-consumption");
        setting("pre_scan_normalization", "full-ordered-oracle-in-65536-row-chunks");
        setting("verification", "running"); config.flush();
        Store store{c};
        std::cout << "Seeding " << c.backend << " keys=" << c.keys << " key_bytes=" << c.keyBytes << '\n' << std::flush;
        const double seedNs = store.seed();
        const size_t memoryBytes = store.memoryBytes();
        store.verify();
        setting("seed_ns", seedNs); setting("seed_ops_per_sec", c.keys * 1e9 / seedNs);
        setting("memtable_bytes", memoryBytes); setting("key_slab_bytes", store.keys.size());
        const uint64_t rowChecksum = observe(store.value);
        for (size_t round = 0; round < c.rounds; ++round) {
            std::mt19937_64 random{0xA44A5EEDULL + round};
            std::uniform_int_distribution<size_t> select(0, c.keys - 1);
            std::vector<uint32_t> hits(c.iterations);
            std::vector<uint8_t> misses(c.iterations * c.keyBytes);
            for (size_t i = 0; i < c.iterations; ++i) {
                hits[i] = static_cast<uint32_t>(select(random));
                encode(misses.data() + i * c.keyBytes, c.keyBytes, 2 * uint64_t{hits[i]} + 1);
            }
            // Rotate operation order between rounds; every backend sees the same
            // queries and sequence. Setup and oracle checks are outside timers.
            std::vector<size_t> operations(c.scanRows.size() + 2);
            std::iota(operations.begin(), operations.end(), 0);
            std::rotate(operations.begin(), operations.begin() + round % operations.size(), operations.end());
            for (const auto operation : operations) {
                const bool scanning = operation >= 2;
                const size_t rowCount = scanning ? c.scanRows[operation - 2] : 0;
                const size_t iterations = scanning ? c.scanIterations : c.iterations;
                std::vector<uint32_t> starts;
                if (scanning) {
                    std::mt19937_64 scanRandom{0x5CA00000ULL + round * 4096 + rowCount};
                    std::uniform_int_distribution<size_t> start(0, c.keys - rowCount);
                    starts.resize(iterations);
                    for (auto& i : starts) { i = static_cast<uint32_t>(start(scanRandom)); }
                    // Include edge ranges even if uniform queries miss them.
                    (void)store.scan(0, rowCount, true);
                    (void)store.scan(c.keys - rowCount, rowCount, true);
                }
                const auto call = [&](size_t i, bool verify) {
                    if (scanning) { return store.scan(starts[i % iterations], rowCount, verify); }
                    if (operation == 0) {
                        const auto value = store.get(store.key(hits[i % iterations]));
                        return value ? observe(*value) : uint64_t{0};
                    }
                    const Bytes key{misses.data() + (i % iterations) * c.keyBytes, c.keyBytes};
                    if (store.get(key)) { throw std::runtime_error("absent key unexpectedly hit"); }
                    return uint64_t{1};
                };
                for (size_t i = 0; i < c.warmup; ++i) {
                    const uint64_t expected = operation == 1 ? 1 : rowChecksum * (scanning ? rowCount : 1);
                    if (call(i, true) != expected) { throw std::runtime_error("warmup checksum differs"); }
                }
                uint64_t checksum = 0;
                std::atomic_signal_fence(std::memory_order_seq_cst);
                const auto start = Clock::now();
                for (size_t i = 0; i < iterations; ++i) { checksum += call(i, false); }
                std::atomic_signal_fence(std::memory_order_seq_cst);
                const double total = ns(start);
                observedChecksum = checksum;
                const uint64_t expected = uint64_t{iterations} * (operation == 1 ? 1 : rowChecksum * (scanning ? rowCount : 1));
                if (checksum != expected || total <= 0) { throw std::runtime_error("measured result verification failed"); }
                summary << round << ',' << c.backend << ',' << c.keys << ',' << c.keyBytes << ',' << c.valueBytes << ','
                    << (scanning ? "scan" : operation == 0 ? "get" : "get.miss") << ',' << rowCount << ',' << iterations << ','
                    << total << ',' << total / iterations << ',' << iterations * 1e9 / total << ','
                    << (scanning ? rowCount * iterations * 1e9 / total : 0) << ',' << checksum << '\n';
                summary.flush();
                std::cout << "Round " << round + 1 << ' ' << (scanning ? "scan" : operation == 0 ? "get" : "get.miss")
                    << " rows=" << rowCount << " ops/s=" << iterations * 1e9 / total << '\n' << std::flush;
            }
        }
        const uint64_t expectedGets = c.keys + uint64_t{c.rounds} * 2 * (uint64_t{c.iterations} + c.warmup);
        const uint64_t normalizationScans = (uint64_t{c.keys} + 65535) / 65536;
        const uint64_t expectedScans = normalizationScans + uint64_t{c.rounds} * c.scanRows.size() * (uint64_t{c.scanIterations} + c.warmup + 2);
        const uint64_t expectedRows = c.keys + uint64_t{c.rounds} * std::accumulate(c.scanRows.begin(), c.scanRows.end(), uint64_t{0}) * (uint64_t{c.scanIterations} + c.warmup + 2);
        if (store.getCalls != expectedGets || store.scanCalls != expectedScans || store.scannedRows != expectedRows || store.puts() != c.keys) {
            throw std::runtime_error("work accounting differs");
        }
        setting("verified_distinct_keys", c.keys); setting("verified_get_calls", store.getCalls);
        setting("verified_scan_calls", store.scanCalls); setting("verified_scan_rows", store.scannedRows);
        setting("verification", "passed");
        std::cout << "Verification: passed\n" << std::flush;
    }
}

int main(int argc, char** argv) {
    akkaradb::test::installMsvcTestErrorHandlers();
    try { run(parse(argc, argv)); return 0; }
    catch (const std::invalid_argument& error) { std::cerr << error.what() << '\n'; return 2; }
    catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}

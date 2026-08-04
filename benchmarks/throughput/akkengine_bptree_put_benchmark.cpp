/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// benchmarks/throughput/akkengine_bptree_put_benchmark.cpp
#include "TestErrorHandlers.hpp"

#include "akk/engine/AkkEngine.hpp"
#include "akk/core/record/KeyFingerprint.hpp"
#include "akk/engine/memtable/ARTMemTable.hpp"
#include "akk/engine/memtable/BPTreeMemTable.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <numeric>
#include <random>
#include <span>
#include <string>
#include <thread>
#include <vector>

using namespace akkaradb::engine;
namespace fs = std::filesystem;

namespace {
    using Clock = std::chrono::steady_clock;

    [[nodiscard]] size_t sizeArg(int argc, char** argv, const char* flag, size_t fallback) {
        const std::string prefix = std::string{flag} + "=";
        for (int i = 1; i < argc; ++i) {
            const std::string_view arg{argv[i]};
            if (arg.starts_with(prefix)) { return static_cast<size_t>(std::stoull(std::string{arg.substr(prefix.size())})); }
            if (arg == flag && i + 1 < argc) { return static_cast<size_t>(std::stoull(argv[i + 1])); }
        }
        return fallback;
    }

    [[nodiscard]] bool hasFlag(int argc, char** argv, const char* flag) {
        for (int i = 1; i < argc; ++i) {
            if (std::strcmp(argv[i], flag) == 0) { return true; }
        }
        return false;
    }

    [[nodiscard]] std::string stringArg(int argc, char** argv, const char* flag, std::string fallback) {
        const std::string prefix = std::string{flag} + "=";
        for (int i = 1; i < argc; ++i) {
            const std::string_view arg{argv[i]};
            if (arg.starts_with(prefix)) { return std::string{arg.substr(prefix.size())}; }
            if (arg == flag && i + 1 < argc) { return argv[i + 1]; }
        }
        return fallback;
    }

    [[nodiscard]] uint64_t u64Arg(int argc, char** argv, const char* flag, uint64_t fallback) {
        const std::string prefix = std::string{flag} + "=";
        for (int i = 1; i < argc; ++i) {
            const std::string_view arg{argv[i]};
            if (arg.starts_with(prefix)) { return std::stoull(std::string{arg.substr(prefix.size())}); }
            if (arg == flag && i + 1 < argc) { return std::stoull(argv[i + 1]); }
        }
        return fallback;
    }

    [[nodiscard]] bool boolFlag(int argc, char** argv, const char* flag, bool fallback = false) {
        const std::string prefix = std::string{flag} + "=";
        for (int i = 1; i < argc; ++i) {
            const std::string_view arg{argv[i]};
            if (arg == flag) { return true; }
            if (arg.starts_with(prefix)) {
                const std::string value{arg.substr(prefix.size())};
                return value == "1" || value == "true" || value == "on" || value == "yes";
            }
        }
        return fallback;
    }

    [[nodiscard]] wal::WalSyncMode walSyncMode(std::string_view value) {
        if (value == "sync") { return wal::WalSyncMode::SYNC; }
        if (value == "async") { return wal::WalSyncMode::ASYNC; }
        if (value == "off") { return wal::WalSyncMode::OFF; }
        throw std::invalid_argument("invalid --wal-sync: expected sync, async, or off");
    }

    [[nodiscard]] wal::WalExecutionMode walExecutionMode(std::string_view value) {
        if (value == "auto") { return wal::WalExecutionMode::AUTO; }
        if (value == "inline") { return wal::WalExecutionMode::INLINE; }
        if (value == "async") { return wal::WalExecutionMode::ASYNC; }
        throw std::invalid_argument("invalid --wal-execution: expected auto, inline, or async");
    }

    [[nodiscard]] wal::WalSyncPolicy walSyncPolicy(std::string_view value) {
        if (value == "auto") { return wal::WalSyncPolicy::AUTO; }
        if (value == "never") { return wal::WalSyncPolicy::NEVER; }
        if (value == "on-sync-ack") { return wal::WalSyncPolicy::ON_SYNC_ACK; }
        if (value == "always") { return wal::WalSyncPolicy::ALWAYS; }
        throw std::invalid_argument("invalid --wal-sync-policy: expected auto, never, on-sync-ack, or always");
    }

    [[nodiscard]] wal::WalBackpressureMode walBackpressureMode(std::string_view value) {
        if (value == "block") { return wal::WalBackpressureMode::BLOCK; }
        if (value == "fail-fast") { return wal::WalBackpressureMode::FAIL_FAST; }
        throw std::invalid_argument("invalid --wal-backpressure: expected block or fail-fast");
    }

    [[nodiscard]] AkkEngineOptions::BackpressureMode backpressureMode(std::string_view value) {
        if (value == "block") { return AkkEngineOptions::BackpressureMode::BLOCK; }
        if (value == "fail-fast") { return AkkEngineOptions::BackpressureMode::FAIL_FAST; }
        throw std::invalid_argument("invalid backpressure mode: expected block or fail-fast");
    }

    [[nodiscard]] memtable::MemTableFlushMode memtableFlushMode(std::string_view value) {
        if (value == "auto") { return memtable::MemTableFlushMode::AUTO; }
        if (value == "bytes-per-shard") { return memtable::MemTableFlushMode::BYTES_PER_SHARD; }
        if (value == "manual-only") { return memtable::MemTableFlushMode::MANUAL_ONLY; }
        throw std::invalid_argument("invalid --memtable-flush-mode: expected auto, bytes-per-shard, or manual-only");
    }

    [[nodiscard]] sst::SSTCompactionMode sstCompactionMode(std::string_view value) {
        if (value == "auto") { return sst::SSTCompactionMode::AUTO; }
        if (value == "background") { return sst::SSTCompactionMode::BACKGROUND; }
        if (value == "disabled") { return sst::SSTCompactionMode::DISABLED; }
        throw std::invalid_argument("invalid --sst-compaction-mode: expected auto, background, or disabled");
    }

    [[nodiscard]] AkkEngineOptions::WriteAdmissionMode writeAdmissionMode(std::string_view value) {
        if (value == "auto") { return AkkEngineOptions::WriteAdmissionMode::AUTO; }
        if (value == "serial") { return AkkEngineOptions::WriteAdmissionMode::SERIAL; }
        if (value == "parallel") { return AkkEngineOptions::WriteAdmissionMode::PARALLEL; }
        throw std::invalid_argument("invalid --write-admission: expected auto, serial, or parallel");
    }

    [[nodiscard]] AkkEngineOptions::WritePolicyPreset writePolicyPreset(std::string_view value) {
        if (value == "custom") { return AkkEngineOptions::WritePolicyPreset::CUSTOM; }
        if (value == "safe") { return AkkEngineOptions::WritePolicyPreset::SAFE; }
        if (value == "balanced") { return AkkEngineOptions::WritePolicyPreset::BALANCED; }
        if (value == "fast") { return AkkEngineOptions::WritePolicyPreset::FAST; }
        throw std::invalid_argument("invalid --write-policy: expected custom, safe, balanced, or fast");
    }

    [[nodiscard]] AkkEngineOptions::WriteDurabilityMode writeDurabilityMode(std::string_view value) {
        if (value == "memory") { return AkkEngineOptions::WriteDurabilityMode::MEMORY; }
        if (value == "enqueued") { return AkkEngineOptions::WriteDurabilityMode::ENQUEUED; }
        if (value == "written") { return AkkEngineOptions::WriteDurabilityMode::WRITTEN; }
        if (value == "synced") { return AkkEngineOptions::WriteDurabilityMode::SYNCED; }
        throw std::invalid_argument("invalid --write-durability: expected memory, enqueued, written, or synced");
    }

    [[nodiscard]] AkkEngineOptions::WriteVisibilityMode writeVisibilityMode(std::string_view value) {
        if (value == "commit-order") { return AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER; }
        if (value == "applied") { return AkkEngineOptions::WriteVisibilityMode::APPLIED; }
        throw std::invalid_argument("invalid --write-visibility: expected commit-order or applied");
    }

    [[nodiscard]] AkkEngineOptions::ReadVisibilityMode readVisibilityMode(std::string_view value) {
        if (value == "auto") { return AkkEngineOptions::ReadVisibilityMode::AUTO; }
        if (value == "commit-order") { return AkkEngineOptions::ReadVisibilityMode::COMMIT_ORDER; }
        if (value == "applied") { return AkkEngineOptions::ReadVisibilityMode::APPLIED; }
        throw std::invalid_argument("invalid --read-visibility: expected auto, commit-order, or applied");
    }

    [[nodiscard]] AkkEngineOptions::SequenceAllocationMode sequenceAllocationMode(std::string_view value) {
        if (value == "global-atomic") { return AkkEngineOptions::SequenceAllocationMode::GLOBAL_ATOMIC; }
        if (value == "thread-local-ranges") { return AkkEngineOptions::SequenceAllocationMode::THREAD_LOCAL_RANGES; }
        throw std::invalid_argument("invalid --sequence-allocation: expected global-atomic or thread-local-ranges");
    }

    [[nodiscard]] std::string_view writeDurabilityName(AkkEngineOptions::WriteDurabilityMode value) noexcept {
        switch (value) {
            case AkkEngineOptions::WriteDurabilityMode::MEMORY:
                return "memory";
            case AkkEngineOptions::WriteDurabilityMode::ENQUEUED:
                return "enqueued";
            case AkkEngineOptions::WriteDurabilityMode::WRITTEN:
                return "written";
            case AkkEngineOptions::WriteDurabilityMode::SYNCED:
                return "synced";
        }
        return "unknown";
    }

    [[nodiscard]] std::string_view writeVisibilityName(AkkEngineOptions::WriteVisibilityMode value) noexcept {
        switch (value) {
            case AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER:
                return "commit-order";
            case AkkEngineOptions::WriteVisibilityMode::APPLIED:
                return "applied";
        }
        return "unknown";
    }

    [[nodiscard]] std::string_view readVisibilityName(AkkEngineOptions::ReadVisibilityMode value) noexcept {
        switch (value) {
            case AkkEngineOptions::ReadVisibilityMode::AUTO:
                return "auto";
            case AkkEngineOptions::ReadVisibilityMode::COMMIT_ORDER:
                return "commit-order";
            case AkkEngineOptions::ReadVisibilityMode::APPLIED:
                return "applied";
        }
        return "unknown";
    }

    struct EffectiveWritePolicy {
        AkkEngineOptions::WriteDurabilityMode durability;
        AkkEngineOptions::WriteVisibilityMode visibility;
    };

    [[nodiscard]] EffectiveWritePolicy effectiveWritePolicy(
        bool walEnabled,
        AkkEngineOptions::WritePolicyPreset policy,
        AkkEngineOptions::WriteDurabilityMode durability,
        AkkEngineOptions::WriteVisibilityMode visibility
    ) {
        switch (policy) {
            case AkkEngineOptions::WritePolicyPreset::CUSTOM:
                break;
            case AkkEngineOptions::WritePolicyPreset::SAFE:
                durability = AkkEngineOptions::WriteDurabilityMode::SYNCED;
                visibility = AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER;
                break;
            case AkkEngineOptions::WritePolicyPreset::BALANCED:
                durability = AkkEngineOptions::WriteDurabilityMode::WRITTEN;
                visibility = AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER;
                break;
            case AkkEngineOptions::WritePolicyPreset::FAST:
                durability = walEnabled ? AkkEngineOptions::WriteDurabilityMode::ENQUEUED : AkkEngineOptions::WriteDurabilityMode::MEMORY;
                visibility = AkkEngineOptions::WriteVisibilityMode::APPLIED;
                break;
        }
        return {durability, visibility};
    }

    [[nodiscard]] AkkEngineOptions::ReadVisibilityMode effectiveReadVisibility(
        AkkEngineOptions::ReadVisibilityMode readVisibility,
        AkkEngineOptions::WriteVisibilityMode legacyVisibility
    ) noexcept {
        if (readVisibility != AkkEngineOptions::ReadVisibilityMode::AUTO) { return readVisibility; }
        return legacyVisibility == AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER
                   ? AkkEngineOptions::ReadVisibilityMode::COMMIT_ORDER
                   : AkkEngineOptions::ReadVisibilityMode::APPLIED;
    }

    [[nodiscard]] std::span<const uint8_t> bytes(const std::vector<uint8_t>& data, size_t index, size_t width) {
        return {data.data() + index * width, width};
    }

    void writeLe64(uint64_t value, uint8_t* out) {
        for (size_t i = 0; i < sizeof(value); ++i) { out[i] = static_cast<uint8_t>(value >> (i * 8)); }
    }

    [[nodiscard]] std::vector<uint8_t> makeFixedRows(size_t count, size_t width, uint64_t salt) {
        std::vector<uint8_t> out(count * width);
        for (size_t i = 0; i < count; ++i) {
            uint8_t* row = out.data() + i * width;
            const uint64_t value = i ^ salt;
            size_t written = 0;
            while (written < width) {
                const size_t chunk = std::min(sizeof(value), width - written);
                uint8_t tmp[sizeof(value)]{};
                writeLe64(value + written, tmp);
                std::memcpy(row + written, tmp, chunk);
                written += chunk;
            }
        }
        return out;
    }

    [[nodiscard]] std::vector<size_t> makeOrder(size_t count, std::string_view order, uint64_t seed) {
        std::vector<size_t> out(count);
        std::iota(out.begin(), out.end(), size_t{0});
        if (order == "random") {
            std::mt19937_64 rng(seed);
            std::shuffle(out.begin(), out.end(), rng);
        }
        return out;
    }

    [[nodiscard]] AkkEngineOptions optionsFor(
        size_t writers,
        size_t shards,
        std::string_view backend,
        bool walEnabled,
        bool sstEnabled,
        bool manifestEnabled,
        bool forceFlushOnClose,
        bool forceSyncOnClose,
        uint64_t flushThresholdBytes,
        uint64_t walShards,
        wal::WalSyncMode syncMode,
        wal::WalExecutionMode walExecution,
        wal::WalSyncPolicy walSyncPolicyValue,
        wal::WalBackpressureMode walBackpressure,
        memtable::MemTableFlushMode flushMode,
        sst::SSTCompactionMode compactionMode,
        AkkEngineOptions::SequenceAllocationMode sequenceAllocation,
        uint32_t sequenceRangeSize,
        uint32_t commitWindowSize,
        AkkEngineOptions::WriteAdmissionMode writeAdmission,
        AkkEngineOptions::WritePolicyPreset writePolicy,
        AkkEngineOptions::WriteDurabilityMode writeDurability,
        AkkEngineOptions::WriteVisibilityMode writeVisibility,
        AkkEngineOptions::ReadVisibilityMode readVisibility,
        AkkEngineOptions::BackpressureMode memtableBackpressureMode,
        uint32_t maxMemtableImmutableTables,
        AkkEngineOptions::BackpressureMode sstBackpressureMode,
        uint32_t maxSstL0Files,
        uint32_t backpressureWaitMicros,
        uint32_t backpressureTimeoutMs,
        const fs::path& dataDir
    ) {
        AkkEngineOptions options;
        options.paths.dataDir = dataDir;
        options.components.walEnabled = walEnabled;
        options.components.blobEnabled = false;
        options.components.manifestEnabled = manifestEnabled;
        options.components.sstEnabled = sstEnabled;
        options.components.versionLogEnabled = false;
        options.components.clusterEnabled = false;
        options.components.apiEnabled = false;
        options.runtime.recoverWal = false;
        options.runtime.recoverSst = false;
        options.runtime.forceFlushOnClose = forceFlushOnClose;
        options.runtime.forceSyncOnClose = forceSyncOnClose;
        options.runtime.writePolicy = writePolicy;
        options.runtime.writeAdmission = writeAdmission;
        options.runtime.writeDurability = writeDurability;
        options.runtime.writeVisibility = writeVisibility;
        options.runtime.visibility.readVisibility = readVisibility;
        options.runtime.sequence.allocation = sequenceAllocation;
        options.runtime.sequence.threadLocalRangeSize = sequenceRangeSize;
        options.runtime.sequence.commitWindowSize = commitWindowSize;
        options.runtime.backpressure.memtableFlushBacklog = memtableBackpressureMode;
        options.runtime.backpressure.maxMemtableImmutableTables = maxMemtableImmutableTables;
        options.runtime.backpressure.sstCompactionBacklog = sstBackpressureMode;
        options.runtime.backpressure.maxSstL0Files = maxSstL0Files;
        options.runtime.backpressure.waitMicros = backpressureWaitMicros;
        options.runtime.backpressure.timeoutMs = backpressureTimeoutMs;
        options.runtime.relaxedConcurrentWrites = true;
        options.runtime.writerThreads = static_cast<uint32_t>(writers);
        options.wal.syncMode = syncMode;
        options.wal.execution = walExecution;
        options.wal.syncPolicy = walSyncPolicyValue;
        options.wal.backpressure = walBackpressure;
        options.wal.shardCount = static_cast<uint16_t>(walShards);
        options.memtable.shardCount = shards;
        options.memtable.expectedConcurrentWriters = writers;
        options.memtable.flushMode = flushMode;
        options.memtable.thresholdBytesPerShard = flushThresholdBytes;
        options.sst.compactionMode = compactionMode;
        if (backend == "art") {
            options.memtable.backendFactory = [] { return std::make_unique<memtable::ARTMemTable>(); };
        }
        else {
            options.memtable.backendFactory = [] { return std::make_unique<memtable::BPTreeMemTable>(); };
        }
        return options;
    }
}

int main(int argc, char** argv) {
    akkaradb::test::installMsvcTestErrorHandlers();

    const size_t ops = std::max<size_t>(1, sizeArg(argc, argv, "--ops", 1'000'000));
    const size_t writers = std::max<size_t>(1, sizeArg(argc, argv, "--writers", 1));
    const size_t shards = sizeArg(argc, argv, "--shards", writers == 1 ? 1 : 64);
    const size_t keyBytes = std::max<size_t>(1, sizeArg(argc, argv, "--key-bytes", 16));
    const size_t valueBytes = std::max<size_t>(0, sizeArg(argc, argv, "--value-bytes", 16));
    const std::string backend = stringArg(argc, argv, "--backend", "bptree");
    const std::string order = stringArg(argc, argv, "--order", "sequential");
    const uint64_t seed = u64Arg(argc, argv, "--seed", 1);
    const bool walEnabled = boolFlag(argc, argv, "--wal");
    const bool sstEnabled = boolFlag(argc, argv, "--sst");
    const bool manifestEnabled = boolFlag(argc, argv, "--manifest", sstEnabled);
    const bool forceFlushOnClose = boolFlag(argc, argv, "--force-flush-on-close", sstEnabled);
    const bool forceSyncOnClose = boolFlag(argc, argv, "--force-sync-on-close", walEnabled);
    const uint64_t flushThresholdBytes = u64Arg(argc, argv, "--flush-threshold-bytes", sstEnabled ? 64ULL * 1024ULL * 1024ULL : 0);
    const uint64_t walShards = u64Arg(argc, argv, "--wal-shards", 0);
    const std::string walSync = stringArg(argc, argv, "--wal-sync", "off");
    const std::string walExecution = stringArg(argc, argv, "--wal-execution", "auto");
    const std::string walSyncPolicyValue = stringArg(argc, argv, "--wal-sync-policy", "auto");
    const std::string walBackpressure = stringArg(argc, argv, "--wal-backpressure", "block");
    const std::string memtableFlushModeValue = stringArg(argc, argv, "--memtable-flush-mode", "auto");
    const std::string sstCompactionModeValue = stringArg(argc, argv, "--sst-compaction-mode", "auto");
    const std::string sequenceAllocation = stringArg(argc, argv, "--sequence-allocation", "global-atomic");
    const uint32_t sequenceRangeSize = static_cast<uint32_t>(sizeArg(argc, argv, "--sequence-range-size", 1));
    const uint32_t commitWindowSize = static_cast<uint32_t>(sizeArg(argc, argv, "--commit-window-size", 0));
    const std::string writeAdmission = stringArg(argc, argv, "--write-admission", "auto");
    const std::string writePolicy = stringArg(argc, argv, "--write-policy", "custom");
    const std::string writeDurability = stringArg(argc, argv, "--write-durability", "synced");
    const std::string writeVisibility = stringArg(argc, argv, "--write-visibility", "commit-order");
    const std::string readVisibility = stringArg(argc, argv, "--read-visibility", "auto");
    const std::string memtableBackpressureModeValue = stringArg(argc, argv, "--backpressure-memtable-mode", "block");
    const uint32_t maxMemtableImmutableTables = static_cast<uint32_t>(sizeArg(argc, argv, "--backpressure-memtable-immutables", 0));
    const std::string sstBackpressureModeValue = stringArg(argc, argv, "--backpressure-sst-mode", "block");
    const uint32_t maxSstL0Files = static_cast<uint32_t>(sizeArg(argc, argv, "--backpressure-sst-l0-files", 0));
    const uint32_t backpressureWaitMicros = static_cast<uint32_t>(sizeArg(argc, argv, "--backpressure-wait-micros", 100));
    const uint32_t backpressureTimeoutMs = static_cast<uint32_t>(sizeArg(argc, argv, "--backpressure-timeout-ms", 30'000));
    const fs::path dataDir = stringArg(argc, argv, "--data-dir", {});
    if (backend != "bptree" && backend != "art") {
        std::cerr << "invalid --backend: expected bptree or art\n";
        return 2;
    }
    if (order != "sequential" && order != "random") {
        std::cerr << "invalid --order: expected sequential or random\n";
        return 2;
    }
    const bool hinted = hasFlag(argc, argv, "--hinted");
    const bool rawMemtable = hasFlag(argc, argv, "--raw-memtable");
    const bool readAfterWrite = hasFlag(argc, argv, "--get");

    auto keys = makeFixedRows(ops, keyBytes, 0xA44A000000000001ULL);
    auto values = makeFixedRows(ops, valueBytes, 0xA44A000000000002ULL);
    auto opOrder = makeOrder(ops, order, seed);
    std::vector<uint64_t> fp64;
    std::vector<uint64_t> mini;
    if (hinted || rawMemtable || readAfterWrite) {
        fp64.resize(ops);
        mini.resize(ops);
        for (size_t i = 0; i < ops; ++i) {
            const auto key = bytes(keys, i, keyBytes);
            fp64[i] = akkaradb::core::computeKeyFp64(key.data(), key.size());
            mini[i] = akkaradb::core::buildMiniKey(key.data(), key.size());
        }
    }

    const auto parsedWritePolicy = writePolicyPreset(writePolicy);
    const auto parsedWriteDurability = writeDurabilityMode(writeDurability);
    const auto parsedWriteVisibility = writeVisibilityMode(writeVisibility);
    const auto parsedReadVisibility = readVisibilityMode(readVisibility);
    const auto effectivePolicy = effectiveWritePolicy(walEnabled, parsedWritePolicy, parsedWriteDurability, parsedWriteVisibility);
    const auto effectiveReadVisibilityValue = effectiveReadVisibility(parsedReadVisibility, effectivePolicy.visibility);
    const auto effectiveWriteVisibilityValue = effectiveReadVisibilityValue == AkkEngineOptions::ReadVisibilityMode::COMMIT_ORDER
                                                   ? AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER
                                                   : AkkEngineOptions::WriteVisibilityMode::APPLIED;

    auto options = optionsFor(
        writers,
        shards,
        backend,
        walEnabled,
        sstEnabled,
        manifestEnabled,
        forceFlushOnClose,
        forceSyncOnClose,
        flushThresholdBytes,
        walShards,
        walSyncMode(walSync),
        walExecutionMode(walExecution),
        walSyncPolicy(walSyncPolicyValue),
        walBackpressureMode(walBackpressure),
        memtableFlushMode(memtableFlushModeValue),
        sstCompactionMode(sstCompactionModeValue),
        sequenceAllocationMode(sequenceAllocation),
        sequenceRangeSize,
        commitWindowSize,
        writeAdmissionMode(writeAdmission),
        parsedWritePolicy,
        parsedWriteDurability,
        parsedWriteVisibility,
        parsedReadVisibility,
        backpressureMode(memtableBackpressureModeValue),
        maxMemtableImmutableTables,
        backpressureMode(sstBackpressureModeValue),
        maxSstL0Files,
        backpressureWaitMicros,
        backpressureTimeoutMs,
        dataDir
    );
    auto engine = AkkEngine::open(options);
    auto memtable = memtable::MemTable::create(options.memtable);

    std::atomic<bool> start{false};
    std::atomic<size_t> ready{0};
    std::vector<std::thread> threads;
    threads.reserve(writers);

    const auto tCreateDone = Clock::now();
    for (size_t tid = 0; tid < writers; ++tid) {
        threads.emplace_back(
            [&, tid] {
                ready.fetch_add(1, std::memory_order_release);
                while (!start.load(std::memory_order_acquire)) { std::this_thread::yield(); }
                for (size_t pos = tid; pos < ops; pos += writers) {
                    const size_t i = opOrder[pos];
                    const auto key = bytes(keys, i, keyBytes);
                    const auto value = bytes(values, i, valueBytes);
                    if (rawMemtable) {
                        const uint64_t seq = memtable->nextSeq();
                        memtable->put(key, value, seq, 0, fp64[i], mini[i]);
                    }
                    else if (hinted) {
                        engine->putHinted(key, value, fp64[i], mini[i]);
                    }
                    else {
                        engine->put(key, value);
                    }
                }
            }
        );
    }

    while (ready.load(std::memory_order_acquire) < writers) { std::this_thread::yield(); }
    const auto t0 = Clock::now();
    start.store(true, std::memory_order_release);
    for (auto& thread : threads) { thread.join(); }
    const auto t1 = Clock::now();

    const double setupSeconds = std::chrono::duration<double>(tCreateDone.time_since_epoch()).count();
    (void)setupSeconds;
    const double seconds = std::chrono::duration<double>(t1 - t0).count();
    std::cout << "ops = " << ops << '\n';
    std::cout << "writers = " << writers << '\n';
    std::cout << "shards = " << shards << '\n';
    std::cout << "keyBytes = " << keyBytes << '\n';
    std::cout << "valueBytes = " << valueBytes << '\n';
    std::cout << "backend = " << backend << '\n';
    std::cout << "order = " << order << '\n';
    std::cout << "seed = " << seed << '\n';
    std::cout << "wal = " << (walEnabled ? "true" : "false") << '\n';
    std::cout << "sst = " << (sstEnabled ? "true" : "false") << '\n';
    std::cout << "manifest = " << (manifestEnabled ? "true" : "false") << '\n';
    std::cout << "walSync = " << walSync << '\n';
    std::cout << "walExecution = " << walExecution << '\n';
    std::cout << "walSyncPolicy = " << walSyncPolicyValue << '\n';
    std::cout << "walBackpressure = " << walBackpressure << '\n';
    std::cout << "walShards = " << walShards << '\n';
    std::cout << "memtableFlushMode = " << memtableFlushModeValue << '\n';
    std::cout << "sstCompactionMode = " << sstCompactionModeValue << '\n';
    std::cout << "sequenceAllocation = " << sequenceAllocation << '\n';
    std::cout << "sequenceRangeSize = " << sequenceRangeSize << '\n';
    std::cout << "commitWindowSize = " << commitWindowSize << '\n';
    std::cout << "writePolicy = " << writePolicy << '\n';
    std::cout << "writeAdmission = " << writeAdmission << '\n';
    std::cout << "writeDurability = " << writeDurability << '\n';
    std::cout << "writeVisibility = " << writeVisibility << '\n';
    std::cout << "readVisibility = " << readVisibility << '\n';
    std::cout << "backpressureMemtableMode = " << memtableBackpressureModeValue << '\n';
    std::cout << "backpressureMemtableImmutables = " << maxMemtableImmutableTables << '\n';
    std::cout << "backpressureSstMode = " << sstBackpressureModeValue << '\n';
    std::cout << "backpressureSstL0Files = " << maxSstL0Files << '\n';
    std::cout << "backpressureWaitMicros = " << backpressureWaitMicros << '\n';
    std::cout << "backpressureTimeoutMs = " << backpressureTimeoutMs << '\n';
    std::cout << "effectiveWriteDurability = " << writeDurabilityName(effectivePolicy.durability) << '\n';
    std::cout << "effectiveWriteVisibility = " << writeVisibilityName(effectiveWriteVisibilityValue) << '\n';
    std::cout << "effectiveReadVisibility = " << readVisibilityName(effectiveReadVisibilityValue) << '\n';
    std::cout << "flushThresholdBytes = " << flushThresholdBytes << '\n';
    std::cout << "forceFlushOnClose = " << (forceFlushOnClose ? "true" : "false") << '\n';
    std::cout << "forceSyncOnClose = " << (forceSyncOnClose ? "true" : "false") << '\n';
    std::cout << "hinted = " << (hinted ? "true" : "false") << '\n';
    std::cout << "rawMemtable = " << (rawMemtable ? "true" : "false") << '\n';
    std::cout << "seconds = " << seconds << '\n';
    std::cout << "throughputOpsSec = " << (seconds > 0.0 ? static_cast<double>(ops) / seconds : 0.0) << '\n';
    const auto stats = rawMemtable ? EngineStats{} : engine->stats();
    std::cout << "currentSeq = " << (rawMemtable ? memtable->lastSeq() : stats.currentSeq) << '\n';
    if (!rawMemtable) {
        std::cout << "effectiveWalExecution = " << stats.config.walExecution << '\n';
        std::cout << "effectiveWalSyncPolicy = " << stats.config.walSyncPolicy << '\n';
        std::cout << "effectiveWriteVisibilityCode = " << stats.config.writeVisibility << '\n';
        std::cout << "effectiveMemtableFlushMode = " << stats.config.memtableFlushMode << '\n';
        std::cout << "effectiveSstCompactionMode = " << stats.config.sstCompactionMode << '\n';
        std::cout << "effectiveSequenceAllocation = " << stats.config.sequenceAllocation << '\n';
        std::cout << "effectiveCommitWindowSize = " << stats.config.commitWindowSize << '\n';
        std::cout << "effectiveReadVisibilityCode = " << stats.config.readVisibility << '\n';
        std::cout << "effectiveMemtableBackpressureMode = " << stats.config.memtableBackpressureMode << '\n';
        std::cout << "effectiveMaxMemtableImmutableTables = " << stats.config.maxMemtableImmutableTables << '\n';
        std::cout << "effectiveSstBackpressureMode = " << stats.config.sstBackpressureMode << '\n';
        std::cout << "effectiveMaxSstL0Files = " << stats.config.maxSstL0Files << '\n';
        std::cout << "effectiveBackpressureWaitMicros = " << stats.config.backpressureWaitMicros << '\n';
        std::cout << "effectiveBackpressureTimeoutMs = " << stats.config.backpressureTimeoutMs << '\n';
        std::cout << "backpressureBlockedWrites = " << stats.backpressure.blockedWrites << '\n';
        std::cout << "backpressureRejectedWrites = " << stats.backpressure.rejectedWrites << '\n';
        std::cout << "backpressureTimedOutWrites = " << stats.backpressure.timedOutWrites << '\n';
        std::cout << "backpressureMemtableStalls = " << stats.backpressure.memtableStalls << '\n';
        std::cout << "backpressureSstStalls = " << stats.backpressure.sstStalls << '\n';
        std::cout << "backpressureWaitMicrosTotal = " << stats.backpressure.waitMicrosTotal << '\n';
        std::cout << "backpressureWaitMicrosMax = " << stats.backpressure.waitMicrosMax << '\n';
        std::cout << "memtableImmutableTables = " << stats.memtable.immutableTables << '\n';
    }

    if (readAfterWrite) {
        ready.store(0, std::memory_order_release);
        start.store(false, std::memory_order_release);
        threads.clear();
        std::atomic<size_t> found{0};
        threads.reserve(writers);
        for (size_t tid = 0; tid < writers; ++tid) {
            threads.emplace_back(
                [&, tid] {
                    std::vector<uint8_t> out;
                    out.reserve(valueBytes);
                    memtable::MemTable::RecordView view;
                    ready.fetch_add(1, std::memory_order_release);
                    while (!start.load(std::memory_order_acquire)) { std::this_thread::yield(); }
                    const uint64_t snapshot = rawMemtable ? memtable->lastSeq() : engine->stats().currentSeq;
                    for (size_t pos = tid; pos < ops; pos += writers) {
                        const size_t i = opOrder[pos];
                        const auto key = bytes(keys, i, keyBytes);
                        if (rawMemtable) {
                            if (memtable->get(key, snapshot, &view, fp64[i])) { found.fetch_add(1, std::memory_order_relaxed); }
                        }
                        else {
                            if (engine->getInto(key, out)) { found.fetch_add(1, std::memory_order_relaxed); }
                        }
                    }
                }
            );
        }

        while (ready.load(std::memory_order_acquire) < writers) { std::this_thread::yield(); }
        const auto getT0 = Clock::now();
        start.store(true, std::memory_order_release);
        for (auto& thread : threads) { thread.join(); }
        const auto getT1 = Clock::now();
        const double getSeconds = std::chrono::duration<double>(getT1 - getT0).count();
        std::cout << "getSeconds = " << getSeconds << '\n';
        std::cout << "getThroughputOpsSec = " << (getSeconds > 0.0 ? static_cast<double>(ops) / getSeconds : 0.0) << '\n';
        std::cout << "getFound = " << found.load(std::memory_order_relaxed) << '\n';
    }

    const auto closeT0 = Clock::now();
    engine->close();
    const auto closeT1 = Clock::now();
    const double closeSeconds = std::chrono::duration<double>(closeT1 - closeT0).count();
    std::cout << "closeSeconds = " << closeSeconds << '\n';
    return 0;
}

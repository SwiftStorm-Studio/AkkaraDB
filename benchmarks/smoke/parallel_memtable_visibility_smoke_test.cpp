/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// benchmarks/smoke/parallel_memtable_visibility_smoke_test.cpp
#include "TestErrorHandlers.hpp"

#include "akk/engine/AkkEngine.hpp"
#include "akk/engine/memtable/ARTMemTable.hpp"
#include "akk/engine/memtable/BPTreeMemTable.hpp"
#include "akk/engine/memtable/MemTable.hpp"
#include "akk/engine/memtable/SkipListMemTable.hpp"

#include <algorithm>
#include <cstdio>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace {
    using akkaradb::core::RecordView;
    using akkaradb::engine::memtable::MemTable;

    void require(bool condition, const char* message) {
        if (!condition) { throw std::runtime_error(message); }
    }

    [[nodiscard]] std::span<const uint8_t> bytes(const std::vector<uint8_t>& value) noexcept {
        return {value.data(), value.size()};
    }

    void verifyOutOfOrderApplyUsesApplyOrder(
        std::string_view backendName,
        MemTable::MemTableFactory factory
    ) {
        MemTable::Options options;
        options.shardCount = 1;
        options.flushMode = akkaradb::engine::memtable::MemTableFlushMode::MANUAL_ONLY;
        options.thresholdBytesPerShard = 0;
        options.backendFactory = std::move(factory);
        auto table = MemTable::create(options);

        const std::vector<uint8_t> key{'s', 'a', 'm', 'e', '-', 'k', 'e', 'y'};
        const std::vector<uint8_t> seqOneValue{'o', 'n', 'e'};
        const std::vector<uint8_t> seqTwoValue{'t', 'w', 'o'};

        // Parallel admission reserves sequences before taking the shard lock. A
        // delayed seq=1 writer can therefore apply after seq=2 for the same key.
        // The current concurrent-write contract exposes that later apply order.
        table->put(bytes(key), bytes(seqTwoValue), 2);
        table->put(bytes(key), bytes(seqOneValue), 1);

        RecordView record;
        require(table->get(bytes(key), 2, &record), "snapshot must find a visible record");
        require(
            record.seq() == 1 && std::ranges::equal(record.value(), bytes(seqOneValue)),
            (std::string{"parallel apply-order contract changed in "} + std::string{backendName}).c_str()
        );
    }

    void verifyKeySequenceConfiguration() {
        using EngineOptions = akkaradb::engine::AkkEngineOptions;
        EngineOptions options;
        options.components.walEnabled = false;
        options.components.blobEnabled = false;
        options.components.manifestEnabled = false;
        options.components.sstEnabled = false;
        options.components.versionLogEnabled = false;
        options.runtime.forceFlushOnClose = false;
        options.runtime.forceSyncOnClose = false;
        options.runtime.writeAdmission = EngineOptions::WriteAdmissionMode::PARALLEL;
        options.runtime.parallelWriteOrder = EngineOptions::ParallelWriteOrderMode::KEY_SEQUENCE;
        options.runtime.sequence.allocation = EngineOptions::SequenceAllocationMode::GLOBAL_ATOMIC;
        options.memtable.flushMode = akkaradb::engine::memtable::MemTableFlushMode::MANUAL_ONLY;
        options.memtable.thresholdBytesPerShard = 0;

        auto engine = akkaradb::engine::AkkEngine::open(options);
        const std::vector<uint8_t> key{'o', 'r', 'd', 'e', 'r'};
        const std::vector<uint8_t> value{'v'};
        engine->put(bytes(key), bytes(value));
        const auto observed = engine->get(bytes(key));
        require(observed.has_value() && *observed == value, "KEY_SEQUENCE write/read path must remain usable");

        // A failed write reserves its sequence before the MemTable rejects the
        // oversized value. Reads must report that terminal failure, rather
        // than waiting forever for the now-unfillable commit prefix.
        const std::vector<uint8_t> oversizedValue(65'536, 0xA5);
        bool writeRejected = false;
        try { engine->put(bytes(key), bytes(oversizedValue)); }
        catch (const std::runtime_error&) { writeRejected = true; }
        require(writeRejected, "oversized KEY_SEQUENCE write must be rejected");
        bool readRejected = false;
        try { (void)engine->get(bytes(key)); }
        catch (const std::runtime_error&) { readRejected = true; }
        require(readRejected, "KEY_SEQUENCE read must fail after a reserved-sequence write failure");
        engine->close();

        options.runtime.sequence.allocation = EngineOptions::SequenceAllocationMode::THREAD_LOCAL_RANGES;
        options.runtime.sequence.threadLocalRangeSize = 2;
        bool rejected = false;
        try { (void)akkaradb::engine::AkkEngine::open(options); }
        catch (const std::invalid_argument&) { rejected = true; }
        require(rejected, "KEY_SEQUENCE must reject THREAD_LOCAL_RANGES allocation");

        options.runtime.sequence.allocation = EngineOptions::SequenceAllocationMode::GLOBAL_ATOMIC;
        options.runtime.visibility.readVisibility = EngineOptions::ReadVisibilityMode::APPLIED;
        rejected = false;
        try { (void)akkaradb::engine::AkkEngine::open(options); }
        catch (const std::invalid_argument&) { rejected = true; }
        require(rejected, "KEY_SEQUENCE must reject APPLIED read visibility");
    }

    void verifySstDisabledFlushRetainsMemtable() {
        using EngineOptions = akkaradb::engine::AkkEngineOptions;
        EngineOptions options;
        options.components.walEnabled = false;
        options.components.blobEnabled = false;
        options.components.manifestEnabled = false;
        options.components.sstEnabled = false;
        options.components.versionLogEnabled = false;
        options.runtime.forceFlushOnClose = false;
        options.runtime.forceSyncOnClose = false;
        options.memtable.flushMode = akkaradb::engine::memtable::MemTableFlushMode::MANUAL_ONLY;

        auto engine = akkaradb::engine::AkkEngine::open(options);
        const std::vector<uint8_t> key{'n', 'o', '-', 's', 's', 't'};
        const std::vector<uint8_t> value{'v'};
        engine->put(bytes(key), bytes(value));
        engine->forceFlush();
        const auto observed = engine->get(bytes(key));
        require(observed.has_value() && *observed == value, "forceFlush without SST must retain MemTable data");
        engine->close();
    }

    void verifyEnginePassesBackendOptionsAtInitialization() {
        using EngineOptions = akkaradb::engine::AkkEngineOptions;
        using akkaradb::engine::memtable::BPTreeConcurrencyMode;
        using akkaradb::engine::memtable::BPTreeIteratorMode;
        using akkaradb::engine::memtable::BPTreeMemTable;
        using akkaradb::engine::memtable::MutableScanMode;

        EngineOptions options;
        options.components.walEnabled = false;
        options.components.blobEnabled = false;
        options.components.manifestEnabled = false;
        options.components.sstEnabled = false;
        options.components.versionLogEnabled = false;
        options.runtime.forceFlushOnClose = false;
        options.runtime.forceSyncOnClose = false;
        options.runtime.scanConsistency = EngineOptions::ScanConsistencyMode::PINNED_SNAPSHOT;
        options.memtable.flushMode = akkaradb::engine::memtable::MemTableFlushMode::MANUAL_ONLY;
        options.memtable.backendOptions.mutableScanMode = MutableScanMode::STREAMING_RESTART;
        options.memtable.backendOptions.bptreeConcurrencyMode = BPTreeConcurrencyMode::LOCKED;
        options.memtable.backendOptions.bptreeIteratorMode = BPTreeIteratorMode::MATERIALIZE_BATCHED_UNPINNED;
        options.memtable.backendOptions.bptreeIteratorBatchSize = 8;

        bool configured = false;
        options.memtable.backendFactoryWithOptions = [&configured](const auto& backendOptions) {
            require(
                backendOptions.mutableScanMode == MutableScanMode::STREAMING_RESTART,
                "AkkEngine must pass MemTable backend options during initialization"
            );
            require(
                backendOptions.bptreeConcurrencyMode == BPTreeConcurrencyMode::LOCKED,
                "AkkEngine must pass BPTree concurrency mode during initialization"
            );
            require(
                backendOptions.bptreeIteratorMode == BPTreeIteratorMode::MATERIALIZE_BATCHED_UNPINNED,
                "AkkEngine must pass BPTree iterator mode during initialization"
            );
            require(
                backendOptions.bptreeIteratorBatchSize == 8,
                "AkkEngine must pass BPTree iterator batch size during initialization"
            );
            configured = true;
            return std::make_unique<BPTreeMemTable>(
                akkaradb::core::BufferArena::DEFAULT_INITIAL_BLOCK_SIZE,
                akkaradb::core::BufferArena::DEFAULT_MAX_BLOCK_SIZE,
                64 * 1024,
                2 * 1024 * 1024,
                backendOptions
            );
        };

        auto engine = akkaradb::engine::AkkEngine::open(options);
        require(configured, "configured MemTable backend factory must run at engine open");
        const std::vector<uint8_t> engineKey{'p', 'i', 'n', 'n', 'e', 'd'};
        const std::vector<uint8_t> engineValue{'v'};
        engine->put(bytes(engineKey), bytes(engineValue));
        akkaradb::core::BufferArena scanArena;
        size_t engineScanRecords = 0;
        for (const auto& record : engine->scan(scanArena, {}, {})) {
            require(
                std::ranges::equal(record.key, bytes(engineKey)) && std::ranges::equal(record.value, bytes(engineValue)),
                "pinned engine scan must return the MemTable record"
            );
            ++engineScanRecords;
        }
        require(engineScanRecords == 1, "pinned engine scan must complete");
        require(engine->count({}, {}) == 1, "pinned engine count must complete");
        engine->close();

        BPTreeMemTable table(
            akkaradb::core::BufferArena::DEFAULT_INITIAL_BLOCK_SIZE,
            akkaradb::core::BufferArena::DEFAULT_MAX_BLOCK_SIZE,
            64 * 1024,
            2 * 1024 * 1024,
            options.memtable.backendOptions
        );
        std::vector<std::vector<uint8_t>> keys;
        keys.reserve(96);
        for (uint64_t i = 0; i < 96; ++i) {
            const std::string text = "key-" + std::to_string(1'000 + i);
            keys.emplace_back(text.begin(), text.end());
        }
        for (uint64_t i = 0; i < keys.size(); ++i) {
            const std::vector<uint8_t> value{static_cast<uint8_t>(i)};
            require(
                table.put(std::as_bytes(bytes(keys[(i * 37) % keys.size()])), std::as_bytes(bytes(value)), i + 1, 0).ok(),
                "BPTree streaming-scan setup insert must succeed"
            );
        }
        size_t expected = 0;
        for (const RecordView& record : table.iterator({}, {}, 96)) {
            require(std::ranges::equal(record.key(), bytes(keys[expected])), "BPTree streaming scan must remain ordered");
            ++expected;
        }
        require(expected == keys.size(), "BPTree streaming scan must return every key");
    }

    void verifySkipListSnapshotRangeAndFreeze() {
        using akkaradb::engine::memtable::SkipListMemTable;
        SkipListMemTable table;
        std::vector<std::vector<uint8_t>> keys;
        std::vector<uint64_t> initialSeq;
        std::vector<uint8_t> initialValue;
        keys.reserve(128);
        initialSeq.resize(128);
        initialValue.resize(128);

        for (uint64_t i = 0; i < 128; ++i) {
            const std::string text = "key-" + std::to_string(1'000 + i);
            keys.emplace_back(text.begin(), text.end());
        }
        for (uint64_t i = 0; i < keys.size(); ++i) {
            const size_t keyIndex = static_cast<size_t>((i * 37) % keys.size());
            const std::vector<uint8_t> value{static_cast<uint8_t>(i)};
            const auto status = table.put(std::as_bytes(bytes(keys[keyIndex])), std::as_bytes(bytes(value)), i + 1, 0);
            require(status.ok(), "SkipList insert must succeed");
            initialSeq[keyIndex] = i + 1;
            initialValue[keyIndex] = static_cast<uint8_t>(i);
        }

        const std::vector<uint8_t> updated{'n', 'e', 'w'};
        require(table.put(std::as_bytes(bytes(keys[64])), std::as_bytes(bytes(updated)), 129, 0).ok(), "SkipList update must succeed");
        RecordView record;
        require(table.get(std::as_bytes(bytes(keys[64])), initialSeq[64], &record), "SkipList historical snapshot must find prior version");
        require(
            record.seq() == initialSeq[64] && record.value().size() == 1 && record.value()[0] == initialValue[64],
            "SkipList historical snapshot must retain the prior value"
        );

        table.freeze();
        require(!table.put(std::as_bytes(bytes(keys[64])), std::as_bytes(bytes(updated)), 130, 0).ok(), "frozen SkipList must reject writes");
        size_t expectedKey = 30;
        for (const RecordView& current : table.iterator(std::as_bytes(bytes(keys[30])), std::as_bytes(bytes(keys[90])), 128)) {
            require(std::ranges::equal(current.key(), bytes(keys[expectedKey])), "SkipList range iteration must remain ordered after freeze");
            ++expectedKey;
        }
        require(expectedKey == 90, "SkipList range iteration must honor its upper bound");
    }

    void verifyConfiguredVersionRetention(
        std::string_view backendName,
        MemTable::ConfiguredMemTableFactory backendFactory
    ) {
        MemTable::Options retainedOptions;
        retainedOptions.shardCount = 1;
        retainedOptions.flushMode = akkaradb::engine::memtable::MemTableFlushMode::MANUAL_ONLY;
        retainedOptions.thresholdBytesPerShard = 0;
        retainedOptions.backendOptions.maxVersionsPerKey = 8;
        retainedOptions.backendFactoryWithOptions = backendFactory;
        auto retained = MemTable::create(retainedOptions);

        const std::vector<uint8_t> key{'v', 'e', 'r', 's', 'i', 'o', 'n', 'e', 'd'};
        for (uint64_t seq = 1; seq <= 6; ++seq) {
            const std::vector<uint8_t> value{static_cast<uint8_t>('0' + seq)};
            retained->put(bytes(key), bytes(value), seq);
        }

        RecordView oldRecord;
        require(
            retained->get(bytes(key), 1, &oldRecord),
            (std::string{"configured "} + std::string{backendName} + " retention must keep older same-key versions").c_str()
        );
        require(oldRecord.seq() == 1 && oldRecord.value().size() == 1 && oldRecord.value()[0] == '1', "configured old version mismatch");

        MemTable::Options shortOptions = retainedOptions;
        shortOptions.backendOptions.maxVersionsPerKey = 2;
        auto shortRetained = MemTable::create(shortOptions);
        for (uint64_t seq = 1; seq <= 3; ++seq) {
            const std::vector<uint8_t> value{static_cast<uint8_t>('a' + seq)};
            shortRetained->put(bytes(key), bytes(value), seq);
        }
        require(
            !shortRetained->get(bytes(key), 1, &oldRecord),
            (std::string{"small "} + std::string{backendName} + " retention must evict versions beyond its capacity").c_str()
        );
    }

    [[nodiscard]] std::vector<uint8_t> fixedKey(size_t value) {
        std::string text = "key-";
        text += std::to_string(1'000'000 + value);
        return {text.begin(), text.end()};
    }

    void verifyBPTreeDeepSplitPointAndRange() {
        using akkaradb::engine::memtable::BPTreeMemTable;
        BPTreeMemTable table;
        constexpr size_t keyCount = 4096;
        std::vector<std::vector<uint8_t>> keys;
        keys.reserve(keyCount);
        for (size_t i = 0; i < keyCount; ++i) { keys.push_back(fixedKey(i)); }

        for (size_t i = 0; i < keyCount; ++i) {
            const size_t keyIndex = (i * 997) % keyCount;
            const std::vector<uint8_t> value{static_cast<uint8_t>(keyIndex & 0xff)};
            require(
                table.put(std::as_bytes(bytes(keys[keyIndex])), std::as_bytes(bytes(value)), i + 1, 0).ok(),
                "BPTree deep split insert must succeed"
            );
        }

        for (size_t i = 0; i < keyCount; ++i) {
            RecordView found;
            require(table.get(std::as_bytes(bytes(keys[i])), keyCount, &found), "BPTree deep split point lookup must find every key");
            require(std::ranges::equal(found.key(), bytes(keys[i])), "BPTree deep split point lookup returned the wrong key");
        }

        size_t expected = 0;
        for (const RecordView& current : table.iterator({}, {}, keyCount)) {
            require(std::ranges::equal(current.key(), bytes(keys[expected])), "BPTree deep split full scan must stay ordered");
            ++expected;
        }
        require(expected == keyCount, "BPTree deep split full scan must return every key");

        expected = 1000;
        for (const RecordView& current : table.iterator(std::as_bytes(bytes(keys[1000])), std::as_bytes(bytes(keys[1100])), keyCount)) {
            require(std::ranges::equal(current.key(), bytes(keys[expected])), "BPTree deep split range scan must stay ordered");
            ++expected;
        }
        require(expected == 1100, "BPTree deep split range scan must honor bounds");
    }
}

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();

    try {
        std::vector<std::string> failures;
        const auto run = [&](std::string_view name, MemTable::MemTableFactory factory) {
            try { verifyOutOfOrderApplyUsesApplyOrder(name, std::move(factory)); }
            catch (const std::exception& ex) { failures.emplace_back(ex.what()); }
        };
        run("SkipList", [] { return std::make_unique<akkaradb::engine::memtable::SkipListMemTable>(); });
        run("BPTree", [] { return std::make_unique<akkaradb::engine::memtable::BPTreeMemTable>(); });
        run("ART", [] { return std::make_unique<akkaradb::engine::memtable::ARTMemTable>(); });
        verifyKeySequenceConfiguration();
        verifySstDisabledFlushRetainsMemtable();
        verifyEnginePassesBackendOptionsAtInitialization();
        verifySkipListSnapshotRangeAndFreeze();
        verifyConfiguredVersionRetention("SkipList", [](const auto& backendOptions) {
            return std::make_unique<akkaradb::engine::memtable::SkipListMemTable>(
                akkaradb::core::BufferArena::DEFAULT_INITIAL_BLOCK_SIZE,
                akkaradb::core::BufferArena::DEFAULT_MAX_BLOCK_SIZE,
                64 * 1024,
                2 * 1024 * 1024,
                backendOptions
            );
        });
        verifyConfiguredVersionRetention("BPTree", [](const auto& backendOptions) {
            return std::make_unique<akkaradb::engine::memtable::BPTreeMemTable>(
                akkaradb::core::BufferArena::DEFAULT_INITIAL_BLOCK_SIZE,
                akkaradb::core::BufferArena::DEFAULT_MAX_BLOCK_SIZE,
                64 * 1024,
                2 * 1024 * 1024,
                backendOptions
            );
        });
        verifyBPTreeDeepSplitPointAndRange();

        if (!failures.empty()) {
            std::string message;
            for (const auto& failure : failures) {
                if (!message.empty()) { message += "; "; }
                message += failure;
            }
            throw std::runtime_error(message);
        }
        return 0;
    }
    catch (const std::exception& ex) {
        std::fprintf(stderr, "parallel memtable visibility smoke failed: %s\n", ex.what());
        return 1;
    }
}

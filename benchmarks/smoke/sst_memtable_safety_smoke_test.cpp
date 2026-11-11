#include "TestErrorHandlers.hpp"
#include "akk/engine/memtable/ARTMemTable.hpp"
#include "akk/engine/memtable/BPTreeMemTable.hpp"
#include "akk/engine/memtable/MemTable.hpp"
#include "akk/engine/memtable/SkipListMemTable.hpp"
#include "akk/engine/sstable/SSTManager.hpp"
#include "akk/core/record/OwnedRecord.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {
    namespace mt = akkaradb::engine::memtable;
    namespace sst = akkaradb::engine::sst;
    namespace fs = std::filesystem;
    using akkaradb::core::RecordView;

    void require(bool condition, const char* message) {
        if (!condition) { throw std::runtime_error(message); }
    }
    std::span<const uint8_t> bytes(std::string_view value) {
        return {reinterpret_cast<const uint8_t*>(value.data()), value.size()};
    }
    template <typename Predicate>
    void waitFor(Predicate predicate) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
        while (!predicate()) {
            require(std::chrono::steady_clock::now() < deadline, "MemTable maintenance timed out");
            std::this_thread::sleep_for(std::chrono::milliseconds{2});
        }
    }

    mt::MemTable::Options optionsFor(int backend) {
        mt::MemTable::Options options;
        options.shardCount = 1;
        options.flushMode = mt::MemTableFlushMode::MANUAL_ONLY;
        options.backendFactory = [backend]() -> std::unique_ptr<mt::IMemTable> {
            if (backend == 0) { return std::make_unique<mt::SkipListMemTable>(); }
            if (backend == 1) { return std::make_unique<mt::BPTreeMemTable>(); }
            return std::make_unique<mt::ARTMemTable>();
        };
        return options;
    }

    #include "detail/sst_memtable_review_regressions.inc"

    void testRetentionAndFlush(const fs::path& root, int backend, bool streaming) {
        std::atomic<uint64_t> frontier{1};
        auto options = optionsFor(backend);
        sst::SSTManager::Options diskOptions;
        diskOptions.compactionMode = sst::SSTCompactionMode::DISABLED;
        diskOptions.targetFileSize = 4096;
        auto disk = sst::SSTManager::create(root, diskOptions, nullptr, [&frontier] { return frontier.load(); });
        if (streaming) {
            options.flushInputMode = mt::MemTableFlushInputMode::STREAMING;
            options.onFlushStream = [&](size_t count, auto records) { disk->flush(count, std::move(records)); };
        }
        else { options.onFlush = [&](auto records) { disk->flush(records); }; }
        auto memory = mt::MemTable::create(options, [&frontier] { return frontier.load(); });
        memory->put(bytes("key"), bytes("old"), 1);
        for (uint64_t seq = 3; seq <= 9; ++seq) {
            if (seq == 5) { memory->remove(bytes("key"), seq); }
            else { memory->put(bytes("key"), bytes("new"), seq); }
        }
        // Cross B+Tree leaf splits and ART branches, with multiple SST outputs.
        for (int i = 0; i < 72; ++i) {
            const auto key = "aux-" + std::to_string(i);
            memory->put(bytes(key), bytes("base"), 1);
            memory->put(bytes(key), bytes("future"), 3);
        }
        const auto baseline = memory->getPinned(bytes("key"), 1);
        require(baseline && baseline->view.valueString() == "old", "unpublished writes evicted the visible baseline");
        auto scan = memory->iterator({}, 1);
        std::optional<RecordView> previous;
        size_t rows = 0;
        while (scan.hasNext()) {
            const auto record = scan.next();
            require(record && (!previous || previous->compareKey(*record) < 0), "retention changed ordinary ordered scan semantics");
            previous = record;
            ++rows;
        }
        require(rows == 73, "ordinary scan emitted physical versions instead of visible keys");
        memory->forceFlush();
        require(!memory->getPinned(bytes("key"), 1), "flush did not unpublish its immutable input");
        const auto flushedBaseline = disk->get(bytes("key"), 1);
        require(flushedBaseline && flushedBaseline->seq == 1, "flush discarded the visible baseline");
        for (uint64_t seq = 3; seq <= 9; ++seq) {
            const auto record = disk->get(bytes("key"), seq);
            require(record && record->seq == seq && record->isTombstone() == (seq == 5), "flush discarded an unpublished intermediate version");
        }
        for (int i = 0; i < 72; ++i) {
            const auto key = "aux-" + std::to_string(i);
            const auto oldValue = disk->get(bytes(key), 1), newValue = disk->get(bytes(key), 3);
            require(oldValue && oldValue->seq == 1 && newValue && newValue->seq == 3, "flush lost versions across structural splits");
        }
        memory.reset(); disk.reset();
        disk = sst::SSTManager::create(root, diskOptions);
        disk->recover();
        const auto recoveredBaseline = disk->get(bytes("key"), 1);
        const auto recoveredDeletion = disk->get(bytes("key"), 5);
        require(recoveredBaseline && recoveredBaseline->seq == 1 && recoveredDeletion && recoveredDeletion->isTombstone(),
            "retained versions did not survive recovery");
    }

    void testMemoryCompaction(int backend) {
        std::atomic<uint64_t> frontier{1};
        auto memory = mt::MemTable::create(optionsFor(backend), [&frontier] { return frontier.load(); });
        memory->put(bytes("key"), bytes("old"), 1);
        auto first = memory->sealAndPinMemoryIterator({}, 1, true, 0, 8);
        for (uint64_t seq = 3; seq <= 8; ++seq) { memory->put(bytes("key"), bytes("new"), seq); }
        auto second = memory->sealAndPinMemoryIterator({}, 1, true, 0, 8);
        first.reset(); second.reset();
        waitFor([&] { return memory->snapshot().memorySnapshotCompactionsCompleted != 0 && !memory->snapshot().memorySnapshotCompactionPending; });
        for (const uint64_t seq : {1ULL, 3ULL, 5ULL, 8ULL}) {
            const auto record = memory->getPinned(bytes("key"), seq);
            require(record && record->view.seq() == seq, "memory compaction discarded a still-visible version");
        }
        frontier.store(8);
        const auto before = memory->snapshot().memorySnapshotCompactionsCompleted;
        memory->put(bytes("key"), bytes("new"), 9);
        auto third = memory->sealAndPinMemoryIterator({}, 8, true, 0, 8);
        third.reset();
        waitFor([&] { return memory->snapshot().memorySnapshotCompactionsCompleted > before && !memory->snapshot().memorySnapshotCompactionPending; });
        require(!memory->getPinned(bytes("key"), 1), "advanced frontier did not reclaim obsolete memory history");
        require(memory->getPinned(bytes("key"), 8)->view.seq() == 8, "advanced frontier lost its baseline");
        // A surviving snapshot must not call the engine's frontier provider after
        // its MemTable wrapper (and potentially its engine) has been destroyed.
        auto survivor = memory->sealAndPinMemoryIterator({}, 8, true, 0, 8);
        memory.reset();
        survivor.reset();
    }

    void testPromotion(int backend) {
        std::atomic<uint64_t> frontier{2};
        auto memory = mt::MemTable::create(optionsFor(backend), [&frontier] { return frontier.load(); });
        memory->put(bytes("key"), bytes("old"), 1);
        const auto old = memory->getPinned(bytes("key"), 1);
        memory->put(bytes("key"), bytes("new"), 2);
        memory->promote(old->view);
        require(memory->getPinned(bytes("key"), 2)->view.valueString() == "new", "promotion overwrote an unflushed update");
        memory->remove(bytes("key"), 3);
        memory->promote(old->view);
        require(memory->getPinned(bytes("key"), 3)->view.isTombstone(), "promotion resurrected an unflushed deletion");
    }
}

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();
    try {
        const auto root = fs::current_path() / ".bench-tmp" / "sst-memtable-safety";
        fs::remove_all(root);
        testOwnedRecordSstLookup(root / "owned-record");
        testReadSurvivesMemoryCompaction();
        testFlushCallbackSelection();
        for (int backend = 0; backend < 3; ++backend) {
            testMemorySnapshotGrowthAdmission(backend);
            testSequenceOrderedFlush(root / ("sequence-bounded-" + std::to_string(backend)), backend, false);
            testSequenceOrderedFlush(root / ("sequence-retained-" + std::to_string(backend)), backend, true);
            testConcurrentSequenceOrder(backend, false);
            testConcurrentSequenceOrder(backend, true);
            for (const bool streaming : {false, true}) {
                testRetentionAndFlush(root / (std::to_string(backend) + (streaming ? "-stream" : "-vector")), backend, streaming);
                testBoundedRetentionFlush(root / ("bounded-" + std::to_string(backend) + (streaming ? "-stream" : "-vector")), backend, streaming);
            }
            testFlushReplacementFailure(backend, false);
            testFlushReplacementFailure(backend, true);
            testMemoryCompaction(backend);
            testPromotion(backend);
        }
        fs::remove_all(root);
        return 0;
    }
    catch (const std::exception& error) { std::fprintf(stderr, "SST MemTable safety: %s\n", error.what()); return 1; }
}

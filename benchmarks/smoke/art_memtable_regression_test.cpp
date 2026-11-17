/*
 * AkkaraDB
 * Copyright (C) 2026 Swift Storm Studio
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. See https://mozilla.org/MPL/2.0/.
 */
#include "TestErrorHandlers.hpp"
#include "akk/engine/memtable/ARTMemTable.hpp"

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <mutex>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#ifdef _WIN32
#include <malloc.h>
#endif

namespace {
    thread_local int failAlignedAllocationAfter = -1;
}

void* operator new(size_t size, std::align_val_t alignment) {
    if (failAlignedAllocationAfter == 0) {
        failAlignedAllocationAfter = -1;
        throw std::bad_alloc{};
    }
    if (failAlignedAllocationAfter > 0) { --failAlignedAllocationAfter; }
    const auto align = static_cast<size_t>(alignment);
    #ifdef _WIN32
    if (auto* memory = _aligned_malloc(size == 0 ? 1 : size, align)) { return memory; }
    #else
    void* memory = nullptr;
    if (posix_memalign(&memory, align, size == 0 ? 1 : size) == 0) { return memory; }
    #endif
    throw std::bad_alloc{};
}

void operator delete(void* memory, std::align_val_t) noexcept {
    #ifdef _WIN32
    _aligned_free(memory);
    #else
    std::free(memory);
    #endif
}

void operator delete(void* memory, size_t, std::align_val_t alignment) noexcept { ::operator delete(memory, alignment); }

namespace {
    using akkaradb::engine::memtable::ARTMemTable;
    using namespace akkaradb::core;

    void require(bool condition, const char* message) { if (!condition) { throw std::runtime_error(message); } }
    ByteView bytes(std::string_view value) { return {reinterpret_cast<const std::byte*>(value.data()), value.size()}; }

    std::string key(size_t index) {
        std::string result = "prefix-";
        result.push_back(static_cast<char>(index >> 8));
        result.push_back(static_cast<char>(index));
        return result + "-tail";
    }

    void put(ARTMemTable& tree, const std::string& k, uint64_t seq, std::string_view value = "value") {
        require(tree.put(bytes(k), bytes(value), seq, 0).ok(), "ART put failed");
    }

    void verifyAllocationGrowth() {
        constexpr size_t count = 4096;
        const std::string value(128, 'v');
        ARTMemTable tree;
        for (size_t i = 0; i < count; ++i) { put(tree, key((i * 997) % count), 1, value); }
        require(tree.sizeBytes() < count * 768, "ART insert still retains copies of the search path");
        const size_t before = tree.sizeBytes();
        for (size_t i = 0; i < count; ++i) { put(tree, key(i), 2, value); }
        const size_t recordBytes = sizeof(OwnedRecord) + key(0).size() + value.size();
        require(tree.sizeBytes() - before == count * recordBytes, "ART update allocates structural copies or counts record memory twice");
        require(tree.entryCount() == count * 2, "ART update count mismatch");

        ARTMemTable small;
        put(small, "k", 1);
        const size_t smallBefore = small.sizeBytes();
        for (uint64_t seq = 2; seq < 12; ++seq) { put(small, "k", seq); }
        require(small.sizeBytes() - smallBefore == 10 * sizeof(OwnedRecord), "ART counts inline payload twice");
        require(small.entryCount() == ARTMemTable::MAX_VERSIONS_PER_KEY, "ART bounded retention grew without bound");
    }

    using Expected = std::map<std::string, uint64_t>;

    void checkCursor(ArenaGenerator<RecordView>::iterator cursor, const Expected& expected) {
        for (const auto& [k, seq] : expected) {
            require(cursor != std::default_sentinel, "ART snapshot ended early");
            require(cursor->keyString() == k && cursor->seq() == seq, "ART write changed a pinned iterator");
            ++cursor;
        }
        require(cursor == std::default_sentinel, "ART snapshot included later inserted keys");
    }

    void verifySnapshots(bool retainAll, bool sequenceOrdered) {
        ARTMemTable tree;
        if (retainAll) { tree.retainAllVersions(); }
        if (sequenceOrdered) { tree.useSequenceOrder(); }
        Expected first;
        for (const auto& k : {std::string{}, key(0), key(32), key(128), key(255)}) {
            put(tree, k, 1);
            first[k] = 1;
        }
        auto firstRows = tree.iterator({}, {}, UINT64_MAX);
        auto firstCursor = firstRows.begin();
        Expected second = first;
        for (size_t i = 0; i < 256; ++i) {
            put(tree, key(i), 2);
            second[key(i)] = 2;
        }
        for (const auto& k : {std::string{"pre"}, std::string{"prefix-"}, std::string{"prefix-z"}, std::string{"z"}}) {
            put(tree, k, 2);
            second[k] = 2;
        }
        const size_t protectedBefore = tree.sizeBytes();
        put(tree, key(64), 2);
        require(
            tree.sizeBytes() - protectedBefore == sizeof(OwnedRecord) + (retainAll ? sizeof(akkaradb::engine::memtable::detail::VersionHistory::Entry) : 0),
            "ART repeatedly copies a path within one iterator epoch"
        );
        {
            auto secondRows = tree.iterator({}, {}, UINT64_MAX);
            auto secondCursor = secondRows.begin();
            for (uint64_t seq = 3; seq < 10; ++seq) { for (size_t i = 0; i < 256; ++i) { put(tree, key(i), seq); } }
            checkCursor(secondCursor, second);
        }
        put(tree, key(64), 10);
        checkCursor(firstCursor, first);
        // Completion/abandonment must release protection; future updates then
        // allocate only a record (plus one history link in unbounded mode).
        firstRows = {};
        {
            auto abandoned = tree.iterator(bytes("prefix-"), bytes("z"), UINT64_MAX);
            require(abandoned.begin() != std::default_sentinel, "ART bounded scan is empty");
        }
        put(tree, key(64), 11);
        const size_t before = tree.sizeBytes();
        put(tree, key(64), 12);
        const size_t expected = sizeof(OwnedRecord) + (retainAll ? sizeof(akkaradb::engine::memtable::detail::VersionHistory::Entry) : 0);
        require(tree.sizeBytes() - before == expected, "ART keeps copying after all iterators finish");

        RecordView row;
        require(tree.get(bytes(key(64)), 12, &row) && row.seq() == 12, "ART latest version mismatch");
        require(tree.get(bytes(key(64)), 10, &row) && row.seq() == 10, "ART retained snapshot lookup mismatch");
        tree.freeze();
        require(!tree.put(bytes("later"), bytes("v"), 13, 0).ok(), "frozen ART accepted a put");
        size_t flushed = 0;
        std::string previous;
        for (const auto& record : tree.flushIterator()) {
            require(previous <= record.keyString(), "ART flush order changed");
            previous = record.keyString();
            ++flushed;
        }
        require(flushed == tree.entryCount(), "ART flush lost retained versions");
    }

    bool exerciseFailure(size_t count, int operation, bool pin, int failAt) {
        ARTMemTable tree{64, 64};
        tree.retainAllVersions();
        Expected original;
        for (size_t i = 0; i < count; ++i) {
            put(tree, key(i), 1);
            original[key(i)] = 1;
        }
        std::optional<ArenaGenerator<RecordView>> snapshot;
        ArenaGenerator<RecordView>::iterator cursor;
        if (pin) {
            snapshot.emplace(tree.iterator({}, {}, UINT64_MAX));
            cursor = snapshot->begin();
        }
        const std::string incoming = operation == 0 ? key(count) : (operation == 1 ? "pre" : key(0));
        bool failed = false;
        failAlignedAllocationAfter = failAt;
        try { put(tree, incoming, 2, std::string(128, 'n')); }
        catch (const std::bad_alloc&) { failed = true; }
        failAlignedAllocationAfter = -1;
        require(tree.entryCount() == count + size_t{!failed}, "failed ART insertion changed logical count");
        RecordView row;
        for (const auto& [k, seq] : original) { require(tree.get(bytes(k), seq, &row) && row.seq() == seq, "ART allocation failure lost an existing key"); }
        const bool found = tree.get(bytes(incoming), UINT64_MAX, &row);
        require(
            failed ? (found == original.contains(incoming) && (!found || row.seq() == 1)) : (found && row.seq() == 2),
            "ART allocation failure published a partial update"
        );
        if (pin) { checkCursor(cursor, original); }
        if (failed) { put(tree, incoming, 2); }
        tree.freeze();
        size_t flushed = 0;
        for (const auto& record : tree.flushIterator()) {
            (void)record;
            ++flushed;
        }
        require(flushed == count + 1, "ART allocation failure damaged retained history");
        return failed;
    }

    void verifyAllocationFailures() {
        for (size_t count : {size_t{0}, size_t{1}, size_t{4}, size_t{16}, size_t{48}, size_t{255}}) {
            for (int operation = 0; operation < 3; ++operation) {
                for (bool pin : {false, true}) {
                    bool failed = false, succeeded = false;
                    for (int failAt = 0; failAt < 32; ++failAt) {
                        if (exerciseFailure(count, operation, pin, failAt)) { failed = true; }
                        else {
                            succeeded = true;
                            break;
                        }
                    }
                    require(failed && succeeded, "ART failure sweep did not reach failure and success");
                }
            }
        }
    }

    void verifyConcurrentReaders() {
        constexpr uint64_t count = 2048;
        ARTMemTable tree;
        std::atomic<uint64_t> published{0};
        std::atomic<unsigned> ready{0};
        std::atomic<bool> done{false}, failed{false};
        std::mutex errorMutex;
        std::string error;
        const auto guarded = [&](auto work) {
            try { work(); }
            catch (const std::exception& ex) {
                std::lock_guard lock{errorMutex};
                error = ex.what();
                failed.store(true);
            }
        };
        std::vector<std::thread> readers;
        for (unsigned id = 0; id < 3; ++id) {
            readers.emplace_back(
                [&, id] {
                    guarded(
                        [&] {
                            ready.fetch_add(1);
                            do {
                                const uint64_t cut = published.load(std::memory_order_acquire);
                                if (cut == 0) {
                                    std::this_thread::yield();
                                    continue;
                                }
                                if (id == 0) {
                                    size_t seen = 0;
                                    for (const auto& row : tree.iterator({}, {}, cut)) {
                                        require(row.keyString() == key(seen++) && row.seq() <= cut, "concurrent ART scan is inconsistent");
                                    }
                                    require(seen == cut, "concurrent ART scan lost a committed key");
                                }
                                else {
                                    RecordView row;
                                    const auto k = key((cut - 1) / id);
                                    require(tree.get(bytes(k), cut, &row) && row.keyString() == k, "concurrent ART get missed a key");
                                }
                                std::this_thread::yield();
                            }
                            while (!done.load(std::memory_order_acquire) && !failed.load());
                        }
                    );
                }
            );
        }
        while (ready.load() != 3) { std::this_thread::yield(); }
        guarded(
            [&] {
                for (uint64_t seq = 1; seq <= count && !failed.load(); ++seq) {
                    put(tree, key(seq - 1), seq);
                    published.store(seq, std::memory_order_release);
                    if ((seq & 15) == 0) { std::this_thread::yield(); }
                }
            }
        );
        done.store(true, std::memory_order_release);
        for (auto& reader : readers) { reader.join(); }
        require(!failed.load(), error.c_str());
        require(tree.entryCount() == count, "concurrent ART writer did not complete");
    }
}

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();
    try {
        verifyAllocationGrowth();
        for (bool retainAll : {false, true}) { for (bool sequenceOrdered : {false, true}) { verifySnapshots(retainAll, sequenceOrdered); } }
        verifyAllocationFailures();
        verifyConcurrentReaders();
        std::puts("ART memtable regression: passed");
        return 0;
    }
    catch (const std::exception& error) {
        failAlignedAllocationAfter = -1;
        std::fprintf(stderr, "ART memtable regression: %s\n", error.what());
        return 1;
    }
}

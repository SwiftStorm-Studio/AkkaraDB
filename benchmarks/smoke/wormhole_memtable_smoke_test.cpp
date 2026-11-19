/*
 * AkkaraDB
 * Copyright (C) 2026 Swift Storm Studio
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. See https://mozilla.org/MPL/2.0/.
 */
#include "TestErrorHandlers.hpp"
#include "akk/engine/memtable/WormholeMemTable.hpp"
#include "akk/core/record/OwnedRecord.hpp"
#include "akk/engine/memtable/detail/WormholeIndex.hpp"

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <map>
#include <new>
#include <numeric>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#ifdef _WIN32
#include <malloc.h>
#endif

namespace {
    thread_local int failAfter = -1;
    thread_local bool traceAllocations = false;
    thread_local size_t allocatedBytes = 0, largestAllocation = 0;
    void traceAllocation(size_t size) {
        if (traceAllocations) {
            allocatedBytes += size;
            largestAllocation = std::max(largestAllocation, size);
        }
    }
    void checkAllocation() {
        if (failAfter == 0) { failAfter = -1; throw std::bad_alloc{}; }
        if (failAfter > 0) { --failAfter; }
    }
}
void* operator new(size_t size) {
    traceAllocation(size);
    checkAllocation();
    if (void* result = std::malloc(size == 0 ? 1 : size)) { return result; }
    throw std::bad_alloc{};
}
void operator delete(void* memory) noexcept { std::free(memory); }
void operator delete(void* memory, size_t) noexcept { std::free(memory); }
void* operator new[](size_t size) { return ::operator new(size); }
void operator delete[](void* memory) noexcept { ::operator delete(memory); }
void operator delete[](void* memory, size_t) noexcept { ::operator delete(memory); }
void* operator new(size_t size, std::align_val_t alignment) {
    traceAllocation(size);
    checkAllocation();
    #ifdef _WIN32
    if (void* memory = _aligned_malloc(size == 0 ? 1 : size, static_cast<size_t>(alignment))) { return memory; }
    #else
    void* memory = nullptr;
    if (posix_memalign(&memory, static_cast<size_t>(alignment), size == 0 ? 1 : size) == 0) { return memory; }
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
    using namespace akkaradb::core;
    using akkaradb::engine::memtable::WormholeMemTable;
    using akkaradb::engine::memtable::MemTableBackendOptions;
    void require(bool value, const char* message) { if (!value) { throw std::runtime_error(message); } }
    ByteView bytes(std::string_view value) { return {reinterpret_cast<const std::byte*>(value.data()), value.size()}; }
    std::string key(uint64_t value) {
        std::string result = "key/";
        for (size_t i = 0; i < 8; ++i) { result.push_back(static_cast<char>(value >> (56 - i * 8))); }
        return result;
    }
    void put(WormholeMemTable& table, const std::string& k, uint64_t seq, std::string_view value = "value", uint8_t flags = 0) {
        require(table.put(bytes(k), bytes(value), seq, flags).ok(), "Wormhole put failed");
    }
    struct Version { uint64_t seq; std::string value; uint8_t flags; };
    using Model = std::map<std::string, std::vector<Version>>;
    void modelPut(Model& model, const std::string& k, Version version, bool sequence, bool all, size_t capacity = 4) {
        auto& list = model[k];
        if (sequence && !all) {
            const auto found = std::find_if(list.begin(), list.end(), [&](const Version& v) { return v.seq == version.seq; });
            if (found == list.end()) { list.push_back(std::move(version)); }
            else { *found = std::move(version); }
            std::sort(list.begin(), list.end(), [](const Version& a, const Version& b) { return a.seq > b.seq; });
        }
        else { list.insert(list.begin(), std::move(version)); }
        if (!all && list.size() > capacity) { list.resize(capacity); }
    }
    const Version* visible(const std::vector<Version>& list, uint64_t seq, bool sequence) {
        const Version* best = nullptr;
        for (const auto& v : list) {
            if (v.seq <= seq && (!best || (sequence && v.seq > best->seq))) {
                best = &v;
                if (!sequence) { break; }
            }
        }
        return best;
    }
    void checkRecord(const RecordView& row, const std::string& k, const Version& v) {
        require(row.keyString() == k && row.seq() == v.seq && row.flags() == v.flags && row.valueString() == v.value,
            "Wormhole record differs from oracle");
    }
    void checkRange(const WormholeMemTable& table, const Model& model, const std::string& start,
        const std::string& end, uint64_t cut, bool sequence) {
        auto rows = table.iterator(bytes(start), bytes(end), cut);
        auto cursor = rows.begin();
        for (const auto& [k, list] : model) {
            if ((!start.empty() && k < start) || (!end.empty() && k >= end)) { continue; }
            if (const auto* v = visible(list, cut, sequence)) {
                require(cursor != std::default_sentinel, "Wormhole range ended early");
                checkRecord(*cursor, k, *v);
                ++cursor;
            }
        }
        require(cursor == std::default_sentinel, "Wormhole range crossed a boundary");
    }
    void testModel(bool sequence, bool all) {
        WormholeMemTable table;
        if (sequence) { table.useSequenceOrder(); }
        if (all) { table.retainAllVersions(); }
        bool rejected = false;
        try { auto rows = table.flushIterator(); (void)rows; } catch (const std::logic_error&) { rejected = true; }
        require(rejected, "Wormhole flush accepted mutable input");
        std::vector<std::string> keys{"", "a", std::string{"a\0", 2}, std::string{"a\0\0", 3},
            std::string{"\0", 1}, std::string{"\xff", 1}, "prefix", "prefix/child"};
        for (size_t i = 0; i < 2048; ++i) { keys.push_back(key(i)); }
        std::mt19937 random{0xA44A5EED};
        std::shuffle(keys.begin(), keys.end(), random);
        Model model;
        for (const auto& k : keys) {
            for (uint64_t seq : {7, 2, 9, 4, 6, 9}) {
                const std::string value = std::to_string(seq) + "/" + std::to_string(model[k].size());
                const uint8_t flags = seq == 4 ? RecordView::FLAG_TOMBSTONE : 0;
                put(table, k, seq, value, flags);
                modelPut(model, k, {seq, value, flags}, sequence, all);
            }
        }
        size_t count = 0;
        for (const auto& [k, list] : model) {
            count += list.size();
            for (uint64_t cut : {uint64_t{0}, uint64_t{2}, uint64_t{5}, uint64_t{8}, UINT64_MAX}) {
                RecordView row;
                const auto* v = visible(list, cut, sequence);
                require(table.get(bytes(k), cut, &row) == (v != nullptr), "Wormhole point visibility differs");
                if (v) { checkRecord(row, k, *v); }
            }
            RecordView missing;
            require(!table.get(bytes(k + "missing"), UINT64_MAX, &missing), "Wormhole miss hit a different key");
        }
        require(table.entryCount() == count, "Wormhole counts keys instead of retained records");
        checkRange(table, model, {}, {}, 0, sequence);
        checkRange(table, model, {}, {}, UINT64_MAX, sequence);
        for (size_t i = 0; i < 400; ++i) {
            std::string a = keys[random() % keys.size()], b = keys[random() % keys.size()];
            if (i % 3 == 0) { a += static_cast<char>(0x7f); }
            for (uint64_t cut : {uint64_t{3}, uint64_t{8}, UINT64_MAX}) { checkRange(table, model, a, b, cut, sequence); }
        }
        auto snapshot = table.iterator({}, {}, UINT64_MAX);
        auto cursor = snapshot.begin();
        std::vector<std::pair<std::string, Version>> captured;
        for (const auto& [k, list] : model) { if (const auto* v = visible(list, UINT64_MAX, sequence)) { captured.emplace_back(k, *v); } }
        for (const auto& k : keys) {
            for (uint64_t seq = 10; seq < 17; ++seq) {
                put(table, k, seq, "later");
                modelPut(model, k, {seq, "later", 0}, sequence, all);
            }
        }
        put(table, "brand-new-key", 17, "new");
        modelPut(model, "brand-new-key", {17, "new", 0}, sequence, all);
        // A suspended cursor owns the visible records and no writer lock. It
        // can resume serially on a different thread after ring overwrites.
        std::exception_ptr error;
        std::thread resume{[&] {
            try {
                for (const auto& [k, v] : captured) {
                    require(cursor != std::default_sentinel, "Wormhole snapshot ended early");
                    checkRecord(*cursor, k, v);
                    ++cursor;
                }
                require(cursor == std::default_sentinel, "Wormhole snapshot included later keys");
            } catch (...) { error = std::current_exception(); }
        }};
        resume.join();
        if (error) { std::rethrow_exception(error); }
        table.freeze();
        require(!table.put(bytes("late"), bytes("value"), 100, 0).ok(), "Wormhole accepted writes after freeze");
        checkRange(table, model, key(90), key(180), 12, sequence);
        auto flush = table.flushIterator();
        auto it = flush.begin();
        count = 0;
        for (const auto& [k, list] : model) {
            for (const auto& v : list) {
                require(it != std::default_sentinel, "Wormhole flush dropped a retained version");
                checkRecord(*it, k, v);
                ++it;
                ++count;
            }
        }
        require(it == std::default_sentinel && count == table.entryCount(), "Wormhole flush/count mismatch");
    }
    void testBinaryAnchors() {
        WormholeMemTable table;
        Model model;
        const auto add = [&](const std::string& k, std::string_view value = "value") {
            put(table, k, 1, value);
            modelPut(model, k, {1, std::string{value}, 0}, false, false);
        };
        for (size_t i = 1; i <= 512; ++i) { add(std::string(i, 'a')); }
        for (size_t i = 1; i <= 260; ++i) { add("binary" + std::string(i, '\0')); }
        const std::string collisionPrefix = std::string{"\xef\xbe\xad\xde", 4} + std::string(8, '\0');
        for (size_t i = 0; i < 129; ++i) { add(collisionPrefix + static_cast<char>(i)); }
        const std::string longPrefix(65520, '\0');
        for (size_t i = 0; i < 129; ++i) { add(longPrefix + static_cast<char>(i)); }
        add(std::string(65535, 'z'), {});
        for (const auto& [k, list] : model) {
            RecordView row;
            require(table.get(bytes(k), 1, &row), "Wormhole anchor lookup missed key");
            checkRecord(row, k, list.front());
        }
        for (size_t i : {1, 63, 64, 65, 127, 128, 129, 511}) {
            checkRange(table, model, std::string(i, 'a'), std::string(i + 1, 'a'), 1, false);
            checkRange(table, model, std::string(i, 'a') + '\0', std::string(i + 2, 'a'), 1, false);
        }
        checkRange(table, model, longPrefix, longPrefix + static_cast<char>(0x7f), 1, false);
        checkRange(table, model, "binary" + std::string(64, '\0'), "binary" + std::string(193, '\0'), 1, false);
        table.freeze();
        checkRange(table, model, {}, {}, 1, false);
    }
    void testConfigurationAndAccounting() {
        {
            WormholeMemTable table;
            const std::string oversized(65535, 'x');
            require(!table.put(bytes(oversized), bytes("v"), 1, 0).ok() && table.entryCount() == 0,
                "Wormhole accepted an oversized combined payload");
        }
        for (size_t capacity : {size_t{1}, size_t{4}, size_t{9}}) {
            MemTableBackendOptions options;
            options.maxVersionsPerKey = capacity;
            WormholeMemTable table{65536, 4194304, 65536, 2097152, options};
            table.useSequenceOrder();
            for (uint64_t seq = 1; seq <= 20; ++seq) { put(table, "k", seq, "v"); }
            require(table.entryCount() == capacity, "Wormhole ignored configured retention");
            const auto before = table.sizeBytes();
            put(table, "k", 21, "v");
            require(table.sizeBytes() - before == sizeof(OwnedRecord), "Wormhole double-counted inline payload or allocated index copies on update");
        }
        for (size_t invalid : {size_t{0}, size_t{65536}}) {
            bool rejected = false;
            try { MemTableBackendOptions options; options.maxVersionsPerKey = invalid; WormholeMemTable table{64, 128, 64, 128, options}; }
            catch (const std::invalid_argument&) { rejected = true; }
            require(rejected, "Wormhole accepted invalid retention");
        }
    }
    void testAllocationFailures() {
        for (size_t scenario = 0; scenario < 8; ++scenario) {
            const size_t count = std::array<size_t, 4>{0, 127, 128, 512}[scenario % 4];
            const bool all = scenario >= 4;
            bool failed = false, succeeded = false;
            for (int fail = 0; fail < 160; ++fail) {
                WormholeMemTable table{64, 128};
                if (all) { table.retainAllVersions(); }
                for (size_t i = 0; i < count; ++i) { put(table, key(i), 1); }
                failAfter = fail;
                bool didFail = false;
                try { put(table, key(10000), 2); } catch (const std::bad_alloc&) { didFail = true; }
                failAfter = -1;
                failed |= didFail;
                require(table.entryCount() == count + static_cast<size_t>(!didFail), "Wormhole failed insert changed record count");
                for (size_t i = 0; i < count; ++i) {
                    RecordView row;
                    require(table.get(bytes(key(i)), 1, &row) && row.seq() == 1, "Wormhole split failure lost an old key");
                }
                RecordView inserted;
                require(table.get(bytes(key(10000)), UINT64_MAX, &inserted) == !didFail, "Wormhole failed insert was partly published");
                if (!didFail) { succeeded = true; break; }
                put(table, key(10000), 2);
                require(table.get(bytes(key(10000)), UINT64_MAX, &inserted), "Wormhole could not retry failed insertion");
            }
            require(failed && succeeded, "Wormhole allocation sweep missed failure or success");
        }
    }
    void testMetadataCollisions() {
        using namespace akkaradb::engine::memtable::detail::wormhole;
        struct Meta { PrefixKey key{}; };
        std::array<std::array<uint8_t, 2>, 80> keys{};
        std::array<Meta, 80> metadata{};
        TaggedDirectory<Meta> directory;
        // Identical full hashes spill into multiple groups. Neither growth nor
        // tag equality may substitute for comparing the complete binary prefix.
        constexpr uint64_t collisionHash = 0x4321000000000003ULL;
        directory.reserve(metadata.size());
        for (size_t i = 0; i < metadata.size(); ++i) {
            keys[i] = {0, static_cast<uint8_t>(i)};
            metadata[i].key = {keys[i].data(), collisionHash, 2, keys[i][1]};
            directory.insert(&metadata[i]);
        }
        const auto verify = [&] {
            require(directory.size() == metadata.size(), "Wormhole directory count changed on growth/failure");
            for (const auto& meta : metadata) { require(directory.find(meta.key) == &meta, "Wormhole lost a colliding prefix"); }
        };
        verify();
        const std::array<uint8_t, 2> absent{0, 255};
        const PrefixKey query{absent.data(), collisionHash, 2, absent[1]};
        require(directory.find(query, false) != nullptr && directory.find(query) == nullptr,
            "Wormhole optimistic tag candidate bypassed exact collision detection");
        for (int fail = 0; fail < 2; ++fail) {
            failAfter = fail;
            bool rejected = false;
            try { directory.reserve(1000); } catch (const std::bad_alloc&) { rejected = true; }
            failAfter = -1;
            require(rejected, "Wormhole directory allocation failure was not injected");
            verify();
        }
        directory.reserve(1000);
        verify();
        require(directory.find(query) == nullptr, "Wormhole collision miss changed after rehash");
    }
    void testLeafTagCollisions() {
        using namespace akkaradb::engine::memtable::detail::wormhole;
        std::vector<std::vector<std::string>> buckets(65536);
        std::vector<std::string> collisions;
        for (uint64_t i = 0; i < 500000; ++i) {
            auto k = key(i);
            uint64_t hash = HASH_SEED;
            for (unsigned char byte : k) { hash = extendHash(hash, byte); }
            auto& bucket = buckets[tagOf(finishHash(hash, k.size()))];
            bucket.push_back(std::move(k));
            if (bucket.size() == 9) { collisions = std::move(bucket); break; }
        }
        require(collisions.size() == 9, "Wormhole could not construct equal-tag test keys");
        WormholeMemTable table;
        Model model;
        for (size_t i = 0; i + 1 < collisions.size(); ++i) {
            const auto& k = collisions[i];
            put(table, k, i + 1);
            modelPut(model, k, {i + 1, "value", 0}, false, false);
        }
        // Same-tag insertion, miss and overwrite must check every candidate.
        RecordView row;
        require(!table.get(bytes(collisions.back()), UINT64_MAX, &row), "Wormhole tag collision returned an absent key");
        for (size_t i = 0; i + 1 < collisions.size(); ++i) {
            const auto& k = collisions[i];
            put(table, k, 100 + i, "updated");
            modelPut(model, k, {100 + i, "updated", 0}, false, false);
            require(table.get(bytes(k), UINT64_MAX, &row), "Wormhole equal-tag key disappeared");
            checkRecord(row, k, model[k].front());
        }
        checkRange(table, model, {}, {}, UINT64_MAX, false);
        table.freeze();
        checkRange(table, model, {}, {}, UINT64_MAX, false);
        auto flush = table.flushIterator();
        size_t count = 0;
        for (const auto& record : flush) { (void)record; ++count; }
        require(count == 16 && table.entryCount() == 16, "Wormhole tag collision merged version chains");
    }
    void testOptimisticPrefixRetry() {
        using namespace akkaradb::engine::memtable::detail::wormhole;
        struct Meta { PrefixKey key; };
        const std::array<uint8_t, 4> query{'q', 'b', 'c', 'd'};
        const std::array<uint8_t, 2> other{'z', 'z'};
        const auto queryPrefix = [&](size_t length) {
            uint64_t hash = HASH_SEED;
            for (size_t i = 0; i < length; ++i) { hash = extendHash(hash, query[i]); }
            return prefix(query, length, hash);
        };
        Meta root{prefix({}, 0, HASH_SEED)};
        Meta real{queryPrefix(1)};
        Meta falseLong{queryPrefix(2)};
        falseLong.key.data = other.data();
        falseLong.key.tail = other[1];
        Meta falseShort{real.key};
        falseShort.key.data = other.data();
        falseShort.key.tail = other[0];
        TaggedDirectory<Meta> directory;
        directory.reserve(4);
        directory.insert(&root);
        directory.insert(&falseLong);
        directory.insert(&falseShort);
        // The initial midpoint is a false positive. Exact retry must reach the
        // root when no query prefix exists, even with several colliding tags.
        auto match = directory.longestPrefix(query, query.size(), &root);
        require(match.meta == &root && match.length == 0 && match.hash == HASH_SEED,
            "Wormhole optimistic LPM failed to fall back to the root");
        directory.insert(&real);
        match = directory.longestPrefix(query, query.size(), &root);
        require(match.meta == &real && match.length == 1 && match.hash == extendHash(HASH_SEED, query[0]),
            "Wormhole optimistic LPM skipped an exact prefix after collision");
        falseLong.key.length = 3;
        match = directory.longestPrefix(query, query.size(), &root);
        require(match.meta == &real && match.length == 1, "Wormhole trusted a tag from the wrong prefix length");
    }
    void testConcurrentReaders() {
        WormholeMemTable table;
        table.useSequenceOrder();
        std::atomic<uint64_t> published{0};
        std::atomic<bool> done{false}, failed{false};
        std::thread reader{[&] {
            try {
                do {
                    const uint64_t cut = published.load(std::memory_order_acquire);
                    if (cut != 0) {
                        RecordView row;
                        require(table.get(bytes(key(cut - 1)), cut, &row), "Wormhole concurrent point miss");
                        size_t seen = 0;
                        for (const auto& record : table.iterator({}, {}, cut)) {
                            require(record.keyString() == key(seen) && record.seq() == seen + 1, "Wormhole concurrent range differs");
                            ++seen;
                        }
                        require(seen == cut, "Wormhole concurrent range lost publication");
                    }
                    std::this_thread::yield();
                } while (!done.load(std::memory_order_acquire));
            } catch (...) { failed.store(true); }
        }};
        for (uint64_t i = 0; i < 2048; ++i) { put(table, key(i), i + 1); published.store(i + 1, std::memory_order_release); }
        done.store(true, std::memory_order_release);
        reader.join();
        require(!failed.load(), "Wormhole concurrent reader failed");
    }
    void testSmallScanAllocation() {
        WormholeMemTable table;
        for (size_t i = 0; i < 64; ++i) { put(table, key(i), i + 1); }
        auto start = key(16), end = key(48);
        allocatedBytes = largestAllocation = 0;
        traceAllocations = true;
        auto rows = table.iterator(bytes(start), bytes(end), UINT64_MAX);
        traceAllocations = false;
        start.assign(100, 'x');
        end.clear();
        traceAllocations = true;
        auto cursor = rows.begin();
        traceAllocations = false;
        require(largestAllocation <= 8192 && allocatedBytes <= 16384,
            "Wormhole short scan allocated a large arena/result buffer");
        for (size_t i = 16; i < 48; ++i) {
            require(cursor != std::default_sentinel && cursor->keyString() == key(i), "Wormhole inline scan changed order/bounds");
            ++cursor;
        }
        require(cursor == std::default_sentinel, "Wormhole inline scan crossed its end");
        for (size_t count : {size_t{0}, size_t{1}, size_t{31}, size_t{32}, size_t{33}}) {
            auto range = table.iterator(bytes(key(0)), bytes(key(count)), UINT64_MAX);
            size_t seen = 0;
            for (const auto& row : range) {
                require(row.keyString() == key(seen++), "Wormhole scan spill boundary changed order");
            }
            require(seen == count, "Wormhole scan spill boundary changed row count");
        }
    }
    void testIncrementalHash() {
        using namespace akkaradb::engine::memtable::detail::wormhole;
        const auto reference = [](uint64_t seed, const uint8_t* bytes, size_t length) {
            uint32_t crc = static_cast<uint32_t>(seed);
            for (size_t i = 0; i < length; ++i) {
                crc ^= bytes[i];
                for (unsigned bit = 0; bit < 8; ++bit) { crc = (crc >> 1) ^ ((crc & 1) ? 0x82f63b78U : 0); }
            }
            return uint64_t{crc};
        };
        const bool hardware = hardwareHashAvailable();
        const auto selected = selectHashRange();
        require(selected == (hardware ? hashRangeHardware : hashRangePortable), "Wormhole hash CPU dispatch differs");
        require(selected(HASH_SEED, nullptr, 0) == HASH_SEED, "Wormhole hash touched empty input");
        const auto* check = reinterpret_cast<const uint8_t*>("123456789");
        require(static_cast<uint32_t>(~hashRangePortable(HASH_SEED, check, 9)) == 0xe3069283U,
            "Wormhole portable CRC32C check vector differs");
        std::vector<uint8_t> data(65535 + 16);
        std::mt19937 random{0xC32C42};
        for (auto& byte : data) { byte = static_cast<uint8_t>(random()); }
        std::vector<size_t> lengths(130);
        std::iota(lengths.begin(), lengths.end(), 0);
        for (size_t length : {size_t{255}, size_t{256}, size_t{257}, size_t{4095}, size_t{4096}, size_t{65535}}) {
            lengths.push_back(length);
        }
        for (size_t offset = 0; offset < 16; ++offset) {
            const auto* bytes = data.data() + offset;
            for (size_t length : lengths) {
                for (uint64_t seed : {uint64_t{0}, HASH_SEED, uint64_t{0x12345678}}) {
                    const auto expected = reference(seed, bytes, length);
                    require(hashRangePortable(seed, bytes, length) == expected, "Wormhole portable CRC32C differs from bit reference");
                    require(selected(seed, bytes, length) == expected, "Wormhole selected CRC32C differs from bit reference");
                    if (hardware) { require(hashRangeHardware(seed, bytes, length) == expected, "Wormhole hardware CRC32C differs"); }
                    const size_t split = length / 2;
                    const auto first = hashRangePortable(seed, bytes, split);
                    require(selected(first, bytes + split, length - split) == expected, "Wormhole incremental CRC32C changed across paths");
                }
            }
        }
        std::printf("Wormhole hash dispatch: %s\n", hardware ? "SSE4.2" : "portable");
    }
    void testSortedRuns() {
        WormholeMemTable table;
        Model model;
        std::vector<size_t> order(768);
        std::iota(order.begin(), order.end(), 0);
        std::mt19937 random{0x316464};
        std::shuffle(order.begin(), order.end(), random);
        for (size_t i = 0; i < order.size(); ++i) {
            const auto k = key(order[i]);
            put(table, k, i + 1);
            modelPut(model, k, {i + 1, "value", 0}, false, false);
            RecordView row;
            require(table.get(bytes(k), UINT64_MAX, &row), "Wormhole pending run lost a point key");
            if (i % 47 == 0 || i % 61 == 0) {
                checkRange(table, model, key(123), key(617), UINT64_MAX, false);
            }
        }
        checkRange(table, model, {}, {}, UINT64_MAX, false);
        // Continue inserting after scans have produced partially filled sorted
        // prefixes, then freeze with a pending tail rather than an exact batch.
        for (size_t i = 768; i < 901; ++i) {
            const auto k = key(i);
            put(table, k, i + 1);
            modelPut(model, k, {i + 1, "value", 0}, false, false);
            if (i % 19 == 0) { checkRange(table, model, key(700), {}, UINT64_MAX, false); }
        }
        table.freeze();
        checkRange(table, model, {}, {}, UINT64_MAX, false);
        size_t seen = 0;
        for (const auto& row : table.flushIterator()) {
            require(row.keyString() == key(seen++), "Wormhole sorted-run flush changed order");
        }
        require(seen == 901, "Wormhole sorted-run flush lost entries");
    }
    void testPackedEntries() {
        for (size_t capacity : {size_t{1}, size_t{3}, size_t{17}, size_t{65535}}) {
            MemTableBackendOptions options;
            options.maxVersionsPerKey = capacity;
            WormholeMemTable table{64, 128, 4096, 2097152, options};
            table.useSequenceOrder();
            Model model;
            for (uint64_t seq : {1, 5, 2, 11, 3, 19, 11}) {
                for (size_t i = 0; i < 24; ++i) {
                    const auto k = key(i);
                    const std::string value(seq == 1 ? 1 : 128 + i, static_cast<char>('a' + seq));
                    const uint8_t flags = seq == 5 ? RecordView::FLAG_TOMBSTONE : 0;
                    put(table, k, seq, value, flags);
                    modelPut(model, k, {seq, value, flags}, true, false, capacity);
                }
            }
            size_t retained = 0;
            for (const auto& [k, list] : model) {
                retained += list.size();
                for (uint64_t cut : {uint64_t{0}, uint64_t{3}, uint64_t{10}, UINT64_MAX}) {
                    RecordView row;
                    const auto* expected = visible(list, cut, true);
                    require(table.get(bytes(k), cut, &row) == (expected != nullptr), "Wormhole packed entry changed visibility");
                    if (expected) { checkRecord(row, k, *expected); }
                }
            }
            require(table.entryCount() == retained, "Wormhole packed ring changed retained count");
            checkRange(table, model, {}, {}, UINT64_MAX, true);
            table.freeze();
            auto rows = table.flushIterator();
            auto cursor = rows.begin();
            for (const auto& [k, list] : model) {
                for (const auto& version : list) {
                    require(cursor != std::default_sentinel, "Wormhole packed ring flush ended early");
                    checkRecord(*cursor, k, version);
                    ++cursor;
                }
            }
            require(cursor == std::default_sentinel, "Wormhole packed ring flush duplicated records");
        }
    }
}

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();
    try {
        for (bool sequence : {false, true}) { for (bool all : {false, true}) { testModel(sequence, all); } }
        testBinaryAnchors();
        testConfigurationAndAccounting();
        testAllocationFailures();
        testMetadataCollisions();
        testLeafTagCollisions();
        testOptimisticPrefixRetry();
        testConcurrentReaders();
        testSmallScanAllocation();
        testIncrementalHash();
        testSortedRuns();
        testPackedEntries();
        std::puts("Wormhole prototype: passed");
        return 0;
    } catch (const std::exception& error) {
        failAfter = -1;
        std::fprintf(stderr, "Wormhole prototype: %s\n", error.what());
        return 1;
    }
}

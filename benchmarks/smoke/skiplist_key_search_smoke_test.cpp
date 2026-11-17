/*
 * Copyright (C) 2026 Swift Storm Studio
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. See https://mozilla.org/MPL/2.0/.
 */
#include "TestErrorHandlers.hpp"
#include "akk/engine/memtable/SkipListMemTable.hpp"

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <map>
#include <mutex>
#include <numeric>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {
    using akkaradb::engine::memtable::SkipListMemTable;
    using akkaradb::core::RecordView;
    using Key = std::vector<uint8_t>;

    struct Version {
        uint64_t seq;
        uint8_t value;
        uint8_t flags;
    };

    using Model = std::map<Key, std::vector<Version>>;

    void require(bool condition, const char* message) { if (!condition) { throw std::runtime_error(message); } }
    auto bytes(const Key& key) { return std::as_bytes(std::span{key}); }
    void appendId(Key& key, uint64_t id) { for (int shift = 56; shift >= 0; shift -= 8) { key.push_back(static_cast<uint8_t>(id >> shift)); } }

    Key numberedKey(uint64_t id) {
        Key result(8, 0x41);
        appendId(result, id);
        return result;
    }

    const Version* visible(const std::vector<Version>& versions, uint64_t snapshot, bool sequenceOrdered) {
        const Version* selected = nullptr;
        for (auto it = versions.rbegin(); it != versions.rend(); ++it) {
            if (it->seq <= snapshot && (!selected || it->seq > selected->seq)) {
                selected = &*it;
                if (!sequenceOrdered) { break; }
            }
        }
        return selected;
    }

    void checkRow(const RecordView& row, const Key& key, const Version& expected) {
        require(std::ranges::equal(row.key(), key), "SkipList returned another key");
        require(
            row.seq() == expected.seq && row.flags() == expected.flags && row.value().size() == 1 && row.value()[0] == expected.value,
            "SkipList version/value mismatch"
        );
    }

    void checkGet(SkipListMemTable& table, const Model& model, const Key& key, uint64_t snapshot, bool sequenceOrdered) {
        const auto found = model.find(key);
        const auto* expected = found == model.end() ? nullptr : visible(found->second, snapshot, sequenceOrdered);
        RecordView row;
        require(table.get(bytes(key), snapshot, &row) == (expected != nullptr), "SkipList hit/miss mismatch");
        if (expected) { checkRow(row, key, *expected); }
    }

    void checkScan(SkipListMemTable& table, const Model& model, uint64_t snapshot, bool sequenceOrdered, const Key& start = {}, const Key& end = {}) {
        auto expected = start.empty() ? model.begin() : model.lower_bound(start);
        const auto advance = [&] { while (expected != model.end() && !visible(expected->second, snapshot, sequenceOrdered)) { ++expected; } };
        advance();
        for (const auto& row : table.iterator(bytes(start), bytes(end), snapshot)) {
            require(expected != model.end() && (end.empty() || expected->first < end), "SkipList scan returned extra rows");
            checkRow(row, expected->first, *visible(expected->second, snapshot, sequenceOrdered));
            ++expected;
            advance();
        }
        require(expected == model.end() || (!end.empty() && !(expected->first < end)), "SkipList lower bound skipped rows");
    }

    std::vector<Key> testKeys() {
        std::vector<Key> keys{{}};
        // Zero padding must not collapse a key and its zero-valued extensions.
        for (size_t length = 1; length <= 33; ++length) {
            for (uint8_t fill : {uint8_t{0}, uint8_t{0x41}, uint8_t{0xff}}) {
                Key key(length, fill);
                keys.push_back(key);
                key.back() ^= 1;
                keys.push_back(std::move(key));
            }
        }
        std::mt19937_64 random{0x5A1F1157};
        for (size_t i = 0; i < 6000; ++i) {
            Key key;
            if (i % 3 == 0) { key.assign(64 + i % 64, 0x61); }
            else if (i % 3 == 1) {
                key.resize(i % 25);
                for (auto& byte : key) { byte = static_cast<uint8_t>(random()); }
            }
            else { key.assign(8, 0x41); }
            appendId(key, i * 2);
            keys.push_back(std::move(key));
        }
        keys.emplace_back(65534, 0);
        keys.emplace_back(65534, 0xff);
        std::ranges::sort(keys);
        keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
        return keys;
    }

    void verifyKeys(bool retainAll, bool sequenceOrdered, bool descending) {
        SkipListMemTable table;
        if (retainAll) { table.retainAllVersions(); }
        if (sequenceOrdered) { table.useSequenceOrder(); }
        Model model;
        auto keys = testKeys();
        std::vector<size_t> order(keys.size());
        std::iota(order.begin(), order.end(), size_t{0});
        if (descending) { std::reverse(order.begin(), order.end()); }
        else {
            std::mt19937 random{7719};
            std::shuffle(order.begin(), order.end(), random);
        }
        const auto put = [&](const Key& key, uint64_t seq, uint8_t value, uint8_t flags) {
            const Key data{value};
            require(table.put(bytes(key), bytes(data), seq, flags).ok(), "SkipList put failed");
            model[key].push_back({seq, value, flags});
        };
        for (size_t i : order) { put(keys[i], 10, 1, 0); }
        for (const auto& key : keys) {
            checkGet(table, model, key, 10, sequenceOrdered);
            Key probe = key;
            if (!probe.empty()) {
                probe.pop_back();
                checkGet(table, model, probe, 10, sequenceOrdered);
            }
            probe = key;
            probe.push_back(0);
            checkGet(table, model, probe, 10, sequenceOrdered);
        }
        checkScan(table, model, 10, sequenceOrdered);
        for (size_t i = 0; i < keys.size(); i += 113) {
            Key lower = keys[i];
            lower.push_back(0);
            checkScan(table, model, 10, sequenceOrdered, lower, keys[std::min(i + 19, keys.size() - 1)]);
        }
        for (size_t i = 0; i < keys.size(); i += 3) {
            put(keys[i], 30, 2, 0);
            put(keys[i], 20, 3, akkaradb::core::MemHdr16::FLAG_TOMBSTONE);
        }
        for (uint64_t snapshot : {uint64_t{9}, uint64_t{10}, uint64_t{25}, UINT64_MAX}) {
            for (const auto& key : keys) { checkGet(table, model, key, snapshot, sequenceOrdered); }
            checkScan(table, model, snapshot, sequenceOrdered);
        }
        table.freeze();
        checkScan(table, model, UINT64_MAX, sequenceOrdered);
        for (const auto& key : keys) { checkGet(table, model, key, UINT64_MAX, sequenceOrdered); }
        size_t flushed = 0;
        Key previous;
        for (const auto& row : table.flushIterator()) {
            const Key current(row.key().begin(), row.key().end());
            require(previous <= current, "SkipList flush is unsorted");
            previous = current;
            ++flushed;
        }
        require(flushed == table.entryCount(), "SkipList flush lost versions");
    }

    void verifyConcurrentPublication() {
        constexpr uint64_t count = 8192;
        SkipListMemTable table;
        std::atomic<uint64_t> published{0};
        std::atomic<bool> done{false}, failed{false};
        std::atomic<unsigned> ready{0};
        std::string failure;
        std::mutex failureMutex;
        const auto guarded = [&](auto work) {
            try { work(); }
            catch (const std::exception& error) {
                std::lock_guard lock{failureMutex};
                failure = error.what();
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
                                const auto cut = published.load(std::memory_order_acquire);
                                if (cut == 0) {
                                    std::this_thread::yield();
                                    continue;
                                }
                                if (id == 0) {
                                    size_t seen = 0;
                                    for (const auto& row : table.iterator({}, {}, cut)) {
                                        require(std::ranges::equal(row.key(), numberedKey(2 * seen++)), "SkipList concurrent scan lost ordering");
                                    }
                                    require(seen == cut, "SkipList concurrent scan missed a published node");
                                }
                                else {
                                    const auto key = numberedKey(2 * ((cut - 1) / id));
                                    RecordView row;
                                    require(
                                        table.get(bytes(key), cut, &row) && std::ranges::equal(row.key(), key),
                                        "SkipList early hit missed a published node"
                                    );
                                    require(!table.get(bytes(numberedKey(2 * ((cut - 1) / id) + 1)), cut, &row), "SkipList miss returned a neighbour");
                                }
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
                    const Key value{1};
                    require(table.put(bytes(numberedKey(2 * (seq - 1))), bytes(value), seq, 0).ok(), "SkipList concurrent put failed");
                    published.store(seq, std::memory_order_release);
                    if ((seq & 31) == 0) { std::this_thread::yield(); }
                }
            }
        );
        done.store(true, std::memory_order_release);
        for (auto& reader : readers) { reader.join(); }
        require(!failed.load(), failure.c_str());
        require(table.entryCount() == count, "SkipList writer failed to complete");
    }
}

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();
    try {
        verifyKeys(false, false, false);
        verifyKeys(false, true, true);
        verifyKeys(true, false, true);
        verifyKeys(true, true, false);
        verifyConcurrentPublication();
        std::puts("SkipList key search smoke: passed");
        return 0;
    }
    catch (const std::exception& error) {
        std::fprintf(stderr, "SkipList key search smoke: %s\n", error.what());
        return 1;
    }
}

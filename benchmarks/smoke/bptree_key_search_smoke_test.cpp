/*
 * Copyright (C) 2026 Swift Storm Studio
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
#include "TestErrorHandlers.hpp"
#include "akk/engine/memtable/BPTreeMemTable.hpp"

#include <algorithm>
#include <cstdio>
#include <map>
#include <numeric>
#include <random>
#include <stdexcept>
#include <vector>

namespace {
    using namespace akkaradb::engine::memtable;
    using Key = std::vector<uint8_t>;
    struct Version { uint64_t seq; std::vector<uint8_t> value; uint8_t flags; };
    using Model = std::map<Key, std::vector<Version>>;

    void require(bool condition, const char* message) {
        if (!condition) { throw std::runtime_error(message); }
    }
    auto bytes(const Key& key) { return std::as_bytes(std::span{key}); }
    const Version* visible(const std::vector<Version>& versions, uint64_t snapshot) {
        for (auto it = versions.rbegin(); it != versions.rend(); ++it) {
            if (it->seq <= snapshot) { return &*it; }
        }
        return nullptr;
    }
    void checkGet(BPTreeMemTable& table, const Model& model, const Key& key, uint64_t snapshot) {
        const auto found = model.find(key);
        const auto* expected = found == model.end() ? nullptr : visible(found->second, snapshot);
        akkaradb::core::RecordView actual;
        require(table.get(bytes(key), snapshot, &actual) == (expected != nullptr), "point hit/miss mismatch");
        if (expected) {
            require(std::ranges::equal(actual.key(), key), "point lookup returned another key");
            require(actual.seq() == expected->seq && actual.flags() == expected->flags, "point version/flags mismatch");
            require(std::ranges::equal(actual.value(), expected->value), "point value mismatch");
        }
    }
    void checkScan(BPTreeMemTable& table, const Model& model, uint64_t snapshot, const Key& start = {}, const Key& end = {}) {
        auto it = start.empty() ? model.begin() : model.lower_bound(start);
        const auto advance = [&] {
            while (it != model.end() && !visible(it->second, snapshot)) { ++it; }
        };
        advance();
        for (const auto& actual : table.iterator(bytes(start), bytes(end), snapshot)) {
            require(it != model.end() && (end.empty() || it->first < end), "scan returned extra record");
            const auto* expected = visible(it->second, snapshot);
            require(std::ranges::equal(actual.key(), it->first), "scan ordering/key mismatch");
            require(actual.seq() == expected->seq && actual.flags() == expected->flags, "scan version mismatch");
            require(std::ranges::equal(actual.value(), expected->value), "scan value mismatch");
            ++it;
            advance();
        }
        require(it == model.end() || (!end.empty() && !(it->first < end)), "scan missed records");
    }

    void appendId(Key& key, uint64_t id) {
        for (int shift = 56; shift >= 0; shift -= 8) { key.push_back(static_cast<uint8_t>(id >> shift)); }
    }
    std::vector<Key> data() {
        std::vector<Key> keys{{}, {0}, {0, 0}, {0, 1}, {0xff}, {0xff, 0}, {0xff, 0xff}};
        std::mt19937_64 random{0xB17CACE};
        for (size_t i = 0; i < 9'000; ++i) {
            Key key;
            if (i % 3 == 0) { key.assign(64, 0x41); }
            else if (i % 3 == 1) {
                key.resize(i % 25);
                for (auto& byte : key) { byte = static_cast<uint8_t>(random()); }
            } else { key.assign(128 + i % 129, 0x90); }
            appendId(key, i);
            keys.push_back(std::move(key));
        }
        // SmallBuffer's u16 limit covers key + value; each test value occupies eight bytes.
        for (size_t length : {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 24, 32, 63, 64, 255, 256, 65527}) {
            Key key(length, 0);
            keys.push_back(key);
            key.back() = 1;
            keys.push_back(key);
            key.back() = 0xff;
            keys.push_back(std::move(key));
        }
        std::ranges::sort(keys);
        keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
        return keys;
    }

    void verify(bool history, BPTreeConcurrencyMode concurrency, bool batched) {
        MemTableBackendOptions options;
        options.bptreeConcurrencyMode = concurrency;
        if (batched) {
            options.bptreeIteratorMode = BPTreeIteratorMode::MATERIALIZE_BATCHED_UNPINNED;
            options.bptreeIteratorBatchSize = 37;
        }
        BPTreeMemTable table(64 * 1024, 2 * 1024 * 1024, 64 * 1024, 2 * 1024 * 1024, options);
        if (history) { table.retainAllVersions(); }
        Model model;
        auto keys = data();
        std::vector<size_t> order(keys.size());
        std::iota(order.begin(), order.end(), size_t{0});
        std::mt19937 random{1771};
        std::shuffle(order.begin(), order.end(), random);
        uint64_t seq = 0;
        const auto put = [&](const Key& key, uint8_t flags) {
            Version version{++seq, {}, flags};
            appendId(version.value, seq);
            require(table.put(bytes(key), bytes(version.value), seq, flags).ok(), "put failed");
            model[key].push_back(std::move(version));
        };
        for (size_t i : order) { put(keys[i], 0); }
        const auto initial = seq;
        for (const auto& key : keys) {
            checkGet(table, model, key, initial);
            Key shortened = key;
            if (!shortened.empty()) { shortened.resize(shortened.size() / 2); }
            checkGet(table, model, shortened, initial);
            Key extended = key;
            if (extended.size() < 65535) { extended.push_back(0); checkGet(table, model, extended, initial); }
        }
        checkScan(table, model, initial);
        checkScan(table, model, initial / 2);
        for (size_t i = 0; i < keys.size(); i += 71) {
            checkScan(table, model, initial, keys[i], keys[std::min(i + 17, keys.size() - 1)]);
        }
        for (size_t i = 0; i < keys.size(); i += 3) {
            put(keys[i], 0);
            put(keys[i], akkaradb::core::MemHdr16::FLAG_TOMBSTONE);
        }
        for (const auto& key : keys) {
            checkGet(table, model, key, initial);
            checkGet(table, model, key, seq);
        }
        checkScan(table, model, initial);
        checkScan(table, model, seq);
        table.freeze();
        checkScan(table, model, initial);
        checkScan(table, model, seq, Key(31, 0x41), Key(65, 0x41));
    }

    void verifyLongKeyCacheTiesAndPrefixChanges() {
        BPTreeMemTable table;
        Model model;
        uint64_t seq = 0;
        const auto put = [&](Key key) {
            Version value{++seq, {static_cast<uint8_t>(seq)}, 0};
            require(table.put(bytes(key), bytes(value.value), seq, 0).ok(), "prefix transition insert failed");
            model[key].push_back(std::move(value));
            for (const auto& [candidate, ignored] : model) { (void)ignored; checkGet(table, model, candidate, seq); }
        };
        for (uint64_t i = 0; i < 24; ++i) {
            Key key(128, 0x61); appendId(key, i); put(std::move(key));
        }
        // Shrink a long node prefix, then force cached 16-byte ties within that node.
        put({0});
        put({0xff});
        put(Key(127, 0x61));
        for (uint64_t i = 24; i < 160; ++i) {
            Key key(128, 0x61); appendId(key, i); put(std::move(key));
        }
        checkScan(table, model, seq);
        checkScan(table, model, seq, Key(64, 0x61), Key(129, 0x61));
    }

    void verifyEmptyValuesAndMaximumKey() {
        BPTreeMemTable table;
        akkaradb::core::RecordView found;
        const Key empty, huge(65535, 0x7c);
        require(table.put(bytes(empty), bytes(empty), 1, 0).ok(), "empty record insert failed");
        require(table.get(bytes(empty), 1, &found) && found.key().empty() && found.value().empty(), "empty record lookup failed");
        require(table.put(bytes(huge), bytes(empty), 2, 0).ok(), "maximum-size key insert failed");
        require(table.get(bytes(huge), 2, &found) && std::ranges::equal(found.key(), huge), "maximum-size key lookup failed");
        BPTreeMemTable longPrefix;
        require(longPrefix.put(bytes(huge), bytes(empty), 1, 0).ok(), "maximum prefix setup failed");
        require(longPrefix.get(bytes(huge), 1, &found), "maximum prefix lookup failed");
        const auto shortened = std::span{huge}.first(65534);
        require(!longPrefix.get(std::as_bytes(shortened), 1, &found), "shorter prefix falsely matched");
    }
}

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();
    try {
        verifyLongKeyCacheTiesAndPrefixChanges();
        verifyEmptyValuesAndMaximumKey();
        verify(false, BPTreeConcurrencyMode::LOCKED, false);
        verify(true, BPTreeConcurrencyMode::LOCKED, true);
        verify(true, BPTreeConcurrencyMode::OPTIMISTIC_UNSAFE, false);
        std::puts("BPTree key search smoke: passed");
        return 0;
    } catch (const std::exception& error) {
        std::fprintf(stderr, "BPTree key search smoke failed: %s\n", error.what());
        return 1;
    }
}

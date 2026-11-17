/*
 * Copyright (C) 2026 Swift Storm Studio
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
#include "TestErrorHandlers.hpp"
#include "akk/core/record/KeyFingerprint.hpp"
#include "akk/engine/memtable/MemTable.hpp"
#include "akk/engine/memtable/ARTMemTable.hpp"
#include "akk/engine/memtable/BPTreeMemTable.hpp"
#include "akk/engine/memtable/SkipListMemTable.hpp"

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <numeric>
#include <random>
#include <stdexcept>
#include <vector>

namespace {
    using namespace akkaradb::engine::memtable;
    using Bytes = std::vector<uint8_t>;
    void require(bool condition, const char* message) { if (!condition) { throw std::runtime_error(message); } }

    Bytes keyFor(uint64_t id) {
        Bytes key(16, 0);
        for (size_t i = 0; i < 8; ++i) { key[i * 2] = static_cast<uint8_t>(id >> (i * 8)); }
        return key;
    }

    void checkRecord(const MemTable::RecordView& record, const Bytes& key, const Bytes& value, uint64_t sequence) {
        require(std::ranges::equal(record.key(), key), "read returned the wrong key");
        require(std::ranges::equal(record.value(), value) && record.seq() == sequence, "read returned the wrong value/version");
        require(record.keyFp64() == akkaradb::core::computeKeyFp64(key), "stored fingerprint changed");
    }

    void verify(MemTable::MemTableFactory factory, size_t shards) {
        MemTable::Options options;
        options.shardCount = shards;
        options.flushMode = MemTableFlushMode::MANUAL_ONLY;
        options.thresholdBytesPerShard = 0;
        options.backendFactory = std::move(factory);
        std::atomic<size_t> flushed{0};
        options.onFlush = [&](std::span<const MemTable::RecordView> records) { flushed.fetch_add(records.size()); };
        auto table = MemTable::create(options);
        require(table->snapshot().shardCount == (shards == 1 ? 1 : 4), "resolved shard count mismatch");
        constexpr size_t count = 257;
        std::vector<Bytes> keys, values;
        for (size_t i = 0; i < count; ++i) {
            keys.push_back(keyFor(i));
            values.push_back(Bytes{static_cast<uint8_t>(i), static_cast<uint8_t>(i >> 8), 0x5a});
            table->put(keys.back(), values.back(), i + 1);
        }
        size_t callbacks = 0;
        auto snapshot = table->readSnapshot(
            [&] {
                ++callbacks;
                return count;
            }
        );
        require(callbacks == 1 && snapshot.snapshotSeq() == count, "snapshot sequence callback mismatch");
        MemTable::ReadSnapshot emptySnapshot;
        require(!emptySnapshot.getPinned(keys.front()), "empty snapshot falsely found a key");
        std::vector<size_t> order(count);
        std::iota(order.begin(), order.end(), size_t{0});
        std::mt19937 random{0xA44A};
        std::shuffle(order.begin(), order.end(), random);
        for (size_t i : order) {
            const auto& key = keys[i];
            MemTable::RecordView record;
            require(table->get(key, count, &record), "ordinary get missed");
            checkRecord(record, key, values[i], i + 1);
            require(table->get(key, count, &record, akkaradb::core::computeKeyFp64(key)), "hinted get missed");
            checkRecord(record, key, values[i], i + 1);
            const auto pinned = table->getPinned(key, count);
            require(pinned.has_value(), "pinned get missed");
            checkRecord(pinned->view, key, values[i], i + 1);
            const auto withSequence = table->getPinnedWithSnapshot(
                key,
                [&] {
                    ++callbacks;
                    return count;
                }
            );
            require(withSequence.has_value(), "sequence-provider get missed");
            checkRecord(withSequence->view, key, values[i], i + 1);
            const auto saved = snapshot.getPinned(key);
            require(saved.has_value(), "snapshot get missed");
            checkRecord(saved->view, key, values[i], i + 1);
            Bytes copied;
            require(table->getInto(key, count, copied) == true && copied == values[i], "getInto mismatch");
            require(table->contains(key, count) == true, "contains mismatch");
        }
        require(callbacks == count + 1, "sequence provider called an unexpected number of times");
        const auto absent = keyFor(count + 1);
        MemTable::RecordView record;
        require(!table->get(absent, count, &record) && !table->getPinned(absent, count) && !snapshot.getPinned(absent), "absent key falsely found");
        Bytes copied;
        require(!table->getInto(absent, count, copied) && !table->contains(absent, count), "absent key lost tri-state result");
        const Bytes updated{0xa5};
        table->put(keys[0], updated, count + 1);
        table->remove(keys[1], count + 2);
        const auto latest = table->getPinned(keys[0], count + 2);
        require(latest.has_value(), "update missed");
        checkRecord(latest->view, keys[0], updated, count + 1);
        const auto tombstone = table->getPinned(keys[1], count + 2);
        require(tombstone && tombstone->view.isTombstone(), "tombstone missed");
        require(table->getInto(keys[1], count + 2, copied) == false && table->contains(keys[1], count + 2) == false, "tombstone lost tri-state result");
        const auto oldValue = snapshot.getPinned(keys[0]), oldRemoved = snapshot.getPinned(keys[1]);
        require(oldValue && oldRemoved, "snapshot lost an updated or removed key");
        checkRecord(oldValue->view, keys[0], values[0], 1);
        checkRecord(oldRemoved->view, keys[1], values[1], 2);
        table->forceFlush();
        table->drainFlushes();
        require(flushed.load() >= count, "flush sink did not receive all keys");
        require(!table->getPinned(keys[0], count + 2), "flushed generation still published");
        table.reset();
        for (size_t i : order) {
            const auto saved = snapshot.getPinned(keys[i]);
            require(saved.has_value(), "snapshot lost a flushed generation after coordinator destruction");
            checkRecord(saved->view, keys[i], values[i], i + 1);
        }
    }
}

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();
    try {
        for (size_t shards : {1, 3, 4}) {
            verify([] { return std::make_unique<BPTreeMemTable>(); }, shards);
            verify([] { return std::make_unique<ARTMemTable>(); }, shards);
            verify([] { return std::make_unique<SkipListMemTable>(); }, shards);
        }
        std::puts("MemTable read routing smoke: passed");
        return 0;
    }
    catch (const std::exception& error) {
        std::fprintf(stderr, "MemTable read routing smoke failed: %s\n", error.what());
        return 1;
    }
}

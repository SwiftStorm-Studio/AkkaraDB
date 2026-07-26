/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "akk/engine/memtable/SkipListMemTable.hpp"

#include <algorithm>
#include <cstring>
#include <limits>
#include <new>
#include <stdexcept>
#include <utility>
#include <vector>

#include "akk/core/record/KeyFingerprint.hpp"
#include "akk/core/record/MemHdr16.hpp"

namespace akkaradb::engine::memtable {
    namespace {
        [[nodiscard]] int compareKeyBytes(std::span<const uint8_t> lhs, std::span<const uint8_t> rhs) noexcept {
            const size_t minLen = std::min(lhs.size(), rhs.size());
            if (minLen > 0) {
                const int cmp = std::memcmp(lhs.data(), rhs.data(), minLen);
                if (cmp != 0) { return cmp < 0 ? -1 : 1; }
            }
            if (lhs.size() < rhs.size()) { return -1; }
            if (lhs.size() > rhs.size()) { return 1; }
            return 0;
        }

        [[nodiscard]] uint16_t resolveMaxVersionsPerKey(size_t value) {
            if (value == 0 || value > SkipListMemTable::MAX_CONFIGURED_VERSIONS_PER_KEY) {
                throw std::invalid_argument("SkipListMemTable: maxVersionsPerKey must be in [1, 65535]");
            }
            return static_cast<uint16_t>(value);
        }
    } // namespace

    SkipListMemTable::SkipListMemTable(
        size_t dataArenaInitialBlockSize,
        size_t dataArenaMaxBlockSize,
        size_t generatorArenaInitialBlockSize,
        size_t generatorArenaMaxBlockSize,
        MemTableBackendOptions backendOptions
    )
        : dataArena_{dataArenaInitialBlockSize, dataArenaMaxBlockSize},
          generatorArenaInitialBlockSize_{generatorArenaInitialBlockSize},
          generatorArenaMaxBlockSize_{generatorArenaMaxBlockSize},
          maxVersionsPerKey_{resolveMaxVersionsPerKey(backendOptions.maxVersionsPerKey)} {
        head_ = newNode(nullptr, MAX_LEVEL);
        currentMaxLevel_.store(1, std::memory_order_relaxed);
    }

    std::span<const uint8_t> SkipListMemTable::asU8(ByteView view) noexcept {
        return {reinterpret_cast<const uint8_t*>(view.data()), view.size()};
    }

    uint64_t SkipListMemTable::nextRandom() noexcept {
        uint64_t x = rngState_;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        rngState_ = x;
        return x;
    }

    uint8_t SkipListMemTable::randomLevel() noexcept {
        uint8_t level = 1;
        while (level < MAX_LEVEL && ((nextRandom() & 0x3ULL) == 0ULL)) { ++level; }
        return level;
    }

    SkipListMemTable::VersionChain* SkipListMemTable::makeChain(const core::OwnedRecord* initialRecord) {
        const size_t slotBytes = static_cast<size_t>(maxVersionsPerKey_) * sizeof(std::atomic<const core::OwnedRecord*>);
        const size_t bytes = sizeof(VersionChain) + slotBytes;
        std::byte* mem = dataArena_.allocate(bytes, alignof(VersionChain));
        VersionChain* chain = new(mem) VersionChain{};
        chain->capacity = maxVersionsPerKey_;
        auto* ring = chain->ring();
        for (uint16_t i = 0; i < maxVersionsPerKey_; ++i) { new(&ring[i]) std::atomic<const core::OwnedRecord*>{nullptr}; }
        ring[0].store(initialRecord, std::memory_order_relaxed);
        chain->head.store(0, std::memory_order_relaxed);
        chain->count.store(1, std::memory_order_relaxed);
        entries_.fetch_add(1, std::memory_order_relaxed);
        bytes_.fetch_add(bytes, std::memory_order_relaxed);
        return chain;
    }

    SkipListMemTable::Node* SkipListMemTable::newNode(const core::OwnedRecord* initialRecord, uint8_t level) {
        const size_t bytes = sizeof(Node) + static_cast<size_t>(level) * sizeof(std::atomic<Node*>);
        std::byte* mem = dataArena_.allocate(bytes, alignof(Node));
        VersionChain* chain = initialRecord != nullptr ? makeChain(initialRecord) : nullptr;
        Node* node = new(mem) Node{level, initialRecord, chain};
        auto* nextSlots = reinterpret_cast<std::atomic<Node*>*>(node + 1);
        for (uint8_t i = 0; i < level; ++i) { new(&nextSlots[i]) std::atomic<Node*>{nullptr}; }
        bytes_.fetch_add(bytes, std::memory_order_relaxed);
        return node;
    }

    core::OwnedRecord* SkipListMemTable::makeRecord(
        std::span<const uint8_t> key,
        std::span<const uint8_t> value,
        uint64_t seq,
        uint8_t flags,
        uint64_t precomputedFp64,
        uint64_t precomputedMk
    ) {
        const uint64_t fp64 = precomputedFp64 != 0 ? precomputedFp64 : (key.empty() ? 0 : core::computeKeyFp64(key.data(), key.size()));
        const uint64_t mini = precomputedMk != 0 ? precomputedMk : (key.empty() ? 0 : core::buildMiniKey(key.data(), key.size()));

        std::byte* mem = dataArena_.allocate(sizeof(core::OwnedRecord), alignof(core::OwnedRecord));
        core::OwnedRecord* record = new(mem) core::OwnedRecord{};
        core::OwnedRecord::createInplace(*record, key, value, seq, flags, dataArena_, fp64, mini);
        bytes_.fetch_add(sizeof(core::OwnedRecord) + key.size() + value.size(), std::memory_order_relaxed);
        return record;
    }

    int SkipListMemTable::compareNodeKey(const Node* node, std::span<const uint8_t> key) noexcept {
        return node->keyRecord->compareKey(key);
    }

    SkipListMemTable::Node* SkipListMemTable::findNodeForWrite(
        std::span<const uint8_t> key,
        std::array<Node*, MAX_LEVEL>& update
    ) noexcept {
        Node* current = head_;
        const uint8_t topLevel = currentMaxLevel_.load(std::memory_order_relaxed);
        for (int level = static_cast<int>(topLevel) - 1; level >= 0; --level) {
            Node* next = current->nextAt(static_cast<uint8_t>(level)).load(std::memory_order_relaxed);
            while (next != nullptr && compareNodeKey(next, key) < 0) {
                current = next;
                next = current->nextAt(static_cast<uint8_t>(level)).load(std::memory_order_relaxed);
            }
            update[static_cast<size_t>(level)] = current;
        }
        return current->nextAt(0).load(std::memory_order_relaxed);
    }

    SkipListMemTable::Node* SkipListMemTable::findNodeForRead(std::span<const uint8_t> key) const noexcept {
        Node* current = head_;
        const uint8_t topLevel = currentMaxLevel_.load(std::memory_order_acquire);
        for (int level = static_cast<int>(topLevel) - 1; level >= 0; --level) {
            Node* next = current->nextAt(static_cast<uint8_t>(level)).load(std::memory_order_acquire);
            while (next != nullptr && compareNodeKey(next, key) < 0) {
                current = next;
                next = current->nextAt(static_cast<uint8_t>(level)).load(std::memory_order_acquire);
            }
        }
        return current->nextAt(0).load(std::memory_order_acquire);
    }

    void SkipListMemTable::appendVersion(VersionChain* chain, const core::OwnedRecord* record, std::atomic<size_t>& entries) noexcept {
        chain->version.fetch_add(1, std::memory_order_acq_rel);
        const uint16_t capacity = chain->capacity;
        const uint16_t prevHead = chain->head.load(std::memory_order_relaxed);
        const uint16_t prevCount = chain->count.load(std::memory_order_relaxed);
        const uint16_t nextHead = static_cast<uint16_t>((prevHead + 1) % capacity);
        chain->ring()[nextHead].store(record, std::memory_order_release);
        if (prevCount < capacity) {
            chain->count.store(static_cast<uint16_t>(prevCount + 1), std::memory_order_relaxed);
            entries.fetch_add(1, std::memory_order_relaxed);
        }
        chain->head.store(nextHead, std::memory_order_release);
        chain->version.fetch_add(1, std::memory_order_release);
    }

    bool SkipListMemTable::visibleRecord(const VersionChain* chain, uint64_t snapshotSeq, RecordView* out) noexcept {
        if (chain == nullptr || out == nullptr) { return false; }
        for (;;) {
            const uint64_t begin = chain->version.load(std::memory_order_acquire);
            if ((begin & 1ULL) != 0ULL) { continue; }
            const uint16_t capacity = chain->capacity;
            const uint16_t head = chain->head.load(std::memory_order_relaxed);
            const uint16_t count = chain->count.load(std::memory_order_relaxed);
            const core::OwnedRecord* selected = nullptr;
            if (count > 0) {
                const core::OwnedRecord* newest = chain->ring()[head].load(std::memory_order_relaxed);
                if (newest != nullptr && newest->seq() <= snapshotSeq) { selected = newest; }
                else {
                    for (uint16_t i = 1; i < count; ++i) {
                        const uint16_t index = static_cast<uint16_t>((head + capacity - i) % capacity);
                        const core::OwnedRecord* candidate = chain->ring()[index].load(std::memory_order_relaxed);
                        if (candidate != nullptr && candidate->seq() <= snapshotSeq) {
                            selected = candidate;
                            break;
                        }
                    }
                }
            }
            const uint64_t end = chain->version.load(std::memory_order_acquire);
            if (begin == end && (end & 1ULL) == 0ULL) {
                if (selected == nullptr) { return false; }
                *out = toView(*selected);
                return true;
            }
        }
    }

    bool SkipListMemTable::visibleFrozenRecord(const VersionChain* chain, uint64_t snapshotSeq, RecordView* out) noexcept {
        if (chain == nullptr || out == nullptr) { return false; }
        const uint16_t capacity = chain->capacity;
        const uint16_t head = chain->head.load(std::memory_order_acquire);
        const uint16_t count = chain->count.load(std::memory_order_acquire);
        for (uint16_t i = 0; i < count; ++i) {
            const uint16_t index = static_cast<uint16_t>((head + capacity - i) % capacity);
            const core::OwnedRecord* candidate = chain->ring()[index].load(std::memory_order_acquire);
            if (candidate != nullptr && candidate->seq() <= snapshotSeq) {
                *out = toView(*candidate);
                return true;
            }
        }
        return false;
    }

    RecordView SkipListMemTable::toView(const core::OwnedRecord& record) noexcept {
        const auto key = record.key();
        const auto value = record.value();
        return {
            key.data(),
            record.hdr.kLen,
            value.data(),
            record.hdr.vLen,
            record.hdr.seq,
            record.hdr.flags,
            record.keyFp64,
            record.miniKey
        };
    }

    Status SkipListMemTable::put(
        ByteView key,
        ByteView value,
        uint64_t seq,
        uint8_t flags,
        uint64_t precomputedFp64,
        uint64_t precomputedMk
    ) {
        if (frozen_.load(std::memory_order_acquire)) { return Status::Error(Status::Code::INVALID_ARGUMENT, "memtable is frozen"); }
        if (key.size() > std::numeric_limits<uint16_t>::max() || value.size() > std::numeric_limits<uint16_t>::max()) {
            return Status::Error(Status::Code::INVALID_ARGUMENT, "key/value too large for MemHdr16");
        }

        const std::span<const uint8_t> keyU8 = asU8(key);
        const std::span<const uint8_t> valueU8 = asU8(value);
        std::array<Node*, MAX_LEVEL> update{};
        Node* candidate = findNodeForWrite(keyU8, update);
        const core::OwnedRecord* record = makeRecord(keyU8, valueU8, seq, flags, precomputedFp64, precomputedMk);

        if (candidate != nullptr && compareNodeKey(candidate, keyU8) == 0) {
            appendVersion(candidate->versions, record, entries_);
            return Status::OK();
        }

        const uint8_t level = randomLevel();
        const uint8_t observedMax = currentMaxLevel_.load(std::memory_order_relaxed);
        for (uint8_t i = observedMax; i < level; ++i) { update[i] = head_; }
        Node* node = newNode(record, level);
        for (uint8_t i = 0; i < level; ++i) {
            Node* next = update[i]->nextAt(i).load(std::memory_order_relaxed);
            node->nextAt(i).store(next, std::memory_order_relaxed);
        }
        for (int i = static_cast<int>(level) - 1; i >= 0; --i) {
            const uint8_t index = static_cast<uint8_t>(i);
            update[index]->nextAt(index).store(node, std::memory_order_release);
        }
        if (level > observedMax) { currentMaxLevel_.store(level, std::memory_order_release); }
        return Status::OK();
    }

    bool SkipListMemTable::get(ByteView key, uint64_t snapshotSeq, RecordView* out) const {
        if (out == nullptr) { return false; }
        const Node* node = findNodeForRead(asU8(key));
        if (node == nullptr || compareNodeKey(node, asU8(key)) != 0) { return false; }
        return frozen_.load(std::memory_order_acquire)
                   ? visibleFrozenRecord(node->versions, snapshotSeq, out)
                   : visibleRecord(node->versions, snapshotSeq, out);
    }

    ArenaGenerator<RecordView> SkipListMemTable::iterateSnapshot(uint64_t snapshotSeq, bool frozen) const {
        Node* current = head_->nextAt(0).load(std::memory_order_acquire);
        while (current != nullptr) {
            RecordView visible;
            const bool found = frozen
                                   ? visibleFrozenRecord(current->versions, snapshotSeq, &visible)
                                   : visibleRecord(current->versions, snapshotSeq, &visible);
            if (found) { co_yield visible; }
            current = current->nextAt(0).load(std::memory_order_acquire);
        }
    }

    ArenaGenerator<RecordView> SkipListMemTable::iterateSnapshotRange(
        uint64_t snapshotSeq,
        std::vector<uint8_t> startKey,
        std::vector<uint8_t> endKey,
        bool frozen
    ) const {
        const std::span<const uint8_t> start{startKey.data(), startKey.size()};
        const std::span<const uint8_t> end{endKey.data(), endKey.size()};
        if (!start.empty() && !end.empty() && compareKeyBytes(start, end) >= 0) { co_return; }

        Node* current = start.empty() ? head_->nextAt(0).load(std::memory_order_acquire) : findNodeForRead(start);
        while (current != nullptr) {
            if (!end.empty() && compareNodeKey(current, end) >= 0) { break; }
            RecordView visible;
            const bool found = frozen
                                   ? visibleFrozenRecord(current->versions, snapshotSeq, &visible)
                                   : visibleRecord(current->versions, snapshotSeq, &visible);
            if (found && (start.empty() || visible.compareKey(start) >= 0)) { co_yield visible; }
            current = current->nextAt(0).load(std::memory_order_acquire);
        }
    }

    ArenaGenerator<RecordView> SkipListMemTable::iterator(ByteView startKey, ByteView endKey, uint64_t snapshotSeq) const {
        const std::span<const uint8_t> start = asU8(startKey);
        const std::span<const uint8_t> end = asU8(endKey);
        const bool frozen = frozen_.load(std::memory_order_acquire);
        if (start.empty() && end.empty()) {
            return ArenaGenerator<RecordView>::withOwnedArena(
                generatorArenaInitialBlockSize_,
                generatorArenaMaxBlockSize_,
                [this, snapshotSeq, frozen] { return iterateSnapshot(snapshotSeq, frozen); }
            );
        }
        std::vector<uint8_t> startOwned(start.begin(), start.end());
        std::vector<uint8_t> endOwned(end.begin(), end.end());
        return ArenaGenerator<RecordView>::withOwnedArena(
            generatorArenaInitialBlockSize_,
            generatorArenaMaxBlockSize_,
            [this, snapshotSeq, frozen, startOwned = std::move(startOwned), endOwned = std::move(endOwned)]() mutable {
                return iterateSnapshotRange(snapshotSeq, std::move(startOwned), std::move(endOwned), frozen);
            }
        );
    }

    void SkipListMemTable::freeze() { frozen_.store(true, std::memory_order_release); }

    size_t SkipListMemTable::sizeBytes() const { return bytes_.load(std::memory_order_acquire); }

    size_t SkipListMemTable::entryCount() const { return entries_.load(std::memory_order_acquire); }
} // namespace akkaradb::engine::memtable

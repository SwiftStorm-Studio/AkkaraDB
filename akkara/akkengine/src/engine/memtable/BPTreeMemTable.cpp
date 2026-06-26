/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/memtable/BPTreeMemTable.cpp
#include "akk/engine/memtable/BPTreeMemTable.hpp"

#include <algorithm>
#include <cstring>
#include <limits>
#include <optional>
#include <utility>
#include <vector>

#include "akk/core/record/KeyFingerprint.hpp"

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
    } // namespace

    BPTreeMemTable::BPTreeMemTable(
        size_t dataArenaInitialBlockSize,
        size_t dataArenaMaxBlockSize,
        size_t generatorArenaInitialBlockSize,
        size_t generatorArenaMaxBlockSize
    )
        : dataArena_{dataArenaInitialBlockSize, dataArenaMaxBlockSize},
          generatorArena_{generatorArenaInitialBlockSize, generatorArenaMaxBlockSize} {
        Node* initialRoot = makeNode(true);
        root_.store(initialRoot, std::memory_order_release);
    }

    std::span<const uint8_t> BPTreeMemTable::asU8(ByteView view) noexcept {
        return {reinterpret_cast<const uint8_t*>(view.data()), view.size()};
    }

    BPTreeMemTable::Node* BPTreeMemTable::makeNode(bool leaf) {
        Node* node = arenaNew<Node>(leaf);
        bytes_.fetch_add(sizeof(Node), std::memory_order_relaxed);
        return node;
    }

    BPTreeMemTable::VersionChain* BPTreeMemTable::makeChain(const core::OwnedRecord* initial) {
        VersionChain* chain = arenaNew<VersionChain>();
        chain->ring[0].store(initial, std::memory_order_relaxed);
        chain->head.store(0, std::memory_order_relaxed);
        chain->count.store(1, std::memory_order_relaxed);
        entries_.fetch_add(1, std::memory_order_relaxed);
        bytes_.fetch_add(sizeof(VersionChain), std::memory_order_relaxed);
        return chain;
    }

    core::OwnedRecord* BPTreeMemTable::makeRecord(
        std::span<const uint8_t> key,
        std::span<const uint8_t> value,
        uint64_t seq,
        uint8_t flags,
        uint64_t precomputedFp64,
        uint64_t precomputedMk
    ) {
        const uint64_t fp64 = precomputedFp64 != 0 ? precomputedFp64 : (key.empty() ? 0ULL : core::computeKeyFp64(key.data(), key.size()));
        const uint64_t mini = precomputedMk != 0 ? precomputedMk : (key.empty() ? 0ULL : core::buildMiniKey(key.data(), key.size()));

        core::OwnedRecord* record = arenaNew<core::OwnedRecord>();
        core::OwnedRecord::createInplace(*record, key, value, seq, flags, dataArena_, fp64, mini);
        bytes_.fetch_add(sizeof(core::OwnedRecord) + key.size() + value.size(), std::memory_order_relaxed);
        return record;
    }

    void BPTreeMemTable::beginWrite(Node* node) noexcept { node->version.fetch_add(1, std::memory_order_acq_rel); }

    void BPTreeMemTable::endWrite(Node* node) noexcept { node->version.fetch_add(1, std::memory_order_release); }

    int BPTreeMemTable::compareRecordKey(const core::OwnedRecord* record, std::span<const uint8_t> key) noexcept {
        return record->compareKey(key);
    }

    int BPTreeMemTable::compareRecordRecord(const core::OwnedRecord* lhs, const core::OwnedRecord* rhs) noexcept {
        return lhs->compareKey(*rhs);
    }

    uint16_t BPTreeMemTable::findLeafPosition(const Node* leaf, std::span<const uint8_t> key) noexcept {
        const uint16_t keyCount = leaf->keyCount.load(std::memory_order_relaxed);
        return findLeafPosition(leaf, key, keyCount);
    }

    uint16_t BPTreeMemTable::findLeafPosition(const Node* leaf, std::span<const uint8_t> key, uint16_t keyCount) noexcept {
        uint16_t lo = 0;
        uint16_t hi = keyCount;
        while (lo < hi) {
            const uint16_t mid = static_cast<uint16_t>(lo + (hi - lo) / 2);
            const core::OwnedRecord* pivot = leaf->keys[mid].load(std::memory_order_relaxed);
            const int cmp = compareRecordKey(pivot, key);
            if (cmp < 0) { lo = static_cast<uint16_t>(mid + 1); }
            else { hi = mid; }
        }
        return lo;
    }

    uint16_t BPTreeMemTable::findChildIndex(const Node* internal, std::span<const uint8_t> key) noexcept {
        const uint16_t keyCount = internal->keyCount.load(std::memory_order_relaxed);
        uint16_t lo = 0;
        uint16_t hi = keyCount;
        while (lo < hi) {
            const uint16_t mid = static_cast<uint16_t>(lo + (hi - lo) / 2);
            const core::OwnedRecord* pivot = internal->keys[mid].load(std::memory_order_relaxed);
            const int cmp = compareRecordKey(pivot, key);
            if (cmp <= 0) { lo = static_cast<uint16_t>(mid + 1); }
            else { hi = mid; }
        }
        return lo;
    }

    void BPTreeMemTable::appendVersion(VersionChain* chain, const core::OwnedRecord* record, std::atomic<size_t>& entries) noexcept {
        if (chain == nullptr) { return; }

        chain->version.fetch_add(1, std::memory_order_acq_rel);

        const uint8_t prevHead = chain->head.load(std::memory_order_relaxed);
        const uint8_t prevCount = chain->count.load(std::memory_order_relaxed);
        const uint8_t nextHead = static_cast<uint8_t>((prevHead + 1) & (MAX_VERSIONS_PER_KEY - 1));

        chain->ring[nextHead].store(record, std::memory_order_release);

        if (prevCount < MAX_VERSIONS_PER_KEY) {
            chain->count.store(static_cast<uint8_t>(prevCount + 1), std::memory_order_relaxed);
            entries.fetch_add(1, std::memory_order_relaxed);
        }
        chain->head.store(nextHead, std::memory_order_release);

        chain->version.fetch_add(1, std::memory_order_release);
    }

    bool BPTreeMemTable::visibleRecord(VersionChain* chain, uint64_t snapshotSeq, RecordView* out) noexcept {
        if (chain == nullptr || out == nullptr) { return false; }

        for (;;) {
            const uint64_t begin = chain->version.load(std::memory_order_acquire);
            if ((begin & 1ULL) != 0ULL) { continue; }

            const uint8_t head = chain->head.load(std::memory_order_relaxed);
            const uint8_t count = chain->count.load(std::memory_order_relaxed);

            const core::OwnedRecord* selected = nullptr;
            if (count > 0) {
                const core::OwnedRecord* newest = chain->ring[head].load(std::memory_order_relaxed);
                if (newest != nullptr && newest->seq() <= snapshotSeq) { selected = newest; }
                else {
                    for (uint8_t i = 1; i < count; ++i) {
                        const uint8_t index = static_cast<uint8_t>((head - i) & (MAX_VERSIONS_PER_KEY - 1));
                        const core::OwnedRecord* candidate = chain->ring[index].load(std::memory_order_relaxed);
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

    RecordView BPTreeMemTable::toView(const core::OwnedRecord& record) noexcept {
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

    std::optional<BPTreeMemTable::SplitResult> BPTreeMemTable::insertRecursive(Node* node, const core::OwnedRecord* record) {
        const std::span<const uint8_t> key = record->key();

        if (node->isLeaf) {
            const uint16_t pos = findLeafPosition(node, key);
            const uint16_t keyCount = node->keyCount.load(std::memory_order_relaxed);

            if (pos < keyCount) {
                const core::OwnedRecord* existing = node->keys[pos].load(std::memory_order_relaxed);
                if (compareRecordKey(existing, key) == 0) {
                    appendVersion(node->chains[pos].load(std::memory_order_relaxed), record, entries_);
                    return std::nullopt;
                }
            }

            VersionChain* chain = makeChain(record);

            if (keyCount < MAX_KEYS) {
                beginWrite(node);
                for (uint16_t i = keyCount; i > pos; --i) {
                    node->keys[i].store(node->keys[i - 1].load(std::memory_order_relaxed), std::memory_order_relaxed);
                    node->chains[i].store(node->chains[i - 1].load(std::memory_order_relaxed), std::memory_order_relaxed);
                }
                node->keys[pos].store(record, std::memory_order_release);
                node->chains[pos].store(chain, std::memory_order_release);
                node->keyCount.store(static_cast<uint16_t>(keyCount + 1), std::memory_order_release);
                endWrite(node);
                return std::nullopt;
            }

            std::array<const core::OwnedRecord*, MAX_KEYS + 1> allKeys{};
            std::array<VersionChain*, MAX_KEYS + 1> allChains{};

            uint16_t wi = 0;
            for (uint16_t i = 0; i < keyCount; ++i) {
                if (wi == pos) {
                    allKeys[wi] = record;
                    allChains[wi] = chain;
                    ++wi;
                }
                allKeys[wi] = node->keys[i].load(std::memory_order_relaxed);
                allChains[wi] = node->chains[i].load(std::memory_order_relaxed);
                ++wi;
            }
            if (wi == pos) {
                allKeys[wi] = record;
                allChains[wi] = chain;
                ++wi;
            }

            Node* right = makeNode(true);

            const uint16_t total = static_cast<uint16_t>(MAX_KEYS + 1);
            const uint16_t leftCount = static_cast<uint16_t>(total / 2);
            const uint16_t rightCount = static_cast<uint16_t>(total - leftCount);

            beginWrite(node);

            for (uint16_t i = 0; i < leftCount; ++i) {
                node->keys[i].store(allKeys[i], std::memory_order_relaxed);
                node->chains[i].store(allChains[i], std::memory_order_relaxed);
            }
            for (uint16_t i = leftCount; i < MAX_KEYS; ++i) {
                node->keys[i].store(nullptr, std::memory_order_relaxed);
                node->chains[i].store(nullptr, std::memory_order_relaxed);
            }
            node->keyCount.store(leftCount, std::memory_order_release);

            for (uint16_t i = 0; i < rightCount; ++i) {
                right->keys[i].store(allKeys[leftCount + i], std::memory_order_relaxed);
                right->chains[i].store(allChains[leftCount + i], std::memory_order_relaxed);
            }
            for (uint16_t i = rightCount; i < MAX_KEYS; ++i) {
                right->keys[i].store(nullptr, std::memory_order_relaxed);
                right->chains[i].store(nullptr, std::memory_order_relaxed);
            }
            right->keyCount.store(rightCount, std::memory_order_release);

            Node* oldNext = node->nextLeaf.load(std::memory_order_relaxed);
            right->nextLeaf.store(oldNext, std::memory_order_release);
            node->nextLeaf.store(right, std::memory_order_release);

            endWrite(node);

            SplitResult split;
            split.separator = right->keys[0].load(std::memory_order_relaxed);
            split.right = right;
            return split;
        }

        const uint16_t childIndex = findChildIndex(node, key);
        Node* child = node->children[childIndex].load(std::memory_order_relaxed);
        if (child == nullptr) { return std::nullopt; }

        std::optional<SplitResult> childSplit = insertRecursive(child, record);
        if (!childSplit.has_value()) { return std::nullopt; }

        const uint16_t keyCount = node->keyCount.load(std::memory_order_relaxed);
        const uint16_t insertPos = childIndex;

        if (keyCount < MAX_KEYS) {
            beginWrite(node);
            for (uint16_t i = keyCount; i > insertPos; --i) {
                node->keys[i].store(node->keys[i - 1].load(std::memory_order_relaxed), std::memory_order_relaxed);
            }
            for (uint16_t i = static_cast<uint16_t>(keyCount + 1); i > static_cast<uint16_t>(insertPos + 1); --i) {
                node->children[i].store(node->children[i - 1].load(std::memory_order_relaxed), std::memory_order_relaxed);
            }
            node->keys[insertPos].store(childSplit->separator, std::memory_order_release);
            node->children[insertPos + 1].store(childSplit->right, std::memory_order_release);
            node->keyCount.store(static_cast<uint16_t>(keyCount + 1), std::memory_order_release);
            endWrite(node);
            return std::nullopt;
        }

        std::array<const core::OwnedRecord*, MAX_KEYS + 1> allKeys{};
        std::array<Node*, MAX_KEYS + 2> allChildren{};

        for (uint16_t i = 0; i < keyCount; ++i) { allKeys[i] = node->keys[i].load(std::memory_order_relaxed); }
        for (uint16_t i = 0; i < static_cast<uint16_t>(keyCount + 1); ++i) {
            allChildren[i] = node->children[i].load(std::memory_order_relaxed);
        }

        for (uint16_t i = keyCount; i > insertPos; --i) { allKeys[i] = allKeys[i - 1]; }
        allKeys[insertPos] = childSplit->separator;

        for (uint16_t i = static_cast<uint16_t>(keyCount + 1); i > static_cast<uint16_t>(insertPos + 1); --i) {
            allChildren[i] = allChildren[i - 1];
        }
        allChildren[insertPos + 1] = childSplit->right;

        const uint16_t totalKeys = static_cast<uint16_t>(MAX_KEYS + 1);
        const uint16_t mid = static_cast<uint16_t>(totalKeys / 2);

        Node* right = makeNode(false);

        beginWrite(node);

        for (uint16_t i = 0; i < mid; ++i) {
            node->keys[i].store(allKeys[i], std::memory_order_relaxed);
            node->children[i].store(allChildren[i], std::memory_order_relaxed);
        }
        node->children[mid].store(allChildren[mid], std::memory_order_relaxed);
        for (uint16_t i = mid; i < MAX_KEYS; ++i) { node->keys[i].store(nullptr, std::memory_order_relaxed); }
        for (uint16_t i = static_cast<uint16_t>(mid + 1); i < MAX_KEYS + 1; ++i) {
            node->children[i].store(nullptr, std::memory_order_relaxed);
        }
        node->keyCount.store(mid, std::memory_order_release);

        const uint16_t rightKeyCount = static_cast<uint16_t>(totalKeys - mid - 1);
        for (uint16_t i = 0; i < rightKeyCount; ++i) {
            right->keys[i].store(allKeys[mid + 1 + i], std::memory_order_relaxed);
            right->children[i].store(allChildren[mid + 1 + i], std::memory_order_relaxed);
        }
        right->children[rightKeyCount].store(allChildren[totalKeys], std::memory_order_relaxed);
        for (uint16_t i = rightKeyCount; i < MAX_KEYS; ++i) { right->keys[i].store(nullptr, std::memory_order_relaxed); }
        for (uint16_t i = static_cast<uint16_t>(rightKeyCount + 1); i < MAX_KEYS + 1; ++i) {
            right->children[i].store(nullptr, std::memory_order_relaxed);
        }
        right->keyCount.store(rightKeyCount, std::memory_order_release);

        const core::OwnedRecord* promoted = allKeys[mid];

        endWrite(node);

        SplitResult split;
        split.separator = promoted;
        split.right = right;
        return split;
    }

    BPTreeMemTable::Node* BPTreeMemTable::descendToCandidateLeaf(std::span<const uint8_t> key) const noexcept {
        Node* current = root_.load(std::memory_order_acquire);
        while (current != nullptr && !current->isLeaf) {
            Node* nextChild = nullptr;
            for (;;) {
                const uint64_t begin = current->version.load(std::memory_order_acquire);
                if ((begin & 1ULL) != 0ULL) { continue; }
                const uint16_t childIndex = findChildIndex(current, key);
                nextChild = current->children[childIndex].load(std::memory_order_acquire);
                const uint64_t end = current->version.load(std::memory_order_acquire);
                if (begin == end && (end & 1ULL) == 0ULL) { break; }
            }
            current = nextChild;
        }
        return current;
    }

    Status BPTreeMemTable::put(
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

        const auto keyU8 = asU8(key);
        const auto valueU8 = asU8(value);
        const core::OwnedRecord* record = makeRecord(keyU8, valueU8, seq, flags, precomputedFp64, precomputedMk);

        Node* currentRoot = root_.load(std::memory_order_acquire);
        std::optional<SplitResult> split = insertRecursive(currentRoot, record);
        if (!split.has_value()) { return Status::OK(); }

        Node* newRoot = makeNode(false);
        newRoot->keys[0].store(split->separator, std::memory_order_relaxed);
        newRoot->children[0].store(currentRoot, std::memory_order_relaxed);
        newRoot->children[1].store(split->right, std::memory_order_relaxed);
        newRoot->keyCount.store(1, std::memory_order_relaxed);

        root_.store(newRoot, std::memory_order_release);
        return Status::OK();
    }

    bool BPTreeMemTable::get(ByteView key, uint64_t snapshotSeq, RecordView* out) const {
        if (out == nullptr) { return false; }

        const std::span<const uint8_t> target = asU8(key);
        Node* leaf = descendToCandidateLeaf(target);

        while (leaf != nullptr) {
            uint16_t pos = 0;
            uint16_t keyCount = 0;
            int cmp = 1;
            VersionChain* chain = nullptr;

            for (;;) {
                const uint64_t begin = leaf->version.load(std::memory_order_acquire);
                if ((begin & 1ULL) != 0ULL) { continue; }
                keyCount = leaf->keyCount.load(std::memory_order_acquire);
                pos = findLeafPosition(leaf, target, keyCount);
                cmp = 1;
                chain = nullptr;
                if (pos < keyCount) {
                    const core::OwnedRecord* candidateKey = leaf->keys[pos].load(std::memory_order_relaxed);
                    cmp = compareRecordKey(candidateKey, target);
                    if (cmp == 0) { chain = leaf->chains[pos].load(std::memory_order_relaxed); }
                }
                const uint64_t end = leaf->version.load(std::memory_order_acquire);
                if (begin == end && (end & 1ULL) == 0ULL) { break; }
            }

            if (pos < keyCount) {
                if (cmp == 0) { return visibleRecord(chain, snapshotSeq, out); }
                if (cmp > 0) { return false; }
            }

            leaf = leaf->nextLeaf.load(std::memory_order_acquire);
        }

        return false;
    }

    ArenaGenerator<RecordView> BPTreeMemTable::iterateFrozenSnapshot(uint64_t snapshotSeq) const {
        Node* node = root_.load(std::memory_order_acquire);
        while (node != nullptr && !node->isLeaf) { node = node->children[0].load(std::memory_order_acquire); }

        while (node != nullptr) {
            const uint16_t keyCount = node->keyCount.load(std::memory_order_acquire);
            for (uint16_t i = 0; i < keyCount; ++i) {
                VersionChain* chain = node->chains[i].load(std::memory_order_acquire);
                RecordView visible;
                if (visibleRecord(chain, snapshotSeq, &visible)) { co_yield visible; }
            }
            node = node->nextLeaf.load(std::memory_order_acquire);
        }
    }

    ArenaGenerator<RecordView> BPTreeMemTable::iterateSnapshot(uint64_t snapshotSeq) const {
        std::vector<RecordView> visibleRecords;
        visibleRecords.reserve(entryCount());
        bool orderedUnique = true;
        bool hasPrev = false;
        RecordView prev;

        Node* node = root_.load(std::memory_order_acquire);
        while (node != nullptr && !node->isLeaf) { node = node->children[0].load(std::memory_order_acquire); }

        while (node != nullptr) {
            std::array<VersionChain*, MAX_KEYS> chains{};
            uint16_t keyCount = 0;
            for (;;) {
                const uint64_t begin = node->version.load(std::memory_order_acquire);
                if ((begin & 1ULL) != 0ULL) { continue; }
                keyCount = node->keyCount.load(std::memory_order_acquire);
                for (uint16_t i = 0; i < keyCount; ++i) { chains[i] = node->chains[i].load(std::memory_order_acquire); }
                const uint64_t end = node->version.load(std::memory_order_acquire);
                if (begin == end && (end & 1ULL) == 0ULL) { break; }
            }

            for (uint16_t i = 0; i < keyCount; ++i) {
                RecordView visible;
                if (visibleRecord(chains[i], snapshotSeq, &visible)) {
                    if (hasPrev) { if (prev.compareKey(visible) >= 0) { orderedUnique = false; } }
                    prev = visible;
                    hasPrev = true;
                    visibleRecords.push_back(visible);
                }
            }

            node = node->nextLeaf.load(std::memory_order_acquire);
        }

        if (orderedUnique) {
            for (const RecordView& rec : visibleRecords) { co_yield rec; }
            co_return;
        }

        std::sort(
            visibleRecords.begin(),
            visibleRecords.end(),
            [](const RecordView& a, const RecordView& b) {
                const int cmp = a.compareKey(b);
                if (cmp != 0) { return cmp < 0; }
                return a.seq() > b.seq();
            }
        );

        for (size_t i = 0; i < visibleRecords.size(); ++i) {
            if (i > 0 && visibleRecords[i - 1].compareKey(visibleRecords[i]) == 0) { continue; }
            co_yield visibleRecords[i];
        }
    }

    ArenaGenerator<RecordView> BPTreeMemTable::iterateFrozenSnapshotRange(
        uint64_t snapshotSeq,
        std::vector<uint8_t> startKey,
        std::vector<uint8_t> endKey
    ) const {
        const std::span<const uint8_t> start{startKey.data(), startKey.size()};
        const std::span<const uint8_t> end{endKey.data(), endKey.size()};
        if (!start.empty() && !end.empty() && compareKeyBytes(start, end) >= 0) { co_return; }

        Node* node = root_.load(std::memory_order_acquire);
        uint16_t firstPos = 0;
        bool firstLeaf = true;

        if (start.empty()) { while (node != nullptr && !node->isLeaf) { node = node->children[0].load(std::memory_order_acquire); } }
        else {
            while (node != nullptr && !node->isLeaf) {
                const uint16_t childIndex = findChildIndex(node, start);
                node = node->children[childIndex].load(std::memory_order_acquire);
            }

            while (node != nullptr) {
                const uint16_t keyCount = node->keyCount.load(std::memory_order_acquire);
                firstPos = findLeafPosition(node, start, keyCount);
                if (firstPos < keyCount) { break; }
                node = node->nextLeaf.load(std::memory_order_acquire);
                firstPos = 0;
            }
        }

        while (node != nullptr) {
            const uint16_t keyCount = node->keyCount.load(std::memory_order_acquire);
            const uint16_t startPos = firstLeaf ? firstPos : 0;
            firstLeaf = false;

            for (uint16_t i = startPos; i < keyCount; ++i) {
                const core::OwnedRecord* keyRecord = node->keys[i].load(std::memory_order_acquire);
                if (keyRecord == nullptr) { continue; }
                if (!end.empty() && compareRecordKey(keyRecord, end) >= 0) { co_return; }

                VersionChain* chain = node->chains[i].load(std::memory_order_acquire);
                RecordView visible;
                if (visibleRecord(chain, snapshotSeq, &visible)) { co_yield visible; }
            }

            node = node->nextLeaf.load(std::memory_order_acquire);
        }
    }

    ArenaGenerator<RecordView> BPTreeMemTable::iterateSnapshotRange(
        uint64_t snapshotSeq,
        std::vector<uint8_t> startKey,
        std::vector<uint8_t> endKey
    ) const {
        const std::span<const uint8_t> start{startKey.data(), startKey.size()};
        const std::span<const uint8_t> end{endKey.data(), endKey.size()};
        if (!start.empty() && !end.empty() && compareKeyBytes(start, end) >= 0) { co_return; }

        std::vector<RecordView> visibleRecords;
        const size_t reserveHint = std::min<size_t>(entryCount(), 1024);
        visibleRecords.reserve(reserveHint);
        bool orderedUnique = true;
        bool hasPrev = false;
        RecordView prev;

        Node* node = nullptr;
        uint16_t firstPos = 0;
        bool firstLeaf = true;

        if (start.empty()) {
            node = root_.load(std::memory_order_acquire);
            while (node != nullptr && !node->isLeaf) { node = node->children[0].load(std::memory_order_acquire); }
        }
        else {
            node = descendToCandidateLeaf(start);
            while (node != nullptr) {
                Node* nextLeaf = nullptr;
                uint16_t keyCount = 0;
                for (;;) {
                    const uint64_t begin = node->version.load(std::memory_order_acquire);
                    if ((begin & 1ULL) != 0ULL) { continue; }
                    keyCount = node->keyCount.load(std::memory_order_acquire);
                    firstPos = findLeafPosition(node, start, keyCount);
                    nextLeaf = node->nextLeaf.load(std::memory_order_acquire);
                    const uint64_t endVersion = node->version.load(std::memory_order_acquire);
                    if (begin == endVersion && (endVersion & 1ULL) == 0ULL) { break; }
                }
                if (firstPos < keyCount) { break; }
                node = nextLeaf;
                firstPos = 0;
            }
        }

        while (node != nullptr) {
            std::array<const core::OwnedRecord*, MAX_KEYS> keys{};
            std::array<VersionChain*, MAX_KEYS> chains{};
            Node* nextLeaf = nullptr;
            uint16_t keyCount = 0;

            for (;;) {
                const uint64_t begin = node->version.load(std::memory_order_acquire);
                if ((begin & 1ULL) != 0ULL) { continue; }
                keyCount = node->keyCount.load(std::memory_order_acquire);
                for (uint16_t i = 0; i < keyCount; ++i) {
                    keys[i] = node->keys[i].load(std::memory_order_acquire);
                    chains[i] = node->chains[i].load(std::memory_order_acquire);
                }
                nextLeaf = node->nextLeaf.load(std::memory_order_acquire);
                const uint64_t endVersion = node->version.load(std::memory_order_acquire);
                if (begin == endVersion && (endVersion & 1ULL) == 0ULL) { break; }
            }

            const uint16_t startPos = firstLeaf ? firstPos : 0;
            firstLeaf = false;

            for (uint16_t i = startPos; i < keyCount; ++i) {
                const core::OwnedRecord* keyRecord = keys[i];
                if (keyRecord == nullptr) { continue; }
                if (!start.empty() && compareRecordKey(keyRecord, start) < 0) { continue; }
                if (!end.empty() && compareRecordKey(keyRecord, end) >= 0) {
                    node = nullptr;
                    break;
                }

                RecordView visible;
                if (!visibleRecord(chains[i], snapshotSeq, &visible)) { continue; }
                if (hasPrev && prev.compareKey(visible) >= 0) { orderedUnique = false; }
                prev = visible;
                hasPrev = true;
                visibleRecords.push_back(visible);
            }

            if (node != nullptr) { node = nextLeaf; }
        }

        if (orderedUnique) {
            for (const RecordView& rec : visibleRecords) { co_yield rec; }
            co_return;
        }

        std::sort(
            visibleRecords.begin(),
            visibleRecords.end(),
            [](const RecordView& a, const RecordView& b) {
                const int cmp = a.compareKey(b);
                if (cmp != 0) { return cmp < 0; }
                return a.seq() > b.seq();
            }
        );

        for (size_t i = 0; i < visibleRecords.size(); ++i) {
            if (i > 0 && visibleRecords[i - 1].compareKey(visibleRecords[i]) == 0) { continue; }
            co_yield visibleRecords[i];
        }
    }

    ArenaGenerator<RecordView> BPTreeMemTable::iterator(ByteView startKey, ByteView endKey, uint64_t snapshotSeq) const {
        const std::span<const uint8_t> start = asU8(startKey);
        const std::span<const uint8_t> end = asU8(endKey);
        const bool frozen = frozen_.load(std::memory_order_acquire);

        std::lock_guard<std::mutex> lock{generatorArenaMutex_};
        if (start.empty() && end.empty()) {
            return ArenaGenerator<RecordView>::withArena(
                generatorArena_,
                [this, snapshotSeq, frozen]() { return frozen ? iterateFrozenSnapshot(snapshotSeq) : iterateSnapshot(snapshotSeq); }
            );
        }

        std::vector<uint8_t> startOwned(start.begin(), start.end());
        std::vector<uint8_t> endOwned(end.begin(), end.end());
        return ArenaGenerator<RecordView>::withArena(
            generatorArena_,
            [this, snapshotSeq, frozen, startOwned = std::move(startOwned), endOwned = std::move(endOwned)]() mutable {
                return frozen
                           ? iterateFrozenSnapshotRange(snapshotSeq, std::move(startOwned), std::move(endOwned))
                           : iterateSnapshotRange(snapshotSeq, std::move(startOwned), std::move(endOwned));
            }
        );
    }

    void BPTreeMemTable::freeze() { frozen_.store(true, std::memory_order_release); }

    size_t BPTreeMemTable::sizeBytes() const { return bytes_.load(std::memory_order_acquire); }

    size_t BPTreeMemTable::entryCount() const { return entries_.load(std::memory_order_acquire); }
} // namespace akkaradb::engine::memtable

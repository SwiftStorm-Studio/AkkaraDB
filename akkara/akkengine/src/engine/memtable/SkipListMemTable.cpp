/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

// akkengine/src/engine/memtable/SkipListMemTable.cpp
#include "akk/engine/memtable/SkipListMemTable.hpp"

#include <algorithm>
#include <cstring>
#include <limits>
#include <new>
#include <utility>
#include <vector>

#include "akk/core/record/KeyFingerprint.hpp"
#include "akk/core/record/MemHdr16.hpp"

namespace akkaradb::engine::memtable {
    namespace {
        template <typename T, typename... Args>
        [[nodiscard]] T* arenaNew(BufferArena& arena, Args&&... args) {
            std::byte* mem = arena.allocate(sizeof(T), alignof(T));
            return new(mem) T(std::forward<Args>(args)...);
        }

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

    SkipListMemTable::SkipListMemTable(
        size_t dataArenaInitialBlockSize,
        size_t dataArenaMaxBlockSize,
        size_t generatorArenaInitialBlockSize,
        size_t generatorArenaMaxBlockSize
    )
        : dataArena_{dataArenaInitialBlockSize, dataArenaMaxBlockSize},
          generatorArena_{generatorArenaInitialBlockSize, generatorArenaMaxBlockSize} {
        head_ = arenaNew<Node>(dataArena_);
        head_->level = MAX_LEVEL;
        head_->keyRecord = nullptr;
        for (uint8_t i = 0; i < MAX_LEVEL; ++i) { head_->next[i].store(nullptr, std::memory_order_relaxed); }
        currentMaxLevel_.store(1, std::memory_order_relaxed);

        bytes_.store(sizeof(Node), std::memory_order_relaxed);
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

    SkipListMemTable::Node* SkipListMemTable::newNode(const core::OwnedRecord* initialRecord, uint8_t level) {
        Node* node = arenaNew<Node>(dataArena_);
        node->level = level;
        node->keyRecord = initialRecord;
        node->head.store(0, std::memory_order_relaxed);
        node->count.store(1, std::memory_order_relaxed);
        node->version.store(0, std::memory_order_relaxed);

        node->ring[0].record.store(initialRecord, std::memory_order_relaxed);
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

        core::OwnedRecord* record = arenaNew<core::OwnedRecord>(dataArena_);
        core::OwnedRecord::createInplace(*record, key, value, seq, flags, dataArena_, fp64, mini);
        return record;
    }

    int SkipListMemTable::compareNodeKey(const Node* node, std::span<const uint8_t> key) noexcept {
        return node->keyRecord->compareKey(key);
    }

    SkipListMemTable::Node* SkipListMemTable::findNode(
        std::span<const uint8_t> key,
        std::array<Node*, MAX_LEVEL>* update,
        bool writerFastPath
    ) const noexcept {
        const uint8_t topLevel = currentMaxLevel_.load(std::memory_order_relaxed);
        const int startLevel = static_cast<int>(topLevel > 0 ? topLevel - 1 : 0);

        if (writerFastPath) {
            Node* current = head_;
            if (update != nullptr) {
                for (int level = startLevel; level >= 0; --level) {
                    Node* next = current->next[level].load(std::memory_order_relaxed);
                    while (next != nullptr && compareNodeKey(next, key) < 0) {
                        current = next;
                        next = current->next[level].load(std::memory_order_relaxed);
                    }
                    (*update)[static_cast<size_t>(level)] = current;
                }
                return current->next[0].load(std::memory_order_relaxed);
            }
            for (int level = startLevel; level >= 0; --level) {
                Node* next = current->next[level].load(std::memory_order_relaxed);
                while (next != nullptr && compareNodeKey(next, key) < 0) {
                    current = next;
                    next = current->next[level].load(std::memory_order_relaxed);
                }
            }
            return current->next[0].load(std::memory_order_relaxed);
        }

        Node* current = head_;
        if (update != nullptr) {
            for (int level = startLevel; level >= 0; --level) {
                Node* next = current->next[level].load(std::memory_order_acquire);
                while (next != nullptr && compareNodeKey(next, key) < 0) {
                    current = next;
                    next = current->next[level].load(std::memory_order_acquire);
                }
                (*update)[static_cast<size_t>(level)] = current;
            }
            return current->next[0].load(std::memory_order_acquire);
        }
        for (int level = startLevel; level >= 0; --level) {
            Node* next = current->next[level].load(std::memory_order_acquire);
            while (next != nullptr && compareNodeKey(next, key) < 0) {
                current = next;
                next = current->next[level].load(std::memory_order_acquire);
            }
        }
        return current->next[0].load(std::memory_order_acquire);
    }

    bool SkipListMemTable::visibleRecord(const Node* node, uint64_t snapshotSeq, RecordView* out) const noexcept {
        for (;;) {
            const uint64_t begin = node->version.load(std::memory_order_acquire);
            if ((begin & 1ULL) != 0ULL) { continue; }

            const uint8_t count = node->count.load(std::memory_order_relaxed);
            const uint8_t headLocal = node->head.load(std::memory_order_relaxed);

            const core::OwnedRecord* selected = nullptr;

            if (count > 0) {
                // Common case: latest snapshot reads. Check the newest slot first
                // and skip ring scan when it is visible.
                const core::OwnedRecord* newest = node->ring[headLocal].record.load(std::memory_order_relaxed);
                if (newest != nullptr && newest->seq() <= snapshotSeq) { selected = newest; }
                else {
                    for (uint8_t i = 1; i < count; ++i) {
                        const uint8_t index = static_cast<uint8_t>((headLocal - i) & (MAX_VERSIONS_PER_KEY - 1));
                        const core::OwnedRecord* candidate = node->ring[index].record.load(std::memory_order_relaxed);
                        if (candidate != nullptr && candidate->seq() <= snapshotSeq) {
                            selected = candidate;
                            break;
                        }
                    }
                }
            }

            const uint64_t end = node->version.load(std::memory_order_acquire);
            if (begin == end && (end & 1ULL) == 0ULL) {
                if (selected == nullptr) { return false; }
                *out = toView(*selected);
                return true;
            }
        }
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

        const auto keyU8 = asU8(key);
        const auto valueU8 = asU8(value);
        std::array<Node*, MAX_LEVEL> update;
        Node* candidate = findNode(keyU8, &update, true);

        const core::OwnedRecord* record = makeRecord(keyU8, valueU8, seq, flags, precomputedFp64, precomputedMk);
        bytes_.fetch_add(sizeof(core::OwnedRecord) + key.size() + value.size(), std::memory_order_relaxed);

        if (candidate != nullptr && compareNodeKey(candidate, keyU8) == 0) {
            candidate->version.fetch_add(1, std::memory_order_acq_rel); // enter write (odd)

            const uint8_t prevHead = candidate->head.load(std::memory_order_relaxed);
            const uint8_t prevCount = candidate->count.load(std::memory_order_relaxed);
            const uint8_t nextHead = static_cast<uint8_t>((prevHead + 1) & (MAX_VERSIONS_PER_KEY - 1));

            candidate->ring[nextHead].record.store(record, std::memory_order_release);
            if (prevCount < MAX_VERSIONS_PER_KEY) {
                candidate->count.store(static_cast<uint8_t>(prevCount + 1), std::memory_order_relaxed);
                entries_.fetch_add(1, std::memory_order_relaxed);
            }
            candidate->head.store(nextHead, std::memory_order_release);
            candidate->version.fetch_add(1, std::memory_order_release); // leave write (even)
            return Status::OK();
        }

        const uint8_t level = randomLevel();
        const uint8_t observedMax = currentMaxLevel_.load(std::memory_order_relaxed);
        if (level > observedMax) { for (uint8_t i = observedMax; i < level; ++i) { update[i] = head_; } }
        Node* node = newNode(record, level);
        bytes_.fetch_add(sizeof(Node), std::memory_order_relaxed);
        entries_.fetch_add(1, std::memory_order_relaxed);

        for (uint8_t i = 0; i < level; ++i) {
            Node* next = update[i]->next[i].load(std::memory_order_relaxed);
            node->next[i].store(next, std::memory_order_relaxed);
        }

        for (int i = static_cast<int>(level) - 1; i >= 0; --i) {
            update[static_cast<size_t>(i)]->next[static_cast<size_t>(i)].store(node, std::memory_order_release);
        }
        if (level > observedMax) { currentMaxLevel_.store(level, std::memory_order_release); }

        return Status::OK();
    }

    bool SkipListMemTable::get(ByteView key, uint64_t snapshotSeq, RecordView* out) const {
        if (out == nullptr) { return false; }

        const auto keyU8 = asU8(key);
        Node* node = findNode(keyU8, nullptr, false);
        if (node == nullptr || compareNodeKey(node, keyU8) != 0) { return false; }

        return visibleRecord(node, snapshotSeq, out);
    }

    ArenaGenerator<RecordView> SkipListMemTable::iterateSnapshot(uint64_t snapshotSeq) const {
        Node* current = head_->next[0].load(std::memory_order_acquire);
        while (current != nullptr) {
            RecordView visible;
            if (visibleRecord(current, snapshotSeq, &visible)) { co_yield visible; }
            current = current->next[0].load(std::memory_order_acquire);
        }
    }

    ArenaGenerator<RecordView> SkipListMemTable::iterateSnapshotRange(
        uint64_t snapshotSeq,
        std::vector<uint8_t> startKey,
        std::vector<uint8_t> endKey
    ) const {
        const std::span<const uint8_t> start{startKey.data(), startKey.size()};
        const std::span<const uint8_t> end{endKey.data(), endKey.size()};
        if (!start.empty() && !end.empty() && compareKeyBytes(start, end) >= 0) { co_return; }

        Node* current = start.empty() ? head_->next[0].load(std::memory_order_acquire) : findNode(start, nullptr, false);

        while (current != nullptr) {
            if (!end.empty() && compareNodeKey(current, end) >= 0) { break; }

            RecordView visible;
            if (visibleRecord(current, snapshotSeq, &visible)) {
                if (!start.empty() && visible.compareKey(start) < 0) {
                    current = current->next[0].load(std::memory_order_acquire);
                    continue;
                }
                if (!end.empty() && visible.compareKey(end) >= 0) { break; }
                co_yield visible;
            }
            current = current->next[0].load(std::memory_order_acquire);
        }
    }

    ArenaGenerator<RecordView> SkipListMemTable::iterator(ByteView startKey, ByteView endKey, uint64_t snapshotSeq) const {
        const std::span<const uint8_t> start = asU8(startKey);
        const std::span<const uint8_t> end = asU8(endKey);
        std::vector<uint8_t> startOwned(start.begin(), start.end());
        std::vector<uint8_t> endOwned(end.begin(), end.end());

        std::lock_guard<std::mutex> lock{generatorArenaMutex_};
        return ArenaGenerator<RecordView>::withArena(
            generatorArena_,
            [this, snapshotSeq, startOwned = std::move(startOwned), endOwned = std::move(endOwned)]() mutable {
                return iterateSnapshotRange(snapshotSeq, std::move(startOwned), std::move(endOwned));
            }
        );
    }

    void SkipListMemTable::freeze() { frozen_.store(true, std::memory_order_release); }

    size_t SkipListMemTable::sizeBytes() const { return bytes_.load(std::memory_order_acquire); }

    size_t SkipListMemTable::entryCount() const { return entries_.load(std::memory_order_acquire); }
} // namespace akkaradb::engine::memtable

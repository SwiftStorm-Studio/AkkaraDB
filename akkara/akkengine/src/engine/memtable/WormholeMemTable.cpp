/*
 * AkkaraDB
 * Copyright (C) 2026 Swift Storm Studio
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. See https://mozilla.org/MPL/2.0/.
 */
#include "akk/engine/memtable/WormholeMemTable.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cstring>
#include <mutex>
#include <new>
#include <shared_mutex>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

#include "akk/core/record/OwnedRecord.hpp"
#include "akk/engine/memtable/detail/VersionHistory.hpp"
#include "akk/engine/memtable/detail/WormholeIndex.hpp"

namespace akkaradb::engine::memtable {
    namespace {
        using namespace detail::wormhole;
        std::span<const uint8_t> asU8(ByteView bytes) noexcept {
            return {reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size()};
        }
        int compare(std::span<const uint8_t> a, std::span<const uint8_t> b) noexcept {
            const size_t size = std::min(a.size(), b.size());
            const int cmp = size == 0 ? 0 : std::memcmp(a.data(), b.data(), size);
            if (cmp != 0) { return cmp; }
            return a.size() < b.size() ? -1 : a.size() > b.size() ? 1 : 0;
        }
    }

    class WormholeMemTable::Impl {
        static constexpr size_t INLINE_SCAN_ROWS = 32;
        static constexpr size_t SORT_RUN_SIZE = 16;
        static constexpr size_t WRITE_MERGE_LIMIT = 64;
        struct ScanBound {
            std::array<uint8_t, 32> local{};
            std::vector<uint8_t> extended;
            size_t size;
            explicit ScanBound(std::span<const uint8_t> key) : size{key.size()} {
                if (size > local.size()) { extended.assign(key.begin(), key.end()); }
                else if (size != 0) { std::memcpy(local.data(), key.data(), size); }
            }
            [[nodiscard]] std::span<const uint8_t> view() const noexcept {
                return {size <= local.size() ? local.data() : extended.data(), size};
            }
        };
        struct Entry {
            const core::OwnedRecord* keyRecord{nullptr};
            detail::VersionHistory history;
            const core::OwnedRecord** ring{nullptr};
            uint16_t head{0};
            uint16_t count{0};
            uint16_t tag{0};
        };
        struct Leaf {
            Leaf* previous{nullptr};
            Leaf* next{nullptr};
            std::span<const uint8_t> anchor;
            size_t count{0};
            uint8_t runCount{0};
            std::array<uint16_t, LEAF_CAPACITY / SORT_RUN_SIZE + 1> runEnds{};
            std::array<Entry*, LEAF_CAPACITY> entries{};
            std::array<uint16_t, LEAF_CAPACITY> tags{};
            std::array<uint8_t, LEAF_CAPACITY> tagSlots{};
            std::array<uint8_t, LEAF_CAPACITY> ordered{};
        };
        struct Meta {
            PrefixKey key;
            std::array<uint64_t, 4> children{};
            Leaf* terminal{nullptr};
            Leaf* leftmost{nullptr};
            Leaf* rightmost{nullptr};
        };
        using Directory = TaggedDirectory<Meta>;

        mutable std::shared_mutex mutex_;
        core::BufferArena arena_;
        std::atomic<size_t> arenaBytes_{0};
        std::atomic<size_t> records_{0};
        Directory directory_;
        const HashRange hashRange_{selectHashRange()};
        Meta* root_{nullptr};
        Leaf* first_{nullptr};
        size_t maxAnchorLength_{0};
        uint16_t capacity_;
        bool retainAll_{false};
        bool sequenceOrder_{false};
        bool frozen_{false};
        size_t generatorInitial_;
        size_t generatorMax_;

        template <typename T, typename... Args>
        T* allocate(Args&&... args) {
            auto* memory = arena_.allocate(sizeof(T), alignof(T));
            arenaBytes_.fetch_add(sizeof(T), std::memory_order_relaxed);
            return new(memory) T(std::forward<Args>(args)...);
        }
        static std::span<const uint8_t> keyOf(const Entry* entry) noexcept { return entry->keyRecord->key(); }
        static Entry* orderedEntry(const Leaf* leaf, size_t i) noexcept { return leaf->entries[leaf->ordered[i]]; }
        static size_t lowerBound(const Leaf* leaf, std::span<const uint8_t> key) noexcept {
            return static_cast<size_t>(std::lower_bound(leaf->ordered.begin(), leaf->ordered.begin() + leaf->count, key,
                [leaf](uint8_t slot, auto bound) { return compare(keyOf(leaf->entries[slot]), bound) < 0; }) - leaf->ordered.begin());
        }
        static size_t tagPosition(const Leaf* leaf, uint16_t tag) noexcept {
            if (leaf->count == 0) { return 0; }
            // Section 3.2: predict the rank from the tag's fraction of its space,
            // then correct in either direction, including all equal-tag entries.
            size_t i = (leaf->count * static_cast<size_t>(tag)) >> 16;
            while (i != 0 && leaf->tags[i - 1] >= tag) { --i; }
            while (i < leaf->count && leaf->tags[i] < tag) { ++i; }
            return i;
        }
        static Entry* findEntry(const Leaf* leaf, std::span<const uint8_t> key, uint16_t tag) noexcept {
            for (size_t i = tagPosition(leaf, tag); i < leaf->count && leaf->tags[i] == tag; ++i) {
                auto* entry = leaf->entries[leaf->tagSlots[i]];
                if (compare(keyOf(entry), key) == 0) { return entry; }
            }
            return nullptr;
        }
        static void appendEntry(Leaf* leaf, Entry* entry) noexcept {
            const size_t position = tagPosition(leaf, entry->tag);
            std::move_backward(leaf->tags.begin() + position, leaf->tags.begin() + leaf->count, leaf->tags.begin() + leaf->count + 1);
            std::move_backward(leaf->tagSlots.begin() + position, leaf->tagSlots.begin() + leaf->count, leaf->tagSlots.begin() + leaf->count + 1);
            const auto slot = static_cast<uint8_t>(leaf->count);
            leaf->entries[slot] = entry;
            leaf->tags[position] = entry->tag;
            leaf->tagSlots[position] = slot;
            leaf->ordered[leaf->count++] = slot;
        }
        static void mergeRuns(Leaf* leaf, size_t run) noexcept {
            const size_t begin = run == 0 ? 0 : leaf->runEnds[run - 1];
            const size_t middle = leaf->runEnds[run], end = leaf->runEnds[run + 1];
            std::array<uint8_t, LEAF_CAPACITY> merged;
            const auto less = [leaf](uint8_t a, uint8_t b) { return compare(keyOf(leaf->entries[a]), keyOf(leaf->entries[b])) < 0; };
            std::merge(leaf->ordered.begin() + begin, leaf->ordered.begin() + middle,
                leaf->ordered.begin() + middle, leaf->ordered.begin() + end, merged.begin(), less);
            std::copy_n(merged.begin(), end - begin, leaf->ordered.begin() + begin);
            leaf->runEnds[run] = static_cast<uint16_t>(end);
            std::move(leaf->runEnds.begin() + run + 2, leaf->runEnds.begin() + leaf->runCount,
                leaf->runEnds.begin() + run + 1);
            --leaf->runCount;
        }
        static void finishRun(Leaf* leaf) noexcept {
            const size_t begin = leaf->runCount == 0 ? 0 : leaf->runEnds[leaf->runCount - 1];
            if (begin == leaf->count) { return; }
            const auto less = [leaf](uint8_t a, uint8_t b) { return compare(keyOf(leaf->entries[a]), keyOf(leaf->entries[b])) < 0; };
            std::sort(leaf->ordered.begin() + begin, leaf->ordered.begin() + leaf->count, less);
            leaf->runEnds[leaf->runCount++] = static_cast<uint16_t>(leaf->count);
            // Size-tier small runs instead of repeatedly merging the whole
            // sorted prefix. The cap bounds work on ordinary insertions.
            while (leaf->runCount > 1) {
                const size_t run = leaf->runCount - 2;
                const size_t first = run == 0 ? 0 : leaf->runEnds[run - 1];
                const size_t a = leaf->runEnds[run] - first;
                const size_t b = leaf->runEnds[run + 1] - leaf->runEnds[run];
                if (a != b || a + b > WRITE_MERGE_LIMIT) { break; }
                mergeRuns(leaf, run);
            }
        }
        static void sortLeaf(Leaf* leaf) noexcept {
            finishRun(leaf);
            while (leaf->runCount > 1) {
                size_t best = 0, smallest = leaf->runEnds[1];
                // Smallest adjacent runs first also handles partial runs left
                // by range scans, without moving a large prefix each time.
                for (size_t run = 1; run + 1 < leaf->runCount; ++run) {
                    const size_t length = leaf->runEnds[run + 1] - leaf->runEnds[run - 1];
                    if (length < smallest) { best = run; smallest = length; }
                }
                mergeRuns(leaf, best);
            }
        }

        Leaf* findLeaf(std::span<const uint8_t> key, uint16_t* tag = nullptr) const {
            const size_t limit = std::min(key.size(), maxAnchorLength_);
            const auto [match, low, hash] = directory_.longestPrefix(key, limit, root_, hashRange_);
            if (tag) {
                const uint64_t full = low == key.size() ? hash : hashRange_(hash, key.data() + low, key.size() - low);
                *tag = tagOf(finishHash(full, key.size()));
            }
            if (low == key.size()) {
                return match->terminal ? match->terminal : (match->leftmost->previous ? match->leftmost->previous : first_);
            }
            const uint8_t missing = key[low];
            size_t word = missing / 64;
            const unsigned bit = missing % 64;
            uint64_t bits = match->children[word] & (bit == 0 ? 0 : ((uint64_t{1} << bit) - 1));
            for (;;) {
                if (bits != 0) {
                    const auto edge = static_cast<uint8_t>(word * 64 + 63 - std::countl_zero(bits));
                    const PrefixKey child{key.data(), finishHash(extendHash(hash, edge), low + 1), static_cast<uint32_t>(low + 1), edge};
                    const auto* found = directory_.find(child);
                    if (!found) { throw std::logic_error("Wormhole: missing metadata child"); }
                    return found->rightmost;
                }
                if (word == 0) { break; }
                bits = match->children[--word];
            }
            return match->terminal ? match->terminal : (match->leftmost->previous ? match->leftmost->previous : first_);
        }

        void registerAnchor(Leaf* leaf) {
            struct Prepared {
                PrefixKey key;
                Meta* meta;
                bool created{false};
            };
            std::vector<Prepared> path;
            path.reserve(leaf->anchor.size() + 1);
            uint64_t hash = HASH_SEED;
            size_t missing = 0;
            for (size_t length = 0; length <= leaf->anchor.size(); ++length) {
                if (length != 0) { hash = extendHash(hash, leaf->anchor[length - 1]); }
                const auto key = prefix(leaf->anchor, length, hash);
                Meta* meta = directory_.find(key);
                path.push_back({key, meta});
                missing += meta == nullptr;
            }
            if (path.back().meta && path.back().meta->terminal) { throw std::logic_error("Wormhole: duplicate anchor"); }
            if (missing != 0) { directory_.reserve(directory_.size() + missing); }
            for (auto& item : path) {
                if (item.meta) { continue; }
                item.meta = allocate<Meta>(item.key);
                item.created = true;
            }
            // No publication or modification of existing metadata until every
            // allocation succeeds. The reserved directory insertion cannot throw.
            for (const auto& item : path) { if (item.created) { directory_.insert(item.meta); } }
            for (size_t i = 0; i < path.size(); ++i) {
                auto& meta = *path[i].meta;
                if (!meta.leftmost || compare(leaf->anchor, meta.leftmost->anchor) < 0) { meta.leftmost = leaf; }
                if (!meta.rightmost || compare(leaf->anchor, meta.rightmost->anchor) > 0) { meta.rightmost = leaf; }
                if (i == leaf->anchor.size()) { meta.terminal = leaf; }
                else {
                    const uint8_t edge = leaf->anchor[i];
                    meta.children[edge / 64] |= uint64_t{1} << (edge % 64);
                }
            }
            maxAnchorLength_ = std::max(maxAnchorLength_, leaf->anchor.size());
        }

        void insert(Leaf* leaf, Entry* entry) {
            if (leaf->count < LEAF_CAPACITY) {
                appendEntry(leaf, entry);
                const size_t sorted = leaf->runCount == 0 ? 0 : leaf->runEnds[leaf->runCount - 1];
                if (leaf->count - sorted >= SORT_RUN_SIZE) { finishRun(leaf); }
                return;
            }
            sortLeaf(leaf);
            const size_t position = lowerBound(leaf, keyOf(entry));
            std::array<Entry*, LEAF_CAPACITY + 1> merged;
            for (size_t i = 0; i < position; ++i) { merged[i] = orderedEntry(leaf, i); }
            merged[position] = entry;
            for (size_t i = position; i < leaf->count; ++i) { merged[i + 1] = orderedEntry(leaf, i); }
            constexpr size_t cut = (LEAF_CAPACITY + 1) / 2;
            auto* right = allocate<Leaf>();
            const auto leftMax = keyOf(merged[cut - 1]), rightMin = keyOf(merged[cut]);
            size_t shared = 0;
            while (shared < std::min(leftMax.size(), rightMin.size()) && leftMax[shared] == rightMin[shared]) { ++shared; }
            // Prefix terminals are explicit, so LCP+1 is safe for arbitrary
            // binary keys without a reserved sentinel or an unbounded fat leaf.
            right->anchor = rightMin.first(shared + 1);
            right->previous = leaf;
            right->next = leaf->next;
            Leaf left;
            left.count = cut;
            left.runCount = 1;
            left.runEnds[0] = static_cast<uint16_t>(left.count);
            right->count = merged.size() - cut;
            right->runCount = 1;
            right->runEnds[0] = static_cast<uint16_t>(right->count);
            std::copy_n(merged.begin(), cut, left.entries.begin());
            std::copy(merged.begin() + cut, merged.end(), right->entries.begin());
            for (size_t i = 0; i < left.count; ++i) { left.ordered[i] = static_cast<uint8_t>(i); }
            for (size_t i = 0; i < right->count; ++i) { right->ordered[i] = static_cast<uint8_t>(i); }
            std::array<uint8_t, LEAF_CAPACITY> ranks;
            for (size_t i = 0; i < leaf->count; ++i) {
                ranks[leaf->ordered[i]] = static_cast<uint8_t>(i + static_cast<size_t>(i >= position));
            }
            size_t leftTags = 0, rightTags = 0;
            // Stable partition of the existing tag order is linear. Repeated
            // tag insertion here concentrates quadratic work on split latency.
            for (size_t i = 0; i < leaf->count; ++i) {
                const size_t rank = ranks[leaf->tagSlots[i]];
                auto* target = rank < cut ? &left : right;
                auto& count = rank < cut ? leftTags : rightTags;
                target->tags[count] = leaf->tags[i];
                target->tagSlots[count++] = static_cast<uint8_t>(rank < cut ? rank : rank - cut);
            }
            auto* target = position < cut ? &left : right;
            const size_t existingTags = target->count - 1;
            const size_t tagIndex = static_cast<size_t>(std::lower_bound(target->tags.begin(), target->tags.begin() + existingTags,
                entry->tag) - target->tags.begin());
            std::move_backward(target->tags.begin() + tagIndex, target->tags.begin() + existingTags, target->tags.begin() + existingTags + 1);
            std::move_backward(target->tagSlots.begin() + tagIndex, target->tagSlots.begin() + existingTags, target->tagSlots.begin() + existingTags + 1);
            target->tags[tagIndex] = entry->tag;
            target->tagSlots[tagIndex] = static_cast<uint8_t>(position < cut ? position : position - cut);
            registerAnchor(right);
            // Nothing below can throw. Publish the prepared split as one locked
            // mutation after the directory has all resources it needs.
            leaf->entries = left.entries;
            leaf->tags = left.tags;
            leaf->tagSlots = left.tagSlots;
            leaf->ordered = left.ordered;
            leaf->count = cut;
            leaf->runCount = left.runCount;
            leaf->runEnds = left.runEnds;
            if (right->next) { right->next->previous = right; }
            leaf->next = right;
        }

        core::OwnedRecord* makeRecord(std::span<const uint8_t> key, std::span<const uint8_t> value,
            uint64_t seq, uint8_t flags, uint64_t fp64, uint64_t mini) {
            auto* record = allocate<core::OwnedRecord>();
            core::OwnedRecord::createInplace(*record, key, value, seq, flags, arena_, fp64, mini);
            if (record->data.isArena()) { arenaBytes_.fetch_add(key.size() + value.size(), std::memory_order_relaxed); }
            return record;
        }
        Entry* makeEntry(const core::OwnedRecord* keyRecord, uint16_t tag) {
            const size_t slots = retainAll_ ? 0 : capacity_ * sizeof(const core::OwnedRecord*);
            auto* memory = arena_.allocate(sizeof(Entry) + slots, alignof(Entry));
            arenaBytes_.fetch_add(sizeof(Entry) + slots, std::memory_order_relaxed);
            auto* entry = new(memory) Entry{};
            entry->keyRecord = keyRecord;
            entry->tag = tag;
            entry->history.sequenceOrdered = sequenceOrder_;
            if (slots != 0) {
                entry->ring = reinterpret_cast<const core::OwnedRecord**>(entry + 1);
                std::fill_n(entry->ring, capacity_, nullptr);
            }
            return entry;
        }
        bool append(Entry* chain, const core::OwnedRecord* record) {
            if (retainAll_) { chain->history.append(record, arena_, arenaBytes_); return true; }
            if (sequenceOrder_) {
                return detail::insertSequenceOrdered(capacity_, chain->head, chain->count, record,
                    [&](uint16_t i) { return chain->ring[i]; },
                    [&](uint16_t i, const core::OwnedRecord* value) { chain->ring[i] = value; });
            }
            const bool grew = chain->count < capacity_;
            chain->head = static_cast<uint16_t>((chain->head + 1) % capacity_);
            chain->ring[chain->head] = record;
            if (grew) { ++chain->count; }
            return grew;
        }
        bool visible(const Entry* chain, uint64_t seq, RecordView* out) const noexcept {
            if (retainAll_) { return chain->history.get(seq, out); }
            for (uint16_t i = 0; i < chain->count; ++i) {
                const auto* record = chain->ring[(chain->head + capacity_ - i) % capacity_];
                if (record->seq() <= seq) { *out = detail::VersionHistory::view(*record); return true; }
            }
            return false;
        }

        ArenaGenerator<RecordView> iterate(uint64_t seq, ScanBound startOwned,
            ScanBound endOwned, bool allVersions) const {
            const auto start = startOwned.view(), end = endOwned.view();
            if (!start.empty() && !end.empty() && compare(start, end) >= 0) { co_return; }
            std::array<RecordView, INLINE_SCAN_ROWS> localRows;
            size_t localCount = 0;
            std::vector<RecordView> extraRows;
            bool frozen;
            {
                std::unique_lock lock{mutex_};
                frozen = frozen_;
                if (allVersions && !frozen) { throw std::logic_error("Wormhole: flush requires a frozen backend"); }
                if (!frozen) {
                    Leaf* leaf = findLeaf(start);
                    sortLeaf(leaf);
                    size_t index = lowerBound(leaf, start);
                    bool finished = false;
                    for (; leaf && !finished; leaf = leaf->next, index = 0) {
                        sortLeaf(leaf);
                        for (; index < leaf->count; ++index) {
                            const auto* entry = orderedEntry(leaf, index);
                            if (!end.empty() && compare(keyOf(entry), end) >= 0) { finished = true; break; }
                            RecordView row;
                            if (visible(entry, seq, &row)) {
                                if (localCount < localRows.size()) { localRows[localCount++] = row; }
                                else {
                                    if (extraRows.empty()) { extraRows.reserve(LEAF_CAPACITY - INLINE_SCAN_ROWS); }
                                    extraRows.push_back(row);
                                }
                            }
                        }
                    }
                }
            }
            if (!frozen) {
                for (size_t i = 0; i < localCount; ++i) { co_yield localRows[i]; }
                for (const auto& row : extraRows) { co_yield row; }
                co_return;
            }
            // freeze() sorts all leaves before publishing immutability. No lock
            // survives a yield, including the mutable snapshot capture above.
            Leaf* leaf = findLeaf(start);
            size_t index = lowerBound(leaf, start);
            for (; leaf; leaf = leaf->next, index = 0) {
                for (; index < leaf->count; ++index) {
                    const auto* entry = orderedEntry(leaf, index);
                    if (!end.empty() && compare(keyOf(entry), end) >= 0) { co_return; }
                    const auto* chain = entry;
                    if (allVersions) {
                        if (retainAll_) { for (const auto& row : chain->history.records()) { co_yield row; } }
                        else {
                            for (uint16_t i = 0; i < chain->count; ++i) {
                                co_yield detail::VersionHistory::view(*chain->ring[(chain->head + capacity_ - i) % capacity_]);
                            }
                        }
                    }
                    else {
                        RecordView row;
                        if (visible(chain, seq, &row)) { co_yield row; }
                    }
                }
            }
        }

        public:
            Impl(size_t initial, size_t maximum, size_t generatorInitial, size_t generatorMax, MemTableBackendOptions options)
                : arena_{initial, maximum}, generatorInitial_{generatorInitial}, generatorMax_{generatorMax} {
                if (options.maxVersionsPerKey == 0 || options.maxVersionsPerKey > UINT16_MAX) {
                    throw std::invalid_argument("Wormhole: maxVersionsPerKey must be in 1..65535");
                }
                capacity_ = static_cast<uint16_t>(options.maxVersionsPerKey);
                first_ = allocate<Leaf>();
                root_ = allocate<Meta>(prefix({}, 0, HASH_SEED));
                root_->terminal = root_->leftmost = root_->rightmost = first_;
                directory_.reserve(1);
                directory_.insert(root_);
            }

            Status put(ByteView key, ByteView value, uint64_t seq, uint8_t flags, uint64_t fp64, uint64_t mini) {
                std::unique_lock lock{mutex_};
                if (frozen_) { return Status::Error(Status::Code::INVALID_ARGUMENT, "memtable is frozen"); }
                if (key.size() > UINT16_MAX || value.size() > UINT16_MAX || key.size() + value.size() > UINT16_MAX) {
                    return Status::Error(Status::Code::INVALID_ARGUMENT, "key/value too large for MemHdr16");
                }
                const auto k = asU8(key);
                uint16_t tag;
                Leaf* leaf = findLeaf(k, &tag);
                auto* existing = findEntry(leaf, k, tag);
                auto* record = makeRecord(k, asU8(value), seq, flags, fp64, mini);
                if (existing) {
                    if (append(existing, record)) { records_.fetch_add(1, std::memory_order_relaxed); }
                }
                else {
                    auto* entry = makeEntry(record, tag);
                    (void)append(entry, record);
                    insert(leaf, entry);
                    records_.fetch_add(1, std::memory_order_relaxed);
                }
                return Status::OK();
            }
            bool get(ByteView key, uint64_t seq, RecordView* out) const {
                if (!out || key.size() > UINT16_MAX) { return false; }
                std::shared_lock lock{mutex_};
                const auto k = asU8(key);
                uint16_t tag;
                const auto* leaf = findLeaf(k, &tag);
                const auto* entry = findEntry(leaf, k, tag);
                return entry && visible(entry, seq, out);
            }
            ArenaGenerator<RecordView> iterator(ByteView start, ByteView end, uint64_t seq, bool allVersions = false) const {
                if (allVersions) {
                    std::shared_lock lock{mutex_};
                    if (!frozen_) { throw std::logic_error("Wormhole: flush requires a frozen backend"); }
                }
                const auto a = asU8(start), b = asU8(end);
                return ArenaGenerator<RecordView>::withOwnedArena(generatorInitial_, generatorMax_,
                    [this, seq, allVersions, startOwned = ScanBound{a}, endOwned = ScanBound{b}]() mutable {
                        return iterate(seq, std::move(startOwned), std::move(endOwned), allVersions);
                    });
            }
            void configure(bool retainAll) {
                std::unique_lock lock{mutex_};
                if (frozen_ || records_.load(std::memory_order_relaxed) != 0) { throw std::logic_error("Wormhole: configure retention before writing or freezing"); }
                if (retainAll) { retainAll_ = true; }
                else { sequenceOrder_ = true; }
            }
            void freeze() {
                std::unique_lock lock{mutex_};
                for (auto* leaf = first_; leaf; leaf = leaf->next) { sortLeaf(leaf); }
                frozen_ = true;
            }
            size_t sizeBytes() const {
                std::shared_lock lock{mutex_};
                return arenaBytes_.load(std::memory_order_relaxed) + directory_.sizeBytes();
            }
            size_t entryCount() const { return records_.load(std::memory_order_relaxed); }
    };

    WormholeMemTable::WormholeMemTable(size_t initial, size_t maximum, size_t generatorInitial, size_t generatorMax, MemTableBackendOptions options)
        : impl_{std::make_unique<Impl>(initial, maximum, generatorInitial, generatorMax, options)} {}
    WormholeMemTable::~WormholeMemTable() = default;
    Status WormholeMemTable::put(ByteView key, ByteView value, uint64_t seq, uint8_t flags, uint64_t fp64, uint64_t mini) {
        return impl_->put(key, value, seq, flags, fp64, mini);
    }
    bool WormholeMemTable::get(ByteView key, uint64_t seq, RecordView* out) const { return impl_->get(key, seq, out); }
    ArenaGenerator<RecordView> WormholeMemTable::iterator(ByteView start, ByteView end, uint64_t seq) const { return impl_->iterator(start, end, seq); }
    ArenaGenerator<RecordView> WormholeMemTable::flushIterator() const { return impl_->iterator({}, {}, UINT64_MAX, true); }
    void WormholeMemTable::retainAllVersions() { impl_->configure(true); }
    void WormholeMemTable::useSequenceOrder() { impl_->configure(false); }
    void WormholeMemTable::freeze() { impl_->freeze(); }
    size_t WormholeMemTable::sizeBytes() const { return impl_->sizeBytes(); }
    size_t WormholeMemTable::entryCount() const { return impl_->entryCount(); }
}

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

namespace akkaradb::engine {
    namespace {
        template <typename T, typename... Args>
        [[nodiscard]] T* arena_new(BufferArena& arena, Args&&... args) {
            std::byte* mem = arena.allocate(sizeof(T), alignof(T));
            return new(mem) T(std::forward<Args>(args)...);
        }

        [[nodiscard]] int compare_key_bytes(std::span<const uint8_t> lhs, std::span<const uint8_t> rhs) noexcept {
            const size_t min_len = std::min(lhs.size(), rhs.size());
            if (min_len > 0) {
                const int cmp = std::memcmp(lhs.data(), rhs.data(), min_len);
                if (cmp != 0) { return cmp < 0 ? -1 : 1; }
            }
            if (lhs.size() < rhs.size()) { return -1; }
            if (lhs.size() > rhs.size()) { return 1; }
            return 0;
        }
    } // namespace

    SkipListMemTable::SkipListMemTable(
        size_t data_arena_initial_block_size,
        size_t data_arena_max_block_size,
        size_t generator_arena_initial_block_size,
        size_t generator_arena_max_block_size
    )
        : data_arena_{data_arena_initial_block_size, data_arena_max_block_size},
          generator_arena_{generator_arena_initial_block_size, generator_arena_max_block_size} {
        head_ = arena_new<Node>(data_arena_);
        head_->level = MAX_LEVEL;
        head_->key_record = nullptr;
        for (uint8_t i = 0; i < MAX_LEVEL; ++i) { head_->next[i].store(nullptr, std::memory_order_relaxed); }
        current_max_level_.store(1, std::memory_order_relaxed);

        bytes_.store(sizeof(Node), std::memory_order_relaxed);
    }

    std::span<const uint8_t> SkipListMemTable::as_u8(ByteView view) noexcept { return {reinterpret_cast<const uint8_t*>(view.data()), view.size()}; }

    uint64_t SkipListMemTable::next_random() noexcept {
        uint64_t x = rng_state_;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        rng_state_ = x;
        return x;
    }

    uint8_t SkipListMemTable::random_level() noexcept {
        uint8_t level = 1;
        while (level < MAX_LEVEL && ((next_random() & 0x3ULL) == 0ULL)) { ++level; }
        return level;
    }

    SkipListMemTable::Node* SkipListMemTable::new_node(const core::OwnedRecord* initial_record, uint8_t level) {
        Node* node = arena_new<Node>(data_arena_);
        node->level = level;
        node->key_record = initial_record;
        node->head.store(0, std::memory_order_relaxed);
        node->count.store(1, std::memory_order_relaxed);
        node->version.store(0, std::memory_order_relaxed);

        node->ring[0].record.store(initial_record, std::memory_order_relaxed);
        return node;
    }

    core::OwnedRecord* SkipListMemTable::make_record(
        std::span<const uint8_t> key,
        std::span<const uint8_t> value,
        uint64_t seq,
        uint8_t flags,
        uint64_t precomputed_fp64,
        uint64_t precomputed_mk
    ) {
        const uint64_t fp64 = precomputed_fp64 != 0 ? precomputed_fp64 : (key.empty() ? 0 : core::compute_key_fp64(key.data(), key.size()));
        const uint64_t mini = precomputed_mk != 0 ? precomputed_mk : (key.empty() ? 0 : core::build_mini_key(key.data(), key.size()));

        core::OwnedRecord* record = arena_new<core::OwnedRecord>(data_arena_);
        core::OwnedRecord::create_inplace(*record, key, value, seq, flags, data_arena_, fp64, mini);
        return record;
    }

    int SkipListMemTable::compare_node_key(const Node* node, std::span<const uint8_t> key) noexcept { return node->key_record->compare_key(key); }

    SkipListMemTable::Node* SkipListMemTable::find_node(
        std::span<const uint8_t> key,
        std::array<Node*, MAX_LEVEL>* update,
        bool writer_fast_path
    ) const noexcept {
        const uint8_t top_level = current_max_level_.load(std::memory_order_relaxed);
        const int start_level = static_cast<int>(top_level > 0 ? top_level - 1 : 0);

        if (writer_fast_path) {
            Node* current = head_;
            if (update != nullptr) {
                for (int level = start_level; level >= 0; --level) {
                    Node* next = current->next[level].load(std::memory_order_relaxed);
                    while (next != nullptr && compare_node_key(next, key) < 0) {
                        current = next;
                        next = current->next[level].load(std::memory_order_relaxed);
                    }
                    (*update)[static_cast<size_t>(level)] = current;
                }
                return current->next[0].load(std::memory_order_relaxed);
            }
            for (int level = start_level; level >= 0; --level) {
                Node* next = current->next[level].load(std::memory_order_relaxed);
                while (next != nullptr && compare_node_key(next, key) < 0) {
                    current = next;
                    next = current->next[level].load(std::memory_order_relaxed);
                }
            }
            return current->next[0].load(std::memory_order_relaxed);
        }

        Node* current = head_;
        if (update != nullptr) {
            for (int level = start_level; level >= 0; --level) {
                Node* next = current->next[level].load(std::memory_order_acquire);
                while (next != nullptr && compare_node_key(next, key) < 0) {
                    current = next;
                    next = current->next[level].load(std::memory_order_acquire);
                }
                (*update)[static_cast<size_t>(level)] = current;
            }
            return current->next[0].load(std::memory_order_acquire);
        }
        for (int level = start_level; level >= 0; --level) {
            Node* next = current->next[level].load(std::memory_order_acquire);
            while (next != nullptr && compare_node_key(next, key) < 0) {
                current = next;
                next = current->next[level].load(std::memory_order_acquire);
            }
        }
        return current->next[0].load(std::memory_order_acquire);
    }

    bool SkipListMemTable::visible_record(const Node* node, uint64_t snapshot_seq, RecordView* out) const noexcept {
        for (;;) {
            const uint64_t begin = node->version.load(std::memory_order_acquire);
            if ((begin & 1ULL) != 0ULL) { continue; }

            const uint8_t count = node->count.load(std::memory_order_relaxed);
            const uint8_t head_local = node->head.load(std::memory_order_relaxed);

            const core::OwnedRecord* selected = nullptr;

            if (count > 0) {
                // Common case: latest snapshot reads. Check the newest slot first
                // and skip ring scan when it is visible.
                const core::OwnedRecord* newest = node->ring[head_local].record.load(std::memory_order_relaxed);
                if (newest != nullptr && newest->seq() <= snapshot_seq) { selected = newest; }
                else {
                    for (uint8_t i = 1; i < count; ++i) {
                        const uint8_t index = static_cast<uint8_t>((head_local - i) & (MAX_VERSIONS_PER_KEY - 1));
                        const core::OwnedRecord* candidate = node->ring[index].record.load(std::memory_order_relaxed);
                        if (candidate != nullptr && candidate->seq() <= snapshot_seq) {
                            selected = candidate;
                            break;
                        }
                    }
                }
            }

            const uint64_t end = node->version.load(std::memory_order_acquire);
            if (begin == end && (end & 1ULL) == 0ULL) {
                if (selected == nullptr) { return false; }
                *out = to_view(*selected);
                return true;
            }
        }
    }

    RecordView SkipListMemTable::to_view(const core::OwnedRecord& record) noexcept {
        const auto key = record.key();
        const auto value = record.value();
        return {key.data(), record.hdr.k_len, value.data(), record.hdr.v_len, record.hdr.seq, record.hdr.flags, record.key_fp64, record.mini_key};
    }

    Status SkipListMemTable::put(ByteView key, ByteView value, uint64_t seq, uint8_t flags, uint64_t precomputed_fp64, uint64_t precomputed_mk) {
        if (frozen_.load(std::memory_order_acquire)) { return Status::Error(Status::Code::InvalidArgument, "memtable is frozen"); }

        if (key.size() > std::numeric_limits<uint16_t>::max() || value.size() > std::numeric_limits<uint16_t>::max()) {
            return Status::Error(Status::Code::InvalidArgument, "key/value too large for MemHdr16");
        }

        const auto key_u8 = as_u8(key);
        const auto value_u8 = as_u8(value);
        std::array<Node*, MAX_LEVEL> update;
        Node* candidate = find_node(key_u8, &update, true);

        const core::OwnedRecord* record = make_record(key_u8, value_u8, seq, flags, precomputed_fp64, precomputed_mk);
        bytes_.fetch_add(sizeof(core::OwnedRecord) + key.size() + value.size(), std::memory_order_relaxed);

        if (candidate != nullptr && compare_node_key(candidate, key_u8) == 0) {
            candidate->version.fetch_add(1, std::memory_order_acq_rel); // enter write (odd)

            const uint8_t prev_head = candidate->head.load(std::memory_order_relaxed);
            const uint8_t prev_count = candidate->count.load(std::memory_order_relaxed);
            const uint8_t next_head = static_cast<uint8_t>((prev_head + 1) & (MAX_VERSIONS_PER_KEY - 1));

            candidate->ring[next_head].record.store(record, std::memory_order_release);
            if (prev_count < MAX_VERSIONS_PER_KEY) {
                candidate->count.store(static_cast<uint8_t>(prev_count + 1), std::memory_order_relaxed);
                entries_.fetch_add(1, std::memory_order_relaxed);
            }
            candidate->head.store(next_head, std::memory_order_release);
            candidate->version.fetch_add(1, std::memory_order_release); // leave write (even)
            return Status::OK();
        }

        const uint8_t level = random_level();
        const uint8_t observed_max = current_max_level_.load(std::memory_order_relaxed);
        if (level > observed_max) { for (uint8_t i = observed_max; i < level; ++i) { update[i] = head_; } }
        Node* node = new_node(record, level);
        bytes_.fetch_add(sizeof(Node), std::memory_order_relaxed);
        entries_.fetch_add(1, std::memory_order_relaxed);

        for (uint8_t i = 0; i < level; ++i) {
            Node* next = update[i]->next[i].load(std::memory_order_relaxed);
            node->next[i].store(next, std::memory_order_relaxed);
        }

        for (int i = static_cast<int>(level) - 1; i >= 0; --i) {
            update[static_cast<size_t>(i)]->next[static_cast<size_t>(i)].store(node, std::memory_order_release);
        }
        if (level > observed_max) { current_max_level_.store(level, std::memory_order_release); }

        return Status::OK();
    }

    bool SkipListMemTable::get(ByteView key, uint64_t snapshot_seq, RecordView* out) const {
        if (out == nullptr) { return false; }

        const auto key_u8 = as_u8(key);
        Node* node = find_node(key_u8, nullptr, false);
        if (node == nullptr || compare_node_key(node, key_u8) != 0) { return false; }

        return visible_record(node, snapshot_seq, out);
    }

    ArenaGenerator<RecordView> SkipListMemTable::iterate_snapshot(uint64_t snapshot_seq) const {
        Node* current = head_->next[0].load(std::memory_order_acquire);
        while (current != nullptr) {
            RecordView visible;
            if (visible_record(current, snapshot_seq, &visible)) { co_yield visible; }
            current = current->next[0].load(std::memory_order_acquire);
        }
    }

    ArenaGenerator<RecordView> SkipListMemTable::iterate_snapshot_range(
        uint64_t snapshot_seq,
        std::vector<uint8_t> start_key,
        std::vector<uint8_t> end_key
    ) const {
        const std::span<const uint8_t> start{start_key.data(), start_key.size()};
        const std::span<const uint8_t> end{end_key.data(), end_key.size()};
        if (!start.empty() && !end.empty() && compare_key_bytes(start, end) >= 0) { co_return; }

        Node* current = start.empty() ? head_->next[0].load(std::memory_order_acquire) : find_node(start, nullptr, false);

        while (current != nullptr) {
            if (!end.empty() && compare_node_key(current, end) >= 0) { break; }

            RecordView visible;
            if (visible_record(current, snapshot_seq, &visible)) {
                if (!start.empty() && visible.compare_key(start) < 0) {
                    current = current->next[0].load(std::memory_order_acquire);
                    continue;
                }
                if (!end.empty() && visible.compare_key(end) >= 0) { break; }
                co_yield visible;
            }
            current = current->next[0].load(std::memory_order_acquire);
        }
    }

    ArenaGenerator<RecordView> SkipListMemTable::iterator(ByteView start_key, ByteView end_key, uint64_t snapshot_seq) const {
        const std::span<const uint8_t> start = as_u8(start_key);
        const std::span<const uint8_t> end = as_u8(end_key);
        std::vector<uint8_t> start_owned(start.begin(), start.end());
        std::vector<uint8_t> end_owned(end.begin(), end.end());

        std::lock_guard<std::mutex> lock{generator_arena_mutex_};
        return ArenaGenerator<RecordView>::with_arena(
            generator_arena_,
            [this, snapshot_seq, start_owned = std::move(start_owned), end_owned = std::move(end_owned)]() mutable {
                return iterate_snapshot_range(snapshot_seq, std::move(start_owned), std::move(end_owned));
            }
        );
    }

    void SkipListMemTable::freeze() { frozen_.store(true, std::memory_order_release); }

    size_t SkipListMemTable::sizeBytes() const { return bytes_.load(std::memory_order_acquire); }

    size_t SkipListMemTable::entryCount() const { return entries_.load(std::memory_order_acquire); }
} // namespace akkaradb::engine

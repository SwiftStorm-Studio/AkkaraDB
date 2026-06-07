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

// akkengine/src/engine/memtable/BPTreeMemTable.cpp
#include "akk/engine/memtable/BPTreeMemTable.hpp"

#include <algorithm>
#include <cstring>
#include <limits>
#include <optional>
#include <utility>
#include <vector>

#include "akk/core/record/KeyFingerprint.hpp"

namespace akkaradb::engine {
    namespace {
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

    BPTreeMemTable::BPTreeMemTable(
        size_t data_arena_initial_block_size,
        size_t data_arena_max_block_size,
        size_t generator_arena_initial_block_size,
        size_t generator_arena_max_block_size
    )
        : data_arena_{data_arena_initial_block_size, data_arena_max_block_size},
          generator_arena_{generator_arena_initial_block_size, generator_arena_max_block_size} {
        Node* initial_root = make_node(true);
        root_.store(initial_root, std::memory_order_release);
    }

    std::span<const uint8_t> BPTreeMemTable::as_u8(ByteView view) noexcept {
        return {reinterpret_cast<const uint8_t*>(view.data()), view.size()};
    }

    BPTreeMemTable::Node* BPTreeMemTable::make_node(bool leaf) {
        Node* node = arena_new<Node>(leaf);
        bytes_.fetch_add(sizeof(Node), std::memory_order_relaxed);
        return node;
    }

    BPTreeMemTable::VersionChain* BPTreeMemTable::make_chain(const core::OwnedRecord* initial) {
        VersionChain* chain = arena_new<VersionChain>();
        chain->ring[0].store(initial, std::memory_order_relaxed);
        chain->head.store(0, std::memory_order_relaxed);
        chain->count.store(1, std::memory_order_relaxed);
        entries_.fetch_add(1, std::memory_order_relaxed);
        bytes_.fetch_add(sizeof(VersionChain), std::memory_order_relaxed);
        return chain;
    }

    core::OwnedRecord* BPTreeMemTable::make_record(
        std::span<const uint8_t> key,
        std::span<const uint8_t> value,
        uint64_t seq,
        uint8_t flags,
        uint64_t precomputed_fp64,
        uint64_t precomputed_mk
    ) {
        const uint64_t fp64 = precomputed_fp64 != 0
                                  ? precomputed_fp64
                                  : (key.empty() ? 0ULL : core::compute_key_fp64(key.data(), key.size()));
        const uint64_t mini = precomputed_mk != 0 ? precomputed_mk : (key.empty() ? 0ULL : core::build_mini_key(key.data(), key.size()));

        core::OwnedRecord* record = arena_new<core::OwnedRecord>();
        core::OwnedRecord::create_inplace(*record, key, value, seq, flags, data_arena_, fp64, mini);
        bytes_.fetch_add(sizeof(core::OwnedRecord) + key.size() + value.size(), std::memory_order_relaxed);
        return record;
    }

    void BPTreeMemTable::begin_write(Node* node) noexcept { node->version.fetch_add(1, std::memory_order_acq_rel); }

    void BPTreeMemTable::end_write(Node* node) noexcept { node->version.fetch_add(1, std::memory_order_release); }

    int BPTreeMemTable::compare_record_key(const core::OwnedRecord* record, std::span<const uint8_t> key) noexcept {
        return record->compare_key(key);
    }

    int BPTreeMemTable::compare_record_record(const core::OwnedRecord* lhs, const core::OwnedRecord* rhs) noexcept {
        return lhs->compare_key(*rhs);
    }

    uint16_t BPTreeMemTable::find_leaf_position(const Node* leaf, std::span<const uint8_t> key) noexcept {
        const uint16_t key_count = leaf->key_count.load(std::memory_order_relaxed);
        return find_leaf_position(leaf, key, key_count);
    }

    uint16_t BPTreeMemTable::find_leaf_position(const Node* leaf, std::span<const uint8_t> key, uint16_t key_count) noexcept {
        uint16_t lo = 0;
        uint16_t hi = key_count;
        while (lo < hi) {
            const uint16_t mid = static_cast<uint16_t>(lo + (hi - lo) / 2);
            const core::OwnedRecord* pivot = leaf->keys[mid].load(std::memory_order_relaxed);
            const int cmp = compare_record_key(pivot, key);
            if (cmp < 0) { lo = static_cast<uint16_t>(mid + 1); }
            else { hi = mid; }
        }
        return lo;
    }

    uint16_t BPTreeMemTable::find_child_index(const Node* internal, std::span<const uint8_t> key) noexcept {
        const uint16_t key_count = internal->key_count.load(std::memory_order_relaxed);
        uint16_t lo = 0;
        uint16_t hi = key_count;
        while (lo < hi) {
            const uint16_t mid = static_cast<uint16_t>(lo + (hi - lo) / 2);
            const core::OwnedRecord* pivot = internal->keys[mid].load(std::memory_order_relaxed);
            const int cmp = compare_record_key(pivot, key);
            if (cmp <= 0) { lo = static_cast<uint16_t>(mid + 1); }
            else { hi = mid; }
        }
        return lo;
    }

    void BPTreeMemTable::append_version(VersionChain* chain, const core::OwnedRecord* record, std::atomic<size_t>& entries) noexcept {
        if (chain == nullptr) { return; }

        chain->version.fetch_add(1, std::memory_order_acq_rel);

        const uint8_t prev_head = chain->head.load(std::memory_order_relaxed);
        const uint8_t prev_count = chain->count.load(std::memory_order_relaxed);
        const uint8_t next_head = static_cast<uint8_t>((prev_head + 1) & (MAX_VERSIONS_PER_KEY - 1));

        chain->ring[next_head].store(record, std::memory_order_release);

        if (prev_count < MAX_VERSIONS_PER_KEY) {
            chain->count.store(static_cast<uint8_t>(prev_count + 1), std::memory_order_relaxed);
            entries.fetch_add(1, std::memory_order_relaxed);
        }
        chain->head.store(next_head, std::memory_order_release);

        chain->version.fetch_add(1, std::memory_order_release);
    }

    bool BPTreeMemTable::visible_record(VersionChain* chain, uint64_t snapshot_seq, RecordView* out) noexcept {
        if (chain == nullptr || out == nullptr) { return false; }

        for (;;) {
            const uint64_t begin = chain->version.load(std::memory_order_acquire);
            if ((begin & 1ULL) != 0ULL) { continue; }

            const uint8_t head = chain->head.load(std::memory_order_relaxed);
            const uint8_t count = chain->count.load(std::memory_order_relaxed);

            const core::OwnedRecord* selected = nullptr;
            if (count > 0) {
                const core::OwnedRecord* newest = chain->ring[head].load(std::memory_order_relaxed);
                if (newest != nullptr && newest->seq() <= snapshot_seq) { selected = newest; }
                else {
                    for (uint8_t i = 1; i < count; ++i) {
                        const uint8_t index = static_cast<uint8_t>((head - i) & (MAX_VERSIONS_PER_KEY - 1));
                        const core::OwnedRecord* candidate = chain->ring[index].load(std::memory_order_relaxed);
                        if (candidate != nullptr && candidate->seq() <= snapshot_seq) {
                            selected = candidate;
                            break;
                        }
                    }
                }
            }

            const uint64_t end = chain->version.load(std::memory_order_acquire);
            if (begin == end && (end & 1ULL) == 0ULL) {
                if (selected == nullptr) { return false; }
                *out = to_view(*selected);
                return true;
            }
        }
    }

    RecordView BPTreeMemTable::to_view(const core::OwnedRecord& record) noexcept {
        const auto key = record.key();
        const auto value = record.value();
        return {
            key.data(),
            record.hdr.k_len,
            value.data(),
            record.hdr.v_len,
            record.hdr.seq,
            record.hdr.flags,
            record.key_fp64,
            record.mini_key
        };
    }

    std::optional<BPTreeMemTable::SplitResult> BPTreeMemTable::insert_recursive(Node* node, const core::OwnedRecord* record) {
        const std::span<const uint8_t> key = record->key();

        if (node->is_leaf) {
            const uint16_t pos = find_leaf_position(node, key);
            const uint16_t key_count = node->key_count.load(std::memory_order_relaxed);

            if (pos < key_count) {
                const core::OwnedRecord* existing = node->keys[pos].load(std::memory_order_relaxed);
                if (compare_record_key(existing, key) == 0) {
                    append_version(node->chains[pos].load(std::memory_order_relaxed), record, entries_);
                    return std::nullopt;
                }
            }

            VersionChain* chain = make_chain(record);

            if (key_count < MAX_KEYS) {
                begin_write(node);
                for (uint16_t i = key_count; i > pos; --i) {
                    node->keys[i].store(node->keys[i - 1].load(std::memory_order_relaxed), std::memory_order_relaxed);
                    node->chains[i].store(node->chains[i - 1].load(std::memory_order_relaxed), std::memory_order_relaxed);
                }
                node->keys[pos].store(record, std::memory_order_release);
                node->chains[pos].store(chain, std::memory_order_release);
                node->key_count.store(static_cast<uint16_t>(key_count + 1), std::memory_order_release);
                end_write(node);
                return std::nullopt;
            }

            std::array<const core::OwnedRecord*, MAX_KEYS + 1> all_keys{};
            std::array<VersionChain*, MAX_KEYS + 1> all_chains{};

            uint16_t wi = 0;
            for (uint16_t i = 0; i < key_count; ++i) {
                if (wi == pos) {
                    all_keys[wi] = record;
                    all_chains[wi] = chain;
                    ++wi;
                }
                all_keys[wi] = node->keys[i].load(std::memory_order_relaxed);
                all_chains[wi] = node->chains[i].load(std::memory_order_relaxed);
                ++wi;
            }
            if (wi == pos) {
                all_keys[wi] = record;
                all_chains[wi] = chain;
                ++wi;
            }

            Node* right = make_node(true);

            const uint16_t total = static_cast<uint16_t>(MAX_KEYS + 1);
            const uint16_t left_count = static_cast<uint16_t>(total / 2);
            const uint16_t right_count = static_cast<uint16_t>(total - left_count);

            begin_write(node);

            for (uint16_t i = 0; i < left_count; ++i) {
                node->keys[i].store(all_keys[i], std::memory_order_relaxed);
                node->chains[i].store(all_chains[i], std::memory_order_relaxed);
            }
            for (uint16_t i = left_count; i < MAX_KEYS; ++i) {
                node->keys[i].store(nullptr, std::memory_order_relaxed);
                node->chains[i].store(nullptr, std::memory_order_relaxed);
            }
            node->key_count.store(left_count, std::memory_order_release);

            for (uint16_t i = 0; i < right_count; ++i) {
                right->keys[i].store(all_keys[left_count + i], std::memory_order_relaxed);
                right->chains[i].store(all_chains[left_count + i], std::memory_order_relaxed);
            }
            for (uint16_t i = right_count; i < MAX_KEYS; ++i) {
                right->keys[i].store(nullptr, std::memory_order_relaxed);
                right->chains[i].store(nullptr, std::memory_order_relaxed);
            }
            right->key_count.store(right_count, std::memory_order_release);

            Node* old_next = node->next_leaf.load(std::memory_order_relaxed);
            right->next_leaf.store(old_next, std::memory_order_release);
            node->next_leaf.store(right, std::memory_order_release);

            end_write(node);

            SplitResult split;
            split.separator = right->keys[0].load(std::memory_order_relaxed);
            split.right = right;
            return split;
        }

        const uint16_t child_index = find_child_index(node, key);
        Node* child = node->children[child_index].load(std::memory_order_relaxed);
        if (child == nullptr) { return std::nullopt; }

        std::optional<SplitResult> child_split = insert_recursive(child, record);
        if (!child_split.has_value()) { return std::nullopt; }

        const uint16_t key_count = node->key_count.load(std::memory_order_relaxed);
        const uint16_t insert_pos = child_index;

        if (key_count < MAX_KEYS) {
            begin_write(node);
            for (uint16_t i = key_count; i > insert_pos; --i) {
                node->keys[i].store(node->keys[i - 1].load(std::memory_order_relaxed), std::memory_order_relaxed);
            }
            for (uint16_t i = static_cast<uint16_t>(key_count + 1); i > static_cast<uint16_t>(insert_pos + 1); --i) {
                node->children[i].store(node->children[i - 1].load(std::memory_order_relaxed), std::memory_order_relaxed);
            }
            node->keys[insert_pos].store(child_split->separator, std::memory_order_release);
            node->children[insert_pos + 1].store(child_split->right, std::memory_order_release);
            node->key_count.store(static_cast<uint16_t>(key_count + 1), std::memory_order_release);
            end_write(node);
            return std::nullopt;
        }

        std::array<const core::OwnedRecord*, MAX_KEYS + 1> all_keys{};
        std::array<Node*, MAX_KEYS + 2> all_children{};

        for (uint16_t i = 0; i < key_count; ++i) { all_keys[i] = node->keys[i].load(std::memory_order_relaxed); }
        for (uint16_t i = 0; i < static_cast<uint16_t>(key_count + 1); ++i) {
            all_children[i] = node->children[i].load(std::memory_order_relaxed);
        }

        for (uint16_t i = key_count; i > insert_pos; --i) { all_keys[i] = all_keys[i - 1]; }
        all_keys[insert_pos] = child_split->separator;

        for (uint16_t i = static_cast<uint16_t>(key_count + 1); i > static_cast<uint16_t>(insert_pos + 1); --i) {
            all_children[i] = all_children[i - 1];
        }
        all_children[insert_pos + 1] = child_split->right;

        const uint16_t total_keys = static_cast<uint16_t>(MAX_KEYS + 1);
        const uint16_t mid = static_cast<uint16_t>(total_keys / 2);

        Node* right = make_node(false);

        begin_write(node);

        for (uint16_t i = 0; i < mid; ++i) {
            node->keys[i].store(all_keys[i], std::memory_order_relaxed);
            node->children[i].store(all_children[i], std::memory_order_relaxed);
        }
        node->children[mid].store(all_children[mid], std::memory_order_relaxed);
        for (uint16_t i = mid; i < MAX_KEYS; ++i) { node->keys[i].store(nullptr, std::memory_order_relaxed); }
        for (uint16_t i = static_cast<uint16_t>(mid + 1); i < MAX_KEYS + 1; ++i) {
            node->children[i].store(nullptr, std::memory_order_relaxed);
        }
        node->key_count.store(mid, std::memory_order_release);

        const uint16_t right_key_count = static_cast<uint16_t>(total_keys - mid - 1);
        for (uint16_t i = 0; i < right_key_count; ++i) {
            right->keys[i].store(all_keys[mid + 1 + i], std::memory_order_relaxed);
            right->children[i].store(all_children[mid + 1 + i], std::memory_order_relaxed);
        }
        right->children[right_key_count].store(all_children[total_keys], std::memory_order_relaxed);
        for (uint16_t i = right_key_count; i < MAX_KEYS; ++i) { right->keys[i].store(nullptr, std::memory_order_relaxed); }
        for (uint16_t i = static_cast<uint16_t>(right_key_count + 1); i < MAX_KEYS + 1; ++i) {
            right->children[i].store(nullptr, std::memory_order_relaxed);
        }
        right->key_count.store(right_key_count, std::memory_order_release);

        const core::OwnedRecord* promoted = all_keys[mid];

        end_write(node);

        SplitResult split;
        split.separator = promoted;
        split.right = right;
        return split;
    }

    BPTreeMemTable::Node* BPTreeMemTable::descend_to_candidate_leaf(std::span<const uint8_t> key) const noexcept {
        Node* current = root_.load(std::memory_order_acquire);
        while (current != nullptr && !current->is_leaf) {
            Node* next_child = nullptr;
            for (;;) {
                const uint64_t begin = current->version.load(std::memory_order_acquire);
                if ((begin & 1ULL) != 0ULL) { continue; }
                const uint16_t child_index = find_child_index(current, key);
                next_child = current->children[child_index].load(std::memory_order_acquire);
                const uint64_t end = current->version.load(std::memory_order_acquire);
                if (begin == end && (end & 1ULL) == 0ULL) { break; }
            }
            current = next_child;
        }
        return current;
    }

    Status BPTreeMemTable::put(
        ByteView key,
        ByteView value,
        uint64_t seq,
        uint8_t flags,
        uint64_t precomputed_fp64,
        uint64_t precomputed_mk
    ) {
        if (frozen_.load(std::memory_order_acquire)) { return Status::Error(Status::Code::InvalidArgument, "memtable is frozen"); }
        if (key.size() > std::numeric_limits<uint16_t>::max() || value.size() > std::numeric_limits<uint16_t>::max()) {
            return Status::Error(Status::Code::InvalidArgument, "key/value too large for MemHdr16");
        }

        const auto key_u8 = as_u8(key);
        const auto value_u8 = as_u8(value);
        const core::OwnedRecord* record = make_record(key_u8, value_u8, seq, flags, precomputed_fp64, precomputed_mk);

        Node* current_root = root_.load(std::memory_order_acquire);
        std::optional<SplitResult> split = insert_recursive(current_root, record);
        if (!split.has_value()) { return Status::OK(); }

        Node* new_root = make_node(false);
        new_root->keys[0].store(split->separator, std::memory_order_relaxed);
        new_root->children[0].store(current_root, std::memory_order_relaxed);
        new_root->children[1].store(split->right, std::memory_order_relaxed);
        new_root->key_count.store(1, std::memory_order_relaxed);

        root_.store(new_root, std::memory_order_release);
        return Status::OK();
    }

    bool BPTreeMemTable::get(ByteView key, uint64_t snapshot_seq, RecordView* out) const {
        if (out == nullptr) { return false; }

        const std::span<const uint8_t> target = as_u8(key);
        Node* leaf = descend_to_candidate_leaf(target);

        while (leaf != nullptr) {
            uint16_t pos = 0;
            uint16_t key_count = 0;
            int cmp = 1;
            VersionChain* chain = nullptr;

            for (;;) {
                const uint64_t begin = leaf->version.load(std::memory_order_acquire);
                if ((begin & 1ULL) != 0ULL) { continue; }
                key_count = leaf->key_count.load(std::memory_order_acquire);
                pos = find_leaf_position(leaf, target, key_count);
                cmp = 1;
                chain = nullptr;
                if (pos < key_count) {
                    const core::OwnedRecord* candidate_key = leaf->keys[pos].load(std::memory_order_relaxed);
                    cmp = compare_record_key(candidate_key, target);
                    if (cmp == 0) { chain = leaf->chains[pos].load(std::memory_order_relaxed); }
                }
                const uint64_t end = leaf->version.load(std::memory_order_acquire);
                if (begin == end && (end & 1ULL) == 0ULL) { break; }
            }

            if (pos < key_count) {
                if (cmp == 0) { return visible_record(chain, snapshot_seq, out); }
                if (cmp > 0) { return false; }
            }

            leaf = leaf->next_leaf.load(std::memory_order_acquire);
        }

        return false;
    }

    ArenaGenerator<RecordView> BPTreeMemTable::iterate_snapshot(uint64_t snapshot_seq) const {
        std::vector<RecordView> visible_records;
        visible_records.reserve(entryCount());
        bool ordered_unique = true;
        bool has_prev = false;
        RecordView prev;

        Node* node = root_.load(std::memory_order_acquire);
        while (node != nullptr && !node->is_leaf) { node = node->children[0].load(std::memory_order_acquire); }

        while (node != nullptr) {
            std::array<VersionChain*, MAX_KEYS> chains{};
            uint16_t key_count = 0;
            for (;;) {
                const uint64_t begin = node->version.load(std::memory_order_acquire);
                if ((begin & 1ULL) != 0ULL) { continue; }
                key_count = node->key_count.load(std::memory_order_acquire);
                for (uint16_t i = 0; i < key_count; ++i) { chains[i] = node->chains[i].load(std::memory_order_acquire); }
                const uint64_t end = node->version.load(std::memory_order_acquire);
                if (begin == end && (end & 1ULL) == 0ULL) { break; }
            }

            for (uint16_t i = 0; i < key_count; ++i) {
                RecordView visible;
                if (visible_record(chains[i], snapshot_seq, &visible)) {
                    if (has_prev) { if (prev.compare_key(visible) >= 0) { ordered_unique = false; } }
                    prev = visible;
                    has_prev = true;
                    visible_records.push_back(visible);
                }
            }

            node = node->next_leaf.load(std::memory_order_acquire);
        }

        if (ordered_unique) {
            for (const RecordView& rec : visible_records) { co_yield rec; }
            co_return;
        }

        std::sort(
            visible_records.begin(),
            visible_records.end(),
            [](const RecordView& a, const RecordView& b) {
                const int cmp = a.compare_key(b);
                if (cmp != 0) { return cmp < 0; }
                return a.seq() > b.seq();
            }
        );

        for (size_t i = 0; i < visible_records.size(); ++i) {
            if (i > 0 && visible_records[i - 1].compare_key(visible_records[i]) == 0) { continue; }
            co_yield visible_records[i];
        }
    }

    ArenaGenerator<RecordView> BPTreeMemTable::iterate_snapshot_range(
        uint64_t snapshot_seq,
        std::vector<uint8_t> start_key,
        std::vector<uint8_t> end_key
    ) const {
        const std::span<const uint8_t> start{start_key.data(), start_key.size()};
        const std::span<const uint8_t> end{end_key.data(), end_key.size()};
        if (!start.empty() && !end.empty() && compare_key_bytes(start, end) >= 0) { co_return; }

        std::vector<RecordView> visible_records;
        visible_records.reserve(128);
        bool ordered_unique = true;
        bool has_prev = false;
        RecordView prev;

        Node* node = nullptr;
        uint16_t first_pos = 0;
        bool first_leaf = true;

        if (start.empty()) {
            node = root_.load(std::memory_order_acquire);
            while (node != nullptr && !node->is_leaf) { node = node->children[0].load(std::memory_order_acquire); }
        }
        else {
            node = descend_to_candidate_leaf(start);
            while (node != nullptr) {
                Node* next_leaf = nullptr;
                uint16_t key_count = 0;
                for (;;) {
                    const uint64_t begin = node->version.load(std::memory_order_acquire);
                    if ((begin & 1ULL) != 0ULL) { continue; }
                    key_count = node->key_count.load(std::memory_order_acquire);
                    first_pos = find_leaf_position(node, start, key_count);
                    next_leaf = node->next_leaf.load(std::memory_order_acquire);
                    const uint64_t end_version = node->version.load(std::memory_order_acquire);
                    if (begin == end_version && (end_version & 1ULL) == 0ULL) { break; }
                }
                if (first_pos < key_count) { break; }
                node = next_leaf;
                first_pos = 0;
            }
        }

        while (node != nullptr) {
            std::array<const core::OwnedRecord*, MAX_KEYS> keys{};
            std::array<VersionChain*, MAX_KEYS> chains{};
            Node* next_leaf = nullptr;
            uint16_t key_count = 0;

            for (;;) {
                const uint64_t begin = node->version.load(std::memory_order_acquire);
                if ((begin & 1ULL) != 0ULL) { continue; }
                key_count = node->key_count.load(std::memory_order_acquire);
                for (uint16_t i = 0; i < key_count; ++i) {
                    keys[i] = node->keys[i].load(std::memory_order_acquire);
                    chains[i] = node->chains[i].load(std::memory_order_acquire);
                }
                next_leaf = node->next_leaf.load(std::memory_order_acquire);
                const uint64_t end_version = node->version.load(std::memory_order_acquire);
                if (begin == end_version && (end_version & 1ULL) == 0ULL) { break; }
            }

            const uint16_t start_pos = first_leaf ? first_pos : 0;
            first_leaf = false;

            for (uint16_t i = start_pos; i < key_count; ++i) {
                const core::OwnedRecord* key_record = keys[i];
                if (key_record == nullptr) { continue; }
                if (!start.empty() && compare_record_key(key_record, start) < 0) { continue; }
                if (!end.empty() && compare_record_key(key_record, end) >= 0) {
                    node = nullptr;
                    break;
                }

                RecordView visible;
                if (!visible_record(chains[i], snapshot_seq, &visible)) { continue; }
                if (has_prev && prev.compare_key(visible) >= 0) { ordered_unique = false; }
                prev = visible;
                has_prev = true;
                visible_records.push_back(visible);
            }

            if (node != nullptr) { node = next_leaf; }
        }

        if (ordered_unique) {
            for (const RecordView& rec : visible_records) { co_yield rec; }
            co_return;
        }

        std::sort(
            visible_records.begin(),
            visible_records.end(),
            [](const RecordView& a, const RecordView& b) {
                const int cmp = a.compare_key(b);
                if (cmp != 0) { return cmp < 0; }
                return a.seq() > b.seq();
            }
        );

        for (size_t i = 0; i < visible_records.size(); ++i) {
            if (i > 0 && visible_records[i - 1].compare_key(visible_records[i]) == 0) { continue; }
            co_yield visible_records[i];
        }
    }

    ArenaGenerator<RecordView> BPTreeMemTable::iterator(ByteView start_key, ByteView end_key, uint64_t snapshot_seq) const {
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

    void BPTreeMemTable::freeze() { frozen_.store(true, std::memory_order_release); }

    size_t BPTreeMemTable::sizeBytes() const { return bytes_.load(std::memory_order_acquire); }

    size_t BPTreeMemTable::entryCount() const { return entries_.load(std::memory_order_acquire); }
} // namespace akkaradb::engine

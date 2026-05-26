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

// akkengine/src/engine/memtable/ARTMemTable.cpp
#include "akk/engine/memtable/ARTMemTable.hpp"

#include <algorithm>
#include <cstring>
#include <limits>

#include "akk/core/record/KeyFingerprint.hpp"

namespace {
    size_t common_prefix_len(std::span<const uint8_t> a, std::span<const uint8_t> b) noexcept {
        const size_t n = std::min(a.size(), b.size());
        size_t i = 0;
        while (i < n && a[i] == b[i]) { ++i; }
        return i;
    }

    bool subtree_all_less_than(std::span<const uint8_t> subtree_prefix, std::span<const uint8_t> bound) noexcept {
        if (bound.empty()) { return false; }

        const size_t cp = common_prefix_len(subtree_prefix, bound);
        const size_t min_len = std::min(subtree_prefix.size(), bound.size());
        if (cp == min_len) { return false; }
        return subtree_prefix[cp] < bound[cp];
    }

    bool subtree_all_ge(std::span<const uint8_t> subtree_prefix, std::span<const uint8_t> bound) noexcept {
        if (bound.empty()) { return false; }

        const size_t cp = common_prefix_len(subtree_prefix, bound);
        const size_t min_len = std::min(subtree_prefix.size(), bound.size());
        if (cp < min_len) { return subtree_prefix[cp] > bound[cp]; }
        if (bound.size() <= subtree_prefix.size()) { return true; }
        return false;
    }
} // namespace

namespace akkaradb::engine {
    ARTMemTable::ARTMemTable(
        size_t data_arena_initial_block_size,
        size_t data_arena_max_block_size,
        size_t generator_arena_initial_block_size,
        size_t generator_arena_max_block_size
    )
        : data_arena_{data_arena_initial_block_size, data_arena_max_block_size},
          generator_arena_{generator_arena_initial_block_size, generator_arena_max_block_size} {}

    std::span<const uint8_t> ARTMemTable::as_u8(ByteView view) noexcept { return {reinterpret_cast<const uint8_t*>(view.data()), view.size()}; }

    int ARTMemTable::compare_key_bytes(std::span<const uint8_t> lhs, std::span<const uint8_t> rhs) noexcept {
        const size_t min_len = std::min(lhs.size(), rhs.size());
        if (min_len > 0) {
            const int cmp = std::memcmp(lhs.data(), rhs.data(), min_len);
            if (cmp != 0) { return cmp < 0 ? -1 : 1; }
        }
        if (lhs.size() < rhs.size()) { return -1; }
        if (lhs.size() > rhs.size()) { return 1; }
        return 0;
    }

    size_t ARTMemTable::common_prefix(std::span<const uint8_t> a, std::span<const uint8_t> b) noexcept {
        const size_t n = std::min(a.size(), b.size());
        size_t i = 0;
        while (i < n && a[i] == b[i]) { ++i; }
        return i;
    }

    const uint8_t* ARTMemTable::copy_prefix(std::span<const uint8_t> prefix) {
        if (prefix.empty()) { return nullptr; }
        std::byte* mem = data_arena_.allocate(prefix.size(), alignof(uint8_t));
        std::memcpy(mem, prefix.data(), prefix.size());
        bytes_.fetch_add(prefix.size(), std::memory_order_relaxed);
        return reinterpret_cast<const uint8_t*>(mem);
    }

    ARTMemTable::VersionChain* ARTMemTable::make_chain(const core::OwnedRecord* initial) {
        VersionChain* chain = arena_new<VersionChain>();
        chain->ring[0] = initial;
        chain->head = 0;
        chain->count = 1;
        entries_.fetch_add(1, std::memory_order_relaxed);
        return chain;
    }

    ARTMemTable::VersionChain* ARTMemTable::append_chain(const VersionChain* previous, const core::OwnedRecord* record) {
        if (previous == nullptr) { return make_chain(record); }

        VersionChain* chain = arena_new<VersionChain>();
        chain->ring = previous->ring;
        chain->head = static_cast<uint8_t>((previous->head + 1) & (MAX_VERSIONS_PER_KEY - 1));
        chain->count = previous->count < MAX_VERSIONS_PER_KEY ? static_cast<uint8_t>(previous->count + 1) : previous->count;
        chain->ring[chain->head] = record;

        if (previous->count < MAX_VERSIONS_PER_KEY) { entries_.fetch_add(1, std::memory_order_relaxed); }
        return chain;
    }

    core::OwnedRecord* ARTMemTable::make_record(
        std::span<const uint8_t> key,
        std::span<const uint8_t> value,
        uint64_t seq,
        uint8_t flags,
        uint64_t precomputed_fp64,
        uint64_t precomputed_mk
    ) {
        const uint64_t fp64 = precomputed_fp64 != 0 ? precomputed_fp64 : (key.empty() ? 0ULL : core::compute_key_fp64(key.data(), key.size()));
        const uint64_t mini = precomputed_mk != 0 ? precomputed_mk : (key.empty() ? 0ULL : core::build_mini_key(key.data(), key.size()));

        core::OwnedRecord* record = arena_new<core::OwnedRecord>();
        core::OwnedRecord::create_inplace(*record, key, value, seq, flags, data_arena_, fp64, mini);
        bytes_.fetch_add(sizeof(core::OwnedRecord) + key.size() + value.size(), std::memory_order_relaxed);
        return record;
    }

    bool ARTMemTable::visible_record(const VersionChain* chain, uint64_t snapshot_seq, RecordView* out) noexcept {
        if (chain == nullptr || out == nullptr) { return false; }

        const uint8_t head = chain->head;
        const uint8_t count = chain->count;
        if (count == 0) { return false; }

        const core::OwnedRecord* newest = chain->ring[head];
        if (newest != nullptr && newest->seq() <= snapshot_seq) {
            *out = to_view(*newest);
            return true;
        }

        for (uint8_t i = 1; i < count; ++i) {
            const uint8_t idx = static_cast<uint8_t>((head - i) & (MAX_VERSIONS_PER_KEY - 1));
            const core::OwnedRecord* candidate = chain->ring[idx];
            if (candidate != nullptr && candidate->seq() <= snapshot_seq) {
                *out = to_view(*candidate);
                return true;
            }
        }

        return false;
    }

    RecordView ARTMemTable::to_view(const core::OwnedRecord& record) noexcept {
        const auto key = record.key();
        const auto value = record.value();
        return {key.data(), record.hdr.k_len, value.data(), record.hdr.v_len, record.hdr.seq, record.hdr.flags, record.key_fp64, record.mini_key};
    }

    void ARTMemTable::export_children(const NodeBase* node, ChildVec& out) {
        out.clear();
        if (node == nullptr) { return; }
        out.reserve(node->child_count);
        for_each_child(node, [&](uint8_t key, NodeBase* child) { out.emplace_back(key, child); });
    }

    void ARTMemTable::insert_child_sorted(ChildVec& children, uint8_t edge, NodeBase* child) {
        const auto pos = std::lower_bound(children.begin(), children.end(), edge, [](const ChildEntry& entry, uint8_t key) { return entry.first < key; });
        children.insert(pos, ChildEntry{edge, child});
    }

    ARTMemTable::NodeBase* ARTMemTable::find_child(const NodeBase* node, uint8_t key) noexcept {
        if (node == nullptr) { return nullptr; }

        switch (node->kind) {
            case NodeBase::Kind::Node4: {
                const auto* n = static_cast<const Node4*>(node);
                const uint16_t count = n->child_count;
                for (uint16_t i = 0; i < count; ++i) {
                    if (n->keys[i] == key) { return n->children[i]; }
                }
                return nullptr;
            }
            case NodeBase::Kind::Node16: {
                const auto* n = static_cast<const Node16*>(node);
                const uint16_t count = n->child_count;
                for (uint16_t i = 0; i < count; ++i) {
                    if (n->keys[i] == key) { return n->children[i]; }
                }
                return nullptr;
            }
            case NodeBase::Kind::Node48: {
                const auto* n = static_cast<const Node48*>(node);
                const uint8_t idx = n->child_index[key];
                if (idx == 0xFF) { return nullptr; }
                return n->children[idx];
            }
            case NodeBase::Kind::Node256: {
                const auto* n = static_cast<const Node256*>(node);
                return n->children[key];
            }
        }
        return nullptr;
    }

    bool ARTMemTable::child_at(const NodeBase* node, uint16_t index, uint8_t* edge, const NodeBase** child) noexcept {
        if (node == nullptr || edge == nullptr || child == nullptr || index >= node->child_count) { return false; }

        switch (node->kind) {
            case NodeBase::Kind::Node4: {
                const auto* n = static_cast<const Node4*>(node);
                *edge = n->keys[index];
                *child = n->children[index];
                return *child != nullptr;
            }
            case NodeBase::Kind::Node16: {
                const auto* n = static_cast<const Node16*>(node);
                *edge = n->keys[index];
                *child = n->children[index];
                return *child != nullptr;
            }
            case NodeBase::Kind::Node48: {
                const auto* n = static_cast<const Node48*>(node);
                *edge = n->ordered_keys[index];
                *child = n->children[index];
                return *child != nullptr;
            }
            case NodeBase::Kind::Node256: {
                const auto* n = static_cast<const Node256*>(node);
                *edge = n->ordered_keys[index];
                *child = n->children[*edge];
                return *child != nullptr;
            }
        }

        return false;
    }

    ARTMemTable::NodeBase* ARTMemTable::build_node(std::span<const uint8_t> prefix, VersionChain* terminal, const ChildVec& children) {
        const uint8_t* prefix_ptr = copy_prefix(prefix);
        const uint16_t pfx_len = static_cast<uint16_t>(prefix.size());
        const size_t n = children.size();

        if (n <= 4) {
            Node4* node = arena_new<Node4>(prefix_ptr, pfx_len, terminal);
            node->child_count = static_cast<uint16_t>(n);
            for (size_t i = 0; i < n; ++i) {
                node->keys[i] = children[i].first;
                node->children[i] = children[i].second;
            }
            return node;
        }
        if (n <= 16) {
            Node16* node = arena_new<Node16>(prefix_ptr, pfx_len, terminal);
            node->child_count = static_cast<uint16_t>(n);
            for (size_t i = 0; i < n; ++i) {
                node->keys[i] = children[i].first;
                node->children[i] = children[i].second;
            }
            return node;
        }
        if (n <= 48) {
            Node48* node = arena_new<Node48>(prefix_ptr, pfx_len, terminal);
            node->child_count = static_cast<uint16_t>(n);
            node->child_index.fill(0xFF);
            for (size_t i = 0; i < n; ++i) {
                node->ordered_keys[i] = children[i].first;
                node->child_index[children[i].first] = static_cast<uint8_t>(i);
                node->children[i] = children[i].second;
            }
            return node;
        }

        Node256* node = arena_new<Node256>(prefix_ptr, pfx_len, terminal);
        node->child_count = static_cast<uint16_t>(n);
        for (size_t i = 0; i < n; ++i) {
            const uint8_t k = children[i].first;
            NodeBase* child = children[i].second;
            node->ordered_keys[i] = k;
            node->children[k] = child;
        }
        return node;
    }

    ARTMemTable::NodeBase* ARTMemTable::build_leaf(std::span<const uint8_t> suffix, const core::OwnedRecord* record) {
        VersionChain* chain = make_chain(record);
        const ChildVec empty;
        return build_node(suffix, chain, empty);
    }

    ARTMemTable::NodeBase* ARTMemTable::clone_with(const NodeBase* node, std::span<const uint8_t> prefix, VersionChain* terminal, const ChildVec& children) {
        (void)node;
        return build_node(prefix, terminal, children);
    }

    ARTMemTable::InsertResult ARTMemTable::insert_recursive(const NodeBase* node, std::span<const uint8_t> key, size_t depth, const core::OwnedRecord* record) {
        if (node == nullptr) { return {build_leaf(key.subspan(depth), record), true, true}; }

        const std::span<const uint8_t> node_prefix = node->prefix_len == 0
                                                         ? std::span<const uint8_t>{}
                                                         : std::span<const uint8_t>{node->prefix, node->prefix_len};
        const std::span<const uint8_t> remaining = key.subspan(depth);

        const size_t matched = common_prefix(node_prefix, remaining);
        if (matched < node_prefix.size()) {
            ChildVec original_children;
            export_children(node, original_children);

            const uint8_t existing_edge = node_prefix[matched];
            NodeBase* existing_child = clone_with(node, node_prefix.subspan(matched + 1), node->terminal, original_children);

            ChildVec split_children;
            split_children.reserve(2);

            VersionChain* new_terminal = nullptr;
            if (matched == remaining.size()) { new_terminal = make_chain(record); }
            else {
                const uint8_t new_edge = remaining[matched];
                NodeBase* new_child = build_leaf(remaining.subspan(matched + 1), record);
                if (new_edge < existing_edge) {
                    split_children.emplace_back(new_edge, new_child);
                    split_children.emplace_back(existing_edge, existing_child);
                }
                else {
                    split_children.emplace_back(existing_edge, existing_child);
                    split_children.emplace_back(new_edge, new_child);
                }
            }
            if (matched == remaining.size()) { split_children.emplace_back(existing_edge, existing_child); }

            NodeBase* parent = build_node(node_prefix.first(matched), new_terminal, split_children);
            return {parent, true, true};
        }

        depth += node_prefix.size();
        if (depth == key.size()) {
            if (node->terminal != nullptr) {
                VersionChain* terminal = append_chain(node->terminal, record);
                ChildVec children;
                export_children(node, children);
                NodeBase* updated = clone_with(node, node_prefix, terminal, children);
                return {updated, true, false};
            }
            VersionChain* terminal = make_chain(record);
            ChildVec children;
            export_children(node, children);
            NodeBase* updated = clone_with(node, node_prefix, terminal, children);
            return {updated, true, true};
        }

        const uint8_t edge = key[depth];
        NodeBase* child = find_child(node, edge);
        if (child == nullptr) {
            ChildVec children;
            export_children(node, children);
            insert_child_sorted(children, edge, build_leaf(key.subspan(depth + 1), record));
            NodeBase* updated = clone_with(node, node_prefix, node->terminal, children);
            return {updated, true, true};
        }

        InsertResult child_result = insert_recursive(child, key, depth + 1, record);
        if (!child_result.tree_changed) { return {const_cast<NodeBase*>(node), false, child_result.inserted_new_key}; }

        ChildVec children;
        export_children(node, children);
        for (auto& entry : children) {
            if (entry.first == edge) {
                entry.second = child_result.node;
                break;
            }
        }
        NodeBase* updated = clone_with(node, node_prefix, node->terminal, children);
        return {updated, true, child_result.inserted_new_key};
    }

    bool ARTMemTable::get_from_root(const NodeBase* root, std::span<const uint8_t> key, uint64_t snapshot_seq, RecordView* out) const {
        if (root == nullptr || out == nullptr) { return false; }

        const NodeBase* node = root;
        size_t depth = 0;

        while (node != nullptr) {
            const std::span<const uint8_t> prefix = node->prefix_len == 0
                                                        ? std::span<const uint8_t>{}
                                                        : std::span<const uint8_t>{node->prefix, node->prefix_len};
            const std::span<const uint8_t> remaining = key.subspan(depth);
            if (remaining.size() < prefix.size()) { return false; }
            if (!prefix.empty() && std::memcmp(prefix.data(), remaining.data(), prefix.size()) != 0) { return false; }
            depth += prefix.size();

            if (depth == key.size()) { return visible_record(node->terminal, snapshot_seq, out); }

            node = find_child(node, key[depth]);
            ++depth;
        }

        return false;
    }

    ArenaGenerator<RecordView> ARTMemTable::iterate_snapshot_range(uint64_t snapshot_seq, std::vector<uint8_t> start_key, std::vector<uint8_t> end_key) const {
        const std::span<const uint8_t> start{start_key.data(), start_key.size()};
        const std::span<const uint8_t> end{end_key.data(), end_key.size()};
        if (!start.empty() && !end.empty() && compare_key_bytes(start, end) >= 0) { co_return; }

        const NodeBase* root = root_.load(std::memory_order_acquire);
        if (root == nullptr) { co_return; }

        if (start.empty() && end.empty()) {
            struct FullScanFrame {
                const NodeBase* node{nullptr};
                uint16_t next_child{0};
                bool terminal_done{false};
            };

            std::vector<FullScanFrame> stack;
            stack.reserve(64);
            stack.push_back({root, 0, false});

            while (!stack.empty()) {
                FullScanFrame& frame = stack.back();
                const NodeBase* node = frame.node;
                if (node == nullptr) {
                    stack.pop_back();
                    continue;
                }

                if (!frame.terminal_done) {
                    frame.terminal_done = true;
                    RecordView visible;
                    if (visible_record(node->terminal, snapshot_seq, &visible)) { co_yield visible; }
                }

                if (frame.next_child >= node->child_count) {
                    stack.pop_back();
                    continue;
                }

                const NodeBase* child = nullptr;
                const uint16_t idx = frame.next_child++;
                switch (node->kind) {
                    case NodeBase::Kind::Node4: {
                        const auto* n = static_cast<const Node4*>(node);
                        child = n->children[idx];
                        break;
                    }
                    case NodeBase::Kind::Node16: {
                        const auto* n = static_cast<const Node16*>(node);
                        child = n->children[idx];
                        break;
                    }
                    case NodeBase::Kind::Node48: {
                        const auto* n = static_cast<const Node48*>(node);
                        child = n->children[idx];
                        break;
                    }
                    case NodeBase::Kind::Node256: {
                        const auto* n = static_cast<const Node256*>(node);
                        const uint8_t edge = n->ordered_keys[idx];
                        child = n->children[edge];
                        break;
                    }
                }
                if (child != nullptr) { stack.push_back({child, 0, false}); }
            }
            co_return;
        }

        struct ScanFrame {
            const NodeBase* node{nullptr};
            uint16_t next_child{0};
            size_t restore_len{0};
            bool entered{false};
            bool terminal_done{false};
        };

        std::vector<ScanFrame> stack;
        stack.reserve(64);
        std::vector<uint8_t> path;
        path.reserve(128);
        stack.push_back({root, 0, 0, false, false});

        bool stop = false;
        while (!stack.empty() && !stop) {
            ScanFrame& frame = stack.back();
            const NodeBase* node = frame.node;
            if (node == nullptr) {
                path.resize(frame.restore_len);
                stack.pop_back();
                continue;
            }

            if (!frame.entered) {
                frame.entered = true;
                if (node->prefix_len > 0) {
                    const size_t old_size = path.size();
                    path.resize(old_size + node->prefix_len);
                    std::memcpy(path.data() + old_size, node->prefix, node->prefix_len);
                }

                const std::span<const uint8_t> subtree_prefix{path.data(), path.size()};
                if (!start.empty() && subtree_all_less_than(subtree_prefix, start)) {
                    path.resize(frame.restore_len);
                    stack.pop_back();
                    continue;
                }
                if (!end.empty() && subtree_all_ge(subtree_prefix, end)) {
                    stop = true;
                    continue;
                }
            }

            if (!frame.terminal_done) {
                frame.terminal_done = true;
                RecordView visible;
                if (visible_record(node->terminal, snapshot_seq, &visible)) {
                    if (!start.empty() && visible.compare_key(start) < 0) {
                        // Skip keys below lower bound.
                    }
                    else if (!end.empty() && visible.compare_key(end) >= 0) {
                        stop = true;
                        continue;
                    }
                    else { co_yield visible; }
                }
            }

            if (frame.next_child >= node->child_count) {
                path.resize(frame.restore_len);
                stack.pop_back();
                continue;
            }

            uint8_t edge = 0;
            const NodeBase* child = nullptr;
            const uint16_t idx = frame.next_child++;
            if (!child_at(node, idx, &edge, &child) || child == nullptr) { continue; }

            const size_t parent_path_len = path.size();
            path.push_back(edge);
            stack.push_back({child, 0, parent_path_len, false, false});
        }
    }

    Status ARTMemTable::put(ByteView key, ByteView value, uint64_t seq, uint8_t flags, uint64_t precomputed_fp64, uint64_t precomputed_mk) {
        if (frozen_.load(std::memory_order_acquire)) { return Status::Error(Status::Code::InvalidArgument, "memtable is frozen"); }
        if (key.size() > std::numeric_limits<uint16_t>::max() || value.size() > std::numeric_limits<uint16_t>::max()) {
            return Status::Error(Status::Code::InvalidArgument, "key/value too large for MemHdr16");
        }

        const std::span<const uint8_t> key_u8 = as_u8(key);
        const std::span<const uint8_t> value_u8 = as_u8(value);
        const core::OwnedRecord* record = make_record(key_u8, value_u8, seq, flags, precomputed_fp64, precomputed_mk);

        NodeBase* root = root_.load(std::memory_order_acquire);
        InsertResult result = insert_recursive(root, key_u8, 0, record);
        if (result.tree_changed) { root_.store(result.node, std::memory_order_release); }
        return Status::OK();
    }

    bool ARTMemTable::get(ByteView key, uint64_t snapshot_seq, RecordView* out) const {
        const std::span<const uint8_t> key_u8 = as_u8(key);
        const NodeBase* root = root_.load(std::memory_order_acquire);
        return get_from_root(root, key_u8, snapshot_seq, out);
    }

    ArenaGenerator<RecordView> ARTMemTable::iterator(ByteView start_key, ByteView end_key, uint64_t snapshot_seq) const {
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

    void ARTMemTable::freeze() { frozen_.store(true, std::memory_order_release); }

    size_t ARTMemTable::sizeBytes() const { return bytes_.load(std::memory_order_acquire); }

    size_t ARTMemTable::entryCount() const { return entries_.load(std::memory_order_acquire); }
} // namespace akkaradb::engine

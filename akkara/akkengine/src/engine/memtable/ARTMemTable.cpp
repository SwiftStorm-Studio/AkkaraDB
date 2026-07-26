/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/memtable/ARTMemTable.cpp
#include "akk/engine/memtable/ARTMemTable.hpp"

#include <algorithm>
#include <cstring>
#include <limits>

#include "akk/core/record/KeyFingerprint.hpp"

namespace {
    size_t commonPrefixLen(std::span<const uint8_t> a, std::span<const uint8_t> b) noexcept {
        const size_t n = std::min(a.size(), b.size());
        size_t i = 0;
        while (i < n && a[i] == b[i]) { ++i; }
        return i;
    }

    bool subtreeAllLessThan(std::span<const uint8_t> subtreePrefix, std::span<const uint8_t> bound) noexcept {
        if (bound.empty()) { return false; }

        const size_t cp = commonPrefixLen(subtreePrefix, bound);
        const size_t minLen = std::min(subtreePrefix.size(), bound.size());
        if (cp == minLen) { return false; }
        return subtreePrefix[cp] < bound[cp];
    }

    bool subtreeAllGe(std::span<const uint8_t> subtreePrefix, std::span<const uint8_t> bound) noexcept {
        if (bound.empty()) { return false; }

        const size_t cp = commonPrefixLen(subtreePrefix, bound);
        const size_t minLen = std::min(subtreePrefix.size(), bound.size());
        if (cp < minLen) { return subtreePrefix[cp] > bound[cp]; }
        if (bound.size() <= subtreePrefix.size()) { return true; }
        return false;
    }
} // namespace

namespace akkaradb::engine::memtable {
    ARTMemTable::ARTMemTable(
        size_t dataArenaInitialBlockSize,
        size_t dataArenaMaxBlockSize,
        size_t generatorArenaInitialBlockSize,
        size_t generatorArenaMaxBlockSize,
        MemTableBackendOptions backendOptions
    )
        : dataArena_{dataArenaInitialBlockSize, dataArenaMaxBlockSize},
          generatorArenaInitialBlockSize_{generatorArenaInitialBlockSize},
          generatorArenaMaxBlockSize_{generatorArenaMaxBlockSize} { (void)backendOptions; }

    std::span<const uint8_t> ARTMemTable::asU8(ByteView view) noexcept {
        return {reinterpret_cast<const uint8_t*>(view.data()), view.size()};
    }

    int ARTMemTable::compareKeyBytes(std::span<const uint8_t> lhs, std::span<const uint8_t> rhs) noexcept {
        const size_t minLen = std::min(lhs.size(), rhs.size());
        if (minLen > 0) {
            const int cmp = std::memcmp(lhs.data(), rhs.data(), minLen);
            if (cmp != 0) { return cmp < 0 ? -1 : 1; }
        }
        if (lhs.size() < rhs.size()) { return -1; }
        if (lhs.size() > rhs.size()) { return 1; }
        return 0;
    }

    size_t ARTMemTable::commonPrefix(std::span<const uint8_t> a, std::span<const uint8_t> b) noexcept {
        const size_t n = std::min(a.size(), b.size());
        size_t i = 0;
        while (i < n && a[i] == b[i]) { ++i; }
        return i;
    }

    const uint8_t* ARTMemTable::copyPrefix(std::span<const uint8_t> prefix) {
        if (prefix.empty()) { return nullptr; }
        std::byte* mem = dataArena_.allocate(prefix.size(), alignof(uint8_t));
        std::memcpy(mem, prefix.data(), prefix.size());
        bytes_.fetch_add(prefix.size(), std::memory_order_relaxed);
        return reinterpret_cast<const uint8_t*>(mem);
    }

    ARTMemTable::VersionChain* ARTMemTable::makeChain(const core::OwnedRecord* initial) {
        VersionChain* chain = arenaNew<VersionChain>();
        chain->ring[0] = initial;
        chain->head = 0;
        chain->count = 1;
        entries_.fetch_add(1, std::memory_order_relaxed);
        return chain;
    }

    ARTMemTable::VersionChain* ARTMemTable::appendChain(const VersionChain* previous, const core::OwnedRecord* record) {
        if (previous == nullptr) { return makeChain(record); }

        VersionChain* chain = arenaNew<VersionChain>();
        chain->ring = previous->ring;
        chain->head = static_cast<uint8_t>((previous->head + 1) & (MAX_VERSIONS_PER_KEY - 1));
        chain->count = previous->count < MAX_VERSIONS_PER_KEY ? static_cast<uint8_t>(previous->count + 1) : previous->count;
        chain->ring[chain->head] = record;

        if (previous->count < MAX_VERSIONS_PER_KEY) { entries_.fetch_add(1, std::memory_order_relaxed); }
        return chain;
    }

    core::OwnedRecord* ARTMemTable::makeRecord(
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

    bool ARTMemTable::visibleRecord(const VersionChain* chain, uint64_t snapshotSeq, RecordView* out) noexcept {
        if (chain == nullptr || out == nullptr) { return false; }

        const uint8_t head = chain->head;
        const uint8_t count = chain->count;
        if (count == 0) { return false; }

        const core::OwnedRecord* newest = chain->ring[head];
        if (newest != nullptr && newest->seq() <= snapshotSeq) {
            *out = toView(*newest);
            return true;
        }

        for (uint8_t i = 1; i < count; ++i) {
            const uint8_t idx = static_cast<uint8_t>((head - i) & (MAX_VERSIONS_PER_KEY - 1));
            const core::OwnedRecord* candidate = chain->ring[idx];
            if (candidate != nullptr && candidate->seq() <= snapshotSeq) {
                *out = toView(*candidate);
                return true;
            }
        }

        return false;
    }

    RecordView ARTMemTable::toView(const core::OwnedRecord& record) noexcept {
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

    void ARTMemTable::exportChildren(const NodeBase* node, ChildVec& out) {
        out.clear();
        if (node == nullptr) { return; }
        out.reserve(node->childCount);
        forEachChild(node, [&](uint8_t key, NodeBase* child) { out.emplace_back(key, child); });
    }

    void ARTMemTable::insertChildSorted(ChildVec& children, uint8_t edge, NodeBase* child) {
        const auto pos = std::lower_bound(
            children.begin(),
            children.end(),
            edge,
            [](const ChildEntry& entry, uint8_t key) { return entry.first < key; }
        );
        children.insert(pos, ChildEntry{edge, child});
    }

    ARTMemTable::NodeBase* ARTMemTable::findChild(const NodeBase* node, uint8_t key) noexcept {
        if (node == nullptr) { return nullptr; }

        switch (node->kind) {
            case NodeBase::Kind::NODE4: {
                const auto* n = static_cast<const Node4*>(node);
                const uint16_t count = n->childCount;
                for (uint16_t i = 0; i < count; ++i) { if (n->keys[i] == key) { return n->children[i]; } }
                return nullptr;
            }
            case NodeBase::Kind::NODE16: {
                const auto* n = static_cast<const Node16*>(node);
                const uint16_t count = n->childCount;
                for (uint16_t i = 0; i < count; ++i) { if (n->keys[i] == key) { return n->children[i]; } }
                return nullptr;
            }
            case NodeBase::Kind::NODE48: {
                const auto* n = static_cast<const Node48*>(node);
                const uint8_t idx = n->childIndex[key];
                if (idx == 0xFF) { return nullptr; }
                return n->children[idx];
            }
            case NodeBase::Kind::NODE256: {
                const auto* n = static_cast<const Node256*>(node);
                return n->children[key];
            }
        }
        return nullptr;
    }

    bool ARTMemTable::childAt(const NodeBase* node, uint16_t index, uint8_t* edge, const NodeBase** child) noexcept {
        if (node == nullptr || edge == nullptr || child == nullptr || index >= node->childCount) { return false; }

        switch (node->kind) {
            case NodeBase::Kind::NODE4: {
                const auto* n = static_cast<const Node4*>(node);
                *edge = n->keys[index];
                *child = n->children[index];
                return *child != nullptr;
            }
            case NodeBase::Kind::NODE16: {
                const auto* n = static_cast<const Node16*>(node);
                *edge = n->keys[index];
                *child = n->children[index];
                return *child != nullptr;
            }
            case NodeBase::Kind::NODE48: {
                const auto* n = static_cast<const Node48*>(node);
                *edge = n->orderedKeys[index];
                *child = n->children[index];
                return *child != nullptr;
            }
            case NodeBase::Kind::NODE256: {
                const auto* n = static_cast<const Node256*>(node);
                *edge = n->orderedKeys[index];
                *child = n->children[*edge];
                return *child != nullptr;
            }
        }

        return false;
    }

    ARTMemTable::NodeBase* ARTMemTable::buildNode(std::span<const uint8_t> prefix, VersionChain* terminal, const ChildVec& children) {
        const uint8_t* prefixPtr = copyPrefix(prefix);
        const uint16_t pfxLen = static_cast<uint16_t>(prefix.size());
        const size_t n = children.size();

        if (n <= 4) {
            Node4* node = arenaNew<Node4>(prefixPtr, pfxLen, terminal);
            node->childCount = static_cast<uint16_t>(n);
            for (size_t i = 0; i < n; ++i) {
                node->keys[i] = children[i].first;
                node->children[i] = children[i].second;
            }
            return node;
        }
        if (n <= 16) {
            Node16* node = arenaNew<Node16>(prefixPtr, pfxLen, terminal);
            node->childCount = static_cast<uint16_t>(n);
            for (size_t i = 0; i < n; ++i) {
                node->keys[i] = children[i].first;
                node->children[i] = children[i].second;
            }
            return node;
        }
        if (n <= 48) {
            Node48* node = arenaNew<Node48>(prefixPtr, pfxLen, terminal);
            node->childCount = static_cast<uint16_t>(n);
            node->childIndex.fill(0xFF);
            for (size_t i = 0; i < n; ++i) {
                node->orderedKeys[i] = children[i].first;
                node->childIndex[children[i].first] = static_cast<uint8_t>(i);
                node->children[i] = children[i].second;
            }
            return node;
        }

        Node256* node = arenaNew<Node256>(prefixPtr, pfxLen, terminal);
        node->childCount = static_cast<uint16_t>(n);
        for (size_t i = 0; i < n; ++i) {
            const uint8_t k = children[i].first;
            NodeBase* child = children[i].second;
            node->orderedKeys[i] = k;
            node->children[k] = child;
        }
        return node;
    }

    ARTMemTable::NodeBase* ARTMemTable::buildLeaf(std::span<const uint8_t> suffix, const core::OwnedRecord* record) {
        VersionChain* chain = makeChain(record);
        const ChildVec empty;
        return buildNode(suffix, chain, empty);
    }

    ARTMemTable::NodeBase* ARTMemTable::cloneWith(
        const NodeBase* node,
        std::span<const uint8_t> prefix,
        VersionChain* terminal,
        const ChildVec& children
    ) {
        (void)node;
        return buildNode(prefix, terminal, children);
    }

    ARTMemTable::InsertResult ARTMemTable::insertRecursive(
        const NodeBase* node,
        std::span<const uint8_t> key,
        size_t depth,
        const core::OwnedRecord* record
    ) {
        if (node == nullptr) { return {buildLeaf(key.subspan(depth), record), true, true}; }

        const std::span<const uint8_t> nodePrefix = node->prefixLen == 0
                                                        ? std::span<const uint8_t>{}
                                                        : std::span<const uint8_t>{node->prefix, node->prefixLen};
        const std::span<const uint8_t> remaining = key.subspan(depth);

        const size_t matched = commonPrefix(nodePrefix, remaining);
        if (matched < nodePrefix.size()) {
            ChildVec originalChildren;
            exportChildren(node, originalChildren);

            const uint8_t existingEdge = nodePrefix[matched];
            NodeBase* existingChild = cloneWith(node, nodePrefix.subspan(matched + 1), node->terminal, originalChildren);

            ChildVec splitChildren;
            splitChildren.reserve(2);

            VersionChain* newTerminal = nullptr;
            if (matched == remaining.size()) { newTerminal = makeChain(record); }
            else {
                const uint8_t newEdge = remaining[matched];
                NodeBase* newChild = buildLeaf(remaining.subspan(matched + 1), record);
                if (newEdge < existingEdge) {
                    splitChildren.emplace_back(newEdge, newChild);
                    splitChildren.emplace_back(existingEdge, existingChild);
                }
                else {
                    splitChildren.emplace_back(existingEdge, existingChild);
                    splitChildren.emplace_back(newEdge, newChild);
                }
            }
            if (matched == remaining.size()) { splitChildren.emplace_back(existingEdge, existingChild); }

            NodeBase* parent = buildNode(nodePrefix.first(matched), newTerminal, splitChildren);
            return {parent, true, true};
        }

        depth += nodePrefix.size();
        if (depth == key.size()) {
            if (node->terminal != nullptr) {
                VersionChain* terminal = appendChain(node->terminal, record);
                ChildVec children;
                exportChildren(node, children);
                NodeBase* updated = cloneWith(node, nodePrefix, terminal, children);
                return {updated, true, false};
            }
            VersionChain* terminal = makeChain(record);
            ChildVec children;
            exportChildren(node, children);
            NodeBase* updated = cloneWith(node, nodePrefix, terminal, children);
            return {updated, true, true};
        }

        const uint8_t edge = key[depth];
        NodeBase* child = findChild(node, edge);
        if (child == nullptr) {
            ChildVec children;
            exportChildren(node, children);
            insertChildSorted(children, edge, buildLeaf(key.subspan(depth + 1), record));
            NodeBase* updated = cloneWith(node, nodePrefix, node->terminal, children);
            return {updated, true, true};
        }

        InsertResult childResult = insertRecursive(child, key, depth + 1, record);
        if (!childResult.treeChanged) { return {const_cast<NodeBase*>(node), false, childResult.insertedNewKey}; }

        ChildVec children;
        exportChildren(node, children);
        for (auto& entry : children) {
            if (entry.first == edge) {
                entry.second = childResult.node;
                break;
            }
        }
        NodeBase* updated = cloneWith(node, nodePrefix, node->terminal, children);
        return {updated, true, childResult.insertedNewKey};
    }

    bool ARTMemTable::getFromRoot(const NodeBase* root, std::span<const uint8_t> key, uint64_t snapshotSeq, RecordView* out) const {
        if (root == nullptr || out == nullptr) { return false; }

        const NodeBase* node = root;
        size_t depth = 0;

        while (node != nullptr) {
            const std::span<const uint8_t> prefix = node->prefixLen == 0
                                                        ? std::span<const uint8_t>{}
                                                        : std::span<const uint8_t>{node->prefix, node->prefixLen};
            const std::span<const uint8_t> remaining = key.subspan(depth);
            if (remaining.size() < prefix.size()) { return false; }
            if (!prefix.empty() && std::memcmp(prefix.data(), remaining.data(), prefix.size()) != 0) { return false; }
            depth += prefix.size();

            if (depth == key.size()) { return visibleRecord(node->terminal, snapshotSeq, out); }

            node = findChild(node, key[depth]);
            ++depth;
        }

        return false;
    }

    ArenaGenerator<RecordView> ARTMemTable::iterateSnapshotRange(
        uint64_t snapshotSeq,
        std::vector<uint8_t> startKey,
        std::vector<uint8_t> endKey
    ) const {
        const std::span<const uint8_t> start{startKey.data(), startKey.size()};
        const std::span<const uint8_t> end{endKey.data(), endKey.size()};
        if (!start.empty() && !end.empty() && compareKeyBytes(start, end) >= 0) { co_return; }

        const NodeBase* root = root_.load(std::memory_order_acquire);
        if (root == nullptr) { co_return; }

        if (start.empty() && end.empty()) {
            struct FullScanFrame {
                const NodeBase* node{nullptr};
                uint16_t nextChild{0};
                bool terminalDone{false};
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

                if (!frame.terminalDone) {
                    frame.terminalDone = true;
                    RecordView visible;
                    if (visibleRecord(node->terminal, snapshotSeq, &visible)) { co_yield visible; }
                }

                if (frame.nextChild >= node->childCount) {
                    stack.pop_back();
                    continue;
                }

                const NodeBase* child = nullptr;
                const uint16_t idx = frame.nextChild++;
                switch (node->kind) {
                    case NodeBase::Kind::NODE4: {
                        const auto* n = static_cast<const Node4*>(node);
                        child = n->children[idx];
                        break;
                    }
                    case NodeBase::Kind::NODE16: {
                        const auto* n = static_cast<const Node16*>(node);
                        child = n->children[idx];
                        break;
                    }
                    case NodeBase::Kind::NODE48: {
                        const auto* n = static_cast<const Node48*>(node);
                        child = n->children[idx];
                        break;
                    }
                    case NodeBase::Kind::NODE256: {
                        const auto* n = static_cast<const Node256*>(node);
                        const uint8_t edge = n->orderedKeys[idx];
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
            uint16_t nextChild{0};
            size_t restoreLen{0};
            bool entered{false};
            bool terminalDone{false};
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
                path.resize(frame.restoreLen);
                stack.pop_back();
                continue;
            }

            if (!frame.entered) {
                frame.entered = true;
                if (node->prefixLen > 0) {
                    const size_t oldSize = path.size();
                    path.resize(oldSize + node->prefixLen);
                    std::memcpy(path.data() + oldSize, node->prefix, node->prefixLen);
                }

                const std::span<const uint8_t> subtreePrefix{path.data(), path.size()};
                if (!start.empty() && subtreeAllLessThan(subtreePrefix, start)) {
                    path.resize(frame.restoreLen);
                    stack.pop_back();
                    continue;
                }
                if (!end.empty() && subtreeAllGe(subtreePrefix, end)) {
                    stop = true;
                    continue;
                }
            }

            if (!frame.terminalDone) {
                frame.terminalDone = true;
                RecordView visible;
                if (visibleRecord(node->terminal, snapshotSeq, &visible)) {
                    if (!start.empty() && visible.compareKey(start) < 0) {
                        // Skip keys below lower bound.
                    }
                    else if (!end.empty() && visible.compareKey(end) >= 0) {
                        stop = true;
                        continue;
                    }
                    else { co_yield visible; }
                }
            }

            if (frame.nextChild >= node->childCount) {
                path.resize(frame.restoreLen);
                stack.pop_back();
                continue;
            }

            uint8_t edge = 0;
            const NodeBase* child = nullptr;
            const uint16_t idx = frame.nextChild++;
            if (!childAt(node, idx, &edge, &child) || child == nullptr) { continue; }

            const size_t parentPathLen = path.size();
            path.push_back(edge);
            stack.push_back({child, 0, parentPathLen, false, false});
        }
    }

    Status ARTMemTable::put(ByteView key, ByteView value, uint64_t seq, uint8_t flags, uint64_t precomputedFp64, uint64_t precomputedMk) {
        if (frozen_.load(std::memory_order_acquire)) { return Status::Error(Status::Code::INVALID_ARGUMENT, "memtable is frozen"); }
        if (key.size() > std::numeric_limits<uint16_t>::max() || value.size() > std::numeric_limits<uint16_t>::max()) {
            return Status::Error(Status::Code::INVALID_ARGUMENT, "key/value too large for MemHdr16");
        }

        const std::span<const uint8_t> keyU8 = asU8(key);
        const std::span<const uint8_t> valueU8 = asU8(value);
        const core::OwnedRecord* record = makeRecord(keyU8, valueU8, seq, flags, precomputedFp64, precomputedMk);

        NodeBase* root = root_.load(std::memory_order_acquire);
        InsertResult result = insertRecursive(root, keyU8, 0, record);
        if (result.treeChanged) { root_.store(result.node, std::memory_order_release); }
        return Status::OK();
    }

    bool ARTMemTable::get(ByteView key, uint64_t snapshotSeq, RecordView* out) const {
        const std::span<const uint8_t> keyU8 = asU8(key);
        const NodeBase* root = root_.load(std::memory_order_acquire);
        return getFromRoot(root, keyU8, snapshotSeq, out);
    }

    ArenaGenerator<RecordView> ARTMemTable::iterator(ByteView startKey, ByteView endKey, uint64_t snapshotSeq) const {
        const std::span<const uint8_t> start = asU8(startKey);
        const std::span<const uint8_t> end = asU8(endKey);
        std::vector<uint8_t> startOwned(start.begin(), start.end());
        std::vector<uint8_t> endOwned(end.begin(), end.end());

        return ArenaGenerator<RecordView>::withOwnedArena(
            generatorArenaInitialBlockSize_,
            generatorArenaMaxBlockSize_,
            [this, snapshotSeq, startOwned = std::move(startOwned), endOwned = std::move(endOwned)]() mutable {
                return iterateSnapshotRange(snapshotSeq, std::move(startOwned), std::move(endOwned));
            }
        );
    }

    void ARTMemTable::freeze() { frozen_.store(true, std::memory_order_release); }

    size_t ARTMemTable::sizeBytes() const { return bytes_.load(std::memory_order_acquire); }

    size_t ARTMemTable::entryCount() const { return entries_.load(std::memory_order_acquire); }
} // namespace akkaradb::engine::memtable

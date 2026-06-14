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

// akkengine/include/akk/engine/memtable/ARTMemTable.hpp
#pragma once

#include "akkaradb/Export.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <span>
#include <utility>
#include <vector>

#include "akk/core/buffer/BufferArena.hpp"
#include "akk/core/record/OwnedRecord.hpp"
#include "akk/engine/memtable/IMemTable.hpp"

namespace akkaradb::engine::memtable {
    class AKDB_API ARTMemTable final : public IMemTable {
        public:
            static constexpr uint8_t MAX_VERSIONS_PER_KEY = 4;

            explicit ARTMemTable(
                size_t dataArenaInitialBlockSize = core::BufferArena::DEFAULT_INITIAL_BLOCK_SIZE,
                size_t dataArenaMaxBlockSize = core::BufferArena::DEFAULT_MAX_BLOCK_SIZE,
                size_t generatorArenaInitialBlockSize = 64 * 1024,
                size_t generatorArenaMaxBlockSize = 2 * 1024 * 1024
            );

            [[nodiscard]] Status put(
                ByteView key,
                ByteView value,
                uint64_t seq,
                uint8_t flags,
                uint64_t precomputedFp64 = 0,
                uint64_t precomputedMk = 0
            ) override;
            [[nodiscard]] bool get(ByteView key, uint64_t snapshotSeq, RecordView* out) const override;
            [[nodiscard]] ArenaGenerator<RecordView> iterator(ByteView startKey, ByteView endKey, uint64_t snapshotSeq) const override;
            void freeze() override;

            [[nodiscard]] size_t sizeBytes() const override;
            [[nodiscard]] size_t entryCount() const override;

        private:
            struct VersionChain {
                uint8_t head{0};
                uint8_t count{0};
                std::array<const core::OwnedRecord*, MAX_VERSIONS_PER_KEY> ring{};

                VersionChain() noexcept = default;
            };

            struct NodeBase {
                enum class Kind : uint8_t {
                    NODE4, NODE16, NODE48, NODE256
                };

                Kind kind{Kind::NODE4};
                uint16_t childCount{0};
                uint16_t prefixLen{0};
                const uint8_t* prefix{nullptr};
                VersionChain* terminal{nullptr};

                NodeBase(Kind k, uint16_t count, const uint8_t* pfx, uint16_t pfxLen, VersionChain* term) noexcept
                    : kind{k}, childCount{count}, prefixLen{pfxLen}, prefix{pfx}, terminal{term} {}
            };

            struct Node4 final : NodeBase {
                std::array<uint8_t, 4> keys{};
                std::array<NodeBase*, 4> children{};

                Node4(const uint8_t* pfx, uint16_t pfxLen, VersionChain* term) noexcept : NodeBase{Kind::NODE4, 0, pfx, pfxLen, term} {}
            };

            struct Node16 final : NodeBase {
                std::array<uint8_t, 16> keys{};
                std::array<NodeBase*, 16> children{};

                Node16(const uint8_t* pfx, uint16_t pfxLen, VersionChain* term) noexcept : NodeBase{Kind::NODE16, 0, pfx, pfxLen, term} {}
            };

            static_assert((MAX_VERSIONS_PER_KEY & (MAX_VERSIONS_PER_KEY - 1)) == 0, "version ring size must be a power of two");

            struct Node48 final : NodeBase {
                std::array<uint8_t, 256> childIndex{};
                std::array<uint8_t, 48> orderedKeys{};
                std::array<NodeBase*, 48> children{};

                Node48(const uint8_t* pfx, uint16_t pfxLen, VersionChain* term) noexcept : NodeBase{Kind::NODE48, 0, pfx, pfxLen, term} {}
            };

            struct Node256 final : NodeBase {
                std::array<uint8_t, 256> orderedKeys{};
                std::array<NodeBase*, 256> children{};

                Node256(const uint8_t* pfx, uint16_t pfxLen, VersionChain* term) noexcept : NodeBase{Kind::NODE256, 0, pfx, pfxLen, term} {}
            };

            struct InsertResult {
                NodeBase* node{nullptr};
                bool treeChanged{false};
                bool insertedNewKey{false};
            };

            using ChildEntry = std::pair<uint8_t, NodeBase*>;
            using ChildVec = std::vector<ChildEntry>;

            core::BufferArena dataArena_;
            mutable core::BufferArena generatorArena_;
            mutable std::mutex generatorArenaMutex_;

            std::atomic<NodeBase*> root_{nullptr};
            std::atomic<bool> frozen_{false};
            std::atomic<size_t> bytes_{0};
            std::atomic<size_t> entries_{0};

            [[nodiscard]] static std::span<const uint8_t> asU8(ByteView view) noexcept;
            [[nodiscard]] static int compareKeyBytes(std::span<const uint8_t> lhs, std::span<const uint8_t> rhs) noexcept;
            [[nodiscard]] static size_t commonPrefix(std::span<const uint8_t> a, std::span<const uint8_t> b) noexcept;

            template <typename T, typename... Args>
            [[nodiscard]] T* arenaNew(Args&&... args) {
                std::byte* mem = dataArena_.allocate(sizeof(T), alignof(T));
                bytes_.fetch_add(sizeof(T), std::memory_order_relaxed);
                return new(mem) T(std::forward<Args>(args)...);
            }

            [[nodiscard]] const uint8_t* copyPrefix(std::span<const uint8_t> prefix);
            [[nodiscard]] VersionChain* makeChain(const core::OwnedRecord* initial);
            [[nodiscard]] VersionChain* appendChain(const VersionChain* previous, const core::OwnedRecord* record);
            [[nodiscard]] core::OwnedRecord* makeRecord(
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint64_t seq,
                uint8_t flags,
                uint64_t precomputedFp64,
                uint64_t precomputedMk
            );

            [[nodiscard]] static bool visibleRecord(const VersionChain* chain, uint64_t snapshotSeq, RecordView* out) noexcept;
            [[nodiscard]] static RecordView toView(const core::OwnedRecord& record) noexcept;

            static void exportChildren(const NodeBase* node, ChildVec& out);
            static void insertChildSorted(ChildVec& children, uint8_t edge, NodeBase* child);
            [[nodiscard]] static NodeBase* findChild(const NodeBase* node, uint8_t key) noexcept;
            [[nodiscard]] static bool childAt(const NodeBase* node, uint16_t index, uint8_t* edge, const NodeBase** child) noexcept;
            template <typename Fn>
            static void forEachChild(const NodeBase* node, Fn&& fn);

            [[nodiscard]] NodeBase* buildNode(std::span<const uint8_t> prefix, VersionChain* terminal, const ChildVec& children);
            [[nodiscard]] NodeBase* buildLeaf(std::span<const uint8_t> suffix, const core::OwnedRecord* record);
            [[nodiscard]] NodeBase* cloneWith(
                const NodeBase* node,
                std::span<const uint8_t> prefix,
                VersionChain* terminal,
                const ChildVec& children
            );

            [[nodiscard]] InsertResult insertRecursive(
                const NodeBase* node,
                std::span<const uint8_t> key,
                size_t depth,
                const core::OwnedRecord* record
            );

            [[nodiscard]] bool getFromRoot(const NodeBase* root, std::span<const uint8_t> key, uint64_t snapshotSeq, RecordView* out) const;

            [[nodiscard]] ArenaGenerator<RecordView> iterateSnapshotRange(
                uint64_t snapshotSeq,
                std::vector<uint8_t> startKey,
                std::vector<uint8_t> endKey
            ) const;
    };
} // namespace akkaradb::engine::memtable

namespace akkaradb::engine::memtable {
    template <typename Fn>
    void ARTMemTable::forEachChild(const NodeBase* node, Fn&& fn) {
        if (node == nullptr) { return; }

        switch (node->kind) {
            case NodeBase::Kind::NODE4: {
                const auto* n = static_cast<const Node4*>(node);
                for (uint16_t i = 0; i < n->childCount; ++i) { fn(n->keys[i], n->children[i]); }
                return;
            }
            case NodeBase::Kind::NODE16: {
                const auto* n = static_cast<const Node16*>(node);
                for (uint16_t i = 0; i < n->childCount; ++i) { fn(n->keys[i], n->children[i]); }
                return;
            }
            case NodeBase::Kind::NODE48: {
                const auto* n = static_cast<const Node48*>(node);
                for (uint16_t i = 0; i < n->childCount; ++i) { fn(n->orderedKeys[i], n->children[i]); }
                return;
            }
            case NodeBase::Kind::NODE256: {
                const auto* n = static_cast<const Node256*>(node);
                for (uint16_t i = 0; i < n->childCount; ++i) {
                    const uint8_t key = n->orderedKeys[i];
                    fn(key, n->children[key]);
                }
                return;
            }
        }
    }
} // namespace akkaradb::engine::memtable

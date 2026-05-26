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

namespace akkaradb::engine {
    class ARTMemTable final : public IMemTable {
        public:
            static constexpr uint8_t MAX_VERSIONS_PER_KEY = 4;

            explicit ARTMemTable(
                size_t data_arena_initial_block_size = core::BufferArena::DEFAULT_INITIAL_BLOCK_SIZE,
                size_t data_arena_max_block_size = core::BufferArena::DEFAULT_MAX_BLOCK_SIZE,
                size_t generator_arena_initial_block_size = 64 * 1024,
                size_t generator_arena_max_block_size = 2 * 1024 * 1024
            );

            [[nodiscard]] Status put(
                ByteView key,
                ByteView value,
                uint64_t seq,
                uint8_t flags,
                uint64_t precomputed_fp64 = 0,
                uint64_t precomputed_mk = 0
            ) override;
            [[nodiscard]] bool get(ByteView key, uint64_t snapshot_seq, RecordView* out) const override;
            [[nodiscard]] ArenaGenerator<RecordView> iterator(ByteView start_key, ByteView end_key, uint64_t snapshot_seq) const override;
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
                    Node4, Node16, Node48, Node256
                };

                Kind kind{Kind::Node4};
                uint16_t child_count{0};
                uint16_t prefix_len{0};
                const uint8_t* prefix{nullptr};
                VersionChain* terminal{nullptr};

                NodeBase(Kind k, uint16_t count, const uint8_t* pfx, uint16_t pfx_len, VersionChain* term) noexcept
                    : kind{k}, child_count{count}, prefix_len{pfx_len}, prefix{pfx}, terminal{term} {}
            };

            struct Node4 final : NodeBase {
                std::array<uint8_t, 4> keys{};
                std::array<NodeBase*, 4> children{};

                Node4(const uint8_t* pfx, uint16_t pfx_len, VersionChain* term) noexcept : NodeBase{Kind::Node4, 0, pfx, pfx_len, term} {}
            };

            struct Node16 final : NodeBase {
                std::array<uint8_t, 16> keys{};
                std::array<NodeBase*, 16> children{};

                Node16(const uint8_t* pfx, uint16_t pfx_len, VersionChain* term) noexcept : NodeBase{Kind::Node16, 0, pfx, pfx_len, term} {}
            };

            static_assert((MAX_VERSIONS_PER_KEY & (MAX_VERSIONS_PER_KEY - 1)) == 0, "version ring size must be a power of two");

            struct Node48 final : NodeBase {
                std::array<uint8_t, 256> child_index{};
                std::array<uint8_t, 48> ordered_keys{};
                std::array<NodeBase*, 48> children{};

                Node48(const uint8_t* pfx, uint16_t pfx_len, VersionChain* term) noexcept : NodeBase{Kind::Node48, 0, pfx, pfx_len, term} {}
            };

            struct Node256 final : NodeBase {
                std::array<uint8_t, 256> ordered_keys{};
                std::array<NodeBase*, 256> children{};

                Node256(const uint8_t* pfx, uint16_t pfx_len, VersionChain* term) noexcept : NodeBase{Kind::Node256, 0, pfx, pfx_len, term} {}
            };

            struct InsertResult {
                NodeBase* node{nullptr};
                bool tree_changed{false};
                bool inserted_new_key{false};
            };

            using ChildEntry = std::pair<uint8_t, NodeBase*>;
            using ChildVec = std::vector<ChildEntry>;

            core::BufferArena data_arena_;
            mutable core::BufferArena generator_arena_;
            mutable std::mutex generator_arena_mutex_;

            std::atomic<NodeBase*> root_{nullptr};
            std::atomic<bool> frozen_{false};
            std::atomic<size_t> bytes_{0};
            std::atomic<size_t> entries_{0};

            [[nodiscard]] static std::span<const uint8_t> as_u8(ByteView view) noexcept;
            [[nodiscard]] static int compare_key_bytes(std::span<const uint8_t> lhs, std::span<const uint8_t> rhs) noexcept;
            [[nodiscard]] static size_t common_prefix(std::span<const uint8_t> a, std::span<const uint8_t> b) noexcept;

            template <typename T, typename... Args>
            [[nodiscard]] T* arena_new(Args&&... args) {
                std::byte* mem = data_arena_.allocate(sizeof(T), alignof(T));
                bytes_.fetch_add(sizeof(T), std::memory_order_relaxed);
                return new(mem) T(std::forward<Args>(args)...);
            }

            [[nodiscard]] const uint8_t* copy_prefix(std::span<const uint8_t> prefix);
            [[nodiscard]] VersionChain* make_chain(const core::OwnedRecord* initial);
            [[nodiscard]] VersionChain* append_chain(const VersionChain* previous, const core::OwnedRecord* record);
            [[nodiscard]] core::OwnedRecord* make_record(
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint64_t seq,
                uint8_t flags,
                uint64_t precomputed_fp64,
                uint64_t precomputed_mk
            );

            [[nodiscard]] static bool visible_record(const VersionChain* chain, uint64_t snapshot_seq, RecordView* out) noexcept;
            [[nodiscard]] static RecordView to_view(const core::OwnedRecord& record) noexcept;

            static void export_children(const NodeBase* node, ChildVec& out);
            static void insert_child_sorted(ChildVec& children, uint8_t edge, NodeBase* child);
            [[nodiscard]] static NodeBase* find_child(const NodeBase* node, uint8_t key) noexcept;
            [[nodiscard]] static bool child_at(const NodeBase* node, uint16_t index, uint8_t* edge, const NodeBase** child) noexcept;
            template <typename Fn>
            static void for_each_child(const NodeBase* node, Fn&& fn);

            [[nodiscard]] NodeBase* build_node(std::span<const uint8_t> prefix, VersionChain* terminal, const ChildVec& children);
            [[nodiscard]] NodeBase* build_leaf(std::span<const uint8_t> suffix, const core::OwnedRecord* record);
            [[nodiscard]] NodeBase* clone_with(const NodeBase* node, std::span<const uint8_t> prefix, VersionChain* terminal, const ChildVec& children);

            [[nodiscard]] InsertResult insert_recursive(const NodeBase* node, std::span<const uint8_t> key, size_t depth, const core::OwnedRecord* record);

            [[nodiscard]] bool get_from_root(const NodeBase* root, std::span<const uint8_t> key, uint64_t snapshot_seq, RecordView* out) const;

            [[nodiscard]] ArenaGenerator<RecordView> iterate_snapshot_range(
                uint64_t snapshot_seq,
                std::vector<uint8_t> start_key,
                std::vector<uint8_t> end_key
            ) const;
    };
} // namespace akkaradb::engine

namespace akkaradb::engine {
    template <typename Fn>
    void ARTMemTable::for_each_child(const NodeBase* node, Fn&& fn) {
        if (node == nullptr) { return; }

        switch (node->kind) {
            case NodeBase::Kind::Node4: {
                const auto* n = static_cast<const Node4*>(node);
                for (uint16_t i = 0; i < n->child_count; ++i) { fn(n->keys[i], n->children[i]); }
                return;
            }
            case NodeBase::Kind::Node16: {
                const auto* n = static_cast<const Node16*>(node);
                for (uint16_t i = 0; i < n->child_count; ++i) { fn(n->keys[i], n->children[i]); }
                return;
            }
            case NodeBase::Kind::Node48: {
                const auto* n = static_cast<const Node48*>(node);
                for (uint16_t i = 0; i < n->child_count; ++i) { fn(n->ordered_keys[i], n->children[i]); }
                return;
            }
            case NodeBase::Kind::Node256: {
                const auto* n = static_cast<const Node256*>(node);
                for (uint16_t i = 0; i < n->child_count; ++i) {
                    const uint8_t key = n->ordered_keys[i];
                    fn(key, n->children[key]);
                }
                return;
            }
        }
    }
} // namespace akkaradb::engine

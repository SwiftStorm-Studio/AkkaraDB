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

// akkengine/include/akk/engine/memtable/BPTreeMemTable.hpp
#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <span>
#include <utility>
#include <vector>

#include "akk/core/buffer/BufferArena.hpp"
#include "akk/core/record/OwnedRecord.hpp"
#include "akk/engine/memtable/IMemTable.hpp"

namespace akkaradb::engine {
    class BPTreeMemTable final : public IMemTable {
        public:
            static constexpr uint16_t MAX_KEYS = 63;
            static constexpr uint8_t MAX_VERSIONS_PER_KEY = 4;

            explicit BPTreeMemTable(
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
                std::atomic<uint64_t> version{0};
                std::atomic<uint8_t> head{0};
                std::atomic<uint8_t> count{0};
                std::array<std::atomic<const core::OwnedRecord*>, MAX_VERSIONS_PER_KEY> ring{};

                VersionChain() noexcept = default;
            };

            struct Node {
                std::atomic<uint64_t> version{0};
                bool is_leaf{true};
                std::atomic<uint16_t> key_count{0};

                std::array<std::atomic<const core::OwnedRecord*>, MAX_KEYS> keys{};
                std::array<std::atomic<VersionChain*>, MAX_KEYS> chains{};
                std::array<std::atomic<Node*>, MAX_KEYS + 1> children{};

                std::atomic<Node*> next_leaf{nullptr};

                explicit Node(bool leaf) noexcept : is_leaf{leaf} {}
            };

            struct SplitResult {
                const core::OwnedRecord* separator{nullptr};
                Node* right{nullptr};
            };

            core::BufferArena data_arena_;
            mutable core::BufferArena generator_arena_;
            mutable std::mutex generator_arena_mutex_;

            std::atomic<Node*> root_{nullptr};
            std::atomic<bool> frozen_{false};
            std::atomic<size_t> bytes_{0};
            std::atomic<size_t> entries_{0};

            [[nodiscard]] static std::span<const uint8_t> as_u8(ByteView view) noexcept;

            template <typename T, typename... Args>
            [[nodiscard]] T* arena_new(Args&&... args) {
                std::byte* mem = data_arena_.allocate(sizeof(T), alignof(T));
                return new(mem) T(std::forward<Args>(args)...);
            }

            [[nodiscard]] Node* make_node(bool leaf);
            [[nodiscard]] VersionChain* make_chain(const core::OwnedRecord* initial);
            [[nodiscard]] core::OwnedRecord* make_record(
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint64_t seq,
                uint8_t flags,
                uint64_t precomputed_fp64,
                uint64_t precomputed_mk
            );

            static void begin_write(Node* node) noexcept;
            static void end_write(Node* node) noexcept;

            [[nodiscard]] static int compare_record_key(const core::OwnedRecord* record, std::span<const uint8_t> key) noexcept;
            [[nodiscard]] static int compare_record_record(const core::OwnedRecord* lhs, const core::OwnedRecord* rhs) noexcept;
            [[nodiscard]] static uint16_t find_leaf_position(const Node* leaf, std::span<const uint8_t> key) noexcept;
            [[nodiscard]] static uint16_t find_leaf_position(const Node* leaf, std::span<const uint8_t> key, uint16_t key_count) noexcept;
            [[nodiscard]] static uint16_t find_child_index(const Node* internal, std::span<const uint8_t> key) noexcept;

            static void append_version(VersionChain* chain, const core::OwnedRecord* record, std::atomic<size_t>& entries) noexcept;
            [[nodiscard]] static bool visible_record(VersionChain* chain, uint64_t snapshot_seq, RecordView* out) noexcept;
            [[nodiscard]] static RecordView to_view(const core::OwnedRecord& record) noexcept;

            [[nodiscard]] std::optional<SplitResult> insert_recursive(Node* node, const core::OwnedRecord* record);
            [[nodiscard]] Node* descend_to_candidate_leaf(std::span<const uint8_t> key) const noexcept;
            [[nodiscard]] ArenaGenerator<RecordView> iterate_snapshot(uint64_t snapshot_seq) const;
            [[nodiscard]] ArenaGenerator<RecordView> iterate_snapshot_range(
                uint64_t snapshot_seq,
                std::vector<uint8_t> start_key,
                std::vector<uint8_t> end_key
            ) const;
    };
} // namespace akkaradb::engine

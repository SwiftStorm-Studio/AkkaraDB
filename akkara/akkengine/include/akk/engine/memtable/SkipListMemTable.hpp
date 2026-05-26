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

// akkengine/include/akk/engine/memtable/SkipListMemTable.hpp
#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <span>
#include <vector>

#include "akk/core/buffer/BufferArena.hpp"
#include "akk/core/record/OwnedRecord.hpp"
#include "akk/engine/memtable/IMemTable.hpp"

namespace akkaradb::engine {
    class SkipListMemTable final : public IMemTable {
        public:
            static constexpr uint8_t MAX_LEVEL = 12;
            static constexpr uint8_t MAX_VERSIONS_PER_KEY = 4;

            explicit SkipListMemTable(
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
            struct VersionSlot {
                std::atomic<const core::OwnedRecord*> record{nullptr};
            };

            struct Node {
                std::atomic<uint64_t> version{0};
                std::atomic<uint8_t> head{0};
                std::atomic<uint8_t> count{0};
                uint8_t level{1};
                const core::OwnedRecord* key_record{nullptr};
                std::array<std::atomic<Node*>, MAX_LEVEL> next{};
                std::array<VersionSlot, MAX_VERSIONS_PER_KEY> ring{};
            };

            core::BufferArena data_arena_;
            mutable core::BufferArena generator_arena_;
            Node* head_{nullptr};
            std::atomic<bool> frozen_{false};
            std::atomic<size_t> bytes_{0};
            std::atomic<size_t> entries_{0};
            std::atomic<uint8_t> current_max_level_{1};
            uint64_t rng_state_{0x9e3779b97f4a7c15ULL};
            mutable std::mutex generator_arena_mutex_;

            [[nodiscard]] static std::span<const uint8_t> as_u8(ByteView view) noexcept;
            [[nodiscard]] uint64_t next_random() noexcept;
            [[nodiscard]] uint8_t random_level() noexcept;

            [[nodiscard]] Node* new_node(const core::OwnedRecord* initial_record, uint8_t level);
            [[nodiscard]] core::OwnedRecord* make_record(
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint64_t seq,
                uint8_t flags,
                uint64_t precomputed_fp64,
                uint64_t precomputed_mk
            );

            [[nodiscard]] static int compare_node_key(const Node* node, std::span<const uint8_t> key) noexcept;
            [[nodiscard]] Node* find_node(std::span<const uint8_t> key, std::array<Node*, MAX_LEVEL>* update, bool writer_fast_path) const noexcept;

            [[nodiscard]] bool visible_record(const Node* node, uint64_t snapshot_seq, RecordView* out) const noexcept;
            [[nodiscard]] static RecordView to_view(const core::OwnedRecord& record) noexcept;

            [[nodiscard]] ArenaGenerator<RecordView> iterate_snapshot(uint64_t snapshot_seq) const;
            [[nodiscard]] ArenaGenerator<RecordView> iterate_snapshot_range(
                uint64_t snapshot_seq,
                std::vector<uint8_t> start_key,
                std::vector<uint8_t> end_key
            ) const;
    };
} // namespace akkaradb::engine

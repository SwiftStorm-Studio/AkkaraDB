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

#include "akkaradb/Export.hpp"

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

namespace akkaradb::engine::memtable {
    class AKDB_API BPTreeMemTable final : public IMemTable {
        public:
            static constexpr uint16_t MAX_KEYS = 63;
            static constexpr uint8_t MAX_VERSIONS_PER_KEY = 4;

            explicit BPTreeMemTable(
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
                std::atomic<uint64_t> version{0};
                std::atomic<uint8_t> head{0};
                std::atomic<uint8_t> count{0};
                std::array<std::atomic<const core::OwnedRecord*>, MAX_VERSIONS_PER_KEY> ring{};

                VersionChain() noexcept = default;
            };

            struct Node {
                std::atomic<uint64_t> version{0};
                bool isLeaf{true};
                std::atomic<uint16_t> keyCount{0};

                std::array<std::atomic<const core::OwnedRecord*>, MAX_KEYS> keys{};
                std::array<std::atomic<VersionChain*>, MAX_KEYS> chains{};
                std::array<std::atomic<Node*>, MAX_KEYS + 1> children{};

                std::atomic<Node*> nextLeaf{nullptr};

                explicit Node(bool leaf) noexcept : isLeaf{leaf} {}
            };

            struct SplitResult {
                const core::OwnedRecord* separator{nullptr};
                Node* right{nullptr};
            };

            core::BufferArena dataArena_;
            mutable core::BufferArena generatorArena_;
            mutable std::mutex generatorArenaMutex_;

            std::atomic<Node*> root_{nullptr};
            std::atomic<bool> frozen_{false};
            std::atomic<size_t> bytes_{0};
            std::atomic<size_t> entries_{0};

            [[nodiscard]] static std::span<const uint8_t> asU8(ByteView view) noexcept;

            template <typename T, typename... Args>
            [[nodiscard]] T* arenaNew(Args&&... args) {
                std::byte* mem = dataArena_.allocate(sizeof(T), alignof(T));
                return new(mem) T(std::forward<Args>(args)...);
            }

            [[nodiscard]] Node* makeNode(bool leaf);
            [[nodiscard]] VersionChain* makeChain(const core::OwnedRecord* initial);
            [[nodiscard]] core::OwnedRecord* makeRecord(
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint64_t seq,
                uint8_t flags,
                uint64_t precomputedFp64,
                uint64_t precomputedMk
            );

            static void beginWrite(Node* node) noexcept;
            static void endWrite(Node* node) noexcept;

            [[nodiscard]] static int compareRecordKey(const core::OwnedRecord* record, std::span<const uint8_t> key) noexcept;
            [[nodiscard]] static int compareRecordRecord(const core::OwnedRecord* lhs, const core::OwnedRecord* rhs) noexcept;
            [[nodiscard]] static uint16_t findLeafPosition(const Node* leaf, std::span<const uint8_t> key) noexcept;
            [[nodiscard]] static uint16_t findLeafPosition(const Node* leaf, std::span<const uint8_t> key, uint16_t keyCount) noexcept;
            [[nodiscard]] static uint16_t findChildIndex(const Node* internal, std::span<const uint8_t> key) noexcept;

            static void appendVersion(VersionChain* chain, const core::OwnedRecord* record, std::atomic<size_t>& entries) noexcept;
            [[nodiscard]] static bool visibleRecord(VersionChain* chain, uint64_t snapshotSeq, RecordView* out) noexcept;
            [[nodiscard]] static RecordView toView(const core::OwnedRecord& record) noexcept;

            [[nodiscard]] std::optional<SplitResult> insertRecursive(Node* node, const core::OwnedRecord* record);
            [[nodiscard]] Node* descendToCandidateLeaf(std::span<const uint8_t> key) const noexcept;
            [[nodiscard]] ArenaGenerator<RecordView> iterateSnapshot(uint64_t snapshotSeq) const;
            [[nodiscard]] ArenaGenerator<RecordView> iterateSnapshotRange(
                uint64_t snapshotSeq,
                std::vector<uint8_t> startKey,
                std::vector<uint8_t> endKey
            ) const;
    };
} // namespace akkaradb::engine::memtable

/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/memtable/SkipListMemTable.hpp
#pragma once

#include "akkaradb/Export.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <new>
#include <span>
#include <vector>

#include "akk/core/buffer/BufferArena.hpp"
#include "akk/core/record/OwnedRecord.hpp"
#include "akk/engine/memtable/IMemTable.hpp"

namespace akkaradb::engine::memtable {
    class AKDB_API SkipListMemTable final : public IMemTable {
        public:
            static constexpr uint8_t MAX_LEVEL = 12;
            static constexpr size_t DEFAULT_MAX_VERSIONS_PER_KEY = 4;
            static constexpr size_t MAX_CONFIGURED_VERSIONS_PER_KEY = 65535;

            // The enclosing MemTable serializes put() and freeze() per shard.
            // get() and iterator() are safe concurrently with that single writer.

            explicit SkipListMemTable(
                size_t dataArenaInitialBlockSize = core::BufferArena::DEFAULT_INITIAL_BLOCK_SIZE,
                size_t dataArenaMaxBlockSize = core::BufferArena::DEFAULT_MAX_BLOCK_SIZE,
                size_t generatorArenaInitialBlockSize = 64 * 1024,
                size_t generatorArenaMaxBlockSize = 2 * 1024 * 1024,
                MemTableBackendOptions backendOptions = {}
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
                uint16_t capacity{0};
                std::atomic<uint16_t> head{0};
                std::atomic<uint16_t> count{0};

                [[nodiscard]] std::atomic<const core::OwnedRecord*>* ring() noexcept {
                    return std::launder(reinterpret_cast<std::atomic<const core::OwnedRecord*>*>(this + 1));
                }

                [[nodiscard]] const std::atomic<const core::OwnedRecord*>* ring() const noexcept {
                    return std::launder(reinterpret_cast<const std::atomic<const core::OwnedRecord*>*>(this + 1));
                }
            };

            struct Node {
                uint8_t level{1};
                const core::OwnedRecord* keyRecord{nullptr};
                VersionChain* versions{nullptr};

                Node(uint8_t nodeLevel, const core::OwnedRecord* key, VersionChain* chain) noexcept
                    : level{nodeLevel}, keyRecord{key}, versions{chain} {}

                [[nodiscard]] std::atomic<Node*>* nextSlots() noexcept {
                    return std::launder(reinterpret_cast<std::atomic<Node*>*>(this + 1));
                }

                [[nodiscard]] const std::atomic<Node*>* nextSlots() const noexcept {
                    return std::launder(reinterpret_cast<const std::atomic<Node*>*>(this + 1));
                }

                [[nodiscard]] std::atomic<Node*>& nextAt(uint8_t index) noexcept { return nextSlots()[index]; }
                [[nodiscard]] const std::atomic<Node*>& nextAt(uint8_t index) const noexcept { return nextSlots()[index]; }
            };

            static_assert(alignof(Node) >= alignof(std::atomic<Node*>));

            core::BufferArena dataArena_;
            Node* head_{nullptr};
            std::atomic<bool> frozen_{false};
            std::atomic<size_t> bytes_{0};
            std::atomic<size_t> entries_{0};
            std::atomic<uint8_t> currentMaxLevel_{1};
            uint64_t rngState_{0x9e3779b97f4a7c15ULL};
            size_t generatorArenaInitialBlockSize_{64 * 1024};
            size_t generatorArenaMaxBlockSize_{2 * 1024 * 1024};
            uint16_t maxVersionsPerKey_{DEFAULT_MAX_VERSIONS_PER_KEY};

            [[nodiscard]] static std::span<const uint8_t> asU8(ByteView view) noexcept;
            [[nodiscard]] uint64_t nextRandom() noexcept;
            [[nodiscard]] uint8_t randomLevel() noexcept;

            [[nodiscard]] Node* newNode(const core::OwnedRecord* initialRecord, uint8_t level);
            [[nodiscard]] VersionChain* makeChain(const core::OwnedRecord* initialRecord);
            [[nodiscard]] core::OwnedRecord* makeRecord(
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint64_t seq,
                uint8_t flags,
                uint64_t precomputedFp64,
                uint64_t precomputedMk
            );

            [[nodiscard]] static int compareNodeKey(const Node* node, std::span<const uint8_t> key) noexcept;
            [[nodiscard]] Node* findNodeForWrite(std::span<const uint8_t> key, std::array<Node*, MAX_LEVEL>& update) noexcept;
            [[nodiscard]] Node* findNodeForRead(std::span<const uint8_t> key) const noexcept;

            static void appendVersion(VersionChain* chain, const core::OwnedRecord* record, std::atomic<size_t>& entries) noexcept;
            [[nodiscard]] static bool visibleRecord(const VersionChain* chain, uint64_t snapshotSeq, RecordView* out) noexcept;
            [[nodiscard]] static bool visibleFrozenRecord(const VersionChain* chain, uint64_t snapshotSeq, RecordView* out) noexcept;
            [[nodiscard]] static RecordView toView(const core::OwnedRecord& record) noexcept;

            [[nodiscard]] ArenaGenerator<RecordView> iterateSnapshot(uint64_t snapshotSeq, bool frozen) const;
            [[nodiscard]] ArenaGenerator<RecordView> iterateSnapshotRange(
                uint64_t snapshotSeq,
                std::vector<uint8_t> startKey,
                std::vector<uint8_t> endKey,
                bool frozen
            ) const;
    };
} // namespace akkaradb::engine::memtable

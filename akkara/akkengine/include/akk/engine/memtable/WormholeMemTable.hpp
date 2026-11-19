/*
 * AkkaraDB
 * Copyright (C) 2026 Swift Storm Studio
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. See https://mozilla.org/MPL/2.0/.
 */
#pragma once

#include <memory>

#include "akk/core/buffer/BufferArena.hpp"
#include "akk/engine/memtable/IMemTable.hpp"

namespace akkaradb::engine::memtable {
    // Experimental Wormhole-style index: tagged anchor prefixes and leaf tag
    // indexes with lazy key sorting. Mutable iterators capture before yielding.
    class AKDB_API WormholeMemTable final : public IMemTable {
        public:
            static constexpr size_t LEAF_CAPACITY = 128;

            explicit WormholeMemTable(
                size_t dataArenaInitialBlockSize = core::BufferArena::DEFAULT_INITIAL_BLOCK_SIZE,
                size_t dataArenaMaxBlockSize = core::BufferArena::DEFAULT_MAX_BLOCK_SIZE,
                size_t generatorArenaInitialBlockSize = 4 * 1024,
                size_t generatorArenaMaxBlockSize = 2 * 1024 * 1024,
                MemTableBackendOptions backendOptions = {}
            );
            ~WormholeMemTable() override;

            [[nodiscard]] Status put(ByteView key, ByteView value, uint64_t seq, uint8_t flags,
                uint64_t precomputedFp64 = 0, uint64_t precomputedMk = 0) override;
            [[nodiscard]] bool get(ByteView key, uint64_t snapshotSeq, RecordView* out) const override;
            [[nodiscard]] ArenaGenerator<RecordView> iterator(ByteView startKey, ByteView endKey, uint64_t snapshotSeq) const override;
            void retainAllVersions() override;
            void useSequenceOrder() override;
            [[nodiscard]] ArenaGenerator<RecordView> flushIterator() const override;
            void freeze() override;
            [[nodiscard]] size_t sizeBytes() const override;
            [[nodiscard]] size_t entryCount() const override;

        private:
            class Impl;
            std::unique_ptr<Impl> impl_;
    };
}

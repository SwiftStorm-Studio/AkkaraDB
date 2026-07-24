/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/memtable/MemTable.hpp
#pragma once

#include "akkaradb/Export.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "akk/core/record/MemHdr16.hpp"
#include "akk/core/record/RecordView.hpp"
#include "akk/engine/memtable/IMemTable.hpp"

namespace akkaradb::engine::memtable {
    enum class MemTableFlushMode : uint8_t {
        AUTO = 0, BYTES_PER_SHARD = 1, MANUAL_ONLY = 2,
    };

    class AKDB_API MemTable {
        public:
            using RecordView = core::RecordView;
            using FlushCallback = std::function<void(std::span<const RecordView>)>;
            using MemTableFactory = std::function<std::unique_ptr<IMemTable>()>;
            using ConfiguredMemTableFactory = std::function<std::unique_ptr<IMemTable>(const MemTableBackendOptions &)>;

            struct Options {
                // Physical write/read partitioning. 0 selects a writer-count based shard count.
                size_t shardCount = 0;
                size_t expectedConcurrentWriters = 0;
                size_t autoShardCountCap = 128;
                // Controls active MemTable rotation into immutable tables.
                // AUTO maps to BYTES_PER_SHARD when thresholdBytesPerShard > 0, otherwise MANUAL_ONLY.
                MemTableFlushMode flushMode = MemTableFlushMode::AUTO;
                size_t thresholdBytesPerShard = 64ULL * 1024 * 1024;
                // Backend construction policy. BPTree uses mutableScanMode;
                // SkipList and ART accept it as a no-op.
                MemTableBackendOptions backendOptions{};
                // Physical in-memory index implementation and optional immutable flush sink.
                // backendFactoryWithOptions takes precedence when supplied.
                MemTableFactory backendFactory = nullptr;
                ConfiguredMemTableFactory backendFactoryWithOptions = nullptr;
                FlushCallback onFlush = nullptr;
            };

            struct KeyRange {
                std::vector<uint8_t> start;
                std::vector<uint8_t> end;
            };

            struct MemTableSnapshot {
                uint32_t shardCount = 0;
                uint64_t thresholdBytesPerShard = 0;
                uint64_t approxBytes = 0;
                uint64_t putsApplied = 0;
                uint64_t removesApplied = 0;
                uint64_t flushesCompleted = 0;
                uint64_t immutableTables = 0;
            };

            class AKDB_API RangeIterator {
                public:
                    ~RangeIterator();
                    RangeIterator(RangeIterator&&) noexcept;
                    RangeIterator& operator=(RangeIterator&&) noexcept;

                    RangeIterator(const RangeIterator&) = delete;
                    RangeIterator& operator=(const RangeIterator&) = delete;

                    [[nodiscard]] bool hasNext() const noexcept;
                    [[nodiscard]] std::optional<RecordView> next() noexcept;
                    [[nodiscard]] uint64_t snapshotSeq() const noexcept;

                private:
                    friend class MemTable;
                    class Impl;
                    explicit RangeIterator(std::unique_ptr<Impl> impl);
                    std::unique_ptr<Impl> impl_;
            };

            [[nodiscard]] static std::unique_ptr<MemTable> create();
            [[nodiscard]] static std::unique_ptr<MemTable> create(const Options& options);

            ~MemTable();

            MemTable(const MemTable&) = delete;
            MemTable& operator=(const MemTable&) = delete;
            MemTable(MemTable&&) = delete;
            MemTable& operator=(MemTable&&) = delete;

            void put(
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint64_t seq,
                uint8_t flags = core::MemHdr16::FLAG_NORMAL,
                uint64_t precomputedFp64 = 0,
                uint64_t precomputedMk = 0
            );

            void remove(std::span<const uint8_t> key, uint64_t seq, uint64_t precomputedFp64 = 0, uint64_t precomputedMk = 0);

            void advanceSeq(uint64_t seq) noexcept;

            [[nodiscard]] bool get(std::span<const uint8_t> key, uint64_t snapshotSeq, RecordView* out) const;
            [[nodiscard]] bool get(std::span<const uint8_t> key, uint64_t snapshotSeq, RecordView* out, uint64_t precomputedFp64) const;

            [[nodiscard]] std::optional<bool> getInto(std::span<const uint8_t> key, uint64_t snapshotSeq, std::vector<uint8_t>& out) const;

            [[nodiscard]] std::optional<bool> contains(std::span<const uint8_t> key, uint64_t snapshotSeq) const;

            [[nodiscard]] RangeIterator iterator(const KeyRange& range, uint64_t snapshotSeq) const;
            // Acquires all shard read locks before invoking snapshotSeqProvider.
            // The returned iterator retains those locks until it is destroyed.
            [[nodiscard]] RangeIterator pinnedIterator(const KeyRange& range, const std::function<uint64_t()>& snapshotSeqProvider) const;

            [[nodiscard]] uint64_t nextSeq() noexcept;
            [[nodiscard]] uint64_t reserveSeq(uint64_t count);
            [[nodiscard]] uint64_t lastSeq() const noexcept;

            void flushHint();
            void forceFlush();
            // Rethrows the first asynchronous immutable-flush failure, if any.
            void throwIfFlushFailed() const;
            void setFlushCallback(const FlushCallback& cb);

            [[nodiscard]] size_t approxSize() const noexcept;
            [[nodiscard]] MemTableSnapshot snapshot() const noexcept;

        private:
            class Impl;
            explicit MemTable(const Options& options);
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::memtable

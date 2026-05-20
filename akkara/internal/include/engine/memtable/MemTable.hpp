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

// internal/include/engine/memtable/MemTable.hpp
#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "core/record/MemHdr16.hpp"
#include "core/record/RecordView.hpp"
#include "engine/memtable/IMemTable.hpp"

namespace akkaradb::engine::memtable {
    class MemTable {
        public:
            using RecordView = core::RecordView;
            using FlushCallback = std::function<void(std::span<const RecordView>)>;
            using MemTableFactory = std::function<std::unique_ptr<IMemTable>()>;

            struct Options {
                size_t shard_count = 0;
                size_t expected_concurrent_writers = 0;
                size_t auto_shard_count_cap = 128;
                size_t threshold_bytes_per_shard = 64ULL * 1024 * 1024;
                MemTableFactory backend_factory = nullptr;
                FlushCallback on_flush = nullptr;
            };

            struct KeyRange {
                std::vector<uint8_t> start;
                std::vector<uint8_t> end;
            };

            struct MemTableSnapshot {
                uint32_t shard_count = 0;
                uint64_t threshold_bytes_per_shard = 0;
                uint64_t approx_bytes = 0;
                uint64_t puts_applied = 0;
                uint64_t removes_applied = 0;
                uint64_t flushes_completed = 0;
            };

            class RangeIterator {
                public:
                    ~RangeIterator();
                    RangeIterator(RangeIterator&&) noexcept;
                    RangeIterator& operator=(RangeIterator&&) noexcept;

                    RangeIterator(const RangeIterator&) = delete;
                    RangeIterator& operator=(const RangeIterator&) = delete;

                    [[nodiscard]] bool has_next() const noexcept;
                    [[nodiscard]] std::optional<RecordView> next() noexcept;

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
                uint64_t precomputed_fp64 = 0,
                uint64_t precomputed_mk = 0
            );

            void remove(std::span<const uint8_t> key, uint64_t seq, uint64_t precomputed_fp64 = 0, uint64_t precomputed_mk = 0);

            void advance_seq(uint64_t seq) noexcept;

            [[nodiscard]] bool get(std::span<const uint8_t> key, uint64_t snapshot_seq, RecordView* out) const;

            [[nodiscard]] std::optional<bool> get_into(std::span<const uint8_t> key, uint64_t snapshot_seq, std::vector<uint8_t>& out) const;

            [[nodiscard]] std::optional<bool> contains(std::span<const uint8_t> key, uint64_t snapshot_seq) const;

            [[nodiscard]] RangeIterator iterator(const KeyRange& range, uint64_t snapshot_seq) const;

            [[nodiscard]] uint64_t next_seq() noexcept;
            [[nodiscard]] uint64_t reserve_seq(uint64_t count);
            [[nodiscard]] uint64_t last_seq() const noexcept;

            void flush_hint();
            void force_flush();
            void set_flush_callback(const FlushCallback& cb);

            [[nodiscard]] size_t approx_size() const noexcept;
            [[nodiscard]] MemTableSnapshot snapshot() const noexcept;

        private:
            class Impl;
            explicit MemTable(const Options& options);
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::memtable

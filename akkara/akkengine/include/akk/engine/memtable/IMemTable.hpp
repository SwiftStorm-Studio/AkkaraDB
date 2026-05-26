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

// akkengine/include/akk/engine/memtable/IMemTable.hpp
#pragma once

#include <cstddef>
#include <cstdint>

#include "akk/core/Status.hpp"
#include "akk/core/record/RecordView.hpp"
#include "akk/core/types/ByteView.hpp"
#include "akk/core/utils/ArenaGenerator.hpp"

using namespace akkaradb::core;

namespace akkaradb::engine {
    /**
     * @brief Abstract interface for pluggable MemTable implementations.
     *
     * IMemTable represents the in-memory mutable ordered index layer
     * positioned between WAL and SST.
     *
     * Supported responsibilities:
     * - append-style writes with sequence numbers
     * - snapshot-aware point lookup
     * - ordered iteration for flush and scans
     * - immutable freeze before SST flush
     *
     * Intended backend implementations include:
     * - Skip List
     * - BPTree
     * - ART
     * - Red-Black Tree
     *
     * Design constraints:
     * - ordered traversal is mandatory
     * - multi-version records are backend-managed
     * - tombstones are represented via record flags
     */
    class IMemTable {
        public:
            virtual ~IMemTable() = default;

            /**
             * @brief Insert or append a new record version.
             *
             * Implementations are expected to support append-style
             * version retention internally.
             *
             * Both key and value are treated as immutable binary blobs.
             * No ownership is transferred.
             *
             * @param key User key bytes.
             * @param value Value payload bytes.
             * @param seq Monotonic sequence number.
             * @param flags Record metadata flags (e.g. tombstone).
             * @param precomputed_fp64 Optional precomputed 64-bit fingerprint for
             * fast backend insertion paths. Use 0 when unavailable.
             * @param precomputed_mk Optional precomputed mixed key material derived
             * from key for backend-specific indexing. Use 0 when unavailable.
             *
             * @return Operation result.
             */
            [[nodiscard]] virtual Status put(
                ByteView key,
                ByteView value,
                uint64_t seq,
                uint8_t flags,
                uint64_t precomputed_fp64 = 0,
                uint64_t precomputed_mk = 0
            ) = 0;

            /**
             * @brief Retrieve the visible version for a snapshot.
             *
             * Returns the newest version whose sequence number is
             * less than or equal to snapshot_seq.
             *
             * @param key User key.
             * @param snapshot_seq Snapshot sequence boundary.
             * @param out Output record view.
             *
             * @return true if visible record exists.
             * @return false otherwise.
             */
            [[nodiscard]] virtual bool get(ByteView key, uint64_t snapshot_seq, RecordView* out) const = 0;

            /**
             * @brief Create ordered iterator for range + snapshot view.
             *
             * Iteration order must be lexicographically ordered by key.
             * Range semantics are:
             * - start_key: inclusive lower bound
             * - end_key: exclusive upper bound
             * - empty bound: unbounded on that side
             *
             * @param start_key Inclusive range start key.
             * @param end_key Exclusive range end key.
             * @param snapshot_seq Snapshot sequence boundary.
             * @return Generator of visible records within range.
             */
            [[nodiscard]] virtual ArenaGenerator<RecordView> iterator(ByteView start_key, ByteView end_key, uint64_t snapshot_seq) const = 0;

            /**
             * @brief Freeze the MemTable into immutable state.
             *
             * After freeze():
             * - writes must be rejected
             * - reads and iteration remain valid
             *
             * Intended for SST flush handoff.
             */
            virtual void freeze() = 0;

            /**
             * @brief Current approximate memory usage in bytes.
             *
             * Used for flush threshold decisions.
             *
             * @return Memory usage.
             */
            [[nodiscard]] virtual size_t sizeBytes() const = 0;

            /**
             * @brief Total logical record count.
             *
             * Includes all retained versions managed by backend.
             *
             * @return Number of stored records.
             */
            [[nodiscard]] virtual size_t entryCount() const = 0;
    };
} // namespace akkaradb::engine

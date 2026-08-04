/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/memtable/IMemTable.hpp
#pragma once

#include "akkaradb/Export.hpp"

#include <cstddef>
#include <cstdint>

#include "akk/core/Status.hpp"
#include "akk/core/record/RecordView.hpp"
#include "akk/core/types/ByteView.hpp"
#include "akk/core/utils/ArenaGenerator.hpp"

using namespace akkaradb::core;

namespace akkaradb::engine::memtable {
    enum class MutableScanMode : uint8_t {
        // Reconcile a mutable backend's scan before exposing records.
        RECONCILE = 0,
        // Stream ordered records while restarting structural traversal as needed.
        STREAMING_RESTART = 1,
    };

    enum class BPTreeConcurrencyMode : uint8_t {
        // Serialize BPTree structural writes against mutable readers.
        LOCKED = 0,
        // Preserve the old optimistic reader path. This is for experiments only;
        // internal splits can expose transient unreachable paths to readers.
        OPTIMISTIC_UNSAFE = 1,
    };

    enum class BPTreeIteratorMode : uint8_t {
        // Materialize the full visible snapshot under one read lock, then release
        // the lock before yielding to the caller.
        MATERIALIZE_ALL = 0,
        // Materialize bounded batches under separate read locks. This reduces
        // writer stalls during long scans, but it does not pin older same-key
        // versions beyond the backend's configured retention window.
        MATERIALIZE_BATCHED_UNPINNED = 1,
    };

    struct MemTableBackendOptions {
        // BPTree uses this to select mutable scan behavior. SkipList and ART
        // already provide stable ordered traversal, so it is intentionally a no-op there.
        MutableScanMode mutableScanMode = MutableScanMode::RECONCILE;
        // BPTree structural concurrency policy. Keep LOCKED for correctness
        // unless a caller is explicitly running unsafe experiments.
        BPTreeConcurrencyMode bptreeConcurrencyMode = BPTreeConcurrencyMode::LOCKED;
        // BPTree iterator policy used with LOCKED concurrency mode.
        BPTreeIteratorMode bptreeIteratorMode = BPTreeIteratorMode::MATERIALIZE_ALL;
        // Maximum visible records collected per read-lock section in
        // MATERIALIZE_BATCHED_UNPINNED mode.
        size_t bptreeIteratorBatchSize = 1024;
        // Number of recent same-key versions retained inside a mutable MemTable
        // backend. SkipList and BPTree honor this option; ART currently keeps
        // its backend-local default.
        size_t maxVersionsPerKey = 4;
    };

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
    class AKDB_API IMemTable {
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
             * @param precomputedFp64 Optional precomputed 64-bit fingerprint for
             * fast backend insertion paths. Use 0 when unavailable.
             * @param precomputedMk Optional precomputed mixed key material derived
             * from key for backend-specific indexing. Use 0 when unavailable.
             *
             * @return Operation result.
             */
            [[nodiscard]] virtual Status put(
                ByteView key,
                ByteView value,
                uint64_t seq,
                uint8_t flags,
                uint64_t precomputedFp64 = 0,
                uint64_t precomputedMk = 0
            ) = 0;

            /**
             * @brief Retrieve the visible version for a snapshot.
             *
             * Returns the newest version whose sequence number is
             * less than or equal to snapshotSeq.
             *
             * @param key User key.
             * @param snapshotSeq Snapshot sequence boundary.
             * @param out Output record view.
             *
             * @return true if visible record exists.
             * @return false otherwise.
             */
            [[nodiscard]] virtual bool get(ByteView key, uint64_t snapshotSeq, RecordView* out) const = 0;

            /**
             * @brief Create ordered iterator for range + snapshot view.
             *
             * Iteration order must be lexicographically ordered by key.
             * Range semantics are:
             * - startKey: inclusive lower bound
             * - endKey: exclusive upper bound
             * - empty bound: unbounded on that side
             *
             * @param startKey Inclusive range start key.
             * @param endKey Exclusive range end key.
             * @param snapshotSeq Snapshot sequence boundary.
             * @return Generator of visible records within range.
             */
            [[nodiscard]] virtual ArenaGenerator<RecordView> iterator(ByteView startKey, ByteView endKey, uint64_t snapshotSeq) const = 0;

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
} // namespace akkaradb::engine::memtable

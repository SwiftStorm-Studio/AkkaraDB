/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/manifest/Manifest.hpp
#pragma once

#include "akkaradb/Export.hpp"

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>
#include <optional>

namespace akkaradb::engine::manifest {
    /**
     * Manifest - Durable, append-only log of storage-engine state changes.
     *
     * Tracks SST file lifecycle (seal, delete, compaction) and checkpoint
     * markers to enable crash recovery and state reconstruction.
     *
     * Binary on-disk format (.akmf):
     *   [ManifestFileHeader:32B] [ManifestRecordHeader:8B][payload]...
     *
     * Thread-safety: All public methods are thread-safe.
     *
     * Modes:
     *   - Sync mode (fastMode=false): Every write is followed by fdatasync.
     *     Suitable for correctness-critical paths.
     *   - Fast mode (fastMode=true): Writes are batched by a background
     *     flusher thread and fsynced periodically.  Lower latency but
     *     relaxed durability guarantee.
     */
    class AKDB_API Manifest {
        public:
            // ================================================================
            // Event types (used for replay / state queries)
            // ================================================================

            struct SSTSealEvent {
                int level;
                std::string file;
                uint64_t entries;
                std::optional<std::string> firstKeyHex;
                std::optional<std::string> lastKeyHex;
                uint64_t tsUs; ///< Timestamp at seal time (microseconds since epoch)
            };

            struct CheckpointEvent {
                std::optional<std::string> name;
                std::optional<uint64_t> stripe;
                std::optional<uint64_t> lastSeq;
                uint64_t tsUs;
            };

            struct NodeJoinEvent {
                uint64_t nodeId;
                uint16_t replPort;
                std::string host;
                uint64_t tsUs;
            };

            struct NodeLeaveEvent {
                uint64_t nodeId;
                uint64_t tsUs;
            };

            struct PrimaryLeaseEvent {
                uint64_t nodeId;
                uint64_t leaseUntilUs;
                uint64_t tsUs;
            };

            struct BlobPutEvent {
                uint64_t blobId;
                uint64_t totalSize;
                uint64_t storedSize;
                uint32_t contentCrc32c;
                uint32_t codec;
                uint64_t tsUs;
            };

            struct BlobDeleteEvent {
                uint64_t blobId;
                uint64_t tsUs;
            };

            struct SSTBlobRefsEvent {
                struct Entry {
                    std::vector<uint8_t> key;
                    uint64_t seq;
                    uint8_t flags;
                    std::optional<uint64_t> blobId;
                };

                std::string file;
                std::vector<Entry> entries;
                uint64_t tsUs;
            };

            // ================================================================
            // Factory / lifecycle
            // ================================================================

            /**
             * Opens (or creates) a manifest at the given path.
             *
             * @param path      Base path for the manifest file (e.g. "db/MANIFEST.akmf").
             *                  Rotated files are stored as "<stem>-1<ext>", "<stem>-2<ext>", etc.
             * @param fastMode Enable background-flusher (batched fsync) mode.
             * @throws std::runtime_error on I/O failure.
             */
            [[nodiscard]] static std::unique_ptr<Manifest> create(const std::filesystem::path& path, bool fastMode = false);

            ~Manifest();

            Manifest(const Manifest&) = delete;
            Manifest& operator=(const Manifest&) = delete;

            /**
             * Starts the background flusher thread (fastMode only).
             * No-op in sync mode.  Must be called before first write.
             */
            void start();

            // ================================================================
            // Write API
            // ================================================================

            /**
             * Records a stripe-counter advance.
             * @throws std::invalid_argument if newCount < current count.
             */
            void advance(uint64_t newCount);

            /**
             * Records that a new SST file has been sealed.
             */
            void sstSeal(
                int level,
                const std::string& file,
                uint64_t entries,
                const std::optional<std::string>& firstKeyHex,
                const std::optional<std::string>& lastKeyHex
            );

            /**
             * Records a checkpoint marker.
             */
            void checkpoint(
                const std::optional<std::string>& name,
                const std::optional<uint64_t>& stripe,
                const std::optional<uint64_t>& lastSeq
            );

            /**
             * Records that a compaction has started on a set of input SSTs.
             */
            void compactionStart(int level, const std::vector<std::string>& inputs);

            /**
             * Records that a compaction has completed, producing one output SST.
             */
            void compactionEnd(
                int level,
                const std::string& output,
                const std::vector<std::string>& inputs,
                uint64_t entries,
                const std::optional<std::string>& firstKeyHex,
                const std::optional<std::string>& lastKeyHex
            );

            /**
             * Records that an SST file has been deleted.
             * @deprecated Use compactionCommit() for compaction-driven deletions.
             */
            void sstDelete(const std::string& file);

            /**
             * Atomically records all outputs produced and all inputs consumed by a
             * compaction in a single CRC-protected manifest record.
             *
             * This replaces the non-atomic (CompactionEnd + SSTDelete x N) pattern.
             * During replay, either the full record is applied (all outputs added,
             * all inputs removed) or it is absent (CRC mismatch from partial write),
             * in which case the pre-compaction state is preserved exactly.
             *
             * @param outputFiles  Filenames of new SST files written by the compaction.
             * @param inputFiles   Filenames of SST files consumed by the compaction.
             */
            void compactionCommit(const std::vector<std::string>& outputFiles, const std::vector<std::string>& inputFiles);

            /** Records the complete key/version/blob-reference set contained in one SST file. */
            void sstBlobRefs(const std::string& file, const std::vector<SSTBlobRefsEvent::Entry>& entries);

            /**
             * Records a truncation marker (informational).
             */
            void truncate(const std::optional<std::string>& reason = std::nullopt);

            /** Records that a cluster node has joined and is advertising a replication endpoint. */
            void nodeJoin(uint64_t nodeId, uint16_t replPort, const std::string& host);

            /** Records that a cluster node has left. */
            void nodeLeave(uint64_t nodeId);

            /** Records the last advertised primary lease window. */
            void primaryLease(uint64_t nodeId, uint64_t leaseUntilUs);

            /** Records that a blob file was durably written. */
            void blobPut(uint64_t blobId, uint64_t totalSize, uint64_t storedSize, uint32_t contentCrc32c, uint32_t codec);

            /** Records that a blob file was deleted by GC. */
            void blobDelete(uint64_t blobId);

            // ================================================================
            // Replay
            // ================================================================

            /**
             * Re-reads all manifest files and rebuilds in-memory state.
             * Normally called internally during construction; exposed for
             * explicit replay after external changes.
             */
            void replay();

            /**
             * Rewrites the current replay state into a fresh base manifest file
             * and removes rotated manifest history.
             *
             * The compacted file is encoded as ordinary manifest records, so no
             * separate snapshot record format is required. Public write methods
             * are serialized with compaction.
             */
            void compact();

            // ================================================================
            // State queries
            // ================================================================

            [[nodiscard]] uint64_t stripesWritten() const noexcept;

            [[nodiscard]] std::optional<CheckpointEvent> lastCheckpoint() const noexcept;

            /** Returns all currently live SST file names. */
            [[nodiscard]] std::vector<std::string> liveSst() const;

            /** Returns all SST file names that have been deleted. */
            [[nodiscard]] std::vector<std::string> deletedSst() const;

            /** Returns all SSTSeal events in replay order. */
            [[nodiscard]] std::vector<SSTSealEvent> sstSeals() const;

            /** Returns all node-join events in replay order. */
            [[nodiscard]] std::vector<NodeJoinEvent> nodeJoins() const;

            /** Returns all node-leave events in replay order. */
            [[nodiscard]] std::vector<NodeLeaveEvent> nodeLeaves() const;

            /** Returns the most recently replayed primary-lease event, if any. */
            [[nodiscard]] std::optional<PrimaryLeaseEvent> lastPrimaryLease() const noexcept;

            /** Returns all currently live blob ids known to the manifest. */
            [[nodiscard]] std::vector<uint64_t> liveBlobs() const;

            /** Returns all blob ids deleted by GC known to the manifest. */
            [[nodiscard]] std::vector<uint64_t> deletedBlobs() const;

            /** Returns all blob-put events in replay order. */
            [[nodiscard]] std::vector<BlobPutEvent> blobPuts() const;

            /** Returns all blob-delete events in replay order. */
            [[nodiscard]] std::vector<BlobDeleteEvent> blobDeletes() const;

            /** Returns all SST blob-reference events in replay order. */
            [[nodiscard]] std::vector<SSTBlobRefsEvent> sstBlobRefs() const;

            /** Returns unique blob ids referenced by latest versions in currently live SST files. */
            [[nodiscard]] std::vector<uint64_t> sstReferencedBlobs() const;

            /** True when every currently live SST has an explicit blob-reference record. */
            [[nodiscard]] bool sstBlobRefsComplete() const;

            // ================================================================
            // Shutdown
            // ================================================================

            /**
             * Flushes any pending writes and closes the manifest file.
             * Idempotent.
             */
            void close();

        private:
            explicit Manifest(const std::filesystem::path& path, bool fastMode);

            class Impl;
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::manifest

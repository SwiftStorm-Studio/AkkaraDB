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

// akkengine/include/akk/engine/manifest/Manifest.hpp
#pragma once

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
     *   - Sync mode (fast_mode=false): Every write is followed by fdatasync.
     *     Suitable for correctness-critical paths.
     *   - Fast mode (fast_mode=true): Writes are batched by a background
     *     flusher thread and fsynced periodically.  Lower latency but
     *     relaxed durability guarantee.
     */
    class Manifest {
        public:
            // ================================================================
            // Event types (used for replay / state queries)
            // ================================================================

            struct SSTSealEvent {
                int level;
                std::string file;
                uint64_t entries;
                std::optional<std::string> first_key_hex;
                std::optional<std::string> last_key_hex;
                uint64_t ts_us; ///< Timestamp at seal time (μs since epoch)
            };

            struct CheckpointEvent {
                std::optional<std::string> name;
                std::optional<uint64_t> stripe;
                std::optional<uint64_t> last_seq;
                uint64_t ts_us;
            };

            // ================================================================
            // Factory / lifecycle
            // ================================================================

            /**
             * Opens (or creates) a manifest at the given path.
             *
             * @param path      Base path for the manifest file (e.g. "db/MANIFEST.akmf").
             *                  Rotated files are stored as "<path>.1", "<path>.2", etc.
             * @param fast_mode Enable background-flusher (batched fsync) mode.
             * @throws std::runtime_error on I/O failure.
             */
            [[nodiscard]] static std::unique_ptr<Manifest> create(const std::filesystem::path& path, bool fast_mode = false);

            ~Manifest();

            Manifest(const Manifest&) = delete;
            Manifest& operator=(const Manifest&) = delete;

            /**
             * Starts the background flusher thread (fast_mode only).
             * No-op in sync mode.  Must be called before first write.
             */
            void start();

            // ================================================================
            // Write API
            // ================================================================

            /**
             * Records a stripe-counter advance.
             * @throws std::invalid_argument if new_count < current count.
             */
            void advance(uint64_t new_count);

            /**
             * Records that a new SST file has been sealed.
             */
            void sst_seal(
                int level,
                const std::string& file,
                uint64_t entries,
                const std::optional<std::string>& first_key_hex,
                const std::optional<std::string>& last_key_hex
            );

            /**
             * Records a checkpoint marker.
             */
            void checkpoint(const std::optional<std::string>& name, const std::optional<uint64_t>& stripe, const std::optional<uint64_t>& last_seq);

            /**
             * Records that a compaction has started on a set of input SSTs.
             */
            void compaction_start(int level, const std::vector<std::string>& inputs);

            /**
             * Records that a compaction has completed, producing one output SST.
             */
            void compaction_end(
                int level,
                const std::string& output,
                const std::vector<std::string>& inputs,
                uint64_t entries,
                const std::optional<std::string>& first_key_hex,
                const std::optional<std::string>& last_key_hex
            );

            /**
             * Records that an SST file has been deleted.
             * @deprecated Use compaction_commit() for compaction-driven deletions.
             */
            void sst_delete(const std::string& file);

            /**
             * Atomically records all outputs produced and all inputs consumed by a
             * compaction in a single CRC-protected manifest record.
             *
             * This replaces the non-atomic (CompactionEnd + SSTDelete ÁEN) pattern.
             * During replay, either the full record is applied (all outputs added,
             * all inputs removed) or it is absent (CRC mismatch from partial write),
             * in which case the pre-compaction state is preserved exactly.
             *
             * @param output_files  Filenames of new SST files written by the compaction.
             * @param input_files   Filenames of SST files consumed by the compaction.
             */
            void compaction_commit(const std::vector<std::string>& output_files, const std::vector<std::string>& input_files);

            /**
             * Records a truncation marker (informational).
             */
            void truncate(const std::optional<std::string>& reason = std::nullopt);

            // ================================================================
            // Replay
            // ================================================================

            /**
             * Re-reads all manifest files and rebuilds in-memory state.
             * Normally called internally during construction; exposed for
             * explicit replay after external changes.
             */
            void replay();

            // ================================================================
            // State queries
            // ================================================================

            [[nodiscard]] uint64_t stripes_written() const noexcept;

            [[nodiscard]] std::optional<CheckpointEvent> last_checkpoint() const noexcept;

            /** Returns all currently live SST file names. */
            [[nodiscard]] std::vector<std::string> live_sst() const;

            /** Returns all SST file names that have been deleted. */
            [[nodiscard]] std::vector<std::string> deleted_sst() const;

            /** Returns all SSTSeal events in replay order. */
            [[nodiscard]] std::vector<SSTSealEvent> sst_seals() const;

            // ================================================================
            // Shutdown
            // ================================================================

            /**
             * Flushes any pending writes and closes the manifest file.
             * Idempotent.
             */
            void close();

        private:
            explicit Manifest(const std::filesystem::path& path, bool fast_mode);

            class Impl;
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::manifest

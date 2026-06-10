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

// akkengine/include/akk/engine/wal/WalWriter.hpp
#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>

namespace akkaradb::engine::wal {
    enum class WalSyncMode : uint8_t {
        SYNC = 0, ASYNC = 1, OFF = 2,
    };

    struct WalOptions {
        std::filesystem::path walDir;
        WalSyncMode syncMode = WalSyncMode::SYNC;
        // 0 means auto: one shard per hardware thread, capped at 16.
        uint16_t shardCount = 0;
        uint32_t groupN = 128;
        uint32_t groupMicros = 100;
        uint64_t groupBytes = 4ULL * 1024ULL * 1024ULL;
        uint64_t asyncMaxPendingBytes = 64ULL * 1024ULL * 1024ULL;
    };

    struct WalWriterSnapshot {
        uint32_t shardCount = 0;
        uint64_t entriesWritten = 0;
        uint64_t bytesWritten = 0;
        uint64_t batchesFlushed = 0;
        uint64_t syncsExecuted = 0;
        uint64_t segmentRotations = 0;
    };

    class WalWriter {
        public:
            [[nodiscard]] static std::unique_ptr<WalWriter> create(WalOptions options);
            ~WalWriter();

            WalWriter(const WalWriter&) = delete;
            WalWriter& operator=(const WalWriter&) = delete;
            WalWriter(WalWriter&&) = delete;
            WalWriter& operator=(WalWriter&&) = delete;

            void append(
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint64_t seq,
                uint8_t flags,
                uint64_t precomputedFp64 = 0
            );

            void forceSync();
            void pruneUntil(uint64_t checkpointSeq);
            [[nodiscard]] WalWriterSnapshot snapshot() const noexcept;
            void close();

        private:
            WalWriter() = default;

            class Impl;
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::wal

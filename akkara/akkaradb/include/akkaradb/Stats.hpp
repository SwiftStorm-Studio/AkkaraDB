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

// akkaradb/include/akkaradb/Stats.hpp
#pragma once

#include "akkaradb/Export.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace akkaradb::engine {
    struct LevelStats {
        int level = 0;
        size_t fileCount = 0;
        uint64_t bytes = 0;
        uint64_t budgetBytes = 0;
    };

    struct AKDB_API EngineStats {
        uint64_t currentSeq = 0;
        uint64_t nodeId = 0;

        uint64_t putsTotal = 0;
        uint64_t removesTotal = 0;
        uint64_t getsTotal = 0;
        uint64_t getsMemtableHit = 0;
        uint64_t getsSstHit = 0;
        uint64_t getsMiss = 0;
        uint64_t existsTotal = 0;
        uint64_t scansTotal = 0;
        uint64_t blobPutsTotal = 0;

        struct ApiStats {
            bool enabled = false;
            bool tcpEnabled = false;
            bool tcpTlsEnabled = false;
            uint8_t tcpIoBackend = 0;
            uint32_t tcpWorkerThreads = 0;
            uint32_t tcpAcceptQueueLimit = 0;
            uint32_t tcpAcceptQueueTimeoutMs = 0;
            uint32_t tcpListenBacklog = 0;
            uint32_t tcpReadTimeoutMs = 0;
            uint32_t tcpWriteTimeoutMs = 0;
            uint64_t tcpConnectionsAcceptedTotal = 0;
            uint64_t tcpConnectionsClosedTotal = 0;
            uint64_t tcpConnectionsActive = 0;
            uint64_t tcpAcceptQueueDepth = 0;
            uint64_t tcpAcceptQueuePeakDepth = 0;
            uint64_t tcpAcceptQueueRejectedTotal = 0;
            uint64_t tcpAcceptQueueExpiredTotal = 0;
            uint64_t tcpRequestsTotal = 0;
            uint64_t tcpResponsesTotal = 0;
            uint64_t tcpBytesReceivedTotal = 0;
            uint64_t tcpBytesSentTotal = 0;
            uint64_t tcpProtocolErrorsTotal = 0;
            uint64_t tcpCrcErrorsTotal = 0;
            uint64_t tcpPipelineBatchesTotal = 0;
            uint64_t tcpBackpressureFlushesTotal = 0;
            uint64_t tcpBackpressureDisconnectsTotal = 0;
            uint64_t tcpBatchPutItemsTotal = 0;
            uint64_t tcpBatchGetItemsTotal = 0;
        } api;

        struct MemTableStats {
            uint32_t shardCount = 0;
            uint64_t thresholdBytesPerShard = 0;
            uint64_t approxBytes = 0;
            uint64_t putsApplied = 0;
            uint64_t removesApplied = 0;
            uint64_t flushesCompleted = 0;
            uint64_t bytesFlushed = 0;
        } memtable;

        struct WalStats {
            bool enabled = false;
            uint32_t shardCount = 0;
            uint64_t entriesWritten = 0;
            uint64_t bytesWritten = 0;
            uint64_t batchesFlushed = 0;
            uint64_t syncsExecuted = 0;
            uint64_t segmentRotations = 0;
        } wal;

        struct BlobStats {
            bool enabled = false;
            uint64_t thresholdBytes = 0;
            uint64_t blobsWritten = 0;
            uint64_t bytesUncompressed = 0;
            uint64_t bytesOnDisk = 0;
            uint64_t blobsDeleted = 0;
            uint64_t gcCycles = 0;
        } blob;

        struct SstStats {
            bool enabled = false;
            std::vector<LevelStats> levels;
            size_t fileCount = 0;
            uint64_t bytes = 0;
            size_t l0FileCount = 0;
            bool compactionPending = false;
            uint64_t compactionsCompleted = 0;
            uint64_t filesCompacted = 0;
            uint64_t bytesCompactedIn = 0;
            uint64_t bytesCompactedOut = 0;
            uint64_t l0Stalls = 0;
        } sst;

        struct VLogStats {
            bool enabled = false;
        } vlog;
    };
} // namespace akkaradb::engine

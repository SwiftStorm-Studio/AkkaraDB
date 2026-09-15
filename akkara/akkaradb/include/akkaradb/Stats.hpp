/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkaradb/include/akkaradb/Stats.hpp
/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkaradb/include/akkaradb/Stats.hpp
#pragma once

#include "akkaradb/Export.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

namespace akkaradb::engine {
    enum class ClusterHealthState : uint32_t {
        HEALTHY = 0,
        DEGRADED = 1,
        FAILED = 2,
    };

    enum class ClusterFailureCode : uint32_t {
        NONE = 0,
        FOREIGN_CLUSTER = 1,
        POLICY_MISMATCH = 2,
        LEASE_RENEWAL = 3,
        ENDPOINT_START = 4,
        PEER_READ_TIMEOUT = 5,
        RAFT_PERSISTENCE = 6,
    };

    /** Self-describing cumulative latency histogram suitable for management UIs. */
    struct ClusterLatencyHistogram {
        std::array<uint64_t, 8> bucketUpperBoundsUs{
            100, 500, 1'000, 5'000, 10'000, 50'000, 250'000, std::numeric_limits<uint64_t>::max(),
        };
        /// Cumulative counts: bucketCounts[i] contains every sample <= bucketUpperBoundsUs[i].
        std::array<uint64_t, 8> bucketCounts{};
        uint64_t sampleCount = 0;
        uint64_t totalUs = 0;
        uint64_t maxUs = 0;
    };

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

        struct ConfigStats {
            uint32_t writeAdmission = 0;
            uint32_t writeDurability = 0;
            uint32_t writeVisibility = 0;
            uint32_t readVisibility = 0;
            uint32_t sequenceAllocation = 0;
            uint32_t sequenceThreadLocalRangeSize = 0;
            uint32_t commitWindowSize = 0;
            uint32_t memtableBackpressureMode = 0;
            uint32_t maxMemtableImmutableTables = 0;
            uint32_t sstBackpressureMode = 0;
            uint32_t maxSstL0Files = 0;
            uint32_t backpressureWaitMicros = 0;
            uint32_t backpressureTimeoutMs = 0;
            uint32_t memtableFlushMode = 0;
            uint32_t walExecution = 0;
            uint32_t walSyncPolicy = 0;
            uint32_t walBackpressure = 0;
            uint32_t sstCompactionMode = 0;
        } config;

        struct BackpressureStats {
            uint64_t blockedWrites = 0;
            uint64_t rejectedWrites = 0;
            uint64_t timedOutWrites = 0;
            uint64_t memtableStalls = 0;
            uint64_t sstStalls = 0;
            uint64_t waitMicrosTotal = 0;
            uint64_t waitMicrosMax = 0;
        } backpressure;

        struct ApiStats {
            bool enabled = false;
            bool httpEnabled = false;
            bool httpTlsEnabled = false;
            uint16_t httpPort = 0;
            uint32_t httpMaxBatchItems = 0;
            uint32_t httpMaxScanItems = 0;
            uint32_t httpMaxHistoryEntries = 0;
            uint64_t httpMaxContentLength = 0;
            uint64_t httpConnectionsAcceptedTotal = 0;
            uint64_t httpConnectionsClosedTotal = 0;
            uint64_t httpConnectionsActive = 0;
            uint64_t httpRequestsTotal = 0;
            uint64_t httpResponsesTotal = 0;
            uint64_t httpBytesReceivedTotal = 0;
            uint64_t httpBytesSentTotal = 0;
            uint64_t httpProtocolErrorsTotal = 0;
            uint64_t httpErrorsTotal = 0;
            uint64_t httpBatchPutItemsTotal = 0;
            uint64_t httpBatchGetItemsTotal = 0;
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
            bool grpcEnabled = false;
            bool grpcTlsEnabled = false;
            uint16_t grpcPort = 0;
            uint32_t grpcWorkerThreads = 0;
            uint32_t grpcCompletionQueues = 0;
            uint32_t grpcMinPollers = 0;
            uint32_t grpcMaxPollers = 0;
            uint32_t grpcMaxConcurrentStreams = 0;
            uint64_t grpcResourceQuotaBytes = 0;
            uint32_t grpcMaxBatchItems = 0;
            uint32_t grpcMaxScanItems = 0;
            uint32_t grpcMaxHistoryEntries = 0;
            uint64_t grpcRequestsTotal = 0;
            uint64_t grpcResponsesTotal = 0;
            uint64_t grpcActiveRequests = 0;
            uint64_t grpcErrorsTotal = 0;
            uint64_t grpcBatchPutItemsTotal = 0;
            uint64_t grpcBatchGetItemsTotal = 0;
        } api;

        struct MemTableStats {
            uint32_t shardCount = 0;
            uint64_t thresholdBytesPerShard = 0;
            uint64_t approxBytes = 0;
            uint64_t putsApplied = 0;
            uint64_t removesApplied = 0;
            uint64_t flushesCompleted = 0;
            uint64_t immutableTables = 0;
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
            uint64_t asyncFailures = 0;
            uint64_t pendingEntries = 0;
            uint64_t pendingBytes = 0;
            uint64_t inFlightBytes = 0;
            bool healthy = true;
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
            uint64_t compactionFailures = 0;
            uint64_t l0Stalls = 0;
        } sst;

        struct ManifestStats {
            bool enabled = false;
            bool hasCheckpoint = false;
            bool sstBlobRefsComplete = false;
            uint64_t lastCheckpointSeq = 0;
            uint64_t lastCheckpointStripe = 0;
            uint64_t liveSstCount = 0;
            uint64_t deletedSstCount = 0;
            uint64_t sstSealCount = 0;
            uint64_t sstReferencedBlobCount = 0;
            uint64_t liveBlobCount = 0;
            uint64_t deletedBlobCount = 0;
            uint64_t blobPutCount = 0;
            uint64_t blobDeleteCount = 0;
            uint64_t lastPrimaryLeaseNodeId = 0;
            uint64_t lastPrimaryLeaseUntilUs = 0;
        } manifest;

        struct ClusterStats {
            struct NodeStats {
                uint64_t nodeId = 0;
                std::string host;
                uint16_t dataPort = 0;
                uint16_t replPort = 0;
                uint32_t capabilities = 0;
            };

            struct PeerStats {
                uint64_t nodeId = 0;
                uint64_t matchIndex = 0;
                uint64_t nextIndex = 0;
                uint64_t replicationLag = 0;
                bool connected = false;
                /// Unix timestamp in microseconds; zero before the first successful contact.
                uint64_t lastSuccessfulContactAtUs = 0;
                uint64_t roundTripsSucceededTotal = 0;
                uint64_t roundTripsFailedTotal = 0;
                uint64_t consecutiveRoundTripFailures = 0;
                /// Unix timestamp in microseconds of the latest completed round trip.
                uint64_t lastRoundTripAtUs = 0;
                /// Unix timestamp in microseconds; zero until the first failed round trip.
                uint64_t lastRoundTripFailureAtUs = 0;
                ClusterLatencyHistogram roundTripLatencyUs;
            };

            bool enabled = false;
            uint32_t role = 0;
            /// Unix timestamp in microseconds at which this snapshot was sampled.
            uint64_t sampledAtUs = 0;
            /// Unix timestamp in microseconds of the latest runtime start attempt.
            uint64_t runtimeStartedAtUs = 0;
            std::array<uint8_t, 16> clusterId{};
            uint32_t replicationMode = 0;
            uint32_t consistencyMode = 0;
            uint32_t transportMode = 0;
            uint64_t clusterGroupId = 0;
            uint64_t clusterGroupEpoch = 0;
            /// Current process-local cluster state; failure history remains below after recovery.
            ClusterHealthState health = ClusterHealthState::HEALTHY;
            ClusterFailureCode lastFailure = ClusterFailureCode::NONE;
            /// Unix timestamp in microseconds, or zero when no failure has been observed.
            uint64_t lastFailureAtUs = 0;
            uint64_t configuredNodeCount = 0;
            uint64_t activeNodeCount = 0;
            bool raftEnabled = false;
            uint64_t raftTerm = 0;
            uint64_t leaderNodeId = 0;
            uint64_t commitIndex = 0;
            uint64_t appliedIndex = 0;
            uint64_t lastLogIndex = 0;
            uint64_t snapshotIndex = 0;
            uint64_t outboundConnectionsTotal = 0;
            uint64_t peerWorkers = 0;
            uint64_t proposalBatches = 0;
            uint64_t proposalQueueDepth = 0;
            uint64_t pendingProposals = 0;
            uint64_t retainedRequestResults = 0;
            uint64_t pendingRequests = 0;
            uint64_t requestCapacity = 0;
            uint64_t expiredRequestsTotal = 0;
            uint64_t rejectedRequestsTotal = 0;
            uint64_t requestJournalBytes = 0;
            uint64_t requestJournalRecords = 0;
            uint64_t requestJournalBytesWrittenTotal = 0;
            uint64_t requestJournalCompactionsTotal = 0;
            uint64_t peerPolicyMismatchRejectsTotal = 0;
            uint64_t foreignClusterRejectsTotal = 0;
            uint64_t leaseRenewFailuresTotal = 0;
            uint64_t endpointStartFailuresTotal = 0;
            uint64_t peerReadTimeoutsTotal = 0;
            uint64_t replicationQueueFrames = 0;
            uint64_t replicationQueueBytes = 0;
            uint64_t transferMemoryBytes = 0;
            uint64_t transferSpoolBytes = 0;
            uint64_t activeTransfers = 0;
            uint64_t transferResumeAttemptsTotal = 0;
            uint64_t transferResumedTotal = 0;
            uint64_t transferResumedBytesTotal = 0;
            uint64_t transferDiscardedPartialsTotal = 0;
            uint64_t transferRetainedPartials = 0;
            std::vector<NodeStats> configuredNodes;
            std::vector<PeerStats> peers;
        } cluster;

        struct StripeReadRepairStats {
            uint64_t attempts = 0;
            uint64_t succeeded = 0;
            uint64_t failed = 0;
            uint64_t lastFailureNodeId = 0; ///< Zero until the first failure; no key or value is exposed.
        } stripeReadRepair;

        struct VLogStats {
            bool enabled = false;
            uint32_t syncMode = 0;
            uint32_t groupN = 0;
            uint32_t groupMicros = 0;
            uint64_t groupBytes = 0;
            uint64_t asyncMaxPendingBytes = 0;
            uint64_t indexedKeys = 0;
            uint64_t indexedEntries = 0;
            uint64_t rollbackEntries = 0;
            uint64_t pendingWrites = 0;
            uint64_t pendingBytes = 0;
            uint64_t durableBytes = 0;
            uint64_t segmentCount = 0;
            uint64_t activeSegmentBytes = 0;
            uint64_t recoveryDurationMicros = 0;
            uint64_t recoveredSegmentCount = 0;
            uint64_t recoveredEntryCount = 0;
            uint64_t sidecarFallbackCount = 0;
            uint64_t sidecarRebuildFailures = 0;
            uint64_t retentionPrunedSegments = 0;
            uint64_t retentionBaseEntriesWritten = 0;
            uint64_t parallelQueueRejects = 0;
            uint64_t parallelLaneCount = 0;
            uint64_t parallelPendingWrites = 0;
            uint64_t parallelPendingBytes = 0;
            bool flushThreadRunning = false;
        } vlog;
    };
} // namespace akkaradb::engine

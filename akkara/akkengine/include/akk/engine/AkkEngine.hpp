/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/AkkEngine.hpp
#pragma once
#include "akk/engine/cluster/ClusterRouting.hpp"

#include "akkaradb/Export.hpp"

#include "akkaradb/Stats.hpp"
#include "akk/engine/blob/BlobManager.hpp"
#include "akk/engine/cluster/ClusterConfig.hpp"
#include "akk/engine/memtable/MemTable.hpp"
#include "akk/engine/sstable/SSTManager.hpp"
#include "akk/engine/vlog/VersionLog.hpp"
#include "akk/engine/wal/WalWriter.hpp"
#include "akk/core/buffer/BufferArena.hpp"
#include "akk/core/utils/ArenaGenerator.hpp"

#include <cstdint>
#include <array>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

namespace akkaradb::engine {
    namespace detail { struct BulkPutEntry; class ProtocolBulkWriter; }
    using VersionEntry = vlog::VersionEntry;

    enum class TransactionIsolation : uint8_t { SERIALIZABLE, SNAPSHOT_ISOLATION };
    struct TransactionOptions { TransactionIsolation isolation = TransactionIsolation::SERIALIZABLE; };
    class TransactionConflict : public std::runtime_error { public: using std::runtime_error::runtime_error; };
    // A storage error after the commit decision may require recovery to determine the outcome.
    class TransactionOutcomeUnknown : public std::runtime_error { public: using std::runtime_error::runtime_error; };

    /** Logical position in one mutation stream. */
    struct AKDB_API Revision {
        uint64_t streamId = 0;
        uint64_t seq = 0;

        friend bool operator==(const Revision&, const Revision&) = default;
    };

    /** A distributed cut expressed as one watermark per logical stream. */
    struct AKDB_API ClusterCheckpoint {
        std::array<uint8_t, 16> clusterId{};
        uint64_t configEpoch = 0;
        uint64_t timelineId = 1;
        std::vector<Revision> watermarks;
    };

    struct AKDB_API ClusterReconfigurationStatus {
        uint64_t authorityNodeId = 0;
        uint64_t activeGeneration = 0;
        uint64_t pendingGeneration = 0;
        bool stripeWritesBlocked = false;
        bool activationStarted = false;
        uint16_t partitionCount = 0;
        uint16_t completedPartitions = 0;
        // STRIPE counters describe the current authority's latest transfer attempt.
        uint64_t transferredBytes = 0;
        uint64_t completedRecords = 0;
        std::string lastError;
    };

    struct AKDB_API ClusterPartitionStats {
        uint64_t streamId = 0;
        bool localHolder = false;
        uint64_t leaderNodeId = 0;
        uint64_t currentTerm = 0;
        uint64_t currentSeq = 0;
        uint64_t commitIndex = 0;
        uint64_t configurationGeneration = 0;
        EngineStats storage;
    };

    enum class RollbackExecutionMode : uint8_t {
        IMMEDIATE = 0,
        IMMEDIATE_AND_DEFER_FAILED = 1,
        NEXT_STARTUP = 2,
    };

    enum class RollbackConflictPolicy : uint8_t {
        FAIL_IF_CHANGED = 0,
        OVERWRITE_LATEST = 1,
    };

    enum class RollbackItemStatus : uint8_t {
        APPLIED = 0,
        DEFERRED = 1,
        CONFLICT = 2,
        PERMANENT_FAILURE = 3,
    };

    struct AKDB_API RollbackOptions {
        RollbackExecutionMode execution = RollbackExecutionMode::IMMEDIATE;
        RollbackConflictPolicy conflict = RollbackConflictPolicy::FAIL_IF_CHANGED;
    };

    struct AKDB_API RollbackItemResult {
        std::vector<uint8_t> key;
        RollbackItemStatus status = RollbackItemStatus::PERMANENT_FAILURE;
        std::string message;
    };

    struct AKDB_API RollbackResult {
        std::array<uint8_t, 16> operationId{};
        std::vector<RollbackItemResult> items;

        [[nodiscard]] bool complete() const noexcept;
        [[nodiscard]] size_t appliedCount() const noexcept;
        [[nodiscard]] size_t deferredCount() const noexcept;
    };

    enum class Codec : uint8_t {
        NONE = 0, ZSTD = 1,
    };

    struct AKDB_API AkkEngineOptions {
        struct Paths {
            std::filesystem::path dataDir;
            std::filesystem::path walDir;
            std::filesystem::path blobDir;
            std::filesystem::path sstDir;
            std::filesystem::path manifestPath;
            std::filesystem::path versionLogPath;
            std::filesystem::path clusterConfigPath;
            std::filesystem::path nodeIdPath;
            std::filesystem::path rollbackJournalPath;
            std::filesystem::path transactionDir;
        } paths;

        struct Components {
            bool walEnabled = true;
            bool blobEnabled = true;
            bool manifestEnabled = true;
            bool sstEnabled = true;
            bool versionLogEnabled = false;
            bool clusterEnabled = false;
            bool apiEnabled = false;
        } components;

        struct ManifestOptions {
            bool fastMode = false;
        } manifest;

        struct ClusterOptions {
            std::optional<cluster::ClusterConfig> config;
            std::filesystem::path runtimeBackendPath;
            cluster::ClusterRuntimeOptions runtime;
        } cluster;

        enum class ApiBackend : uint8_t {
            HTTP = 0, TCP = 1, GRPC = 2,
        };

        enum class ApiIoBackend : uint8_t {
            AUTO = 0, THREAD_POOL = 1,
        };

        enum class ApiTransportMode : uint8_t {
            TLS = 0, PLAIN = 1,
        };

        enum class WriteAdmissionMode : uint8_t {
            AUTO = 0, SERIAL = 1, PARALLEL = 2,
        };

        enum class ParallelWriteOrderMode : uint8_t {
            // Preserve the current fast path: a read observes the last write
            // applied to a key, even when parallel writers reserved sequences
            // in a different order.
            APPLY_ORDER = 0,
            // Serialize writes that route to the same ordering stripe before
            // sequence allocation. Different stripes still insert concurrently.
            KEY_SEQUENCE = 1,
        };

        enum class WritePolicyPreset : uint8_t {
            CUSTOM = 0, SAFE = 1, BALANCED = 2, FAST = 3,
        };

        enum class WriteDurabilityMode : uint8_t {
            // Valid only without WAL: the write is retained only by the current process.
            MEMORY = 0,
            // The WAL queue accepted the record and the MemTable was updated. The
            // write is visible but not durable; a later async WAL failure poisons
            // the engine and is rethrown by subsequent public operations.
            ENQUEUED = 1,
            // The WAL flusher wrote and flushed the record, but did not necessarily sync it to storage.
            WRITTEN = 2,
            // The WAL record completed fdatasync before the write returns.
            SYNCED = 3,
        };

        enum class WriteVisibilityMode : uint8_t {
            COMMIT_ORDER = 0, APPLIED = 1,
        };

        enum class ReadVisibilityMode : uint8_t {
            AUTO = 0, COMMIT_ORDER = 1, APPLIED = 2,
        };

        enum class ScanConsistencyMode : uint8_t {
            // Ordered scan with no scan-lifetime write exclusion.
            WEAK_ORDERED = 0,
            // Hold every MemTable shard read lock for the scan lifetime and
            // capture the sequence after those locks are acquired.
            PINNED_SNAPSHOT = 1,
        };

        enum class SequenceAllocationMode : uint8_t {
            GLOBAL_ATOMIC = 0, THREAD_LOCAL_RANGES = 1,
        };

        enum class BackpressureMode : uint8_t {
            BLOCK = 0, FAIL_FAST = 1,
        };

        struct SequenceOptions {
            // GLOBAL_ATOMIC preserves gap-free commit-order sequencing.
            // THREAD_LOCAL_RANGES reduces seqGen contention but is only valid with APPLIED visibility.
            SequenceAllocationMode allocation = SequenceAllocationMode::GLOBAL_ATOMIC;
            uint32_t threadLocalRangeSize = 1;
            // 0 selects the engine default. Rounded up to a power of two internally.
            uint32_t commitWindowSize = 0;
        };

        struct VisibilityOptions {
            // AUTO keeps the legacy runtime.writeVisibility value. Explicit values override writePolicy preset visibility.
            ReadVisibilityMode readVisibility = ReadVisibilityMode::AUTO;
        };

        struct BackpressureOptions {
            // 0 disables the corresponding admission throttle.
            uint32_t maxMemtableImmutableTables = 0;
            uint32_t maxSstL0Files = 0;
            uint32_t waitMicros = 100;
            // 0 explicitly permits waiting indefinitely. The default bounds write
            // admission stalls so a failed or undersized compaction setup does not
            // leave callers blocked forever.
            uint32_t timeoutMs = 30'000;
            BackpressureMode memtableFlushBacklog = BackpressureMode::BLOCK;
            BackpressureMode sstCompactionBacklog = BackpressureMode::BLOCK;
        };

        struct ApiTlsOptions {
            std::filesystem::path certPath;
            std::filesystem::path keyPath;
            std::filesystem::path caPath;
            std::vector<uint8_t> psk;
            std::string pskIdentity;
            bool verifyPeer = true;
        };

        struct ApiOptions {
            std::vector<ApiBackend> backends;
            std::filesystem::path serverBackendPath;
            std::filesystem::path transportBackendPath;
            std::filesystem::path httpBackendPath;
            std::filesystem::path tcpBackendPath;
            std::filesystem::path grpcBackendPath;
            std::string bindHost;
            uint16_t httpPort = 7070;
            uint32_t httpMaxBatchItems = 4096;
            uint32_t httpMaxScanItems = 4096;
            uint32_t httpMaxHistoryEntries = 4096;
            uint64_t httpMaxContentLength = 64ULL * 1024ULL * 1024ULL;
            uint16_t tcpPort = 7071;
            uint16_t grpcPort = 7072;
            uint32_t grpcWorkerThreads = 0;
            uint32_t grpcCompletionQueues = 0;
            uint32_t grpcMinPollers = 0;
            uint32_t grpcMaxPollers = 0;
            uint32_t grpcMaxConcurrentStreams = 0;
            uint64_t grpcResourceQuotaBytes = 0;
            uint32_t grpcMaxBatchItems = 4096;
            uint32_t grpcMaxScanItems = 4096;
            uint32_t grpcMaxHistoryEntries = 4096;
            ApiIoBackend tcpIoBackend = ApiIoBackend::AUTO;
            uint32_t tcpWorkerThreads = 0;
            uint32_t tcpAcceptQueueLimit = 4096;
            uint32_t tcpAcceptQueueTimeoutMs = 60000;
            uint32_t tcpListenBacklog = 1024;
            uint32_t tcpRecvBufferBytes = 0;
            uint32_t tcpSendBufferBytes = 0;
            uint32_t tcpPipelineBatchLimit = 64;
            uint32_t tcpMaxBatchItems = 4096;
            uint64_t tcpMaxPendingResponseBytes = 8ULL * 1024ULL * 1024ULL;
            uint32_t tcpReadTimeoutMs = 60000;
            uint32_t tcpWriteTimeoutMs = 30000;
            bool tcpNoDelay = true;
            bool tcpKeepAlive = true;
            ApiTransportMode transportMode = ApiTransportMode::TLS;
            ApiTlsOptions tls;
        } api;

        struct RuntimeOptions {
            uint32_t writerThreads = 0;
            bool recoverWal = true;
            bool truncateCorruptWalOnRecovery = false;
            bool ignoreVersionLogSupplementErrors = false;
            bool recoverSst = true;
            bool pruneWalOnFlush = true;
            bool forceFlushOnClose = true;
            bool forceSyncOnClose = true;
            bool sstPromoteReads = false;
            WritePolicyPreset writePolicy = WritePolicyPreset::CUSTOM;
            WriteAdmissionMode writeAdmission = WriteAdmissionMode::AUTO;
            ParallelWriteOrderMode parallelWriteOrder = ParallelWriteOrderMode::APPLY_ORDER;
            WriteDurabilityMode writeDurability = WriteDurabilityMode::SYNCED;
            // Legacy alias for visibility.readVisibility. Kept for source compatibility.
            WriteVisibilityMode writeVisibility = WriteVisibilityMode::COMMIT_ORDER;
            VisibilityOptions visibility;
            ScanConsistencyMode scanConsistency = ScanConsistencyMode::WEAK_ORDERED;
            SequenceOptions sequence;
            BackpressureOptions backpressure;
            // Enables the concurrent write path when WAL, blob, and cluster are disabled. VersionLog chooses its own admission mode.
            // Concurrent readers may observe relaxed cross-writer visibility while writes are in flight.
            bool relaxedConcurrentWrites = false;
            // Store mutable engine files under an active generation directory.
            // Disabled by default so existing data directories retain their layout.
            bool generationLayoutEnabled = false;
        } runtime;

        struct TransactionRuntimeOptions {
            uint32_t maxOpen = 64;
            uint32_t maxLifetimeMs = 300'000;
            uint32_t maxWrites = 65'536;
            uint64_t maxWriteBytes = 64ull * 1024 * 1024;
            uint64_t maxReadSetBytes = 32ull * 1024 * 1024;
            uint64_t maxPinnedBytes = 512ull * 1024 * 1024;
            uint64_t maxTrackedBytes = 64ull * 1024 * 1024;
            uint64_t maxJournalBytes = 256ull * 1024 * 1024;
        } transactions;

        memtable::MemTable::Options memtable;
        wal::WalOptions wal;
        blob::BlobManager::Options blob;
        sst::SSTManager::Options sst;
        vlog::VersionLogOptions vlog;
    };

    class AKDB_API AkkEngine {
        public:
            struct ScanRecordView {
                std::span<const uint8_t> key;
                std::span<const uint8_t> value;
            };

            struct BatchGetResult {
                bool found = false;
                std::vector<uint8_t> value;
            };

            [[nodiscard]] static std::unique_ptr<AkkEngine> open(AkkEngineOptions options);
            ~AkkEngine();

            AkkEngine(const AkkEngine&) = delete;
            AkkEngine& operator=(const AkkEngine&) = delete;
            AkkEngine(AkkEngine&&) = delete;
            AkkEngine& operator=(AkkEngine&&) = delete;

            void put(std::span<const uint8_t> key, std::span<const uint8_t> value);
            void putHinted(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t fp64, uint64_t miniKey);
            // Standalone, move-only session. Concurrent use is unsupported; sequential
            // handoff to another thread is supported. Finish sessions/cursors before close().
            class AKDB_API Transaction {
                public:
                    ~Transaction();
                    Transaction(Transaction&&) noexcept;
                    Transaction& operator=(Transaction&&) noexcept;
                    Transaction(const Transaction&) = delete;
                    Transaction& operator=(const Transaction&) = delete;
                    [[nodiscard]] std::optional<std::vector<uint8_t>> get(std::span<const uint8_t> key);
                    [[nodiscard]] bool exists(std::span<const uint8_t> key);
                    [[nodiscard]] std::vector<BatchGetResult> getBatch(std::span<const std::span<const uint8_t>> keys);
                    void put(std::span<const uint8_t> key, std::span<const uint8_t> value);
                    void remove(std::span<const uint8_t> key);
                    [[nodiscard]] core::ArenaGenerator<ScanRecordView> scan(core::BufferArena& arena,
                        std::span<const uint8_t> startKey = {}, std::span<const uint8_t> endKey = {});
                    [[nodiscard]] size_t count(std::span<const uint8_t> startKey = {}, std::span<const uint8_t> endKey = {});
                    // Conflicts abort the transaction. Storage errors can have an unknown outcome.
                    void end();
                    void rollback() noexcept;
                    [[nodiscard]] bool active() const noexcept;
                private:
                    friend class AkkEngine;
                    class Impl;
                    explicit Transaction(std::shared_ptr<Impl> impl);
                    std::shared_ptr<Impl> impl_;
            };
            // Native standalone transactions; independent of VersionLog enablement.
            // Transactions and their cursors must finish before closing the engine.
            [[nodiscard]] Transaction begin(TransactionOptions options = {});
            void remove(std::span<const uint8_t> key);
            [[nodiscard]] cluster::ClusterRequestId newRequestId(uint64_t retentionMs = 0) const;
            cluster::ClusterRequestResult putWithRequest(const cluster::ClusterRequestId& id,
                std::span<const uint8_t> key, std::span<const uint8_t> value);
            cluster::ClusterRequestResult removeWithRequest(const cluster::ClusterRequestId& id, std::span<const uint8_t> key);
            [[nodiscard]] cluster::ClusterRequestResult queryRequest(const cluster::ClusterRequestId& id);
            // PARTITIONED request identities and results belong to the key's
            // stable partition stream. Use the same key when resolving a retry.
            [[nodiscard]] cluster::ClusterRequestResult queryRequest(const cluster::ClusterRequestId& id, std::span<const uint8_t> key);
            void removeHinted(std::span<const uint8_t> key, uint64_t fp64, uint64_t miniKey);

            [[nodiscard]] std::optional<std::vector<uint8_t>> get(std::span<const uint8_t> key) const;
            // Returns all results from one visibility snapshot captured when the call begins.
            [[nodiscard]] std::vector<BatchGetResult> getBatch(std::span<const std::span<const uint8_t>> keys) const;
            [[nodiscard]] bool exists(std::span<const uint8_t> key) const;
            [[nodiscard]] bool getInto(std::span<const uint8_t> key, std::vector<uint8_t>& out) const;
            [[nodiscard]] bool getIntoArena(std::span<const uint8_t> key, core::BufferArena& arena, std::span<const uint8_t>& out) const;
            // Cluster-wide public-key range reads. PARTITIONED captures one cut
            // per owner, not a globally atomic transaction snapshot. MIRROR
            // honors readMode; STRIPE uses a metadata-quorum read barrier.
            [[nodiscard]] size_t count(std::span<const uint8_t> startKey = {}, std::span<const uint8_t> endKey = {}) const;
            [[nodiscard]] core::ArenaGenerator<ScanRecordView> scan(
                core::BufferArena& arena,
                std::span<const uint8_t> startKey = {},
                std::span<const uint8_t> endKey = {}
            ) const;
            // Explicit node-local storage inspection. In cluster mode this may
            // expose replicated records and internal STRIPE keys; it is not a
            // distributed user-key scan and has no cluster-wide snapshot.
            [[nodiscard]] core::ArenaGenerator<ScanRecordView> scanLocalStorage(
                core::BufferArena& arena,
                std::span<const uint8_t> startKey = {},
                std::span<const uint8_t> endKey = {}
            ) const;

            // atSeq is in this key's logical stream (owner stream for PARTITIONED,
            // shared stream for MIRROR/Raft/STRIPE). Revision additionally validates
            // the stream. History values are materialized, never Blob/shard refs.
            [[nodiscard]] std::optional<std::vector<uint8_t>> getAt(std::span<const uint8_t> key, uint64_t atSeq) const;
            [[nodiscard]] std::optional<std::vector<uint8_t>> getAt(std::span<const uint8_t> key, Revision revision) const;
            // Captures at the call; owns its Arena and keeps the engine alive until completion or destruction.
            // Entry references expire on advancement; iteration can throw after a prefix.
            [[nodiscard]] core::ArenaGenerator<VersionEntry> history(std::span<const uint8_t> key) const;
            // Scalar sequence overloads are first-class non-cluster APIs.
            // Cluster callers must use Revision/ClusterCheckpoint so a
            // PARTITIONED owner stream can never be mistaken for a global seq.
            [[nodiscard]] RollbackResult rollbackTo(uint64_t targetSeq, RollbackOptions options = {});
            [[nodiscard]] RollbackResult rollbackKey(
                std::span<const uint8_t> key,
                uint64_t targetSeq,
                RollbackOptions options = {}
            );
            [[nodiscard]] RollbackResult rollbackTo(const ClusterCheckpoint& target, RollbackOptions options = {});
            [[nodiscard]] RollbackResult rollbackKey(
                std::span<const uint8_t> key,
                Revision target,
                RollbackOptions options = {}
            );
            [[nodiscard]] ClusterCheckpoint createClusterCheckpoint() const;
            [[nodiscard]] uint64_t revisionStreamId(std::span<const uint8_t> key) const;

            [[nodiscard]] EngineStats stats() const noexcept;

            void addClusterVotingNode(const cluster::NodeInfo& node);
            // Leader-only learner administration. Bootstrap joining processes
            // with RAFT_LEARNER; promotion requires catch-up and joint consensus.
            void addClusterLearner(const cluster::NodeInfo& node);
            void promoteClusterLearner(uint64_t nodeId);
            void removeClusterLearner(uint64_t nodeId);
            void removeClusterVotingNode(uint64_t nodeId);
            void transferClusterLeadership(uint64_t targetNodeId);
            void transferMirrorAuthorityLeadership(uint64_t targetNodeId);
            /** Explicitly attempts recovery of an unresolved MIRROR write grant. */
            bool recoverMirrorWrite();
            /** Starts an explicit election; PARTITIONED requires a stable revision stream id. */
            void campaignClusterLeadership(uint64_t revisionStreamId = 0);
            /**
             * Replaces MIRROR data-consensus membership, PARTITIONED holders and
             * copy count, or STRIPE placement and hot spare while running.
             * Existing endpoints, partition count, stripe geometry, identity,
             * replication mode and consensus policy remain fixed.
             */
            void reconfigureCluster(cluster::ClusterConfig config);
            /// Cancel a pending STRIPE plan before authority membership activation starts.
            /// Returns false if no plan remains; quorum is required to commit cancellation.
            bool cancelClusterReconfiguration();
            [[nodiscard]] ClusterReconfigurationStatus clusterReconfigurationStatus() const;
            [[nodiscard]] std::vector<ClusterPartitionStats> clusterPartitionStats() const;

            void forceSync();
            void forceFlush();
            void runBlobGc();
            void close();

        private:
            friend class detail::ProtocolBulkWriter;
            void applyProtocolBulk(std::span<const detail::BulkPutEntry> entries);
            AkkEngine();

            class Impl;
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine

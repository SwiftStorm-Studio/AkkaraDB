/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/detail/AkkEngineImpl.hpp
#pragma once

#include "AkkEngineSupport.hpp"

namespace akkaradb::engine {
    namespace fs = std::filesystem;
    using namespace detail::runtime;

    // Private runtime composition. Public entry points only dispatch to these operations.
    class AkkEngine::Impl {
        public:
            #include "AkkEngineState.inc"
            #include "AkkEngineRuntime.inc"
            #include "AkkEngineStorage.inc"
            #include "AkkEngineStripeFormat.inc"
            #include "AkkEnginePointRead.inc"
            #include "AkkEngineStripeRead.inc"
            #include "AkkEngineStripeWrite.inc"
            #include "AkkEngineStripeMaintenance.inc"
            #include "AkkEngineRouting.inc"
            #include "AkkEnginePointAccess.inc"
            #include "AkkEngineWriteAdmission.inc"
            #include "AkkEngineBlobGc.inc"
            #include "AkkEngineMutation.inc"
            #include "AkkEngineSnapshots.inc"
            #include "AkkEngineWriteCoordinators.inc"
            #include "AkkEngineRollbackJournal.inc"
            #include "AkkEngineRollbackHistory.inc"

            static std::unique_ptr<AkkEngine> create(AkkEngineOptions options);
            void initializeStorage();
            void initializeCluster();
            cluster::ClusterEngineCallbacks makeClusterCallbacks(const cluster::ClusterConfig& cfg);
            cluster::ClusterRequestId newRequestId(uint64_t retentionMs = 0);
            cluster::ClusterRequestResult putWithRequest(const cluster::ClusterRequestId& id, std::span<const uint8_t> key, std::span<const uint8_t> value);
            cluster::ClusterRequestResult removeWithRequest(const cluster::ClusterRequestId& id, std::span<const uint8_t> key);
            cluster::ClusterRequestResult queryLocalRequestResult(const cluster::ClusterRequestId& id);
            cluster::ClusterRequestResult queryRequest(const cluster::ClusterRequestId& id);
            cluster::ClusterRequestResult queryRequest(const cluster::ClusterRequestId& id, std::span<const uint8_t> key);
            void put(std::span<const uint8_t> key, std::span<const uint8_t> value);
            void putHinted(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t fp64, uint64_t miniKey);
            void applyProtocolBulk(std::span<const detail::BulkPutEntry> entries);
            void remove(std::span<const uint8_t> key);
            void removeHinted(std::span<const uint8_t> key, uint64_t fp64, uint64_t miniKey);
            std::optional<std::vector<uint8_t>> get(std::span<const uint8_t> key);
            std::vector<BatchGetResult> getBatch(std::span<const std::span<const uint8_t>> keys);
            bool exists(std::span<const uint8_t> key);
            bool getInto(std::span<const uint8_t> key, std::vector<uint8_t>& out);
            bool getIntoArena(std::span<const uint8_t> key, BufferArena& arena, std::span<const uint8_t>& out);
            size_t count(std::span<const uint8_t> startKey, std::span<const uint8_t> endKey);
            ArenaGenerator<ScanRecordView> scan(
                BufferArena& arena,
                std::span<const uint8_t> startKey,
                std::span<const uint8_t> endKey);
            ArenaGenerator<ScanRecordView> scanLocalStorage(
                BufferArena& arena,
                std::span<const uint8_t> startKey,
                std::span<const uint8_t> endKey);
            std::optional<std::vector<uint8_t>> getAt(std::span<const uint8_t> key, uint64_t atSeq);
            std::optional<std::vector<uint8_t>> getAt(std::span<const uint8_t> key, Revision revision);
            ArenaGenerator<VersionEntry> history(std::span<const uint8_t> key);
            RollbackResult rollbackTo(uint64_t targetSeq, RollbackOptions options);
            RollbackResult rollbackKey(std::span<const uint8_t> key, uint64_t targetSeq, RollbackOptions options);
            uint64_t revisionStreamId(std::span<const uint8_t> key);
            ClusterCheckpoint createClusterCheckpoint();
            RollbackResult rollbackKey(std::span<const uint8_t> key, Revision target, RollbackOptions options);
            RollbackResult rollbackTo(const ClusterCheckpoint& target, RollbackOptions options);
            void addClusterVotingNode(const cluster::NodeInfo& node);
            void addClusterLearner(const cluster::NodeInfo& node);
            void transferMirrorAuthorityLeadership(uint64_t targetNodeId);
            void reconfigureRequestAuthority(const std::vector<cluster::NodeInfo>& nodes);
            bool recoverMirrorWrite();
            void promoteClusterLearner(uint64_t nodeId);
            void removeClusterLearner(uint64_t nodeId);
            void removeClusterVotingNode(uint64_t nodeId);
            void transferClusterLeadership(uint64_t targetNodeId);
            void campaignClusterLeadership(uint64_t streamId);
            bool cancelClusterReconfiguration();
            ClusterReconfigurationStatus clusterReconfigurationStatus();
            std::vector<ClusterPartitionStats> clusterPartitionStats();
            cluster::ClusterAvailability clusterAvailability();
            void forceSync();
            void forceFlush();
            void runBlobGc();
            void shutdown();
            Transaction begin(TransactionOptions options);
            cluster::ClusterTransactionResult transactionOutcome(const cluster::ClusterTransactionId& id);
            EngineStats stats() noexcept;
    };
}

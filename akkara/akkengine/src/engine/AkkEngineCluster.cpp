/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/AkkEngineCluster.cpp
#include "detail/AkkEngineImpl.hpp"

namespace akkaradb::engine {
    void AkkEngine::Impl::addClusterVotingNode(const cluster::NodeInfo& node) {
        OperationGuard operation{*this};
        if (!clusterRuntime) { throw std::runtime_error("AkkEngine: cluster runtime is not enabled"); }
        auto transactionsAdmission = distributedConfigurationAdmission();
        clusterRuntime->addRaftVotingNode(node);
    }

    void AkkEngine::Impl::addClusterLearner(const cluster::NodeInfo& node) {
        OperationGuard operation{*this};
        if (!clusterRuntime) { throw std::runtime_error("AkkEngine: cluster runtime is not enabled"); }
        auto transactionsAdmission = distributedConfigurationAdmission();
        clusterRuntime->addRaftLearner(node);
    }

    void AkkEngine::Impl::transferMirrorAuthorityLeadership(uint64_t targetNodeId) {
        OperationGuard operation{*this};
        if (!clusterRuntime) { throw std::runtime_error("AkkEngine: cluster runtime is not enabled"); }
        clusterRuntime->transferMirrorAuthorityLeadership(targetNodeId);
    }

    void AkkEngine::Impl::reconfigureRequestAuthority(const std::vector<cluster::NodeInfo>& nodes) {
        OperationGuard operation{*this};
        if (!clusterRuntime) { throw std::logic_error("AkkEngine: cluster runtime is not enabled"); }
        clusterRuntime->reconfigureRequestAuthority(nodes);
    }

    bool AkkEngine::Impl::recoverMirrorWrite() {
        OperationGuard operation{*this};
        if (!clusterRuntime) { throw std::runtime_error("AkkEngine: cluster runtime is not enabled"); }
        return clusterRuntime->recoverMirrorWrite();
    }

    void AkkEngine::Impl::promoteClusterLearner(uint64_t nodeId) {
        OperationGuard operation{*this};
        if (!clusterRuntime) { throw std::runtime_error("AkkEngine: cluster runtime is not enabled"); }
        auto transactionsAdmission = distributedConfigurationAdmission();
        clusterRuntime->promoteRaftLearner(nodeId);
    }

    void AkkEngine::Impl::removeClusterLearner(uint64_t nodeId) {
        OperationGuard operation{*this};
        if (!clusterRuntime) { throw std::runtime_error("AkkEngine: cluster runtime is not enabled"); }
        auto transactionsAdmission = distributedConfigurationAdmission();
        clusterRuntime->removeRaftLearner(nodeId);
    }

    void AkkEngine::Impl::removeClusterVotingNode(uint64_t nodeId) {
        OperationGuard operation{*this};
        if (!clusterRuntime) { throw std::runtime_error("AkkEngine: cluster runtime is not enabled"); }
        auto transactionsAdmission = distributedConfigurationAdmission();
        clusterRuntime->removeRaftVotingNode(nodeId);
    }

    void AkkEngine::Impl::transferClusterLeadership(uint64_t targetNodeId) {
        OperationGuard operation{*this};
        if (!clusterRuntime) { throw std::runtime_error("AkkEngine: cluster runtime is not enabled"); }
        clusterRuntime->transferRaftLeadership(targetNodeId);
    }

    void AkkEngine::Impl::campaignClusterLeadership(uint64_t streamId) {
        OperationGuard operation{*this};
        if (!clusterRuntime) { throw std::logic_error("AkkEngine: cluster runtime is not enabled"); }
        if (usesPartitionRaft()) {
            auto children = partitionSnapshot();
            if (streamId == 0 || streamId > children.size() || !children[streamId - 1]) {
                throw std::invalid_argument("AkkEngine: campaign requires a locally hosted partition stream");
            }
            children[streamId - 1]->campaignClusterLeadership();
        }
        else {
            if (streamId != 0) { throw std::invalid_argument("AkkEngine: MIRROR uses revision stream zero"); }
            clusterRuntime->campaignLeadership();
        }
    }

    bool AkkEngine::Impl::cancelClusterReconfiguration() {
        OperationGuard operation{*this};
        return cancelPlacement();
    }

    ClusterReconfigurationStatus AkkEngine::Impl::clusterReconfigurationStatus() {
        OperationGuard operation{*this};
        if (!usesPartitionRaft() && clusterReplicationMode != cluster::ReplicationMode::STRIPE) {
            throw std::logic_error("AkkEngine: placement status requires PARTITIONED consensus or STRIPE");
        }
        const auto state = clusterRuntime->placementState(true);
        ClusterReconfigurationStatus result;
        result.authorityNodeId = state.leaderNodeId;
        result.activeGeneration = state.generation;
        result.pendingGeneration = state.pendingGeneration;
        result.stripeWritesBlocked = state.stripeWritesBlocked;
        result.activationStarted = state.activationStarted;
        result.partitionCount = clusterReplicationMode == cluster::ReplicationMode::STRIPE ? 1 : clusterConfig.partition().partitionCount;
        {
            std::lock_guard progress{placementProgressMu};
            result.lastError = placementLastError;
        }
        if (state.pendingGeneration == 0) { result.completedPartitions = result.partitionCount; }
        if (clusterReplicationMode == cluster::ReplicationMode::STRIPE) {
            const auto response = partitionRpc(state.leaderNodeId, partitionAdminHeader(PartitionAdmin::STATUS, 0, 0));
            size_t cursor = 0;
            if (!pullU64(response.value, cursor, result.transferredBytes) || !pullU64(response.value, cursor, result.completedRecords) || cursor !=
                response.value.size()) { throw std::runtime_error("Cluster placement: malformed stripe progress response"); }
            return result;
        }
        if (state.pendingGeneration == 0) { return result; }
        const auto active = cluster::ClusterConfig::decode(state.activeConfig);
        const auto target = cluster::ClusterConfig::decode(state.pendingConfig);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{opts.cluster.runtime.forwardingTimeoutMs};
        for (uint16_t index = 0; index < result.partitionCount; ++index) {
            try {
                const auto response = partitionRpcLeader(index, partitionAdminHeader(PartitionAdmin::STATUS, index, 0), active, deadline);
                if (response.value.size() < 16 || response.value[11] != 0 || partitionAdminNumber(std::span{response.value}.first(11)) < state.
                    pendingGeneration) { continue; }
                size_t position = 12;
                uint32_t count = 0;
                if (!pullU32(response.value, position, count)) { continue; }
                std::vector<uint64_t> members;
                for (uint32_t i = 0; i < count; ++i) {
                    uint64_t id = 0;
                    if (!pullU64(response.value, position, id)) { throw std::runtime_error("Cluster placement: malformed status membership"); }
                    members.push_back(id);
                }
                std::vector<uint64_t> desired;
                for (const auto& member : partitionConfig(target, index).dataNodes()) { desired.push_back(member.nodeId); }
                std::ranges::sort(members);
                std::ranges::sort(desired);
                if (position == response.value.size() && members == desired) { ++result.completedPartitions; }
            }
            catch (const std::exception& error) { result.lastError = error.what(); }
        }
        return result;
    }

    std::vector<ClusterPartitionStats> AkkEngine::Impl::clusterPartitionStats() {
        OperationGuard operation{*this};
        if (!usesPartitionRaft()) { throw std::logic_error("AkkEngine: partition statistics require PARTITIONED Raft"); }
        std::vector<ClusterPartitionStats> result;
        const auto children = partitionSnapshot();
        for (size_t index = 0; index < children.size(); ++index) {
            ClusterPartitionStats item;
            item.streamId = index + 1;
            if (children[index]) {
                const auto members = children[index]->impl_->clusterRuntime->activeNodes();
                item.localHolder = std::ranges::any_of(members, [&](const auto& member) { return member.nodeId == nodeId && !member.raftLearner(); });
            }
            if (children[index]) {
                const auto stats = children[index]->impl_->clusterRuntime->raftStats();
                item.leaderNodeId = stats.leaderNodeId;
                item.currentTerm = stats.currentTerm;
                item.currentSeq = children[index]->impl_->snapshotSeq();
                item.commitIndex = stats.commitIndex;
                item.configurationGeneration = stats.configurationGeneration;
                item.storage = children[index]->stats();
            }
            result.push_back(item);
        }
        return result;
    }

    cluster::ClusterAvailability AkkEngine::Impl::clusterAvailability() {
        OperationGuard operation{*this};
        if (!clusterRuntime) { throw std::logic_error("AkkEngine: cluster availability requires Cluster"); }
        return currentPlacement().availability();
    }
}

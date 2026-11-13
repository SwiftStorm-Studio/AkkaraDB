/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/AkkEngineRollback.cpp
#include "detail/AkkEngineImpl.hpp"

namespace akkaradb::engine {
    bool RollbackResult::complete() const noexcept {
        return std::ranges::all_of(items, [](const RollbackItemResult& item) { return item.status == RollbackItemStatus::APPLIED; });
    }

    size_t RollbackResult::appliedCount() const noexcept {
        return std::ranges::count_if(items, [](const RollbackItemResult& item) { return item.status == RollbackItemStatus::APPLIED; });
    }

    size_t RollbackResult::deferredCount() const noexcept {
        return std::ranges::count_if(items, [](const RollbackItemResult& item) { return item.status == RollbackItemStatus::DEFERRED; });
    }

    RollbackResult AkkEngine::Impl::rollbackTo(uint64_t targetSeq, RollbackOptions options) {
        OperationGuard operation{*this};
        throwIfBackgroundFailed();
        requireClusterDisabledOperation("rollbackTo(uint64_t)");
        validateRollbackOptions(options);
        if (!versionLog) { throw std::runtime_error("AkkEngine: version log is disabled"); }
        RollbackResult result;
        crypto::secureRandom(result.operationId);
        std::optional<std::array<uint8_t, 16>> deferredTask;
        const uint64_t plannedWatermark = options.execution == RollbackExecutionMode::IMMEDIATE ? UINT64_MAX : captureRollbackWatermark();
        if (options.execution != RollbackExecutionMode::IMMEDIATE) {
            deferredTask = scheduleDeferredRollback(
                result.operationId,
                cluster::StripeControlAction::ROLLBACK_STREAM,
                false,
                0,
                targetSeq,
                plannedWatermark,
                options.conflict
            );
            if (options.execution == RollbackExecutionMode::NEXT_STARTUP) {
                result.items.push_back(RollbackItemResult{.key = {}, .status = RollbackItemStatus::DEFERRED, .message = "scheduled for the next startup"});
                return result;
            }
        }
        try {
            std::unique_lock epochLock{mutationEpochMu};
            versionLog->forceSync();
            for (const auto& key : rollbackKeysChangedAfter(targetSeq)) {
                result.items.push_back(rollbackKeyLocal(key, targetSeq, options.conflict, 0, deferredTask ? plannedWatermark : UINT64_MAX));
            }
            if (deferredTask) { completeDeferredRollback(*deferredTask); }
        }
        catch (const std::exception& error) {
            if (!deferredTask) { throw; }
            result.items.push_back(RollbackItemResult{.key = {}, .status = RollbackItemStatus::DEFERRED, .message = error.what()});
        }
        return result;
    }

    RollbackResult AkkEngine::Impl::rollbackKey(std::span<const uint8_t> key, uint64_t targetSeq, RollbackOptions options) {
        OperationGuard operation{*this};
        throwIfBackgroundFailed();
        requireClusterDisabledOperation("rollbackKey(uint64_t)");
        validateRollbackOptions(options);
        if (!versionLog) { throw std::runtime_error("AkkEngine: version log is disabled"); }
        RollbackResult result;
        crypto::secureRandom(result.operationId);
        std::optional<std::array<uint8_t, 16>> deferredTask;
        const uint64_t plannedWatermark = options.execution == RollbackExecutionMode::IMMEDIATE ? UINT64_MAX : captureRollbackWatermark();
        if (options.execution != RollbackExecutionMode::IMMEDIATE) {
            deferredTask = scheduleDeferredRollback(
                result.operationId,
                cluster::StripeControlAction::ROLLBACK_KEY,
                false,
                0,
                targetSeq,
                plannedWatermark,
                options.conflict,
                key
            );
            if (options.execution == RollbackExecutionMode::NEXT_STARTUP) {
                result.items.push_back(
                    RollbackItemResult{.key = {key.begin(), key.end()}, .status = RollbackItemStatus::DEFERRED, .message = "scheduled for the next startup",}
                );
                return result;
            }
        }
        try {
            std::unique_lock epochLock{mutationEpochMu};
            versionLog->forceSync();
            result.items.push_back(rollbackKeyLocal(key, targetSeq, options.conflict, 0, deferredTask ? plannedWatermark : UINT64_MAX));
            if (deferredTask) { completeDeferredRollback(*deferredTask); }
        }
        catch (const std::exception& error) {
            if (!deferredTask) { throw; }
            result.items.push_back(RollbackItemResult{.key = {key.begin(), key.end()}, .status = RollbackItemStatus::DEFERRED, .message = error.what()});
        }
        return result;
    }

    uint64_t AkkEngine::Impl::revisionStreamId(std::span<const uint8_t> key) {
        OperationGuard operation{*this};
        if (!clusterRuntime) { throw std::logic_error("AkkEngine: revision streams require cluster mode"); }
        return logicalStreamId(key);
    }

    ClusterCheckpoint AkkEngine::Impl::createClusterCheckpoint() {
        OperationGuard operation{*this};
        throwIfBackgroundFailed();
        waitForVersionLogRecovery();
        if (!clusterRuntime) { throw std::logic_error("AkkEngine: cluster checkpoint requires cluster mode"); }
        if (!versionLog) { throw std::runtime_error("AkkEngine: version log is disabled"); }
        ClusterCheckpoint checkpoint;
        checkpoint.clusterId = clusterConfig.clusterId();
        checkpoint.configEpoch = clusterRuntime->configurationEpoch();
        checkpoint.timelineId = 1;
        if (usesPartitionRaft()) {
            const auto placement = livePlacement.load();
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{opts.cluster.runtime.forwardingTimeoutMs};
            for (uint16_t index = 0; index < placement->partition().partitionCount; ++index) {
                const auto response = partitionRpcLeader(
                    index,
                    partitionAdminHeader(PartitionAdmin::WATERMARK, index, 0),
                    *placement,
                    deadline
                );
                checkpoint.watermarks.push_back(Revision{.streamId = uint64_t{index} + 1, .seq = partitionAdminNumber(response.value)});
            }
            return checkpoint;
        }
        const auto watermarkFor = [&](uint64_t targetNodeId) {
            cluster::StripeControlRequest request;
            request.action = cluster::StripeControlAction::ROLLBACK_WATERMARK;
            request.ownerNodeId = targetNodeId;
            const auto response = clusterRuntime->rollbackControl(targetNodeId, std::move(request));
            if (response.status != cluster::StripeControlStatus::COMMITTED) {
                throw std::runtime_error(
                    "AkkEngine: checkpoint watermark request failed with status " + std::to_string(static_cast<unsigned>(response.status))
                );
            }
            return response.fenceToken;
        };
        if (clusterReplicationMode == cluster::ReplicationMode::PARTITIONED) {
            for (const auto& node : clusterConfig.dataNodes()) {
                checkpoint.watermarks.push_back(Revision{.streamId = node.nodeId, .seq = watermarkFor(node.nodeId)});
            }
        }
        else if (clusterReplicationMode == cluster::ReplicationMode::STRIPE) {
            const uint64_t leader = clusterRuntime->stripeMetadataLeaderNodeId();
            if (leader == 0) { throw std::runtime_error("AkkEngine: STRIPE metadata leader is unavailable"); }
            checkpoint.watermarks.push_back(Revision{.streamId = 0, .seq = watermarkFor(leader)});
        }
        else {
            if (clusterRuntime->role() != cluster::NodeRole::PRIMARY) {
                throw std::runtime_error("AkkEngine: checkpoint must be created on the cluster leader");
            }
            checkpoint.watermarks.push_back(Revision{.streamId = 0, .seq = captureRollbackWatermark()});
        }
        return checkpoint;
    }

    RollbackResult AkkEngine::Impl::rollbackKey(std::span<const uint8_t> key, Revision target, RollbackOptions options) {
        OperationGuard operation{*this};
        throwIfBackgroundFailed();
        waitForVersionLogRecovery();
        validateRollbackOptions(options);
        if (!clusterRuntime) { throw std::logic_error("AkkEngine: Revision rollback requires cluster mode"); }
        if (!versionLog) { throw std::runtime_error("AkkEngine: version log is disabled"); }
        if (usesPartitionRaft()) {
            if (target.streamId != logicalStreamId(key)) { throw std::invalid_argument("AkkEngine: revision stream does not own key"); }
            return partitionRollback(partitionIndex(key), target.seq, options, false, key);
        }
        uint64_t controlNode = nodeId;
        if (clusterReplicationMode == cluster::ReplicationMode::PARTITIONED) {
            controlNode = clusterRuntime->ownerNodeId(key);
            if (target.streamId != controlNode) { throw std::invalid_argument("AkkEngine: revision stream does not own key"); }
        }
        else {
            if (target.streamId != 0) { throw std::invalid_argument("AkkEngine: global revision stream id must be zero"); }
            if (clusterReplicationMode == cluster::ReplicationMode::STRIPE) {
                controlNode = clusterRuntime->stripeMetadataLeaderNodeId();
                if (controlNode == 0) { throw std::runtime_error("AkkEngine: STRIPE metadata leader is unavailable"); }
            }
            else if (clusterRuntime->role() != cluster::NodeRole::PRIMARY) {
                throw std::runtime_error("AkkEngine: rollback must be submitted to the cluster leader");
            }
        }
        cluster::StripeControlRequest watermarkRequest;
        watermarkRequest.action = cluster::StripeControlAction::ROLLBACK_WATERMARK;
        watermarkRequest.ownerNodeId = controlNode;
        const auto watermarkResponse = clusterRuntime->rollbackControl(controlNode, std::move(watermarkRequest));
        if (watermarkResponse.status != cluster::StripeControlStatus::COMMITTED) {
            throw std::runtime_error(
                "AkkEngine: rollback planning watermark request failed with status " + std::to_string(static_cast<unsigned>(watermarkResponse.status))
            );
        }
        const uint64_t plannedWatermark = watermarkResponse.fenceToken;
        if (target.seq > plannedWatermark) { throw std::invalid_argument("AkkEngine: revision is ahead of the current stream watermark"); }
        RollbackResult result;
        crypto::secureRandom(result.operationId);
        std::optional<std::array<uint8_t, 16>> deferredTask;
        if (options.execution != RollbackExecutionMode::IMMEDIATE) {
            deferredTask = scheduleDeferredRollback(
                result.operationId,
                cluster::StripeControlAction::ROLLBACK_KEY,
                true,
                clusterReplicationMode == cluster::ReplicationMode::STRIPE ? 0 : controlNode,
                target.seq,
                plannedWatermark,
                options.conflict,
                key
            );
            if (options.execution == RollbackExecutionMode::NEXT_STARTUP) {
                result.items.push_back(
                    RollbackItemResult{.key = {key.begin(), key.end()}, .status = RollbackItemStatus::DEFERRED, .message = "scheduled for the next startup",}
                );
                return result;
            }
        }
        cluster::StripeControlRequest request;
        request.action = cluster::StripeControlAction::ROLLBACK_KEY;
        request.ownerNodeId = controlNode;
        request.fenceToken = target.seq;
        request.key.assign_range(key);
        request.metadata = {std::to_underlying(RollbackExecutionMode::IMMEDIATE), std::to_underlying(options.conflict)};
        if (deferredTask) { pushU64(request.metadata, plannedWatermark); }
        try {
            const auto response = clusterRuntime->rollbackControl(controlNode, std::move(request));
            if (response.status != cluster::StripeControlStatus::COMMITTED) { throw std::runtime_error("AkkEngine: cluster rollback request was rejected"); }
            result.items = decodeRollbackItems(response.metadata);
            if (deferredTask) { completeDeferredRollback(*deferredTask); }
        }
        catch (const std::exception& error) {
            if (!deferredTask) { throw; }
            result.items.push_back(RollbackItemResult{.key = {key.begin(), key.end()}, .status = RollbackItemStatus::DEFERRED, .message = error.what()});
        }
        return result;
    }

    RollbackResult AkkEngine::Impl::rollbackTo(const ClusterCheckpoint& target, RollbackOptions options) {
        OperationGuard operation{*this};
        throwIfBackgroundFailed();
        waitForVersionLogRecovery();
        validateRollbackOptions(options);
        if (!clusterRuntime) { throw std::logic_error("AkkEngine: ClusterCheckpoint rollback requires cluster mode"); }
        if (!versionLog) { throw std::runtime_error("AkkEngine: version log is disabled"); }
        if (usesPartitionRaft()) {
            if (target.clusterId != clusterConfig.clusterId() || target.timelineId != 1 || target.watermarks.size() != clusterConfig.partition().
                partitionCount) { throw std::invalid_argument("AkkEngine: checkpoint belongs to another partition layout or timeline"); }
            std::map<uint64_t, uint64_t> streams;
            for (const auto& revision : target.watermarks) {
                if (revision.streamId == 0 || revision.streamId > clusterConfig.partition().partitionCount || !streams.emplace(
                    revision.streamId,
                    revision.seq
                ).second) { throw std::invalid_argument("AkkEngine: checkpoint contains unknown or duplicate partition streams"); }
            }
            const auto current = createClusterCheckpoint();
            for (const auto& revision : current.watermarks) {
                if (streams.at(revision.streamId) > revision.seq) { throw std::invalid_argument("AkkEngine: checkpoint is ahead of a current stream"); }
            }
            RollbackResult result;
            crypto::secureRandom(result.operationId);
            for (const auto& [stream, sequence] : streams) {
                auto part = partitionRollback(stream - 1, sequence, options, true);
                result.items.append_range(part.items | std::views::as_rvalue);
            }
            return result;
        }
        if (target.clusterId != clusterConfig.clusterId() || (clusterReplicationMode != cluster::ReplicationMode::STRIPE && target.configEpoch !=
            clusterRuntime->configurationEpoch()) || target.timelineId != 1) {
            throw std::invalid_argument("AkkEngine: checkpoint belongs to a different cluster configuration or timeline");
        }
        std::unordered_map<uint64_t, uint64_t> watermarks;
        for (const auto& revision : target.watermarks) {
            if (!watermarks.emplace(revision.streamId, revision.seq).second) {
                throw std::invalid_argument("AkkEngine: checkpoint contains duplicate streams");
            }
        }
        std::vector<std::pair<uint64_t, uint64_t>> requests;
        if (clusterReplicationMode == cluster::ReplicationMode::PARTITIONED) {
            for (const auto& node : clusterConfig.dataNodes()) {
                const auto it = watermarks.find(node.nodeId);
                if (it == watermarks.end()) { throw std::invalid_argument("AkkEngine: checkpoint is missing a partition stream"); }
                requests.emplace_back(node.nodeId, it->second);
            }
            if (watermarks.size() != requests.size()) { throw std::invalid_argument("AkkEngine: checkpoint contains unknown streams"); }
        }
        else {
            const auto it = watermarks.find(0);
            if (it == watermarks.end() || watermarks.size() != 1) {
                throw std::invalid_argument("AkkEngine: checkpoint must contain exactly the global stream");
            }
            uint64_t controlNode = nodeId;
            if (clusterReplicationMode == cluster::ReplicationMode::STRIPE) {
                controlNode = clusterRuntime->stripeMetadataLeaderNodeId();
                if (controlNode == 0) { throw std::runtime_error("AkkEngine: STRIPE metadata leader is unavailable"); }
            }
            else if (clusterRuntime->role() != cluster::NodeRole::PRIMARY) {
                throw std::runtime_error("AkkEngine: rollback must be submitted to the cluster leader");
            }
            requests.emplace_back(controlNode, it->second);
        }
        RollbackResult result;
        crypto::secureRandom(result.operationId);
        struct PlannedStreamRollback {
            uint64_t controlNode = 0;
            uint64_t targetWatermark = 0;
            uint64_t plannedWatermark = 0;
        };
        std::vector<PlannedStreamRollback> plannedRequests;
        plannedRequests.reserve(requests.size());
        for (const auto& [controlNode, targetWatermark] : requests) {
            cluster::StripeControlRequest watermarkRequest;
            watermarkRequest.action = cluster::StripeControlAction::ROLLBACK_WATERMARK;
            watermarkRequest.ownerNodeId = controlNode;
            const auto watermarkResponse = clusterRuntime->rollbackControl(controlNode, std::move(watermarkRequest));
            if (watermarkResponse.status != cluster::StripeControlStatus::COMMITTED) {
                throw std::runtime_error(
                    "AkkEngine: rollback planning watermark request failed with status " + std::to_string(static_cast<unsigned>(watermarkResponse.status))
                );
            }
            if (targetWatermark > watermarkResponse.fenceToken) { throw std::invalid_argument("AkkEngine: checkpoint is ahead of a current stream watermark"); }
            plannedRequests.push_back(
                PlannedStreamRollback{.controlNode = controlNode, .targetWatermark = targetWatermark, .plannedWatermark = watermarkResponse.fenceToken,}
            );
        }
        for (const auto& planned : plannedRequests) {
            std::optional<std::array<uint8_t, 16>> deferredTask;
            if (options.execution != RollbackExecutionMode::IMMEDIATE) {
                deferredTask = scheduleDeferredRollback(
                    result.operationId,
                    cluster::StripeControlAction::ROLLBACK_STREAM,
                    true,
                    clusterReplicationMode == cluster::ReplicationMode::STRIPE ? 0 : planned.controlNode,
                    planned.targetWatermark,
                    planned.plannedWatermark,
                    options.conflict
                );
                if (options.execution == RollbackExecutionMode::NEXT_STARTUP) {
                    result.items.push_back(
                        RollbackItemResult{
                            .key = {},
                            .status = RollbackItemStatus::DEFERRED,
                            .message = "stream " + std::to_string(planned.controlNode) + " is scheduled for the next startup",
                        }
                    );
                    continue;
                }
            }
            try {
                std::vector<uint8_t> cursor;
                for (;;) {
                    cluster::StripeControlRequest request;
                    request.action = cluster::StripeControlAction::ROLLBACK_STREAM;
                    request.ownerNodeId = planned.controlNode;
                    request.fenceToken = planned.targetWatermark;
                    request.key = cursor;
                    request.metadata = {std::to_underlying(RollbackExecutionMode::IMMEDIATE), std::to_underlying(options.conflict),};
                    if (deferredTask) { pushU64(request.metadata, planned.plannedWatermark); }
                    const auto response = clusterRuntime->rollbackControl(planned.controlNode, std::move(request));
                    if (response.status != cluster::StripeControlStatus::COMMITTED) {
                        throw std::runtime_error("AkkEngine: cluster rollback stream request was rejected");
                    }
                    auto batch = decodeRollbackStreamBatch(response.metadata);
                    result.items.append_range(batch.items | std::views::as_rvalue);
                    if (batch.done) { break; }
                    cursor = std::move(batch.nextCursor);
                }
                if (deferredTask) { completeDeferredRollback(*deferredTask); }
            }
            catch (const std::exception& error) {
                if (!deferredTask) { throw; }
                result.items.push_back(RollbackItemResult{.key = {}, .status = RollbackItemStatus::DEFERRED, .message = error.what()});
            }
        }
        return result;
    }
}

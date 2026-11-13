/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/AkkEngineRequests.cpp
#include "detail/AkkEngineImpl.hpp"

namespace akkaradb::engine {
    cluster::ClusterRequestId AkkEngine::Impl::newRequestId(uint64_t retentionMs) {
        OperationGuard operation{*this};
        const auto& options = opts.cluster.runtime.requests;
        if (!clusterRuntime || !options.enabled) { throw std::logic_error("AkkEngine: retry-safe requests are disabled"); }
        if (retentionMs == 0) { retentionMs = options.defaultRetentionMs; }
        if (retentionMs > options.maxRetentionMs) { throw std::invalid_argument("AkkEngine: request retention exceeds configured maximum"); }
        cluster::ClusterRequestId id;
        crypto::secureRandom(id.nonce);
        id.expiresAtUnixMs = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).
            count() + retentionMs;
        return id;
    }

    cluster::ClusterRequestResult AkkEngine::Impl::putWithRequest(const cluster::ClusterRequestId& id, std::span<const uint8_t> key, std::span<const uint8_t> value) {
        OperationGuard operation{*this};
        throwIfBackgroundFailed();
        waitForVersionLogRecovery();
        try {
            if (auto forwarded = tryForwardPoint(cluster::ForwardOperation::PUT_REQUEST, key, value, id)) { return forwarded->requestResult; }
            std::shared_lock epochLock{mutationEpochMu};
            return writeCoordinator->writeWithRequest(id, cluster::ReplOpType::PUT, key, value);
        }
        catch (cluster::ClusterRoutingError& error) {
            error.requestId = id;
            throw;
        }
    }

    cluster::ClusterRequestResult AkkEngine::Impl::removeWithRequest(const cluster::ClusterRequestId& id, std::span<const uint8_t> key) {
        OperationGuard operation{*this};
        throwIfBackgroundFailed();
        waitForVersionLogRecovery();
        try {
            if (auto forwarded = tryForwardPoint(cluster::ForwardOperation::REMOVE_REQUEST, key, {}, id)) { return forwarded->requestResult; }
            std::shared_lock epochLock{mutationEpochMu};
            return writeCoordinator->writeWithRequest(id, cluster::ReplOpType::REMOVE, key, {});
        }
        catch (cluster::ClusterRoutingError& error) {
            error.requestId = id;
            throw;
        }
    }

    cluster::ClusterRequestResult AkkEngine::Impl::queryLocalRequestResult(const cluster::ClusterRequestId& id) {
        OperationGuard operation{*this};
        throwIfBackgroundFailed();
        std::lock_guard lock{writeMu};
        return queryLocalRequest(id);
    }

    cluster::ClusterRequestResult AkkEngine::Impl::queryRequest(const cluster::ClusterRequestId& id) {
        OperationGuard operation{*this};
        if (!clusterRuntime) { throw std::logic_error("AkkEngine: retry-safe requests require an enabled cluster runtime"); }
        if (clusterReplicationMode == cluster::ReplicationMode::PARTITIONED) {
            throw std::invalid_argument("AkkEngine: PARTITIONED queryRequest requires the original key");
        }
        if (!clusterConfig.usesDataConsensus()) { return clusterRuntime->queryRequest(id); }
        cluster::ForwardRequest request;
        request.operation = cluster::ForwardOperation::QUERY_REQUEST;
        request.deduplicationId = id;
        request.entries.emplace_back();
        if (auto forwarded = tryForward(std::move(request))) { return forwarded->requestResult; }
        return clusterRuntime->queryRequest(id);
    }

    cluster::ClusterRequestResult AkkEngine::Impl::queryRequest(const cluster::ClusterRequestId& id, std::span<const uint8_t> key) {
        OperationGuard operation{*this};
        if (!clusterRuntime) { throw std::logic_error("AkkEngine: retry-safe requests require an enabled cluster runtime"); }
        if (!clusterConfig.usesDataConsensus()) { return clusterRuntime->queryRequest(id); }
        if (auto forwarded = tryForwardPoint(cluster::ForwardOperation::QUERY_REQUEST, key, {}, id)) { return forwarded->requestResult; }
        return usesPartitionRaft() ? requireLocalPartition(key).impl_->clusterRuntime->queryRequest(id) : clusterRuntime->queryRequest(id);
    }
}

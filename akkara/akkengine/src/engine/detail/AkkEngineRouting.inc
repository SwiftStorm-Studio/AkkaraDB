/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/detail/AkkEngineRouting.inc
// Included inside AkkEngine::Impl by AkkEngineImpl.hpp.
[[nodiscard]] cluster::ReadResponse readLocalForCluster(std::span<const uint8_t> key, uint64_t requestedSnapshot) {
    if (usesPartitionRaft()) {
        auto* child = localPartition(key);
        if (!child) {
            cluster::ReadResponse response;
            response.status = cluster::ReadStatus::NOT_FOUND;
            return response;
        }
        if (opts.cluster.runtime.readMode == cluster::ClusterReadMode::OWNER_LINEARIZABLE) {
            return child->impl_->clusterRuntime->linearizableReadKey(key);
        }
        return child->impl_->readLocalForCluster(key, requestedSnapshot);
    }
    if (clusterReplicationMode == cluster::ReplicationMode::STRIPE) {
        cluster::ReadResponse response;
        const auto value = readStripeValue(key);
        response.seq = snapshotSeq();
        if (!value) {
            response.status = cluster::ReadStatus::NOT_FOUND;
            return response;
        }
        response.status = cluster::ReadStatus::FOUND;
        response.value = *value;
        return response;
    }
    const auto snapshot = requestedSnapshot == 0 ? std::optional<uint64_t>{} : std::optional<uint64_t>{requestedSnapshot};
    const auto value = getValueInternal(key, false, snapshot);
    cluster::ReadResponse response;
    response.seq = requestedSnapshot == 0 ? snapshotSeq() : requestedSnapshot;
    if (!value) {
        response.status = cluster::ReadStatus::NOT_FOUND;
        return response;
    }
    response.status = cluster::ReadStatus::FOUND;
    response.value = *value;
    return response;
}

[[nodiscard]] cluster::ReadResponse readLocalStorageForCluster(std::span<const uint8_t> key, uint64_t requestedSnapshot) {
    if (clusterReplicationMode != cluster::ReplicationMode::STRIPE) { return readLocalForCluster(key, requestedSnapshot); }
    if (isStripeInternalKey(key) && key[4] == 'M') {
        checkStripeHealthy();
        if (!clusterRuntime || clusterRuntime->stripeMetadataLeaderNodeId() != nodeId) {
            throw std::runtime_error("AkkEngine: STRIPE metadata must be read from its authority node");
        }
    }
    const auto snapshot = requestedSnapshot == 0 ? std::optional<uint64_t>{} : std::optional<uint64_t>{requestedSnapshot};
    const auto value = getValueInternal(key, false, snapshot);
    cluster::ReadResponse response;
    response.seq = requestedSnapshot == 0 ? snapshotSeq() : requestedSnapshot;
    response.status = value ? cluster::ReadStatus::FOUND : cluster::ReadStatus::NOT_FOUND;
    if (value) { response.value = *value; }
    return response;
}

[[nodiscard]] bool clusterReadMustRoute() const noexcept {
    return clusterRuntime && (clusterReplicationMode == cluster::ReplicationMode::STRIPE || opts.cluster.runtime.readMode !=
        cluster::ClusterReadMode::LOCAL_STALE_OK);
}

void requireClusterDisabledOperation(std::string_view operation) const {
    if (clusterRuntime) { throw std::runtime_error("AkkEngine: " + std::string{operation} + " is not supported while cluster runtime is enabled"); }
}

[[nodiscard]] std::optional<std::vector<uint8_t>> getValueClusterAware(std::span<const uint8_t> key, bool countStats = true) {
    if (!clusterReadMustRoute()) { return getValueInternal(key, countStats); }
    const bool localStripeCoordinator = clusterReplicationMode == cluster::ReplicationMode::STRIPE && opts.cluster.runtime.stripeReadCoordinatorMode
        == cluster::StripeReadCoordinatorMode::LOCAL_COORDINATOR && opts.cluster.runtime.readMode != cluster::ClusterReadMode::OWNER_ONLY;
    if (!localStripeCoordinator) {
        if (auto forwarded = tryForwardPoint(
            cluster::ForwardOperation::GET,
            key,
            {},
            {},
            opts.cluster.runtime.readMode != cluster::ClusterReadMode::OWNER_ONLY
        )) {
            if (countStats) {
                getsTotal.fetch_add(1, std::memory_order_relaxed);
                if (!forwarded->found) { getsMiss.fetch_add(1, std::memory_order_relaxed); }
            }
            if (!forwarded->found) { return std::nullopt; }
            return std::move(forwarded->value);
        }
    }
    if (countStats) { getsTotal.fetch_add(1, std::memory_order_relaxed); }
    if (clusterReplicationMode == cluster::ReplicationMode::STRIPE) {
        if (opts.cluster.runtime.readMode == cluster::ClusterReadMode::OWNER_ONLY && !clusterRuntime->ownsWriteKey(key)) {
            throw std::runtime_error("AkkEngine: cluster owner read failed");
        }
        std::optional<std::vector<uint8_t>> value;
        if (opts.cluster.runtime.stripeReadCoordinatorMode == cluster::StripeReadCoordinatorMode::LOCAL_COORDINATOR) {
            value = readStripeValue(key);
        }
        else {
            const auto response = clusterRuntime->readKey(key, 0);
            if (response.status == cluster::ReadStatus::FOUND) { value = response.value; }
            else if (response.status != cluster::ReadStatus::NOT_FOUND) { throw std::runtime_error("AkkEngine: cluster owner read failed"); }
        }
        if (!value) {
            if (countStats) { getsMiss.fetch_add(1, std::memory_order_relaxed); }
            return std::nullopt;
        }
        if (countStats) { getsMemtableHit.fetch_add(1, std::memory_order_relaxed); }
        return value;
    }
    const auto response = clusterRuntime->readKey(key, 0);
    if (response.status == cluster::ReadStatus::NOT_FOUND) {
        if (countStats) { getsMiss.fetch_add(1, std::memory_order_relaxed); }
        return std::nullopt;
    }
    if (response.status != cluster::ReadStatus::FOUND) { throw std::runtime_error("AkkEngine: cluster owner read failed"); }
    if (countStats) { getsMemtableHit.fetch_add(1, std::memory_order_relaxed); }
    return response.value;
}

[[nodiscard]] bool getIntoClusterAware(std::span<const uint8_t> key, std::vector<uint8_t>& out) {
    auto value = getValueClusterAware(key);
    if (!value) { return false; }
    out = std::move(*value);
    return true;
}

void requireOwnsWriteKey(std::span<const uint8_t> key) const {
    if (clusterRuntime && !clusterRuntime->ownsWriteKey(key)) {
        throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::NOT_OWNER, clusterRuntime->routeTarget(key));
    }
}

template <typename EntryRange>
[[nodiscard]] std::optional<cluster::ClusterRouteTarget> resolveForwardEntries(
    const EntryRange& entries,
    bool allowForward,
    bool queryRequest = false
) {
    if (!clusterRuntime) { return std::nullopt; }
    cluster::ClusterRouteTarget target;
    uint64_t destination = 0;
    const auto inspect = [&](std::span<const uint8_t> key) {
        const bool local = clusterRuntime->ownsWriteKey(key);
        auto candidate = clusterRuntime->routeTarget(key);
        const uint64_t owner = local ? nodeId : candidate.nodeId;
        if (owner == 0 || (!local && owner == nodeId)) {
            throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::NO_TARGET, candidate, "No writable owner is currently available");
        }
        if (destination != 0 && destination != owner) {
            throw cluster::ClusterRoutingError(
                cluster::ClusterRoutingCode::CROSS_OWNER_BATCH,
                {},
                "Batch spans multiple owners; no entries were applied and the batch was not split"
            );
        }
        destination = owner;
        target = std::move(candidate);
    };
    if (queryRequest) { inspect({}); }
    else { for (const auto& entry : entries) { inspect(entry.key); } }
    if (destination == 0 || destination == nodeId) { return std::nullopt; }
    if (allowForward && opts.cluster.runtime.routingMode == cluster::ClusterRoutingMode::LOCAL_ONLY) {
        throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::LOCAL_ONLY, {}, "This node accepts local-owner operations only");
    }
    if (!allowForward || opts.cluster.runtime.routingMode != cluster::ClusterRoutingMode::FORWARD) {
        throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::NOT_OWNER, target);
    }
    return target;
}

[[nodiscard]] std::optional<cluster::ClusterRouteTarget> resolveForwardTarget(const cluster::ForwardRequest& request, bool allowForward) {
    return resolveForwardEntries(
        request.entries,
        allowForward,
        request.operation == cluster::ForwardOperation::QUERY_REQUEST && clusterReplicationMode != cluster::ReplicationMode::PARTITIONED
    );
}

[[nodiscard]] std::optional<cluster::ForwardResponse> tryForward(cluster::ForwardRequest request, bool allowForward = true) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(opts.cluster.runtime.forwardingTimeoutMs);
    const auto resolved = resolveForwardTarget(request, allowForward);
    if (!resolved) { return std::nullopt; }
    const auto& target = *resolved;
    if (request.entries.size() > 65536) { throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::PAYLOAD_TOO_LARGE, target); }
    size_t payloadSize = cluster::FORWARD_REQUEST_BASE_SIZE;
    for (const auto& entry : request.entries) {
        if (entry.key.size() > cluster::MAX_FORWARD_PAYLOAD || entry.value.size() > cluster::MAX_FORWARD_PAYLOAD) {
            throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::PAYLOAD_TOO_LARGE, target);
        }
        payloadSize += 8 + entry.key.size() + entry.value.size();
        if (payloadSize > cluster::MAX_FORWARD_PAYLOAD) {
            throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::PAYLOAD_TOO_LARGE, target);
        }
    }
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now()).count();
    if (remaining <= 0) { throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::FORWARD_UNAVAILABLE, target); }
    request.timeoutMs = remaining;
    const std::optional<uint16_t> partition = usesPartitionRaft() && !request.entries.empty()
                                                  ? std::optional{partitionIndex(request.entries.front().key)}
                                                  : std::nullopt;
    try {
        auto response = clusterRuntime->forwardTo(target.nodeId, std::move(request));
        if (!response.success) {
            throw cluster::ClusterRoutingError(
                response.errorCode,
                response.target.nodeId == 0 ? target : response.target,
                response.message.empty() ? "Forward operation failed" : response.message
            );
        }
        return response;
    }
    catch (cluster::ClusterRoutingError& error) {
        if (partition) {
            auto& hint = partitionLeaderHints[*partition];
            if (error.code == cluster::ClusterRoutingCode::NOT_OWNER) { hint.store(error.target.nodeId, std::memory_order_release); }
            else if (error.code == cluster::ClusterRoutingCode::FORWARD_UNAVAILABLE) { hint.store(0, std::memory_order_release); }
        }
        if (error.target.nodeId == 0) { error.target = target; }
        throw;
    }
}

[[nodiscard]] std::optional<cluster::ForwardResponse> tryForwardPoint(
    cluster::ForwardOperation operation,
    std::span<const uint8_t> key,
    std::span<const uint8_t> value = {},
    const cluster::ClusterRequestId& requestId = {},
    bool allowForward = true
) {
    if (!clusterRuntime || clusterRuntime->ownsWriteKey(key)) { return std::nullopt; }
    if (usesPartitionRaft() && opts.cluster.runtime.routingMode == cluster::ClusterRoutingMode::FORWARD && currentPartitionLeader(key) == 0) {
        const auto index = partitionIndex(key);
        const auto placement = livePlacement.load();
        const auto response = partitionRpcLeader(
            index,
            partitionAdminHeader(PartitionAdmin::STATUS, index, 0),
            *placement,
            std::chrono::steady_clock::now() + std::chrono::milliseconds{opts.cluster.runtime.forwardingTimeoutMs}
        );
        partitionLeaderHints[index].store(response.target.nodeId, std::memory_order_release);
    }
    const bool forwarding = allowForward && opts.cluster.runtime.routingMode == cluster::ClusterRoutingMode::FORWARD;
    if (forwarding && (key.size() > cluster::MAX_FORWARD_PAYLOAD || value.size() > cluster::MAX_FORWARD_PAYLOAD || key.size() + value.size() +
        cluster::FORWARD_REQUEST_BASE_SIZE + 8 > cluster::MAX_FORWARD_PAYLOAD)) {
        throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::PAYLOAD_TOO_LARGE, clusterRuntime->routeTarget(key));
    }
    cluster::ForwardRequest request;
    request.operation = operation;
    request.deduplicationId = requestId;
    request.entries.push_back({{key.begin(), key.end()}, {}});
    if (forwarding) { request.entries.back().value.assign_range(value); }
    return tryForward(std::move(request), allowForward);
}

cluster::ForwardResponse receiveForward(const cluster::ForwardRequest& request) {
    OperationGuard operation{*this};
    throwIfBackgroundFailed();
    // Runtime starts before coordinator installation. Startup/close
    // never acknowledges a forwarded operation that it did not run.
    if (!forwardReady.load(std::memory_order_acquire)) { throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::FORWARD_UNAVAILABLE); }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(request.timeoutMs);
    waitForVersionLogRecovery();
    if (std::chrono::steady_clock::now() >= deadline) { throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::FORWARD_UNAVAILABLE); }
    if (request.operation == cluster::ForwardOperation::READ_QUERY) { return receiveReadQuery(request); }
    if (request.operation == cluster::ForwardOperation::CLUSTER_ADMIN) { return receivePartitionAdmin(request); }
    if (request.operation == cluster::ForwardOperation::TRANSACTION) { return receiveClusterTransaction(request); }
    // Destination revalidates every key before taking a fencing grant
    // or mutating storage. It never follows another redirect.
    (void)resolveForwardTarget(request, false);
    cluster::ForwardResponse response;
    response.success = true;
    if (request.operation == cluster::ForwardOperation::QUERY_REQUEST) {
        response.requestResult = usesPartitionRaft()
                                     ? requireLocalPartition(request.entries.front().key).impl_->clusterRuntime->queryRequest(
                                         request.deduplicationId
                                     )
                                     : clusterRuntime->queryRequest(request.deduplicationId);
    }
    else if (request.operation == cluster::ForwardOperation::GET) {
        const auto result = clusterRuntime->linearizableReadKey(request.entries.front().key);
        if (result.status == cluster::ReadStatus::ERROR_STATUS) { throw std::runtime_error("Forwarded owner read failed"); }
        response.found = result.status == cluster::ReadStatus::FOUND;
        response.value = result.value;
    }
    else {
        std::shared_lock epochLock{mutationEpochMu};
        if (std::chrono::steady_clock::now() >= deadline) { throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::FORWARD_UNAVAILABLE); }
        if (request.operation == cluster::ForwardOperation::PUT_BATCH) {
            std::vector<BatchPutEntry> entries;
            for (const auto& entry : request.entries) { entries.push_back({entry.key, entry.value}); }
            writeCoordinator->putBatch(entries);
        }
        else {
            const auto& entry = request.entries.front();
            switch (request.operation) {
                case cluster::ForwardOperation::PUT: writeCoordinator->put(entry.key, entry.value);
                    break;
                case cluster::ForwardOperation::REMOVE: writeCoordinator->remove(entry.key);
                    break;
                case cluster::ForwardOperation::PUT_REQUEST:
                case cluster::ForwardOperation::REMOVE_REQUEST: response.requestResult = writeCoordinator->writeWithRequest(
                        request.deduplicationId,
                        request.operation == cluster::ForwardOperation::PUT_REQUEST ? cluster::ReplOpType::PUT : cluster::ReplOpType::REMOVE,
                        entry.key,
                        entry.value
                    );
                    break;
                default: throw std::invalid_argument("Unsupported forward operation");
            }
        }
    }
    return response;
}

#include "ClusterQueries.inc"
#include "AkkTransactionState.inc"
#include "ClusterTransactionState.inc"
#include "ClusterTransactions.inc"
#include "LocalRequestApplication.inc"

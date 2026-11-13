/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/AkkEngineWrite.cpp
#include "detail/AkkEngineImpl.hpp"

namespace akkaradb::engine {
    void detail::ProtocolBulkWriter::put(AkkEngine& engine, std::span<const BulkPutEntry> entries) {
        engine.applyProtocolBulk(entries);
    }

    void AkkEngine::Impl::put(std::span<const uint8_t> key, std::span<const uint8_t> value) {
        OperationGuard operation{*this};
        if (clusterRuntime && opts.cluster.runtime.requests.enabled) {
            const auto id = newRequestId();
            const auto result = putWithRequest(id, key, value);
            if (result.status != cluster::ClusterRequestStatus::APPLIED) {
                throw cluster::ClusterRoutingError(
                    cluster::ClusterRoutingCode::OUTCOME_UNKNOWN,
                    {},
                    "AkkEngine: request is pending or expired; query the original ID before considering another write",
                    id
                );
            }
            return;
        }
        if (clusterRuntime) { throwIfBackgroundFailed(); }
        waitForVersionLogRecovery();
        if (tryForwardPoint(cluster::ForwardOperation::PUT, key, value)) { return; }
        std::shared_lock epochLock{mutationEpochMu};
        writeCoordinator->put(key, value);
    }

    void AkkEngine::Impl::putHinted(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t fp64, uint64_t miniKey) {
        OperationGuard operation{*this};
        if (clusterRuntime && opts.cluster.runtime.requests.enabled) {
            const auto id = newRequestId();
            const auto result = putWithRequest(id, key, value);
            if (result.status != cluster::ClusterRequestStatus::APPLIED) {
                throw cluster::ClusterRoutingError(
                    cluster::ClusterRoutingCode::OUTCOME_UNKNOWN,
                    {},
                    "AkkEngine: request is pending or expired; query the original ID before considering another write",
                    id
                );
            }
            return;
        }
        if (clusterRuntime) { throwIfBackgroundFailed(); }
        waitForVersionLogRecovery();
        if (tryForwardPoint(cluster::ForwardOperation::PUT, key, value)) { return; }
        std::shared_lock epochLock{mutationEpochMu};
        writeCoordinator->putHinted(key, value, fp64, miniKey);
    }

    void AkkEngine::Impl::applyProtocolBulk(std::span<const detail::BulkPutEntry> entries) {
        OperationGuard operation{*this};
        if (entries.empty()) {
            throwIfBackgroundFailed();
            return;
        }
        if (clusterRuntime) { throwIfBackgroundFailed(); }
        waitForVersionLogRecovery();
        if (clusterRuntime && std::ranges::any_of(
            entries,
            [&](const detail::BulkPutEntry& entry) { return !clusterRuntime->ownsWriteKey(entry.key); }
        )) {
            if (const auto target = resolveForwardEntries(entries, true)) {
                size_t payloadSize = cluster::FORWARD_REQUEST_BASE_SIZE;
                if (entries.size() > 65536) { throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::PAYLOAD_TOO_LARGE, *target); }
                for (const auto& entry : entries) {
                    if (entry.key.size() > cluster::MAX_FORWARD_PAYLOAD || entry.value.size() > cluster::MAX_FORWARD_PAYLOAD) {
                        throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::PAYLOAD_TOO_LARGE, *target);
                    }
                    payloadSize += 8 + entry.key.size() + entry.value.size();
                    if (payloadSize > cluster::MAX_FORWARD_PAYLOAD) {
                        throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::PAYLOAD_TOO_LARGE, *target);
                    }
                }
                cluster::ForwardRequest request;
                request.operation = cluster::ForwardOperation::PUT_BATCH;
                request.entries.reserve(entries.size());
                for (const auto& entry : entries) {
                    request.entries.push_back({{entry.key.begin(), entry.key.end()}, {entry.value.begin(), entry.value.end()}});
                }
                if (tryForward(std::move(request))) { return; }
            }
        }
        std::shared_lock epochLock{mutationEpochMu};
        writeCoordinator->putBatch(entries);
    }

    void AkkEngine::Impl::remove(std::span<const uint8_t> key) {
        OperationGuard operation{*this};
        if (clusterRuntime && opts.cluster.runtime.requests.enabled) {
            const auto id = newRequestId();
            const auto result = removeWithRequest(id, key);
            if (result.status != cluster::ClusterRequestStatus::APPLIED) {
                throw cluster::ClusterRoutingError(
                    cluster::ClusterRoutingCode::OUTCOME_UNKNOWN,
                    {},
                    "AkkEngine: request is pending or expired; query the original ID before considering another write",
                    id
                );
            }
            return;
        }
        if (clusterRuntime) { throwIfBackgroundFailed(); }
        waitForVersionLogRecovery();
        if (tryForwardPoint(cluster::ForwardOperation::REMOVE, key)) { return; }
        std::shared_lock epochLock{mutationEpochMu};
        writeCoordinator->remove(key);
    }

    void AkkEngine::Impl::removeHinted(std::span<const uint8_t> key, uint64_t fp64, uint64_t miniKey) {
        OperationGuard operation{*this};
        if (clusterRuntime && opts.cluster.runtime.requests.enabled) {
            const auto id = newRequestId();
            const auto result = removeWithRequest(id, key);
            if (result.status != cluster::ClusterRequestStatus::APPLIED) {
                throw cluster::ClusterRoutingError(
                    cluster::ClusterRoutingCode::OUTCOME_UNKNOWN,
                    {},
                    "AkkEngine: request is pending or expired; query the original ID before considering another write",
                    id
                );
            }
            return;
        }
        if (clusterRuntime) { throwIfBackgroundFailed(); }
        waitForVersionLogRecovery();
        if (tryForwardPoint(cluster::ForwardOperation::REMOVE, key)) { return; }
        std::shared_lock epochLock{mutationEpochMu};
        writeCoordinator->removeHinted(key, fp64, miniKey);
    }
}

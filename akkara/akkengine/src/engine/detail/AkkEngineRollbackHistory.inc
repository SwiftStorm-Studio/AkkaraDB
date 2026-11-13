/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/detail/AkkEngineRollbackHistory.inc
// Included inside AkkEngine::Impl by AkkEngineImpl.hpp.
[[nodiscard]] std::optional<StripeMetadata> stripeMetadataAt(std::span<const uint8_t> publicKey, uint64_t logicalSeq) {
    std::optional<StripeMetadata> selected;
    if (!versionLog) { return selected; }
    for (const auto& entry : versionLog->history(stripeMetaKey(publicKey))) {
        auto metadata = decodeStripeMetadata(entry.value);
        if (metadata) { *metadata = effectiveStripeMetadata(publicKey, *metadata); }
        if (!metadata || metadata->logicalSeq == 0 || metadata->logicalSeq > logicalSeq) { continue; }
        if (!selected || selected->logicalSeq <= metadata->logicalSeq) { selected = std::move(metadata); }
    }
    return selected;
}

[[nodiscard]] bool stripeHistoryUnavailableBefore(std::span<const uint8_t> publicKey, uint64_t logicalSeq) const {
    if (!versionLog) { return true; }
    for (const auto& entry : versionLog->history(stripeMetaKey(publicKey))) {
        const auto metadata = decodeStripeMetadata(entry.value);
        if (metadata && metadata->logicalSeq > logicalSeq && (entry.flags & vlog::VLOG_FLAG_RETENTION_BASE) != 0) { return true; }
    }
    return false;
}

[[nodiscard]] RollbackItemResult applyMaterializedRollback(
    std::span<const uint8_t> key,
    std::span<const uint8_t> value,
    bool tombstone,
    uint64_t expectedHead,
    RollbackConflictPolicy conflict
) {
    RollbackItemResult result{.key = {key.begin(), key.end()}, .message = {}};
    uint64_t currentHead = 0;
    if (clusterReplicationMode == cluster::ReplicationMode::STRIPE) {
        if (auto current = readStripeMetadata(key)) { currentHead = current->logicalSeq; }
    }
    else if (versionLog) { if (const auto head = versionLog->getAt(key, UINT64_MAX)) { currentHead = head->seq; } }
    if (conflict == RollbackConflictPolicy::FAIL_IF_CHANGED && expectedHead != 0 && currentHead != expectedHead) {
        result.status = RollbackItemStatus::CONFLICT;
        result.message = "key changed after rollback planning";
        return result;
    }
    writeCoordinator->rollback(key, value, tombstone);
    result.status = RollbackItemStatus::APPLIED;
    return result;
}

[[nodiscard]] RollbackItemResult rollbackKeyLocal(
    std::span<const uint8_t> key,
    uint64_t targetSeq,
    RollbackConflictPolicy conflict,
    uint64_t expectedHead = 0,
    uint64_t maximumAllowedHead = UINT64_MAX
) {
    if (!versionLog) {
        return RollbackItemResult{
            .key = {key.begin(), key.end()},
            .status = RollbackItemStatus::PERMANENT_FAILURE,
            .message = "version log is disabled",
        };
    }
    if (clusterReplicationMode == cluster::ReplicationMode::STRIPE) {
        const auto current = readStripeMetadata(key);
        const uint64_t currentHead = current ? current->logicalSeq : 0;
        if (conflict == RollbackConflictPolicy::FAIL_IF_CHANGED && currentHead > maximumAllowedHead) {
            return RollbackItemResult{
                .key = {key.begin(), key.end()},
                .status = RollbackItemStatus::CONFLICT,
                .message = "key changed after rollback was deferred",
            };
        }
        if (expectedHead == 0) { expectedHead = currentHead; }
        if (currentHead <= targetSeq) {
            return RollbackItemResult{.key = {key.begin(), key.end()}, .status = RollbackItemStatus::APPLIED, .message = {}};
        }
        const auto target = stripeMetadataAt(key, targetSeq);
        if (!target && stripeHistoryUnavailableBefore(key, targetSeq)) {
            return RollbackItemResult{
                .key = {key.begin(), key.end()},
                .status = RollbackItemStatus::PERMANENT_FAILURE,
                .message = "target revision predates retained STRIPE metadata history",
            };
        }
        if (!target || target->tombstone) { return applyMaterializedRollback(key, {}, true, expectedHead, conflict); }
        try {
            const auto value = readStripeValueFromMetadata(key, *target, false);
            if (!value) { throw std::runtime_error("AkkEngine: historical STRIPE value is absent"); }
            return applyMaterializedRollback(key, *value, false, expectedHead, conflict);
        }
        catch (const std::exception& error) {
            return RollbackItemResult{.key = {key.begin(), key.end()}, .status = RollbackItemStatus::PERMANENT_FAILURE, .message = error.what(),};
        }
    }

    const auto head = versionLog->getAt(key, UINT64_MAX);
    const uint64_t currentHead = head ? head->seq : 0;
    if (conflict == RollbackConflictPolicy::FAIL_IF_CHANGED && currentHead > maximumAllowedHead) {
        return RollbackItemResult{
            .key = {key.begin(), key.end()},
            .status = RollbackItemStatus::CONFLICT,
            .message = "key changed after rollback was deferred",
        };
    }
    if (expectedHead == 0) { expectedHead = currentHead; }
    if (currentHead <= targetSeq) {
        return RollbackItemResult{.key = {key.begin(), key.end()}, .status = RollbackItemStatus::APPLIED, .message = {}};
    }
    const auto target = versionLog->getAt(key, targetSeq);
    if (!target || (target->flags & MemHdr16::FLAG_TOMBSTONE) != 0) {
        auto history = versionLog->history(key);
        const auto first = history.begin();
        if (first != std::default_sentinel && first->seq > targetSeq && (first->flags & vlog::VLOG_FLAG_RETENTION_BASE) != 0) {
            return RollbackItemResult{
                .key = {key.begin(), key.end()},
                .status = RollbackItemStatus::PERMANENT_FAILURE,
                .message = "target revision predates retained history",
            };
        }
        return applyMaterializedRollback(key, {}, true, expectedHead, conflict);
    }
    auto value = resolveValue(target->flags, target->value);
    if (!value) {
        return RollbackItemResult{
            .key = {key.begin(), key.end()},
            .status = RollbackItemStatus::PERMANENT_FAILURE,
            .message = "historical value is unavailable",
        };
    }
    return applyMaterializedRollback(key, *value, false, expectedHead, conflict);
}

[[nodiscard]] RollbackItemResult rollbackStripeCoordinated(
    std::span<const uint8_t> key,
    uint64_t targetSeq,
    RollbackConflictPolicy conflict,
    uint64_t maximumAllowedHead = UINT64_MAX
) {
    const auto current = readStripeMetadata(key);
    const uint64_t expectedHead = current ? current->logicalSeq : 0;
    if (conflict == RollbackConflictPolicy::FAIL_IF_CHANGED && expectedHead > maximumAllowedHead) {
        return RollbackItemResult{
            .key = {key.begin(), key.end()},
            .status = RollbackItemStatus::CONFLICT,
            .message = "key changed after rollback was deferred",
        };
    }
    if (expectedHead <= targetSeq) {
        return RollbackItemResult{.key = {key.begin(), key.end()}, .status = RollbackItemStatus::APPLIED, .message = {}};
    }
    bool tombstone = true;
    std::vector<uint8_t> value;
    const auto target = stripeMetadataAt(key, targetSeq);
    if (!target && stripeHistoryUnavailableBefore(key, targetSeq)) {
        return RollbackItemResult{
            .key = {key.begin(), key.end()},
            .status = RollbackItemStatus::PERMANENT_FAILURE,
            .message = "target revision predates retained STRIPE metadata history",
        };
    }
    if (target && !target->tombstone) {
        try {
            auto historical = readStripeValueFromMetadata(key, *target, false);
            if (!historical) { throw std::runtime_error("AkkEngine: historical STRIPE value is absent"); }
            value = std::move(*historical);
        }
        catch (const std::exception& error) {
            return RollbackItemResult{.key = {key.begin(), key.end()}, .status = RollbackItemStatus::PERMANENT_FAILURE, .message = error.what(),};
        }
        tombstone = false;
    }

    uint64_t authorityNodeId = clusterRuntime->ownerNodeId(key);
    if (!clusterRuntime->stripeNodeReachable(authorityNodeId)) {
        const uint64_t failover = clusterRuntime->stripeFailoverNodeId(key);
        if (failover != 0) { authorityNodeId = failover; }
    }
    cluster::StripeControlRequest request;
    request.action = cluster::StripeControlAction::ROLLBACK_APPLY;
    request.ownerNodeId = authorityNodeId;
    request.key.assign_range(key);
    request.metadata.push_back(std::to_underlying(conflict));
    pushU64(request.metadata, expectedHead);
    request.metadata.push_back(tombstone ? 1U : 0U);
    request.metadata.append_range(value);
    if (authorityNodeId == nodeId) { return applyMaterializedRollback(key, value, tombstone, expectedHead, conflict); }
    const auto response = clusterRuntime->rollbackControl(authorityNodeId, std::move(request));
    if (response.status != cluster::StripeControlStatus::COMMITTED) {
        throw std::runtime_error("AkkEngine: rollback authority rejected the mutation");
    }
    auto items = decodeRollbackItems(response.metadata);
    if (items.size() != 1) { throw std::runtime_error("AkkEngine: invalid rollback authority response"); }
    return std::move(items.front());
}

[[nodiscard]] std::vector<std::vector<uint8_t>> rollbackKeysChangedAfter(uint64_t targetSeq) {
    std::vector<std::vector<uint8_t>> keys;
    if (clusterReplicationMode == cluster::ReplicationMode::STRIPE) {
        auto blobReadPin = blobManager ? blobManager->pinReads() : blob::BlobManager::ReadPin{};
        BufferArena arena;
        memtable::MemTable::KeyRange range;
        auto sources = captureStoredRangeRead(range);
        for (const auto& record : scanGenerator(arena, std::move(sources.memory), std::move(sources.disk), blobManager.get(), std::move(blobReadPin))) {
            auto publicKey = publicKeyFromStripeMetaKey(record.key);
            if (!publicKey) { continue; }
            const auto metadata = decodeStripeMetadata(record.value);
            if (!metadata) { throw std::runtime_error("AkkEngine: corrupt STRIPE metadata history head"); }
            if (metadata->logicalSeq > targetSeq) { keys.push_back(std::move(*publicKey)); }
        }
        std::ranges::sort(keys);
        return keys;
    }
    for (auto& [key, _] : versionLog->collectRollbackTargets(targetSeq)) {
        if (isStripeInternalKey(key)) { continue; }
        if (clusterReplicationMode == cluster::ReplicationMode::PARTITIONED && clusterRuntime && clusterRuntime->ownerNodeId(key) != nodeId) {
            continue;
        }
        keys.push_back(std::move(key));
    }
    std::ranges::sort(keys);
    return keys;
}

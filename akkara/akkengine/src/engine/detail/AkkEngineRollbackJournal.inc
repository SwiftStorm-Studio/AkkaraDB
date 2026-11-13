/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/detail/AkkEngineRollbackJournal.inc
// Included inside AkkEngine::Impl by AkkEngineImpl.hpp.
[[nodiscard]] uint64_t localRollbackWatermark() {
    if (clusterReplicationMode == cluster::ReplicationMode::STRIPE && clusterRuntime) {
        return clusterRuntime->stripeMetadataLinearizableWatermark();
    }
    return snapshotSeq();
}

[[nodiscard]] uint64_t captureRollbackWatermark() {
    std::unique_lock epochLock{mutationEpochMu};
    versionLog->forceSync();
    return localRollbackWatermark();
}

[[nodiscard]] static std::vector<uint8_t> encodeRollbackItems(std::span<const RollbackItemResult> items) {
    std::vector<uint8_t> out{'A', 'K', 'R', 'R', '1'};
    pushU32(out, items.size());
    for (const auto& item : items) {
        out.push_back(std::to_underlying(item.status));
        pushU32(out, item.key.size());
        out.append_range(item.key);
        pushU32(out, item.message.size());
        out.append_range(item.message);
    }
    return out;
}

[[nodiscard]] static std::vector<RollbackItemResult> decodeRollbackItems(std::span<const uint8_t> bytes) {
    if (bytes.size() < 9 || std::memcmp(bytes.data(), "AKRR1", 5) != 0) {
        throw std::runtime_error("AkkEngine: corrupt rollback control response");
    }
    size_t cursor = 5;
    uint32_t count = 0;
    if (!pullU32(bytes, cursor, count)) { throw std::runtime_error("AkkEngine: truncated rollback control response"); }
    if (count > (bytes.size() - cursor) / 9) { throw std::runtime_error("AkkEngine: invalid rollback control result count"); }
    std::vector<RollbackItemResult> out;
    out.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        if (cursor >= bytes.size()) { throw std::runtime_error("AkkEngine: truncated rollback result status"); }
        const auto status = static_cast<RollbackItemStatus>(bytes[cursor++]);
        if (status > RollbackItemStatus::PERMANENT_FAILURE) { throw std::runtime_error("AkkEngine: invalid rollback result status"); }
        uint32_t keySize = 0;
        uint32_t messageSize = 0;
        std::vector<uint8_t> key;
        std::vector<uint8_t> message;
        if (!pullU32(bytes, cursor, keySize) || !pullBytes(bytes, cursor, keySize, key) || !pullU32(bytes, cursor, messageSize) || !pullBytes(
            bytes,
            cursor,
            messageSize,
            message
        )) { throw std::runtime_error("AkkEngine: truncated rollback result item"); }
        out.push_back(RollbackItemResult{.key = std::move(key), .status = status, .message = std::string{message.begin(), message.end()},});
    }
    if (cursor != bytes.size()) { throw std::runtime_error("AkkEngine: trailing rollback control response"); }
    return out;
}

struct RollbackStreamBatch {
    std::vector<RollbackItemResult> items;
    std::vector<uint8_t> nextCursor;
    bool done = true;
};

[[nodiscard]] static std::vector<uint8_t> encodeRollbackStreamBatch(
    std::span<const RollbackItemResult> items,
    std::span<const uint8_t> nextCursor,
    bool done
) {
    std::vector<uint8_t> out{'A', 'K', 'R', 'B', '1'};
    out.push_back(done ? 1U : 0U);
    pushU32(out, nextCursor.size());
    out.append_range(nextCursor);
    auto encodedItems = encodeRollbackItems(items);
    out.append_range(encodedItems);
    return out;
}

[[nodiscard]] static RollbackStreamBatch decodeRollbackStreamBatch(std::span<const uint8_t> bytes) {
    if (bytes.size() < 19 || std::memcmp(bytes.data(), "AKRB1", 5) != 0 || bytes[5] > 1) {
        throw std::runtime_error("AkkEngine: corrupt rollback stream response");
    }
    size_t cursor = 6;
    uint32_t cursorSize = 0;
    RollbackStreamBatch out;
    out.done = bytes[5] != 0;
    if (!pullU32(bytes, cursor, cursorSize) || !pullBytes(bytes, cursor, cursorSize, out.nextCursor)) {
        throw std::runtime_error("AkkEngine: truncated rollback stream cursor");
    }
    out.items = decodeRollbackItems(bytes.subspan(cursor));
    if (out.done != out.nextCursor.empty()) { throw std::runtime_error("AkkEngine: invalid rollback stream continuation"); }
    return out;
}

void persistRollbackJournalLocked() const {
    if (opts.paths.rollbackJournalPath.empty()) {
        if (!deferredRollbacks.empty()) { throw std::runtime_error("AkkEngine: rollback journal path is required for deferred rollback"); }
        return;
    }
    std::vector<uint8_t> bytes{'A', 'K', 'R', 'J', '1'};
    pushU32(bytes, deferredRollbacks.size());
    for (const auto& task : deferredRollbacks) {
        bytes.append_range(task.taskId);
        bytes.append_range(task.operationId);
        bytes.append_range(task.createdStartupId);
        bytes.append_range(task.clusterId);
        pushU64(bytes, task.configEpoch);
        bytes.push_back(std::to_underlying(task.action));
        bytes.push_back(task.clusterTask ? 1U : 0U);
        bytes.push_back(std::to_underlying(task.conflict));
        pushU64(bytes, task.targetNodeId);
        pushU64(bytes, task.targetSeq);
        pushU64(bytes, task.plannedWatermark);
        pushU32(bytes, task.key.size());
        bytes.append_range(task.key);
    }
    const uint32_t crc = cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size());
    pushU32(bytes, crc);
    writeFileAtomicallyDurable(opts.paths.rollbackJournalPath, bytes);
}

void loadRollbackJournal() {
    crypto::secureRandom(rollbackStartupId);
    if (opts.paths.rollbackJournalPath.empty() || !fs::exists(opts.paths.rollbackJournalPath)) { return; }
    std::ifstream in{opts.paths.rollbackJournalPath, std::ios::binary};
    if (!in) { throw std::runtime_error("AkkEngine: failed to open rollback journal"); }
    std::vector<uint8_t> bytes{std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{}};
    if (bytes.size() < 13 || std::memcmp(bytes.data(), "AKRJ1", 5) != 0) { throw std::runtime_error("AkkEngine: corrupt rollback journal header"); }
    size_t crcCursor = bytes.size() - 4;
    uint32_t storedCrc = 0;
    if (!pullU32(bytes, crcCursor, storedCrc) || storedCrc != cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size() - 4)) {
        throw std::runtime_error("AkkEngine: rollback journal checksum mismatch");
    }
    size_t cursor = 5;
    uint32_t count = 0;
    if (!pullU32(bytes, cursor, count)) { throw std::runtime_error("AkkEngine: truncated rollback journal"); }
    if (count > (bytes.size() - 13) / 103) { throw std::runtime_error("AkkEngine: invalid rollback journal task count"); }
    deferredRollbacks.clear();
    deferredRollbacks.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        DeferredRollbackTask task;
        if (cursor + 75 > bytes.size() - 4) { throw std::runtime_error("AkkEngine: truncated rollback journal task"); }
        std::ranges::copy(std::span{bytes}.subspan(cursor, 16), task.taskId.begin());
        cursor += 16;
        std::ranges::copy(std::span{bytes}.subspan(cursor, 16), task.operationId.begin());
        cursor += 16;
        std::ranges::copy(std::span{bytes}.subspan(cursor, 16), task.createdStartupId.begin());
        cursor += 16;
        std::ranges::copy(std::span{bytes}.subspan(cursor, 16), task.clusterId.begin());
        cursor += 16;
        if (!pullU64(bytes, cursor, task.configEpoch)) { throw std::runtime_error("AkkEngine: truncated rollback journal cluster epoch"); }
        task.action = static_cast<cluster::StripeControlAction>(bytes[cursor++]);
        task.clusterTask = bytes[cursor++] != 0;
        task.conflict = static_cast<RollbackConflictPolicy>(bytes[cursor++]);
        uint32_t keySize = 0;
        if ((task.action != cluster::StripeControlAction::ROLLBACK_KEY && task.action != cluster::StripeControlAction::ROLLBACK_STREAM) || task.
            conflict > RollbackConflictPolicy::OVERWRITE_LATEST || !pullU64(bytes, cursor, task.targetNodeId) || !
            pullU64(bytes, cursor, task.targetSeq) || !pullU64(bytes, cursor, task.plannedWatermark) || !pullU32(bytes, cursor, keySize) || keySize
            > bytes.size() - 4 - cursor) { throw std::runtime_error("AkkEngine: invalid rollback journal task"); }
        task.key.assign_range(std::span{bytes}.subspan(cursor, keySize));
        cursor += keySize;
        deferredRollbacks.push_back(std::move(task));
    }
    if (cursor != bytes.size() - 4) { throw std::runtime_error("AkkEngine: trailing rollback journal data"); }
}

void enqueueDeferredRollback(DeferredRollbackTask task) {
    if (task.taskId == std::array<uint8_t, 16>{}) { crypto::secureRandom(task.taskId); }
    task.createdStartupId = rollbackStartupId;
    std::lock_guard lock{rollbackJournalMu};
    deferredRollbacks.push_back(std::move(task));
    try { persistRollbackJournalLocked(); }
    catch (...) {
        deferredRollbacks.pop_back();
        throw;
    }
}

[[nodiscard]] std::array<uint8_t, 16> scheduleDeferredRollback(
    const std::array<uint8_t, 16>& operationId,
    cluster::StripeControlAction action,
    bool clusterTask,
    uint64_t targetNodeId,
    uint64_t targetSeq,
    uint64_t plannedWatermark,
    RollbackConflictPolicy conflict,
    std::span<const uint8_t> key = {}
) {
    DeferredRollbackTask task;
    crypto::secureRandom(task.taskId);
    task.operationId = operationId;
    task.action = action;
    task.clusterTask = clusterTask;
    if (clusterTask) {
        task.clusterId = clusterConfig.clusterId();
        task.configEpoch = clusterRuntime->configurationEpoch();
    }
    task.targetNodeId = targetNodeId;
    task.targetSeq = targetSeq;
    task.plannedWatermark = plannedWatermark;
    task.conflict = conflict;
    task.key.assign_range(key);
    const auto taskId = task.taskId;
    enqueueDeferredRollback(std::move(task));
    return taskId;
}

void completeDeferredRollback(const std::array<uint8_t, 16>& taskId) {
    std::lock_guard lock{rollbackJournalMu};
    const auto previous = deferredRollbacks;
    std::erase_if(deferredRollbacks, [&](const DeferredRollbackTask& task) { return task.taskId == taskId; });
    try { persistRollbackJournalLocked(); }
    catch (...) {
        deferredRollbacks = previous;
        throw;
    }
}

[[nodiscard]] bool executeDeferredRollback(const DeferredRollbackTask& task, const std::stop_token& stop) {
    if (task.clusterTask) {
        if (!clusterRuntime) { return false; }
        uint64_t controlNode = task.targetNodeId;
        if (clusterReplicationMode == cluster::ReplicationMode::STRIPE) {
            controlNode = clusterRuntime->stripeMetadataLeaderNodeId();
            if (controlNode == 0) { return false; }
        }
        else if (clusterReplicationMode != cluster::ReplicationMode::PARTITIONED) {
            if (clusterConfig.usesDataConsensus()) {
                controlNode = clusterRuntime->raftStats().leaderNodeId;
                if (controlNode == 0) { return false; }
            }
            else {
                if (clusterRuntime->role() != cluster::NodeRole::PRIMARY) { return false; }
                controlNode = nodeId;
            }
        }
        std::vector<uint8_t> cursor = task.key;
        for (;;) {
            if (stop.stop_requested()) { return false; }
            cluster::StripeControlRequest request;
            request.action = task.action;
            request.ownerNodeId = controlNode;
            request.fenceToken = task.targetSeq;
            request.key = cursor;
            request.metadata = {std::to_underlying(RollbackExecutionMode::IMMEDIATE), std::to_underlying(task.conflict),};
            pushU64(request.metadata, task.plannedWatermark);
            const auto response = clusterRuntime->rollbackControl(controlNode, std::move(request));
            if (response.status != cluster::StripeControlStatus::COMMITTED) { return false; }
            if (task.action != cluster::StripeControlAction::ROLLBACK_STREAM) { return true; }
            auto batch = decodeRollbackStreamBatch(response.metadata);
            if (batch.done) { return true; }
            cursor = std::move(batch.nextCursor);
        }
    }
    std::unique_lock epochLock{mutationEpochMu};
    versionLog->forceSync();
    if (stop.stop_requested()) { return false; }
    if (task.action == cluster::StripeControlAction::ROLLBACK_KEY) {
        (void)rollbackKeyLocal(task.key, task.targetSeq, task.conflict, 0, task.plannedWatermark);
    }
    else {
        for (const auto& key : rollbackKeysChangedAfter(task.targetSeq)) {
            if (stop.stop_requested()) { return false; }
            (void)rollbackKeyLocal(key, task.targetSeq, task.conflict, 0, task.plannedWatermark);
        }
    }
    return true;
}

void startRollbackRecovery() {
    if (deferredRollbacks.empty()) { return; }
    rollbackRecoveryThread = std::jthread(
        [this](const std::stop_token& stop) {
            while (!stop.stop_requested()) {
                std::vector<DeferredRollbackTask> tasks;
                {
                    std::lock_guard lock{rollbackJournalMu};
                    for (const auto& task : deferredRollbacks) { if (task.createdStartupId != rollbackStartupId) { tasks.push_back(task); } }
                }
                for (const auto& task : tasks) {
                    if (stop.stop_requested()) { return; }
                    bool complete = false;
                    try { complete = executeDeferredRollback(task, stop); }
                    catch (...) {}
                    if (!complete) { continue; }
                    std::lock_guard lock{rollbackJournalMu};
                    auto previous = deferredRollbacks;
                    std::erase_if(deferredRollbacks, [&](const DeferredRollbackTask& candidate) { return candidate.taskId == task.taskId; });
                    try { persistRollbackJournalLocked(); }
                    catch (...) { deferredRollbacks = std::move(previous); }
                }
                std::unique_lock lock{lifecycleMu};
                lifecycleCv.wait_for(lock, std::chrono::seconds{1});
            }
        }
    );
}

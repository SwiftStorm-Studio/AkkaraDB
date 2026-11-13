/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/detail/AkkEnginePointAccess.inc
// Included inside AkkEngine::Impl by AkkEngineImpl.hpp.
void requireOwnsWriteBatch(std::span<const BatchPutEntry> entries) const { for (const auto& entry : entries) { requireOwnsWriteKey(entry.key); } }

[[nodiscard]] bool getIntoInternal(
    std::span<const uint8_t> key,
    std::vector<uint8_t>& out,
    bool countStats,
    std::optional<uint64_t> requestedSnapshot = std::nullopt,
    const StoredReadSnapshot* snapshot = nullptr
) {
    if (usesPartitionRaft()) {
        auto* child = localPartition(key);
        return child && child->impl_->getIntoInternal(key, out, countStats, requestedSnapshot, snapshot);
    }
    throwIfBackgroundFailed();
    if (!requestedSnapshot.has_value()) { waitForKeySequenceReadVisibility(); }
    if (blobManager) {
        auto value = getValueInternal(key, countStats, requestedSnapshot, snapshot);
        if (!value) { return false; }
        out = std::move(*value);
        return true;
    }

    auto transactionRead = distributedReadAdmission(key, {}, true);
    if (countStats) { getsTotal.fetch_add(1, std::memory_order_relaxed); }
    const auto record = readStoredPoint(key, requestedSnapshot, snapshot);
    if (record && !record.view().isTombstone()) {
        promoteStoredPoint(record);
        const auto value = record.view().value();
        out.assign_range(value);
        (record.disk ? getsSstHit : getsMemtableHit).fetch_add(1, std::memory_order_relaxed);
        return true;
    }
    getsMiss.fetch_add(1, std::memory_order_relaxed);
    return false;
}

[[nodiscard]] bool getIntoArenaInternal(std::span<const uint8_t> key, BufferArena& arena, std::span<const uint8_t>& out, bool countStats) {
    if (usesPartitionRaft()) {
        auto* child = localPartition(key);
        out = {};
        return child && child->impl_->getIntoArenaInternal(key, arena, out, countStats);
    }
    throwIfBackgroundFailed();
    waitForKeySequenceReadVisibility();
    out = {};
    if (blobManager) {
        auto value = getValueInternal(key, countStats);
        if (!value) { return false; }
        out = copySpanToArena(*value, arena);
        return true;
    }

    auto transactionRead = distributedReadAdmission(key, {}, true);
    if (countStats) { getsTotal.fetch_add(1, std::memory_order_relaxed); }
    const auto record = readStoredPoint(key);
    if (record && !record.view().isTombstone()) {
        promoteStoredPoint(record);
        out = copySpanToArena(record.view().value(), arena);
        (record.disk ? getsSstHit : getsMemtableHit).fetch_add(1, std::memory_order_relaxed);
        return true;
    }
    getsMiss.fetch_add(1, std::memory_order_relaxed);
    return false;
}

[[nodiscard]] bool canRunBlobGc() const noexcept {
    return blobManager != nullptr && versionLog == nullptr && !transactionJournalPending.load(std::memory_order_acquire);
}

void waitForVersionLogRecovery() const { if (versionLog) { versionLog->waitUntilReady(); } }

/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/detail/AkkEnginePointRead.inc
// Included inside AkkEngine::Impl by AkkEngineImpl.hpp.
struct StoredPointRead {
    std::optional<memtable::MemTable::PinnedRecord> memory;
    std::optional<sst::SSTRecord> disk;

    [[nodiscard]] explicit operator bool() const noexcept { return memory.has_value() || disk.has_value(); }
    [[nodiscard]] RecordView view() const noexcept {
        if (memory) { return memory->view; }
        return {disk->key.data(), static_cast<uint16_t>(disk->key.size()), disk->value.data(),
            static_cast<uint16_t>(disk->value.size()), disk->seq, disk->flags, disk->keyFp64, disk->miniKey};
    }
};

struct StoredReadSnapshot {
    memtable::MemTable::ReadSnapshot memory;
    sst::SSTManager::ReadSnapshot disk;
};

[[nodiscard]] StoredReadSnapshot captureStoredReadSnapshot() const {
    StoredReadSnapshot result;
    result.memory = memtable->readSnapshot([&] {
        if (sstManager) { result.disk = sstManager->readSnapshot(); }
        return snapshotSeq();
    });
    return result;
}

struct StoredRangeRead {
    memtable::MemTable::RangeIterator memory;
    sst::SSTManager::Iterator disk;
};

[[nodiscard]] StoredRangeRead captureStoredRangeRead(
    const memtable::MemTable::KeyRange& range, bool pinWriters = false) const {
    sst::SSTManager::ReadSnapshot disk;
    const auto captureSequence = [&] {
        if (sstManager) { disk = sstManager->readSnapshot(); }
        return snapshotSeq();
    };
    // Pin memory ownership, SST generation and visibility before a
    // backend iterator can pause while flush/compaction replaces files.
    auto memory = pinWriters ? memtable->pinnedIterator(range, captureSequence)
                             : memtable->iteratorWithSnapshot(range, captureSequence);
    auto iterator = disk.scanIter(range.start, range.end, memory.snapshotSeq(), true);
    return {std::move(memory), std::move(iterator)};
}

[[nodiscard]] StoredPointRead readStoredPoint(
    std::span<const uint8_t> key,
    std::optional<uint64_t> requestedSnapshot = std::nullopt,
    const StoredReadSnapshot* snapshot = nullptr
) const {
    StoredPointRead result;
    sst::SSTManager::ReadSnapshot disk;
    uint64_t seq;
    if (snapshot) {
        disk = snapshot->disk;
        seq = snapshot->memory.snapshotSeq();
        result.memory = snapshot->memory.getPinned(key);
    }
    else {
        // Capture both sources and the sequence before a lookup can
        // pause. Owning memory sources closes the flush/unpublish
        // gap; the SST snapshot retains pre-compaction versions.
        result.memory = memtable->getPinnedWithSnapshot(key, [&] {
            if (sstManager) { disk = sstManager->readSnapshot(); }
            seq = requestedSnapshot.value_or(snapshotSeq());
            return seq;
        });
    }
    if (!result.memory || disk.maxSequence() > result.memory->view.seq()) {
        result.disk = disk.get(key, seq);
    }
    if (result.memory && result.disk && result.memory->view.seq() >= result.disk->seq) { result.disk.reset(); }
    if (result.disk) { result.memory.reset(); }
    return result;
}

void promoteStoredPoint(const StoredPointRead& record) {
    if (opts.runtime.sstPromoteReads && record.disk) {
        const auto view = record.view();
        memtable->promote(view);
    }
}

[[nodiscard]] std::optional<std::vector<uint8_t>> getValueInternal(
    std::span<const uint8_t> key,
    bool countStats,
    std::optional<uint64_t> requestedSnapshot = std::nullopt,
    const StoredReadSnapshot* snapshot = nullptr
) {
    if (usesPartitionRaft()) {
        auto* child = localPartition(key);
        return child ? child->impl_->getValueInternal(key, countStats, requestedSnapshot, snapshot) : std::nullopt;
    }
    throwIfBackgroundFailed();
    if (!requestedSnapshot.has_value()) { waitForKeySequenceReadVisibility(); }
    auto transactionRead = distributedReadAdmission(key, {}, true);
    auto blobReadPin = blobManager ? blobManager->pinReads() : blob::BlobManager::ReadPin{};
    if (countStats) { getsTotal.fetch_add(1, std::memory_order_relaxed); }
    const auto record = readStoredPoint(key, requestedSnapshot, snapshot);
    if (record && !record.view().isTombstone()) {
        promoteStoredPoint(record);
        const auto view = record.view();
        auto value = resolveValue(view.flags(), view.value());
        if (value) { (record.disk ? getsSstHit : getsMemtableHit).fetch_add(1, std::memory_order_relaxed); }
        else { getsMiss.fetch_add(1, std::memory_order_relaxed); }
        return value;
    }
    getsMiss.fetch_add(1, std::memory_order_relaxed);
    return std::nullopt;
}

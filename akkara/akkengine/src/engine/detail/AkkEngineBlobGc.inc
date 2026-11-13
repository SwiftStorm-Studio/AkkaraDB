/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/detail/AkkEngineBlobGc.inc
// Included inside AkkEngine::Impl by AkkEngineImpl.hpp.
struct LatestBlobReference {
    uint64_t seq = 0;
    std::optional<uint64_t> blobId;
};

static void recordLatestBlobReference(
    std::unordered_map<std::string, LatestBlobReference>& latestByKey,
    std::span<const uint8_t> key,
    uint64_t seq,
    std::optional<uint64_t> blobId
) {
    const std::string keyBytes{reinterpret_cast<const char*>(key.data()), key.size()};
    auto& latest = latestByKey[keyBytes];
    if (seq >= latest.seq) { latest = LatestBlobReference{seq, blobId}; }
}

[[nodiscard]] std::unordered_set<uint64_t> collectReferencedBlobIds() const {
    // Pin memory before capturing SST state. A completed flush cannot
    // disappear from both sources, and live-file refs are sampled atomically.
    memtable::MemTable::KeyRange range;
    auto mt = memtable->pinnedIterator(range, [] { return UINT64_MAX; });
    std::unordered_map<std::string, LatestBlobReference> latestByKey;
    if (const auto refs = manifest ? manifest->liveSstBlobRefs() : std::nullopt) {
        for (const auto& entry : *refs) { recordLatestBlobReference(latestByKey, entry.key, entry.seq, entry.blobId); }
    }
    else if (sstManager) {
        auto sst = sstManager->scanIter({}, {}, UINT64_MAX, true);
        while (sst.hasNext()) {
            const auto record = sst.next();
            const auto blobId = record->isTombstone() ? std::nullopt : decodeBlobIdIfReference(record->flags, record->value);
            recordLatestBlobReference(latestByKey, record->key, record->seq, blobId);
        }
    }
    while (mt.hasNext()) {
        const auto record = mt.next();
        if (!record.has_value()) { continue; }
        std::optional<uint64_t> blobId;
        if (!record->isTombstone()) { blobId = decodeBlobIdIfReference(record->flags(), record->value()); }
        recordLatestBlobReference(latestByKey, record->key(), record->seq(), blobId);
    }

    std::unordered_set<uint64_t> live;
    for (const auto& latest : latestByKey | std::views::values) { if (latest.blobId.has_value()) { live.insert(*latest.blobId); } }
    return live;
}

void runBlobGcIfSafe() {
    if (!canRunBlobGc()) { return; }
    std::unique_lock planning{blobGcPlanMu, std::try_to_lock};
    if (!planning.owns_lock() || !canRunBlobGc()) { return; }
    std::unique_lock admission{mutationEpochMu, std::try_to_lock};
    if (!admission.owns_lock()) { return; }
    // Blob writers publish the file before its reference. Do not admit
    // a new file between reference capture and the orphan directory scan.
    // A flush callback must not wait for a writer that is draining it.
    std::unique_lock writer{writeMu, std::try_to_lock};
    if (!writer.owns_lock() || !canRunBlobGc()) { return; }
    const auto live = collectReferencedBlobIds();
    blobManager->scanOrphans([&live](uint64_t blobId) { return live.contains(blobId); });
}

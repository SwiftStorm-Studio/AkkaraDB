/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/detail/AkkEngineStripeWrite.inc
// Included inside AkkEngine::Impl by AkkEngineImpl.hpp.
struct StripeTransaction {
    StripeMetadata next;
    std::optional<StripeMetadata> previous;
};

static std::vector<uint8_t> encodeStripeTransaction(const StripeMetadata& next, const std::optional<StripeMetadata>& previous) {
    std::vector<uint8_t> out{'A', 'K', 'S', 'T', '1'};
    const auto nextBytes = encodeStripeMetadata(next);
    const auto previousBytes = previous ? encodeStripeMetadata(*previous) : std::vector<uint8_t>{};
    pushU32(out, nextBytes.size());
    out.append_range(nextBytes);
    pushU32(out, previousBytes.size());
    out.append_range(previousBytes);
    return out;
}

static std::optional<StripeTransaction> decodeStripeTransaction(
    std::span<const uint8_t> bytes,
    uint64_t expectedVersion,
    uint64_t expectedAuthority
) {
    if (bytes.size() < 13 || std::memcmp(bytes.data(), "AKST1", 5) != 0) { return std::nullopt; }
    size_t cursor = 5;
    StripeTransaction out;
    for (int n = 0; n < 2; ++n) {
        uint32_t length = 0;
        if (!pullU32(bytes, cursor, length) || length > bytes.size() - cursor || (n == 0 && length == 0)) { return std::nullopt; }
        if (length != 0) {
            auto metadata = decodeStripeMetadata(bytes.subspan(cursor, length));
            if (!metadata) { return std::nullopt; }
            if (n == 0) { out.next = std::move(*metadata); }
            else { out.previous = std::move(*metadata); }
        }
        cursor += length;
    }
    if (out.next.nodeIds.empty() || out.next.version != expectedVersion || out.next.authorityNodeId != expectedAuthority || cursor != bytes.
        size()) { return std::nullopt; }
    return out;
}

static void validateStripeShardStorage(size_t keySize, size_t valueSize, erasure::ErasureLayout layout) {
    // The shard key adds 20 bytes; the encoded value header adds 35.
    validateStoredRecord(keySize, 55);
    validateStoredRecord(keySize + 55, erasure::RsErasureCodec::shardPayloadSize(valueSize, layout.dataShards));
}

void writeStripeValue(std::span<const uint8_t> publicKey, std::span<const uint8_t> value, bool tombstone = false, bool rollback = false) {
    validateStoredRecord(publicKey.size(), 20);
    if (!tombstone) { validateStripeShardStorage(publicKey.size(), value.size(), stripeLayoutFor(currentPlacement())); }
    requireOwnsWriteKey(publicKey);
    auto keyLock = stripeKeys.lock(publicKey);
    checkStripeHealthy();
    const uint64_t originalOwner = stripeOwnerFor(currentPlacement(), publicKey);
    auto lease = clusterRuntime
                     ? clusterRuntime->acquireStripeOperation(publicKey, originalOwner)
                     : cluster::StripeOperationLease{
                         .ownerNodeId = originalOwner,
                         .authorityNodeId = originalOwner,
                         .fenceToken = 0,
                         .coordinated = false,
                         .metadata = {},
                     };
    bool publishing = false;
    try {
        auto previous = lease.coordinated && !lease.metadata.empty() ? decodeStripeMetadata(lease.metadata) : readStripeMetadata(publicKey);
        if (lease.coordinated && !lease.metadata.empty() && !previous) { throw std::runtime_error("AkkEngine: corrupt fenced STRIPE metadata"); }
        if (previous && previous->ownerNodeId != originalOwner) { throw std::runtime_error("AkkEngine: fenced STRIPE metadata owner mismatch"); }
        StripeMetadata next;
        next.tombstone = tombstone;
        next.rollback = rollback;
        next.version = lease.coordinated ? lease.fenceToken : 0;
        next.ownerNodeId = originalOwner;
        next.authorityNodeId = lease.authorityNodeId;
        next.originNodeId = lease.authorityNodeId;
        next.timestampNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
        next.fenceToken = lease.coordinated ? lease.fenceToken : 1;
        next.originalSize = value.size();
        next.layout = stripeLayoutFor(currentPlacement());
        next.copiesPerShard = currentPlacement().stripe().copiesPerShard;
        const uint64_t excludedOwner = lease.authorityNodeId == originalOwner ? 0 : originalOwner;
        next.nodeIds = stripeNodeIdsFor(currentPlacement(), publicKey, excludedOwner, lease.authorityNodeId);
        if (next.copiesPerShard == 2 && previous && previous->nodeIds.size() == next.nodeIds.size()) {
            const auto placementView = currentPlacement();
            const auto normal = stripeNodeIdsFor(placementView, publicKey);
            // A reachable owner replacement may differ from the
            // current authority, which can already hold another shard.
            if (const auto ownerPosition = std::ranges::find(normal, excludedOwner); ownerPosition != normal.end()) {
                const size_t position = ownerPosition - normal.begin();
                const auto spare = previous->nodeIds[position];
                if (placementView.isStripeFailoverNode(spare) && clusterRuntime->stripeNodeReachable(spare)) { next.nodeIds[position] = spare; }
            }
            for (size_t position = 0; position < previous->nodeIds.size(); ++position) {
                const auto spare = previous->nodeIds[position];
                if (placementView.isStripeFailoverNode(spare) && !std::ranges::contains(next.nodeIds, spare) && (excludedOwner == 0 || normal[
                    position] != excludedOwner)) { next.nodeIds[position] = spare; }
            }
        }
        cluster::detail::validateStripeFaultDomains(currentPlacement(), next.nodeIds);
        next.shardPresent.assign(next.nodeIds.size(), 0);
        // Placement may have changed while the lease was acquired.
        if (!tombstone) { validateStripeShardStorage(publicKey.size(), value.size(), next.layout); }
        const auto metadataSize = encodeStripeMetadata(next).size();
        validateStoredRecord(stripeMetaKey(publicKey).size(), metadataSize);
        validateStoredRecord(stripeHistoryPlacementKey(publicKey, 0, 0).size(), metadataSize + 8);
        validateStoredRecord(stripeTransactionKey(publicKey, next.version).size(), encodeStripeTransaction(next, previous).size());
        // The fencing token is also the immutable generation id.
        // Local WAL ordering continues to use a local storage seq.
        {
            std::lock_guard lock{writeMu};
            try {
                const uint64_t intentSeq = memtable->reserveSeq(1);
                if (next.version == 0) { next.version = intentSeq; }
                const auto intentKey = stripeTransactionKey(publicKey, next.version);
                const auto intentValue = encodeStripeTransaction(next, previous);
                appendAll(intentSeq, intentKey, intentValue, MemHdr16::FLAG_NORMAL, nodeId, 0, 0, 0xFF, true);
                forceClusterLocalDurable();
                std::lock_guard stateLock{stripeStateMu};
                stripePending[intentKey] = intentValue;
            }
            catch (...) {
                failStripe(std::current_exception());
                throw;
            }
        }
        crashAtTestPoint("stripe.after_intent");
        if (!tombstone) {
            uint16_t durablePlacements = 0;
            std::vector<uint8_t> durableCopies(next.layout.totalShards(), 0);
            const auto shards = erasure::RsErasureCodec::encode(value, next.layout);
            stripeExecutor->forEach(
                next.nodeIds.size(),
                [&](size_t placement) {
                    const uint16_t shardIndex = placement / next.copiesPerShard;
                    const auto targetNodeId = next.nodeIds[placement];
                    try {
                        if (targetNodeId != nodeId && clusterRuntime && !clusterRuntime->stripeNodeReachable(targetNodeId)) { return; }
                        writeStripeRecordToNode(
                            targetNodeId,
                            stripeShardKey(publicKey, next.version, shardIndex),
                            encodeStripeShard(next.version, shards[shardIndex], next.layout),
                            next.version
                        );
                        next.shardPresent[placement] = 1;
                        crashAtTestPoint("stripe.after_shard");
                    }
                    catch (const std::runtime_error&) {}
                }
            );
            for (size_t placement = 0; placement < next.shardPresent.size(); ++placement) {
                if (next.shardPresent[placement] != 0) {
                    ++durablePlacements;
                    ++durableCopies[placement / next.copiesPerShard];
                }
            }
            bool sufficient = durablePlacements == next.nodeIds.size();
            uint16_t requiredPlacements = next.nodeIds.size();
            if (opts.cluster.runtime.stripeWriteCommitMode == cluster::StripeWriteCommitMode::DATA_SHARDS) {
                if (next.copiesPerShard == 2) {
                    sufficient = std::ranges::all_of(durableCopies, [](uint8_t copies) { return copies != 0; });
                    requiredPlacements = next.layout.dataShards;
                }
                else if (next.layout.parityShards != 0) {
                    sufficient = durablePlacements >= next.layout.dataShards;
                    requiredPlacements = next.layout.dataShards;
                }
            }
            if (!sufficient) {
                throw std::runtime_error(
                    "AkkEngine: insufficient durable STRIPE placements (durable=" + std::to_string(durablePlacements) + ", required=" +
                    std::to_string(requiredPlacements) + ")"
                );
            }
            if (durablePlacements < next.nodeIds.size()) {
                std::lock_guard statsLock{stripeRebuildStatsMu};
                stripeRebuildStats.active = true;
            }
        }
        crashAtTestPoint("stripe.before_publish");
        const auto encoded = encodeStripeMetadata(next);
        publishing = true;
        if (lease.coordinated) { clusterRuntime->commitStripeMetadata(lease, publicKey, encoded); }
        else { writeStripeLocal(stripeMetaKey(publicKey), encoded); }
        crashAtTestPoint("stripe.after_publish");
        if (tombstone) { removesTotal.fetch_add(1, std::memory_order_relaxed); }
        else { putsTotal.fetch_add(1, std::memory_order_relaxed); }
    }
    catch (...) {
        const auto failure = std::current_exception();
        if (clusterRuntime) { clusterRuntime->releaseStripeOperation(lease, publicKey); }
        if (publishing) {
            throw cluster::ClusterRoutingError(
                cluster::ClusterRoutingCode::OUTCOME_UNKNOWN,
                {},
                "AkkEngine: STRIPE metadata publication outcome is unknown"
            );
        }
        std::rethrow_exception(failure);
    }
}

void removeStripeValue(std::span<const uint8_t> publicKey) { writeStripeValue(publicKey, {}, true); }

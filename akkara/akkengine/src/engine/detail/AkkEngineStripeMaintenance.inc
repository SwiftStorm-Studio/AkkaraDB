/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/detail/AkkEngineStripeMaintenance.inc
// Included inside AkkEngine::Impl by AkkEngineImpl.hpp.
bool stripePlacementProtected() const {
    if (!clusterRuntime) { return false; }
    const auto state = clusterRuntime->placementState(false);
    return state.pendingGeneration != 0 || stripeProtectionGeneration.load(std::memory_order_acquire) > state.lastIssuedGeneration;
}

bool stripeGenerationRetainedLocal(std::span<const uint8_t> publicKey, uint64_t generation) {
    const auto metadataKey = stripeMetaKey(publicKey);
    const auto retained = [&](std::span<const uint8_t> bytes) {
        const auto metadata = decodeStripeMetadata(bytes);
        if (!metadata) { throw std::runtime_error("AkkEngine: corrupt retained STRIPE metadata"); }
        return metadata->version == generation;
    };
    if (const auto current = getValueInternal(metadataKey, false); current && retained(*current)) { return true; }
    if (versionLog) { for (const auto& entry : versionLog->history(metadataKey)) { if (retained(entry.value)) { return true; } } }
    return false;
}

bool reclaimStripeGeneration(std::span<const uint8_t> publicKey, std::span<const uint8_t> encoded, const std::function<bool()>& retainedByPeers) {
    // The authority reservation already serializes this key. A local
    // writer can hold stripeKeys while waiting for that reservation.
    std::unique_lock reclamationLock{stripeGcEpochMu};
    if (stripePlacementProtected() || stripeQueryPins->load(std::memory_order_acquire) != 0) { return false; }
    const auto candidate = decodeStripeMetadata(encoded);
    if (!candidate || !candidate->version || candidate->ownerNodeId != stripeOwnerFor(currentPlacement(), publicKey) || candidate->layout.dataShards
        != currentPlacement().stripe().dataShards || candidate->layout.parityShards != currentPlacement().stripe().parityShards || candidate->
        copiesPerShard != currentPlacement().stripe().copiesPerShard) {
        throw std::runtime_error("AkkEngine: invalid STRIPE reclamation candidate");
    }
    if (candidate->tombstone || stripeGenerationRetainedLocal(publicKey, candidate->version) || retainedByPeers()) { return true; }
    for (size_t placement = 0; placement < candidate->nodeIds.size(); ++placement) {
        const uint16_t shardIndex = placement / candidate->copiesPerShard;
        writeStripeRecordToNode(
            candidate->nodeIds[placement],
            stripeShardKey(publicKey, candidate->version, shardIndex),
            {},
            candidate->version,
            cluster::ReplOpType::REMOVE,
            candidate->authorityNodeId
        );
    }
    return true;
}

void collectStripeGarbage() {
    if (stripePlacementProtected()) { return; }
    checkStripeHealthy();
    if (!stripeGcLoaded) {
        std::lock_guard lock{writeMu};
        auto blobReadPin = blobManager ? blobManager->pinReads() : blob::BlobManager::ReadPin{};
        BufferArena arena;
        memtable::MemTable::KeyRange range;
        const auto seq = snapshotSeq();
        auto mt = memtable->iterator(range, seq);
        sst::SSTManager::Iterator sst;
        if (sstManager) { sst = sstManager->scanIter({}, {}, seq, true); }
        for (const auto& record : scanGenerator(arena, std::move(mt), std::move(sst), blobManager.get(), std::move(blobReadPin))) {
            if (isStripeInternalKey(record.key) && record.key[4] == 'T') {
                std::lock_guard stateLock{stripeStateMu};
                stripePending[std::vector<uint8_t>{record.key.begin(), record.key.end()}] = std::vector<uint8_t>{
                    record.value.begin(),
                    record.value.end()
                };
            }
        }
        stripeGcLoaded = true;
    }
    std::vector<uint8_t> previousIntent;
    for (;;) {
        struct PendingView {
            std::vector<uint8_t> key;
            std::vector<uint8_t> value;
        };
        PendingView item;
        {
            std::lock_guard stateLock{stripeStateMu};
            const auto it = stripePending.upper_bound(previousIntent);
            if (it == stripePending.end()) { break; }
            item = {it->first, it->second};
        }
        previousIntent = item.key;
        size_t cursor = 6;
        uint32_t keySize = 0;
        if (!pullU32(item.key, cursor, keySize) || item.key.size() - cursor < 8 || keySize != item.key.size() - cursor - 8) {
            throw std::runtime_error("AkkEngine: corrupt STRIPE intent key");
        }
        const auto publicKey = std::span<const uint8_t>{item.key.data() + cursor, keySize};
        cursor += keySize;
        uint64_t version = 0;
        const uint64_t originalOwner = stripeOwnerFor(currentPlacement(), publicKey);
        if (!pullU64(item.key, cursor, version)) { throw std::runtime_error("AkkEngine: corrupt STRIPE intent version"); }
        auto transaction = decodeStripeTransaction(item.value, version, nodeId);
        if (!transaction) { throw std::runtime_error("AkkEngine: corrupt STRIPE intent"); }
        // Placement can retire the writer while its valid intent is
        // still awaiting cleanup. Its source storage stays retained.
        if (originalOwner != nodeId && !currentPlacement().isStripeFailoverNode(nodeId)) { continue; }
        std::vector<StripeMetadata> candidates;
        candidates.push_back(transaction->next);
        if (transaction->previous) { candidates.push_back(*transaction->previous); }
        bool complete = true;
        for (const auto& candidate : candidates) {
            if (candidate.tombstone) { continue; }
            if (!clusterRuntime->reclaimStripeGeneration(publicKey, encodeStripeMetadata(candidate))) { complete = false; }
        }
        if (complete) {
            writeStripeLocal(item.key, {}, cluster::ReplOpType::REMOVE);
            std::lock_guard stateLock{stripeStateMu};
            stripePending.erase(item.key);
        }
    }
}

void rebuildStripeShardsBatch() {
    if (stripePlacementProtected()) { return; }
    if (!opts.cluster.runtime.stripeAutoRebuild || (currentPlacement().stripe().parityShards == 0 && currentPlacement().stripe().copiesPerShard ==
        1)) { return; }
    struct Candidate {
        std::vector<uint8_t> key;
        StripeMetadata metadata;
    };
    std::vector<Candidate> candidates;
    candidates.reserve(opts.cluster.runtime.stripeRebuildBatchKeys);
    {
        checkStripeHealthy();
        {
            std::lock_guard statsLock{stripeRebuildStatsMu};
            ++stripeRebuildStats.cycles;
            if (stripeRebuildCursor.empty()) { stripeRebuildStats.active = false; }
        }
        auto blobReadPin = blobManager ? blobManager->pinReads() : blob::BlobManager::ReadPin{};
        BufferArena arena;
        memtable::MemTable::KeyRange range;
        range.start = stripeRebuildCursor;
        auto sources = captureStoredRangeRead(range);
        std::vector<uint8_t> lastMetadataKey;
        uint32_t metadataKeysVisited = 0;
        for (const auto& record : scanGenerator(arena, std::move(sources.memory), std::move(sources.disk), blobManager.get(), std::move(blobReadPin))) {
            std::vector<uint8_t> internalKey{record.key.begin(), record.key.end()};
            if (!stripeRebuildCursor.empty() && internalKey <= stripeRebuildCursor) { continue; }
            auto publicKey = publicKeyFromStripeMetaKey(record.key);
            if (!publicKey) { continue; }
            lastMetadataKey = std::move(internalKey);
            ++metadataKeysVisited;
            auto metadata = decodeStripeMetadata(record.value);
            if (!metadata) { throw std::runtime_error("AkkEngine: corrupt STRIPE rebuild metadata"); }
            *metadata = effectiveStripeMetadata(*publicKey, *metadata);
            const bool localMetadataStore = clusterRuntime && clusterRuntime->stripeMetadataLeaderNodeId() == nodeId;
            if (!metadata->tombstone && (metadata->layout.parityShards != 0 || metadata->copiesPerShard == 2) && localMetadataStore) {
                candidates.push_back(Candidate{.key = std::move(*publicKey), .metadata = std::move(*metadata)});
            }
            if (metadataKeysVisited >= opts.cluster.runtime.stripeRebuildBatchKeys) { break; }
        }
        stripeRebuildCursor = metadataKeysVisited >= opts.cluster.runtime.stripeRebuildBatchKeys
                                  ? std::move(lastMetadataKey)
                                  : std::vector<uint8_t>{};
        std::lock_guard statsLock{stripeRebuildStatsMu};
        stripeRebuildStats.keysScanned += metadataKeysVisited;
    }

    for (auto& candidate : candidates) {
        auto keyLock = stripeKeys.lock(candidate.key);
        checkStripeHealthy();
        const auto latestBytes = getValueInternal(stripeMetaKey(candidate.key), false);
        auto latest = latestBytes ? decodeStripeMetadata(*latestBytes) : std::nullopt;
        if (latest) { *latest = effectiveStripeMetadata(candidate.key, *latest); }
        if (!latest || latest->version != candidate.metadata.version || latest->authorityNodeId != candidate.metadata.authorityNodeId) { continue; }
        auto shards = readStripeShards(candidate.key, candidate.metadata);
        bool missing = false;
        std::vector<uint8_t> observed(candidate.metadata.nodeIds.size(), 0);
        for (size_t i = 0; i < observed.size(); ++i) {
            if (i < shards.present.size() && shards.present[i]) { observed[i] = 1; }
            else { missing = true; }
        }
        if (missing) {
            std::lock_guard statsLock{stripeRebuildStatsMu};
            stripeRebuildStats.active = true;
        }
        const auto originalNodes = candidate.metadata.nodeIds;
        for (size_t placement = 0; placement < observed.size(); ++placement) {
            if (observed[placement] != 0) { continue; }
            const uint16_t shardIndex = placement / candidate.metadata.copiesPerShard;
            uint64_t targetNodeId = candidate.metadata.nodeIds[placement];
            if (candidate.metadata.copiesPerShard == 2) {
                const auto placementView = currentPlacement();
                const bool targetReachable = targetNodeId == nodeId || (clusterRuntime && clusterRuntime->stripeNodeReachable(targetNodeId));
                if (!targetReachable) {
                    for (const auto& [score, spare] : cluster::detail::rankNodes(candidate.key, placementView.stripeFailoverNodes())) {
                        (void)score;
                        if (!std::ranges::contains(candidate.metadata.nodeIds, spare.nodeId) && (spare.nodeId == nodeId || (clusterRuntime &&
                            clusterRuntime->stripeNodeReachable(spare.nodeId)))) {
                            auto proposed = candidate.metadata.nodeIds;
                            proposed[placement] = spare.nodeId;
                            try { cluster::detail::validateStripeFaultDomains(placementView, proposed); }
                            catch (const std::invalid_argument&) { continue; }
                            targetNodeId = spare.nodeId;
                            candidate.metadata.nodeIds = std::move(proposed);
                            break;
                        }
                    }
                }
            }
            if (targetNodeId != nodeId && clusterRuntime && !clusterRuntime->stripeNodeReachable(targetNodeId)) { continue; }
            {
                std::lock_guard statsLock{stripeRebuildStatsMu};
                ++stripeRebuildStats.attempts;
            }
            try {
                if (shards.available.size() < candidate.metadata.layout.dataShards) {
                    throw std::runtime_error("AkkEngine: insufficient STRIPE shards for rebuild");
                }
                erasure::ErasureShard repaired;
                if (candidate.metadata.copiesPerShard == 2) {
                    const auto source = std::ranges::find(shards.available, shardIndex, &erasure::ErasureShard::index);
                    if (source == shards.available.end()) { throw std::runtime_error("AkkEngine: missing RAID.10 mirror source for rebuild"); }
                    repaired = *source;
                }
                else { repaired = erasure::RsErasureCodec::repairOne(shardIndex, shards.available, candidate.metadata.layout); }
                writeStripeRecordToNode(
                    targetNodeId,
                    stripeShardKey(candidate.key, candidate.metadata.version, shardIndex),
                    encodeStripeShard(candidate.metadata.version, repaired, candidate.metadata.layout),
                    candidate.metadata.version,
                    cluster::ReplOpType::PUT,
                    candidate.metadata.authorityNodeId
                );
                observed[placement] = 1;
                shards.present[placement] = true;
                if (candidate.metadata.copiesPerShard == 1) { shards.available.push_back(std::move(repaired)); }
                std::lock_guard statsLock{stripeRebuildStatsMu};
                ++stripeRebuildStats.succeeded;
            }
            catch (...) {
                std::lock_guard statsLock{stripeRebuildStatsMu};
                ++stripeRebuildStats.failed;
                stripeRebuildStats.lastFailureNodeId = targetNodeId;
            }
        }

        if (observed == candidate.metadata.shardPresent && candidate.metadata.nodeIds == originalNodes) { continue; }
        checkStripeHealthy();
        const auto currentBytes = getValueInternal(stripeMetaKey(candidate.key), false);
        const auto current = currentBytes ? decodeStripeMetadata(*currentBytes) : std::nullopt;
        if (!current || current->version != candidate.metadata.version || current->authorityNodeId != candidate.metadata.authorityNodeId) {
            continue;
        }
        cluster::detail::validateStripeFaultDomains(currentPlacement(), candidate.metadata.nodeIds);
        candidate.metadata.shardPresent = std::move(observed);
        const auto encoded = encodeStripeMetadata(candidate.metadata);
        (void)clusterRuntime->repairStripeMetadata(candidate.key, candidate.metadata.version, encoded);
    }
}

void startStripeGarbageCollector() {
    if (clusterReplicationMode != cluster::ReplicationMode::STRIPE) { return; }
    stripeGcThread = std::jthread(
        [this](const std::stop_token& stop) {
            auto nextGarbageCollection = std::chrono::steady_clock::now();
            auto nextRebuild = std::chrono::steady_clock::now();
            while (!stop.stop_requested()) {
                try { collectStripeGarbage(); }
                catch (const cluster::StripeMetadataUnavailable&) {
                    std::this_thread::sleep_for(std::chrono::milliseconds{50});
                    continue;
                }
                catch (...) {
                    failStripe(std::current_exception());
                    return;
                }
                const auto now = std::chrono::steady_clock::now();
                if (now >= nextRebuild) {
                    try { rebuildStripeShardsBatch(); }
                    catch (const cluster::StripeMetadataUnavailable&) {
                        nextRebuild = now + std::chrono::milliseconds{opts.cluster.runtime.stripeRebuildIntervalMs};
                        continue;
                    }
                    catch (...) {
                        failStripe(std::current_exception());
                        return;
                    }
                    nextRebuild = now + std::chrono::milliseconds{opts.cluster.runtime.stripeRebuildIntervalMs};
                }
                nextGarbageCollection = now + std::chrono::milliseconds{500};
                while (!stop.stop_requested() && std::chrono::steady_clock::now() < nextGarbageCollection) {
                    std::this_thread::sleep_for(std::chrono::milliseconds{50});
                }
            }
        }
    );
}

void reconfigureCluster(cluster::ClusterConfig config) { reconfigurePlacement(std::move(config)); }

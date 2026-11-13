/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/detail/AkkEngineStripeRead.inc
// Included inside AkkEngine::Impl by AkkEngineImpl.hpp.
[[nodiscard]] erasure::ErasureLayout stripeLayoutFor(const cluster::ClusterConfig& config) const {
    return erasure::ErasureLayout{.dataShards = config.stripe().dataShards, .parityShards = config.stripe().parityShards,};
}

[[nodiscard]] std::vector<uint64_t> stripeNodeIdsFor(
    const cluster::ClusterConfig& config,
    std::span<const uint8_t> key,
    uint64_t excludedNodeId = 0,
    uint64_t replacementNodeId = 0
) const {
    const uint16_t totalShards = config.stripe().totalShards();
    const uint16_t totalPlacements = config.stripe().totalPlacements();
    if (totalShards == 0) { throw std::runtime_error("AkkEngine: invalid STRIPE shard count"); }
    const auto dataNodes = config.stripePlacementNodes();
    if (dataNodes.size() < totalPlacements) { throw std::runtime_error("AkkEngine: not enough data-bearing nodes for STRIPE"); }

    if (config.stripe().copiesPerShard == 2) {
        struct MirrorPair {
            uint64_t score = 0;
            uint64_t first = 0;
            uint64_t second = 0;
        };
        const auto paired = cluster::detail::raidPairNodes(config);
        std::vector<MirrorPair> pairs;
        pairs.reserve(totalShards);
        for (uint16_t i = 0; i < totalShards; ++i) {
            uint64_t first = paired[i * 2].nodeId;
            uint64_t second = paired[i * 2 + 1].nodeId;
            const uint64_t firstScore = rendezvousScore(key, first);
            const uint64_t secondScore = rendezvousScore(key, second);
            if (secondScore > firstScore || (secondScore == firstScore && second < first)) { std::swap(first, second); }
            const auto [minId, maxId] = std::minmax(first, second);
            pairs.push_back(MirrorPair{.score = rendezvousScore(key, minId, maxId), .first = first, .second = second});
        }
        std::ranges::sort(
            pairs,
            [](const auto& left, const auto& right) {
                if (left.score != right.score) { return left.score > right.score; }
                return std::min(left.first, left.second) < std::min(right.first, right.second);
            }
        );
        std::vector<uint64_t> ids;
        ids.reserve(totalPlacements);
        for (const auto& [score, first, second] : pairs) {
            ids.push_back(first);
            ids.push_back(second);
        }
        if (excludedNodeId != 0) {
            if (replacementNodeId == 0 && clusterRuntime) { replacementNodeId = clusterRuntime->stripeFailoverNodeId(key); }
            const auto replaced = std::ranges::find(ids, excludedNodeId);
            if (!config.isStripeFailoverNode(replacementNodeId) || replaced == ids.end()) {
                throw std::runtime_error("AkkEngine: RAID.10 hot spare cannot replace the unavailable owner");
            }
            *replaced = replacementNodeId;
        }
        cluster::detail::validateStripeFaultDomains(config, ids);
        return ids;
    }

    auto candidates = dataNodes;
    std::erase_if(candidates, [&](const auto& target) { return target.nodeId == excludedNodeId; });
    const auto selected = cluster::detail::placementNodes(key, candidates, totalShards, config.faultDomains());
    std::vector<uint64_t> ids;
    for (const auto& target : selected) { ids.push_back(target.nodeId); }
    cluster::detail::validateStripeFaultDomains(config, ids);
    return ids;
}

[[nodiscard]] uint64_t stripeOwnerFor(const cluster::ClusterConfig& config, std::span<const uint8_t> key) const {
    const auto ids = stripeNodeIdsFor(config, key);
    if (ids.empty()) { throw std::runtime_error("AkkEngine: STRIPE key has no owner"); }
    return ids.front();
}

void checkStripeHealthy() const {
    std::lock_guard lock{stripeStateMu};
    if (stripeFailure) { std::rethrow_exception(stripeFailure); }
}

void failStripe(const std::exception_ptr& failure) {
    std::lock_guard lock{stripeStateMu};
    if (!stripeFailure) { stripeFailure = failure; }
}

static std::vector<uint8_t> stripeTransactionKey(std::span<const uint8_t> key, uint64_t version) {
    // Intent and shard keys retain their generation-oriented format.
    std::vector<uint8_t> out{0, 'A', 'K', 'S', 'T', '1'};
    pushU32(out, key.size());
    out.append_range(key);
    pushU64(out, version);
    return out;
}

uint64_t writeStripeLocal(
    std::span<const uint8_t> key,
    std::span<const uint8_t> value,
    cluster::ReplOpType op = cluster::ReplOpType::PUT,
    uint64_t seq = 0,
    uint64_t sourceNodeId = 0,
    uint8_t versionLogFlags = 0xFF
) {
    validateStoredRecord(key.size(), value.size());
    std::lock_guard lock{writeMu};
    try {
        if (seq == 0) { seq = memtable->reserveSeq(1); }
        const uint8_t flags = op == cluster::ReplOpType::REMOVE ? MemHdr16::FLAG_TOMBSTONE : MemHdr16::FLAG_NORMAL;
        appendAll(seq, key, value, flags, sourceNodeId == 0 ? nodeId : sourceNodeId, 0, 0, versionLogFlags, true);
        forceClusterLocalDurable();
        return seq;
    }
    catch (...) {
        failStripe(std::current_exception());
        throw;
    }
}

void writeStripeRecordToNode(
    uint64_t targetNodeId,
    std::span<const uint8_t> key,
    std::span<const uint8_t> value,
    uint64_t version,
    cluster::ReplOpType op = cluster::ReplOpType::PUT,
    uint64_t logicalSourceNodeId = 0
) {
    validateStoredRecord(key.size(), value.size());
    cluster::detail::ReconfigurationDeadline::check();
    if (targetNodeId == nodeId) {
        writeStripeLocal(key, value, op);
        cluster::detail::ReconfigurationDeadline::check();
        return;
    }
    if (!clusterRuntime) { throw std::runtime_error("AkkEngine: STRIPE write requires cluster runtime"); }
    const uint64_t sourceNodeId = logicalSourceNodeId == 0 ? nodeId : logicalSourceNodeId;
    const auto deadline = cluster::detail::ReconfigurationDeadline::cap(
        std::chrono::steady_clock::now() + std::chrono::milliseconds{std::max<uint32_t>(1, clusterConfig.consistency().ackTimeoutMs)}
    );
    while (true) {
        try {
            clusterRuntime->shipEntryTo(targetNodeId, version, op, key, value, MemHdr16::FLAG_NORMAL, sourceNodeId);
            return;
        }
        catch (...) {
            cluster::detail::ReconfigurationDeadline::check();
            if (std::chrono::steady_clock::now() >= deadline) { throw; }
            std::this_thread::sleep_for(std::chrono::milliseconds{25});
        }
    }
}

[[nodiscard]] std::optional<std::vector<uint8_t>> readStripeRecordFromNode(uint64_t targetNodeId, std::span<const uint8_t> key) {
    if (targetNodeId == nodeId) { return getValueInternal(key, false); }
    if (!clusterRuntime) { throw std::runtime_error("AkkEngine: STRIPE read requires cluster runtime"); }
    const auto response = clusterRuntime->readLocalStorageFromNode(targetNodeId, key, 0);
    if (response.status == cluster::ReadStatus::NOT_FOUND) { return std::nullopt; }
    if (response.status != cluster::ReadStatus::FOUND) {
        throw std::runtime_error(
            "AkkEngine: STRIPE node read failed: " + std::to_string(targetNodeId) + "/" + std::to_string(static_cast<unsigned>(response.status))
        );
    }
    return response.value;
}

[[nodiscard]] std::optional<StripeMetadata> readStripeMetadata(std::span<const uint8_t> publicKey) {
    checkStripeHealthy();
    const auto owner = stripeOwnerFor(currentPlacement(), publicKey);
    if (!clusterRuntime) { throw std::runtime_error("AkkEngine: STRIPE metadata Raft runtime is unavailable"); }
    const auto value = clusterRuntime->readStripeMetadata(publicKey, owner);
    if (!value) { return std::nullopt; }
    auto metadata = decodeStripeMetadata(*value);
    if (!metadata || metadata->version == 0 || metadata->ownerNodeId != owner || metadata->authorityNodeId == 0 || metadata->fenceToken == 0 ||
        metadata->nodeIds.empty() || metadata->shardPresent.size() != metadata->nodeIds.size() || metadata->layout.dataShards != currentPlacement().
        stripe().dataShards || metadata->layout.parityShards != currentPlacement().stripe().parityShards || metadata->copiesPerShard !=
        currentPlacement().stripe().copiesPerShard || (metadata->authorityNodeId != owner && !currentPlacement().isStripeFailoverNode(
            metadata->authorityNodeId
        ))) { throw std::runtime_error("AkkEngine: corrupt STRIPE metadata"); }
    const auto expectedNodes = stripeNodeIdsFor(
        currentPlacement(),
        publicKey,
        metadata->authorityNodeId == owner ? 0 : owner,
        metadata->authorityNodeId
    );
    bool validPlacement = metadata->nodeIds == expectedNodes;
    if (!validPlacement && metadata->copiesPerShard == 2 && metadata->nodeIds.size() == expectedNodes.size()) {
        const auto placementView = currentPlacement();
        size_t replacements = 0;
        std::unordered_set<uint64_t> used;
        validPlacement = true;
        for (size_t i = 0; validPlacement && i < expectedNodes.size(); ++i) {
            if (!used.insert(metadata->nodeIds[i]).second) {
                validPlacement = false;
                break;
            }
            if (metadata->nodeIds[i] == expectedNodes[i]) { continue; }
            validPlacement = placementView.isStripeFailoverNode(metadata->nodeIds[i]);
            ++replacements;
        }
        validPlacement = validPlacement && replacements != 0;
    }
    if (!validPlacement) { throw std::runtime_error("AkkEngine: invalid STRIPE shard placement metadata"); }
    cluster::detail::validateStripeFaultDomains(currentPlacement(), metadata->nodeIds);
    return metadata;
}

void repairStripeShards(
    std::span<const uint8_t> publicKey,
    const StripeMetadata& metadata,
    const std::vector<erasure::ErasureShard>& available,
    const std::vector<bool>& present
) {
    if (!opts.cluster.runtime.stripeReadRepair || (metadata.layout.parityShards == 0 && metadata.copiesPerShard == 1) || metadata.tombstone ||
        metadata.authorityNodeId != nodeId) { return; }
    // Only the owner repairs, under the key lock, so repair cannot
    // resurrect a retired generation after the owner's GC.
    for (size_t placement = 0; placement < metadata.nodeIds.size(); ++placement) {
        if (placement < present.size() && present[placement]) { continue; }
        const uint16_t shardIndex = placement / metadata.copiesPerShard;
        {
            std::lock_guard lock{stripeRepairStatsMu};
            ++stripeRepairStats.attempts;
        }
        try {
            erasure::ErasureShard repaired;
            if (metadata.copiesPerShard == 2) {
                const auto source = std::ranges::find(available, shardIndex, &erasure::ErasureShard::index);
                if (source == available.end()) { throw std::runtime_error("AkkEngine: missing RAID.10 mirror source"); }
                repaired = *source;
            }
            else { repaired = erasure::RsErasureCodec::repairOne(shardIndex, available, metadata.layout); }
            writeStripeRecordToNode(
                metadata.nodeIds[placement],
                stripeShardKey(publicKey, metadata.version, shardIndex),
                encodeStripeShard(metadata.version, repaired, metadata.layout),
                metadata.version
            );
            std::lock_guard lock{stripeRepairStatsMu};
            ++stripeRepairStats.succeeded;
        }
        catch (...) {
            std::lock_guard lock{stripeRepairStatsMu};
            ++stripeRepairStats.failed;
            stripeRepairStats.lastFailureNodeId = metadata.nodeIds[placement];
        }
    }
}

struct StripeShardSet {
    std::vector<erasure::ErasureShard> available;
    std::vector<bool> present;
};

[[nodiscard]] StripeShardSet readStripeShards(
    std::span<const uint8_t> publicKey,
    const StripeMetadata& metadata,
    cluster::detail::BoundedExecutor* migrationExecutor = nullptr
) {
    StripeShardSet out;
    out.present.assign(metadata.nodeIds.size(), false);
    std::vector<std::optional<erasure::ErasureShard>> decoded(metadata.nodeIds.size());
    const auto budget = cluster::detail::ReconfigurationDeadline::current();
    auto* executor = migrationExecutor ? migrationExecutor : stripeExecutor.get();
    executor->forEach(
        metadata.nodeIds.size(),
        [&](size_t placement) {
            cluster::detail::ReconfigurationDeadline scope{budget};
            const uint16_t shardIndex = placement / metadata.copiesPerShard;
            std::optional<std::vector<uint8_t>> stored;
            try { stored = readStripeRecordFromNode(metadata.nodeIds[placement], stripeShardKey(publicKey, metadata.version, shardIndex)); }
            catch (const std::runtime_error&) { return; }
            if (!stored) { return; }
            auto shard = decodeStripeShard(*stored, metadata.version, metadata.layout);
            if (shard && shard->index == shardIndex && shard->originalSize == metadata.originalSize) { decoded[placement] = std::move(shard); }
        }
    );
    std::vector<bool> logicalPresent(metadata.layout.totalShards(), false);
    for (size_t placement = 0; placement < decoded.size(); ++placement) {
        if (!decoded[placement]) { continue; }
        const auto index = decoded[placement]->index;
        out.present[placement] = true;
        if (!logicalPresent[index]) {
            logicalPresent[index] = true;
            out.available.push_back(std::move(*decoded[placement]));
        }
    }
    return out;
}

[[nodiscard]] std::optional<std::vector<uint8_t>> readStripeValueFromMetadata(
    std::span<const uint8_t> publicKey,
    const StripeMetadata& metadata,
    bool repair,
    cluster::detail::BoundedExecutor* migrationExecutor = nullptr
) {
    if (metadata.tombstone) { return std::nullopt; }
    if (metadata.nodeIds.size() != metadata.layout.totalShards() * metadata.copiesPerShard) {
        throw std::runtime_error("AkkEngine: corrupt STRIPE metadata");
    }
    auto shards = readStripeShards(publicKey, metadata, migrationExecutor);
    if (shards.available.size() < metadata.layout.dataShards) {
        throw std::runtime_error("AkkEngine: not enough STRIPE shards to reconstruct value");
    }
    auto value = erasure::RsErasureCodec::decode(shards.available, metadata.layout);
    if (repair) { repairStripeShards(publicKey, metadata, shards.available, shards.present); }
    return value;
}

[[nodiscard]] std::optional<std::vector<uint8_t>> readStripeValue(std::span<const uint8_t> publicKey) {
    auto keyLock = stripeKeys.lock(publicKey);
    checkStripeHealthy();
    const uint64_t originalOwner = stripeOwnerFor(currentPlacement(), publicKey);
    const bool coordinatedRead = clusterRuntime && (nodeId == originalOwner || (currentPlacement().isStripeFailoverNode(nodeId) && !clusterRuntime->
        stripeNodeReachable(originalOwner)));
    auto lease = coordinatedRead
                     ? clusterRuntime->acquireStripeOperation(publicKey, originalOwner)
                     : cluster::StripeOperationLease{
                         .ownerNodeId = originalOwner,
                         .authorityNodeId = originalOwner,
                         .fenceToken = 0,
                         .coordinated = false,
                         .metadata = {},
                     };
    if (!lease.coordinated) {
        for (unsigned attempt = 0; attempt < 8; ++attempt) {
            const auto metadata = readStripeMetadata(publicKey);
            if (!metadata || metadata->tombstone) { return std::nullopt; }
            std::optional<std::vector<uint8_t>> value;
            std::exception_ptr failure;
            try { value = readStripeValueFromMetadata(publicKey, *metadata, metadata->authorityNodeId == nodeId); }
            catch (...) { failure = std::current_exception(); }
            const auto current = readStripeMetadata(publicKey);
            if (!current || current->version != metadata->version) { continue; }
            if (failure) { std::rethrow_exception(failure); }
            return value;
        }
        throw std::runtime_error("AkkEngine: STRIPE generation changed repeatedly during read");
    }
    try {
        auto metadata = lease.metadata.empty() ? std::optional<StripeMetadata>{} : decodeStripeMetadata(lease.metadata);
        if (!lease.metadata.empty() && !metadata) { throw std::runtime_error("AkkEngine: corrupt fenced STRIPE metadata"); }
        if (metadata && metadata->ownerNodeId != originalOwner) { throw std::runtime_error("AkkEngine: fenced STRIPE metadata owner mismatch"); }
        if (!metadata || metadata->tombstone) {
            if (clusterRuntime) { clusterRuntime->releaseStripeOperation(lease, publicKey); }
            return std::nullopt;
        }
        auto value = readStripeValueFromMetadata(publicKey, *metadata, lease.authorityNodeId == nodeId);
        if (clusterRuntime) { clusterRuntime->releaseStripeOperation(lease, publicKey); }
        return value;
    }
    catch (...) {
        if (clusterRuntime) { clusterRuntime->releaseStripeOperation(lease, publicKey); }
        throw;
    }
}

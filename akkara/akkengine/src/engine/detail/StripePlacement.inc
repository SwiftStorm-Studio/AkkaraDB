// Included inside AkkEngine::Impl. Placement aliases preserve public revisions.
cluster::ClusterConfig currentPlacement() const {
    const auto view = livePlacement.load(std::memory_order_acquire);
    return view ? *view : clusterConfig;
}

template <typename Visitor>
void forEachStripeMigrationRecord(Visitor&& visit) {
    auto retention = versionLog ? versionLog->captureRecords() : vlog::VersionLog::RecordSnapshot{};
    core::BufferArena arena;
    memtable::MemTable::KeyRange range;
    const auto visible = snapshotSeq();
    auto mt = memtable->iterator(range, visible);
    sst::SSTManager::Iterator sst;
    if (sstManager) { sst = sstManager->scanIter({}, {}, visible); }
    for (const auto& record : scanGenerator(arena, std::move(mt), std::move(sst), blobManager.get())) {
        cluster::detail::ReconfigurationDeadline::check();
        auto key = publicKeyFromStripeMetaKey(record.key);
        if (!key) { continue; }
        std::map<uint64_t, StripeMetadata> revisions;
        if (versionLog) {
            for (const auto& entry : versionLog->history(stripeMetaKey(*key))) {
                auto metadata = decodeStripeMetadata(entry.value);
                if (!metadata) { throw std::runtime_error("Cluster placement: malformed stripe history"); }
                revisions[metadata->logicalSeq] = effectiveStripeMetadata(*key, *metadata);
            }
        }
        auto head = decodeStripeMetadata(record.value);
        if (!head) { throw std::runtime_error("Cluster placement: malformed stripe head"); }
        revisions[head->logicalSeq] = effectiveStripeMetadata(*key, *head);
        for (const auto& [sequence, metadata] : revisions) { (void)sequence; visit(*key, metadata); }
    }
}

void reconfigureStripePlacement(cluster::ClusterConfig target) {
    auto state = clusterRuntime->placementState(true);
    if (state.leaderNodeId != nodeId) {
        if (opts.cluster.runtime.routingMode != cluster::ClusterRoutingMode::FORWARD) {
            cluster::ClusterRouteTarget destination; destination.nodeId = state.leaderNodeId;
            throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::NOT_OWNER, destination);
        }
        auto command = partitionAdminHeader(PartitionAdmin::RECONFIGURE, 0, 0);
        const auto image = target.encode(); command.insert(command.end(), image.begin(), image.end());
        (void)partitionRpc(state.leaderNodeId, std::move(command), opts.cluster.runtime.reconfiguration.timeoutMs);
        return;
    }
    const auto active = cluster::ClusterConfig::decode(state.activeConfig);
    active.validatePlacementChange(target);
    if (opts.cluster.runtime.transportMode == cluster::TransportMode::SECURE) {
        std::vector<uint64_t> ids;
        for (const auto& node : target.dataNodes()) { if (node.nodeId != nodeId) { ids.push_back(node.nodeId); } }
        opts.cluster.runtime.secure.validatePins(ids);
    }
    const auto generation = clusterRuntime->beginPlacementChange(target);
    state = clusterRuntime->placementState(true);
    if (state.pendingGeneration == 0) { return; }
    auto participants = active.dataNodes();
    for (const auto& node : target.dataNodes()) {
        if (std::ranges::none_of(participants, [&](const auto& old) { return old.nodeId == node.nodeId; })) { participants.push_back(node); }
    }
    for (const auto& node : participants) {
        try { (void)partitionRpc(node.nodeId, partitionAdminHeader(PartitionAdmin::PREPARE, 0, generation)); }
        catch (const cluster::ClusterRoutingError& error) {
            const auto* retained = target.findById(node.nodeId);
            if ((retained && retained->dataBearing()) ||
                (error.code != cluster::ClusterRoutingCode::FORWARD_UNAVAILABLE &&
                 error.code != cluster::ClusterRoutingCode::OUTCOME_UNKNOWN)) { throw; }
            // An unavailable retired node need not prepare. The committed intent
            // already fences publication, and reconstruction checks source availability.
        }
    }
    if (state.stripeWritesBlocked) { clusterRuntime->waitForStripeOperations(); }
    const auto started = std::chrono::steady_clock::now();
    uint64_t transferred = 0;
    stripeMigrationBytes.store(0, std::memory_order_relaxed);
    stripeMigrationRecords.store(0, std::memory_order_relaxed);
    // Migration cannot queue behind unrelated owner RPCs with longer budgets.
    cluster::detail::BoundedExecutor migrationExecutor{8, 64};
    const auto check = [&] {
        cluster::detail::ReconfigurationDeadline::check();
        if (placementClosing() ||
            (opts.cluster.runtime.reconfiguration.cancelled && opts.cluster.runtime.reconfiguration.cancelled())) {
            throw std::runtime_error("Cluster placement: stripe transfer interrupted; committed intent remains resumable");
        }
    };
    const auto copy = [&](const auto& key, const StripeMetadata& original) {
        check();
        // Staged aliases are durable checkpoints, scoped to this exact plan.
        const auto checkpoint = getValueInternal(stripeHistoryPlacementKey(key, original.logicalSeq, generation), false);
        if (checkpoint) {
            if (checkpoint->size() < 8) { throw std::runtime_error("Cluster placement: truncated migration checkpoint"); }
            auto copied = decodeStripeMetadata(std::span{*checkpoint}.subspan(8));
            if (!copied || copied->logicalSeq != original.logicalSeq || copied->version != original.version ||
                copied->originNodeId != original.originNodeId || copied->timestampNs != original.timestampNs) {
                throw std::runtime_error("Cluster placement: invalid migration checkpoint");
            }
            return;
        }
        auto next = original;
        next.ownerNodeId = stripeOwnerFor(target, key);
        next.authorityNodeId = next.ownerNodeId;
        next.nodeIds = stripeNodeIdsFor(target, key);
        next.shardPresent.assign(next.nodeIds.size(), 0);
        if (!next.tombstone) {
            const auto value = readStripeValueFromMetadata(key, original, false, &migrationExecutor);
            if (!value) { throw std::runtime_error("Cluster placement: historical stripe value unavailable"); }
            const auto shards = erasure::RsErasureCodec::encode(*value, next.layout);
            for (size_t i = 0; i < next.nodeIds.size(); ++i) {
                check();
                const auto index = static_cast<uint16_t>(i / next.copiesPerShard);
                const auto encoded = encodeStripeShard(next.version, shards[index], next.layout);
                if (encoded.size() > UINT64_MAX - transferred) { throw std::overflow_error("Cluster placement: transfer counter exhausted"); }
                transferred += encoded.size();
                const auto rate = opts.cluster.runtime.reconfiguration.maxTransferBytesPerSecond;
                if (rate != 0) {
                    const auto scheduled = started + std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                        std::chrono::duration<double>{static_cast<double>(transferred) / static_cast<double>(rate)});
                    while (std::chrono::steady_clock::now() < scheduled) { check(); std::this_thread::sleep_for(std::chrono::milliseconds{25}); }
                }
                writeStripeRecordToNode(next.nodeIds[i], stripeShardKey(key, next.version, index), encoded,
                    next.version, cluster::ReplOpType::PUT, next.authorityNodeId);
                stripeMigrationBytes.store(transferred, std::memory_order_relaxed);
                next.shardPresent[i] = 1;
            }
        }
        clusterRuntime->publishStripePlacementAlias(key, encodeStripeMetadata(next), generation);
        stripeMigrationRecords.fetch_add(1, std::memory_order_relaxed);
    };
    forEachStripeMigrationRecord(copy);
    if (!state.stripeWritesBlocked) {
        clusterRuntime->freezeStripePlacement(generation);
        clusterRuntime->waitForStripeOperations();
        // Only revisions published during the bulk pass need another transfer.
        forEachStripeMigrationRecord(copy);
    }
    check();
    clusterRuntime->finishPlacementChange(generation);
    { std::lock_guard progress{placementProgressMu}; placementLastError.clear(); }
}

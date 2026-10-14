// Included inside AkkEngine::Impl. Placement aliases preserve public revisions.
cluster::ClusterConfig currentPlacement() const {
    const auto view = livePlacement.load(std::memory_order_acquire);
    return view ? *view : clusterConfig;
}

core::ArenaGenerator<OwnedQueryRecord> stripeMigrationRevisionStream(FixedQuerySource source, vlog::VersionLog::RecordSnapshot retention) {
    (void)retention;
    auto storagePin = source.storage;
    for (const auto& record : fixedQueryRecords(std::move(source))) {
        cluster::detail::ReconfigurationDeadline::check();
        auto key = publicKeyFromStripeMetaKey(record.key);
        if (!key) { continue; }
        std::map<uint64_t, StripeMetadata> revisions;
        if (storagePin->versionLog) {
            for (const auto& entry : storagePin->versionLog->history(record.key)) {
                auto metadata = decodeStripeMetadata(entry.value);
                if (!metadata) { throw std::runtime_error("Cluster placement: malformed stripe history"); }
                revisions[metadata->logicalSeq] = effectiveStripeMetadata(*key, *metadata);
            }
        }
        auto head = decodeStripeMetadata(record.value);
        if (!head) { throw std::runtime_error("Cluster placement: malformed stripe head"); }
        revisions[head->logicalSeq] = effectiveStripeMetadata(*key, *head);
        for (const auto& [sequence, metadata] : revisions) {
            (void)sequence;
            co_yield OwnedQueryRecord{*key, encodeStripeMetadata(metadata), 0, nullptr};
        }
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
        for (const auto& node : target.stripeMetadataNodes()) { if (node.nodeId != nodeId) { ids.push_back(node.nodeId); } }
        opts.cluster.runtime.secure.validatePins(ids);
    }
    const auto generation = clusterRuntime->beginPlacementChange(target);
    state = clusterRuntime->placementState(true);
    if (state.pendingGeneration == 0) { return; }
    auto participants = active.stripeMetadataNodes();
    for (const auto& node : target.stripeMetadataNodes()) {
        if (std::ranges::none_of(participants, [&](const auto& old) { return old.nodeId == node.nodeId; })) { participants.push_back(node); }
    }
    for (const auto& node : participants) {
        try { (void)partitionRpc(node.nodeId, partitionAdminHeader(PartitionAdmin::PREPARE, 0, generation)); }
        catch (const cluster::ClusterRoutingError& error) {
            const auto* retained = target.findById(node.nodeId);
            if (retained ||
                (error.code != cluster::ClusterRoutingCode::FORWARD_UNAVAILABLE &&
                 error.code != cluster::ClusterRoutingCode::OUTCOME_UNKNOWN)) { throw; }
            // An unavailable retired node need not prepare. The committed intent
            // already fences publication, and reconstruction checks source availability.
        }
    }
    if (state.stripeWritesBlocked) { clusterRuntime->waitForStripeOperations(); }
    const auto finish = [&] {
        clusterRuntime->finishPlacementChange(generation);
        std::lock_guard progress{placementProgressMu}; placementLastError.clear();
    };
    // Activation intent is durable only after the frozen union was copied.
    // Removed replicas may no longer receive later Raft cuts during recovery.
    if (state.activationStarted) { finish(); return; }
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
            cluster::detail::validateStripeFaultDomains(target, copied->nodeIds);
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
    const auto copyRetainedRevisions = [&] {
        const auto cut = clusterRuntime->stripeMetadataLinearizableWatermark();
        // A peer may retain older revisions than this leader, independently of
        // its VersionLog policy. Each source must observe this migration cut.
        for (const auto& peer : active.stripeMetadataNodes()) {
            cluster::ForwardRequest request;
            auto& value = request.entries.emplace_back().value;
            value.push_back(static_cast<uint8_t>(QueryAction::STRIPE_REVISIONS));
            pushU64(value, generation); pushU64(value, state.generation); pushU64(value, cut);
            QueryReader reader{*this, peer.nodeId, std::move(request)};
            while (auto record = reader.nextRecord()) {
                const auto metadata = decodeStripeMetadata(record->second);
                if (!metadata || !metadata->logicalSeq) { throw std::runtime_error("Cluster placement: invalid retained peer revision"); }
                copy(record->first, *metadata);
            }
        }
    };
    copyRetainedRevisions();
    if (!state.stripeWritesBlocked) {
        clusterRuntime->freezeStripePlacement(generation);
        clusterRuntime->waitForStripeOperations();
        // Only revisions published during the bulk pass need another transfer.
        copyRetainedRevisions();
    }
    check();
    finish();
}

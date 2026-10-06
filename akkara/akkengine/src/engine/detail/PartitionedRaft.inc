// Included inside AkkEngine::Impl. Each stable partition has an independent
// storage sequence and Raft journal; holder count bounds data replication.
std::vector<std::shared_ptr<AkkEngine>> partitionEngines;
mutable std::shared_mutex partitionEnginesMu;
std::unique_ptr<cluster::detail::BoundedExecutor> partitionQueryExecutor;
std::atomic<std::shared_ptr<const cluster::ClusterConfig>> livePlacement;
std::timed_mutex clusterReconfigurationMu;
std::timed_mutex partitionCreationMu;
std::atomic<uint64_t> activePlacementGeneration{1};
std::array<std::atomic<uint64_t>, 256> partitionLeaderHints{};
std::jthread placementRecoveryThread;
mutable std::mutex placementProgressMu;
std::string placementLastError;
std::atomic<uint64_t> stripeProtectionGeneration{0};
std::atomic<uint64_t> stripeMigrationBytes{0}, stripeMigrationRecords{0};

enum class PartitionAdmin : uint8_t { RECONFIGURE, PREPARE, MEMBERSHIP, STATUS, WATERMARK, ROLLBACK, CANCEL };

static std::vector<uint8_t> partitionAdminHeader(PartitionAdmin action, uint16_t index, uint64_t generation) {
    std::vector<uint8_t> out{static_cast<uint8_t>(action), static_cast<uint8_t>(index), static_cast<uint8_t>(index >> 8)};
    for (size_t i = 0; i < 8; ++i) { out.push_back(static_cast<uint8_t>(generation >> (i * 8))); }
    return out;
}

cluster::ForwardResponse partitionRpc(uint64_t node, std::vector<uint8_t> command, uint32_t timeout = 20000) {
    cluster::ForwardRequest request;
    request.operation = cluster::ForwardOperation::CLUSTER_ADMIN;
    request.timeoutMs = cluster::detail::ReconfigurationDeadline::remaining(timeout);
    request.entries.push_back({{}, std::move(command)});
    auto response = node == nodeId ? receivePartitionAdmin(request) : clusterRuntime->forwardTo(node, std::move(request));
    if (!response.success) { throw cluster::ClusterRoutingError(response.errorCode, response.target, response.message); }
    response.target.nodeId = node;
    return response;
}

bool placementClosing() const {
    std::lock_guard lock{lifecycleMu};
    return closing;
}

cluster::ForwardResponse partitionRpcLeader(uint16_t index, std::vector<uint8_t> command,
    const cluster::ClusterConfig& placement, std::chrono::steady_clock::time_point deadline) {
    const auto group = partitionConfig(placement, index);
    auto candidates = group.dataNodes();
    uint64_t preferred = 0;
    std::exception_ptr last;
    do {
        if (preferred != 0) {
            const auto found = std::ranges::find(candidates, preferred, &cluster::NodeInfo::nodeId);
            if (found != candidates.end()) { std::rotate(candidates.begin(), found, found + 1); }
            else {
                auto nodes = clusterRuntime->activeNodes();
                const auto node = std::ranges::find(nodes, preferred, &cluster::NodeInfo::nodeId);
                if (node != nodes.end()) { candidates.insert(candidates.begin(), *node); }
            }
        }
        cluster::detail::ReconfigurationDeadline::check();
        for (const auto& node : candidates) {
            if (std::chrono::steady_clock::now() >= deadline) { break; }
            const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now()).count();
            try { return partitionRpc(node.nodeId, command, static_cast<uint32_t>(std::min<int64_t>(remaining, 20000))); }
            catch (const cluster::ClusterRoutingError& error) {
                if (command[0] == static_cast<uint8_t>(PartitionAdmin::ROLLBACK) && error.outcomeUnknown()) { throw; }
                preferred = error.target.nodeId; last = std::current_exception();
            }
            catch (...) { last = std::current_exception(); }
        }
        if (placementClosing()) { throw std::runtime_error("Cluster placement: engine is closing"); }
        std::this_thread::sleep_for(std::chrono::milliseconds{50});
    } while (std::chrono::steady_clock::now() < deadline);
    if (last) { std::rethrow_exception(last); }
    throw std::runtime_error("Cluster placement: partition leader unavailable");
}

cluster::ForwardResponse receivePartitionAdmin(const cluster::ForwardRequest& request) {
    if ((!usesPartitionRaft() && clusterReplicationMode != cluster::ReplicationMode::STRIPE) || request.entries.size() != 1 || !request.entries[0].key.empty() || request.entries[0].value.size() < 11) {
        throw std::invalid_argument("Cluster placement: invalid administration request");
    }
    cluster::detail::ReconfigurationDeadline requestBudget{request.timeoutMs};
    const auto& bytes = request.entries[0].value;
    const auto action = static_cast<PartitionAdmin>(bytes[0]);
    if (action > PartitionAdmin::CANCEL ||
        (action != PartitionAdmin::RECONFIGURE && action != PartitionAdmin::ROLLBACK && bytes.size() != 11)) {
        throw std::invalid_argument("Cluster placement: malformed administration payload");
    }
    const uint16_t index = static_cast<uint16_t>(bytes[1] | (uint16_t{bytes[2]} << 8));
    uint64_t generation = 0;
    for (size_t i = 0; i < 8; ++i) { generation |= uint64_t{bytes[3 + i]} << (i * 8); }
    cluster::ForwardResponse response;
    response.success = true;
    if (action == PartitionAdmin::CANCEL) {
        response.value = {static_cast<uint8_t>(cancelPlacement(generation))};
        return response;
    }
    if (action == PartitionAdmin::RECONFIGURE) {
        reconfigurePlacement(cluster::ClusterConfig::decode(std::span{bytes}.subspan(11)));
        return response;
    }
    if (clusterReplicationMode == cluster::ReplicationMode::STRIPE) {
        if (index == 0 && action == PartitionAdmin::STATUS) {
            const auto state = clusterRuntime->placementState(true);
            if (state.leaderNodeId != nodeId) { throw std::runtime_error("Cluster placement: stripe progress requires its authority"); }
            pushU64(response.value, stripeMigrationBytes.load(std::memory_order_relaxed));
            pushU64(response.value, stripeMigrationRecords.load(std::memory_order_relaxed));
            return response;
        }
        if (index != 0 || action != PartitionAdmin::PREPARE) { throw std::invalid_argument("Cluster placement: invalid stripe administration action"); }
        const auto state = clusterRuntime->placementState(true);
        if (state.pendingGeneration != generation) { throw std::runtime_error("Cluster placement: stale stripe preparation"); }
        stripeProtectionGeneration.store(generation, std::memory_order_release);
        clusterRuntime->preparePlacementNodes(cluster::ClusterConfig::decode(state.pendingConfig));
        return response;
    }
    if (index >= clusterConfig.partition().partitionCount) { throw std::invalid_argument("Cluster placement: invalid partition"); }
    if (action == PartitionAdmin::PREPARE || action == PartitionAdmin::MEMBERSHIP) {
        const auto state = clusterRuntime->placementState(true);
        if (state.pendingGeneration != generation) {
            if (state.pendingGeneration == 0 && state.generation == generation) { return response; }
            throw std::runtime_error("Cluster placement: stale administration generation");
        }
        const auto target = cluster::ClusterConfig::decode(state.pendingConfig);
        if (action == PartitionAdmin::PREPARE) {
            if (!partitionConfig(target, index).findById(nodeId)) { throw std::invalid_argument("Cluster placement: unexpected holder"); }
            auto creation = cluster::detail::ReconfigurationDeadline::lock(partitionCreationMu);
            { std::shared_lock lock{partitionEnginesMu}; if (partitionEngines[index]) { return response; } }
            auto child = openPartition(index, cluster::ClusterConfig::decode(state.activeConfig), true);
            std::unique_lock lock{partitionEnginesMu};
            partitionEngines[index] = std::move(child);
            return response;
        }
        auto children = partitionSnapshot();
        if (!children[index]) { throw std::runtime_error("Cluster placement: partition is not hosted here"); }
        children[index]->impl_->clusterRuntime->reconfigure(partitionConfig(target, index), generation);
        return response;
    }
    auto children = partitionSnapshot();
    if (!children[index]) { throw std::runtime_error("Cluster placement: partition is not hosted here"); }
    auto& child = *children[index];
    if (child.impl_->clusterRuntime->role() != cluster::NodeRole::PRIMARY) {
        cluster::ClusterRouteTarget target;
        target.nodeId = child.impl_->clusterRuntime->raftStats().leaderNodeId;
        throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::NOT_OWNER, target);
    }
    child.impl_->clusterRuntime->queryReadBarrier();
    if (action == PartitionAdmin::STATUS) {
        const auto stats = child.impl_->clusterRuntime->raftStats();
        response.value = partitionAdminHeader(action, index, stats.configurationGeneration);
        response.value.push_back(stats.jointConsensus ? 1 : 0);
        const auto members = child.impl_->clusterRuntime->activeNodes();
        pushU32(response.value, static_cast<uint32_t>(members.size()));
        for (const auto& member : members) { pushU64(response.value, member.nodeId); }
        return response;
    }
    if (action == PartitionAdmin::WATERMARK) {
        const auto seq = child.impl_->captureRollbackWatermark();
        response.value = partitionAdminHeader(action, index, seq);
        return response;
    }
    if (action == PartitionAdmin::ROLLBACK) {
        if (bytes.size() < 14 || bytes[11] > 1) { throw std::invalid_argument("Cluster placement: invalid rollback request"); }
        RollbackOptions options;
        options.execution = static_cast<RollbackExecutionMode>(bytes[12]);
        options.conflict = static_cast<RollbackConflictPolicy>(bytes[13]);
        validateRollbackOptions(options);
        RollbackResult result;
        if (bytes[11] == 0) {
            const auto key = std::span{bytes}.subspan(14);
            if (partitionIndex(key) != index) { throw std::invalid_argument("Cluster placement: rollback partition mismatch"); }
            result = child.rollbackKey(key, Revision{.streamId = 0, .seq = generation}, options);
        }
        else {
            if (bytes.size() != 14) { throw std::invalid_argument("Cluster placement: malformed stream rollback"); }
            auto checkpoint = child.createClusterCheckpoint();
            checkpoint.watermarks.front().seq = generation;
            result = child.rollbackTo(checkpoint, options);
        }
        response.value.assign(result.operationId.begin(), result.operationId.end());
        const auto items = encodeRollbackItems(result.items);
        response.value.insert(response.value.end(), items.begin(), items.end());
        return response;
    }
    throw std::invalid_argument("Cluster placement: unsupported partition action");
}

bool cancelPlacement(uint64_t expected = 0) {
    if (!clusterRuntime || clusterReplicationMode != cluster::ReplicationMode::STRIPE) {
        throw std::logic_error("AkkEngine: cancellation requires STRIPE placement");
    }
    cluster::detail::ReconfigurationDeadline budget{opts.cluster.runtime.reconfiguration.timeoutMs, [this] { return placementClosing(); }};
    auto serialization = cluster::detail::ReconfigurationDeadline::lock(clusterReconfigurationMu);
    const auto state = clusterRuntime->placementState(true);
    if (state.pendingGeneration == 0) { return false; }
    if (expected != 0 && state.pendingGeneration != expected) { throw std::runtime_error("Cluster placement: stale cancellation"); }
    if (state.leaderNodeId != nodeId) {
        if (opts.cluster.runtime.routingMode != cluster::ClusterRoutingMode::FORWARD) {
            cluster::ClusterRouteTarget destination; destination.nodeId = state.leaderNodeId;
            throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::NOT_OWNER, destination);
        }
        const auto response = partitionRpc(state.leaderNodeId, partitionAdminHeader(PartitionAdmin::CANCEL, 0, state.pendingGeneration));
        return !response.value.empty() && response.value[0] != 0;
    }
    clusterRuntime->cancelPlacementChange(state.pendingGeneration);
    return true;
}

void reconfigurePlacement(cluster::ClusterConfig target, bool recovery = false) {
    cluster::detail::ReconfigurationDeadline budget{opts.cluster.runtime.reconfiguration.timeoutMs, [this] { return placementClosing(); }};
    std::unique_lock serialization{clusterReconfigurationMu, std::defer_lock};
    if (recovery) { if (!serialization.try_lock()) { return; } }
    else { serialization = cluster::detail::ReconfigurationDeadline::lock(clusterReconfigurationMu); }
    if (recovery) {
        const auto pending = clusterRuntime->placementState(true);
        if (pending.pendingGeneration == 0 || pending.leaderNodeId != nodeId) { return; }
        if (pending.cancelOnInterruption && !pending.activationStarted) {
            clusterRuntime->cancelPlacementChange(pending.pendingGeneration);
            return;
        }
    }
    cluster::detail::ReconfigurationDeadline cancellation{opts.cluster.runtime.reconfiguration.timeoutMs,
        [this] { return placementClosing() || (opts.cluster.runtime.reconfiguration.cancelled && opts.cluster.runtime.reconfiguration.cancelled()); }};
    if (!clusterRuntime) { throw std::logic_error("AkkEngine: cluster mode is disabled"); }
    target.validate();
    if (clusterReplicationMode == cluster::ReplicationMode::STRIPE) { reconfigureStripePlacement(std::move(target)); return; }
    if (!usesPartitionRaft()) {
        if (clusterReplicationMode != cluster::ReplicationMode::MIRROR ||
            !clusterConfig.usesDataConsensus() ||
            target.mode() != clusterConfig.mode() || target.clusterId() != clusterConfig.clusterId()) {
            throw std::invalid_argument("AkkEngine: online placement requires PARTITIONED Raft or MIRROR Raft membership replacement");
        }
        clusterRuntime->reconfigure(target);
        cluster::ClusterConfig::save(opts.paths.clusterConfigPath, target);
        return;
    }
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
    if (!target.raft().membership.allowLearners || !target.raft().membership.allowOnlineVoterChanges) {
        throw std::invalid_argument("Cluster placement: online migration requires learners and joint consensus");
    }
    for (uint16_t partition = 0; partition < target.partition().partitionCount; ++partition) {
        (void)partitionConfig(target, partition);
    }
    const auto generation = clusterRuntime->beginPlacementChange(target);
    state = clusterRuntime->placementState(true);
    if (state.pendingGeneration == 0) { return; }
    const auto deadline = cluster::detail::ReconfigurationDeadline::current().deadline;
    for (uint16_t first = 0; first < target.partition().partitionCount;) {
        cluster::detail::ReconfigurationDeadline::check();
        const auto context = cluster::detail::ReconfigurationDeadline::current();
        std::vector<std::future<void>> transfers;
        for (uint16_t launched = 0; launched < opts.cluster.runtime.reconfiguration.maxConcurrentPartitions &&
            first < target.partition().partitionCount; ++launched, ++first) {
            const auto partition = first;
            transfers.push_back(std::async(std::launch::async, [this, partition, generation, &target, &active, deadline, context] {
                cluster::detail::ReconfigurationDeadline scope{context};
                for (const auto& holder : partitionConfig(target, partition).dataNodes()) {
                    (void)partitionRpc(holder.nodeId, partitionAdminHeader(PartitionAdmin::PREPARE, partition, generation));
                }
                (void)partitionRpcLeader(partition, partitionAdminHeader(PartitionAdmin::MEMBERSHIP, partition, generation), active, deadline);
            }));
        }
        std::exception_ptr failure;
        for (auto& transfer : transfers) {
            try { transfer.get(); } catch (...) { if (!failure) { failure = std::current_exception(); } }
        }
        if (failure) { std::rethrow_exception(failure); }
    }
    clusterRuntime->finishPlacementChange(generation);
    { std::lock_guard progress{placementProgressMu}; placementLastError.clear(); }
}

void startPlacementRecovery() {
    if (!usesPartitionRaft() && clusterReplicationMode != cluster::ReplicationMode::STRIPE) { return; }
    placementRecoveryThread = std::jthread([this](std::stop_token stop) {
        while (!stop.stop_requested()) {
            try {
                OperationGuard operation{*this};
                const auto state = clusterRuntime->placementState(false);
                if (state.generation != activePlacementGeneration.load(std::memory_order_acquire)) {
                    acceptPlacement(cluster::ClusterConfig::decode(state.activeConfig), state.generation);
                }
                if ((opts.cluster.runtime.reconfiguration.resumePendingChanges || (state.cancelOnInterruption && !state.activationStarted)) && state.leaderNodeId == nodeId && state.pendingGeneration != 0) {
                    reconfigurePlacement(cluster::ClusterConfig::decode(state.pendingConfig), true);
                }
            }
            catch (const std::exception& error) {
                std::lock_guard progress{placementProgressMu}; placementLastError = error.what();
            }
            std::unique_lock lock{lifecycleMu};
            lifecycleCv.wait_for(lock, std::chrono::milliseconds{250}, [&] { return stop.stop_requested(); });
        }
    });
}

bool usesPartitionRaft() const noexcept {
    return clusterReplicationMode == cluster::ReplicationMode::PARTITIONED &&
        clusterConfig.usesDataConsensus();
}

uint16_t partitionIndex(std::span<const uint8_t> key) const {
    return cluster::detail::partitionForKey(key, clusterConfig.partition().partitionCount);
}

AkkEngine* localPartition(std::span<const uint8_t> key) const {
    const auto index = partitionIndex(key);
    std::shared_lock lock{partitionEnginesMu};
    return index < partitionEngines.size() ? partitionEngines[index].get() : nullptr;
}

uint64_t currentPartitionLeader(std::span<const uint8_t> key) const {
    if (auto* child = localPartition(key)) {
        const auto members = child->impl_->clusterRuntime->activeNodes();
        if (std::ranges::any_of(members, [this](const auto& member) { return member.nodeId == nodeId; })) {
            return child->impl_->clusterRuntime->raftStats().leaderNodeId;
        }
    }
    return partitionLeaderHints[partitionIndex(key)].load(std::memory_order_acquire);
}

std::vector<std::shared_ptr<AkkEngine>> partitionSnapshot() const {
    std::shared_lock lock{partitionEnginesMu};
    return partitionEngines;
}

AkkEngine& requireLocalPartition(std::span<const uint8_t> key) const {
    if (auto* engine = localPartition(key)) { return *engine; }
    throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::NOT_OWNER,
        clusterRuntime ? clusterRuntime->routeTarget(key) : cluster::ClusterRouteTarget{});
}

uint64_t logicalStreamId(std::span<const uint8_t> key) const {
    if (usesPartitionRaft()) { return static_cast<uint64_t>(partitionIndex(key)) + 1; }
    return clusterReplicationMode == cluster::ReplicationMode::PARTITIONED ? clusterRuntime->ownerNodeId(key) : 0;
}

cluster::ClusterConfig partitionConfig(const cluster::ClusterConfig& placement, uint16_t index,
    bool joining = false) const {
        const std::array<uint8_t, 2> group{static_cast<uint8_t>(index), static_cast<uint8_t>(index >> 8)};
        auto candidates = placement.dataNodes();
        std::erase_if(candidates, [](const auto& member) { return member.raftLearner(); });
        auto ranked = cluster::detail::rankNodes(group, candidates);
        std::vector<cluster::NodeInfo> members;
        for (size_t i = 0; i < placement.partitionCopies(); ++i) {
            auto node = ranked[i].second;
            node.replPort = static_cast<uint16_t>(node.partitionReplBasePort + index);
            node.stripeMetadataPort = 0;
            node.partitionReplBasePort = 0;
            members.push_back(std::move(node));
        }
        if (joining && std::ranges::none_of(members, [this](const auto& node) { return node.nodeId == nodeId; })) {
            const auto* configured = placement.findById(nodeId);
            if (!configured) { configured = clusterConfig.findById(nodeId); }
            if (!configured || !placement.raft().membership.allowLearners) {
                throw std::invalid_argument("AkkEngine: joining a partition requires configured membership and allowLearners");
            }
            auto self = *configured;
            self.replPort = static_cast<uint16_t>(self.partitionReplBasePort + index);
            self.stripeMetadataPort = 0;
            self.partitionReplBasePort = 0;
            self.capabilities |= cluster::RAFT_LEARNER;
            members.push_back(std::move(self));
        }
        if (std::ranges::none_of(members, [](const auto& member) { return member.coordinatorEligible() && !member.raftLearner(); })) {
            throw std::invalid_argument("AkkEngine: every partition requires a coordinator-eligible holder");
        }
        return cluster::ClusterConfig{std::move(members), cluster::ReplicationMode::MIRROR,
            placement.ackPolicy(), placement.consistency(), placement.raft(), {}, 0, placement.clusterId(), {}, placement.failover()};
}

std::shared_ptr<AkkEngine> openPartition(uint16_t index, const cluster::ClusterConfig& bootstrap, bool joining) {
        const std::array<uint8_t, 2> group{static_cast<uint8_t>(index), static_cast<uint8_t>(index >> 8)};
        auto child = opts;
        child.paths = {};
        child.paths.dataDir = opts.paths.dataDir / "partitions" / std::to_string(index);
        child.paths.nodeIdPath = opts.paths.nodeIdPath;
        child.runtime.generationLayoutEnabled = false;
        child.runtime.scanConsistency = AkkEngineOptions::ScanConsistencyMode::PINNED_SNAPSHOT;
        child.components.apiEnabled = false;
        child.cluster.config = partitionConfig(bootstrap, index, joining);
        child.cluster.runtime.clusterMembershipPath.clear();
        child.cluster.runtime.resetClusterMembership = false;
        child.cluster.runtime.primaryHost.clear();
        child.cluster.runtime.primaryReplPort = 0;
        child.cluster.runtime.primaryNodeId = 0;
        child.cluster.runtime.secure.expectedPrimaryNodeId = 0;
        child.cluster.runtime.mirrorPromotion = {};
        child.cluster.runtime.mirrorFencing = {};
        child.cluster.runtime.mirrorRecovery = {};
        child.cluster.runtime.reconfiguration.stripeMigrationMode = cluster::StripeMigrationMode::FREEZE;
        child.cluster.runtime.reconfiguration.stripeInterruptionPolicy = cluster::StripeInterruptionPolicy::RETAIN;
        child.cluster.runtime.transfer.spoolDirectory = child.paths.dataDir / "transfer-spool";
        const auto cancellation = child.cluster.runtime.reconfiguration.cancelled;
        child.cluster.runtime.reconfiguration.cancelled = [this, cancellation] {
            return placementClosing() || (cancellation && cancellation());
        };
        // Distinct group identities remain stable through holder changes.
        std::array<uint8_t, 10> identity{};
        for (size_t i = 0; i < 8; ++i) { identity[i] = static_cast<uint8_t>(opts.cluster.runtime.clusterGroupId >> (8 * i)); }
        identity[8] = group[0]; identity[9] = group[1];
        const std::array parts{std::span<const uint8_t>{clusterConfig.clusterId()}, std::span<const uint8_t>{identity}};
        const auto digest = crypto::hash256(parts);
        uint64_t groupId = 0;
        for (size_t i = 0; i < 8; ++i) { groupId |= static_cast<uint64_t>(digest[i]) << (8 * i); }
        child.cluster.runtime.clusterGroupId = groupId == 0 ? 1 : groupId;
        return AkkEngine::open(std::move(child));
}

void openPartitionEngines() {
    livePlacement.store(std::make_shared<const cluster::ClusterConfig>(clusterConfig));
    if (!usesPartitionRaft()) { return; }
    if (opts.paths.dataDir.empty()) { throw std::invalid_argument("AkkEngine: PARTITIONED Raft requires dataDir"); }
    partitionEngines.resize(clusterConfig.partition().partitionCount);
    for (uint16_t index = 0; index < partitionEngines.size(); ++index) {
        const auto group = partitionConfig(clusterConfig, index);
        if (!group.findById(nodeId)) { continue; }
        partitionEngines[index] = openPartition(index, clusterConfig, false);
    }
    partitionQueryExecutor = std::make_unique<cluster::detail::BoundedExecutor>(
        std::min<size_t>(8, partitionEngines.size()), 64);
}

void acceptPlacement(const cluster::ClusterConfig& config, uint64_t generation) {
    if (clusterReplicationMode == cluster::ReplicationMode::STRIPE) {
        if (config.clusterId() != clusterConfig.clusterId() || config.mode() != clusterConfig.mode()) {
            throw std::runtime_error("AkkEngine: invalid committed stripe placement");
        }
        std::lock_guard publication{partitionCreationMu};
        if (generation < activePlacementGeneration.load(std::memory_order_acquire)) { return; }
        if (config.findById(nodeId)) { cluster::ClusterConfig::save(opts.paths.clusterConfigPath, config); }
        livePlacement.store(std::make_shared<const cluster::ClusterConfig>(config));
        activePlacementGeneration.store(generation, std::memory_order_release);
        return;
    }
    if (!usesPartitionRaft()) { return; }
    if (config.clusterId() != clusterConfig.clusterId() || config.mode() != clusterConfig.mode() ||
        config.partition().partitionCount != clusterConfig.partition().partitionCount) {
        throw std::runtime_error("AkkEngine: invalid committed placement identity");
    }
    std::lock_guard creation{partitionCreationMu};
    if (generation < activePlacementGeneration.load(std::memory_order_acquire)) { return; }
    for (uint16_t index = 0; index < partitionEngines.size(); ++index) {
        if (!partitionConfig(config, index).findById(nodeId)) { continue; }
        { std::shared_lock lock{partitionEnginesMu}; if (partitionEngines[index]) { continue; } }
        auto child = openPartition(index, config, false);
        std::unique_lock lock{partitionEnginesMu};
        partitionEngines[index] = std::move(child);
    }
    if (config.findById(nodeId)) { cluster::ClusterConfig::save(opts.paths.clusterConfigPath, config); }
    livePlacement.store(std::make_shared<const cluster::ClusterConfig>(config));
    activePlacementGeneration.store(generation, std::memory_order_release);
    for (auto& hint : partitionLeaderHints) { hint.store(0, std::memory_order_release); }
}

core::ArenaGenerator<StoredScanRecordView> partitionLocalScan(std::vector<uint8_t> start,
    std::vector<uint8_t> end, bool ownersOnly, std::vector<uint16_t> expected = {}) {
    struct Cursor {
        core::BufferArena arena;
        core::ArenaGenerator<ScanRecordView> records;
        core::ArenaGenerator<ScanRecordView>::iterator position;
    };
    std::vector<std::unique_ptr<Cursor>> cursors;
    const auto children = partitionSnapshot();
    for (uint16_t index = 0; index < children.size(); ++index) {
        const auto& child = children[index];
        const bool required = !expected.empty() && std::ranges::find(expected, index) != expected.end();
        if (!expected.empty() && !required) { continue; }
        if (!child) {
            if (required) { throw std::runtime_error("Cluster query: required partition unavailable"); }
            continue;
        }
        if (ownersOnly) {
            if (child->impl_->clusterRuntime->role() != cluster::NodeRole::PRIMARY) {
                if (required) { throw std::runtime_error("Cluster query: partition leadership changed; retry query"); }
                continue;
            }
            child->impl_->clusterRuntime->queryReadBarrier();
        }
        auto cursor = std::make_unique<Cursor>();
        cursor->records = child->scanLocalStorage(cursor->arena, start, end);
        cursor->position = cursor->records.begin();
        cursors.push_back(std::move(cursor));
    }
    while (true) {
        Cursor* next = nullptr;
        for (auto& cursor : cursors) {
            if (cursor->position == std::default_sentinel) { continue; }
            if (!next || compareKey(cursor->position->key, next->position->key) < 0) { next = cursor.get(); }
        }
        if (!next) { co_return; }
        co_yield StoredScanRecordView{next->position->key, next->position->value, MemHdr16::FLAG_NORMAL};
        ++next->position;
    }
}

static uint64_t partitionAdminNumber(std::span<const uint8_t> bytes) {
    if (bytes.size() != 11) { throw std::runtime_error("Cluster placement: malformed numeric response"); }
    uint64_t value = 0;
    for (size_t i = 0; i < 8; ++i) { value |= uint64_t{bytes[3 + i]} << (i * 8); }
    return value;
}

RollbackResult partitionRollback(uint16_t index, uint64_t sequence, RollbackOptions options,
    bool wholeStream, std::span<const uint8_t> key = {}) {
    auto command = partitionAdminHeader(PartitionAdmin::ROLLBACK, index, sequence);
    command.push_back(wholeStream ? 1 : 0);
    command.push_back(static_cast<uint8_t>(options.execution));
    command.push_back(static_cast<uint8_t>(options.conflict));
    command.insert(command.end(), key.begin(), key.end());
    const auto placement = livePlacement.load();
    const auto response = partitionRpcLeader(index, std::move(command), *placement,
        std::chrono::steady_clock::now() + std::chrono::milliseconds{opts.cluster.runtime.reconfiguration.timeoutMs});
    if (response.value.size() < 16) { throw std::runtime_error("Cluster placement: malformed rollback result"); }
    RollbackResult result;
    std::copy_n(response.value.begin(), 16, result.operationId.begin());
    result.items = decodeRollbackItems(std::span{response.value}.subspan(16));
    return result;
}

core::ArenaGenerator<ScanRecordView> partitionPublicLocalScan(std::vector<uint8_t> start,
    std::vector<uint8_t> end, std::shared_ptr<OperationGuard> operation) {
    (void)operation;
    for (const auto& record : partitionLocalScan(std::move(start), std::move(end), false)) {
        co_yield ScanRecordView{record.key, record.value};
    }
}

struct PartitionRangeTarget { uint64_t node; std::vector<uint16_t> partitions; };
std::vector<PartitionRangeTarget> rangeQueryPlan(std::span<const uint8_t> key) {
    std::vector<PartitionRangeTarget> out;
    if (!usesPartitionRaft()) {
        for (const auto target : queryTargets(key, true)) { out.push_back({target, {}}); }
        return out;
    }
    const auto placement = livePlacement.load();
    cluster::detail::ReconfigurationDeadline budget{opts.cluster.runtime.forwardingTimeoutMs,
        [this] { return placementClosing(); }};
    const auto context = cluster::detail::ReconfigurationDeadline::current();
    const auto deadline = context.deadline;
    std::vector<uint64_t> leaders(placement->partition().partitionCount);
    // The shared executor bounds work across concurrent queries. Its forEach
    // joins every submitted task even on failure, retaining this captured view.
    partitionQueryExecutor->forEach(leaders.size(), [&](size_t slot) {
        cluster::detail::ReconfigurationDeadline scope{context};
        cluster::detail::ReconfigurationDeadline::check();
        const auto index = static_cast<uint16_t>(slot);
        const auto response = partitionRpcLeader(index, partitionAdminHeader(PartitionAdmin::STATUS, index, 0), *placement, deadline);
        const auto target = response.target.nodeId;
        if (opts.cluster.runtime.readMode == cluster::ClusterReadMode::OWNER_ONLY && target != nodeId) {
            cluster::ClusterRouteTarget destination; destination.nodeId = target;
            throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::NOT_OWNER, destination);
        }
        leaders[slot] = target;
    }, deadline);
    cluster::detail::ReconfigurationDeadline::check();
    for (uint16_t index = 0; index < leaders.size(); ++index) {
        const auto target = leaders[index];
        auto found = std::ranges::find(out, target, &PartitionRangeTarget::node);
        if (found == out.end()) { out.push_back({target, {index}}); }
        else { found->partitions.push_back(index); }
    }
    return out;
}

class PartitionedRaftWriteCoordinator final : public WriteCoordinator {
    public:
        using WriteCoordinator::WriteCoordinator;
        void put(std::span<const uint8_t> key, std::span<const uint8_t> value) override {
            engine_.requireLocalPartition(key).put(key, value);
        }
        void putHinted(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t fp, uint64_t mini) override {
            engine_.requireLocalPartition(key).putHinted(key, value, fp, mini);
        }
        void putBatch(std::span<const BatchPutEntry> entries) override {
            // A batch is ordered per partition and does not claim cross-group atomicity.
            std::map<uint16_t, std::vector<BatchPutEntry>> batches;
            for (const auto& entry : entries) { batches[engine_.partitionIndex(entry.key)].push_back(entry); }
            for (const auto& [index, batch] : batches) {
                auto* child = engine_.localPartition(batch.front().key);
                if (!child) {
                    throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::NOT_OWNER,
                        engine_.clusterRuntime->routeTarget(batch.front().key));
                }
                child->applyProtocolBulk(batch);
            }
        }
        void remove(std::span<const uint8_t> key) override { engine_.requireLocalPartition(key).remove(key); }
        void removeHinted(std::span<const uint8_t> key, uint64_t fp, uint64_t mini) override {
            engine_.requireLocalPartition(key).removeHinted(key, fp, mini);
        }
        void rollback(std::span<const uint8_t> key, std::span<const uint8_t> value, bool tombstone) override {
            engine_.requireLocalPartition(key).impl_->writeCoordinator->rollback(key, value, tombstone);
        }
        cluster::ClusterRequestResult writeWithRequest(const cluster::ClusterRequestId& id, cluster::ReplOpType op,
            std::span<const uint8_t> key, std::span<const uint8_t> value) override {
            auto& child = engine_.requireLocalPartition(key);
            return op == cluster::ReplOpType::PUT ? child.putWithRequest(id, key, value) : child.removeWithRequest(id, key);
        }
};

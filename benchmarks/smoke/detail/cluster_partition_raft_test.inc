void testPartitionQueryQueueDeadline() {
    using namespace std::chrono_literals;
    // Cover both a full admission queue and an admitted task which cannot start.
    for (const size_t count : {size_t{1}, size_t{4}}) {
        std::atomic<size_t> executed{0};
        std::promise<void> entered, release;
        auto released = release.get_future().share();
        akkaradb::engine::cluster::detail::BoundedExecutor executor{1, 1};
        AKK_CLUSTER_CHECK(executor.trySubmit([&] { entered.set_value(); released.wait(); }));
        const bool running = entered.get_future().wait_for(5s) == std::future_status::ready;
        auto query = std::async(std::launch::async, [&] {
            try { executor.forEach(count, [&](size_t) { ++executed; }, std::chrono::steady_clock::now() + 50ms); }
            catch (const std::runtime_error& error) { return std::string_view{error.what()}.find("deadline exceeded") != std::string_view::npos; }
            return false;
        });
        const bool bounded = query.wait_for(1s) == std::future_status::ready;
        release.set_value();
        const bool failed = query.get();
        AKK_CLUSTER_CHECK(running && bounded && failed && executed.load() == 0);
        // Cancelling this query must leave the shared executor usable.
        executor.forEach(2, [&](size_t) { ++executed; });
        AKK_CLUSTER_CHECK(executed.load() == 2);
    }
}

struct PartitionQueryStatusGate {
    std::mutex mutex;
    std::condition_variable cv;
    bool enabled = false, released = false;
    size_t entered = 0, active = 0, peak = 0;
    void pause() {
        std::unique_lock lock{mutex};
        if (!enabled) { return; }
        ++entered; ++active; peak = std::max(peak, active); cv.notify_all();
        const bool ready = cv.wait_for(lock, std::chrono::seconds{10}, [&] { return released; });
        --active;
        if (!ready) { throw std::runtime_error("partition query gate timed out"); }
    }
    void enable() { std::lock_guard lock{mutex}; enabled = true; }
    bool awaitEight() {
        std::unique_lock lock{mutex};
        return cv.wait_for(lock, std::chrono::seconds{5}, [&] { return active == 8; });
    }
    void release() { std::lock_guard lock{mutex}; released = true; cv.notify_all(); }
};

std::shared_ptr<PartitionQueryStatusGate> partitionQueryStatusGate;

std::unique_ptr<IClusterRuntime> partitionQueryTestFactory(std::filesystem::path dir, ClusterConfig config,
    uint64_t id, ClusterEngineCallbacks callbacks, ClusterRuntimeOptions options) {
    if (config.mode() == ReplicationMode::PARTITIONED && partitionQueryStatusGate) {
        callbacks.forward = [gate = partitionQueryStatusGate, forward = std::move(callbacks.forward)](const ForwardRequest& request) {
            // Intercept only partition STATUS; the real callback still performs
            // leader validation and ReadIndex after this deterministic gate.
            if (request.operation == ForwardOperation::CLUSTER_ADMIN && request.entries.size() == 1 &&
                request.entries[0].key.empty() && request.entries[0].value.size() == 11 && request.entries[0].value[0] == 3) {
                gate->pause();
            }
            return forward(request);
        };
    }
    return ClusterRuntime::create(std::move(dir), std::move(config), id, std::move(callbacks), std::move(options));
}

struct PartitionQueryFactoryScope {
    PartitionQueryFactoryScope() {
        partitionQueryStatusGate = std::make_shared<PartitionQueryStatusGate>();
        AKK_CLUSTER_CHECK(registerClusterRuntimeFactory(partitionQueryTestFactory));
    }
    ~PartitionQueryFactoryScope() {
        partitionQueryStatusGate->release();
        partitionQueryStatusGate.reset();
        (void)registerClusterRuntimeFactory([](std::filesystem::path dir, ClusterConfig config,
            uint64_t id, ClusterEngineCallbacks callbacks, ClusterRuntimeOptions options) -> std::unique_ptr<IClusterRuntime> {
            return ClusterRuntime::create(std::move(dir), std::move(config), id, std::move(callbacks), std::move(options));
        });
    }
};

void testPartitionQueryParallelism() {
    using akkaradb::engine::AkkEngine;
    using akkaradb::engine::AkkEngineOptions;
    using namespace std::chrono_literals;
    PartitionQueryFactoryScope factory;
    const auto dir = makeTempDir("partition-query-parallelism");
    auto members = std::vector{node(1, 30801, 30811), node(2, 30802, 30812), node(3, 30803, 30813, DATA_AND_COORDINATOR | RAFT_LEARNER)};
    members[0].partitionReplBasePort = 30900;
    members[1].partitionReplBasePort = 30932;
    members[2].partitionReplBasePort = 30964;
    RaftOptions raft; raft.membership.allowLearners = true;
    const ClusterConfig config{members, ReplicationMode::PARTITIONED, {},
        ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 500}, raft, {}, 0,
        testClusterId(9568), {.replicationFactor = 1, .partitionCount = 16}};
    std::array<std::unique_ptr<AkkEngine>, 3> engines;
    for (size_t i = 0; i < engines.size(); ++i) {
        AkkEngineOptions options;
        options.paths.dataDir = dir / std::to_string(i + 1);
        options.components.blobEnabled = false;
        options.components.sstEnabled = false;
        options.components.manifestEnabled = false;
        options.components.clusterEnabled = true;
        options.cluster.config = config;
        options.cluster.runtime.transportMode = TransportMode::PLAIN;
        options.cluster.runtime.clusterGroupId = 9568;
        options.cluster.runtime.routingMode = ClusterRoutingMode::FORWARD;
        options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
        options.cluster.runtime.forwardingTimeoutMs = 1500;
        options.cluster.runtime.raftHeartbeatIntervalMs = 30;
        writeNodeIdFile(options.paths.dataDir / "node.id", i + 1);
        engines[i] = AkkEngine::open(std::move(options));
    }
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { return engines[2]->count() == 0; } catch (...) { return false; }
    }, 15s));
    std::map<std::string, std::string> expected;
    std::set<uint64_t> partitions;
    for (size_t index = 0; index < 10000 && partitions.size() < 16; ++index) {
        const auto key = "key-" + std::to_string(index);
        if (!partitions.insert(engines[2]->revisionStreamId(bytesOf(key))).second) { continue; }
        const auto value = "value-" + std::to_string(index);
        engines[2]->put(bytesOf(key), bytesOf(value));
        expected.emplace(key, value);
    }
    AKK_CLUSTER_CHECK(partitions.size() == 16);
    auto gate = partitionQueryStatusGate;
    gate->enable();
    auto first = std::async(std::launch::async, [&] { return engines[2]->count(); });
    auto second = std::async(std::launch::async, [&] { return engines[2]->count(); });
    // Release before asserting so a failed concurrency check also drains both
    // real queries, rather than leaving blocked RPC callbacks during unwinding.
    const bool overlapped = gate->awaitEight();
    gate->release();
    const auto firstCount = first.get(), secondCount = second.get();
    { std::lock_guard lock{gate->mutex}; AKK_CLUSTER_CHECK(overlapped && gate->peak == 8 && gate->entered == 32); }
    AKK_CLUSTER_CHECK(firstCount == expected.size() && secondCount == expected.size());
    AKK_CLUSTER_CHECK(engines[0]->count() == expected.size());
    akkaradb::core::BufferArena arena;
    std::map<std::string, std::string> actual;
    std::string previous;
    for (const auto& row : engines[2]->scan(arena)) {
        const auto key = textOf(row.key);
        AKK_CLUSTER_CHECK(previous.empty() || previous < key);
        AKK_CLUSTER_CHECK(actual.emplace(key, textOf(row.value)).second);
        previous = key;
    }
    AKK_CLUSTER_CHECK(actual == expected);
    const auto start = std::next(expected.begin(), 3)->first, end = std::next(expected.begin(), 11)->first;
    AKK_CLUSTER_CHECK(engines[2]->count(bytesOf(start), bytesOf(end)) == 8);
    engines[1]->close();
    bool failed = false;
    const auto began = std::chrono::steady_clock::now();
    try { (void)engines[2]->count(); } catch (...) { failed = true; }
    AKK_CLUSTER_CHECK(failed && std::chrono::steady_clock::now() - began < 5s);
    auto pending = std::async(std::launch::async, [&] {
        try { (void)engines[2]->count(); } catch (...) { return true; }
        return false;
    });
    engines[2]->close();
    AKK_CLUSTER_CHECK(pending.get());
    for (auto& engine : engines) { engine->close(); }
    std::filesystem::remove_all(dir);
    std::fprintf(stderr, "[partition query] 16 partitions, bounded concurrent STATUS, complete ranges and shared failure deadline passed\n");
}

void testPartitionRaftFailover() {
    using akkaradb::engine::AkkEngine;
    using akkaradb::engine::AkkEngineOptions;
    using namespace std::chrono_literals;
    const auto dir = makeTempDir("partition-raft");
    auto members = std::vector{node(1, 27601, 27611), node(2, 27602, 27612), node(3, 27603, 27613)};
    for (size_t i = 0; i < members.size(); ++i) { members[i].partitionReplBasePort = static_cast<uint16_t>(27700 + i * 10); }
    ConsistencyOptions consistency;
    consistency.mode = ConsistencyMode::RAFT_QUORUM;
    consistency.ackTimeoutMs = 1500;
    const ClusterConfig config{members, ReplicationMode::PARTITIONED, {}, consistency, {}, {}, 0,
        testClusterId(9560), {.replicationFactor = 3, .partitionCount = 2}};
    std::array<std::unique_ptr<AkkEngine>, 3> engines;
    for (size_t i = 0; i < engines.size(); ++i) {
        AkkEngineOptions options;
        options.paths.dataDir = dir / std::to_string(i + 1);
        options.components.blobEnabled = false;
        options.components.sstEnabled = false;
        options.components.manifestEnabled = false;
        options.components.versionLogEnabled = true;
        options.components.clusterEnabled = true;
        options.cluster.config = config;
        options.cluster.runtime.transportMode = TransportMode::PLAIN;
        options.cluster.runtime.clusterGroupId = 9560;
        options.cluster.runtime.routingMode = ClusterRoutingMode::FORWARD;
        options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
        options.cluster.runtime.raftHeartbeatIntervalMs = 30;
        writeNodeIdFile(options.paths.dataDir / "node.id", i + 1);
        engines[i] = AkkEngine::open(std::move(options));
    }
    const std::string keyText = "partition-key";
    const auto key = bytesOf(keyText);
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { engines[0]->put(key, bytesOf("before")); return true; } catch (...) { return false; }
    }, 15s));
    const auto before = akk_test::collectHistory(engines[0]->history(key));
    AKK_CLUSTER_CHECK(!before.empty() && textOf(before.back().value) == "before");
    const auto leader = before.back().sourceNodeId;
    AKK_CLUSTER_CHECK(leader >= 1 && leader <= 3);
    engines[leader - 1]->close(); engines[leader - 1].reset();
    AkkEngine* survivor = engines[0] ? engines[0].get() : engines[1].get();
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { survivor->put(key, bytesOf("after")); return true; } catch (...) { return false; }
    }, 15s));
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { const auto value = survivor->get(key); return value && textOf(*value) == "after"; } catch (...) { return false; }
    }, 10s));
    const auto history = akk_test::collectHistory(survivor->history(key));
    AKK_CLUSTER_CHECK(history.size() >= 2 && textOf(history.front().value) == "before" && textOf(history.back().value) == "after");
    AKK_CLUSTER_CHECK(history.back().seq > history.front().seq);
    AKK_CLUSTER_CHECK(survivor->count() == 1);
    auto checkpoint = survivor->createClusterCheckpoint();
    AKK_CLUSTER_CHECK(checkpoint.watermarks.size() == 2);
    const auto restored = survivor->rollbackKey(key, {.streamId = survivor->revisionStreamId(key),
        .seq = history.front().seq}, {.conflict = akkaradb::engine::RollbackConflictPolicy::OVERWRITE_LATEST});
    AKK_CLUSTER_CHECK(restored.complete() && textOf(*survivor->get(key)) == "before");
    for (auto& engine : engines) { if (engine) { engine->close(); } }
    std::filesystem::remove_all(dir);
}

void testPartitionRaftLiveCopies() {
    using akkaradb::engine::AkkEngine;
    using akkaradb::engine::AkkEngineOptions;
    using namespace std::chrono_literals;
    const auto dir = makeTempDir("partition-raft-live-copies");
    auto members = std::vector{node(1, 27801, 27811), node(2, 27802, 27812), node(3, 27803, 27813), node(4, 27804, 27814)};
    for (size_t i = 0; i < members.size(); ++i) { members[i].partitionReplBasePort = static_cast<uint16_t>(27900 + i * 10); }
    ConsistencyOptions consistency{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 1500};
    RaftOptions raft;
    raft.membership = {.mode = RaftMembershipMode::JOINT_CONSENSUS, .allowOnlineVoterChanges = true, .allowLearners = true};
    const auto configFor = [&](uint16_t copies, size_t count = 4, bool joining = false) {
        auto selected = members; selected.resize(count);
        if (joining) { selected.back().capabilities |= RAFT_LEARNER; }
        return ClusterConfig{selected, ReplicationMode::PARTITIONED, {}, consistency, raft, {}, 0,
            testClusterId(9561), {.replicationFactor = copies, .partitionCount = 2}};
    };
    std::array<std::unique_ptr<AkkEngine>, 4> engines;
    std::array<AkkEngineOptions, 4> options;
    for (size_t i = 0; i < engines.size(); ++i) {
        auto& opt = options[i];
        opt.paths.dataDir = dir / std::to_string(i + 1);
        opt.components.blobEnabled = false;
        opt.components.sstEnabled = false;
        opt.components.manifestEnabled = false;
        opt.components.versionLogEnabled = true;
        opt.components.clusterEnabled = true;
        opt.cluster.config = i == 3 ? configFor(2, 4, true) : configFor(2, 3);
        opt.cluster.runtime.transportMode = TransportMode::PLAIN;
        opt.cluster.runtime.clusterGroupId = 9561;
        opt.cluster.runtime.routingMode = ClusterRoutingMode::FORWARD;
        opt.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
        opt.cluster.runtime.forwardingTimeoutMs = 30000;
        opt.cluster.runtime.raftHeartbeatIntervalMs = 30;
        opt.cluster.runtime.raftSnapshot.minLogEntries = 1;
        opt.cluster.runtime.requests.enabled = true;
        writeNodeIdFile(opt.paths.dataDir / "node.id", i + 1);
        if (i != 3) { engines[i] = AkkEngine::open(opt); }
    }
    const std::string keyText = "migration-history";
    const auto key = bytesOf(keyText);
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { engines[0]->put(key, bytesOf("before")); return true; } catch (...) { return false; }
    }, 20s));
    const auto before = akk_test::collectHistory(engines[0]->history(key));
    const auto checkpoint = engines[0]->createClusterCheckpoint();
    AKK_CLUSTER_CHECK(waitUntil([&] {
        bool compacted = false;
        for (size_t i = 0; i < 3; ++i) {
            for (const auto& partition : engines[i]->clusterPartitionStats()) {
                if (partition.currentSeq == 0) { continue; }
                if (partition.storage.cluster.snapshotIndex == 0) { return false; }
                compacted = true;
            }
        }
        return compacted;
    }, 10s));
    std::fprintf(stderr, "[partition snapshot] new holder\n");
    engines[3] = AkkEngine::open(options[3]);
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { engines[0]->reconfigureCluster(configFor(3)); return true; }
        catch (...) { return false; }
    }, 60s));
    std::fprintf(stderr, "[partition snapshot] expanded membership\n");
    const auto status = engines[0]->clusterReconfigurationStatus();
    AKK_CLUSTER_CHECK(status.activeGeneration == 2 && status.pendingGeneration == 0 && status.completedPartitions == 2);
    AKK_CLUSTER_CHECK(engines[0]->clusterPartitionStats().size() == 2);
    AKK_CLUSTER_CHECK(textOf(*engines[3]->get(key)) == "before");
    const auto migrated = akk_test::collectHistory(engines[3]->history(key));
    AKK_CLUSTER_CHECK(migrated.front().seq == before.front().seq && migrated.front().value == before.front().value && migrated.front().timestampNs == before.front().timestampNs);
    const auto id = engines[0]->newRequestId(60000);
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { return engines[0]->putWithRequest(id, key, bytesOf("after")).status == ClusterRequestStatus::APPLIED; } catch (...) { return false; }
    }, 15s));
    AKK_CLUSTER_CHECK(engines[3]->queryRequest(id, key).status == ClusterRequestStatus::APPLIED);
    AKK_CLUSTER_CHECK(engines[3]->count() == 1);
    const auto rollback = engines[0]->rollbackTo(checkpoint, {.conflict = akkaradb::engine::RollbackConflictPolicy::OVERWRITE_LATEST});
    AKK_CLUSTER_CHECK(rollback.complete() && textOf(*engines[0]->get(key)) == "before");
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { engines[0]->reconfigureCluster(configFor(2, 3)); return true; } catch (...) { return false; }
    }, 60s));
    for (auto& engine : engines) { engine->close(); engine.reset(); }
    for (size_t i = 0; i < 3; ++i) { options[i].cluster.config.reset(); engines[i] = AkkEngine::open(options[i]); }
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { const auto value = engines[0]->get(key); return value && textOf(*value) == "before" && akk_test::collectHistory(engines[0]->history(key)).size() >= 3; }
        catch (...) { return false; }
    }, 20s));
    for (auto& engine : engines) { if (engine) { engine->close(); } }
    std::filesystem::remove_all(dir);
}

void testMirrorFailoverPolicies() {
    using akkaradb::engine::AkkEngine;
    using akkaradb::engine::AkkEngineOptions;
    using namespace std::chrono_literals;
    for (const auto [policy, localCompletion] : std::array{
        std::pair{FailoverPolicy::NONE, false}, std::pair{FailoverPolicy::NONE, true},
        std::pair{FailoverPolicy::ALLOW_ACKNOWLEDGED_LOSS, true}}) {
        const auto dir = makeTempDir("mirror-failover-choice");
        const auto base = static_cast<uint16_t>(policy == FailoverPolicy::NONE ? (localCompletion ? 28400 : 28000) : 28100);
        std::vector<NodeInfo> members;
        for (uint64_t id = 1; id <= 3; ++id) {
            members.push_back(node(id, static_cast<uint16_t>(base + id), static_cast<uint16_t>(base + 10 + id)));
        }
        ConsistencyOptions consistency{.mode = localCompletion ? ConsistencyMode::ASYNC : ConsistencyMode::RAFT_QUORUM,
            .writeConsistency = WriteConsistency::LOCAL, .ackTimeoutMs = 1000};
        RaftOptions raft;
        if (policy == FailoverPolicy::NONE && localCompletion) { raft = onlineRaftMembership(); }
        const ClusterConfig config{members, ReplicationMode::MIRROR, {}, consistency, raft, {}, 0,
            testClusterId(base), {}, policy};
        AKK_CLUSTER_CHECK(ClusterConfig::decode(config.encode()).failover() == policy);
        std::array<std::unique_ptr<AkkEngine>, 3> engines;
        for (size_t i = 0; i < engines.size(); ++i) {
            AkkEngineOptions options;
            options.paths.dataDir = dir / std::to_string(i + 1);
            options.components.blobEnabled = false;
            options.components.sstEnabled = false;
            options.components.manifestEnabled = false;
            options.components.versionLogEnabled = true;
            options.components.clusterEnabled = true;
            options.cluster.config = config;
            options.cluster.runtime.transportMode = TransportMode::PLAIN;
            options.cluster.runtime.routingMode = ClusterRoutingMode::FORWARD;
            options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
            options.cluster.runtime.raftHeartbeatIntervalMs = 30;
            writeNodeIdFile(options.paths.dataDir / "node.id", i + 1);
            engines[i] = AkkEngine::open(std::move(options));
        }
        if (policy == FailoverPolicy::NONE) {
            std::this_thread::sleep_for(1200ms);
            for (const auto& engine : engines) { AKK_CLUSTER_CHECK(engine->stats().cluster.leaderNodeId == 0); }
            engines[0]->campaignClusterLeadership();
        }
        const std::string keyText = "owner-choice";
        const auto key = bytesOf(keyText);
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try { engines[0]->put(key, bytesOf("before")); return true; } catch (...) { return false; }
        }, 15s));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try { return akk_test::collectHistory(engines[0]->history(key)).size() != 0; } catch (...) { return false; }
        }, 10s));
        const auto before = akk_test::collectHistory(engines[0]->history(key));
        const auto oldOwner = before.back().sourceNodeId;
        engines[oldOwner - 1]->close(); engines[oldOwner - 1].reset();
        AkkEngine* candidate = nullptr;
        for (auto& engine : engines) { if (engine) { candidate = engine.get(); break; } }
        if (policy == FailoverPolicy::NONE) {
            std::this_thread::sleep_for(1500ms);
            bool accepted = false;
            try { candidate->put(key, bytesOf("unexpected")); accepted = true; } catch (...) {}
            AKK_CLUSTER_CHECK(!accepted);
            candidate->campaignClusterLeadership();
        }
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try { candidate->put(key, bytesOf("after")); return true; } catch (...) { return false; }
        }, 15s));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try { const auto value = candidate->get(key); return value && textOf(*value) == "after"; } catch (...) { return false; }
        }, 10s));
        const auto after = akk_test::collectHistory(candidate->history(key));
        AKK_CLUSTER_CHECK(after.front().value == before.front().value && after.front().timestampNs == before.front().timestampNs);
        for (auto& engine : engines) { if (engine) { engine->close(); } }
        std::filesystem::remove_all(dir);
    }
}

void testStripeOnlinePlacement(bool secure = false) {
    using akkaradb::engine::AkkEngine;
    using akkaradb::engine::AkkEngineOptions;
    using namespace std::chrono_literals;
    std::fprintf(stderr, "[stripe online] %s\n", secure ? "SECURE" : "PLAIN");
    const auto dir = makeTempDir("stripe-online-placement");
    std::vector<NodeInfo> members;
    for (uint64_t id = 1; id <= 4; ++id) { members.push_back(node(id, static_cast<uint16_t>(28200 + id), static_cast<uint16_t>(28210 + id))); }
    const auto configFor = [&](std::vector<NodeInfo> selected) {
        return ClusterConfig{std::move(selected), ReplicationMode::STRIPE, {},
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 2000}, {},
            {.dataShards = 2, .parityShards = 1}, 0, testClusterId(9562)};
    };
    auto initialMembers = members; initialMembers.resize(3);
    auto joiningMembers = members; joiningMembers.back().capabilities |= PLACEMENT_STANDBY;
    auto cancelMigration = std::make_shared<std::atomic<bool>>(false);
    ClusterSecureOptions security;
    if (secure) {
        for (uint64_t id = 1; id <= 4; ++id) {
            const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
            security.pinnedPeers.push_back({id, identity.publicKey});
        }
    }
    std::array<AkkEngineOptions, 4> options;
    std::array<std::unique_ptr<AkkEngine>, 4> engines;
    for (size_t i = 0; i < engines.size(); ++i) {
        auto& opt = options[i];
        opt.paths.dataDir = dir / std::to_string(i + 1);
        opt.components.blobEnabled = false;
        opt.components.sstEnabled = false;
        opt.components.manifestEnabled = false;
        opt.components.versionLogEnabled = true;
        opt.components.clusterEnabled = true;
        opt.cluster.config = configFor(i == 3 ? joiningMembers : initialMembers);
        opt.cluster.runtime.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
        if (secure) { opt.cluster.runtime.secure = security; opt.cluster.runtime.secure.identitySeedPath = dir / ("identity-" + std::to_string(i + 1)); }
        opt.cluster.runtime.clusterGroupId = 9562;
        // Forwarded reconfiguration must complete even with one ordinary channel.
        opt.cluster.runtime.raftMaxForwardRequests = 1;
        opt.cluster.runtime.reconfiguration.resumePendingChanges = false;
        opt.cluster.runtime.reconfiguration.writePolicy = ReconfigurationWritePolicy::REJECT;
        opt.cluster.runtime.reconfiguration.maxTransferBytesPerSecond = 64;
        opt.cluster.runtime.reconfiguration.cancelled = [cancelMigration] { return cancelMigration->load(); };
        opt.cluster.runtime.routingMode = ClusterRoutingMode::FORWARD;
        opt.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
        opt.cluster.runtime.forwardingTimeoutMs = 30000;
        opt.cluster.runtime.raftHeartbeatIntervalMs = 30;
        opt.cluster.runtime.raftSnapshot.minLogEntries = 1;
        opt.cluster.runtime.stripeRebuildIntervalMs = 100;
        writeNodeIdFile(opt.paths.dataDir / "node.id", i + 1);
        if (i != 3) { engines[i] = AkkEngine::open(opt); }
    }
    const std::string keyText = keyOwnedBy(configFor(initialMembers), 3, "stripe-migration-history-");
    const auto key = bytesOf(keyText);
    AKK_CLUSTER_CHECK(waitUntil([&] { try { engines[0]->put(key, bytesOf("first")); return true; } catch (...) { return false; } }, 15s));
    const auto checkpoint = engines[0]->createClusterCheckpoint();
    engines[0]->put(key, bytesOf("second"));
    const auto before = akk_test::collectHistory(engines[0]->history(key));
    AKK_CLUSTER_CHECK(waitUntil([&] {
        for (size_t i = 1; i <= 3; ++i) {
            if (readRaftLogLastIncludedIndex(dir / std::to_string(i) / "placement-authority" / "cluster-raft.log") == 0) { return false; }
        }
        return true;
    }, 10s));
    engines[3] = AkkEngine::open(options[3]);
    const auto authority = engines[0]->clusterReconfigurationStatus().authorityNodeId;
    auto migration = std::async(std::launch::async, [&] {
        try { engines[authority - 1]->reconfigureCluster(configFor(members)); return false; }
        catch (const std::exception&) { return true; }
    });
    const auto intentObserved = waitUntil([&] {
        try { return engines[0]->clusterReconfigurationStatus().pendingGeneration != 0; }
        catch (...) { return false; }
    }, 10s);
    cancelMigration->store(true);
    AKK_CLUSTER_CHECK(migration.get());
    AKK_CLUSTER_CHECK(intentObserved);
    const auto pending = engines[0]->clusterReconfigurationStatus();
    AKK_CLUSTER_CHECK(pending.activeGeneration == 1 && pending.pendingGeneration == 2);
    bool acceptedWhilePaused = false;
    try { engines[0]->put(key, bytesOf("must-not-publish")); acceptedWhilePaused = true; }
    catch (const std::exception&) {}
    AKK_CLUSTER_CHECK(!acceptedWhilePaused);
    const auto pendingAuthorityLog = dir / std::to_string(pending.authorityNodeId) / "placement-authority" / "cluster-raft.log";
    const auto pendingCommitIndex = readRaftLogHeaderValue(pendingAuthorityLog, 5);
    AKK_CLUSTER_CHECK(pendingCommitIndex != 0);
    AKK_CLUSTER_CHECK(waitUntil([&] {
        return readRaftLogLastIncludedIndex(pendingAuthorityLog) >= pendingCommitIndex;
    }, 10s));
    std::fprintf(stderr, "[stripe online] cancelled intent and write rejection\n");
    for (auto& engine : engines) { engine->close(); engine.reset(); }
    cancelMigration->store(false);
    for (size_t i = 0; i < engines.size(); ++i) {
        options[i].cluster.config.reset();
        options[i].cluster.runtime.reconfiguration.resumePendingChanges = true;
        options[i].cluster.runtime.reconfiguration.maxTransferBytesPerSecond = 0;
        engines[i] = AkkEngine::open(options[i]);
    }
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try {
            const auto status = engines[0]->clusterReconfigurationStatus();
            return status.activeGeneration == 2 && status.pendingGeneration == 0;
        }
        catch (...) { return false; }
    }, 90s));
    engines[0]->reconfigureCluster(configFor(members)); // Repeating a completed plan is idempotent.
    std::fprintf(stderr, "[stripe online] resumed after restart\n");
    AKK_CLUSTER_CHECK(textOf(*engines[3]->get(key)) == "second");
    engines[2]->close(); engines[2].reset(); // Remove an unavailable shard holder.
    auto remaining = members; remaining.erase(remaining.begin() + 2);
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { engines[0]->reconfigureCluster(configFor(remaining)); return true; }
        catch (...) { return false; }
    }, 90s));
    std::fprintf(stderr, "[stripe online] unavailable member removed\n");
    const auto after = akk_test::collectHistory(engines[3]->history(key));
    AKK_CLUSTER_CHECK(after.size() == before.size());
    for (size_t i = 0; i < before.size(); ++i) {
        AKK_CLUSTER_CHECK(after[i].seq == before[i].seq && after[i].value == before[i].value &&
            after[i].sourceNodeId == before[i].sourceNodeId && after[i].timestampNs == before[i].timestampNs);
    }
    AKK_CLUSTER_CHECK(engines[0]->clusterReconfigurationStatus().activeGeneration == 3);
    AKK_CLUSTER_CHECK(engines[3]->count() == 1);
    const auto rollback = engines[0]->rollbackTo(checkpoint, {.conflict = akkaradb::engine::RollbackConflictPolicy::OVERWRITE_LATEST});
    AKK_CLUSTER_CHECK(rollback.complete() && textOf(*engines[0]->get(key)) == "first");
    for (auto& engine : engines) { if (engine) { engine->close(); engine.reset(); } }
    for (const size_t i : {size_t{0}, size_t{1}, size_t{3}}) { options[i].cluster.config.reset(); engines[i] = AkkEngine::open(options[i]); }
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { const auto value = engines[3]->get(key); return value && textOf(*value) == "first" && akk_test::collectHistory(engines[3]->history(key)).size() >= 3; }
        catch (...) { return false; }
    }, 20s));
    for (auto& engine : engines) { if (engine) { engine->close(); } }
    std::filesystem::remove_all(dir);
}

void testMirrorLocalAcceptanceLoss() {
    using akkaradb::engine::AkkEngine;
    using akkaradb::engine::AkkEngineOptions;
    using namespace std::chrono_literals;
    const auto dir = makeTempDir("mirror-local-acceptance-loss");
    std::vector<NodeInfo> members;
    std::array<std::unique_ptr<RaftInboundFaultProxy>, 3> proxies;
    for (size_t i = 0; i < 3; ++i) {
        const auto port = static_cast<uint16_t>(28311 + i);
        members.push_back(node(i + 1, static_cast<uint16_t>(28301 + i), port));
        proxies[i] = std::make_unique<RaftInboundFaultProxy>(port, true);
        proxies[i]->setBlocked(false);
    }
    const ClusterConfig config{members, ReplicationMode::MIRROR, {},
        {.mode = ConsistencyMode::ASYNC, .writeConsistency = WriteConsistency::LOCAL, .ackTimeoutMs = 2000},
        {}, {}, 0, testClusterId(9563), {}, FailoverPolicy::ALLOW_ACKNOWLEDGED_LOSS};
    std::array<AkkEngineOptions, 3> options;
    std::array<std::unique_ptr<AkkEngine>, 3> engines;
    for (size_t i = 0; i < 3; ++i) {
        auto& opt = options[i];
        opt.paths.dataDir = dir / std::to_string(i + 1);
        opt.components.blobEnabled = false;
        opt.components.sstEnabled = false;
        opt.components.manifestEnabled = false;
        opt.components.versionLogEnabled = true;
        opt.components.clusterEnabled = true;
        opt.cluster.config = config;
        opt.cluster.runtime.transportMode = TransportMode::PLAIN;
        opt.cluster.runtime.replBindHost = "127.0.0.2";
        opt.cluster.runtime.routingMode = ClusterRoutingMode::FORWARD;
        opt.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
        opt.cluster.runtime.raftHeartbeatIntervalMs = 30;
        writeNodeIdFile(opt.paths.dataDir / "node.id", i + 1);
        engines[i] = AkkEngine::open(opt);
    }
    const std::string keptKey = "quorum-visible", lostKey = "local-only";
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { engines[0]->put(bytesOf(keptKey), bytesOf("kept")); return akk_test::collectHistory(engines[0]->history(bytesOf(keptKey))).size() != 0; }
        catch (...) { return false; }
    }, 15s));
    const auto before = akk_test::collectHistory(engines[0]->history(bytesOf(keptKey)));
    const auto leader = static_cast<size_t>(before.back().sourceNodeId - 1);
    for (auto& proxy : proxies) { proxy->rejectMutations(true); }
    // Peer heartbeats and fencing still succeed, but neither follower can
    // persist this mutation. Local acknowledgement must precede quorum commit.
    engines[leader]->put(bytesOf(lostKey), bytesOf("accepted"));
    AKK_CLUSTER_CHECK(!engines[leader]->get(bytesOf(lostKey)));
    engines[leader]->close(); engines[leader].reset();
    AkkEngine* survivor = nullptr;
    for (auto& engine : engines) { if (engine) { survivor = engine.get(); break; } }
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { const auto value = survivor->get(bytesOf(keptKey)); return value && textOf(*value) == "kept"; }
        catch (...) { return false; }
    }, 15s));
    AKK_CLUSTER_CHECK(!survivor->get(bytesOf(lostKey)) && akk_test::collectHistory(survivor->history(bytesOf(lostKey))).empty());
    for (auto& proxy : proxies) { proxy->rejectMutations(false); }
    engines[leader] = AkkEngine::open(options[leader]);
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { engines[leader]->put(bytesOf("new-owner"), bytesOf("after")); return survivor->get(bytesOf("new-owner")).has_value(); }
        catch (...) { return false; }
    }, 15s));
    AKK_CLUSTER_CHECK(!engines[leader]->get(bytesOf(lostKey)));
    const auto after = akk_test::collectHistory(engines[leader]->history(bytesOf(keptKey)));
    AKK_CLUSTER_CHECK(after.front().seq == before.front().seq && after.front().timestampNs == before.front().timestampNs);
    for (auto& engine : engines) { engine->close(); engine.reset(); }
    for (auto& proxy : proxies) { AKK_CLUSTER_CHECK(!proxy->failed()); proxy.reset(); }
    std::filesystem::remove_all(dir);
}

void testMirrorOnlineRetainsHistory() {
    using akkaradb::engine::AkkEngine;
    using akkaradb::engine::AkkEngineOptions;
    using namespace std::chrono_literals;
    const auto dir = makeTempDir("mirror-online-history");
    const std::vector members{node(1, 28501, 28511), node(2, 28502, 28512), node(3, 28503, 28513)};
    auto raft = onlineRaftMembership(); raft.membership.allowLearners = true;
    const auto configFor = [&](bool joining, bool final) {
        auto selected = members;
        if (!joining && !final) { selected.resize(2); }
        if (joining) { selected.back().capabilities |= RAFT_LEARNER; }
        return ClusterConfig{selected, ReplicationMode::MIRROR, {}, {.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 2000},
            raft, {}, 0, testClusterId(9564)};
    };
    std::array<std::unique_ptr<AkkEngine>, 3> engines;
    const auto open = [&](size_t i) {
        AkkEngineOptions options;
        options.paths.dataDir = dir / std::to_string(i + 1);
        options.components.blobEnabled = false;
        options.components.sstEnabled = false;
        options.components.manifestEnabled = false;
        options.components.versionLogEnabled = true;
        options.components.clusterEnabled = true;
        options.cluster.config = configFor(i == 2, false);
        options.cluster.runtime.transportMode = TransportMode::PLAIN;
        options.cluster.runtime.routingMode = ClusterRoutingMode::FORWARD;
        options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
        options.cluster.runtime.raftHeartbeatIntervalMs = 30;
        options.cluster.runtime.raftSnapshot.minLogEntries = 1;
        writeNodeIdFile(options.paths.dataDir / "node.id", i + 1);
        engines[i] = AkkEngine::open(std::move(options));
    };
    open(0); open(1);
    const std::string keyText = "mirror-history"; const auto key = bytesOf(keyText);
    AKK_CLUSTER_CHECK(waitUntil([&] { try { engines[0]->put(key, bytesOf("first")); return true; } catch (...) { return false; } }, 15s));
    engines[0]->put(key, bytesOf("second"));
    const auto before = akk_test::collectHistory(engines[0]->history(key));
    AKK_CLUSTER_CHECK(waitUntil([&] {
        return engines[0]->stats().cluster.snapshotIndex != 0 && engines[1]->stats().cluster.snapshotIndex != 0;
    }, 10s));
    const auto owner = static_cast<size_t>(before.back().sourceNodeId - 1);
    open(2);
    std::string migrationError;
    const bool migrated = waitUntil([&] {
        try { engines[owner]->reconfigureCluster(configFor(false, true)); return true; }
        catch (const std::exception& e) { migrationError = e.what(); return false; }
    }, 20s);
    if (!migrated) { std::fprintf(stderr, "[mirror migration] %s\n", migrationError.c_str()); }
    AKK_CLUSTER_CHECK(migrated);
    const auto after = akk_test::collectHistory(engines[2]->history(key));
    AKK_CLUSTER_CHECK(after.size() == before.size());
    for (size_t i = 0; i < before.size(); ++i) {
        AKK_CLUSTER_CHECK(after[i].seq == before[i].seq && after[i].value == before[i].value &&
            after[i].sourceNodeId == before[i].sourceNodeId && after[i].timestampNs == before[i].timestampNs);
    }
    for (auto& engine : engines) { engine->close(); engine.reset(); }
    std::filesystem::remove_all(dir);
}

void testRaid10OnlineSpareReplacement() {
    using akkaradb::engine::AkkEngine;
    using akkaradb::engine::AkkEngineOptions;
    using namespace std::chrono_literals;
    const auto dir = makeTempDir("raid10-online-spare");
    std::vector<NodeInfo> members;
    for (uint64_t id = 1; id <= 6; ++id) { members.push_back(node(id, static_cast<uint16_t>(28600 + id), static_cast<uint16_t>(28610 + id))); }
    members[4].capabilities |= STRIPE_FAILOVER_ELIGIBLE;
    const auto configFor = [&](std::vector<NodeInfo> selected) {
        return ClusterConfig{selected, ReplicationMode::STRIPE, {}, {.ackTimeoutMs = 2000}, {},
            {.dataShards = 2, .parityShards = 0, .copiesPerShard = 2}, 0, testClusterId(9565)};
    };
    auto initial = members; initial.resize(5);
    auto joining = members; joining.back().capabilities |= PLACEMENT_STANDBY;
    std::array<std::unique_ptr<AkkEngine>, 6> engines;
    const auto open = [&](size_t i) {
        AkkEngineOptions options;
        options.paths.dataDir = dir / std::to_string(i + 1);
        options.components.blobEnabled = false;
        options.components.sstEnabled = false;
        options.components.manifestEnabled = false;
        options.components.versionLogEnabled = true;
        options.components.clusterEnabled = true;
        options.cluster.config = configFor(i == 5 ? joining : initial);
        options.cluster.runtime.transportMode = TransportMode::PLAIN;
        options.cluster.runtime.clusterGroupId = 9565;
        options.cluster.runtime.routingMode = ClusterRoutingMode::FORWARD;
        options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
        options.cluster.runtime.forwardingTimeoutMs = 30000;
        options.cluster.runtime.raftHeartbeatIntervalMs = 30;
        writeNodeIdFile(options.paths.dataDir / "node.id", i + 1);
        engines[i] = AkkEngine::open(std::move(options));
    };
    for (size_t i = 0; i < 5; ++i) { open(i); }
    const std::string keyText = keyOwnedBy(configFor(initial), 3, "raid10-history-"); const auto key = bytesOf(keyText);
    AKK_CLUSTER_CHECK(waitUntil([&] { try { engines[0]->put(key, bytesOf("first")); return true; } catch (...) { return false; } }, 15s));
    engines[0]->put(key, bytesOf("second"));
    const auto before = akk_test::collectHistory(engines[0]->history(key));
    open(5);
    auto replacement = members; replacement.erase(replacement.begin() + 2);
    AKK_CLUSTER_CHECK(waitUntil([&] { try { engines[0]->reconfigureCluster(configFor(replacement)); return true; } catch (...) { return false; } }, 45s));
    engines[2]->close(); engines[2].reset();
    replacement.front().capabilities |= STRIPE_FAILOVER_ELIGIBLE;
    for (auto& node : replacement) { if (node.nodeId == 5) { node.capabilities &= ~STRIPE_FAILOVER_ELIGIBLE; } }
    AKK_CLUSTER_CHECK(waitUntil([&] { try { engines[0]->reconfigureCluster(configFor(replacement)); return true; } catch (...) { return false; } }, 45s));
    const auto after = akk_test::collectHistory(engines[5]->history(key));
    AKK_CLUSTER_CHECK(after.size() == before.size());
    for (size_t i = 0; i < before.size(); ++i) {
        AKK_CLUSTER_CHECK(after[i].seq == before[i].seq && after[i].value == before[i].value &&
            after[i].sourceNodeId == before[i].sourceNodeId && after[i].timestampNs == before[i].timestampNs);
    }
    AKK_CLUSTER_CHECK(textOf(*engines[5]->get(key)) == "second");
    for (auto& engine : engines) { if (engine) { engine->close(); engine.reset(); } }
    std::filesystem::remove_all(dir);
}

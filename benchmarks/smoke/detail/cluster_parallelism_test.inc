struct ParallelismGate {
    std::mutex mutex;
    std::condition_variable cv;
    bool entered = false, released = false;
    void pause() {
        std::unique_lock lock{mutex}; entered = true; cv.notify_all();
        if (!cv.wait_for(lock, std::chrono::seconds{10}, [&] { return released; })) {
            throw std::runtime_error("parallelism test: gate was not released");
        }
    }
    bool await() {
        std::unique_lock lock{mutex};
        return cv.wait_for(lock, std::chrono::seconds{5}, [&] { return entered; });
    }
    void release() { std::lock_guard lock{mutex}; released = true; cv.notify_all(); }
};
struct ReleaseParallelismGate {
    ParallelismGate& gate;
    ~ReleaseParallelismGate() { gate.release(); }
};

void testTargetedAckMatchesRequest() {
    const auto config = ClusterConfig{{node(1, 25601, 25611), node(2, 25602, 25612)}, ReplicationMode::MIRROR,
        AckPolicy{.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::APPLIED}, ConsistencyOptions{.ackTimeoutAction = AckTimeoutAction::FAIL_ACK,
            .ackTimeoutMs = 3000}, {}, {}, 1, testClusterId(9560)};
    ClusterRuntimeOptions options; options.transportMode = TransportMode::PLAIN;
    options.clusterGroupId = 9560; options.replicationClusterId = config.clusterId();
    options.replicationConfigFingerprint = config.replicationFingerprint();
    auto server = ReplicationServer::create(25611, 1, [] { return uint64_t{0}; }, config.ackPolicy(),
        config.consistency(), 1, {2}, options);
    auto client = ReplicationClient::create("127.0.0.1", 25611, 2, [] { return uint64_t{0}; }, config.ackPolicy(), options, true);
    ParallelismGate gate;
    client->setApplyCallback([&](uint64_t seq, auto, auto, auto, auto, auto) { if (seq == 1) { gate.pause(); } });
    server->start(); client->start();
    AKK_CLUSTER_CHECK(waitUntil([&] { return server->replicaCount() == 1; }));
    server->shipEntryTo(2, 2, ReplOpType::PUT, bytesOf("two"), bytesOf("value"), 0, 1, true);
    auto earlier = std::async(std::launch::async, [&] {
        server->shipEntryTo(2, 1, ReplOpType::PUT, bytesOf("one"), bytesOf("value"), 0, 1, true);
    });
    ReleaseParallelismGate release{gate};
    AKK_CLUSTER_CHECK(gate.await());
    AKK_CLUSTER_CHECK(earlier.wait_for(std::chrono::milliseconds{100}) == std::future_status::timeout);
    gate.release(); earlier.get(); client->close(); server->close();
    std::fprintf(stderr, "[parallelism] a later targeted ACK cannot acknowledge an earlier request\n");
}

void testRollbackControlSerialization() {
    const auto config = ClusterConfig{{node(1, 25661, 25671), node(2, 25662, 25672)}, ReplicationMode::MIRROR,
        {}, {}, {}, {}, 1, testClusterId(9563)};
    ClusterRuntimeOptions options; options.transportMode = TransportMode::PLAIN;
    options.clusterGroupId = 9563; options.replicationClusterId = config.clusterId();
    options.replicationConfigFingerprint = config.replicationFingerprint();
    auto server = ReplicationServer::create(25671, 1, [] { return uint64_t{0}; }, config.ackPolicy(),
        config.consistency(), 1, {2}, options);
    auto client = ReplicationClient::create("127.0.0.1", 25671, 2, [] { return uint64_t{0}; }, config.ackPolicy(), options, true);
    ParallelismGate gate;
    server->setStripeControlCallback([&](auto, const StripeControlRequest& request, auto) {
        if (textOf(request.key) == "first") { gate.pause(); }
        StripeControlResponse response; response.status = StripeControlStatus::GRANTED; return response;
    });
    server->start(); client->start();
    AKK_CLUSTER_CHECK(waitUntil([&] { return server->replicaCount() == 1; }));
    const auto call = [&](StripeControlAction action, std::string_view key) {
        StripeControlRequest request; request.action = action; request.key.assign(key.begin(), key.end());
        return client->stripeControl(std::move(request), 3000);
    };
    auto first = std::async(std::launch::async, [&] { return call(StripeControlAction::ROLLBACK_WATERMARK, "first"); });
    ReleaseParallelismGate release{gate};
    AKK_CLUSTER_CHECK(gate.await());
    auto second = std::async(std::launch::async, [&] { return call(StripeControlAction::ROLLBACK_KEY, "second"); });
    AKK_CLUSTER_CHECK(second.wait_for(std::chrono::milliseconds{100}) == std::future_status::timeout);
    auto third = std::async(std::launch::async, [&] { return call(StripeControlAction::ROLLBACK_STREAM, "third"); });
    AKK_CLUSTER_CHECK(third.wait_for(std::chrono::milliseconds{100}) == std::future_status::timeout);
    AKK_CLUSTER_CHECK(call(StripeControlAction::ACQUIRE, "metadata").status == StripeControlStatus::GRANTED);
    gate.release();
    AKK_CLUSTER_CHECK(first.get().status == StripeControlStatus::GRANTED);
    AKK_CLUSTER_CHECK(second.get().status == StripeControlStatus::GRANTED);
    AKK_CLUSTER_CHECK(third.get().status == StripeControlStatus::GRANTED);
    client->close(); server->close();
    std::fprintf(stderr, "[parallelism] rollback controls serialize while independent metadata RPC proceeds\n");
}

void testStripeMetadataParallelism(bool secure) {
    const auto dir = makeTempDir(secure ? "stripe-metadata-parallel-secure" : "stripe-metadata-parallel-plain");
    const ClusterConfig config{{node(1, 25621, 25631), node(2, 25622, 25632),
        node(3, 25623, 25633, DATA_AND_COORDINATOR | STRIPE_FAILOVER_ELIGIBLE)}, ReplicationMode::STRIPE, {},
        ConsistencyOptions{.ackTimeoutMs = 3000}, {}, StripeOptions{.dataShards = 1, .parityShards = 1}, 0, testClusterId(9561)};
    ClusterRuntimeOptions options; options.clusterGroupId = 9561;
    if (secure) {
        for (uint64_t id = 1; id <= 3; ++id) {
            const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
            options.secure.pinnedPeers.push_back({id, identity.publicKey});
        }
    }
    const auto key = keyOwnedBy(config, 1, "pending-metadata"), otherKey = keyOwnedBy(config, 1, "independent-metadata");
    ParallelismGate gate;
    auto leaderOptions = options, spareOptions = options;
    leaderOptions.secure.identitySeedPath = dir / "identity-2"; spareOptions.secure.identitySeedPath = dir / "identity-3";
    auto leader = makeRuntime(dir / "n2", config, 2, leaderOptions, secure, {}, [&](auto& callbacks) {
        const auto apply = callbacks.commitStripeMetadata;
        callbacks.commitStripeMetadata = [&, apply](auto seq, auto receivedKey, auto metadata) {
            if (textOf(receivedKey) == key) { gate.pause(); }
            apply(seq, receivedKey, metadata);
        };
    });
    auto spare = makeRuntime(dir / "n3", config, 3, spareOptions, secure);
    leader->runtime->start(); spare->runtime->start();
    AKK_CLUSTER_CHECK(waitUntil([&] { return leader->runtime->stripeMetadataLeaderNodeId() == 2 &&
        runtimePeerConnected(*leader->runtime, 3) && runtimePeerConnected(*spare->runtime, 2); }, std::chrono::seconds{20}));
    const auto lease = spare->runtime->acquireStripeOperation(bytesOf(key), 1);
    std::vector<uint8_t> metadata(62, 0); std::memcpy(metadata.data(), "AKSM1", 5);
    for (const auto [offset, value] : {std::pair{6u, lease.fenceToken}, {14u, lease.ownerNodeId},
            {22u, lease.authorityNodeId}, {30u, lease.fenceToken}}) {
        for (size_t i = 0; i < 8; ++i) { metadata[offset + i] = static_cast<uint8_t>(value >> (8 * i)); }
    }
    auto commit = std::async(std::launch::async, [&] { spare->runtime->commitStripeMetadata(lease, bytesOf(key), metadata); });
    ReleaseParallelismGate release{gate};
    AKK_CLUSTER_CHECK(gate.await());
    auto other = std::async(std::launch::async, [&] { return spare->runtime->acquireStripeOperation(bytesOf(otherKey), 1); });
    AKK_CLUSTER_CHECK(other.wait_for(std::chrono::milliseconds{1500}) == std::future_status::ready);
    const auto otherLease = other.get(); spare->runtime->releaseStripeOperation(otherLease, bytesOf(otherKey));
    AKK_CLUSTER_CHECK(commit.wait_for(std::chrono::milliseconds{0}) == std::future_status::timeout);
    spare->runtime->releaseStripeOperation(lease, bytesOf(key));
    auto ownerOptions = options; ownerOptions.secure.identitySeedPath = dir / "identity-1";
    auto owner = makeRuntime(dir / "n1", config, 1, ownerOptions, secure);
    owner->runtime->start();
    AKK_CLUSTER_CHECK(waitUntil([&] { return runtimePeerConnected(*owner->runtime, 2); }));
    auto handoff = std::async(std::launch::async, [&] { return owner->runtime->acquireStripeOperation(bytesOf(otherKey), 1); });
    AKK_CLUSTER_CHECK(handoff.wait_for(std::chrono::milliseconds{100}) == std::future_status::timeout);
    gate.release(); commit.get();
    const auto ownerLease = handoff.get();
    AKK_CLUSTER_CHECK(ownerLease.authorityNodeId == 1);
    owner->runtime->releaseStripeOperation(ownerLease, bytesOf(otherKey));
    AKK_CLUSTER_CHECK(spare->runtime->readStripeMetadata(bytesOf(key), 1) == metadata);
    const auto next = owner->runtime->acquireStripeOperation(bytesOf(key), 1);
    AKK_CLUSTER_CHECK(next.fenceToken != lease.fenceToken);
    bool fenced = false;
    try { spare->runtime->commitStripeMetadata(lease, bytesOf(key), metadata); } catch (const std::runtime_error&) { fenced = true; }
    AKK_CLUSTER_CHECK(fenced); owner->runtime->releaseStripeOperation(next, bytesOf(key));
    owner->runtime->close(); spare->runtime->close(); leader->runtime->close();
    std::fprintf(stderr, "[parallelism] independent RPC proceeds; pending commit blocks owner handoff (%s)\n", secure ? "SECURE" : "PLAIN");
}

void testStripeEngineIndependentKeys(bool secure) {
    using Engine = akkaradb::engine::AkkEngine;
    const auto dir = makeTempDir(secure ? "stripe-engine-parallel-secure" : "stripe-engine-parallel-plain");
    const ClusterConfig config{{node(1, 25641, 25651), node(2, 25642, 25652), node(3, 25643, 25653), node(4, 25644, 25654)},
        ReplicationMode::STRIPE, AckPolicy{.stage = AckStage::DURABLE}, ConsistencyOptions{.ackTimeoutMs = 3000}, {},
        StripeOptions{.dataShards = 2, .parityShards = 1}, 0, testClusterId(9562)};
    ClusterRuntimeOptions runtime; runtime.clusterGroupId = 9562; runtime.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
    if (secure) {
        for (uint64_t id = 1; id <= 4; ++id) {
            const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
            runtime.secure.pinnedPeers.push_back({id, identity.publicKey});
        }
    }
    const auto optionsFor = [&](uint64_t id) {
        akkaradb::engine::AkkEngineOptions options; options.paths.dataDir = dir / ("n" + std::to_string(id));
        options.components.clusterEnabled = true; options.components.blobEnabled = false;
        options.cluster.config = config; options.cluster.runtime = runtime;
        options.cluster.runtime.secure.identitySeedPath = dir / ("identity-" + std::to_string(id));
        options.cluster.runtime.stripeWriteCommitMode = StripeWriteCommitMode::ALL_SHARDS;
        writeNodeIdFile(options.paths.dataDir / "node.id", id); return options;
    };
    ClusterRouter router{config};
    const auto findKey = [&](bool usesTwo, std::string prefix) {
        for (unsigned i = 0; i < 10000; ++i) {
            const auto key = prefix + std::to_string(i);
            const auto targets = router.writeTargets(bytesOf(key));
            if (targets.front().nodeId == 1 && std::ranges::any_of(targets, [](const auto& target) { return target.nodeId == 2; }) == usesTwo) { return key; }
        }
        throw std::runtime_error("parallelism test: no placement key");
    };
    const auto slowKey = findKey(true, "slow-"), fastKey = findKey(false, "fast-");
    ParallelismGate gate; std::atomic<bool> enabled{false};
    auto one = Engine::open(optionsFor(1));
    auto peerOptions = runtime; peerOptions.secure.identitySeedPath = dir / "identity-2";
    auto slowPeer = makeRuntime(dir / "n2", config, 2, peerOptions, secure, {}, [&](auto& callbacks) {
        const auto apply = callbacks.apply;
        callbacks.apply = [&, apply](auto seq, auto op, auto key, auto value, auto flags, auto source, auto timestamp) {
            if (enabled.load()) { gate.pause(); }
            apply(seq, op, key, value, flags, source, timestamp);
        };
    });
    slowPeer->runtime->start(); auto three = Engine::open(optionsFor(3)), four = Engine::open(optionsFor(4));
    AKK_CLUSTER_CHECK(waitUntil([&] { try { one->put(bytesOf(fastKey), bytesOf("before")); return true; } catch (...) { return false; } }, std::chrono::seconds{20}));
    enabled.store(true);
    auto slow = std::async(std::launch::async, [&] { one->put(bytesOf(slowKey), bytesOf("slow-value")); });
    ReleaseParallelismGate release{gate};
    AKK_CLUSTER_CHECK(gate.await());
    auto fast = std::async(std::launch::async, [&] { one->put(bytesOf(fastKey), bytesOf("after")); });
    AKK_CLUSTER_CHECK(fast.wait_for(std::chrono::milliseconds{1500}) == std::future_status::ready); fast.get();
    AKK_CLUSTER_CHECK(textOf(*one->get(bytesOf(fastKey))) == "after");
    AKK_CLUSTER_CHECK(slow.wait_for(std::chrono::milliseconds{0}) == std::future_status::timeout);
    gate.release(); slow.get();
    AKK_CLUSTER_CHECK(textOf(*one->get(bytesOf(slowKey))) == "slow-value");
    four->close(); three->close(); slowPeer->runtime->close(); one->close();
    std::fprintf(stderr, "[parallelism] engine writes on an independent placement bypass a blocked shard (%s)\n", secure ? "SECURE" : "PLAIN");
}

void testRaftLogDeltaIndex() {
    using Index = akkaradb::engine::cluster::detail::RaftLogIndex;
    Index index;
    for (uint64_t i = 1; i <= 1000; ++i) { index.append({i, 1, (i - 1) / 100 + 1, (i - 1) % 100 * 20, (i - 1) % 100 * 20 + 20}); }
    AKK_CLUSTER_CHECK(index.size() == 1000 && index.extents().size() == 10);
    AKK_CLUSTER_CHECK(!index.reconcile(1, 1001, UINT64_MAX));
    AKK_CLUSTER_CHECK(index.reconcile(151, 1001, UINT64_MAX));
    AKK_CLUSTER_CHECK(index.size() == 850 && index.extents().front().begin == 1000);
    AKK_CLUSTER_CHECK(index.reconcile(151, 1001, 951));
    AKK_CLUSTER_CHECK(index.size() == 800 && index.back().index == 950 && index.extents().back().end == 1000);
    for (uint64_t i = 951; i <= 1000; ++i) { index.append({i, 2, 11, (i - 951) * 20, (i - 950) * 20}); }
    AKK_CLUSTER_CHECK(index.extents().size() == 10 && index.back().term == 2);
    AKK_CLUSTER_CHECK(index.reconcile(1001, 1001, UINT64_MAX) && index.empty() && index.extents().empty());
    uint64_t checksum = 0;
    for (uint64_t retained : {1000ull, 100000ull}) {
        Index cached; std::vector<Index::Location> previous;
        for (uint64_t i = 1; i <= retained; ++i) {
            const Index::Location location{i, 1, 1, (i - 1) * 20, i * 20}; cached.append(location); previous.push_back(location);
        }
        constexpr unsigned rounds = 500;
        const auto before = std::chrono::steady_clock::now();
        for (unsigned r = 0; r < rounds; ++r) {
            std::vector<Index::Location> rebuilt; rebuilt.reserve(previous.size());
            for (const auto& location : previous) { rebuilt.push_back(location); }
            checksum += rebuilt.back().index;
        }
        const auto middle = std::chrono::steady_clock::now();
        for (unsigned r = 0; r < rounds; ++r) {
            AKK_CLUSTER_CHECK(!cached.reconcile(1, retained + 1, UINT64_MAX)); checksum += cached.back().index;
        }
        const auto after = std::chrono::steady_clock::now();
        std::fprintf(stderr, "[raft index CPU] %llu retained, %u metadata updates: rebuild=%lldus cached=%lldus\n",
            static_cast<unsigned long long>(retained), rounds,
            static_cast<long long>(std::chrono::duration_cast<std::chrono::microseconds>(middle - before).count()),
            static_cast<long long>(std::chrono::duration_cast<std::chrono::microseconds>(after - middle).count()));
    }
    AKK_CLUSTER_CHECK(checksum != 0);
}

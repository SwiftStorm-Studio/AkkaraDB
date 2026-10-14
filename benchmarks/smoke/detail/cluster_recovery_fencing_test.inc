/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */

void testNonRaftAheadReplicaIsQuarantined(bool secure, bool history) {
    const auto dir = makeTempDir("ahead-replica-quarantine");
    ClusterRuntimeOptions options;
    options.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
    options.replBindHost = "127.0.0.1";
    options.clusterGroupId = 99141; options.clusterGroupEpoch = 1;
    options.secure.expectedPrimaryNodeId = 1;
    if (secure) {
        for (uint64_t id : {1, 2}) {
            const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
            options.secure.pinnedPeers.push_back({id, identity.publicKey});
        }
    }
    auto serverOptions = options, clientOptions = options;
    serverOptions.secure.identitySeedPath = dir / "identity-1";
    clientOptions.secure.identitySeedPath = dir / "identity-2";
    const AckPolicy ack{.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::DURABLE};
    ConsistencyOptions consistency;
    consistency.ackTimeoutMs = 200;
    consistency.ackTimeoutAction = AckTimeoutAction::ACCEPT_LOCAL;
    std::atomic<uint64_t> primarySeq{0}, applied{0}, snapshots{0};
    std::atomic<uint8_t> replicaValue{'A'};
    ReplicationServer::HistoryProvider journal;
    if (history) { journal = [](uint64_t, uint64_t) { return std::optional{std::vector<ReplEntry>{}}; }; }
    auto server = ReplicationServer::create(39141, 1, [&] { return primarySeq.load(); }, ack,
        consistency, 1, {2}, serverOptions, journal, [&]() -> std::optional<ReplicationServer::Snapshot> {
            ++snapshots; return {};
        });
    auto client = ReplicationClient::create("127.0.0.1", 39141, 2, [] { return uint64_t{1}; }, ack, clientOptions);
    client->setApplyCallback([&](uint64_t, ReplOpType, auto, auto value, uint8_t, uint64_t) {
        replicaValue.store(value[0]); ++applied;
    });
    client->setForceDurableCallback([] {});
    server->start(); client->start();
    AKK_CLUSTER_CHECK(waitUntil([&] { return server->stats().recoveryRequired; }));
    // A new local branch reaching the same number must not turn this peer LIVE.
    primarySeq.store(1);
    bool rejected = false;
    try { server->shipEntry(1, ReplOpType::PUT, bytesOf("k"), bytesOf("B"), 0, 1); }
    catch (const std::runtime_error& error) { rejected = std::string_view{error.what()}.find("recovery/resynchronization required") != std::string_view::npos; }
    AKK_CLUSTER_CHECK(rejected && server->replicaCount() == 0 && applied.load() == 0 && replicaValue.load() == 'A' && snapshots.load() == 0);
    client->close(); client->start();
    std::this_thread::sleep_for(std::chrono::milliseconds{500});
    AKK_CLUSTER_CHECK(!client->connected() && server->replicaCount() == 0 && applied.load() == 0);
    client->close(); server->close();
    rejected = false;
    try { server->start(); } catch (const std::runtime_error&) { rejected = true; }
    AKK_CLUSTER_CHECK(rejected && !client->connected() && applied.load() == 0);
}

void testNonRaftQuarantineSurvivesReopen() {
    const auto dir = makeTempDir("nonraft-durable-quarantine");
    const ClusterConfig cfg{{node(1, 39301, 39311), node(2, 39302, 39312)}, ReplicationMode::MIRROR,
        {.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::DURABLE},
        {.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 500}, {}, {}, 1, testClusterId(99301)};
    ClusterRuntimeOptions primary, replica;
    primary.clusterGroupId = replica.clusterGroupId = 99301;
    primary.clusterGroupEpoch = replica.clusterGroupEpoch = 1;
    primary.startupRole = NodeStartupRole::PRIMARY; replica.startupRole = NodeStartupRole::REPLICA;
    std::atomic<bool> fatal{false};
    auto n1 = makeRuntime(dir / "n1", cfg, 1, primary, false, {}, [&](auto& callbacks) {
        callbacks.fatalFailure = [&](auto) { fatal.store(true); };
    });
    auto n2 = makeRuntime(dir / "n2", cfg, 2, replica);
    n2->setState(1, bytesOf("k"), bytesOf("A"));
    n1->runtime->start(); n2->runtime->start();
    AKK_CLUSTER_CHECK(waitUntil([&] { return fatal.load(); }));
    const auto state = n1->runtime->raftStats();
    AKK_CLUSTER_CHECK(state.health == akkaradb::engine::ClusterHealthState::FAILED && state.lastFailure == akkaradb::engine::ClusterFailureCode::REPLICATION_DIVERGENCE);
    bool entered = false, rejected = false;
    try { n1->runtime->executePrimaryWrite([&] { entered = true; }); }
    catch (const std::runtime_error&) { rejected = true; }
    AKK_CLUSTER_CHECK(rejected && !entered && n1->lastSeq.load() == 0 && n2->hasState(1, "k", "A"));
    n2->runtime->close(); n1->runtime->close(); n1.reset();
    primary.resetClusterMembership = true;
    rejected = false;
    try { (void)makeRuntime(dir / "n1", cfg, 1, primary); }
    catch (const std::runtime_error& error) { rejected = std::string_view{error.what()}.find("persistent replication inconsistency") != std::string_view::npos; }
    AKK_CLUSTER_CHECK(rejected && std::filesystem::exists(dir / "n1" / "cluster.membership.replication-recovery-required"));
}

void testNonRaftFrontierRejectsLostTailBeforeReconnect() {
    const auto dir = makeTempDir("nonraft-outbound-frontier");
    const ClusterConfig cfg{{node(1, 39401, 39411), node(2, 39402, 39412)}, ReplicationMode::MIRROR,
        {}, {.mode = ConsistencyMode::PRIMARY_ACK}, {}, {}, 1, testClusterId(99401)};
    ClusterRuntimeOptions options; options.startupRole = NodeStartupRole::PRIMARY;
    options.clusterGroupId = 99401; options.clusterGroupEpoch = 1;
    options.clusterMembershipPath = dir.filename().string() + ".members";
    {
        auto n1 = makeRuntime(dir / "n1", cfg, 1, options);
        n1->setState(1, bytesOf("k"), bytesOf("A")); n1->runtime->start();
        n1->runtime->shipEntry(1, ReplOpType::PUT, bytesOf("k"), bytesOf("A"), 0, 1);
        n1->runtime->close();
    }
    auto frontier = options.clusterMembershipPath; frontier += ".replication-frontier";
    AKK_CLUSTER_CHECK(std::filesystem::exists(frontier));
    {
        auto reopened = makeRuntime(dir / "n1", cfg, 1, options);
        AKK_CLUSTER_CHECK(reopened->hasState(1, "k", "A"));
        reopened->runtime->start(); reopened->runtime->close();
    }
    bool rejected = false;
    try {
        (void)makeRuntime(dir / "n1", cfg, 1, options, false, {}, [](auto& callbacks) {
            // Model a valid recovered WAL prefix which lost the outbound tail.
            callbacks.getLastSeq = [] { return uint64_t{0}; };
        });
    } catch (const std::runtime_error& error) {
        rejected = std::string_view{error.what()}.find("behind the durable replication frontier") != std::string_view::npos;
    }
    AKK_CLUSTER_CHECK(rejected);
    corruptFileByte(frontier);
    options.resetClusterMembership = true;
    options.corruptStateAction = CorruptClusterStateAction::BACKUP_AND_RECREATE;
    rejected = false;
    try { (void)makeRuntime(dir / "n1", cfg, 1, options); } catch (const std::runtime_error&) { rejected = true; }
    AKK_CLUSTER_CHECK(rejected && std::filesystem::exists(frontier));
}

void testStripeRepairRejectsChangedValidationTerm() {
    const auto dir = makeTempDir("stripe-repair-validation-term");
    const ClusterConfig cfg{{node(1, 39201, 39211), node(2, 39202, 39212), node(3, 39203, 39213)},
        ReplicationMode::STRIPE, {}, ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 30000},
        {}, StripeOptions{.dataShards = 2, .parityShards = 1}, 0, testClusterId(99201)};
    ClusterRuntimeOptions options;
    options.clusterGroupId = 99201; options.clusterGroupEpoch = 1;
    options.raftHeartbeatIntervalMs = 100;
    std::mutex gateMu; std::condition_variable gateCv;
    std::atomic<bool> armed{false}; bool entered = false, released = false;
    auto n1 = makeRuntime(dir / "n1", cfg, 1, options, false, {}, [&](auto& callbacks) {
        auto read = callbacks.readStripeMetadata;
        callbacks.readStripeMetadata = [&, read](auto key) {
            auto saved = read(key);
            if (armed.exchange(false)) {
                std::unique_lock lock{gateMu}; entered = true; gateCv.notify_all();
                if (!gateCv.wait_for(lock, std::chrono::seconds{40}, [&] { return released; })) {
                    throw std::runtime_error("stripe repair validation gate timed out");
                }
            }
            return saved;
        };
    });
    auto n2 = makeRuntime(dir / "n2", cfg, 2, options), n3 = makeRuntime(dir / "n3", cfg, 3, options);
    n1->runtime->start(); n2->runtime->start(); n3->runtime->start();
    RuntimeHarness* nodes[]{n1.get(), n2.get(), n3.get()};
    const auto leaderIs = [&](uint64_t id) {
        return std::ranges::all_of(nodes, [&](auto* n) { return n->runtime->stripeMetadataLeaderNodeId() == id; });
    };
    AKK_CLUSTER_CHECK(waitUntil([&] { return leaderIs(1); }, std::chrono::seconds{20}));
    const auto key = keyOwnedBy(cfg, 2, "validation-term-key");
    const auto metadata = [](const StripeOperationLease& lease, uint8_t marker) {
        std::vector<uint8_t> out{'A','K','S','M','1',0};
        for (auto field : {lease.fenceToken, lease.ownerNodeId, lease.authorityNodeId, lease.fenceToken}) { appendRaftTestInteger(out, field); }
        out.resize(62); out.back() = marker; return out;
    };
    auto first = n2->runtime->acquireStripeOperation(bytesOf(key), 2);
    const auto original = metadata(first, 'A');
    n2->runtime->commitStripeMetadata(first, bytesOf(key), original);
    AKK_CLUSTER_CHECK(waitUntil([&] { std::lock_guard lock{n1->stateMutex}; return n1->stripeMetadata[key] == original; }));
    auto repair = original; repair.back() = 'R'; armed.store(true);
    auto pending = std::async(std::launch::async, [&] { return n1->runtime->repairStripeMetadata(bytesOf(key), first.fenceToken, repair); });
    const auto release = [&] { std::lock_guard lock{gateMu}; released = true; gateCv.notify_all(); };
    std::vector<uint8_t> newer;
    bool accepted = true; std::exception_ptr failure;
    try {
        { std::unique_lock lock{gateMu}; AKK_CLUSTER_CHECK(gateCv.wait_for(lock, std::chrono::seconds{5}, [&] { return entered; })); }
        auto members = cfg.stripeMetadataNodes();
        for (auto& member : members) {
            member.dataPort = 0; member.replPort = member.stripeMetadataPort;
            member.stripeMetadataPort = member.partitionReplBasePort = 0;
            member.capabilities = DATA_BEARING | COORDINATOR_ELIGIBLE;
        }
        auto raft = onlineRaftMembership(); raft.membership.allowLearners = true;
        const ClusterConfig control{members, ReplicationMode::MIRROR,
            AckPolicy{.mode = AckPolicyMode::QUORUM, .stage = AckStage::DURABLE, .quorum = 1},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .writeConsistency = WriteConsistency::QUORUM,
                .ackTimeoutAction = AckTimeoutAction::FAIL_WRITE, .replicaLagAction = ReplicaLagAction::ASYNC_RESYNC, .ackTimeoutMs = 30000},
            raft, {}, 0, cfg.clusterId()};
        const auto forceLeader = [&](uint64_t from, uint64_t target) {
            const auto term = nodes[target - 1]->runtime->stripeMetadataRaftStats().currentTerm;
            auto wireOptions = options; wireOptions.raftGroupKind = RaftGroupKind::PLACEMENT;
            RaftTestSocket socket;
            // The observed leader's persistent channel is occupied. This trusted
            // PLAIN fixture delivers its TimeoutNow on the third peer's spare channel.
            openRaftTestPeer(socket, cfg.findById(target)->stripeMetadataPort, 3, control, '3', wireOptions);
            std::vector<uint8_t> bytes;
            for (auto value : {term, from, target}) { appendRaftTestInteger(bytes, value); }
            AKK_CLUSTER_CHECK(socket.send(encodeFrame(ReplMsgType::RAFT_TIMEOUT_NOW, bytes)));
            const auto reply = socket.receiveFrame();
            AKK_CLUSTER_CHECK(reply.type == ReplMsgType::RAFT_TIMEOUT_NOW_RESPONSE && reply.payload.at(8));
            AKK_CLUSTER_CHECK(waitUntil([&] { return leaderIs(target); }, std::chrono::seconds{10}));
        };
        forceLeader(1, 2);
        auto lease = n2->runtime->acquireStripeOperation(bytesOf(key), 2);
        newer = metadata(lease, 'B'); n2->runtime->commitStripeMetadata(lease, bytesOf(key), newer);
        AKK_CLUSTER_CHECK(waitUntil([&] { std::lock_guard lock{n1->stateMutex}; return n1->stripeMetadata[key] == newer; }));
        forceLeader(2, 1);
    } catch (...) { failure = std::current_exception(); }
    release();
    try { accepted = pending.get(); } catch (...) { accepted = false; }
    if (!failure) {
        try {
            AKK_CLUSTER_CHECK(!accepted);
            const auto allMetadataEquals = [&](const auto& expected) {
                return std::ranges::all_of(nodes, [&](auto* n) {
                    std::lock_guard lock{n->stateMutex}; return n->stripeMetadata[key] == expected;
                });
            };
            AKK_CLUSTER_CHECK(waitUntil([&] { return allMetadataEquals(newer); }));
            auto validRepair = newer; validRepair.back() = 'C';
            AKK_CLUSTER_CHECK(n1->runtime->repairStripeMetadata(bytesOf(key), readU64Le(newer, 6), validRepair));
            AKK_CLUSTER_CHECK(waitUntil([&] { return allMetadataEquals(validRepair); }));
        } catch (...) { failure = std::current_exception(); }
    }
    n3->runtime->close(); n2->runtime->close(); n1->runtime->close();
    if (failure) { std::rethrow_exception(failure); }
}

void runClusterRecoveryFencingTests() {
    for (bool secure : {false, true}) { for (bool history : {false, true}) { testNonRaftAheadReplicaIsQuarantined(secure, history); } }
    testNonRaftQuarantineSurvivesReopen();
    testNonRaftFrontierRejectsLostTailBeforeReconnect();
    testStripeRepairRejectsChangedValidationTerm();
}

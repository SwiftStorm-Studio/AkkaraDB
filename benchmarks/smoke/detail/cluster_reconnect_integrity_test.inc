/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
void testReconnectDurableAcknowledgement(bool secure, bool missingCallback = false) {
    const auto dir = makeTempDir(secure ? "reconnect-durable-secure" : missingCallback ? "reconnect-durable-missing" : "reconnect-durable-plain");
    ClusterRuntimeOptions options;
    options.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
    options.clusterGroupId = 991087;
    options.clusterMembershipPath = dir / "membership";
    if (secure) {
        for (uint64_t id : {1, 2}) {
            const auto identity = akkaradb::crypto::IdentityStore{dir / std::to_string(id)}.loadOrCreate();
            options.secure.pinnedPeers.push_back({id, identity.publicKey});
        }
        options.secure.identitySeedPath = dir / "1";
    }
    const AckPolicy ack{.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::DURABLE};
    const ConsistencyOptions consistency{.mode = ConsistencyMode::PRIMARY_ACK,
        .writeConsistency = WriteConsistency::ALL_CONFIGURED,
        .ackTimeoutAction = AckTimeoutAction::FAIL_WRITE, .ackTimeoutMs = 1000};
    std::atomic<uint64_t> primary{0}, applied{0}, durable{0}, applyCalls{0}, syncCalls{0};
    std::atomic<bool> failSync{true};
    auto server = ReplicationServer::create(38781, 1, [&] { return primary.load(); }, ack, consistency, 1, {2}, options);
    auto clientOptions = options;
    clientOptions.clusterMembershipPath = dir / "replica-membership";
    clientOptions.secure.identitySeedPath = dir / "2";
    clientOptions.secure.expectedPrimaryNodeId = 1;
    auto client = ReplicationClient::create("127.0.0.1", 38781, 2, [&] { return applied.load(); }, ack, clientOptions);
    client->setApplyCallback([&](uint64_t seq, auto, auto, auto, auto, auto) { applied.store(seq); ++applyCalls; });
    const auto sync = [&] {
        ++syncCalls;
        if (failSync.load()) { throw std::runtime_error("injected durability failure"); }
        durable.store(applied.load());
    };
    if (!missingCallback) { client->setForceDurableCallback(sync); }
    server->start(); client->start();
    AKK_CLUSTER_CHECK(waitUntil([&] { return server->replicaCount() == 1; }));
    primary.store(1);
    bool rejected = false;
    try { server->shipEntry(1, ReplOpType::PUT, bytesOf("key"), bytesOf("value"), 0, 1); }
    catch (const std::exception&) { rejected = true; }
    AKK_CLUSTER_CHECK(rejected && applied.load() == 1 && durable.load() == 0);
    if (!missingCallback) { AKK_CLUSTER_CHECK(syncCalls.load() != 0); }
    // Reconnect may acknowledge the existing application only after a successful sync.
    failSync.store(false);
    client->setForceDurableCallback(sync);
    AKK_CLUSTER_CHECK(waitUntil([&] { return durable.load() == 1 && client->connected() && server->replicaCount() == 1; }));
    server->shipEntry(1, ReplOpType::PUT, bytesOf("key"), bytesOf("value"), 0, 1);
    AKK_CLUSTER_CHECK(applyCalls.load() == 1 && durable.load() == 1);
    client->close(); server->close();
}

void testAppliedHelloDoesNotAcknowledgeEarlierDurableWrites() {
    ClusterRuntimeOptions options; options.transportMode = TransportMode::PLAIN;
    const AckPolicy ack{.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::DURABLE};
    const ConsistencyOptions consistency{.mode = ConsistencyMode::PRIMARY_ACK,
        .writeConsistency = WriteConsistency::ALL_CONFIGURED,
        .ackTimeoutAction = AckTimeoutAction::FAIL_WRITE, .ackTimeoutMs = 500};
    auto server = ReplicationServer::create(38782, 1, [] { return uint64_t{2}; }, ack, consistency, 1, {2}, options);
    auto client = ReplicationClient::create("127.0.0.1", 38782, 2, [] { return uint64_t{2}; }, ack, options);
    std::atomic<bool> syncEntered{false}, releaseSync{false};
    client->setForceDurableCallback([&] {
        syncEntered.store(true);
        while (!releaseSync.load()) { std::this_thread::sleep_for(std::chrono::milliseconds{1}); }
        throw std::runtime_error("injected reconnect sync failure");
    });
    server->start(); client->start();
    const bool connected = waitUntil([&] { return syncEntered.load() && server->replicaCount() == 1; });
    bool rejected = false;
    if (connected) {
        try { server->shipEntry(1, ReplOpType::PUT, bytesOf("earlier"), bytesOf("value"), 0, 1); }
        catch (const std::exception&) { rejected = true; }
    }
    releaseSync.store(true); client->close(); server->close();
    AKK_CLUSTER_CHECK(connected && rejected);
}

void testRollbackCatchupHistory(bool secure) {
    using namespace akkaradb::engine;
    const auto dir = makeTempDir(secure ? "rollback-catchup-secure" : "rollback-catchup-plain");
    const ClusterConfig config{{node(1, 38791, 38793), node(2, 38792, 38794)},
        ReplicationMode::MIRROR, {}, {.mode = ConsistencyMode::ASYNC}, {}, {}, 1, testClusterId(991088)};
    std::vector<ClusterPeerPublicKeyPin> pins;
    if (secure) {
        for (uint64_t id : {1, 2}) {
            const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
            pins.push_back({id, identity.publicKey});
        }
    }
    std::array<AkkEngineOptions, 2> options;
    std::array<std::unique_ptr<AkkEngine>, 2> engines;
    for (size_t i = 0; i < engines.size(); ++i) {
        auto& opt = options[i]; opt.paths.dataDir = dir / std::to_string(i + 1);
        opt.components.clusterEnabled = opt.components.versionLogEnabled = true;
        opt.components.blobEnabled = opt.components.sstEnabled = opt.components.manifestEnabled = false;
        opt.cluster.config = config;
        opt.cluster.runtime.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
        opt.cluster.runtime.secure.pinnedPeers = pins;
        opt.cluster.runtime.secure.identitySeedPath = dir / ("identity-" + std::to_string(i + 1));
        opt.cluster.runtime.clusterGroupId = 991088;
        opt.cluster.runtime.startupRole = i ? NodeStartupRole::REPLICA : NodeStartupRole::PRIMARY;
        opt.cluster.runtime.readMode = ClusterReadMode::LOCAL_STALE_OK;
        writeNodeIdFile(opt.paths.dataDir / "node.id", i + 1);
        engines[i] = AkkEngine::open(opt);
    }
    engines[0]->put(bytesOf("restore"), bytesOf("old"));
    const auto cut = engines[0]->stats().currentSeq;
    engines[0]->put(bytesOf("restore"), bytesOf("new"));
    engines[0]->put(bytesOf("delete"), bytesOf("temporary"));
    AKK_CLUSTER_CHECK(waitUntil([&] {
        return engineGet(*engines[1], "restore") == std::optional{std::vector<uint8_t>{'n','e','w'}} && engineGet(*engines[1], "delete").has_value();
    }));
    engines[1]->close(); engines[1].reset();
    (void)engines[0]->rollbackKey(bytesOf("restore"), Revision{0, cut});
    (void)engines[0]->rollbackKey(bytesOf("delete"), Revision{0, cut});
    // The source must reconstruct both rollback kinds from a reopened WAL.
    engines[0]->close(); engines[0] = AkkEngine::open(options[0]);
    engines[1] = AkkEngine::open(options[1]);
    const auto caughtUp = [&] {
        return engineGet(*engines[1], "restore") == std::optional{std::vector<uint8_t>{'o','l','d'}} && !engineGet(*engines[1], "delete");
    };
    AKK_CLUSTER_CHECK(waitUntil(caughtUp));
    const auto checkHistory = [&] {
        for (const std::string key : {"restore", "delete"}) {
            const auto primaryHistory = akk_test::collectHistory(engines[0]->history(bytesOf(key)));
            const auto replicaHistory = akk_test::collectHistory(engines[1]->history(bytesOf(key)));
            AKK_CLUSTER_CHECK(primaryHistory.size() == replicaHistory.size() && !primaryHistory.empty());
            for (size_t i = 0; i < primaryHistory.size(); ++i) {
                const auto& expected = primaryHistory[i]; const auto& actual = replicaHistory[i];
                AKK_CLUSTER_CHECK(actual.seq == expected.seq && actual.sourceNodeId == expected.sourceNodeId &&
                    actual.flags == expected.flags && actual.value == expected.value);
            }
            AKK_CLUSTER_CHECK((replicaHistory.back().flags & akkaradb::engine::vlog::VLOG_FLAG_ROLLBACK) != 0);
            AKK_CLUSTER_CHECK(replicaHistory.back().sourceNodeId == akkaradb::engine::vlog::ROLLBACK_NODE);
        }
    };
    checkHistory();
    engines[1]->close(); engines[1] = AkkEngine::open(options[1]);
    AKK_CLUSTER_CHECK(caughtUp()); checkHistory();
    for (auto& engine : engines) { engine->close(); }
}

void runClusterReconnectIntegrityTests() {
    testReconnectDurableAcknowledgement(false);
    testReconnectDurableAcknowledgement(true);
    testReconnectDurableAcknowledgement(false, true);
    testAppliedHelloDoesNotAcknowledgeEarlierDurableWrites();
    testRollbackCatchupHistory(false);
    testRollbackCatchupHistory(true);
}

/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */

void testTransactionStorageLimits(bool partitioned, bool secure, bool blobs) {
    using namespace akkaradb::engine;
    std::fprintf(stderr, "[storage] transaction mode partitioned=%d secure=%d blobs=%d\n", partitioned, secure, blobs);
    ClusterTransactionFixture fixture{makeTempDir("transaction-storage-limits"), partitioned, secure, 300000, false, 0,
        [blobs](size_t, AkkEngineOptions& opt) {
            opt.components.blobEnabled = blobs;
            opt.cluster.runtime.raftBlobPolicy = blobs ? RaftBlobPolicy::RAFT_LOG : RaftBlobPolicy::REJECT;
        }};
    auto tx = fixture.begin();
    const auto id = *tx.clusterId();
    const std::vector<uint8_t> oversized(size_t{UINT16_MAX} + 1, 0x72), fullKey(UINT16_MAX, 0x6b);
    const auto rejects = [&](auto operation) {
        bool rejected = false;
        try { operation(); } catch (const std::length_error&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected && tx.active());
        AKK_CLUSTER_CHECK(fixture.engines[0]->transactionOutcome(id).status == ClusterTransactionStatus::OPEN);
    };
    rejects([&] { tx.put(oversized, {}); });
    rejects([&] { tx.remove(oversized); });
    rejects([&] { tx.put(fullKey, bytesOf("x")); });
    rejects([&] { tx.put(fullKey, oversized); });
    if (!blobs) { rejects([&] { tx.put(bytesOf("too-large"), oversized); }); }
    const std::vector<uint8_t> boundaryKey(blobs ? UINT16_MAX - akkaradb::engine::blob::BLOB_REF_SIZE : 2, 0x62);
    const std::vector<uint8_t> boundaryValue(blobs ? 65536 : UINT16_MAX - boundaryKey.size(), 0x76);
    tx.put(boundaryKey, boundaryValue);
    tx.put(fullKey, {});
    tx.put(bytesOf("after-rejection"), bytesOf("committed"));
    tx.end();
    AKK_CLUSTER_CHECK(fixture.engines[0]->transactionOutcome(id).status == ClusterTransactionStatus::COMMITTED);
    fixture.close(); fixture.open();
    auto proof = fixture.begin();
    AKK_CLUSTER_CHECK(proof.get(boundaryKey) == std::optional{boundaryValue});
    AKK_CLUSTER_CHECK(proof.get(fullKey) == std::optional{std::vector<uint8_t>{}});
    AKK_CLUSTER_CHECK(textOf(*proof.get(bytesOf("after-rejection"))) == "committed");
    proof.remove(boundaryKey); proof.end();
    fixture.close(); fixture.open();
    auto removed = fixture.begin(); AKK_CLUSTER_CHECK(!removed.get(boundaryKey)); removed.rollback();
}

void testTransactionSealStorageLimits() {
    using namespace akkaradb::engine::cluster::detail;
    TransactionScalingHookScope factory;
    ClusterTransactionFixture fixture{makeTempDir("transaction-seal-storage-limits"), false, false, 300000, false, 0,
        [](size_t, akkaradb::engine::AkkEngineOptions& opt) {
            opt.components.blobEnabled = false; opt.cluster.runtime.raftBlobPolicy = RaftBlobPolicy::REJECT;
        }};
    for (bool combined : {false, true}) {
        auto tx = fixture.begin(); const auto id = *tx.clusterId();
        const std::vector<uint8_t> key(combined ? 32768 : 1, 0x6b), value(combined ? 32768 : 65536, 0x76);
        (void)tx.get(key); // Reserve the key through the public transaction API.
        const auto leader = fixture.engines[0]->stats().cluster.leaderNodeId;
        IClusterRuntime *runtime, *targetRuntime;
        {
            std::lock_guard lock{factory.hooks->mutex};
            targetRuntime = factory.hooks->runtimes.at(leader);
            runtime = factory.hooks->runtimes.at(leader == 1 ? 2 : 1);
        }
        const auto send = [&](TransactionMessage message) {
            message.id = id;
            ForwardRequest request; request.operation = ForwardOperation::TRANSACTION; request.timeoutMs = 5000;
            request.entries.push_back({{}, encodeTransactionMessage(message)});
            auto response = runtime->forwardTo(leader, std::move(request));
            if (!response.success) { throw std::runtime_error(response.message); }
            return response;
        };
        (void)send({.action = TransactionAction::START_PREPARE});
        const auto prepared = encodeTransactionPrepare({.writes = {{key, value}}});
        std::vector<uint8_t> timestamp; transactionInteger(timestamp, 1);
        (void)send({.action = TransactionAction::PREPARE, .argument = prepared.size(), .payload = timestamp});
        (void)send({.action = TransactionAction::APPEND, .payload = prepared});
        const auto before = targetRuntime->raftStats().lastLogIndex;
        bool rejected = false;
        try { (void)send({.action = TransactionAction::SEAL}); }
        catch (const std::exception& error) { rejected = std::string_view{error.what()}.find("65535-byte storage limit") != std::string_view::npos; }
        AKK_CLUSTER_CHECK(rejected && targetRuntime->raftStats().lastLogIndex == before);
        const auto status = send({.action = TransactionAction::PARTICIPANT_STATUS});
        AKK_CLUSTER_CHECK(status.value.size() == 1 && (status.value[0] & 2) == 0);
        bool commitRejected = false;
        try { (void)send({.action = TransactionAction::DECIDE_COMMIT}); }
        catch (const std::exception&) { commitRejected = true; }
        AKK_CLUSTER_CHECK(commitRejected && fixture.engines[0]->transactionOutcome(id).status == ClusterTransactionStatus::PREPARING);
        tx.rollback();
        AKK_CLUSTER_CHECK(fixture.engines[0]->transactionOutcome(id).status == ClusterTransactionStatus::ABORTED);
    }
    auto healthy = fixture.begin(); healthy.put(bytesOf("after-bad-seal"), bytesOf("healthy")); healthy.end();
    fixture.close(); fixture.open();
    auto proof = fixture.begin(); AKK_CLUSTER_CHECK(textOf(*proof.get(bytesOf("after-bad-seal"))) == "healthy"); proof.rollback();
}

using StripeStorageApply = decltype(ClusterEngineCallbacks{}.apply);
std::shared_ptr<std::map<uint64_t, StripeStorageApply>> stripeStorageReceivers;

void testStripeStorageLimits(uint8_t dataShards, uint8_t parityShards, uint8_t copies, bool secure) {
    using namespace akkaradb::engine;
    std::fprintf(stderr, "[storage] stripe data=%u parity=%u copies=%u secure=%d\n", dataShards, parityShards, copies, secure);
    const auto dir = makeTempDir("stripe-storage-limits");
    auto receivers = std::make_shared<std::map<uint64_t, StripeStorageApply>>();
    stripeStorageReceivers = receivers;
    AKK_CLUSTER_CHECK(registerClusterRuntimeFactory([](std::filesystem::path dir, ClusterConfig config, uint64_t id,
        ClusterEngineCallbacks callbacks, ClusterRuntimeOptions options) -> std::unique_ptr<IClusterRuntime> {
        stripeStorageReceivers->insert_or_assign(id, callbacks.apply);
        return ClusterRuntime::create(std::move(dir), std::move(config), id, std::move(callbacks), std::move(options));
    }));
    auto restore = std::shared_ptr<void>{receivers.get(), [](void*) {
        stripeStorageReceivers.reset();
        (void)registerClusterRuntimeFactory([](std::filesystem::path dir, ClusterConfig config, uint64_t id,
            ClusterEngineCallbacks callbacks, ClusterRuntimeOptions options) -> std::unique_ptr<IClusterRuntime> {
            return ClusterRuntime::create(std::move(dir), std::move(config), id, std::move(callbacks), std::move(options));
        });
    }};
    const size_t count = std::max<size_t>(3, (dataShards + parityShards) * copies);
    std::vector<NodeInfo> nodes;
    std::vector<ClusterPeerPublicKeyPin> pins;
    for (uint64_t id = 1; id <= count; ++id) {
        nodes.push_back(node(id, static_cast<uint16_t>(36500 + id), static_cast<uint16_t>(36510 + id)));
        if (secure) {
            const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
            pins.push_back({id, identity.publicKey});
        }
    }
    const ClusterConfig config{nodes, ReplicationMode::STRIPE, {}, {.ackTimeoutMs = 1000}, {},
        {.dataShards = dataShards, .parityShards = parityShards, .copiesPerShard = copies}, 0, testClusterId(36500)};
    std::vector<AkkEngineOptions> options(count);
    std::vector<std::unique_ptr<AkkEngine>> engines(count);
    const auto open = [&] {
        for (size_t i = 0; i < count; ++i) { engines[i] = AkkEngine::open(options[i]); }
    };
    const auto close = [&] {
        for (auto& engine : engines) { engine->close(); engine.reset(); }
    };
    for (size_t i = 0; i < count; ++i) {
        auto& opt = options[i]; opt.paths.dataDir = dir / std::to_string(i + 1);
        opt.components.clusterEnabled = true;
        opt.components.blobEnabled = opt.components.sstEnabled = opt.components.manifestEnabled = false;
        opt.cluster.config = config;
        auto& rt = opt.cluster.runtime;
        rt.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
        rt.clusterGroupId = 36500; rt.routingMode = ClusterRoutingMode::FORWARD;
        rt.readMode = ClusterReadMode::OWNER_LINEARIZABLE; rt.stripeAutoRebuild = false; rt.raftHeartbeatIntervalMs = 30;
        rt.secure.pinnedPeers = pins; rt.secure.identitySeedPath = dir / ("identity-" + std::to_string(i + 1));
        writeNodeIdFile(opt.paths.dataDir / "node.id", i + 1);
    }
    open();
    const auto key = bytesOf("stripe-boundary");
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { engines[0]->put(key, bytesOf("ready")); return true; } catch (...) { return false; }
    }, std::chrono::seconds{20}));
    const std::vector<uint8_t> boundary((UINT16_MAX - 20 - 35 - key.size()) * dataShards, 0x71);
    engines[0]->put(key, boundary);
    // Successful publication returns before follower metadata application and
    // background retirement of its durable intent have necessarily finished.
    AKK_CLUSTER_CHECK(waitUntil([&] {
        uint64_t cut = 0;
        for (const auto& engine : engines) { cut = std::max(cut, engine->stats().cluster.stripeMetadataCommitIndex); }
        for (const auto& engine : engines) {
            if (engine->stats().cluster.stripeMetadataAppliedIndex < cut || stripeInternalCounts(*engine).second != 0) { return false; }
        }
        return true;
    }, std::chrono::seconds{20}));
    const auto reject = [&](auto operation) {
        std::vector<uint64_t> before;
        for (const auto& engine : engines) { before.push_back(engine->stats().currentSeq); }
        bool rejected = false;
        try { operation(); }
        catch (const std::exception& error) { rejected = std::string_view{error.what()}.find("65535-byte storage limit") != std::string_view::npos; }
        AKK_CLUSTER_CHECK(rejected);
        for (size_t i = 0; i < count; ++i) { AKK_CLUSTER_CHECK(engines[i]->stats().currentSeq == before[i]); }
    };
    const std::vector<uint8_t> tooLarge(boundary.size() + 1, 0x78), fullKey(UINT16_MAX, 0x6b);
    reject([&] { engines[0]->put(key, tooLarge); });
    reject([&] { engines[0]->put(fullKey, {}); });
    reject([&] { engines[0]->remove(fullKey); });
    for (const auto sizes : {std::pair<size_t, size_t>{65536, 0}, {1, 65536}, {32768, 32768}}) {
        const std::vector<uint8_t> recordKey(sizes.first, 0x6b), recordValue(sizes.second, 0x76);
        reject([&] { receivers->at(1)(1, ReplOpType::PUT, recordKey, recordValue, 0, 1, 1); });
    }
    // Metadata has 78 fixed bytes and nine bytes per physical placement.
    const size_t metadataSize = 78 + 9 * (dataShards + parityShards) * copies;
    const std::vector<uint8_t> intentOverflow(UINT16_MAX - 18 - 13 - metadataSize + 1, 0x69);
    reject([&] { engines[0]->put(intentOverflow, {}); });
    for (auto& engine : engines) { AKK_CLUSTER_CHECK(engine->get(key) == std::optional{boundary}); }
    const std::vector<uint8_t> intentBoundary(UINT16_MAX - 18 - 13 - 2 * metadataSize, 0x62);
    engines[0]->put(intentBoundary, {}); engines[0]->put(intentBoundary, {});
    close(); open();
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { return engines[0]->get(key) == std::optional{boundary}; } catch (...) { return false; }
    }, std::chrono::seconds{20}));
    for (auto& engine : engines) {
        AKK_CLUSTER_CHECK(engine->get(key) == std::optional{boundary});
        AKK_CLUSTER_CHECK(engine->get(intentBoundary) == std::optional{std::vector<uint8_t>{}});
        AKK_CLUSTER_CHECK(!engine->get(intentOverflow));
    }
    engines[0]->remove(key); engines[0]->put(bytesOf("after-rejection"), bytesOf("healthy"));
    close(); open();
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { return !engines[0]->get(key) && textOf(*engines[0]->get(bytesOf("after-rejection"))) == "healthy"; } catch (...) { return false; }
    }, std::chrono::seconds{20}));
    close();
}

void runTransactionStorageTests() {
    std::fprintf(stderr, "[storage] 2PC stage: MIRROR, PARTITIONED/SECURE and Blob boundaries\n");
    testTransactionStorageLimits(false, false, false);
    testTransactionStorageLimits(true, true, false);
    testTransactionStorageLimits(false, false, true);
    std::fprintf(stderr, "[storage] participant SEAL rejects invalid inline prepared records\n");
    testTransactionSealStorageLimits();
}

void runStripeStorageTests() {
    std::fprintf(stderr, "[storage] STRIPE shard/intent bounds, rejected writes and reopen\n");
    testStripeStorageLimits(1, 1, 1, false);
    testStripeStorageLimits(2, 1, 1, true);
    testStripeStorageLimits(2, 0, 2, false);
}

/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
struct GlobalQueryInterception {
    std::mutex mutex;
    std::function<void()> beforeValidation;
    std::map<uint64_t, ClusterEngineCallbacks> callbacks;
    std::atomic<unsigned> captures{0};
    std::atomic<bool> failTransactionRelease{false};
};
std::shared_ptr<GlobalQueryInterception> globalQueryInterception;
struct GlobalQueryFactoryScope {
    std::shared_ptr<GlobalQueryInterception> hook = std::make_shared<GlobalQueryInterception>();
    GlobalQueryFactoryScope() {
        globalQueryInterception = hook;
        AKK_CLUSTER_CHECK(registerClusterRuntimeFactory([](std::filesystem::path dir, ClusterConfig config,
            uint64_t id, ClusterEngineCallbacks callbacks, ClusterRuntimeOptions options) -> std::unique_ptr<IClusterRuntime> {
            if (config.mode() == ReplicationMode::MIRROR) {
                std::lock_guard lock{globalQueryInterception->mutex};
                globalQueryInterception->callbacks.emplace(id, callbacks);
            }
            if (callbacks.forward) {
                callbacks.forward = [hook = globalQueryInterception, forward = std::move(callbacks.forward)](const ForwardRequest& request) {
                    if (hook->failTransactionRelease.load() && request.operation == ForwardOperation::TRANSACTION && request.entries.size() == 1 &&
                        akkaradb::engine::cluster::detail::decodeTransactionMessage(request.entries[0].value).action ==
                            akkaradb::engine::cluster::detail::TransactionAction::RELEASE) {
                        throw ClusterRoutingError(ClusterRoutingCode::TRANSACTION_CONFLICT, {}, "injected query reservation release failure");
                    }
                    if (request.operation == ForwardOperation::READ_QUERY && request.entries.size() == 1 && !request.entries[0].value.empty()) {
                        // Wire actions SNAPSHOT_SCAN/COUNT and VALIDATE. Injection
                        // runs only after the coordinator has captured all sources.
                        const auto action = request.entries[0].value[0];
                        if (action == 8 || action == 9) { hook->captures.fetch_add(1); }
                        if (action == 10) {
                            std::lock_guard lock{hook->mutex};
                            if (hook->beforeValidation) { hook->beforeValidation(); }
                        }
                    }
                    return forward(request);
                };
            }
            return ClusterRuntime::create(std::move(dir), std::move(config), id, std::move(callbacks), std::move(options));
        }));
    }
    ~GlobalQueryFactoryScope() {
        globalQueryInterception.reset();
        (void)registerClusterRuntimeFactory([](std::filesystem::path dir, ClusterConfig config,
            uint64_t id, ClusterEngineCallbacks callbacks, ClusterRuntimeOptions options) -> std::unique_ptr<IClusterRuntime> {
            return ClusterRuntime::create(std::move(dir), std::move(config), id, std::move(callbacks), std::move(options));
        });
    }
};

void testGlobalQueryRetries(bool streaming, bool secure) {
    using Engine = akkaradb::engine::AkkEngine;
    using Options = akkaradb::engine::AkkEngineOptions;
    using namespace std::chrono_literals;
    std::fprintf(stderr, "[global queries] retries, pins and deadlines (%d/%d)\n", streaming, secure);
    GlobalQueryFactoryScope factory;
    const auto dir = makeTempDir("global-query-retries");
    const ClusterConfig config{{node(1, 28901, 28911), node(2, 28902, 28912), node(3, 28903, 28913)},
        ReplicationMode::PARTITIONED, {}, {.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 1000}, {}, {}, 0,
        testClusterId(9592), {.replicationFactor = 1}};
    std::vector<ClusterPeerPublicKeyPin> pins;
    if (secure) {
        for (uint64_t id = 1; id <= 3; ++id) {
            const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
            pins.push_back({id, identity.publicKey});
        }
    }
    std::array<std::unique_ptr<Engine>, 3> engines;
    for (size_t i = 0; i < engines.size(); ++i) {
        Options options; options.paths.dataDir = dir / std::to_string(i + 1);
        options.components.clusterEnabled = true; options.components.blobEnabled = false;
        options.components.sstEnabled = options.components.manifestEnabled = false;
        options.cluster.config = config;
        auto& runtime = options.cluster.runtime;
        runtime.clusterGroupId = 9592; runtime.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
        runtime.secure.pinnedPeers = pins; runtime.secure.identitySeedPath = dir / ("identity-" + std::to_string(i + 1));
        runtime.queryConsistencyMode = QueryConsistencyMode::GLOBAL_SNAPSHOT;
        runtime.queryResultMode = streaming ? QueryResultMode::STREAMING : QueryResultMode::PREPARED;
        runtime.queryMaxPinnedSnapshots = 1; runtime.queryMaxOpenCursors = 2;
        runtime.queryCursorIdleTimeoutMs = 500; runtime.queryCursorMaxLifetimeMs = 2000;
        runtime.forwardingTimeoutMs = 1000;
        // The range policy must select authorities even with this point-read default.
        runtime.readMode = ClusterReadMode::LOCAL_STALE_OK;
        writeNodeIdFile(options.paths.dataDir / "node.id", i + 1);
        engines[i] = Engine::open(options);
    }
    auto& reader = *engines[2];
    AKK_CLUSTER_CHECK(waitUntil([&] { try { return reader.count() == 0; } catch (...) { return false; } }, 10s));
    const auto first = keyOwnedBy(config, 1, "global-a-");
    const auto second = keyOwnedBy(config, 2, "global-b-");
    const auto extra = keyOwnedBy(config, 1, "global-extra-");
    engines[0]->put(bytesOf(first), bytesOf("old")); engines[1]->put(bytesOf(second), bytesOf("old"));
    unsigned injections = 0;
    factory.hook->captures.store(0);
    {
        std::lock_guard lock{factory.hook->mutex};
        factory.hook->beforeValidation = [&] {
            if (injections++ == 0) {
                engines[0]->put(bytesOf(first), bytesOf("new"));
                engines[1]->put(bytesOf(second), bytesOf("new"));
            }
        };
    }
    {
        akkaradb::core::BufferArena arena;
        auto snapshot = reader.scan(arena);
        AKK_CLUSTER_CHECK(factory.hook->captures.load() >= 4);
        { std::lock_guard lock{factory.hook->mutex}; factory.hook->beforeValidation = {}; }
        engines[0]->put(bytesOf(first), bytesOf("later")); engines[1]->remove(bytesOf(second));
        std::map<std::string, std::string> actual;
        for (const auto& row : snapshot) { actual.emplace(textOf(row.key), textOf(row.value)); }
        AKK_CLUSTER_CHECK((actual == std::map<std::string, std::string>{{first, "new"}, {second, "new"}}));
    }
    AKK_CLUSTER_CHECK(reader.count() == 1);
    injections = 0; factory.hook->captures.store(0);
    {
        std::lock_guard lock{factory.hook->mutex};
        factory.hook->beforeValidation = [&] { if (injections++ == 0) { engines[0]->put(bytesOf(extra), bytesOf("added")); } };
    }
    AKK_CLUSTER_CHECK(reader.count() == 2 && factory.hook->captures.load() >= 4);
    {
        std::lock_guard lock{factory.hook->mutex};
        factory.hook->beforeValidation = [&] { engines[0]->put(bytesOf(first), bytesOf("churn")); };
    }
    const auto started = std::chrono::steady_clock::now();
    bool failed = false;
    try { (void)reader.count(); } catch (const std::runtime_error&) { failed = true; }
    const auto elapsed = std::chrono::steady_clock::now() - started;
    AKK_CLUSTER_CHECK(failed && elapsed >= 800ms && elapsed < 3s);
    { std::lock_guard lock{factory.hook->mutex}; factory.hook->beforeValidation = {}; }
    // Lost/expired CLOSE replies use the existing bounded server TTL. A tight
    // pin quota must become usable again, including after deadline failure.
    AKK_CLUSTER_CHECK(waitUntil([&] { try { return reader.count() == 2; } catch (...) { return false; } }, 5s));
    for (auto& engine : engines) { engine->close(); }
}

void testGlobalQueryValidation() {
    using Options = akkaradb::engine::AkkEngineOptions;
    const ClusterConfig config{{node(1, 28921, 28931)}, ReplicationMode::MIRROR, {}};
    ClusterRuntimeOptions runtime;
    AKK_CLUSTER_CHECK(runtime.queryConsistencyMode == QueryConsistencyMode::PER_PARTITION);
    runtime.globalSnapshotConflictPolicy = static_cast<GlobalSnapshotConflictPolicy>(255);
    bool policyRejected = false;
    try { config.validateRuntime(1, runtime); } catch (const std::invalid_argument&) { policyRejected = true; }
    AKK_CLUSTER_CHECK(policyRejected);
    runtime.globalSnapshotConflictPolicy = GlobalSnapshotConflictPolicy::BLOCK_WRITES;
    policyRejected = false;
    try { config.validateRuntime(1, runtime); } catch (const std::invalid_argument&) { policyRejected = true; }
    AKK_CLUSTER_CHECK(policyRejected);
    runtime.globalSnapshotConflictPolicy = GlobalSnapshotConflictPolicy::RETRY;
    runtime.queryConsistencyMode = static_cast<QueryConsistencyMode>(255);
    bool rejected = false;
    try { config.validateRuntime(1, runtime); } catch (const std::invalid_argument&) { rejected = true; }
    AKK_CLUSTER_CHECK(rejected);
    Options options; options.paths.dataDir = makeTempDir("global-query-invalid");
    options.components.clusterEnabled = true; options.components.blobEnabled = false;
    options.cluster.config = config; options.cluster.runtime.transportMode = TransportMode::PLAIN;
    options.cluster.runtime.queryConsistencyMode = QueryConsistencyMode::GLOBAL_SNAPSHOT;
    options.runtime.writeVisibility = Options::WriteVisibilityMode::APPLIED;
    writeNodeIdFile(options.paths.dataDir / "node.id", 1);
    rejected = false;
    try { (void)akkaradb::engine::AkkEngine::open(options); } catch (const std::invalid_argument&) { rejected = true; }
    AKK_CLUSTER_CHECK(rejected);
}

void testGlobalQueryTransactionVisibility() {
    using namespace std::chrono_literals;
    std::fprintf(stderr, "[global queries] partial 2PC application stays hidden\n");
    ClusterTransactionFactoryScope factory;
    ClusterTransactionFixture fixture{makeTempDir("global-query-2pc"), true, false, 300000, false, 0,
        [](size_t, akkaradb::engine::AkkEngineOptions& options) {
            options.cluster.runtime.queryConsistencyMode = QueryConsistencyMode::GLOBAL_SNAPSHOT;
            options.cluster.runtime.queryResultMode = QueryResultMode::STREAMING;
        }};
    auto& engine = *fixture.engines[0];
    auto [first, second] = clusterTransactionKeys(fixture.config, "global-2pc-");
    if (engine.revisionStreamId(bytesOf(first)) > engine.revisionStreamId(bytesOf(second))) { std::swap(first, second); }
    auto tx = fixture.begin(); tx.put(bytesOf(first), bytesOf("new")); tx.put(bytesOf(second), bytesOf("new"));
    clusterTransactionApplyGate->enable(static_cast<uint16_t>(engine.revisionStreamId(bytesOf(second)) - 1));
    auto commit = std::async(std::launch::async, [tx = std::move(tx)]() mutable { tx.end(); });
    const bool paused = clusterTransactionApplyGate->await();
    bool rejected = false;
    if (paused) {
        try { akkaradb::core::BufferArena arena; (void)engine.scan(arena); }
        catch (const std::exception&) { rejected = true; }
    }
    clusterTransactionApplyGate->release(); commit.get();
    AKK_CLUSTER_CHECK(paused && rejected);
    AKK_CLUSTER_CHECK(waitUntil([&] { try { return engine.count() == 2; } catch (...) { return false; } }, 10s));
    akkaradb::core::BufferArena arena;
    size_t rows = 0;
    for (const auto& row : engine.scan(arena)) { AKK_CLUSTER_CHECK(textOf(row.value) == "new"); ++rows; }
    AKK_CLUSTER_CHECK(rows == 2);
}

void testGlobalQuerySnapshotReplacement() {
    using Engine = akkaradb::engine::AkkEngine;
    using namespace std::chrono_literals;
    std::fprintf(stderr, "[global queries] same-sequence snapshot replacement\n");
    GlobalQueryFactoryScope factory;
    const auto dir = makeTempDir("global-query-replacement");
    const ClusterConfig config{{node(1, 28941, 28951), node(2, 28942, 28952)},
        ReplicationMode::MIRROR, {}, {.mode = ConsistencyMode::PRIMARY_ACK}, {}, {}, 1, testClusterId(9593)};
    std::array<std::unique_ptr<Engine>, 2> engines;
    for (size_t i = 0; i < engines.size(); ++i) {
        akkaradb::engine::AkkEngineOptions options; options.paths.dataDir = dir / std::to_string(i + 1);
        options.components.clusterEnabled = true; options.components.blobEnabled = false;
        options.components.sstEnabled = options.components.manifestEnabled = false;
        options.cluster.config = config;
        auto& runtime = options.cluster.runtime;
        runtime.clusterGroupId = 9593; runtime.transportMode = TransportMode::PLAIN;
        runtime.startupRole = i == 0 ? NodeStartupRole::PRIMARY : NodeStartupRole::REPLICA;
        runtime.queryConsistencyMode = QueryConsistencyMode::GLOBAL_SNAPSHOT;
        runtime.queryResultMode = QueryResultMode::STREAMING;
        writeNodeIdFile(options.paths.dataDir / "node.id", i + 1);
        engines[i] = Engine::open(options);
    }
    AKK_CLUSTER_CHECK(waitUntil([&] { try { return engines[1]->count() == 0; } catch (...) { return false; } }, 10s));
    engines[0]->put(bytesOf("key"), bytesOf("old"));
    bool replaced = false;
    factory.hook->captures.store(0);
    {
        std::lock_guard lock{factory.hook->mutex};
        factory.hook->beforeValidation = [&] {
            if (replaced) { return; }
            replaced = true;
            const auto& callbacks = factory.hook->callbacks.at(1);
            const auto seq = callbacks.getCurrentSeq();
            // A retransmitted prefix preserves the same sequence/content.
            // Installation must still invalidate the earlier capture stamp.
            const auto value = bytesOf("old");
            callbacks.beginSnapshot(seq, 1);
            callbacks.beginSnapshotEntry(bytesOf("key"), value.size(), akkaradb::engine::blob::crc32c(value));
            callbacks.appendSnapshotEntryChunk(0, value); callbacks.finishSnapshotEntry();
            callbacks.prepareSnapshot(seq, 1); callbacks.finishSnapshot(seq, 1);
            AKK_CLUSTER_CHECK(callbacks.getCurrentSeq() == seq);
        };
    }
    akkaradb::core::BufferArena arena;
    auto snapshot = engines[1]->scan(arena);
    AKK_CLUSTER_CHECK(replaced && factory.hook->captures.load() >= 2);
    { std::lock_guard lock{factory.hook->mutex}; factory.hook->beforeValidation = {}; }
    size_t rows = 0;
    for (const auto& row : snapshot) { AKK_CLUSTER_CHECK(textOf(row.value) == "old"); ++rows; }
    AKK_CLUSTER_CHECK(rows == 1);
    snapshot = {};
    for (auto& engine : engines) { engine->close(); }
}

void testGlobalQuerySingleSourceWrites() {
    using Engine = akkaradb::engine::AkkEngine;
    using namespace std::chrono_literals;
    GlobalQueryFactoryScope factory;
    auto dir = makeTempDir("global-query-single-source-writes");
    const ClusterConfig config{{node(1, 28961, 28971), node(2, 28962, 28972)}, ReplicationMode::MIRROR, {},
        {.mode = ConsistencyMode::PRIMARY_ACK}, {}, {}, 1, testClusterId(9594)};
    std::array<std::unique_ptr<Engine>, 2> engines;
    for (size_t i = 0; i < engines.size(); ++i) {
        akkaradb::engine::AkkEngineOptions options;
        options.paths.dataDir = dir / std::to_string(i + 1); options.components.clusterEnabled = true;
        options.components.blobEnabled = options.components.sstEnabled = options.components.manifestEnabled = false;
        options.cluster.config = config; options.cluster.runtime.transportMode = TransportMode::PLAIN;
        options.cluster.runtime.startupRole = i == 0 ? NodeStartupRole::PRIMARY : NodeStartupRole::REPLICA;
        options.cluster.runtime.queryConsistencyMode = QueryConsistencyMode::GLOBAL_SNAPSHOT;
        writeNodeIdFile(options.paths.dataDir / "node.id", i + 1);
        engines[i] = Engine::open(options);
    }
    AKK_CLUSTER_CHECK(waitUntil([&] { try { return engines[1]->count() == 0; } catch (...) { return false; } }, 10s));
    auto& engine = engines[0];
    engine->put(bytesOf("key"), bytesOf("captured"));
    unsigned writes = 0;
    {
        std::lock_guard lock{factory.hook->mutex};
        factory.hook->beforeValidation = [&] { ++writes; engine->put(bytesOf("key"), bytesOf("later")); };
    }
    akkaradb::core::BufferArena arena;
    auto snapshot = engines[1]->scan(arena);
    AKK_CLUSTER_CHECK(writes == 1);
    { std::lock_guard lock{factory.hook->mutex}; factory.hook->beforeValidation = {}; }
    size_t rows = 0;
    for (const auto& row : snapshot) { AKK_CLUSTER_CHECK(textOf(row.value) == "captured"); ++rows; }
    AKK_CLUSTER_CHECK(rows == 1 && textOf(*engine->get(bytesOf("key"))) == "later");
    snapshot = {}; for (auto& member : engines) { member->close(); }
}
void testGlobalQueryWriteBarrier(bool streaming) {
    using namespace std::chrono_literals;
    GlobalQueryFactoryScope factory;
    ClusterTransactionFixture fixture{makeTempDir("global-query-write-barrier"), true, false, 300000, false, 0,
        [streaming](size_t, akkaradb::engine::AkkEngineOptions& options) {
            options.cluster.runtime.queryResultMode = streaming ? QueryResultMode::STREAMING : QueryResultMode::PREPARED;
            options.cluster.runtime.queryConsistencyMode = QueryConsistencyMode::GLOBAL_SNAPSHOT;
            options.cluster.runtime.globalSnapshotConflictPolicy = GlobalSnapshotConflictPolicy::BLOCK_WRITES;
            options.cluster.runtime.forwardingTimeoutMs = 60000;
        }};
    auto& engine = *fixture.engines[0];
    auto ready = fixture.begin(); ready.rollback();
    auto [first, second] = clusterTransactionKeys(fixture.config, "query-barrier-");
    engine.put(bytesOf(first), bytesOf("before")); engine.put(bytesOf(second), bytesOf("before"));
    AKK_CLUSTER_CHECK(engine.count(bytesOf(first), bytesOf(first)) == 0);
    {
        std::future<void> write;
        bool waited = false;
        {
            std::lock_guard lock{factory.hook->mutex};
            factory.hook->beforeValidation = [&] {
                if (write.valid()) { return; }
                engine.put(bytesOf("outside-query-range"), bytesOf("continues"));
                write = std::async(std::launch::async, [&] { engine.put(bytesOf(first), bytesOf("after")); });
                waited = write.wait_for(100ms) == std::future_status::timeout;
            };
        }
        akkaradb::core::BufferArena arena;
        auto snapshot = engine.scan(arena, bytesOf("query-barrier-"), bytesOf("query-barrier."));
        { std::lock_guard lock{factory.hook->mutex}; factory.hook->beforeValidation = {}; }
        write.get(); AKK_CLUSTER_CHECK(waited);
        size_t rows = 0;
        for (const auto& row : snapshot) { AKK_CLUSTER_CHECK(textOf(row.value) == "before"); ++rows; }
        AKK_CLUSTER_CHECK(rows == 2 && textOf(*engine.get(bytesOf(first))) == "after");
        snapshot = {}; engine.put(bytesOf(first), bytesOf("before"));
    }
    {
        std::lock_guard lock{factory.hook->mutex};
        factory.hook->beforeValidation = [] { throw std::runtime_error("injected global query validation failure"); };
    }
    bool failed = false;
    try { (void)engine.count(); } catch (const std::exception&) { failed = true; }
    { std::lock_guard lock{factory.hook->mutex}; factory.hook->beforeValidation = {}; }
    AKK_CLUSTER_CHECK(failed);
    engine.put(bytesOf(first), bytesOf("released"));
    AKK_CLUSTER_CHECK(engine.count() == 3);
    if (streaming) {
        // Leave an aborted query reservation durable, then reopen before recovery
        // can release it. The waiting-writer flag must survive decoding as well.
        factory.hook->failTransactionRelease.store(true);
        {
            akkaradb::core::BufferArena arena;
            auto snapshot = engine.scan(arena);
            size_t rows = 0;
            for (const auto& row : snapshot) { (void)row; ++rows; }
            AKK_CLUSTER_CHECK(rows == 3);
        }
        fixture.close(); fixture.open();
        bool pending = false;
        for (const auto& member : fixture.engines) { pending = pending || member->stats().cluster.pendingDistributedTransactions != 0; }
        factory.hook->failTransactionRelease.store(false);
        AKK_CLUSTER_CHECK(pending);
        fixture.engines[0]->put(bytesOf(first), bytesOf("reopened"));
        fixture.engines[0]->put(bytesOf(second), bytesOf("reopened"));
        AKK_CLUSTER_CHECK(fixture.engines[0]->count() == 3);
    }
}

void runGlobalQueryConsistencyTests() {
    testGlobalQueryValidation();
    testGlobalQueryRetries(false, false); testGlobalQueryRetries(true, true);
    testGlobalQueryTransactionVisibility(); testGlobalQuerySnapshotReplacement();
}
void runGlobalQueryDeliveryTests() {
    testQueryDeliverySnapshots(false, true); testQueryDeliverySnapshots(true, true);
    testPartitionQueryDelivery(true);
}
void runGlobalQueryMirrorTests() {
    testNativeClusterQueries(ReplicationMode::MIRROR, false, false, false, true);
}
void runGlobalQueryStripeTests() {
    testNativeClusterQueries(ReplicationMode::STRIPE, false, true, true, true);
}
void runGlobalQueryTests() {
    runGlobalQueryConsistencyTests(); runGlobalQueryDeliveryTests();
    runGlobalQueryMirrorTests(); runGlobalQueryStripeTests();
}

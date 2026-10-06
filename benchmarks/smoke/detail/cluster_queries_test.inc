// Cluster public range/history regression scenarios; included by cluster_smoke_test.cpp.
void testNativeClusterQueries(ReplicationMode mode, bool raft, bool secure, bool streaming = false) {
    using Engine = akkaradb::engine::AkkEngine;
    using Options = akkaradb::engine::AkkEngineOptions;
    const auto suffix = std::to_string(static_cast<unsigned>(mode)) + (raft ? "-raft" : "") + (secure ? "-secure" : "");
    const auto dir = makeTempDir("public-queries-" + suffix);
    auto raftOptions = onlineRaftMembership(); raftOptions.membership.allowLearners = true;
    const auto cfg = ClusterConfig{{node(1, 25201, 25211), node(2, 25202, 25212),
        node(3, 25203, 25213, raft ? DATA_AND_COORDINATOR | RAFT_LEARNER : DATA_AND_COORDINATOR)}, mode, {},
        ConsistencyOptions{.mode = raft ? ConsistencyMode::RAFT_QUORUM : ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 3000},
        raft ? raftOptions : RaftOptions{}, StripeOptions{.dataShards = 2, .parityShards = 1},
        mode == ReplicationMode::MIRROR && !raft ? 1ull : 0ull, testClusterId(9520)};
    std::vector<ClusterPeerPublicKeyPin> pins;
    if (secure) {
        for (uint64_t id = 1; id <= 3; ++id) {
            const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
            pins.push_back({id, identity.publicKey});
        }
    }
    auto makeOptions = [&](uint64_t id, ClusterReadMode readMode = ClusterReadMode::OWNER_LINEARIZABLE) {
        Options options;
        options.paths.dataDir = dir / ("n" + std::to_string(id));
        options.components.clusterEnabled = true;
        options.components.versionLogEnabled = !(mode == ReplicationMode::PARTITIONED && id == 3);
        options.components.blobEnabled = raft;
        options.blob.thresholdBytes = 32;
        options.cluster.config = cfg;
        auto& runtime = options.cluster.runtime;
        runtime.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
        runtime.secure.pinnedPeers = pins;
        runtime.secure.identitySeedPath = dir / ("identity-" + std::to_string(id));
        runtime.clusterGroupId = 9520; runtime.clusterGroupEpoch = 1;
        if (mode == ReplicationMode::MIRROR && !raft) {
            runtime.startupRole = id == 1 ? NodeStartupRole::PRIMARY : NodeStartupRole::REPLICA;
        }
        runtime.raftBlobPolicy = RaftBlobPolicy::RAFT_LOG;
        runtime.forwardingTimeoutMs = 30'000;
        runtime.routingMode = ClusterRoutingMode::REDIRECT;
        runtime.readMode = readMode;
        runtime.queryResultMode = streaming ? QueryResultMode::STREAMING : QueryResultMode::PREPARED;
        runtime.stripeAutoRebuild = false;
        options.runtime.scanConsistency = Options::ScanConsistencyMode::WEAK_ORDERED;
        writeNodeIdFile(options.paths.dataDir / "node.id", id);
        return options;
    };
    auto n1 = Engine::open(makeOptions(1)), n2 = Engine::open(makeOptions(2)), n3 = Engine::open(makeOptions(3));
    Engine* nodes[]{n1.get(), n2.get(), n3.get()};
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try {
            return (!raft || findEngineLeader(nodes)) && n3->count() == 0;
        }
        catch (...) { return false; }
    }, std::chrono::seconds{20}));
    const auto writerFor = [&](const std::string& key) -> Engine& {
        if (raft) { return *findEngineLeader(nodes); }
        if (mode == ReplicationMode::MIRROR) { return *n1; }
        ClusterRouter router{cfg};
        return *nodes[router.writeTargets(bytesOf(key)).front().nodeId - 1];
    };
    const auto key1 = mode == ReplicationMode::MIRROR ? std::string{"b"} : keyOwnedBy(cfg, 1, "b");
    const auto key2 = mode == ReplicationMode::MIRROR ? std::string{"aa"} : keyOwnedBy(cfg, 2, "aa");
    const auto key3 = mode == ReplicationMode::MIRROR ? std::string{"z"} : keyOwnedBy(cfg, 1, "z");
    std::map<std::string, std::string> expected{{key1, "first"}, {key2, "second"}, {key3, ""}};
    expected.emplace(std::string{"c\0\xff", 3}, "binary-key");
    for (const auto& [key, value] : expected) { writerFor(key).put(bytesOf(key), bytesOf(value)); }
    std::fprintf(stderr, "[cluster queries] initial ranges\n");
    const auto scanRows = [&](Engine& engine, std::span<const uint8_t> start = {}, std::span<const uint8_t> end = {}) {
        akkaradb::core::BufferArena arena;
        std::map<std::string, std::string> result;
        std::string previous;
        for (const auto& record : engine.scan(arena, start, end)) {
            const auto key = textOf(record.key);
            AKK_CLUSTER_CHECK(result.empty() || previous < key);
            AKK_CLUSTER_CHECK(result.emplace(key, textOf(record.value)).second);
            previous = key;
        }
        return result;
    };
    for (auto* engine : nodes) {
        AKK_CLUSTER_CHECK(engine->count() == expected.size());
        AKK_CLUSTER_CHECK(scanRows(*engine) == expected);
        const auto first = expected.begin()->first, last = expected.rbegin()->first;
        std::map<std::string, std::string> bounded;
        for (const auto& [key, value] : expected) { if (key >= first && key < last) { bounded.emplace(key, value); } }
        AKK_CLUSTER_CHECK(engine->count(bytesOf(first), bytesOf(last)) == bounded.size());
        AKK_CLUSTER_CHECK(scanRows(*engine, bytesOf(first), bytesOf(last)) == bounded);
        AKK_CLUSTER_CHECK(engine->count(bytesOf(last), bytesOf(first)) == 0);
        AKK_CLUSTER_CHECK(scanRows(*engine, bytesOf(last), bytesOf(first)).empty());
    }
    auto& flushed = writerFor(key1);
    flushed.forceFlush();
    AKK_CLUSTER_CHECK(waitUntil([&] { return flushed.stats().sst.fileCount != 0; }));
    AKK_CLUSTER_CHECK(scanRows(*n3) == expected);
    const auto before = akk_test::collectHistory(n3->history(bytesOf(key1)));
    std::fprintf(stderr, "[cluster queries] history and fixed snapshot\n");
    AKK_CLUSTER_CHECK(before.size() == 1 && textOf(before[0].value) == "first");
    const akkaradb::engine::Revision revision{mode == ReplicationMode::PARTITIONED ? 1ull : 0ull, before[0].seq};
    AKK_CLUSTER_CHECK(textOf(*n3->getAt(bytesOf(key1), revision)) == "first");
    bool rejected = false;
    try { (void)n3->getAt(bytesOf(key1), akkaradb::engine::Revision{revision.streamId + 100, revision.seq}); }
    catch (const std::invalid_argument&) { rejected = true; }
    AKK_CLUSTER_CHECK(rejected);

    // Snapshot preparation happens at scan(), before its coroutine is first resumed.
    {
        akkaradb::core::BufferArena arena;
        auto snapshot = n3->scan(arena);
        writerFor(key1).put(bytesOf(key1), bytesOf("after-open"));
        std::map<std::string, std::string> saved;
        for (const auto& record : snapshot) { saved.emplace(textOf(record.key), textOf(record.value)); }
        AKK_CLUSTER_CHECK(saved == expected);
    }
    // Raft Blobs can exceed 4 MiB; non-Raft inline values retain their existing
    // 16-bit size limit, but a complete range may still exceed the frame cap.
    std::string large(raft ? 5 * 1024 * 1024 + 91 : 60 * 1024, 'L'); large[large.size() / 2] = '\0';
    std::fprintf(stderr, "[cluster queries] large values\n");
    writerFor(key1).put(bytesOf(key1), bytesOf(large));
    writerFor(key2).remove(bytesOf(key2));
    expected[key1] = large; expected.erase(key2);
    if (!raft && !secure) {
        for (unsigned i = 0; i < 70; ++i) {
            const auto key = "query-bulk-" + std::to_string(i);
            writerFor(key).put(bytesOf(key), bytesOf(large)); expected.emplace(key, large);
        }
    }
    AKK_CLUSTER_CHECK(n3->count() == expected.size());
    AKK_CLUSTER_CHECK(scanRows(*n3) == expected);
    const auto versions = akk_test::collectHistory(n3->history(bytesOf(key1)));
    AKK_CLUSTER_CHECK(versions.size() == 3 && textOf(versions.back().value) == large);
    AKK_CLUSTER_CHECK((versions.back().flags & akkaradb::core::MemHdr16::FLAG_BLOB) == 0);
    AKK_CLUSTER_CHECK(textOf(*n3->getAt(bytesOf(key1), versions.back().seq)) == large);
    AKK_CLUSTER_CHECK(textOf(*n3->getAt(bytesOf(key1), revision.seq)) == "first");
    AKK_CLUSTER_CHECK(!n3->getAt(bytesOf(key1), 0));
    const auto deleted = akk_test::collectHistory(n3->history(bytesOf(key2)));
    AKK_CLUSTER_CHECK(deleted.size() == 2 && (deleted.back().flags & akkaradb::core::MemHdr16::FLAG_TOMBSTONE));
    AKK_CLUSTER_CHECK(!n3->getAt(bytesOf(key2), deleted.back().seq));
    AKK_CLUSTER_CHECK(akk_test::collectHistory(n3->history(bytesOf("missing-query-key"))).empty());
    AKK_CLUSTER_CHECK(!n3->getAt(bytesOf("missing-query-key"), UINT64_MAX));
    AKK_CLUSTER_CHECK(writerFor(key1).rollbackKey(bytesOf(key1), revision).complete());
    expected[key1] = "first";
    const auto rolledBack = akk_test::collectHistory(n3->history(bytesOf(key1)));
    AKK_CLUSTER_CHECK(rolledBack.size() == 4 && textOf(rolledBack.back().value) == "first" &&
        (rolledBack.back().flags & akkaradb::engine::vlog::VLOG_FLAG_ROLLBACK) &&
        rolledBack.back().sourceNodeId == akkaradb::engine::vlog::ROLLBACK_NODE);

    // OWNER_ONLY refuses a remote authority, never silently returns its local portion.
    std::fprintf(stderr, "[cluster queries] restart and policy\n");
    n3->close(); n3 = Engine::open(makeOptions(3, ClusterReadMode::OWNER_ONLY)); nodes[2] = n3.get();
    if (mode == ReplicationMode::STRIPE) {
        AKK_CLUSTER_CHECK(waitUntil([&] { return n3->stats().cluster.stripeMetadataLeaderNodeId != 0; }));
    }
    const bool localStripeAuthority = mode == ReplicationMode::STRIPE && n3->stats().cluster.stripeMetadataLeaderNodeId == 3;
    rejected = false;
    try { (void)akk_test::collectHistory(n3->history(bytesOf(key1))); } catch (const std::runtime_error&) { rejected = true; }
    AKK_CLUSTER_CHECK(rejected != localStripeAuthority);
    n3->close(); n3 = Engine::open(makeOptions(3)); nodes[2] = n3.get();
    AKK_CLUSTER_CHECK(waitUntil([&] { try { return scanRows(*n3) == expected; } catch (...) { return false; } }, std::chrono::seconds{20}));
    // Missing owner/quorum is a failed complete query, not a successful partial count.
    if (mode == ReplicationMode::PARTITIONED) {
        n2->close();
        rejected = false;
        try { (void)n1->count(); } catch (const std::runtime_error&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected);
    }
    if (raft) {
        auto* leader = findEngineLeader(nodes);
        AKK_CLUSTER_CHECK(leader != nullptr);
        (leader == n1.get() ? n2 : n1)->close();
        rejected = false;
        try { (void)n3->count(); } catch (const std::runtime_error&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected);
        rejected = false;
        try { (void)akk_test::collectHistory(n3->history(bytesOf(key1))); } catch (const std::runtime_error&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected);
    }
    n3->close(); n2->close(); n1->close();
}

void testNativeQueryLimitsAndLifetime() {
    using Engine = akkaradb::engine::AkkEngine;
    const auto dir = makeTempDir("public-query-limits");
    const ClusterConfig cfg{{node(1, 25221, 25231)}, ReplicationMode::MIRROR, {},
        ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 300}};
    akkaradb::engine::AkkEngineOptions options;
    options.paths.dataDir = dir; options.cluster.config = cfg;
    options.components.clusterEnabled = true; options.components.blobEnabled = false;
    options.components.versionLogEnabled = true;
    options.cluster.runtime.transportMode = TransportMode::PLAIN;
    options.cluster.runtime.queryMaxSpoolBytes = 256;
    options.cluster.runtime.queryMaxOpenCursors = 1;
    options.cluster.runtime.queryCursorIdleTimeoutMs = 100;
    writeNodeIdFile(dir / "node.id", 1);
    auto engine = Engine::open(options);
    Engine* nodes[]{engine.get()};
    runOnEngineLeader(nodes, [&](Engine& leader) { leader.put(bytesOf("key"), bytesOf("value")); });
    {
        akkaradb::core::BufferArena arena;
        auto first = engine->scan(arena);
        bool rejected = false;
        try { akkaradb::core::BufferArena other; (void)engine->scan(other); } catch (const std::runtime_error&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected && engine->count() == 1);
    }
    {
        akkaradb::core::BufferArena arena;
        auto expired = engine->scan(arena);
        std::this_thread::sleep_for(std::chrono::milliseconds{150});
        bool rejected = false;
        try { (void)expired.begin(); } catch (const std::runtime_error&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected);
    }
    engine->put(bytesOf("key"), bytesOf(std::string(400, 'x')));
    bool rejected = false;
    try { akkaradb::core::BufferArena arena; (void)engine->scan(arena); } catch (const std::runtime_error&) { rejected = true; }
    AKK_CLUSTER_CHECK(rejected && engine->count() == 1);
    engine->put(bytesOf("key"), bytesOf("value"));
    {
        akkaradb::core::BufferArena arena;
        auto scan = engine->scan(arena);
        auto closing = std::async(std::launch::async, [&] { engine->close(); });
        AKK_CLUSTER_CHECK(closing.wait_for(std::chrono::milliseconds{50}) == std::future_status::timeout);
        size_t rows = 0;
        for (const auto& record : scan) { AKK_CLUSTER_CHECK(textOf(record.value) == "value"); ++rows; }
        AKK_CLUSTER_CHECK(rows == 1);
        scan = {};
        AKK_CLUSTER_CHECK(closing.wait_for(std::chrono::seconds{5}) == std::future_status::ready);
        closing.get();
    }
}

void runNativeClusterQueryTests() {
    for (bool secure : {false, true}) {
        for (auto mode : {ReplicationMode::PARTITIONED, ReplicationMode::MIRROR, ReplicationMode::STRIPE}) {
            std::fprintf(stderr, "[cluster queries] mode %u, secure %u\n", static_cast<unsigned>(mode), secure);
            testNativeClusterQueries(mode, false, secure);
        }
        std::fprintf(stderr, "[cluster queries] Raft with learner, secure %u\n", secure);
        testNativeClusterQueries(ReplicationMode::MIRROR, true, secure);
    }
    testNativeQueryLimitsAndLifetime();
}

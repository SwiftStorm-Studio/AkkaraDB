// Snapshot stability, writer progress, quota release and lazy history lifetime.
void testQueryDeliverySnapshots(bool streaming) {
    using Engine = akkaradb::engine::AkkEngine;
    using namespace std::chrono_literals;
    const auto dir = makeTempDir("query-delivery");
    const ClusterConfig config{{node(1, 28701, 28711)}, ReplicationMode::MIRROR, {},
        {.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 1000}};
    akkaradb::engine::AkkEngineOptions options;
    options.paths.dataDir = dir; options.cluster.config = config;
    options.components.clusterEnabled = options.components.versionLogEnabled = true;
    options.components.blobEnabled = options.components.sstEnabled = options.components.manifestEnabled = false;
    auto& policy = options.cluster.runtime;
    policy.transportMode = TransportMode::PLAIN;
    policy.queryResultMode = streaming ? QueryResultMode::STREAMING : QueryResultMode::PREPARED;
    policy.queryMaxSpoolBytes = streaming ? 64 : 1024 * 1024;
    policy.queryMaxPinnedSnapshots = 1;
    policy.querySnapshotAdmission = QuerySnapshotAdmission::REJECT;
    policy.queryCursorIdleTimeoutMs = 1000;
    writeNodeIdFile(dir / "node.id", 1);
    auto engine = Engine::open(options);
    Engine* nodes[]{engine.get()};
    runOnEngineLeader(nodes, [&](Engine& leader) { leader.put(bytesOf("key"), bytesOf("first")); });
    for (unsigned i = 0; i < 40; ++i) { engine->put(bytesOf("row-" + std::to_string(i)), bytesOf(std::string(4096, 'v'))); }
    {
        akkaradb::core::BufferArena arena;
        auto snapshot = engine->scan(arena);
        auto writer = std::async(std::launch::async, [&] {
            engine->put(bytesOf("key"), bytesOf("second"));
            engine->remove(bytesOf("row-0"));
        });
        AKK_CLUSTER_CHECK(writer.wait_for(3s) == std::future_status::ready); writer.get();
        if (streaming) {
            bool rejected = false;
            try { akkaradb::core::BufferArena other; (void)engine->scan(other); } catch (const std::runtime_error&) { rejected = true; }
            AKK_CLUSTER_CHECK(rejected);
        }
        size_t rows = 0;
        for (const auto& record : snapshot) {
            if (textOf(record.key) == "key") { AKK_CLUSTER_CHECK(textOf(record.value) == "first"); }
            ++rows;
        }
        AKK_CLUSTER_CHECK(rows == 41);
    }
    AKK_CLUSTER_CHECK(engine->count() == 40);
    // No owning history vector in production; a captured history excludes a
    // later revision, survives thread handoff, and releases admission on abandon.
    auto history = engine->history(bytesOf("key"));
    engine->put(bytesOf("key"), bytesOf("third"));
    auto consumed = std::async(std::launch::async, [history = std::move(history)]() mutable {
        std::vector<std::string> values;
        for (const auto& entry : history) { values.push_back(textOf(entry.value)); }
        return values;
    });
    AKK_CLUSTER_CHECK(consumed.get() == std::vector<std::string>({"first", "second"}));
    {
        auto abandoned = engine->history(bytesOf("key"));
        auto first = abandoned.begin();
        AKK_CLUSTER_CHECK(first != std::default_sentinel && textOf(first->value) == "first");
    }
    {
        auto cursor = engine->history(bytesOf("key"));
        auto closing = std::async(std::launch::async, [&] { engine->close(); });
        AKK_CLUSTER_CHECK(closing.wait_for(50ms) == std::future_status::timeout);
        size_t revisions = 0; for (const auto& entry : cursor) { (void)entry; ++revisions; }
        AKK_CLUSTER_CHECK(revisions == 3); cursor = {};
        AKK_CLUSTER_CHECK(closing.wait_for(3s) == std::future_status::ready); closing.get();
    }
}

void testPartitionQueryDelivery() {
    using Engine = akkaradb::engine::AkkEngine;
    using namespace std::chrono_literals;
    const auto dir = makeTempDir("partition-query-delivery");
    std::vector<NodeInfo> members{node(1, 28721, 28731), node(2, 28722, 28732), node(3, 28723, 28733)};
    for (size_t i = 0; i < members.size(); ++i) { members[i].partitionReplBasePort = static_cast<uint16_t>(28800 + i * 10); }
    const ClusterConfig config{members, ReplicationMode::PARTITIONED, {},
        {.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 1500}, {}, {}, 0,
        testClusterId(9591), {.replicationFactor = 3, .partitionCount = 2}};
    std::array<std::unique_ptr<Engine>, 3> engines;
    for (size_t i = 0; i < engines.size(); ++i) {
        akkaradb::engine::AkkEngineOptions options;
        options.paths.dataDir = dir / std::to_string(i + 1); options.cluster.config = config;
        options.components.clusterEnabled = options.components.versionLogEnabled = true;
        options.components.blobEnabled = options.components.sstEnabled = options.components.manifestEnabled = false;
        options.cluster.runtime.transportMode = TransportMode::PLAIN;
        options.cluster.runtime.clusterGroupId = 9591;
        options.cluster.runtime.routingMode = ClusterRoutingMode::FORWARD;
        options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
        options.cluster.runtime.queryResultMode = QueryResultMode::STREAMING;
        writeNodeIdFile(options.paths.dataDir / "node.id", i + 1);
        engines[i] = Engine::open(options);
    }
    for (unsigned i = 0; i < 16; ++i) {
        const auto key = "row-" + std::to_string(i);
        AKK_CLUSTER_CHECK(waitUntil([&] { try { engines[0]->put(bytesOf(key), bytesOf("before")); return true; } catch (...) { return false; } }, 10s));
    }
    {
        akkaradb::core::BufferArena arena;
        auto snapshot = engines[2]->scan(arena);
        engines[0]->put(bytesOf("row-0"), bytesOf("after"));
        engines[0]->put(bytesOf("new"), bytesOf("after"));
        size_t rows = 0;
        for (const auto& record : snapshot) { AKK_CLUSTER_CHECK(textOf(record.value) == "before"); ++rows; }
        AKK_CLUSTER_CHECK(rows == 16);
    }
    AKK_CLUSTER_CHECK(engines[2]->count() == 17);
    for (auto& engine : engines) { engine->close(); }
}

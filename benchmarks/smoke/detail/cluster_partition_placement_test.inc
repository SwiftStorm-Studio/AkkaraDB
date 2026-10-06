// Placement distribution and bounded PARTITIONED storage regressions.
void testClusterPlacementDistribution() {
    for (const unsigned nodeCount : {3u, 6u}) {
        std::vector<NodeInfo> members;
        for (unsigned i = 1; i <= nodeCount; ++i) { members.push_back(node(i, 25500 + i, 25510 + i)); }
        const auto identity = testClusterId(9550);
        const ClusterConfig config{members, ReplicationMode::PARTITIONED, {}, {}, {}, {}, 0, identity};
        ClusterRouter router{config};
        std::ranges::reverse(members);
        ClusterRouter reversed{ClusterConfig{members, ReplicationMode::PARTITIONED, {}, {}, {}, {}, 0, identity}};
        std::vector<unsigned> owners(nodeCount), holders(nodeCount);
        constexpr unsigned samples = 100'000;
        for (unsigned i = 0; i < samples; ++i) {
            const auto key = "user:" + std::to_string(i);
            const auto targets = router.writeTargets(bytesOf(key));
            const auto reordered = reversed.writeTargets(bytesOf(key));
            AKK_CLUSTER_CHECK(targets.size() == 3 && reordered.size() == targets.size());
            ++owners[targets.front().nodeId - 1];
            std::set<uint64_t> unique;
            for (size_t j = 0; j < targets.size(); ++j) {
                AKK_CLUSTER_CHECK(targets[j].nodeId == reordered[j].nodeId);
                AKK_CLUSTER_CHECK(unique.insert(targets[j].nodeId).second);
                ++holders[targets[j].nodeId - 1];
            }
        }
        std::fprintf(stderr, "[placement] %u nodes, %u sequential keys; owners:", nodeCount, samples);
        for (const auto count : owners) {
            std::fprintf(stderr, " %u", count);
            const double expected = static_cast<double>(samples) / nodeCount;
            AKK_CLUSTER_CHECK(count > expected * .95 && count < expected * 1.05);
        }
        std::fprintf(stderr, "; holders:");
        for (const auto count : holders) {
            std::fprintf(stderr, " %u", count);
            const double expected = static_cast<double>(samples) * 3 / nodeCount;
            AKK_CLUSTER_CHECK(count > expected * .95 && count < expected * 1.05);
        }
        std::fprintf(stderr, "\n");
    }
    const auto dir = makeTempDir("partition-config");
    const auto members = std::vector{node(1, 25501, 25511), node(2, 25502, 25512), node(3, 25503, 25513), node(4, 25504, 25514)};
    const ClusterConfig one{members, ReplicationMode::PARTITIONED, {}, {}, {}, {}, 0, testClusterId(9551), {.replicationFactor = 1}};
    const ClusterConfig two{members, ReplicationMode::PARTITIONED, {}, {}, {}, {}, 0, one.clusterId(), {.replicationFactor = 2}};
    ClusterConfig::save(dir / "config.akcc", two);
    const auto loaded = ClusterConfig::load(dir / "config.akcc");
    AKK_CLUSTER_CHECK(loaded.partition().replicationFactor == 2 && loaded.partitionCopies() == 2);
    AKK_CLUSTER_CHECK(one.replicationFingerprint() != two.replicationFingerprint());
    AKK_CLUSTER_CHECK(loaded.replicationFingerprint() == two.replicationFingerprint());
    auto added = members; added.push_back(node(5, 25505, 25515));
    ClusterRouter before{one}, after{ClusterConfig{added, ReplicationMode::PARTITIONED, {}, {}, {}, {}, 0, one.clusterId(), {.replicationFactor = 1}}};
    for (unsigned i = 0; i < 1000; ++i) {
        const auto key = deterministicBytes(16, i + 1);
        const auto oldOwner = before.writeTargets(key).front().nodeId, newOwner = after.writeTargets(key).front().nodeId;
        AKK_CLUSTER_CHECK(oldOwner == newOwner || newOwner == 5);
        const auto targets = ClusterRouter{two}.writeTargets(key);
        AKK_CLUSTER_CHECK(targets.size() == 2 && targets.front().nodeId == oldOwner);
    }
    for (const uint16_t copies : {uint16_t{0}, uint16_t{1}}) {
        bool rejected = false;
        try {
            (void)ClusterConfig{members, ReplicationMode::PARTITIONED,
                AckPolicy{.mode = AckPolicyMode::QUORUM, .quorum = 1}, {}, {}, {}, 0, one.clusterId(), {.replicationFactor = copies}};
        } catch (const std::invalid_argument&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected);
    }
}

void testPartitionedCatchupTargets(bool historyAvailable) {
    const ClusterConfig config{{node(1, 25541, 25551), node(2, 25542, 25552), node(3, 25543, 25553), node(4, 25544, 25554)},
        ReplicationMode::PARTITIONED, {}, {}, {}, {}, 0, testClusterId(9553), {.replicationFactor = 2}};
    ClusterRouter router{config};
    const auto keyForReplica = [&](uint64_t replica) {
        for (unsigned i = 0; i < 10000; ++i) {
            const auto key = "catchup-" + std::to_string(i);
            const auto targets = router.writeTargets(bytesOf(key));
            if (targets.front().nodeId == 1 && targets[1].nodeId == replica) { return key; }
        }
        throw std::runtime_error("partition test: cannot find replica key");
    };
    ReplicationServer::EntryTargets targetsFor = [&](auto key) {
        std::vector<uint64_t> result;
        const auto targets = router.writeTargets(key);
        if (targets.front().nodeId == 1) { result.push_back(targets[1].nodeId); }
        return result;
    };
    ClusterRuntimeOptions options; options.transportMode = TransportMode::PLAIN;
    options.clusterGroupId = 9553; options.clusterGroupEpoch = 1;
    options.replicationClusterId = config.clusterId(); options.replicationConfigFingerprint = config.replicationFingerprint();
    std::vector<ReplEntry> entries;
    const std::array keys{keyForReplica(2), keyForReplica(3), keyOwnedBy(config, 2, "foreign-")};
    for (size_t i = 0; i < keys.size(); ++i) {
        entries.push_back(ReplEntry{.seq = i + 1, .sourceNodeId = 1, .op = ReplOpType::PUT,
            .key = {keys[i].begin(), keys[i].end()}, .value = {'v'}});
    }
    ReplicationServer::HistoryProvider history;
    if (historyAvailable) {
        history = [&](uint64_t after, uint64_t through) -> std::optional<std::vector<ReplEntry>> {
            std::vector<ReplEntry> selected;
            for (const auto& entry : entries) { if (entry.seq > after && entry.seq <= through) { selected.push_back(entry); } }
            return selected;
        };
    }
    auto server = ReplicationServer::create(25551, 1, [] { return uint64_t{3}; }, {}, {}, 3, {2, 3, 4}, options, history, {}, {}, targetsFor);
    for (const auto& entry : entries) { server->shipEntry(entry.seq, entry.op, entry.key, entry.value, 0, 1); }
    server->start();
    std::array<std::atomic<unsigned>, 3> applied{};
    std::array<std::unique_ptr<ReplicationClient>, 3> clients;
    for (size_t i = 0; i < clients.size(); ++i) {
        clients[i] = ReplicationClient::create("127.0.0.1", 25551, i + 2, [] { return uint64_t{0}; }, {}, options, false, {}, true);
        clients[i]->setApplyCallback([&, i](uint64_t, ReplOpType, auto key, auto, uint8_t, uint64_t) {
            const auto targets = targetsFor(key);
            AKK_CLUSTER_CHECK(std::ranges::find(targets, i + 2) != targets.end()); ++applied[i];
        });
        clients[i]->start();
    }
    AKK_CLUSTER_CHECK(waitUntil([&] { return server->replicaCount() == 3 && applied[0] == 1 && applied[1] == 1; }));
    AKK_CLUSTER_CHECK(applied[2] == 0);
    server->shipEntry(4, ReplOpType::PUT, bytesOf(keyForReplica(4)), bytesOf("live"), 0, 1);
    AKK_CLUSTER_CHECK(waitUntil([&] { return applied[2] == 1; }));
    AKK_CLUSTER_CHECK(applied[0] == 1 && applied[1] == 1);
    for (auto& client : clients) { client->close(); } server->close();
    std::fprintf(stderr, "[partition] RF=2 selective %s catch-up and live cutover passed\n", historyAvailable ? "WAL" : "buffered");
}

void testPartitionedSingleCopy() {
    const auto dir = makeTempDir("partition-one-copy");
    const ClusterConfig config{{node(1, 25561, 25571), node(2, 25562, 25572), node(3, 25563, 25573)},
        ReplicationMode::PARTITIONED, {}, ConsistencyOptions{.writeConsistency = WriteConsistency::ALL_CONFIGURED,
            .ackTimeoutAction = AckTimeoutAction::FAIL_ACK, .ackTimeoutMs = 100}, {}, {}, 0, testClusterId(9554), {.replicationFactor = 1}};
    ClusterRuntimeOptions options; options.clusterGroupId = 9554;
    auto one = makeRuntime(dir / "one", config, 1, options), two = makeRuntime(dir / "two", config, 2, options),
        three = makeRuntime(dir / "three", config, 3, options);
    one->runtime->start(); two->runtime->start(); three->runtime->start();
    AKK_CLUSTER_CHECK(waitUntil([&] { return runtimePeerConnected(*one->runtime, 2) && runtimePeerConnected(*one->runtime, 3); }));
    const auto key = keyOwnedBy(config, 1, "one-copy-");
    one->runtime->shipEntry(1, ReplOpType::PUT, bytesOf(key), bytesOf("local"), 0, 1);
    one->setState(1, bytesOf(key), bytesOf("local"));
    bool rejected = false;
    try { one->runtime->shipEntryTo(2, 2, ReplOpType::PUT, bytesOf(key), bytesOf("invalid"), 0, 1); }
    catch (const std::runtime_error&) { rejected = true; }
    AKK_CLUSTER_CHECK(rejected);
    std::this_thread::sleep_for(std::chrono::milliseconds{100});
    AKK_CLUSTER_CHECK(two->appliedCount == 0 && three->appliedCount == 0);
    three->runtime->close(); two->runtime->close(); one->runtime->close();
}

void expirePartitionFixtureWal(const std::filesystem::path& nodeDirectory) {
    // Simulate exhausted retention after the fixture's durable SST was published.
    // Keep the closed WAL files in the same fixture for inspection.
    const auto archived = nodeDirectory / "expired-wal";
    std::filesystem::create_directories(archived);
    for (const auto& entry : std::filesystem::directory_iterator(nodeDirectory / "wal")) {
        if (entry.is_regular_file() && entry.path().extension() == ".akwal") {
            std::filesystem::rename(entry.path(), archived / entry.path().filename());
        }
    }
}

void testPartitionedEmptySnapshots() {
    using Engine = akkaradb::engine::AkkEngine;
    const auto dir = makeTempDir("partition-empty-snapshots");
    const ClusterConfig config{{node(1, 25581, 25591), node(2, 25582, 25592), node(3, 25583, 25593)},
        ReplicationMode::PARTITIONED, {}, ConsistencyOptions{.writeConsistency = WriteConsistency::ALL_CONFIGURED,
            .ackTimeoutAction = AckTimeoutAction::FAIL_ACK}, {}, {}, 0, testClusterId(9555), {.replicationFactor = 1}};
    const auto optionsFor = [&](uint64_t id) {
        akkaradb::engine::AkkEngineOptions options;
        options.paths.dataDir = dir / ("n" + std::to_string(id));
        options.components.clusterEnabled = true; options.components.blobEnabled = false;
        options.wal.shardCount = 1;
        options.cluster.config = config; options.cluster.runtime.transportMode = TransportMode::PLAIN;
        options.cluster.runtime.clusterGroupId = 9555;
        options.cluster.runtime.clusterMembershipPath = options.paths.dataDir / "members.state";
        writeNodeIdFile(options.paths.dataDir / "node.id", id);
        return options;
    };
    const auto first = keyOwnedBy(config, 1, "empty-owner1-"), second = keyOwnedBy(config, 2, "empty-owner2-");
    auto one = Engine::open(optionsFor(1));
    one->put(bytesOf(first), bytesOf("first")); one->forceFlush();
    AKK_CLUSTER_CHECK(waitUntil([&] { return one->stats().sst.fileCount != 0; }));
    one->close();
    expirePartitionFixtureWal(dir / "n1");
    one = Engine::open(optionsFor(1));
    auto two = Engine::open(optionsFor(2)), three = Engine::open(optionsFor(3));
    const auto connected = [](Engine& engine) {
        const auto stats = engine.stats();
        return std::ranges::count_if(stats.cluster.peers, [](const auto& peer) { return peer.connected; }) == 2;
    };
    AKK_CLUSTER_CHECK(waitUntil([&] { return connected(*one) && connected(*two) && connected(*three); }, std::chrono::seconds{20}));
    AKK_CLUSTER_CHECK(waitUntil([&] {
        return std::filesystem::exists(dir / "n2" / "members.state.peer-1.progress") &&
            std::filesystem::exists(dir / "n3" / "members.state.peer-1.progress");
    }));
    AKK_CLUSTER_CHECK(two->stats().currentSeq == 0 && three->stats().currentSeq == 0);
    two->put(bytesOf(second), bytesOf("second"));
    AKK_CLUSTER_CHECK(two->stats().currentSeq == 1 && three->stats().currentSeq == 0);
    three->close(); two->close(); one->close();
    one = Engine::open(optionsFor(1)); two = Engine::open(optionsFor(2)); three = Engine::open(optionsFor(3));
    AKK_CLUSTER_CHECK(waitUntil([&] { return connected(*one) && connected(*two) && connected(*three); }, std::chrono::seconds{20}));
    AKK_CLUSTER_CHECK(two->stats().currentSeq == 1 && three->stats().currentSeq == 0);
    AKK_CLUSTER_CHECK(three->count() == 2);
    akkaradb::core::BufferArena arena;
    unsigned rows = 0;
    for (const auto& record : three->scanLocalStorage(arena)) { (void)record; ++rows; }
    AKK_CLUSTER_CHECK(rows == 0);
    three->close(); two->close(); one->close();
    std::fprintf(stderr, "[partition] RF=1 empty snapshots preserve local sequences across restart\n");
}

void testPartitionedBoundedStorage(bool secure) {
    using Engine = akkaradb::engine::AkkEngine;
    using Options = akkaradb::engine::AkkEngineOptions;
    const auto dir = makeTempDir(secure ? "partition-holders-secure" : "partition-holders-plain");
    const ClusterConfig config{{node(1, 25521, 25531), node(2, 25522, 25532), node(3, 25523, 25533),
        node(4, 25524, 25534), node(5, 25525, 25535)}, ReplicationMode::PARTITIONED,
        AckPolicy{.stage = AckStage::DURABLE},
        ConsistencyOptions{.writeConsistency = WriteConsistency::ALL_CONFIGURED, .ackTimeoutAction = AckTimeoutAction::FAIL_ACK, .ackTimeoutMs = 600},
        {}, {}, 0, testClusterId(9552)};
    ClusterRouter router{config};
    std::vector<ClusterPeerPublicKeyPin> pins;
    if (secure) {
        for (uint64_t id = 1; id <= 5; ++id) {
            const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
            pins.push_back({id, identity.publicKey});
        }
    }
    const auto optionsFor = [&](uint64_t id) {
        Options options;
        options.paths.dataDir = dir / ("n" + std::to_string(id));
        options.components.clusterEnabled = true;
        options.components.blobEnabled = false;
        options.wal.shardCount = 1;
        options.cluster.config = config;
        options.cluster.runtime.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
        options.cluster.runtime.secure.pinnedPeers = pins;
        options.cluster.runtime.secure.identitySeedPath = dir / ("identity-" + std::to_string(id));
        options.cluster.runtime.clusterGroupId = 9552;
        options.cluster.runtime.readMode = ClusterReadMode::LOCAL_STALE_OK;
        writeNodeIdFile(options.paths.dataDir / "node.id", id);
        return options;
    };
    std::array<std::unique_ptr<Engine>, 5> engines;
    for (uint64_t id = 1; id <= 5; ++id) { engines[id - 1] = Engine::open(optionsFor(id)); }
    const auto connectedCount = [](Engine& engine) {
        const auto stats = engine.stats();
        return std::ranges::count_if(stats.cluster.peers, [](const auto& peer) { return peer.connected; });
    };
    const auto allConnected = [&] {
        return std::ranges::all_of(engines, [&](const auto& engine) { return engine && connectedCount(*engine) == 4; });
    };
    AKK_CLUSTER_CHECK(waitUntil(allConnected, std::chrono::seconds{20}));
    std::map<std::string, std::string> expected;
    for (unsigned i = 0; i < 30; ++i) { expected.emplace("bounded-" + std::to_string(i), "value-" + std::to_string(i)); }
    expected.emplace(std::string{"bin\0\xff", 5}, std::string(40'000, 'B'));
    const auto writer = [&](const std::string& key) -> Engine& { return *engines[router.writeTargets(bytesOf(key)).front().nodeId - 1]; };
    for (const auto& [key, value] : expected) { writer(key).put(bytesOf(key), bytesOf(value)); }
    const auto localRows = [&](Engine& engine) {
        akkaradb::core::BufferArena arena;
        std::map<std::string, std::string> rows;
        for (const auto& record : engine.scanLocalStorage(arena)) { rows.emplace(textOf(record.key), textOf(record.value)); }
        return rows;
    };
    const auto verifyHolders = [&] {
        for (size_t i = 0; i < engines.size(); ++i) {
            if (!engines[i]) { continue; }
            std::map<std::string, std::string> wanted;
            for (const auto& [key, value] : expected) {
                const auto targets = router.writeTargets(bytesOf(key));
                if (std::ranges::any_of(targets, [&](const auto& target) { return target.nodeId == i + 1; })) { wanted.emplace(key, value); }
            }
            if (localRows(*engines[i]) != wanted) { return false; }
        }
        return true;
    };
    AKK_CLUSTER_CHECK(waitUntil(verifyHolders));
    std::fprintf(stderr, "[partition] live storage uses exactly three holders, including a large value\n");
    // A non-holder outage must not participate in ALL_CONFIGURED ACKs.
    const auto findKey = [&](bool heldByFive, const std::string& prefix) {
        for (unsigned i = 0; i < 10000; ++i) {
            const auto key = prefix + std::to_string(i);
            const auto targets = router.writeTargets(bytesOf(key));
            if (targets.front().nodeId == 1 && std::ranges::any_of(targets, [](const auto& target) { return target.nodeId == 5; }) == heldByFive) { return key; }
        }
        throw std::runtime_error("partition test: cannot find key");
    };
    const auto deletedKey = findKey(true, "deleted-"), largeKey = findKey(true, "snapshot-large-"), ignoredKey = findKey(false, "nonholder-");
    writer(deletedKey).put(bytesOf(deletedKey), bytesOf("delete me")); expected[deletedKey] = "delete me";
    writer(largeKey).put(bytesOf(largeKey), bytesOf("before")); expected[largeKey] = "before";
    engines[4]->close(); engines[4].reset();
    AKK_CLUSTER_CHECK(waitUntil([&] { return connectedCount(*engines[0]) == 3; }));
    writer(ignoredKey).put(bytesOf(ignoredKey), bytesOf("unaffected")); expected[ignoredKey] = "unaffected";
    const auto committedDespiteAckFailure = [&](auto mutation) {
        bool failed = false;
        try { mutation(); } catch (const std::runtime_error&) { failed = true; }
        AKK_CLUSTER_CHECK(failed);
    };
    committedDespiteAckFailure([&] { writer(deletedKey).remove(bytesOf(deletedKey)); }); expected.erase(deletedKey);
    expected[largeKey] = std::string(50'000, 'S');
    committedDespiteAckFailure([&] { writer(largeKey).put(bytesOf(largeKey), bytesOf(expected[largeKey])); });
    // Exhaust retained history only after a durable SST contains the current state.
    engines[0]->forceFlush();
    AKK_CLUSTER_CHECK(waitUntil([&] { return engines[0]->stats().sst.fileCount != 0; }));
    engines[0]->close(); engines[0].reset();
    expirePartitionFixtureWal(dir / "n1");
    engines[0] = Engine::open(optionsFor(1));
    engines[4] = Engine::open(optionsFor(5));
    AKK_CLUSTER_CHECK(waitUntil(allConnected, std::chrono::seconds{30}));
    AKK_CLUSTER_CHECK(waitUntil(verifyHolders, std::chrono::seconds{10}));
    std::fprintf(stderr, "[partition] snapshot repaired offline holder, deletion and a large value; other owners preserved\n");
    for (auto& engine : engines) { engine->close(); engine.reset(); }
    for (uint64_t id = 1; id <= 5; ++id) { engines[id - 1] = Engine::open(optionsFor(id)); }
    AKK_CLUSTER_CHECK(waitUntil(allConnected, std::chrono::seconds{30}));
    AKK_CLUSTER_CHECK(waitUntil(verifyHolders, std::chrono::seconds{10}));
    // Cluster public queries still gather the complete owner set.
    AKK_CLUSTER_CHECK(engines[3]->count() == expected.size());
    akkaradb::core::BufferArena arena;
    std::map<std::string, std::string> rows;
    for (const auto& record : engines[3]->scan(arena)) { rows.emplace(textOf(record.key), textOf(record.value)); }
    AKK_CLUSTER_CHECK(rows == expected);
    for (auto& engine : engines) { engine->close(); }
    std::fprintf(stderr, "[partition] restart and public range/count passed (%s)\n", secure ? "SECURE" : "PLAIN");
}

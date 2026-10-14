/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */

void testRaftEngineRecoveryIntegrity() {
    using namespace akkaradb::engine;
    const auto dir = makeTempDir("raft-state-machine-recovery");
    const ClusterConfig config{{node(1, 34501, 34511)}, ReplicationMode::MIRROR, {},
        {.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 1000}, {}, {}, 0, testClusterId(9991)};
    AkkEngineOptions options;
    options.components.clusterEnabled = true;
    options.components.blobEnabled = options.components.sstEnabled = options.components.manifestEnabled = false;
    options.wal.shardCount = 1;
    options.cluster.config = config;
    options.cluster.runtime.transportMode = TransportMode::PLAIN;
    options.cluster.runtime.clusterGroupId = 9991;
    options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
    options.cluster.runtime.requests.enabled = true;
    options.cluster.runtime.raftSnapshot.enabled = false;
    for (unsigned variant = 0; variant < 4; ++variant) {
        auto invalid = options;
        invalid.paths.dataDir = dir / ("invalid-" + std::to_string(variant));
        writeNodeIdFile(invalid.paths.dataDir / "node.id", 1);
        if (variant == 0) { invalid.components.walEnabled = false; }
        if (variant == 1) { invalid.runtime.recoverWal = false; }
        if (variant == 2) { invalid.components.sstEnabled = true; invalid.runtime.recoverSst = false; }
        if (variant == 3) { invalid.runtime.truncateCorruptWalOnRecovery = true; }
        bool rejected = false;
        try { (void)AkkEngine::open(invalid); } catch (const std::invalid_argument&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected);
    }
    // Cover a valid reopen, a corrupt record, complete state loss, and loss
    // behind a compacted Raft prefix. Never return APPLIED with missing data.
    for (unsigned variant = 0; variant < 4; ++variant) {
        auto opt = options;
        opt.paths.dataDir = dir / ("reopen-" + std::to_string(variant));
        opt.cluster.runtime.raftSnapshot.enabled = variant == 3;
        opt.cluster.runtime.raftSnapshot.minLogEntries = 1;
        opt.cluster.runtime.raftSnapshot.maxIntervalMs = 100;
        writeNodeIdFile(opt.paths.dataDir / "node.id", 1);
        auto engine = AkkEngine::open(opt);
        const auto id = engine->newRequestId(60000);
        AKK_CLUSTER_CHECK(engine->putWithRequest(id, bytesOf("key"), bytesOf("value")).status == ClusterRequestStatus::APPLIED);
        if (variant == 3) { AKK_CLUSTER_CHECK(waitUntil([&] { return engine->stats().cluster.snapshotIndex >= 2; })); }
        engine->close(); engine.reset();
        bool modified = false;
        for (const auto& file : std::filesystem::directory_iterator(opt.paths.dataDir / "wal")) {
            if (file.path().extension() != ".akwal" || std::filesystem::file_size(file.path()) <= wal::WalSegmentHeader::SIZE) { continue; }
            if (variant == 1) { std::filesystem::resize_file(file.path(), std::filesystem::file_size(file.path()) - 1); modified = true; }
            if (variant >= 2) { std::filesystem::resize_file(file.path(), wal::WalSegmentHeader::SIZE); modified = true; }
        }
        AKK_CLUSTER_CHECK(variant == 0 || modified);
        if (variant == 0) {
            engine = AkkEngine::open(opt);
            AKK_CLUSTER_CHECK(textOf(*engine->get(bytesOf("key"))) == "value");
            AKK_CLUSTER_CHECK(engine->queryRequest(id).status == ClusterRequestStatus::APPLIED);
            AKK_CLUSTER_CHECK(engine->putWithRequest(id, bytesOf("key"), bytesOf("value")).status == ClusterRequestStatus::APPLIED);
            AKK_CLUSTER_CHECK(textOf(*engine->get(bytesOf("key"))) == "value");
            engine->close();
        } else {
            bool rejected = false;
            try { (void)AkkEngine::open(opt); }
            catch (const std::runtime_error& error) {
                const std::string_view message{error.what()};
                rejected = message.find("corrupt WAL") != std::string_view::npos;
            }
            AKK_CLUSTER_CHECK(rejected);
        }
    }
}

void testRaftRecoveryRejectsShardLoss() {
    using namespace akkaradb::engine;
    const auto dir = makeTempDir("raft-recovery-shard-loss");
    for (unsigned variant = 0; variant < 4; ++variant) {
        AkkEngineOptions options; options.paths.dataDir = dir / std::to_string(variant);
        options.components.clusterEnabled = true;
        options.components.blobEnabled = options.components.sstEnabled = options.components.manifestEnabled = options.components.versionLogEnabled = false;
        options.wal.shardCount = 2;
        options.cluster.config = ClusterConfig{{node(1, 39901, 39911)}, ReplicationMode::MIRROR, {},
            {.mode = ConsistencyMode::RAFT_QUORUM}, {}, {}, 0, testClusterId(39900)};
        auto& runtime = options.cluster.runtime;
        runtime.transportMode = TransportMode::PLAIN; runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
        runtime.requests.enabled = true; runtime.raftSnapshot.enabled = false;
        writeNodeIdFile(options.paths.dataDir / "node.id", 1);
        auto engine = AkkEngine::open(options);
        const std::vector<uint8_t> first{'a'}, value{'v'};
        std::vector<uint8_t> second{0};
        while (akkaradb::core::computeKeyFp64(first) % 2 == akkaradb::core::computeKeyFp64(second) % 2) { ++second[0]; }
        const auto id = engine->newRequestId(60000);
        AKK_CLUSTER_CHECK(engine->putWithRequest(id, first, value).status == ClusterRequestStatus::APPLIED);
        std::filesystem::path firstWal;
        for (const auto& file : std::filesystem::directory_iterator(options.paths.dataDir / "wal")) {
            if (file.path().extension() == ".akwal" && std::filesystem::file_size(file.path()) > wal::WalSegmentHeader::SIZE) { firstWal = file.path(); }
        }
        AKK_CLUSTER_CHECK(!firstWal.empty());
        engine->put(second, value);
        AKK_CLUSTER_CHECK(engine->get(first) == std::optional{value} && engine->get(second) == std::optional{value});
        engine->close(); engine.reset();
        if (variant == 1 || variant == 2) { std::filesystem::resize_file(firstWal, variant == 1 ? wal::WalSegmentHeader::SIZE : std::filesystem::file_size(firstWal) - 1); }
        if (variant == 3) { std::filesystem::remove(firstWal); }
        if (variant == 0) {
            engine = AkkEngine::open(options);
            AKK_CLUSTER_CHECK(engine->get(first) == std::optional{value} && engine->get(second) == std::optional{value});
            AKK_CLUSTER_CHECK(engine->queryRequest(id).status == ClusterRequestStatus::APPLIED);
            AKK_CLUSTER_CHECK(engine->putWithRequest(id, first, value).status == ClusterRequestStatus::APPLIED);
            AKK_CLUSTER_CHECK(engine->get(first) == std::optional{value}); engine->close();
        } else {
            bool rejected = false;
            try { (void)AkkEngine::open(options); }
            catch (const std::runtime_error& error) { rejected = std::string_view{error.what()}.find("corrupt WAL") != std::string_view::npos; }
            AKK_CLUSTER_CHECK(rejected);
        }
    }
}

void testStripeEngineRecoveryIntegrity() {
    using namespace akkaradb::engine;
    const auto dir = makeTempDir("stripe-metadata-recovery-integrity");
    const ClusterConfig config{{node(1, 39801, 39811), node(2, 39802, 39812), node(3, 39803, 39813),
        node(4, 39804, 39814, COORDINATOR_ELIGIBLE | STRIPE_METADATA_VOTER)}, ReplicationMode::STRIPE, {},
        {.ackTimeoutMs = 1000}, {}, {.dataShards = 2, .parityShards = 1, .metadataPlacement = StripeMetadataPlacement::EXPLICIT_VOTERS},
        0, testClusterId(39800)};
    for (unsigned variant = 0; variant < 4; ++variant) {
        AkkEngineOptions invalid; invalid.paths.dataDir = dir / ("invalid-" + std::to_string(variant));
        invalid.components.clusterEnabled = true; invalid.components.blobEnabled = false; invalid.cluster.config = config;
        invalid.cluster.runtime.transportMode = TransportMode::PLAIN; invalid.cluster.runtime.clusterGroupId = 39800;
        writeNodeIdFile(invalid.paths.dataDir / "node.id", 4);
        if (variant == 0) { invalid.components.walEnabled = false; }
        if (variant == 1) { invalid.runtime.recoverWal = false; }
        if (variant == 2) { invalid.runtime.recoverSst = false; }
        if (variant == 3) { invalid.runtime.truncateCorruptWalOnRecovery = true; }
        bool rejected = false;
        try { (void)AkkEngine::open(invalid); }
        catch (const std::invalid_argument& error) { rejected = std::string_view{error.what()}.find("require WAL/SST recovery") != std::string_view::npos; }
        AKK_CLUSTER_CHECK(rejected);
    }
    for (unsigned variant = 0; variant < 5; ++variant) {
        std::array<AkkEngineOptions, 4> options; std::array<std::unique_ptr<AkkEngine>, 4> engines;
        for (size_t i = 0; i < engines.size(); ++i) {
            auto& opt = options[i]; opt.paths.dataDir = dir / std::to_string(variant) / std::to_string(i + 1);
            opt.components.clusterEnabled = true;
            opt.components.blobEnabled = opt.components.sstEnabled = opt.components.manifestEnabled = opt.components.versionLogEnabled = false;
            opt.wal.shardCount = 1; opt.cluster.config = config;
            auto& runtime = opt.cluster.runtime;
            runtime.transportMode = TransportMode::PLAIN; runtime.clusterGroupId = 39800;
            runtime.routingMode = ClusterRoutingMode::FORWARD; runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
            runtime.stripeAutoRebuild = false; runtime.raftSnapshot.enabled = false;
            writeNodeIdFile(opt.paths.dataDir / "node.id", i + 1); engines[i] = AkkEngine::open(opt);
        }
        const std::string keyText = "saved", valueText = "value";
        const auto key = bytesOf(keyText), value = bytesOf(valueText);
        AKK_CLUSTER_CHECK(waitUntil([&] { try { engines[0]->put(key, value); return true; } catch (...) { return false; } }));
        AKK_CLUSTER_CHECK(textOf(*engines[0]->get(key)) == "value");
        // This node owns only metadata. Its control checkpoint must not hide loss in its engine WAL.
        engines[3]->close(); engines[3].reset();
        std::filesystem::path metadataWal;
        for (const auto& file : std::filesystem::directory_iterator(options[3].paths.dataDir / "wal")) {
            if (file.path().extension() == ".akwal" && std::filesystem::file_size(file.path()) > wal::WalSegmentHeader::SIZE) { metadataWal = file.path(); }
        }
        AKK_CLUSTER_CHECK(!metadataWal.empty());
        if (variant == 1) { std::filesystem::resize_file(metadataWal, std::filesystem::file_size(metadataWal) - 1); }
        if (variant == 2) { std::filesystem::resize_file(metadataWal, wal::WalSegmentHeader::SIZE); }
        if (variant == 3) { std::filesystem::remove(metadataWal); }
        if (variant == 4) { std::filesystem::rename(options[3].paths.dataDir / "wal", options[3].paths.dataDir / "retained-original-wal"); }
        if (variant == 0) {
            engines[3] = AkkEngine::open(options[3]);
            AKK_CLUSTER_CHECK(waitUntil([&] { try { return engines[3]->count() == 1 && textOf(*engines[0]->get(key)) == "value"; } catch (...) { return false; } }, std::chrono::seconds{15}));
        } else {
            bool rejected = false;
            try { (void)AkkEngine::open(options[3]); }
            catch (const std::runtime_error& error) {
                const std::string_view message{error.what()};
                rejected = message.find(variant == 4 ? "behind its authority checkpoint" : "corrupt WAL") != std::string_view::npos;
            }
            AKK_CLUSTER_CHECK(rejected);
        }
        for (auto& engine : engines) { if (engine) { engine->close(); engine.reset(); } }
    }
}

void testStripeMigrationRetainsPeerHistory(bool secure) {
    using namespace akkaradb::engine;
    std::fprintf(stderr, "[peer history] setup secure=%u\n", secure);
    const auto dir = makeTempDir(secure ? "stripe-peer-history-secure" : "stripe-peer-history-plain");
    std::vector<NodeInfo> members;
    for (uint64_t id = 1; id <= 8; ++id) {
        auto flags = id >= 6 ? uint32_t{COORDINATOR_ELIGIBLE | STRIPE_METADATA_VOTER} : DATA_AND_COORDINATOR;
        if (id == 4 || id == 5) { flags |= PLACEMENT_STANDBY; }
        members.push_back(node(id, static_cast<uint16_t>(34600 + id), static_cast<uint16_t>(34610 + id), flags));
    }
    const auto configFor = [&](std::vector<NodeInfo> selected) {
        return ClusterConfig{std::move(selected), ReplicationMode::STRIPE, {}, {.ackTimeoutMs = 2000}, {},
            {.dataShards = 2, .parityShards = 1, .metadataPlacement = StripeMetadataPlacement::EXPLICIT_VOTERS}, 0, testClusterId(9992)};
    };
    std::vector<ClusterPeerPublicKeyPin> pins;
    if (secure) { for (uint64_t id = 1; id <= 8; ++id) {
        const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
        pins.push_back({id, identity.publicKey});
    } }
    std::array<AkkEngineOptions, 8> options;
    std::array<std::unique_ptr<AkkEngine>, 8> engines;
    for (size_t i = 0; i < engines.size(); ++i) {
        auto& opt = options[i]; opt.paths.dataDir = dir / std::to_string(i + 1);
        opt.components.blobEnabled = opt.components.sstEnabled = opt.components.manifestEnabled = false;
        opt.components.versionLogEnabled = i >= 6;
        opt.components.clusterEnabled = true; opt.cluster.config = configFor(members);
        auto& runtime = opt.cluster.runtime;
        runtime.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
        runtime.secure.pinnedPeers = pins; runtime.secure.identitySeedPath = dir / ("identity-" + std::to_string(i + 1));
        runtime.clusterGroupId = 9992; runtime.routingMode = ClusterRoutingMode::FORWARD;
        runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE; runtime.stripeAutoRebuild = false;
        runtime.queryResultMode = QueryResultMode::STREAMING;
        runtime.reconfiguration.resumePendingChanges = false;
        writeNodeIdFile(opt.paths.dataDir / "node.id", i + 1);
        if (i != 5) { engines[i] = AkkEngine::open(opt); }
    }
    const std::string key = "peer-history";
    AKK_CLUSTER_CHECK(waitUntil([&] { return engines[2]->stats().cluster.stripeMetadataLeaderNodeId == 7; }, std::chrono::seconds{20}));
    AKK_CLUSTER_CHECK(waitUntil([&] { try { return engines[6]->count() == 0; } catch (...) { return false; } }));
    engines[2]->put(bytesOf(key), bytesOf("old-value"));
    const auto checkpoint = engines[6]->createClusterCheckpoint();
    const Revision revision{0, checkpoint.watermarks.front().seq};
    engines[2]->put(bytesOf(key), bytesOf("new-value"));
    AKK_CLUSTER_CHECK(textOf(*engines[6]->getAt(bytesOf(key), revision)) == "old-value");
    engines[5] = AkkEngine::open(options[5]);
    const auto applied = engines[6]->stats().cluster.stripeMetadataAppliedIndex;
    AKK_CLUSTER_CHECK(waitUntil([&] { return engines[5]->stats().cluster.stripeMetadataAppliedIndex >= applied; }));
    // Move administration to a leader whose VersionLog has no retained history.
    // Restart the other voter so its fresh election deadline follows node 6.
    engines[7]->close(); engines[7].reset();
    engines[6]->close(); engines[6].reset();
    engines[7] = AkkEngine::open(options[7]);
    AKK_CLUSTER_CHECK(waitUntil([&] { return engines[2]->stats().cluster.stripeMetadataLeaderNodeId == 6; }, std::chrono::seconds{20}));
    AKK_CLUSTER_CHECK(waitUntil([&] { try { return engines[5]->count() == 1; } catch (...) { return false; } }));
    std::fprintf(stderr, "[peer history] leader without history secure=%u\n", secure);
    engines[6] = AkkEngine::open(options[6]);
    auto selected = members;
    std::erase_if(selected, [](const auto& member) { return member.nodeId == 1 || member.nodeId == 2; });
    for (auto& member : selected) { member.capabilities &= ~uint32_t{PLACEMENT_STANDBY}; }
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { engines[5]->reconfigureCluster(configFor(selected)); return true; } catch (...) { return false; }
    }, std::chrono::seconds{30}));
    AKK_CLUSTER_CHECK(engines[5]->clusterReconfigurationStatus().completedRecords == 2);
    std::fprintf(stderr, "[peer history] migrated secure=%u\n", secure);
    for (size_t i : {size_t{0}, size_t{1}, size_t{5}}) { engines[i]->close(); engines[i].reset(); }
    AKK_CLUSTER_CHECK(waitUntil([&] {
        const auto leader = engines[2]->stats().cluster.stripeMetadataLeaderNodeId; return leader == 7 || leader == 8;
    }, std::chrono::seconds{20}));
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { return textOf(*engines[2]->get(bytesOf(key))) == "new-value" && textOf(*engines[6]->getAt(bytesOf(key), revision)) == "old-value"; }
        catch (...) { return false; }
    }, std::chrono::seconds{20}));
    const auto history = akk_test::collectHistory(engines[6]->history(bytesOf(key)));
    AKK_CLUSTER_CHECK(history.size() == 2 && textOf(history[0].value) == "old-value" && textOf(history[1].value) == "new-value");
    std::fprintf(stderr, "[peer history] retained after retirement secure=%u\n", secure);
    const auto rollback = engines[6]->rollbackTo(checkpoint, {.conflict = RollbackConflictPolicy::OVERWRITE_LATEST});
    AKK_CLUSTER_CHECK(rollback.complete() && textOf(*engines[2]->get(bytesOf(key))) == "old-value");
    std::fprintf(stderr, "[peer history] rollback verified secure=%u\n", secure);
    for (auto& engine : engines) { if (engine) { engine->close(); engine.reset(); } }
    for (size_t i : {size_t{2}, size_t{3}, size_t{4}, size_t{6}, size_t{7}}) {
        options[i].cluster.config.reset(); engines[i] = AkkEngine::open(options[i]);
    }
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { return textOf(*engines[6]->getAt(bytesOf(key), revision)) == "old-value" && textOf(*engines[2]->get(bytesOf(key))) == "old-value"; }
        catch (...) { return false; }
    }, std::chrono::seconds{20}));
}

/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */

void testRaftBlobStorageCompatibility(bool secure) {
    using namespace akkaradb::engine;
    const auto dir = makeTempDir(secure ? "raft-blob-storage-secure" : "raft-blob-storage-plain");
    const ClusterConfig config{{node(1, 36701, 36711), node(2, 36702, 36712), node(3, 36703, 36713)},
        ReplicationMode::MIRROR, {}, {.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 1000}};
    std::vector<ClusterPeerPublicKeyPin> pins;
    if (secure) {
        for (uint64_t id = 1; id <= 3; ++id) {
            const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
            pins.push_back({id, identity.publicKey});
        }
    }
    std::array<AkkEngineOptions, 3> options;
    std::array<std::unique_ptr<AkkEngine>, 3> engines;
    for (size_t i = 0; i < engines.size(); ++i) {
        auto& opt = options[i]; opt.paths.dataDir = dir / ("n" + std::to_string(i + 1));
        opt.components.clusterEnabled = true; opt.components.blobEnabled = i != 2;
        opt.components.sstEnabled = opt.components.manifestEnabled = opt.components.versionLogEnabled = false;
        opt.blob.thresholdBytes = 4096;
        opt.cluster.config = config;
        auto& runtime = opt.cluster.runtime;
        runtime.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
        runtime.clusterGroupId = 36700; runtime.raftBlobPolicy = RaftBlobPolicy::RAFT_LOG;
        runtime.raftSnapshot.enabled = false;
        runtime.routingMode = ClusterRoutingMode::FORWARD;
        runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
        runtime.secure.identitySeedPath = dir / ("identity-" + std::to_string(i + 1));
        runtime.secure.pinnedPeers = pins;
        writeNodeIdFile(opt.paths.dataDir / "node.id", i + 1);
        engines[i] = AkkEngine::open(opt);
    }
    AkkEngine* compatible[] = {engines[0].get(), engines[1].get()};
    AKK_CLUSTER_CHECK(waitUntil([&] { return findEngineLeader(compatible) != nullptr; }, std::chrono::seconds{20}));
    auto* leader = findEngineLeader(compatible);
    const std::vector<uint8_t> value(8192, 0x73);
    leader->put(bytesOf("blob-storage"), value);
    AKK_CLUSTER_CHECK(waitUntil([&] { return engines[2]->stats().cluster.peerPolicyMismatchRejectsTotal > 0; }));
    const auto mismatch = engines[2]->stats().cluster;
    AKK_CLUSTER_CHECK(mismatch.lastLogIndex == 0 && mismatch.appliedIndex == 0);
    AKK_CLUSTER_CHECK(engines[2]->stats().cluster.role != static_cast<uint32_t>(NodeRole::PRIMARY));
    for (auto* engine : compatible) { AKK_CLUSTER_CHECK(engine->get(bytesOf("blob-storage")) == std::optional{value}); }
    const uint64_t target = leader == engines[0].get() ? 2 : 1;
    const auto beforeReject = leader->stats().cluster;
    const std::vector<uint8_t> oversizedKey(size_t{UINT16_MAX} + 1, 0x74), blobKey(UINT16_MAX, 0x75);
    bool rejectedKey = false, rejectedBlobRecord = false;
    try { engines[target - 1]->put(oversizedKey, bytesOf("forwarded")); }
    catch (const ClusterRoutingError& error) { rejectedKey = std::string_view{error.what()}.find("storage limit") != std::string_view::npos; }
    try { leader->put(blobKey, value); }
    catch (const std::invalid_argument&) { rejectedBlobRecord = true; }
    AKK_CLUSTER_CHECK(rejectedKey && rejectedBlobRecord);
    AKK_CLUSTER_CHECK(leader->stats().cluster.lastLogIndex == beforeReject.lastLogIndex);
    leader->transferClusterLeadership(target);
    AKK_CLUSTER_CHECK(waitUntil([&] {
        const auto* current = findEngineLeader(compatible);
        return current && current->stats().nodeId == target;
    }, std::chrono::seconds{20}));
    AKK_CLUSTER_CHECK(engines[target - 1]->get(bytesOf("blob-storage")) == std::optional{value});
    for (auto& engine : engines) { engine->close(); engine.reset(); }

    auto changed = options[0]; changed.components.blobEnabled = false;
    bool rejected = false;
    try { (void)AkkEngine::open(changed); }
    catch (const std::runtime_error& error) {
        rejected = std::string_view{error.what()}.find("apply policy mismatch") != std::string_view::npos;
    }
    AKK_CLUSTER_CHECK(rejected);
    // The log must enforce the same contract when hard state is absent.
    const auto logOnly = dir / "log-only";
    std::filesystem::create_directories(logOnly);
    std::filesystem::copy_file(options[0].paths.dataDir / "cluster-raft.log", logOnly / "cluster-raft.log");
    rejected = false;
    try { (void)makeRuntime(logOnly, config, 1, options[0].cluster.runtime, secure, {}, [](auto& callbacks) {
        callbacks.beginBlob = {}; callbacks.appendBlobChunk = {}; callbacks.finishBlob = {}; callbacks.abortBlob = {};
    }); }
    catch (const std::runtime_error& error) {
        rejected = std::string_view{error.what()}.find("apply policy mismatch") != std::string_view::npos;
    }
    AKK_CLUSTER_CHECK(rejected);
    for (size_t i = 0; i < engines.size(); ++i) { engines[i] = AkkEngine::open(options[i]); }
    compatible[0] = engines[0].get(); compatible[1] = engines[1].get();
    AKK_CLUSTER_CHECK(waitUntil([&] { return findEngineLeader(compatible) != nullptr; }, std::chrono::seconds{20}));
    for (size_t i = 0; i < 2; ++i) { AKK_CLUSTER_CHECK(engines[i]->get(bytesOf("blob-storage")) == std::optional{value}); }
    for (auto& engine : engines) { engine->close(); }
}

void testRaftEngineStorageLimits(bool requests) {
    using namespace akkaradb::engine;
    const auto dir = makeTempDir(requests ? "raft-storage-requests" : "raft-storage-mutations");
    const ClusterConfig config{{node(1, 36301, 36311)}, ReplicationMode::MIRROR, {}, {.mode = ConsistencyMode::RAFT_QUORUM}};
    AkkEngineOptions options; options.paths.dataDir = dir;
    options.components.clusterEnabled = true; options.components.blobEnabled = false;
    options.components.sstEnabled = options.components.manifestEnabled = options.components.versionLogEnabled = false;
    options.cluster.config = config;
    options.cluster.runtime.transportMode = TransportMode::PLAIN;
    options.cluster.runtime.clusterGroupId = 36300;
    options.cluster.runtime.raftSnapshot.enabled = false;
    options.cluster.runtime.requests.enabled = requests;
    options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
    writeNodeIdFile(dir / "node.id", 1);
    auto engine = AkkEngine::open(options);
    const std::vector<uint8_t> boundaryKey(UINT16_MAX, 0x61), boundaryValue(UINT16_MAX, 0x62),
        oversizedKey(size_t{UINT16_MAX} + 1, 0x63), oversizedValue(size_t{UINT16_MAX} + 1, 0x64);
    engine->put(boundaryKey, {});
    engine->put({}, boundaryValue);
    const auto original = engine->stats().cluster;
    const auto rejectedWrite = [&](auto&& write) {
        bool rejected = false;
        try { write(); } catch (const std::invalid_argument&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected);
        const auto state = engine->stats().cluster;
        AKK_CLUSTER_CHECK(state.lastLogIndex == original.lastLogIndex && state.commitIndex == original.commitIndex && state.appliedIndex == original.appliedIndex);
        AKK_CLUSTER_CHECK(state.health == original.health);
        AKK_CLUSTER_CHECK(engine->get(boundaryKey) == std::optional{std::vector<uint8_t>{}});
        AKK_CLUSTER_CHECK(engine->get({}) == std::optional{boundaryValue});
    };
    rejectedWrite([&] { engine->put(oversizedKey, bytesOf("value")); });
    rejectedWrite([&] { engine->put(bytesOf("oversized-value"), oversizedValue); });
    rejectedWrite([&] { engine->put(boundaryKey, bytesOf("combined-overflow")); });
    rejectedWrite([&] { engine->put(bytesOf("combined-overflow"), boundaryValue); });
    rejectedWrite([&] { engine->putHinted(oversizedKey, bytesOf("value"), 0, 0); });
    rejectedWrite([&] { engine->remove(oversizedKey); });
    rejectedWrite([&] { engine->removeHinted(oversizedKey, 0, 0); });
    const std::array batch{akkaradb::engine::detail::BulkPutEntry{bytesOf("bad-batch"), oversizedValue}};
    rejectedWrite([&] { akkaradb::engine::detail::ProtocolBulkWriter::put(*engine, batch); });
    if (requests) {
        const auto id = engine->newRequestId();
        rejectedWrite([&] { (void)engine->putWithRequest(id, bytesOf("bad-request"), oversizedValue); });
        AKK_CLUSTER_CHECK(engine->queryRequest(id).status == ClusterRequestStatus::NOT_FOUND);
    }
    engine->put(bytesOf("after-reject"), bytesOf("usable"));
    engine->close(); engine.reset();
    engine = AkkEngine::open(options);
    AKK_CLUSTER_CHECK(engine->get(boundaryKey) == std::optional{std::vector<uint8_t>{}});
    AKK_CLUSTER_CHECK(engine->get({}) == std::optional{boundaryValue});
    AKK_CLUSTER_CHECK(textOf(*engine->get(bytesOf("after-reject"))) == "usable");
    engine->remove(boundaryKey); AKK_CLUSTER_CHECK(!engine->get(boundaryKey));
    engine->close();
}

void testRaftEmbeddingStorageLimits() {
    const auto dir = makeTempDir("raft-embedding-storage-limits");
    const ClusterConfig config{{node(1, 36801, 36811)}, ReplicationMode::MIRROR, {}, {.mode = ConsistencyMode::RAFT_QUORUM}};
    ClusterRuntimeOptions options; options.raftSnapshot.enabled = false;
    auto harness = makeRuntime(dir, config, 1, options);
    harness->runtime->start();
    harness->runtime->shipEntry(1, ReplOpType::PUT, bytesOf("good"), bytesOf("value"), 0, 1);
    const auto before = harness->runtime->raftStats();
    const std::vector<uint8_t> large(size_t{UINT16_MAX} + 1, 0x65);
    for (unsigned operation = 0; operation < 4; ++operation) {
        bool rejected = false;
        try {
            if (operation < 2) {
                (void)harness->runtime->submitEntry(2, ReplOpType::PUT, operation == 0 ? std::span{large} : bytesOf("bad"),
                    operation == 1 ? std::span{large} : bytesOf("value"), 0, 1);
            } else {
                (void)harness->runtime->submitMutation([&](uint64_t) {
                    ClusterMutation mutation; mutation.key = operation == 2 ? large : std::vector<uint8_t>{'b'};
                    mutation.value = operation == 3 ? large : std::vector<uint8_t>{'v'}; return mutation;
                });
            }
        } catch (const std::invalid_argument&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected);
        const auto after = harness->runtime->raftStats();
        AKK_CLUSTER_CHECK(after.lastLogIndex == before.lastLogIndex && after.commitIndex == before.commitIndex && after.appliedIndex == before.appliedIndex);
    }
    harness->runtime->shipEntry(10, ReplOpType::PUT, bytesOf("after"), bytesOf("usable"), 0, 1);
    AKK_CLUSTER_CHECK(harness->hasState(10, "after", "usable"));
    harness->runtime->close();
}

void testRaftReceiverStorageLimits() {
    const auto dir = makeTempDir("raft-receiver-storage-limits");
    const ClusterConfig config{{node(1, 36901, 36911), node(2, 36902, 36912), node(3, 36903, 36913)},
        ReplicationMode::MIRROR, {}, {.mode = ConsistencyMode::RAFT_QUORUM}};
    auto harness = makeRuntime(dir, config, 1);
    harness->runtime->start();
    RaftTestSocket peer; openRaftTestPeer(peer, 36911, 2, config);
    const auto append = [&](size_t keyLength, size_t valueLength) {
        std::vector<uint8_t> entry;
        for (uint64_t value : {2ull, 1ull, 1ull}) { appendRaftTestInteger(entry, value); }
        entry.insert(entry.end(), {0, static_cast<uint8_t>(ReplOpType::PUT), 0, 1});
        appendRaftTestInteger(entry, 2); appendRaftTestInteger(entry, 1);
        appendRaftTestInteger(entry, keyLength, 4); appendRaftTestInteger(entry, valueLength, 4);
        entry.push_back(0); entry.insert(entry.end(), keyLength, 'k'); entry.insert(entry.end(), valueLength, 'v');
        std::vector<uint8_t> request;
        for (uint64_t value : {2ull, 2ull, 0ull, 0ull, 1ull}) { appendRaftTestInteger(request, value); }
        appendRaftTestInteger(request, 1, 4); appendRaftTestInteger(request, entry.size(), 4);
        request.insert(request.end(), entry.begin(), entry.end());
        AKK_CLUSTER_CHECK(peer.send(encodeFrame(ReplMsgType::RAFT_APPEND_ENTRIES, request)));
        return peer.receiveFrame();
    };
    for (const auto& sizes : {std::pair{size_t{65536}, size_t{1}}, std::pair{size_t{1}, size_t{65536}},
        std::pair{size_t{65535}, size_t{1}}}) {
        const auto response = append(sizes.first, sizes.second);
        AKK_CLUSTER_CHECK(response.type == ReplMsgType::RAFT_APPEND_ENTRIES_RESPONSE && response.payload.at(8) == 0);
        const auto state = harness->runtime->raftStats();
        AKK_CLUSTER_CHECK(state.lastLogIndex == 0 && state.commitIndex == 0 && state.appliedIndex == 0 && harness->appliedCount.load() == 0);
    }
    const auto response = append(1, 1);
    AKK_CLUSTER_CHECK(response.payload.at(8) == 1 && harness->hasState(1, "k", "v"));
    peer.close(); harness->runtime->close();
}

void runRaftStorageContractTests() {
    std::fprintf(stderr, "[raft-storage] Blob capability PLAIN/SECURE, leadership transfer and reopen\n");
    testRaftBlobStorageCompatibility(false); testRaftBlobStorageCompatibility(true);
    std::fprintf(stderr, "[raft-storage] engine size boundaries, requests, batch and reopen\n");
    testRaftEngineStorageLimits(false); testRaftEngineStorageLimits(true);
    std::fprintf(stderr, "[raft-storage] low-level proposal admission\n");
    testRaftEmbeddingStorageLimits();
    std::fprintf(stderr, "[raft-storage] receiver rejects unrepresentable committed records\n");
    testRaftReceiverStorageLimits();
}

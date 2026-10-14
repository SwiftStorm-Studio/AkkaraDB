/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
void testNonRaftEngineRequests() {
    const auto dir = makeTempDir("nonraft-engine-requests");
    akkaradb::engine::AkkEngineOptions options;
    options.paths.dataDir = dir; options.components.clusterEnabled = true;
    options.components.blobEnabled = false;
    options.cluster.config = ClusterConfig{{node(1, 32401, 32411)}, ReplicationMode::MIRROR, {},
        ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 2000}, {}, {}, 1, testClusterId(9631)};
    options.cluster.runtime.transportMode = TransportMode::PLAIN;
    options.cluster.runtime.requests.enabled = true;
    options.cluster.runtime.requests.authorityNodes = {node(1, 0, 32421)};
    options.cluster.runtime.requests.maxPendingRequests = 8;
    options.cluster.runtime.raftSnapshot.minLogEntries = 1;
    auto invalid = options.cluster.runtime; invalid.requests.authorityNodes.clear();
    bool missingAuthority = false;
    try { options.cluster.config->validateRuntime(1, invalid); } catch (const std::invalid_argument&) { missingAuthority = true; }
    AKK_CLUSTER_CHECK(missingAuthority);
    invalid = options.cluster.runtime; invalid.requests.authorityNodes.front().replPort = 32411;
    bool conflictingPort = false;
    try { options.cluster.config->validateRuntime(1, invalid); } catch (const std::invalid_argument&) { conflictingPort = true; }
    AKK_CLUSTER_CHECK(conflictingPort);
    writeNodeIdFile(dir / "node.id", 1);
    auto engine = akkaradb::engine::AkkEngine::open(options);
    const auto nowMs = [] {
        return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
    };
    const auto beforeId = nowMs();
    const auto id = engine->newRequestId();
    AKK_CLUSTER_CHECK(id.expiresAtUnixMs >= beforeId + 600'000 && id.expiresAtUnixMs <= nowMs() + 600'000);
    const auto beforeLongId = nowMs();
    const auto longId = engine->newRequestId(24ull * 60 * 60 * 1000);
    AKK_CLUSTER_CHECK(longId.expiresAtUnixMs >= beforeLongId + 86'400'000 && longId.expiresAtUnixMs <= nowMs() + 86'400'000);
    bool excessiveRetention = false;
    try { (void)engine->newRequestId(86'400'001); } catch (const std::invalid_argument&) { excessiveRetention = true; }
    AKK_CLUSTER_CHECK(excessiveRetention);
    AKK_CLUSTER_CHECK(waitUntil([&] { try { return engine->queryRequest(id).status == ClusterRequestStatus::NOT_FOUND; } catch (...) { return false; } }));
    const std::string value(4096, 'b');
    const auto first = engine->putWithRequest(id, bytesOf("once"), bytesOf(value));
    AKK_CLUSTER_CHECK(first.status == ClusterRequestStatus::APPLIED && first.sequence != 0);
    engine->put(bytesOf("once"), bytesOf("newer"));
    const auto before = engine->stats().currentSeq;
    const auto retried = engine->putWithRequest(id, bytesOf("once"), bytesOf(value));
    AKK_CLUSTER_CHECK(retried.status == ClusterRequestStatus::APPLIED && retried.sequence == first.sequence);
    AKK_CLUSTER_CHECK(engine->stats().currentSeq == before && textOf(*engineGet(*engine, "once")) == "newer");
    const auto boundary = engine->newRequestId();
    AKK_CLUSTER_CHECK(engine->putWithRequest(boundary, bytesOf("ab"), bytesOf("c")).status == ClusterRequestStatus::APPLIED);
    bool mismatch = false;
    try { (void)engine->putWithRequest(boundary, bytesOf("a"), bytesOf("bc")); }
    catch (const ClusterRoutingError& error) { mismatch = error.code == ClusterRoutingCode::WRITE_REJECTED; }
    AKK_CLUSTER_CHECK(mismatch && !engineGet(*engine, "a"));
    const auto removal = engine->newRequestId();
    AKK_CLUSTER_CHECK(engine->removeWithRequest(removal, bytesOf("ab")).status == ClusterRequestStatus::APPLIED);
    engine->put(bytesOf("ab"), bytesOf("restored"));
    AKK_CLUSTER_CHECK(engine->removeWithRequest(removal, bytesOf("ab")).status == ClusterRequestStatus::APPLIED);
    AKK_CLUSTER_CHECK(textOf(*engineGet(*engine, "ab")) == "restored");
    engine->close(); engine = akkaradb::engine::AkkEngine::open(options);
    AKK_CLUSTER_CHECK(waitUntil([&] { try { return engine->queryRequest(id).status == ClusterRequestStatus::APPLIED; } catch (...) { return false; } }));
    AKK_CLUSTER_CHECK(engine->putWithRequest(id, bytesOf("once"), bytesOf(value)).sequence == first.sequence);
    AKK_CLUSTER_CHECK(textOf(*engineGet(*engine, "once")) == "newer");
    auto expired = id; expired.expiresAtUnixMs = 1;
    AKK_CLUSTER_CHECK(engine->putWithRequest(expired, bytesOf("once"), bytesOf(value)).status == ClusterRequestStatus::EXPIRED);
    const auto shortId = engine->newRequestId(1);
    std::this_thread::sleep_for(std::chrono::milliseconds{20});
    AKK_CLUSTER_CHECK(engine->putWithRequest(shortId, bytesOf("expired-new"), bytesOf(value)).status == ClusterRequestStatus::EXPIRED);
    AKK_CLUSTER_CHECK(!engineGet(*engine, "expired-new"));
    engine->close();
    auto shorterDefault = options; shorterDefault.cluster.runtime.requests.defaultRetentionMs = 30'000;
    engine = akkaradb::engine::AkkEngine::open(shorterDefault);
    const auto beforeShorterId = nowMs();
    const auto shorterId = engine->newRequestId();
    AKK_CLUSTER_CHECK(shorterId.expiresAtUnixMs >= beforeShorterId + 30'000 && shorterId.expiresAtUnixMs <= nowMs() + 30'000);
    AKK_CLUSTER_CHECK(waitUntil([&] { try { return engine->queryRequest(id).status == ClusterRequestStatus::APPLIED; } catch (...) { return false; } }));
    engine->close();
    auto downgraded = options; downgraded.cluster.runtime.requests = {};
    bool rejected = false; try { (void)akkaradb::engine::AkkEngine::open(downgraded); } catch (const std::runtime_error&) { rejected = true; }
    AKK_CLUSTER_CHECK(rejected);
    auto changed = options; changed.cluster.runtime.requests.maxPendingRequests += 1;
    bool changedPolicy = false; try { (void)akkaradb::engine::AkkEngine::open(changed); } catch (const std::runtime_error&) { changedPolicy = true; }
    AKK_CLUSTER_CHECK(changedPolicy);
    const auto statePath = dir / "request-authority" / "state.akrq";
    std::ifstream stateFile{statePath, std::ios::binary};
    std::vector<char> image((std::istreambuf_iterator<char>(stateFile)), {}); stateFile.close();
    AKK_CLUSTER_CHECK(image.size() > 4); image.back() ^= 1;
    { std::ofstream corrupt{statePath, std::ios::binary | std::ios::trunc}; corrupt.write(image.data(), static_cast<std::streamsize>(image.size())); }
    bool corruptRejected = false;
    try { (void)akkaradb::engine::AkkEngine::open(options); } catch (const std::runtime_error&) { corruptRejected = true; }
    AKK_CLUSTER_CHECK(corruptRejected);
}

void testIndependentRequestAuthority(bool secure) {
    const auto dir = makeTempDir(secure ? "request-membership-secure" : "request-membership");
    const ClusterConfig config{{node(1, 32801, 32811), node(2, 32802, 32812), node(3, 32803, 32813), node(4, 32804, 32814)},
        ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::ASYNC, .ackTimeoutMs = 2000},
        {}, {}, 1, testClusterId(9636)};
    ClusterRuntimeOptions options; options.requests.enabled = true;
    options.requests.authorityNodes = {node(1, 0, 32821), node(2, 0, 32822), node(3, 0, 32823)};
    options.forwardingTimeoutMs = 10000; options.reconfiguration.timeoutMs = 20000;
    options.raftHeartbeatIntervalMs = 30; options.raftSnapshot.minLogEntries = 1;
    if (secure) {
        for (uint64_t id = 1; id <= 4; ++id) {
            const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
            options.secure.pinnedPeers.push_back({id, identity.publicKey});
        }
    }
    std::vector<std::unique_ptr<RuntimeHarness>> nodes(4);
    const auto open = [&](uint64_t id, ClusterRuntimeOptions local) {
        local.secure.identitySeedPath = dir / ("identity-" + std::to_string(id));
        local.startupRole = id == 1 ? NodeStartupRole::PRIMARY : NodeStartupRole::REPLICA;
        nodes[id - 1] = makeRuntime(dir / std::to_string(id), config, id, local, secure); nodes[id - 1]->runtime->start();
    };
    for (uint64_t id = 1; id <= 4; ++id) { open(id, options); }
    const auto makeId = [](uint8_t nonce) {
        ClusterRequestId id; id.nonce[0] = nonce;
        id.expiresAtUnixMs = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()) + 600000;
        return id;
    };
    const auto success = makeId(1), pending = makeId(2), later = makeId(3);
    std::string lastQueryError;
    const bool proxyReady = waitUntil([&] { try { return nodes[3]->runtime->queryRequest(success).status == ClusterRequestStatus::NOT_FOUND; } catch (const std::exception& error) { lastQueryError = error.what(); return false; } }, std::chrono::seconds{20});
    if (!proxyReady) { throw std::runtime_error("Authority proxy discovery: " + lastQueryError); }
    AKK_CLUSTER_CHECK(!std::filesystem::exists(dir / "4" / "request-authority" / "cluster-raft.state"));
    std::array<uint8_t, 32> fingerprint{}; fingerprint[0] = 7; unsigned applications = 0;
    const auto write = [&] { ++applications; return uint64_t{77}; };
    AKK_CLUSTER_CHECK(nodes[3]->runtime->executeRequest(success, fingerprint, write).status == ClusterRequestStatus::APPLIED);
    auto invalid = options.requests.authorityNodes; invalid.front().replPort = 32811;
    bool rejected = false;
    try { nodes[3]->runtime->reconfigureRequestAuthority(invalid); } catch (const std::invalid_argument&) { rejected = true; }
    AKK_CLUSTER_CHECK(rejected);
    invalid = options.requests.authorityNodes; for (auto& member : invalid) { member.capabilities |= RAFT_LEARNER; }
    rejected = false;
    try { nodes[3]->runtime->reconfigureRequestAuthority(invalid); } catch (const std::invalid_argument&) { rejected = true; }
    AKK_CLUSTER_CHECK(rejected && nodes[3]->runtime->queryRequest(success).status == ClusterRequestStatus::APPLIED);
    try { (void)nodes[3]->runtime->executeRequest(pending, fingerprint, [&]() -> uint64_t { ++applications; throw std::runtime_error("interrupted"); }); }
    catch (const ClusterRoutingError& error) { AKK_CLUSTER_CHECK(error.code == ClusterRoutingCode::OUTCOME_UNKNOWN); }
    AKK_CLUSTER_CHECK(applications == 2);
    // Reopen only the joining host as a learner, then replace one voter.
    nodes[3]->runtime->close(); nodes[3].reset();
    auto joining = options; auto learner = node(4, 0, 32824); learner.capabilities |= RAFT_LEARNER;
    joining.requests.authorityNodes.push_back(learner); open(4, joining);
    const std::vector<NodeInfo> target{node(2, 0, 32822), node(3, 0, 32823), node(4, 0, 32824)};
    std::string lastMembershipError;
    const bool replaced = waitUntil([&] {
        try { nodes[3]->runtime->reconfigureRequestAuthority(target); return true; }
        catch (const std::exception& error) { lastMembershipError = error.what(); return false; }
    }, std::chrono::seconds{30});
    if (!replaced) { throw std::runtime_error("Authority replacement: " + lastMembershipError); }
    nodes[0]->runtime->close();
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { return nodes[3]->runtime->queryRequest(success).status == ClusterRequestStatus::APPLIED && nodes[3]->runtime->queryRequest(pending).status == ClusterRequestStatus::PENDING; }
        catch (...) { return false; }
    }, std::chrono::seconds{20}));
    AKK_CLUSTER_CHECK(nodes[3]->runtime->executeRequest(pending, fingerprint, write).status == ClusterRequestStatus::PENDING && applications == 2);
    AKK_CLUSTER_CHECK(nodes[3]->runtime->executeRequest(later, fingerprint, write).status == ClusterRequestStatus::APPLIED && applications == 3);
    for (auto& n : nodes) { n->runtime->close(); n.reset(); }
    options.requests.authorityNodes = target;
    for (uint64_t id = 1; id <= 4; ++id) { open(id, options); }
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { return nodes[0]->runtime->queryRequest(success).status == ClusterRequestStatus::APPLIED && nodes[0]->runtime->queryRequest(pending).status == ClusterRequestStatus::PENDING; }
        catch (...) { return false; }
    }, std::chrono::seconds{20}));
    AKK_CLUSTER_CHECK(nodes[0]->runtime->executeRequest(success, fingerprint, write).sequence == 77 && applications == 3);
    for (auto& n : nodes) { n->runtime->close(); }
}

void testRequestAuthorityRetiringPlacementCaller() {
    const auto dir = makeTempDir("request-authority-retiring-placement-caller");
    auto physical = std::vector{node(1, 33101, 33111), node(2, 33102, 33112), node(3, 33103, 33113)};
    for (auto& member : physical) { member.stripeMetadataPort = static_cast<uint16_t>(33130 + member.nodeId); }
    const ClusterConfig config{physical, ReplicationMode::STRIPE, {},
        ConsistencyOptions{.mode = ConsistencyMode::ASYNC, .ackTimeoutMs = 5000}, {},
        StripeOptions{.dataShards = 1, .parityShards = 1}, 0, testClusterId(9638)};
    ClusterRuntimeOptions options; options.clusterGroupId = 9638; options.requests.enabled = true;
    options.requests.authorityNodes = {node(1, 0, 33121), node(2, 0, 33122)};
    options.forwardingTimeoutMs = 10000; options.raftHeartbeatIntervalMs = 30;
    std::vector<std::unique_ptr<RuntimeHarness>> nodes;
    for (uint64_t id = 1; id <= 3; ++id) {
        nodes.push_back(makeRuntime(dir / std::to_string(id), config, id, options)); nodes.back()->runtime->start();
    }
    ClusterRequestId probe; probe.nonce[0] = 1;
    probe.expiresAtUnixMs = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count()) + 600000;
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { return nodes[2]->runtime->queryRequest(probe).status == ClusterRequestStatus::NOT_FOUND; }
        catch (const std::exception&) { return false; }
    }, std::chrono::seconds{20}));
    physical.pop_back();
    const ClusterConfig target{physical, config.mode(), config.ackPolicy(), config.consistency(), config.raft(), config.stripe(),
        config.primaryNodeId(), config.clusterId(), config.partition(), config.failover(), config.faultDomains()};
    // Membership validation must pass even if the retiring caller is not the
    // placement leader; only the normal leader redirect may then reject it.
    try { AKK_CLUSTER_CHECK(nodes[2]->runtime->beginPlacementChange(target) != 0); }
    catch (const ClusterRoutingError& error) { AKK_CLUSTER_CHECK(error.code == ClusterRoutingCode::NOT_OWNER); }
    for (auto& member : nodes) { member->runtime->close(); }
}

void testNonRaftRequestAuthority(bool secure) {
    const auto dir = makeTempDir(secure ? "nonraft-request-authority-secure" : "nonraft-request-authority");
    const ClusterConfig config{{node(1, 32501, 32511), node(2, 32502, 32512), node(3, 32503, 32513)},
        ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::ASYNC, .ackTimeoutMs = 2000},
        {}, {}, 1, testClusterId(9632)};
    ClusterRuntimeOptions options;
    options.requests.enabled = true; options.requests.defaultRetentionMs = 60'000; options.requests.maxRetentionMs = 60'000; options.requests.maxPendingRequests = 4; options.requests.maxRetainedResultBytes = 4 * 155;
    options.requests.authorityNodes = {node(1, 0, 32521), node(2, 0, 32522), node(3, 0, 32523)};
    options.raftSnapshot.minLogEntries = 1;
    options.forwardingTimeoutMs = 10000;
    if (secure) {
        for (uint64_t id = 1; id <= 3; ++id) {
            const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
            options.secure.pinnedPeers.push_back({id, identity.publicKey});
        }
    }
    std::vector<std::unique_ptr<RuntimeHarness>> nodes;
    const auto open = [&] {
        for (uint64_t id = 1; id <= 3; ++id) {
            auto local = options; local.secure.identitySeedPath = dir / ("identity-" + std::to_string(id)); local.startupRole = id == 1 ? NodeStartupRole::PRIMARY : NodeStartupRole::REPLICA;
            nodes.push_back(makeRuntime(dir / std::to_string(id), config, id, local, secure)); nodes.back()->runtime->start();
        }
    };
    const auto makeId = [](uint8_t nonce) {
        ClusterRequestId id; id.nonce[0] = nonce;
        id.expiresAtUnixMs = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count()) + 50'000;
        return id;
    };
    open(); const auto success = makeId(1), interrupted = makeId(2), parallel = makeId(3);
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { for (auto& n : nodes) { if (n->runtime->queryRequest(success).status != ClusterRequestStatus::NOT_FOUND) { return false; } } return true; }
        catch (...) { return false; }
    }));
    std::array<uint8_t, 32> fingerprint{}; fingerprint[0] = 9;
    std::atomic<unsigned> applied{0};
    const auto write = [&] { ++applied; return uint64_t{42}; };
    AKK_CLUSTER_CHECK(nodes[0]->runtime->executeRequest(success, fingerprint, write).status == ClusterRequestStatus::APPLIED);
    AKK_CLUSTER_CHECK(nodes[1]->runtime->executeRequest(success, fingerprint, write).status == ClusterRequestStatus::APPLIED && applied == 1);
    bool unknown = false;
    try { (void)nodes[2]->runtime->executeRequest(interrupted, fingerprint, [&]() -> uint64_t { ++applied; throw std::runtime_error("crash after application"); }); }
    catch (const ClusterRoutingError& error) { unknown = error.code == ClusterRoutingCode::OUTCOME_UNKNOWN && error.requestId == interrupted; }
    AKK_CLUSTER_CHECK(unknown && applied == 2);
    AKK_CLUSTER_CHECK(nodes[0]->runtime->queryRequest(interrupted).status == ClusterRequestStatus::PENDING);
    AKK_CLUSTER_CHECK(nodes[0]->runtime->executeRequest(interrupted, fingerprint, write).status == ClusterRequestStatus::PENDING && applied == 2);
    std::vector<std::future<ClusterRequestResult>> competing;
    for (unsigned i = 0; i < 9; ++i) {
        competing.push_back(std::async(std::launch::async, [&, i] { return nodes[i % 3]->runtime->executeRequest(parallel, fingerprint, write); }));
    }
    unsigned completed = 0;
    for (auto& attempt : competing) { if (attempt.get().status == ClusterRequestStatus::APPLIED) { ++completed; } }
    AKK_CLUSTER_CHECK(completed >= 1 && applied == 3);
    const auto last = makeId(4), overflow = makeId(5);
    AKK_CLUSTER_CHECK(nodes[1]->runtime->executeRequest(last, fingerprint, write).status == ClusterRequestStatus::APPLIED);
    bool full = false;
    try { (void)nodes[2]->runtime->executeRequest(overflow, fingerprint, write); }
    catch (const ClusterRoutingError& error) { full = error.code == ClusterRoutingCode::WRITE_REJECTED; }
    AKK_CLUSTER_CHECK(full && applied == 4);
    // Drop the configured data Primary; the independent receipt quorum still
    // answers and prevents reapplication on a different node.
    nodes[0]->runtime->close();
    std::string failoverError;
    const bool readable = waitUntil([&] {
        try { return nodes[1]->runtime->queryRequest(success).status == ClusterRequestStatus::APPLIED; }
        catch (const std::exception& error) { failoverError = error.what(); return false; }
    }, std::chrono::milliseconds{10000});
    if (!readable) { throw std::runtime_error("Receipt failover failed: " + failoverError); }
    AKK_CLUSTER_CHECK(nodes[1]->runtime->executeRequest(success, fingerprint, write).status == ClusterRequestStatus::APPLIED && applied == 4);
    for (auto& n : nodes) { n->runtime->close(); } nodes.clear(); open();
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { return nodes[2]->runtime->queryRequest(interrupted).status == ClusterRequestStatus::PENDING; } catch (...) { return false; }
    }));
    AKK_CLUSTER_CHECK(nodes[2]->runtime->executeRequest(interrupted, fingerprint, write).status == ClusterRequestStatus::PENDING && applied == 4);
    AKK_CLUSTER_CHECK(nodes[2]->runtime->executeRequest(success, fingerprint, write).status == ClusterRequestStatus::APPLIED && applied == 4);
    for (auto& n : nodes) { n->runtime->close(); }
}

void testNonRaftRequestForwarding(ReplicationMode mode) {
    const auto dir = makeTempDir("nonraft-request-forwarding");
    const auto config = mode == ReplicationMode::STRIPE ? stripeStorageConfig() : ClusterConfig{
        {node(1, 32601, 32611), node(2, 32602, 32612), node(3, 32603, 32613)}, mode, {},
        ConsistencyOptions{.mode = ConsistencyMode::ASYNC, .ackTimeoutMs = 2000}, {}, {},
        mode == ReplicationMode::MIRROR ? 1ull : 0ull, testClusterId(9633)};
    const std::string key = "forward-once";
    const auto ownerId = ClusterRouter{config}.writeTargets(bytesOf(key)).front().nodeId;
    const auto clientId = ownerId == 3 ? 2ull : 3ull;
    std::vector<std::unique_ptr<akkaradb::engine::AkkEngine>> engines;
    for (uint64_t id = 1; id <= 3; ++id) {
        auto options = stripeStorageOptions(dir, id); options.cluster.config = config;
        options.cluster.runtime.startupRole = id == 1 ? NodeStartupRole::PRIMARY : NodeStartupRole::REPLICA;
        options.cluster.runtime.requests.enabled = true;
        options.cluster.runtime.requests.authorityNodes = {node(1, 0, 32621), node(2, 0, 32622), node(3, 0, 32623)};
        options.cluster.runtime.nodeResources.incomingForwardRequests = 1;
        options.cluster.runtime.nodeResources.outgoingForwardRequests = 1;
        engines.push_back(akkaradb::engine::AkkEngine::open(options));
    }
    auto& client = *engines[clientId - 1]; auto& owner = *engines[ownerId - 1];
    const auto id = client.newRequestId();
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try {
            for (auto& engine : engines) { if (engine->queryRequest(id, bytesOf(key)).status != ClusterRequestStatus::NOT_FOUND) { return false; } }
            return mode != ReplicationMode::STRIPE || engines[0]->stats().cluster.stripeMetadataLeaderNodeId != 0;
        } catch (...) { return false; }
    }));
    AKK_CLUSTER_CHECK(client.putWithRequest(id, bytesOf(key), bytesOf("first")).status == ClusterRequestStatus::APPLIED);
    owner.put(bytesOf(key), bytesOf("newer"));
    AKK_CLUSTER_CHECK(client.putWithRequest(id, bytesOf(key), bytesOf("first")).status == ClusterRequestStatus::APPLIED);
    AKK_CLUSTER_CHECK(textOf(*engineGet(owner, key)) == "newer");
    AKK_CLUSTER_CHECK(client.queryRequest(id, bytesOf(key)).status == ClusterRequestStatus::APPLIED);
    if (mode == ReplicationMode::STRIPE) {
        // An authority member must be retired separately before its physical node.
        auto added = config.nodes(); std::erase_if(added, [](const auto& node) { return node.nodeId == 3; });
        added.push_back(node(4, 32604, 32614));
        const ClusterConfig target{added, mode, config.ackPolicy(), config.consistency(), config.raft(), config.stripe(),
            config.primaryNodeId(), config.clusterId(), config.partition(), config.failover(), config.faultDomains()};
        const auto administratorId = engines[0]->stats().cluster.stripeMetadataLeaderNodeId;
        AKK_CLUSTER_CHECK(administratorId >= 1 && administratorId <= engines.size());
        bool blocked = false;
        try { engines[administratorId - 1]->reconfigureCluster(target); } catch (const std::invalid_argument&) { blocked = true; }
        AKK_CLUSTER_CHECK(blocked);
        AKK_CLUSTER_CHECK(client.queryRequest(id, bytesOf(key)).status == ClusterRequestStatus::APPLIED);
    }
    for (auto& engine : engines) { engine->close(); }
}

void testNonRaftRequests() {
    const auto run = [](const char* label, auto test) {
        try { test(); } catch (const std::exception& error) { throw std::runtime_error(std::string{label} + ": " + error.what()); }
    };
    run("engine requests", [] { testNonRaftEngineRequests(); });
    run("plain authority", [] { testNonRaftRequestAuthority(false); });
    run("secure authority", [] { testNonRaftRequestAuthority(true); });
    run("MIRROR forwarding", [] { testNonRaftRequestForwarding(ReplicationMode::MIRROR); });
    run("PARTITIONED forwarding", [] { testNonRaftRequestForwarding(ReplicationMode::PARTITIONED); });
    run("STRIPE forwarding", [] { testNonRaftRequestForwarding(ReplicationMode::STRIPE); });
}

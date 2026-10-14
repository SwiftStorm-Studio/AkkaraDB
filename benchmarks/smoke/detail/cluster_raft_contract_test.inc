/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
void testRaftGroupIsolation(bool secure) {
    for (unsigned mismatch = 0; mismatch < 3; ++mismatch) {
        const auto dir = makeTempDir(secure ? "raft-group-secure" : "raft-group-plain");
        const ClusterConfig config{{node(1, 31601, 31611), node(2, 31602, 31612), node(3, 31603, 31613)},
            ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 2000}};
        ClusterRuntimeOptions options;
        options.clusterGroupId = 123; options.raftGroupKind = RaftGroupKind::PARTITION_DATA;
        if (secure) {
            for (uint64_t id = 1; id <= 3; ++id) {
                const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
                options.secure.pinnedPeers.push_back({id, identity.publicKey});
            }
        }
        auto makePeer = [&](uint64_t id) {
            auto runtime = options;
            runtime.secure.identitySeedPath = dir / ("identity-" + std::to_string(id));
            if (id == 3) {
                if (mismatch == 0) { ++runtime.clusterGroupId; }
                if (mismatch == 1) { runtime.raftGroupKind = RaftGroupKind::PLACEMENT; }
                if (mismatch == 2) { ++runtime.raftPartitionIndex; }
            }
            return makeRuntime(dir / ("n" + std::to_string(id)), config, id, runtime, secure);
        };
        auto n1 = makePeer(1), n2 = makePeer(2), n3 = makePeer(3);
        n1->runtime->start(); n2->runtime->start(); n3->runtime->start();
        RuntimeHarness* compatible[] = {n1.get(), n2.get()};
        AKK_CLUSTER_CHECK(waitUntil([&] { return findLeader(compatible) != nullptr; }, std::chrono::seconds{15}));
        auto* leader = findLeader(compatible);
        leader->runtime->shipEntry(1, ReplOpType::PUT, bytesOf("group"), bytesOf("isolated"), 0, leader->nodeId);
        AKK_CLUSTER_CHECK(waitUntil([&] { return n1->hasState(1, "group", "isolated") && n2->hasState(1, "group", "isolated"); }));
        AKK_CLUSTER_CHECK(waitUntil([&] { return n3->runtime->raftStats().foreignClusterRejects > 0; }));
        AKK_CLUSTER_CHECK(n3->runtime->raftStats().lastLogIndex == 0 && n3->runtime->role() != NodeRole::PRIMARY);
        n3->runtime->close(); n2->runtime->close(); n1->runtime->close();
    }
}

void testRaftDurableGroupIsolation() {
    const auto dir = makeTempDir("raft-durable-group");
    const ClusterConfig config{{node(1, 31621, 31631)}, ReplicationMode::MIRROR, {},
        ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM}};
    ClusterRuntimeOptions options;
    options.clusterGroupId = 123; options.raftGroupKind = RaftGroupKind::PARTITION_DATA;
    {
        auto original = makeRuntime(dir / "n1", config, 1, options);
        original->runtime->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return original->runtime->role() == NodeRole::PRIMARY; }));
        original->runtime->shipEntry(1, ReplOpType::PUT, bytesOf("group"), bytesOf("durable"), 0, 1);
        original->runtime->close();
    }
    for (unsigned mismatch = 0; mismatch < 3; ++mismatch) {
        auto foreign = options;
        if (mismatch == 0) { ++foreign.clusterGroupId; }
        if (mismatch == 1) { foreign.raftGroupKind = RaftGroupKind::PLACEMENT; }
        if (mismatch == 2) { ++foreign.raftPartitionIndex; }
        bool rejected = false;
        try { (void)makeRuntime(dir / "n1", config, 1, foreign); }
        catch (const std::runtime_error& error) { rejected = std::string_view{error.what()}.find("different Raft group") != std::string_view::npos; }
        AKK_CLUSTER_CHECK(rejected);
        // The log must remain bound even when hard state is absent.
        const auto logOnly = dir / ("log-only-" + std::to_string(mismatch));
        std::filesystem::create_directories(logOnly);
        std::filesystem::copy_file(dir / "n1" / "cluster-raft.log", logOnly / "cluster-raft.log");
        rejected = false;
        try { (void)makeRuntime(logOnly, config, 1, foreign); }
        catch (const std::runtime_error& error) { rejected = std::string_view{error.what()}.find("different Raft group") != std::string_view::npos; }
        AKK_CLUSTER_CHECK(rejected);
    }
    auto recovered = makeRuntime(dir / "n1", config, 1, options);
    AKK_CLUSTER_CHECK(recovered->hasState(1, "group", "durable"));
}

void testRaftTransactionApplyPolicy(bool secure) {
    for (bool blobMismatch : {false, true}) {
        std::fprintf(stderr, "[raft-contract] transaction apply policy secure %u, mismatch %s\n",
            secure, blobMismatch ? "Blob threshold" : "journal capacity");
        ClusterTransactionFixture fixture{makeTempDir("raft-transaction-policy"), false, secure, 300000, false, 0,
            [=](size_t index, akkaradb::engine::AkkEngineOptions& options) {
                if (index == 2) {
                    if (blobMismatch) { options.blob.thresholdBytes = 8192; }
                    else { options.transactions.maxJournalBytes = 1024; }
                }
            }};
        // Listener startup does not dial peers: the first election can begin
        // after this test's five-second observation budget. Begin confirms an
        // elected, committed compatible quorum before checking the rejection.
        auto tx = fixture.begin();
        AKK_CLUSTER_CHECK(waitUntil([&] { return fixture.engines[2]->stats().cluster.peerPolicyMismatchRejectsTotal > 0; }));
        const std::vector<uint8_t> value(4096, 0x62);
        tx.put(bytesOf("policy"), value); tx.end();
        AKK_CLUSTER_CHECK(fixture.engines[2]->stats().cluster.lastLogIndex == 0);
        AKK_CLUSTER_CHECK(engineGet(*fixture.engines[0], "policy") == std::optional{value});
        AKK_CLUSTER_CHECK(waitUntil([&] { return engineGet(*fixture.engines[1], "policy") == std::optional{value}; }));
        fixture.close();
        auto changed = fixture.options[0];
        if (blobMismatch) { changed.blob.thresholdBytes = 8192; }
        else { changed.transactions.maxJournalBytes = 1024; }
        bool rejected = false;
        try { (void)akkaradb::engine::AkkEngine::open(changed); }
        catch (const std::runtime_error& error) { rejected = std::string_view{error.what()}.find("transaction apply policy mismatch") != std::string_view::npos; }
        AKK_CLUSTER_CHECK(rejected);
        auto recovered = akkaradb::engine::AkkEngine::open(fixture.options[0]);
        AKK_CLUSTER_CHECK(recovered->stats().cluster.appliedIndex > 0);
        recovered->close();
    }
}

void testRaftIncomingForwardIsolation(bool secure) {
    const auto dir = makeTempDir(secure ? "raft-receive-secure" : "raft-receive-plain");
    const ClusterConfig config{{node(1, 31641, 31651), node(2, 31642, 31652), node(3, 31643, 31653)},
        ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 2000}};
    ClusterRuntimeOptions options;
    options.raftMaxIncomingForwardRequests = 2;
    options.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
    if (secure) {
        for (uint64_t id = 1; id <= 3; ++id) {
            const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
            options.secure.pinnedPeers.push_back({id, identity.publicKey});
        }
    }
    std::mutex gateMutex; std::condition_variable gateCv;
    unsigned entered = 0; bool released = false;
    const auto execute = [&](const ForwardRequest& request) {
        if (request.operation == ForwardOperation::GET) {
            std::unique_lock lock{gateMutex}; ++entered; gateCv.notify_all();
            if (!gateCv.wait_for(lock, std::chrono::seconds{15}, [&] { return released; })) { throw std::runtime_error("forward gate timed out"); }
        }
        ForwardResponse response; response.success = true; return response;
    };
    auto makePeer = [&](uint64_t id) {
        auto runtime = options;
        runtime.secure.identitySeedPath = dir / ("identity-" + std::to_string(id));
        return makeRuntime(dir / ("n" + std::to_string(id)), config, id, runtime, secure, execute);
    };
    auto n1 = makePeer(1), n2 = makePeer(2), n3 = makePeer(3);
    n1->runtime->start(); n2->runtime->start(); n3->runtime->start();
    RuntimeHarness* nodes[] = {n1.get(), n2.get(), n3.get()};
    ensurePreferredLeader(nodes, std::chrono::seconds{20});
    ForwardRequest request; request.operation = ForwardOperation::GET; request.timeoutMs = 10000; request.entries.push_back({{1}, {}});
    auto first = std::async(std::launch::async, [&] { return n2->runtime->forwardTo(1, request); });
    auto second = std::async(std::launch::async, [&] { return n3->runtime->forwardTo(1, request); });
    bool saturated = false, rejected = false, consensus = false, control = false;
    std::exception_ptr failure;
    try {
        {
            std::unique_lock lock{gateMutex};
            saturated = gateCv.wait_for(lock, std::chrono::seconds{5}, [&] { return entered == 2; });
        }
        if (saturated) {
            saturated = n1->runtime->raftStats().incomingForwardConnections == 2;
            try {
                const auto response = n2->runtime->forwardTo(1, request);
                rejected = !response.success && response.errorCode == ClusterRoutingCode::FORWARD_UNAVAILABLE;
            } catch (const ClusterRoutingError& error) { rejected = error.code == ClusterRoutingCode::FORWARD_UNAVAILABLE && !error.outcomeUnknown(); }
            n1->runtime->shipEntry(1, ReplOpType::PUT, bytesOf("consensus"), bytesOf("live"), 0, 1);
            consensus = n1->runtime->linearizableReadKey(bytesOf("consensus")).status == ReadStatus::FOUND;
            ForwardRequest placement; placement.operation = ForwardOperation::CLUSTER_ADMIN; placement.timeoutMs = 2000; placement.entries.push_back({{}, {}});
            control = n2->runtime->forwardTo(1, placement).success;
        }
    } catch (...) { failure = std::current_exception(); }
    // Always release before waiting on futures, including assertion/error paths.
    { std::lock_guard lock{gateMutex}; released = true; gateCv.notify_all(); }
    const auto firstResult = first.get(), secondResult = second.get();
    if (failure) { std::rethrow_exception(failure); }
    AKK_CLUSTER_CHECK(saturated && rejected && consensus && control && firstResult.success && secondResult.success);
    AKK_CLUSTER_CHECK(waitUntil([&] { return n1->runtime->raftStats().incomingForwardConnections == 0; }));
    AKK_CLUSTER_CHECK(n2->runtime->forwardTo(1, request).success);
    n3->runtime->close(); n2->runtime->close(); n1->runtime->close();
}

void testRaftForwardReceiveMemoryIsolation() {
    const auto dir = makeTempDir("raft-forward-memory");
    const ClusterConfig config{{node(1, 31661, 31671), node(2, 31662, 31672), node(3, 31663, 31673)},
        ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 2000}};
    ClusterRuntimeOptions options; options.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
    options.raftMaxReceiveMemoryBytes = options.raftMaxForwardReceiveMemoryBytes =
        3ull * (ReplFrameHeader::SIZE + ReplFrameHeader::MAX_RAFT_PAYLOAD_SIZE);
    auto n1 = makeRuntime(dir / "n1", config, 1, options), n2 = makeRuntime(dir / "n2", config, 2, options);
    n1->runtime->start(); n2->runtime->start();
    RuntimeHarness* nodes[] = {n1.get(), n2.get()};
    ensurePreferredLeader(nodes, std::chrono::seconds{20});
    RaftTestSocket stalled;
    openRaftTestPeer(stalled, 31671, 3, config, '3', options, 1);
    const auto wire = encodeFrame(ReplMsgType::FORWARD_REQUEST, std::vector<uint8_t>(ReplFrameHeader::MAX_RAFT_PAYLOAD_SIZE));
    AKK_CLUSTER_CHECK(stalled.send(std::span<const uint8_t>{wire}.first(ReplFrameHeader::SIZE)));
    AKK_CLUSTER_CHECK(waitUntil([&] { return n1->runtime->raftStats().forwardReceiveMemoryBytes >= ReplFrameHeader::MAX_RAFT_PAYLOAD_SIZE; }, std::chrono::milliseconds{500}));
    n1->runtime->shipEntry(1, ReplOpType::PUT, bytesOf("memory"), bytesOf("live"), 0, 1);
    AKK_CLUSTER_CHECK(n1->runtime->linearizableReadKey(bytesOf("memory")).status == ReadStatus::FOUND);
    AKK_CLUSTER_CHECK(n1->runtime->raftStats().forwardReceiveMemoryBytes >= ReplFrameHeader::MAX_RAFT_PAYLOAD_SIZE);
    stalled.close();
    AKK_CLUSTER_CHECK(waitUntil([&] { return n1->runtime->raftStats().forwardReceiveMemoryBytes == 0; }));
    // Forward channels cannot send votes or otherwise consume the consensus class.
    RaftTestSocket wrongClass; openRaftTestPeer(wrongClass, 31671, 3, config, '3', options, 1);
    AKK_CLUSTER_CHECK(wrongClass.send(encodeFrame(ReplMsgType::RAFT_PRE_VOTE, std::vector<uint8_t>(32))));
    std::array<uint8_t, 1> response{};
    AKK_CLUSTER_CHECK(!wrongClass.receive(response));
    n2->runtime->close(); n1->runtime->close();
}

void runRaftContractTests() {
    std::fprintf(stderr, "[raft-contract] group isolation PLAIN/SECURE and persistence\n");
    testRaftGroupIsolation(false); testRaftGroupIsolation(true); testRaftDurableGroupIsolation();
    std::fprintf(stderr, "[raft-contract] transaction apply policy PLAIN/SECURE and persistence\n");
    testRaftTransactionApplyPolicy(false); testRaftTransactionApplyPolicy(true);
    std::fprintf(stderr, "[raft-contract] forwarding saturation PLAIN/SECURE and receive memory\n");
    testRaftIncomingForwardIsolation(false); testRaftIncomingForwardIsolation(true); testRaftForwardReceiveMemoryIsolation();
}

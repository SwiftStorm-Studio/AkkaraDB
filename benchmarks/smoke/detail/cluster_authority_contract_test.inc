// Non-Raft admission and STRIPE authority lifetime regressions.
bool runtimePeerConnected(const ClusterRuntime& runtime, uint64_t nodeId) {
    const auto stats = runtime.raftStats();
    const auto peer = std::ranges::find(stats.peers, nodeId, &RaftPeerStats::nodeId);
    return peer != stats.peers.end() && peer->connected;
}

void testNonRaftReplicationContract(bool secure) {
    for (const auto mode : {ReplicationMode::MIRROR, ReplicationMode::PARTITIONED, ReplicationMode::STRIPE}) {
        const auto dir = makeTempDir("replication-contract-" + std::to_string(static_cast<unsigned>(mode)) + (secure ? "-secure" : "-plain"));
        const std::vector<NodeInfo> members{node(1, 25301, 25311), node(2, 25302, 25312), node(3, 25303, 25313)};
        const auto identity = testClusterId(9530);
        const auto primary = mode == ReplicationMode::MIRROR ? 1ull : 0ull;
        const StripeOptions layout{.dataShards = 1, .parityShards = 1};
        const ClusterConfig config{members, mode, {}, {}, {}, layout, primary, identity};
        ClusterRuntimeOptions options;
        options.startupRole = NodeStartupRole::PRIMARY;
        options.clusterGroupId = 9530; options.clusterGroupEpoch = 1;
        if (secure) {
            for (uint64_t id = 1; id <= 3; ++id) {
                const auto peer = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
                options.secure.pinnedPeers.push_back({id, peer.publicKey});
            }
            options.secure.identitySeedPath = dir / "identity-1";
        }
        auto source = makeRuntime(dir / "source", config, 1, options, secure);
        source->runtime->start();
        auto clientOptions = options;
        clientOptions.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
        clientOptions.secure.expectedPrimaryNodeId = 1;
        if (secure) { clientOptions.secure.identitySeedPath = dir / "identity-2"; }
        clientOptions.replicationClusterId = identity;
        clientOptions.replicationConfigFingerprint = config.replicationFingerprint();
        const auto makeClient = [&](const ClusterRuntimeOptions& contract, std::atomic<uint64_t>& applied) {
            auto client = ReplicationClient::create("127.0.0.1", 25311, 2, [] { return uint64_t{0}; }, {}, contract,
                mode == ReplicationMode::STRIPE);
            client->setApplyCallback([&applied](auto, auto, auto, auto, auto, auto) { ++applied; });
            client->setForceDurableCallback([] {});
            client->start();
            return client;
        };
        // Prove admission and application work on the same endpoint before
        // testing rejection, so an unavailable listener cannot pass the test.
        std::atomic<uint64_t> matchingApplied{0};
        auto matching = makeClient(clientOptions, matchingApplied);
        AKK_CLUSTER_CHECK(waitUntil([&] { return matching->connected(); }));
        const auto key = keyOwnedBy(config, 1, "contract-value");
        source->runtime->shipEntryTo(2, 1, ReplOpType::PUT, bytesOf(key), bytesOf("admitted"), 0, 1, false);
        AKK_CLUSTER_CHECK(waitUntil([&] { return matchingApplied.load() != 0; }));
        matching->close();
        AKK_CLUSTER_CHECK(waitUntil([&] { return !runtimePeerConnected(*source->runtime, 2); }));

        std::vector<ClusterConfig> mismatches;
        auto otherIdentity = identity; otherIdentity[0] ^= 1;
        mismatches.emplace_back(members, mode, AckPolicy{}, ConsistencyOptions{}, RaftOptions{}, layout, primary, otherIdentity);
        if (mode != ReplicationMode::STRIPE) {
            auto topology = members; topology.back().nodeId = 4;
            mismatches.emplace_back(topology, mode, AckPolicy{}, ConsistencyOptions{}, RaftOptions{}, layout, primary, identity);
            topology = members; ++topology.back().replPort;
            mismatches.emplace_back(topology, mode, AckPolicy{}, ConsistencyOptions{}, RaftOptions{}, layout, primary, identity);
            topology = members; topology.back().capabilities = DATA_BEARING;
            mismatches.emplace_back(topology, mode, AckPolicy{}, ConsistencyOptions{}, RaftOptions{}, layout, primary, identity);
        }
        AckPolicy otherAck; otherAck.stage = AckStage::DURABLE;
        mismatches.emplace_back(members, mode, otherAck, ConsistencyOptions{}, RaftOptions{}, layout, primary, identity);
        ConsistencyOptions otherConsistency; ++otherConsistency.ackTimeoutMs;
        mismatches.emplace_back(members, mode, AckPolicy{}, otherConsistency, RaftOptions{}, layout, primary, identity);
        const auto otherMode = mode == ReplicationMode::MIRROR ? ReplicationMode::PARTITIONED : ReplicationMode::MIRROR;
        mismatches.emplace_back(members, otherMode, AckPolicy{}, ConsistencyOptions{}, RaftOptions{}, layout,
            otherMode == ReplicationMode::MIRROR ? 1ull : 0ull, identity);
        if (mode == ReplicationMode::PARTITIONED) {
            mismatches.emplace_back(members, mode, AckPolicy{}, ConsistencyOptions{}, RaftOptions{}, layout,
                0, identity, PartitionOptions{.replicationFactor = 2});
        }
        if (mode == ReplicationMode::STRIPE) {
            mismatches.emplace_back(members, mode, AckPolicy{}, ConsistencyOptions{}, RaftOptions{},
                StripeOptions{.dataShards = 2, .parityShards = 1}, 0, identity);
        }
        std::vector<ClusterRuntimeOptions> rejectedContracts;
        for (const auto& mismatch : mismatches) {
            auto invalid = clientOptions;
            invalid.replicationClusterId = mismatch.clusterId();
            invalid.replicationConfigFingerprint = mismatch.replicationFingerprint();
            AKK_CLUSTER_CHECK(invalid.replicationConfigFingerprint != clientOptions.replicationConfigFingerprint);
            rejectedContracts.push_back(std::move(invalid));
        }
        // Cluster identity is independently mandatory, even when a low-level
        // caller accidentally reuses the correct configuration digest.
        auto wrongIdentity = clientOptions;
        wrongIdentity.replicationClusterId = otherIdentity;
        rejectedContracts.push_back(std::move(wrongIdentity));
        for (const auto& invalid : rejectedContracts) {
            std::atomic<uint64_t> rejectedApplied{0};
            auto rejected = makeClient(invalid, rejectedApplied);
            // Includes multiple reconnect attempts. None may receive buffered
            // data, persist membership, or count as an acknowledgement target.
            std::this_thread::sleep_for(std::chrono::milliseconds{500});
            AKK_CLUSTER_CHECK(!rejected->connected() && !runtimePeerConnected(*source->runtime, 2));
            AKK_CLUSTER_CHECK(rejectedApplied.load() == 0 && rejected->lastSuccessfulContactAtUs() == 0);
            rejected->close();
        }
        // Admission recovers immediately when the contract is corrected.
        matching = makeClient(clientOptions, matchingApplied);
        AKK_CLUSTER_CHECK(waitUntil([&] { return matching->connected(); }));
        matching->close(); source->runtime->close();
    }
}

void testReplicationFingerprintCanonicalPlacement() {
    const auto dir = makeTempDir("replication-fingerprint");
    auto members = std::vector{node(1, 25321, 25331), node(2, 25322, 25332), node(3, 25323, 25333), node(4, 25324, 25334)};
    const auto identity = testClusterId(9531);
    const ClusterConfig original{members, ReplicationMode::PARTITIONED, {}, {}, {}, {}, 0, identity};
    std::ranges::reverse(members);
    const ClusterConfig reordered{members, ReplicationMode::PARTITIONED, {}, {}, {}, {}, 0, identity};
    AKK_CLUSTER_CHECK(original.replicationFingerprint() == reordered.replicationFingerprint());
    ClusterConfig::save(dir / "cluster.akcc", original);
    AKK_CLUSTER_CHECK(original.replicationFingerprint() == ClusterConfig::load(dir / "cluster.akcc").replicationFingerprint());
    const StripeOptions mirrors{.dataShards = 2, .parityShards = 0, .copiesPerShard = 2};
    const ClusterConfig raid10{members, ReplicationMode::STRIPE, {}, {}, {}, mirrors, 0, identity};
    std::swap(members[1], members[2]);
    const ClusterConfig otherPairs{members, ReplicationMode::STRIPE, {}, {}, {}, mirrors, 0, identity};
    // Mutable STRIPE topology is committed by placement consensus, allowing
    // old and joining peers to transfer shards before atomic activation.
    AKK_CLUSTER_CHECK(raid10.replicationFingerprint() == otherPairs.replicationFingerprint());
    AKK_CLUSTER_CHECK(raid10.encode() != otherPairs.encode());
}

void testReplicaRejectsIncompatibleServerHello() {
    for (unsigned mismatch = 0; mismatch < 3; ++mismatch) {
        const auto dir = makeTempDir("server-hello-contract-" + std::to_string(mismatch));
        ClusterRuntimeOptions options;
        options.transportMode = TransportMode::PLAIN;
        options.clusterGroupId = 9534; options.clusterGroupEpoch = 1;
        options.replicationClusterId = testClusterId(9534);
        options.replicationConfigFingerprint = {1, 2, 3, 4};
        options.clusterMembershipPath = dir / "cluster.membership";
        RaftTestSocket listener;
        listener.listen(25371);
        std::atomic<uint64_t> applied{0};
        auto client = ReplicationClient::create("127.0.0.1", 25371, 2, [] { return uint64_t{0}; }, {}, options);
        client->setApplyCallback([&](auto, auto, auto, auto, auto, auto) { ++applied; });
        client->setForceDurableCallback([] {});
        auto peer = std::async(std::launch::async, [&] {
            RaftTestSocket connection{::accept(listener.handle, nullptr, nullptr)};
            AKK_CLUSTER_CHECK(connection.handle != RaftTestSocket::INVALID);
            const auto frame = connection.receiveFrame();
            ClientHello hello;
            AKK_CLUSTER_CHECK(frame.type == ReplMsgType::CLIENT_HELLO && decodeClientHello(frame.payload, hello));
            AKK_CLUSTER_CHECK(hello.clusterId == options.replicationClusterId &&
                hello.configFingerprint == options.replicationConfigFingerprint);
            ServerHello response{.nodeId = 1, .groupId = 9534, .groupEpoch = 1,
                .clusterId = hello.clusterId, .configFingerprint = hello.configFingerprint};
            if (mismatch == 0) { response.clusterId[0] ^= 1; }
            if (mismatch == 1) { response.configFingerprint[0] ^= 1; }
            auto wire = encodeServerHello(response);
            const ReplEntry entry{.seq = 1, .sourceNodeId = 1, .key = {'k'}, .value = {'v'}};
            const auto entryWire = encodeEntry(entry);
            wire.insert(wire.end(), entryWire.begin(), entryWire.end());
            AKK_CLUSTER_CHECK(connection.send(wire));
            if (mismatch == 2) {
                AKK_CLUSTER_CHECK(waitUntil([&] { return applied.load() == 1; }));
            }
            else {
                // A rejected hello must close this connection. If the client
                // accepts it instead, receipt of an ACK makes the test fail.
                std::array<uint8_t, 1> reply{};
                AKK_CLUSTER_CHECK(!connection.receive(reply));
            }
        });
        client->start();
        peer.get();
        client->close();
        AKK_CLUSTER_CHECK(applied.load() == (mismatch == 2 ? 1u : 0u));
        AKK_CLUSTER_CHECK(std::filesystem::exists(options.clusterMembershipPath) == (mismatch == 2));
        AKK_CLUSTER_CHECK((client->lastSuccessfulContactAtUs() != 0) == (mismatch == 2));
    }
}

void testStripeFailoverLeaseRecovery(bool secure) {
    const auto dir = makeTempDir(secure ? "stripe-lease-recovery-secure" : "stripe-lease-recovery-plain");
    const ClusterConfig config{{node(1, 25341, 25351), node(2, 25342, 25352),
        node(3, 25343, 25353, DATA_AND_COORDINATOR | STRIPE_FAILOVER_ELIGIBLE)},
        ReplicationMode::STRIPE, {}, ConsistencyOptions{.ackTimeoutMs = 300}, {},
        StripeOptions{.dataShards = 1, .parityShards = 1}, 0, testClusterId(9532)};
    ClusterRuntimeOptions options;
    options.clusterGroupId = 9532; options.clusterGroupEpoch = 1;
    if (secure) {
        for (uint64_t id = 1; id <= 3; ++id) {
            const auto peer = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
            options.secure.pinnedPeers.push_back({id, peer.publicKey});
        }
    }
    const auto make = [&](uint64_t id) {
        auto peerOptions = options;
        if (secure) { peerOptions.secure.identitySeedPath = dir / ("identity-" + std::to_string(id)); }
        auto harness = makeRuntime(dir / ("n" + std::to_string(id)), config, id, peerOptions, secure);
        harness->runtime->start();
        return harness;
    };
    const auto key = keyOwnedBy(config, 1, "lease-recovery");
    const auto otherKey = keyOwnedBy(config, 1, "lease-other");
    auto leader = make(2), spare = make(3);
    AKK_CLUSTER_CHECK(waitUntil([&] { return leader->runtime->stripeMetadataLeaderNodeId() == 2 &&
        runtimePeerConnected(*leader->runtime, 3) && runtimePeerConnected(*spare->runtime, 2); }));
    const auto term = leader->runtime->stripeMetadataRaftStats().currentTerm;
    const auto abandoned = spare->runtime->acquireStripeOperation(bytesOf(key), 1);
    // Another key verifies that owner handoff collects all orphan leases,
    // rather than only the key named in the next request.
    const auto abandonedOther = spare->runtime->acquireStripeOperation(bytesOf(otherKey), 1);
    spare->runtime->close(); spare.reset();
    spare = make(3);
    AKK_CLUSTER_CHECK(waitUntil([&] { return runtimePeerConnected(*leader->runtime, 3) && runtimePeerConnected(*spare->runtime, 2); }));
    const auto recovered = spare->runtime->acquireStripeOperation(bytesOf(key), 1);
    AKK_CLUSTER_CHECK(recovered.fenceToken != abandoned.fenceToken);
    AKK_CLUSTER_CHECK(leader->runtime->stripeMetadataRaftStats().currentTerm == term);
    // Simulate an abandoned/lost grant on a still-live connection.
    const auto renewed = spare->runtime->acquireStripeOperation(bytesOf(key), 1);
    AKK_CLUSTER_CHECK(renewed.fenceToken != recovered.fenceToken);
    const auto metadataFor = [](const StripeOperationLease& lease) {
        std::vector<uint8_t> metadata(62, 0);
        std::memcpy(metadata.data(), "AKSM1", 5);
        const auto integer = [&](size_t offset, uint64_t value) {
            for (size_t i = 0; i < 8; ++i) { metadata[offset + i] = static_cast<uint8_t>(value >> (8 * i)); }
        };
        integer(6, lease.fenceToken); integer(14, lease.ownerNodeId);
        integer(22, lease.authorityNodeId); integer(30, lease.fenceToken);
        return metadata;
    };
    bool rejected = false;
    try { spare->runtime->commitStripeMetadata(recovered, bytesOf(key), metadataFor(recovered)); }
    catch (const std::runtime_error&) { rejected = true; }
    AKK_CLUSTER_CHECK(rejected);
    spare->runtime->commitStripeMetadata(renewed, bytesOf(key), metadataFor(renewed));
    AKK_CLUSTER_CHECK(spare->runtime->readStripeMetadata(bytesOf(key), 1) == metadataFor(renewed));
    rejected = false;
    try { spare->runtime->commitStripeMetadata(abandonedOther, bytesOf(otherKey), metadataFor(abandonedOther)); }
    catch (const std::runtime_error&) { rejected = true; }
    AKK_CLUSTER_CHECK(rejected);

    const auto handoff = spare->runtime->acquireStripeOperation(bytesOf(otherKey), 1);
    spare->runtime->close(); spare.reset();
    auto owner = make(1);
    AKK_CLUSTER_CHECK(waitUntil([&] { return runtimePeerConnected(*leader->runtime, 1) && runtimePeerConnected(*owner->runtime, 2); }));
    const auto returned = owner->runtime->acquireStripeOperation(bytesOf(key), 1);
    AKK_CLUSTER_CHECK(returned.authorityNodeId == 1);
    owner->runtime->releaseStripeOperation(returned, bytesOf(key));
    const auto returnedOther = owner->runtime->acquireStripeOperation(bytesOf(otherKey), 1);
    AKK_CLUSTER_CHECK(returnedOther.authorityNodeId == 1 && returnedOther.fenceToken != handoff.fenceToken);
    AKK_CLUSTER_CHECK(leader->runtime->stripeMetadataRaftStats().currentTerm == term);
    owner->runtime->releaseStripeOperation(returnedOther, bytesOf(otherKey));
    owner->runtime->close(); leader->runtime->close();
}

void runClusterAuthorityContractTests() {
    testReplHandshakeCarriesClusterGroupIdentity();
    testReplicationFingerprintCanonicalPlacement();
    testReplicaRejectsIncompatibleServerHello();
    for (bool secure : {false, true}) {
        testNonRaftReplicationContract(secure);
        testStripeFailoverLeaseRecovery(secure);
    }
}

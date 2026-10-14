/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
void testStripeOwnerReadRequiresQuorum(bool secure) {
    using namespace akkaradb::engine;
    const auto dir = makeTempDir(secure ? "stripe-owner-read-quorum-secure" : "stripe-owner-read-quorum-plain");
    const ClusterConfig config{{node(1, 34401, 34411), node(2, 34402, 34412), node(3, 34403, 34413)},
        ReplicationMode::STRIPE, {}, {.ackTimeoutMs = 500}, {}, {.dataShards = 2, .parityShards = 1}, 0, testClusterId(34400)};
    std::array<AkkEngineOptions, 3> options;
    std::array<std::unique_ptr<AkkEngine>, 3> engines;
    std::vector<ClusterPeerPublicKeyPin> pins;
    if (secure) {
        for (uint64_t id = 1; id <= 3; ++id) {
            const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
            pins.push_back({id, identity.publicKey});
        }
    }
    for (size_t i = 0; i < engines.size(); ++i) {
        auto& opt = options[i]; opt.paths.dataDir = dir / std::to_string(i + 1);
        opt.components.blobEnabled = opt.components.sstEnabled = opt.components.manifestEnabled = false;
        opt.components.clusterEnabled = true; opt.cluster.config = config;
        auto& runtime = opt.cluster.runtime;
        runtime.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
        runtime.clusterGroupId = 34400; runtime.routingMode = ClusterRoutingMode::FORWARD;
        runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
        runtime.raftHeartbeatIntervalMs = 30; runtime.stripeAutoRebuild = false;
        runtime.secure.pinnedPeers = pins; runtime.secure.identitySeedPath = dir / ("identity-" + std::to_string(i + 1));
        writeNodeIdFile(opt.paths.dataDir / "node.id", i + 1); engines[i] = AkkEngine::open(opt);
    }
    uint64_t leader = 0;
    AKK_CLUSTER_CHECK(waitUntil([&] {
        leader = engines[0]->stats().cluster.stripeMetadataLeaderNodeId;
        return leader && engines[leader - 1]->stats().cluster.stripeMetadataCommitIndex != 0;
    }, std::chrono::seconds{15}));
    const auto key = keyOwnedBy(config, leader, "missing-before-quorum-loss");
    for (const auto& engine : engines) { AKK_CLUSTER_CHECK(!engine->get(bytesOf(key))); }
    for (size_t i = 0; i < engines.size(); ++i) {
        if (i + 1 != leader) { engines[i]->close(); engines[i].reset(); }
    }
    // The isolated node still considers itself leader; role alone cannot prove
    // that its metadata (including absence) is the latest committed state.
    for (bool batch : {false, true}) {
        bool rejected = false;
        try {
            if (batch) { const std::array keys{bytesOf(key)}; (void)engines[leader - 1]->getBatch(keys); }
            else { (void)engines[leader - 1]->get(bytesOf(key)); }
        } catch (const std::exception&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected);
    }
    for (size_t i = 0; i < engines.size(); ++i) {
        if (!engines[i]) { engines[i] = AkkEngine::open(options[i]); }
    }
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { engines[leader - 1]->put(bytesOf(key), bytesOf("visible-after-recovery")); return true; }
        catch (...) { return false; }
    }, std::chrono::seconds{20}));
    for (const auto& engine : engines) {
        const auto value = engine->get(bytesOf(key));
        AKK_CLUSTER_CHECK(value && textOf(*value) == "visible-after-recovery");
    }
    for (auto& engine : engines) { engine->close(); }
}

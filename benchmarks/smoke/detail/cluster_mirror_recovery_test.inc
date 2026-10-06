/*
 * Copyright (C) 2026 Swift Storm Studio
 * This Source Code Form is subject to the Mozilla Public License, v. 2.0.
 * https://mozilla.org/MPL/2.0/
 */

class TestMirrorRecoveryProvider final : public IMirrorRecoveryProvider {
public:
    std::atomic<unsigned> calls{0};
    std::function<std::optional<MirrorRecoveryProof>(const MirrorRecoveryRequest&, std::stop_token)> handler;
    std::optional<MirrorRecoveryProof> recover(const MirrorRecoveryRequest& request, std::stop_token cancelled) override {
        ++calls;
        AKK_CLUSTER_CHECK(request.groupId != 0 && request.operationId != 0 && request.previousPrimaryNodeId == 1 &&
            request.previousGroupEpoch == 1 && request.candidateNodeId == 1 && request.action == MirrorRecoveryAction::RESUME_PRIMARY);
        return handler(request, cancelled);
    }
};

void testMirrorRecoveryValidation() {
    const ClusterConfig config{{node(1, 25101, 25111), node(2, 25102, 25112), node(3, 25103, 25113)},
        ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::ASYNC}, {}, {}, 1};
    ClusterRuntimeOptions options;
    options.transportMode = TransportMode::PLAIN;
    options.clusterGroupId = 25100;
    options.startupRole = NodeStartupRole::PRIMARY;
    options.mirrorFencing.mode = MirrorFencingMode::QUORUM_FENCED;
    options.mirrorFencing.authorityNodes = {node(1, 25201, 25211), node(2, 25202, 25212), node(3, 25203, 25213)};
    const auto rejects = [&](const ClusterRuntimeOptions& candidate) {
        bool rejected = false;
        try { config.validateRuntime(1, candidate); } catch (const std::invalid_argument&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected);
    };
    config.validateRuntime(1, options);
    options.mirrorRecovery.mode = MirrorRecoveryMode::MANUAL; rejects(options);
    options.mirrorRecovery.provider = std::make_shared<TestMirrorRecoveryProvider>();
    config.validateRuntime(1, options);
    options.mirrorRecovery.mode = MirrorRecoveryMode::AUTOMATIC; config.validateRuntime(1, options);
    options.mirrorRecovery.mode = MirrorRecoveryMode::BLOCK; rejects(options);
    options.mirrorRecovery.mode = static_cast<MirrorRecoveryMode>(255); rejects(options);
    options.mirrorRecovery.mode = MirrorRecoveryMode::MANUAL;
    options.mirrorRecovery.retryIntervalMs = 0; rejects(options);
    options.mirrorRecovery.retryIntervalMs = 1000;
    options.mirrorFencing.mode = MirrorFencingMode::STATIC; options.mirrorFencing.authorityNodes.clear(); rejects(options);
    bool rejected = false;
    try { (void)MirrorFencingRecoveryProvider{nullptr}; } catch (const std::invalid_argument&) { rejected = true; }
    AKK_CLUSTER_CHECK(rejected);
}

void testMirrorManualRecovery(ConsistencyMode consistency) {
    const auto dir = makeTempDir("mirror-manual-recovery");
    const ClusterConfig config{{node(1, 25101, 25111), node(2, 25102, 25112), node(3, 25103, 25113)},
        ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = consistency, .ackTimeoutMs = 1000}, {}, {}, 1};
    const auto provider = std::make_shared<TestMirrorRecoveryProvider>();
    bool failDurability = false;
    const auto makePeer = [&](uint64_t id) {
        ClusterRuntimeOptions options;
        options.clusterGroupId = 25100;
        options.startupRole = id == 1 ? NodeStartupRole::PRIMARY : NodeStartupRole::REPLICA;
        options.mirrorFencing.mode = MirrorFencingMode::QUORUM_FENCED;
        options.mirrorFencing.authorityNodes = {node(1, 25201, 25211), node(2, 25202, 25212), node(3, 25203, 25213)};
        if (id == 1) { options.mirrorRecovery = {MirrorRecoveryMode::MANUAL, provider}; }
        return makeRuntime(dir / ("n" + std::to_string(id)), config, id, options, false, {}, [&](ClusterEngineCallbacks& callbacks) {
            const auto durable = callbacks.forceDurable;
            callbacks.forceDurable = [&, id, durable] {
                if (id == 1 && failDurability) { throw std::runtime_error("injected recovery durability failure"); }
                durable();
            };
        });
    };
    auto n1 = makePeer(1), n2 = makePeer(2), n3 = makePeer(3);
    n1->runtime->start(); n2->runtime->start(); n3->runtime->start();
    AKK_CLUSTER_CHECK(waitUntil([&] {
        const auto stats = n1->runtime->raftStats();
        return stats.mirrorAuthorityLeaderNodeId == 1 && stats.mirrorAuthorityPrimaryNodeId == 1 && stats.mirrorAuthorityEpoch == 1;
    }, std::chrono::seconds{30}));
    AKK_CLUSTER_CHECK(!n1->runtime->recoverMirrorWrite() && provider->calls == 0);
    bool failed = false;
    try {
        n1->runtime->executePrimaryWrite([&] {
            n1->setState(1, bytesOf("partial"), bytesOf("retained"));
            throw std::runtime_error("interrupted write");
        });
    } catch (const std::runtime_error&) { failed = true; }
    AKK_CLUSTER_CHECK(failed && n1->runtime->raftStats().mirrorWritePending && provider->calls == 0);
    provider->handler = [](const auto&, auto) -> std::optional<MirrorRecoveryProof> { return std::nullopt; };
    AKK_CLUSTER_CHECK(!n1->runtime->recoverMirrorWrite() && n1->runtime->raftStats().mirrorWritePending);
    // An otherwise valid provider result must match every part of the request.
    for (unsigned field = 0; field < 8; ++field) {
        provider->handler = [field](const auto& request, auto) -> std::optional<MirrorRecoveryProof> {
            auto proof = MirrorRecoveryProof{request};
            switch (field) {
                case 0: ++proof.request.clusterId[0]; break;
                case 1: ++proof.request.groupId; break;
                case 2: ++proof.request.previousPrimaryNodeId; break;
                case 3: ++proof.request.previousGroupEpoch; break;
                case 4: ++proof.request.operationId; break;
                case 5: ++proof.request.candidateNodeId; break;
                case 6: ++proof.request.expectedDurableSeq; break;
                case 7: proof.request.action = MirrorRecoveryAction::PROMOTE_PRIMARY; break;
            }
            return proof;
        };
        failed = false;
        try { (void)n1->runtime->recoverMirrorWrite(); } catch (const std::runtime_error&) { failed = true; }
        AKK_CLUSTER_CHECK(failed && n1->runtime->raftStats().mirrorWritePending && n1->durableCount == 0);
    }
    provider->handler = [](const auto&, auto) -> std::optional<MirrorRecoveryProof> { throw std::runtime_error("uncertain recovery"); };
    failed = false;
    try { (void)n1->runtime->recoverMirrorWrite(); } catch (const std::runtime_error&) { failed = true; }
    AKK_CLUSTER_CHECK(failed && n1->runtime->raftStats().mirrorWritePending);
    provider->handler = [](const auto& request, auto) -> std::optional<MirrorRecoveryProof> { return MirrorRecoveryProof{request}; };
    failDurability = true; failed = false;
    try { (void)n1->runtime->recoverMirrorWrite(); } catch (const std::runtime_error&) { failed = true; }
    AKK_CLUSTER_CHECK(failed && n1->runtime->raftStats().mirrorWritePending &&
        !std::filesystem::exists(dir / "n1" / "mirror-authority" / "completed"));
    failDurability = false;
    AKK_CLUSTER_CHECK(n1->runtime->recoverMirrorWrite());
    AKK_CLUSTER_CHECK(!n1->runtime->raftStats().mirrorWritePending && n1->hasState(1, "partial", "retained"));
    AKK_CLUSTER_CHECK(n1->durableCount == 1 && std::filesystem::exists(dir / "n1" / "mirror-authority" / "completed"));
    const auto calls = provider->calls.load();
    AKK_CLUSTER_CHECK(!n1->runtime->recoverMirrorWrite() && provider->calls == calls);
    n1->runtime->executePrimaryWrite([&] { n1->setState(2, bytesOf("next"), bytesOf("accepted")); });
    AKK_CLUSTER_CHECK(n1->hasState(2, "next", "accepted"));
    n3->runtime->close(); n2->runtime->close(); n1->runtime->close();
}

void testMirrorAutomaticRecovery() {
    const auto dir = makeTempDir("mirror-automatic-recovery");
    const ClusterConfig config{{node(1, 25301, 25311), node(2, 25302, 25312), node(3, 25303, 25313)},
        ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::ASYNC, .ackTimeoutMs = 1000}, {}, {}, 1};
    const auto provider = std::make_shared<TestMirrorRecoveryProvider>();
    std::atomic<unsigned> phase{0};
    std::atomic<bool> entered{false}, cancelled{false};
    provider->handler = [&](const auto& request, std::stop_token stop) -> std::optional<MirrorRecoveryProof> {
        if (phase == 0) { return std::nullopt; }
        if (phase == 1) { throw std::logic_error("temporary provider failure"); }
        if (phase == 3) {
            entered = true;
            AKK_CLUSTER_CHECK(waitUntil([&] { return stop.stop_requested(); }));
            cancelled = true;
        }
        return MirrorRecoveryProof{request};
    };
    auto mode = MirrorRecoveryMode::AUTOMATIC;
    const auto makePeer = [&](uint64_t id) {
        ClusterRuntimeOptions options;
        options.clusterGroupId = 25300;
        options.startupRole = id == 1 ? NodeStartupRole::PRIMARY : NodeStartupRole::REPLICA;
        options.mirrorFencing.mode = MirrorFencingMode::QUORUM_FENCED;
        options.mirrorFencing.authorityNodes = {node(1, 25401, 25411), node(2, 25402, 25412), node(3, 25403, 25413)};
        if (id == 1) { options.mirrorRecovery = {mode, provider, 100}; }
        return makeRuntime(dir / ("n" + std::to_string(id)), config, id, options);
    };
    auto n1 = makePeer(1), n2 = makePeer(2), n3 = makePeer(3);
    n1->runtime->start(); n2->runtime->start(); n3->runtime->start();
    AKK_CLUSTER_CHECK(waitUntil([&] { return n1->runtime->raftStats().mirrorAuthorityLeaderNodeId == 1; }, std::chrono::seconds{30}));
    const auto interrupt = [&](uint64_t seq) {
        bool failed = false;
        try { n1->runtime->executePrimaryWrite([&] { n1->setState(seq, bytesOf("partial"), bytesOf("saved")); throw std::runtime_error("interrupted"); }); }
        catch (const std::runtime_error&) { failed = true; }
        AKK_CLUSTER_CHECK(failed);
    };
    interrupt(1);
    AKK_CLUSTER_CHECK(waitUntil([&] { return provider->calls >= 2; }));
    AKK_CLUSTER_CHECK(n1->runtime->raftStats().mirrorWritePending);
    phase = 1;
    const auto calls = provider->calls.load();
    AKK_CLUSTER_CHECK(waitUntil([&] { return provider->calls >= calls + 2; }));
    AKK_CLUSTER_CHECK(n1->runtime->raftStats().mirrorWritePending);
    phase = 2;
    AKK_CLUSTER_CHECK(waitUntil([&] { return !n1->runtime->raftStats().mirrorWritePending; }));
    AKK_CLUSTER_CHECK(n1->hasState(1, "partial", "saved"));
    phase = 3; interrupt(2);
    AKK_CLUSTER_CHECK(waitUntil([&] { return entered.load(); }));
    const auto closing = std::chrono::steady_clock::now();
    n1->runtime->close();
    AKK_CLUSTER_CHECK(cancelled && std::chrono::steady_clock::now() - closing < std::chrono::seconds{5});
    AKK_CLUSTER_CHECK(n1->runtime->raftStats().mirrorWritePending);
    n1.reset(); mode = MirrorRecoveryMode::MANUAL; phase = 2;
    n1 = makePeer(1); n1->runtime->start();
    AKK_CLUSTER_CHECK(waitUntil([&] { return n1->runtime->raftStats().mirrorAuthorityLeaderNodeId == 1; }, std::chrono::seconds{30}));
    AKK_CLUSTER_CHECK(n1->runtime->raftStats().mirrorWritePending);
    AKK_CLUSTER_CHECK(n1->runtime->recoverMirrorWrite() && n1->hasState(2, "partial", "saved"));
    n3->runtime->close(); n2->runtime->close(); n1->runtime->close();
}

void testMirrorRecoveryEngineApi() {
    const auto dir = makeTempDir("mirror-recovery-engine-api");
    const ClusterConfig config{{node(1, 25501, 25511), node(2, 25502, 25512), node(3, 25503, 25513, COORDINATOR_ELIGIBLE)},
        ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::ASYNC, .ackTimeoutMs = 1000}, {}, {}, 1};
    const auto provider = std::make_shared<TestMirrorRecoveryProvider>();
    std::atomic<bool> entered{false};
    provider->handler = [&](const auto& request, std::stop_token stop) -> std::optional<MirrorRecoveryProof> {
        entered = true;
        AKK_CLUSTER_CHECK(waitUntil([&] { return stop.stop_requested(); }));
        return MirrorRecoveryProof{request};
    };
    const auto makeOptions = [&](uint64_t id) {
        akkaradb::engine::AkkEngineOptions options;
        options.paths.dataDir = dir / ("n" + std::to_string(id));
        options.components.clusterEnabled = true;
        options.components.blobEnabled = false;
        options.cluster.config = config;
        auto& runtime = options.cluster.runtime;
        runtime.transportMode = TransportMode::PLAIN;
        runtime.clusterGroupId = 25500;
        runtime.startupRole = id == 1 ? NodeStartupRole::PRIMARY : NodeStartupRole::REPLICA;
        runtime.mirrorFencing.mode = MirrorFencingMode::QUORUM_FENCED;
        runtime.mirrorFencing.authorityNodes = {node(1, 25601, 25611), node(2, 25602, 25612), node(3, 25603, 25613)};
        if (id == 1) { runtime.mirrorRecovery = {MirrorRecoveryMode::MANUAL, provider}; }
        writeNodeIdFile(options.paths.dataDir / "node.id", id);
        return options;
    };
    auto n1 = akkaradb::engine::AkkEngine::open(makeOptions(1));
    auto n2 = akkaradb::engine::AkkEngine::open(makeOptions(2));
    auto n3 = akkaradb::engine::AkkEngine::open(makeOptions(3));
    AKK_CLUSTER_CHECK(waitUntil([&] {
        const auto stats = n1->stats().cluster;
        return stats.mirrorAuthorityLeaderNodeId == 1 && stats.mirrorAuthorityPrimaryNodeId == 1 && stats.mirrorAuthorityEpoch == 1;
    }, std::chrono::seconds{30}));
    AKK_CLUSTER_CHECK(!n1->recoverMirrorWrite());
    // WAL serialization rejects this synchronously, after authority BEGIN.
    const std::vector<uint8_t> oversizedKey(65536, 'x');
    bool failed = false;
    try { n1->put(oversizedKey, bytesOf("invalid")); } catch (const std::invalid_argument&) { failed = true; }
    AKK_CLUSTER_CHECK(failed && n1->stats().cluster.mirrorWritePending);
    auto recovery = std::async(std::launch::async, [&] {
        try { (void)n1->recoverMirrorWrite(); return false; } catch (const std::runtime_error&) { return true; }
    });
    AKK_CLUSTER_CHECK(waitUntil([&] { return entered.load(); }));
    n1->close();
    AKK_CLUSTER_CHECK(recovery.get()); // Native close cancels before waiting on the API operation guard.
    provider->handler = [](const auto& request, auto) -> std::optional<MirrorRecoveryProof> { return MirrorRecoveryProof{request}; };
    n1 = akkaradb::engine::AkkEngine::open(makeOptions(1));
    AKK_CLUSTER_CHECK(waitUntil([&] { return n1->stats().cluster.mirrorAuthorityLeaderNodeId == 1; }, std::chrono::seconds{30}));
    AKK_CLUSTER_CHECK(n1->stats().cluster.mirrorWritePending && n1->recoverMirrorWrite());
    AKK_CLUSTER_CHECK(!n1->stats().cluster.mirrorWritePending);
    n3->close(); n2->close(); n1->close();
}

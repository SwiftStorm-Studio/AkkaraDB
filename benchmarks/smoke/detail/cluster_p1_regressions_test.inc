/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */

void testReadKeySpaceFraming() {
    using namespace akkaradb::engine::cluster::detail;
    auto budget = std::make_shared<TransferBudget>(ReplicationTransferOptions{});
    for (const auto space : {ReadKeySpace::PUBLIC, ReadKeySpace::LOCAL_STORAGE}) {
        ReadRequest request{.requestId = 17, .snapshotSeq = 29, .keySpace = space, .key = {0, 'A', 'K', 'S', 'M', '1', 255}};
        DecodedFrame frame;
        AKK_CLUSTER_CHECK(decodeFrame(encodeReadRequest(request), frame));
        ReadRequest decoded;
        AKK_CLUSTER_CHECK(decodeReadRequest(frame.payload, decoded));
        AKK_CLUSTER_CHECK(decoded.requestId == request.requestId && decoded.snapshotSeq == request.snapshotSeq &&
            decoded.keySpace == space && decoded.key == request.key);
        const auto transferred = readRequestMessage(request.requestId, request.snapshotSeq, request.key, budget, space);
        // Chunk-transfer and direct framing must carry the same namespace and bytes.
        AKK_CLUSTER_CHECK(std::ranges::equal(transferred->payload, frame.payload));
        auto invalid = frame.payload;
        invalid[20] = 2;
        AKK_CLUSTER_CHECK(!decodeReadRequest(invalid, decoded));
        invalid = frame.payload; invalid.erase(invalid.begin() + 20);
        AKK_CLUSTER_CHECK(!decodeReadRequest(invalid, decoded));
        frame.payload.push_back(0);
        AKK_CLUSTER_CHECK(!decodeReadRequest(frame.payload, decoded));
        request.keySpace = static_cast<ReadKeySpace>(2);
        bool rejected = false;
        try { (void)encodeReadRequest(request); } catch (const std::invalid_argument&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected);
        rejected = false;
        try { (void)readRequestMessage(request.requestId, request.snapshotSeq, request.key, budget, request.keySpace); }
        catch (const std::invalid_argument&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected);
    }
    // Old payloads must be rejected even when their key bytes look like a namespace.
    for (const std::vector<uint8_t>& key : {std::vector<uint8_t>{}, std::vector<uint8_t>{0}, std::vector<uint8_t>{1}}) {
        DecodedFrame frame;
        AKK_CLUSTER_CHECK(decodeFrame(encodeReadRequest({.requestId = 1, .snapshotSeq = 0, .keySpace = ReadKeySpace::PUBLIC, .key = key}), frame));
        frame.payload.erase(frame.payload.begin() + 20);
        ReadRequest decoded;
        AKK_CLUSTER_CHECK(!decodeReadRequest(frame.payload, decoded));
    }
}

void testStripePublicKeyNamespaces(bool secure) {
    using Engine = akkaradb::engine::AkkEngine;
    const auto dir = makeTempDir(secure ? "stripe-public-namespace-secure" : "stripe-public-namespace-plain");
    const ClusterConfig config{{node(1, 30901, 30911), node(2, 30902, 30912), node(3, 30903, 30913)},
        ReplicationMode::STRIPE, {}, {.ackTimeoutMs = 2000}, {}, {.dataShards = 1, .parityShards = 1},
        0, testClusterId(30900)};
    std::array<akkaradb::engine::AkkEngineOptions, 3> options;
    std::array<std::unique_ptr<Engine>, 3> engines;
    std::vector<ClusterPeerPublicKeyPin> pins;
    if (secure) {
        for (uint64_t id = 1; id <= 3; ++id) {
            const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
            pins.push_back({id, identity.publicKey});
        }
    }
    for (size_t i = 0; i < options.size(); ++i) {
        auto& opt = options[i]; opt.paths.dataDir = dir / std::to_string(i + 1);
        opt.components.clusterEnabled = true; opt.components.blobEnabled = false;
        opt.cluster.config = config; auto& runtime = opt.cluster.runtime;
        runtime.clusterGroupId = 30900; runtime.routingMode = ClusterRoutingMode::FORWARD;
        runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE; runtime.stripeAutoRebuild = false;
        runtime.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
        runtime.secure.pinnedPeers = pins; runtime.secure.identitySeedPath = dir / ("identity-" + std::to_string(i + 1));
        writeNodeIdFile(opt.paths.dataDir / "node.id", i + 1);
    }
    const auto open = [&] { for (size_t i = 0; i < engines.size(); ++i) { engines[i] = Engine::open(options[i]); } };
    const auto close = [&] { for (auto& engine : engines) { engine->close(); engine.reset(); } };
    std::map<std::vector<uint8_t>, std::vector<uint8_t>> expected;
    const std::vector<uint8_t> emptyKey;
    expected[emptyKey] = {'e', 'm', 'p', 't', 'y'};
    expected[{'x'}] = {'o', 'r', 'd', 'i', 'n', 'a', 'r', 'y'};
    for (const std::string prefix : {"AKSM1", "AKSS1", "AKST1", "AKSH2"}) {
        std::vector<uint8_t> key{0}; key.insert(key.end(), prefix.begin(), prefix.end());
        for (const std::vector<uint8_t>& suffix : {std::vector<uint8_t>{}, std::vector<uint8_t>{'x'}, std::vector<uint8_t>{0, 255}}) {
            auto publicKey = key; publicKey.insert(publicKey.end(), suffix.begin(), suffix.end());
            expected[publicKey] = publicKey; // Distinct binary values expose any metadata leakage.
        }
    }
    open();
    AKK_CLUSTER_CHECK(waitUntil([&] { try { return engines[0]->count() == 0; } catch (...) { return false; } }, std::chrono::seconds{20}));
    for (const auto& [key, value] : expected) { engines[0]->put(key, value); }
    ClusterRouter router{config};
    const auto verify = [&](bool ownerOnly) {
        for (const auto& [key, value] : expected) {
            const auto owner = router.writeTargets(key).front().nodeId - 1;
            for (size_t i = 0; i < engines.size(); ++i) {
                if (ownerOnly && i != owner) {
                    bool rejected = false;
                    try { (void)engines[i]->get(key); } catch (const std::runtime_error&) { rejected = true; }
                    AKK_CLUSTER_CHECK(rejected);
                    continue;
                }
                AKK_CLUSTER_CHECK(engines[i]->get(key) == std::optional{value} && engines[i]->exists(key));
                std::vector<uint8_t> into;
                AKK_CLUSTER_CHECK(engines[i]->getInto(key, into) && into == value);
                akkaradb::core::BufferArena arena; std::span<const uint8_t> view;
                AKK_CLUSTER_CHECK(engines[i]->getIntoArena(key, arena, view) && std::ranges::equal(view, value));
            }
        }
        if (!ownerOnly) {
            AKK_CLUSTER_CHECK(engines[0]->count() == expected.size());
            akkaradb::core::BufferArena arena;
            std::map<std::vector<uint8_t>, std::vector<uint8_t>> actual;
            for (const auto& record : engines[0]->scan(arena)) {
                actual.emplace(std::vector<uint8_t>{record.key.begin(), record.key.end()},
                    std::vector<uint8_t>{record.value.begin(), record.value.end()});
            }
            AKK_CLUSTER_CHECK(actual == expected);
        }
    };
    verify(false);
    expected[emptyKey] = {'u', 'p', 'd', 'a', 't', 'e', 'd'};
    engines[1]->put(emptyKey, expected.at(emptyKey));
    verify(false);
    engines[2]->remove(emptyKey); expected.erase(emptyKey);
    for (const auto& engine : engines) { AKK_CLUSTER_CHECK(!engine->get(emptyKey) && !engine->exists(emptyKey)); }
    verify(false);
    expected[emptyKey] = {'r', 'e', 'c', 'r', 'e', 'a', 't', 'e', 'd'};
    engines[2]->put(emptyKey, expected.at(emptyKey));
    verify(false);
    const std::vector<uint8_t> removed{0, 'A', 'K', 'S', 'M', '1', 'x'};
    engines[1]->remove(removed); expected.erase(removed);
    for (const auto& engine : engines) { AKK_CLUSTER_CHECK(!engine->get(removed) && !engine->exists(removed)); }
    close();
    for (auto& opt : options) { opt.cluster.runtime.stripeReadCoordinatorMode = StripeReadCoordinatorMode::LOCAL_COORDINATOR; }
    open();
    AKK_CLUSTER_CHECK(waitUntil([&] { try { return engines[0]->count() == expected.size(); } catch (...) { return false; } }, std::chrono::seconds{20}));
    verify(false); close();
    for (auto& opt : options) { opt.cluster.runtime.readMode = ClusterReadMode::OWNER_ONLY; }
    open();
    AKK_CLUSTER_CHECK(waitUntil([&] { try { return engines[0]->count() == expected.size(); } catch (...) { return false; } }, std::chrono::seconds{20}));
    verify(true); close();
}

void testRaftSlowForwardResponseDeadline(bool secure) {
    const auto dir = makeTempDir(secure ? "raft-slow-response-secure" : "raft-slow-response-plain");
    const ClusterConfig config{{node(1, 30401, 30411), node(2, 30402, 30412)}, ReplicationMode::MIRROR,
        {}, {.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 1000}, {}, {}, 0, testClusterId(30400), {}, FailoverPolicy::NONE};
    ClusterRuntimeOptions options; options.clusterGroupId = 30400;
    if (secure) {
        for (uint64_t id : {1, 2}) {
            const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
            options.secure.pinnedPeers.push_back({id, identity.publicKey});
        }
    }
    RaftInboundFaultProxy proxy{30411};
    std::atomic<unsigned> executions{0};
    auto serverOptions = options; serverOptions.replBindHost = "127.0.0.2";
    serverOptions.secure.identitySeedPath = dir / "identity-1";
    auto server = makeRuntime(dir / "n1", config, 1, serverOptions, secure, [&](const ForwardRequest& request) {
        ++executions;
        if (request.entries.front().key == std::vector<uint8_t>{'s'}) { proxy.slowResponses(true); }
        ForwardResponse response; response.success = true; response.found = true; response.value.assign(96, 42); return response;
    });
    options.secure.identitySeedPath = dir / "identity-2";
    auto client = makeRuntime(dir / "n2", config, 2, options, secure);
    server->runtime->start(); client->runtime->start(); proxy.setBlocked(false);
    ForwardRequest request; request.operation = ForwardOperation::GET; request.timeoutMs = 2000; request.entries.push_back({{'f'}, {}});
    AKK_CLUSTER_CHECK(client->runtime->forwardTo(1, request).success);
    for (const auto operation : {ForwardOperation::GET, ForwardOperation::PUT}) {
        proxy.slowResponses(false);
        request.operation = operation; request.timeoutMs = 150; request.entries[0].key = {'s'};
        const auto started = std::chrono::steady_clock::now();
        bool rejected = false;
        try { (void)client->runtime->forwardTo(1, request); }
        catch (const ClusterRoutingError& error) {
            rejected = error.code == (operation == ForwardOperation::PUT ? ClusterRoutingCode::OUTCOME_UNKNOWN : ClusterRoutingCode::FORWARD_UNAVAILABLE);
        }
        const auto elapsed = std::chrono::steady_clock::now() - started;
        AKK_CLUSTER_CHECK(rejected && elapsed < std::chrono::milliseconds{600});
        // A fresh request must not consume the timed-out response or replay the old operation.
        proxy.slowResponses(false); request.timeoutMs = 2000; request.entries[0].key = {'f'};
        AKK_CLUSTER_CHECK(client->runtime->forwardTo(1, request).success);
    }
    AKK_CLUSTER_CHECK(executions.load() == 5 && !proxy.failed());
    client->runtime->close(); server->runtime->close();
}

void runClusterP1RegressionTests() {
    testReadKeySpaceFraming();
    for (const bool secure : {false, true}) {
        std::fprintf(stderr, "[cluster P1] STRIPE public key namespaces (%u)\n", secure);
        testStripePublicKeyNamespaces(secure);
        std::fprintf(stderr, "[cluster P1] Raft slow forward response deadline (%u)\n", secure);
        testRaftSlowForwardResponseDeadline(secure);
    }
}

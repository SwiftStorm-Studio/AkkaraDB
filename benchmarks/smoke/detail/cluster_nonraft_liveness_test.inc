/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */

    void testNonRaftIdleBlackhole(bool secure, bool dropRequests, bool dropResponses) {
        const auto dir = makeTempDir("nonraft-idle-blackhole");
        constexpr uint16_t port = 24990;
        ClusterRuntimeOptions options;
        options.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
        options.replBindHost = "127.0.0.2";
        options.replicationLiveness = {.heartbeatIntervalMs = 100, .peerTimeoutMs = 600};
        if (secure) {
            for (uint64_t id : {1, 2}) {
                const auto identity = akkaradb::crypto::IdentityStore{dir / std::to_string(id)}.loadOrCreate();
                options.secure.pinnedPeers.push_back({id, identity.publicKey});
            }
            options.secure.identitySeedPath = dir / "1";
        }
        auto server = ReplicationServer::create(port, 1, [] { return uint64_t{0}; }, {}, {}, 1, {2}, options);
        server->setReadCallback([](const ReadRequest& request) {
            ReadResponse response;
            response.requestId = request.requestId; response.status = ReadStatus::FOUND; response.value = {'o', 'k'};
            return response;
        });
        server->start();
        RaftInboundFaultProxy proxy{port};
        proxy.setBlocked(false);
        auto clientOptions = options;
        clientOptions.secure.identitySeedPath = dir / "2";
        clientOptions.secure.expectedPrimaryNodeId = 1;
        auto client = ReplicationClient::create("127.0.0.1", port, 2, [] { return uint64_t{0}; }, {}, clientOptions);
        client->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return client->connected() && server->replicaCount() == 1; }));
        const auto beforeIdle = client->lastSuccessfulContactAtUs();
        std::this_thread::sleep_for(std::chrono::milliseconds{1300});
        AKK_CLUSTER_CHECK(client->connected() && server->replicaCount() == 1);
        AKK_CLUSTER_CHECK(client->lastSuccessfulContactAtUs() > beforeIdle);
        AKK_CLUSTER_CHECK(server->stats().queuedFrames == 0);
        proxy.blackhole(dropRequests, dropResponses);
        const bool detected = waitUntil([&] { return !client->connected() && server->replicaCount() == 0; },
            std::chrono::milliseconds{1800});
        if (!detected) { std::fprintf(stderr, "idle blackhole: secure=%d requests=%d responses=%d client=%d replicas=%zu\n",
            secure, dropRequests, dropResponses, client->connected(), server->replicaCount()); }
        AKK_CLUSTER_CHECK(detected);
        proxy.blackhole(false, false);
        AKK_CLUSTER_CHECK(waitUntil([&] { return client->connected() && server->replicaCount() == 1; }));
        const auto read = client->readKey(bytesOf("after-reconnect"), 0, 1000);
        AKK_CLUSTER_CHECK(read.status == ReadStatus::FOUND && textOf(read.value) == "ok");
        client->close();
        server->close();
        AKK_CLUSTER_CHECK(!proxy.failed());
    }

    void testNonRaftLivenessFramingAndPolicy() {
        for (const auto type : {ReplMsgType::HEARTBEAT_PING, ReplMsgType::HEARTBEAT_PONG}) {
            DecodedFrame frame;
            AKK_CLUSTER_CHECK(decodeFrame(encodeHeartbeat(type, 123), frame));
            uint64_t nonce = 0;
            AKK_CLUSTER_CHECK(frame.type == type && decodeHeartbeat(frame.payload, nonce) && nonce == 123);
            AKK_CLUSTER_CHECK(!decodeHeartbeat(std::span{frame.payload}.first(7), nonce));
            frame.payload.push_back(0);
            AKK_CLUSTER_CHECK(!decodeHeartbeat(frame.payload, nonce));
            frame.payload.assign(8, 0);
            AKK_CLUSTER_CHECK(!decodeHeartbeat(frame.payload, nonce));
        }
        for (const auto operation : {0, 1}) {
            bool rejected = false;
            try { (void)encodeHeartbeat(operation == 0 ? ReplMsgType::ACK : ReplMsgType::HEARTBEAT_PING, 0); }
            catch (const std::invalid_argument&) { rejected = true; }
            AKK_CLUSTER_CHECK(rejected);
        }
        const ReplicationLivenessOptions policy{.heartbeatIntervalMs = 50, .peerTimeoutMs = 150};
        for (const bool server : {false, true}) {
            DecodedFrame frame;
            if (server) { ServerHello hello; hello.liveness = policy; AKK_CLUSTER_CHECK(decodeFrame(encodeServerHello(hello), frame)); }
            else { ClientHello hello; hello.liveness = policy; AKK_CLUSTER_CHECK(decodeFrame(encodeClientHello(hello), frame)); }
            AKK_CLUSTER_CHECK(frame.payload.size() == 90);
            ClientHello client; ServerHello primary;
            AKK_CLUSTER_CHECK(server ? decodeServerHello(frame.payload, primary) : decodeClientHello(frame.payload, client));
            AKK_CLUSTER_CHECK((server ? primary.liveness : client.liveness) == policy);
            AKK_CLUSTER_CHECK(!(server ? decodeServerHello(std::span{frame.payload}.first(82), primary) : decodeClientHello(std::span{frame.payload}.first(82), client)));
            std::fill(frame.payload.begin() + 82, frame.payload.begin() + 86, 0);
            AKK_CLUSTER_CHECK(!(server ? decodeServerHello(frame.payload, primary) : decodeClientHello(frame.payload, client)));
        }
        detail::PeerLiveness liveness{policy};
        std::this_thread::sleep_until(liveness.nextProbeDeadline());
        const auto nonce = liveness.beginProbe();
        AKK_CLUSTER_CHECK(nonce != 0 && !liveness.acknowledge(nonce + 1));
        AKK_CLUSTER_CHECK(liveness.acknowledge(nonce) && !liveness.acknowledge(nonce));
        AKK_CLUSTER_CHECK(!liveness.receiveProbe(0));
        AKK_CLUSTER_CHECK(liveness.receiveProbe(1) && liveness.receiveProbe(2));
        AKK_CLUSTER_CHECK(!liveness.receiveProbe(1) && liveness.takePong() == 2 && liveness.takePong() == 0);

        ClusterRuntimeOptions options;
        options.transportMode = TransportMode::PLAIN;
        options.replicationLiveness = {.heartbeatIntervalMs = 100, .peerTimeoutMs = 200};
        for (const bool server : {false, true}) {
            bool rejected = false;
            try {
                if (server) { (void)ReplicationServer::create(24994, 1, [] { return uint64_t{0}; }, {}, {}, 1, {2}, options); }
                else { (void)ReplicationClient::create("127.0.0.1", 24994, 2, [] { return uint64_t{0}; }, {}, options); }
            } catch (const std::invalid_argument&) { rejected = true; }
            AKK_CLUSTER_CHECK(rejected);
        }
        options.replicationLiveness.peerTimeoutMs = 600;
        auto server = ReplicationServer::create(24994, 1, [] { return uint64_t{0}; }, {}, {}, 1, {2}, options);
        server->start();
        options.replicationLiveness.peerTimeoutMs = 900;
        auto client = ReplicationClient::create("127.0.0.1", 24994, 2, [] { return uint64_t{0}; }, {}, options);
        client->start();
        std::this_thread::sleep_for(std::chrono::milliseconds{1100});
        AKK_CLUSTER_CHECK(!client->connected() && client->lastSuccessfulContactAtUs() == 0 && server->replicaCount() == 0);
        client->close(); server->close();
    }

    void testNonRaftLivenessDuringTransfer(bool secure) {
        const auto dir = makeTempDir("nonraft-liveness-transfer");
        constexpr uint16_t port = 24995;
        ClusterRuntimeOptions options;
        options.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
        options.replBindHost = "127.0.0.2";
        options.replicationLiveness = {.heartbeatIntervalMs = 100, .peerTimeoutMs = 4000};
        options.transfer.thresholdBytes = 4096; options.transfer.chunkBytes = 8192;
        options.transfer.spoolDirectory = dir / "spools";
        if (secure) {
            for (uint64_t id : {1, 2}) {
                const auto identity = akkaradb::crypto::IdentityStore{dir / std::to_string(id)}.loadOrCreate();
                options.secure.pinnedPeers.push_back({id, identity.publicKey});
            }
            options.secure.identitySeedPath = dir / "1";
        }
        const AckPolicy ack{.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::APPLIED};
        auto server = ReplicationServer::create(port, 1, [] { return uint64_t{0}; }, ack,
            ConsistencyOptions{.ackTimeoutAction = AckTimeoutAction::FAIL_WRITE, .ackTimeoutMs = 15000}, 1, {2}, options);
        server->start();
        RaftInboundFaultProxy proxy{port}; proxy.setBlocked(false);
        auto clientOptions = options;
        clientOptions.secure.expectedPrimaryNodeId = 1; clientOptions.secure.identitySeedPath = dir / "2";
        auto client = ReplicationClient::create("127.0.0.1", port, 2, [] { return uint64_t{0}; }, ack, clientOptions);
        const std::string value(4u * 1024u * 1024u, 'H');
        std::atomic<unsigned> applied{0};
        client->setApplyCallback([&](uint64_t seq, ReplOpType, auto key, auto bytes, uint8_t, uint64_t) {
            AKK_CLUSTER_CHECK(seq == 1 && textOf(key) == "slow" && textOf(bytes) == value); ++applied;
        });
        client->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return client->connected() && server->replicaCount() == 1; }));
        proxy.slowResponses(true, 8192, 10);
        const auto started = std::chrono::steady_clock::now();
        server->shipEntry(1, ReplOpType::PUT, bytesOf("slow"), bytesOf(value), 0, 1);
        AKK_CLUSTER_CHECK(std::chrono::steady_clock::now() - started > std::chrono::milliseconds{4000});
        AKK_CLUSTER_CHECK(applied.load() == 1 && client->connected() && server->replicaCount() == 1);
        proxy.slowResponses(false);
        client->close(); server->close();
        AKK_CLUSTER_CHECK(!proxy.failed());
    }

    void testStripeIdleReachability() {
        const auto dir = makeTempDir("stripe-idle-reachability");
        const ClusterConfig config{{node(1, 24881, 24991), node(2, 24882, 24992), node(3, 24883, 24993)},
            ReplicationMode::STRIPE, {}, ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 500},
            {}, StripeOptions{.dataShards = 2, .parityShards = 1}};
        ClusterRuntimeOptions options;
        options.transportMode = TransportMode::PLAIN;
        options.clusterGroupId = 9551; options.clusterGroupEpoch = 1;
        options.replicationLiveness = {.heartbeatIntervalMs = 100, .peerTimeoutMs = 600};
        options.replicationClusterId = config.clusterId(); options.replicationConfigFingerprint = config.replicationFingerprint();
        auto peerOptions = options; peerOptions.replBindHost = "127.0.0.2";
        auto peer = ReplicationServer::create(24992, 2, [] { return uint64_t{0}; }, {}, {}, 1, {1}, peerOptions);
        peer->start();
        RaftInboundFaultProxy proxy{24992}; proxy.setBlocked(false);
        auto local = makeRuntime(dir / "n1", config, 1, options);
        local->runtime->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return local->runtime->stripeNodeReachable(2); }));
        proxy.blackhole(true, true);
        AKK_CLUSTER_CHECK(waitUntil([&] { return !local->runtime->stripeNodeReachable(2); }, std::chrono::milliseconds{1800}));
        proxy.blackhole(false, false);
        AKK_CLUSTER_CHECK(waitUntil([&] { return local->runtime->stripeNodeReachable(2); }));
        local->runtime->close(); peer->close();
        AKK_CLUSTER_CHECK(!proxy.failed());
    }

    void runNonRaftLivenessTests() {
        testNonRaftLivenessFramingAndPolicy();
        for (const bool secure : {false, true}) {
            testNonRaftIdleBlackhole(secure, true, true);
            testNonRaftIdleBlackhole(secure, true, false);
            testNonRaftIdleBlackhole(secure, false, true);
        }
        testNonRaftLivenessDuringTransfer(false);
        testNonRaftLivenessDuringTransfer(true);
        testStripeIdleReachability();
    }

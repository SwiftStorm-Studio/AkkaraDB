// Range preparation and total RPC deadline regressions.
void testStripeStreamingQueryRanges() {
    using Engine = akkaradb::engine::AkkEngine;
    const auto dir = makeTempDir("stripe-streaming-query");
    const ClusterConfig config{{node(1, 25401, 25411), node(2, 25402, 25412), node(3, 25403, 25413)},
        ReplicationMode::STRIPE, {}, ConsistencyOptions{.ackTimeoutMs = 2000}, {},
        StripeOptions{.dataShards = 1, .parityShards = 1}, 0, testClusterId(9540)};
    const auto optionsFor = [&](uint64_t id) {
        akkaradb::engine::AkkEngineOptions options;
        options.paths.dataDir = dir / ("n" + std::to_string(id));
        options.components.clusterEnabled = true; options.components.blobEnabled = false;
        options.cluster.config = config;
        options.cluster.runtime.transportMode = TransportMode::PLAIN;
        options.cluster.runtime.clusterGroupId = 9540;
        options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
        options.cluster.runtime.queryMaxSpoolBytes = 384;
        options.cluster.runtime.stripeAutoRebuild = false;
        writeNodeIdFile(options.paths.dataDir / "node.id", id);
        return options;
    };
    auto n1 = Engine::open(optionsFor(1)), n2 = Engine::open(optionsFor(2)), n3 = Engine::open(optionsFor(3));
    Engine* nodes[]{n1.get(), n2.get(), n3.get()};
    AKK_CLUSTER_CHECK(waitUntil([&] { try { return n3->count() == 0; } catch (...) { return false; } }));
    std::set<std::vector<uint8_t>> keys{{'a'}, {'a', 'a'}, {'b'}, {0}, {0, 255}, {255}, {255, 0}};
    for (unsigned i = 0; i < 32; ++i) {
        const auto key = "bulk-" + std::to_string(i) + std::string(i % 31, 'x');
        keys.emplace(key.begin(), key.end());
    }
    ClusterRouter router{config};
    for (const auto& key : keys) { nodes[router.writeTargets(key).front().nodeId - 1]->put(key, bytesOf("v")); }
    for (auto* engine : nodes) { engine->forceFlush(); }
    AKK_CLUSTER_CHECK(n3->count() == keys.size());
    const auto verifyRange = [&](const std::vector<uint8_t>& start, const std::vector<uint8_t>& end) {
        std::vector<std::vector<uint8_t>> expected;
        for (const auto& key : keys) { if (key >= start && (end.empty() || key < end)) { expected.push_back(key); } }
        AKK_CLUSTER_CHECK(n3->count(start, end) == expected.size());
        akkaradb::core::BufferArena arena;
        std::vector<std::vector<uint8_t>> actual;
        for (const auto& record : n3->scan(arena, start, end)) {
            actual.emplace_back(record.key.begin(), record.key.end());
            AKK_CLUSTER_CHECK(textOf(record.value) == "v");
        }
        AKK_CLUSTER_CHECK(actual == expected);
    };
    verifyRange({'a'}, {'a', 'a'});
    verifyRange({0}, {1});
    verifyRange({255}, {});
    verifyRange({'z'}, {'a'});
    // Quota failure releases the cursor/reservation. Counts remain independent
    // of result size, and a subsequent small scan still succeeds.
    bool rejected = false;
    try { akkaradb::core::BufferArena arena; (void)n3->scan(arena); }
    catch (const std::runtime_error&) { rejected = true; }
    AKK_CLUSTER_CHECK(rejected && n3->count() == keys.size());
    verifyRange({'a'}, {'a', 'a'});
    // The physical metadata index has public byte order despite different key
    // lengths. The exclusive prefix upper bound excludes shards and intents.
    for (auto* engine : nodes) {
        akkaradb::core::BufferArena arena;
        const std::vector<uint8_t> begin{0, 'A', 'K', 'S', 'M', '1'}, end{0, 'A', 'K', 'S', 'M', '2'};
        std::vector<std::vector<uint8_t>> indexed;
        for (const auto& record : engine->scanLocalStorage(arena, begin, end)) {
            indexed.emplace_back(record.key.begin() + 6, record.key.end());
        }
        AKK_CLUSTER_CHECK(indexed == std::vector<std::vector<uint8_t>>(keys.begin(), keys.end()));
    }
    n3->close(); n2->close(); n1->close();
    n1 = Engine::open(optionsFor(1)); n2 = Engine::open(optionsFor(2)); n3 = Engine::open(optionsFor(3));
    AKK_CLUSTER_CHECK(waitUntil([&] { try { return n3->count() == keys.size(); } catch (...) { return false; } }));
    verifyRange({'a'}, {'a', 'a'});
    n3->close(); n2->close(); n1->close();
}

void testRpcAdmissionDeadline(bool secure, bool stripe, bool rollback = false) {
    const auto dir = makeTempDir("rpc-admission-deadline");
    ClusterRuntimeOptions options;
    options.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
    if (secure) {
        for (uint64_t id : {1, 2}) {
            const auto identity = akkaradb::crypto::IdentityStore{dir / std::to_string(id)}.loadOrCreate();
            options.secure.pinnedPeers.push_back({id, identity.publicKey});
        }
        options.secure.identitySeedPath = dir / "1";
    }
    auto server = ReplicationServer::create(25451, 1, [] { return uint64_t{0}; }, {}, {}, 1, {2}, options);
    std::promise<void> entered, release;
    auto started = entered.get_future(); auto released = release.get_future().share();
    std::atomic<unsigned> calls{0};
    const auto block = [&] { if (++calls == 1) { entered.set_value(); released.wait(); } };
    server->setReadCallback([&](const ReadRequest&) { block(); return ReadResponse{.status = ReadStatus::NOT_FOUND, .value = {}}; });
    server->setStripeControlCallback([&](uint64_t, const StripeControlRequest&, const auto&) {
        block(); return StripeControlResponse{.status = StripeControlStatus::GRANTED, .metadata = {}};
    });
    auto clientOptions = options;
    clientOptions.secure.expectedPrimaryNodeId = 1;
    if (secure) { clientOptions.secure.identitySeedPath = dir / "2"; }
    auto client = ReplicationClient::create("127.0.0.1", 25451, 2, [] { return uint64_t{0}; }, {}, clientOptions);
    client->setApplyCallback([](auto, auto, auto, auto, auto, auto) {}); client->setForceDurableCallback([] {});
    server->start(); client->start();
    AKK_CLUSTER_CHECK(waitUntil([&] { return client->connected() && server->replicaCount() == 1; }));
    const auto invoke = [&](uint32_t timeout) {
        if (stripe) {
            (void)client->stripeControl(StripeControlRequest{
                .action = rollback ? StripeControlAction::ROLLBACK_WATERMARK : StripeControlAction::ACQUIRE,
                .ownerNodeId = 1, .key = {'k'}, .metadata = {}}, timeout);
        }
        else { (void)client->readKey(bytesOf("k"), 0, timeout); }
    };
    auto first = std::async(std::launch::async, [&] { try { invoke(2000); return true; } catch (...) { return false; } });
    const bool firstEntered = started.wait_for(std::chrono::seconds{3}) == std::future_status::ready;
    const auto begin = std::chrono::steady_clock::now();
    bool timedOut = false;
    try { invoke(60); } catch (const std::runtime_error&) { timedOut = true; }
    const auto elapsed = std::chrono::steady_clock::now() - begin;
    const bool firstStillPending = first.wait_for(std::chrono::milliseconds{0}) == std::future_status::timeout;
    release.set_value();
    const bool firstSucceeded = first.get();
    const bool serialized = !stripe || rollback;
    AKK_CLUSTER_CHECK(firstEntered && timedOut == serialized && elapsed < std::chrono::milliseconds{500} &&
        firstStillPending && firstSucceeded);
    const unsigned completedCalls = serialized ? 1 : 2;
    AKK_CLUSTER_CHECK(calls.load() == completedCalls && client->connected());
    invoke(1000); // Admission timeout and parallel completion both leave the connection usable.
    AKK_CLUSTER_CHECK(calls.load() == completedCalls + 1);
    bool expired = false;
    try { invoke(0); } catch (const std::runtime_error&) { expired = true; }
    AKK_CLUSTER_CHECK(expired && calls.load() == completedCalls + 1);
    if (stripe) {
        bool oversized = false;
        try { (void)client->stripeControl(StripeControlRequest{.ownerNodeId = 1, .key = {'k'},
            .metadata = std::vector<uint8_t>(65536)}, 1000); }
        catch (const std::length_error&) { oversized = true; }
        AKK_CLUSTER_CHECK(oversized);
        invoke(1000); // Local encoding failures must not strand pending state.
        AKK_CLUSTER_CHECK(calls.load() == completedCalls + 2);
    }
    client->close(); server->close();
}

void testReadSendAndSocketDeadline() {
    RaftTestSocket listener;
    listener.listen(25471);
    std::promise<void> uploadBegan;
    auto began = uploadBegan.get_future();
    auto peer = std::async(std::launch::async, [&] {
        RaftTestSocket connection{::accept(listener.handle, nullptr, nullptr)};
        const auto hello = connection.receiveFrame();
        AKK_CLUSTER_CHECK(hello.type == ReplMsgType::CLIENT_HELLO);
        AKK_CLUSTER_CHECK(connection.send(encodeServerHello(ServerHello{.nodeId = 1})));
        const auto begin = connection.receiveFrame();
        AKK_CLUSTER_CHECK(begin.type == ReplMsgType::TRANSFER_BEGIN);
        uploadBegan.set_value();
        // Do not send READY. The request budget must cancel the otherwise long
        // transfer wait and close the socket; no slow peer can extend it.
        std::array<uint8_t, 1> byte{};
        AKK_CLUSTER_CHECK(!connection.receive(byte));
    });
    ClusterRuntimeOptions options; options.transportMode = TransportMode::PLAIN;
    auto client = ReplicationClient::create("127.0.0.1", 25471, 2, [] { return uint64_t{0}; }, {}, options);
    client->setApplyCallback([](auto, auto, auto, auto, auto, auto) {}); client->setForceDurableCallback([] {});
    client->start(); AKK_CLUSTER_CHECK(waitUntil([&] { return client->connected(); }));
    const auto start = std::chrono::steady_clock::now();
    auto upload = std::async(std::launch::async, [&] {
        try {
            (void)client->readKey(std::vector<uint8_t>(256 * 1024, 'k'), 0, 1500);
        } catch (const std::runtime_error&) { return true; }
        return false;
    });
    const bool sending = began.wait_for(std::chrono::seconds{2}) == std::future_status::ready;
    const auto contend = [&](bool otherStripe) {
        const auto begin = std::chrono::steady_clock::now();
        bool timedOut = false;
        try {
            if (otherStripe) { (void)client->stripeControl(StripeControlRequest{.ownerNodeId = 1, .key = {'k'}, .metadata = {}}, 60); }
            else { (void)client->readKey(bytesOf("k"), 0, 60); }
        } catch (const std::runtime_error&) { timedOut = true; }
        return timedOut && std::chrono::steady_clock::now() - begin < std::chrono::milliseconds{500};
    };
    const bool socketBounded = contend(true), admissionBounded = contend(false);
    const bool failed = upload.get();
    peer.get(); client->close();
    AKK_CLUSTER_CHECK(sending && socketBounded && admissionBounded && failed);
    AKK_CLUSTER_CHECK(std::chrono::steady_clock::now() - start < std::chrono::seconds{3});
}

void testStripeControlBlockedSendDeadline() {
    RaftTestSocket listener;
    listener.listen(25471);
    const int smallBuffer = 1024;
    AKK_CLUSTER_CHECK(::setsockopt(listener.handle, SOL_SOCKET, SO_RCVBUF,
        reinterpret_cast<const char*>(&smallBuffer), sizeof(smallBuffer)) == 0);
    std::promise<void> release;
    auto released = release.get_future();
    auto peer = std::async(std::launch::async, [&] {
        RaftTestSocket connection{::accept(listener.handle, nullptr, nullptr)};
        AKK_CLUSTER_CHECK(::setsockopt(connection.handle, SOL_SOCKET, SO_RCVBUF,
            reinterpret_cast<const char*>(&smallBuffer), sizeof(smallBuffer)) == 0);
        AKK_CLUSTER_CHECK(connection.receiveFrame().type == ReplMsgType::CLIENT_HELLO);
        AKK_CLUSTER_CHECK(connection.send(encodeServerHello(ServerHello{.nodeId = 1})));
        // Consume no requests. Repeated valid control frames fill the sender's
        // TCP buffer even on systems with a large default send buffer.
        (void)released.wait_for(std::chrono::seconds{20});
        connection.shutdown();
    });
    ClusterRuntimeOptions options; options.transportMode = TransportMode::PLAIN;
    auto client = ReplicationClient::create("127.0.0.1", 25471, 2, [] { return uint64_t{0}; }, {}, options);
    client->setApplyCallback([](auto, auto, auto, auto, auto, auto) {}); client->setForceDurableCallback([] {});
    client->start(); AKK_CLUSTER_CHECK(waitUntil([&] { return client->connected(); }));
    bool bounded = true;
    std::chrono::steady_clock::duration longest{};
    unsigned attempts = 0;
    std::string lastError;
    for (unsigned attempt = 0; attempt < 96 && client->connected(); ++attempt) {
        ++attempts;
        const auto started = std::chrono::steady_clock::now();
        try {
            (void)client->stripeControl(StripeControlRequest{.ownerNodeId = 1, .key = {'k'},
                .metadata = std::vector<uint8_t>(60000, 'x')}, 60);
            bounded = false;
        } catch (const std::runtime_error& error) { lastError = error.what(); }
        const auto elapsed = std::chrono::steady_clock::now() - started;
        longest = std::max(longest, elapsed);
        bounded &= elapsed < std::chrono::milliseconds{500};
    }
    const bool interrupted = waitUntil([&] { return !client->connected(); }, std::chrono::milliseconds{500});
    release.set_value(); peer.get(); client->close();
    std::fprintf(stderr, "[cluster resources] control backpressure: attempts %u, longest %lld ms, disconnected %u, last %s\n",
        attempts, static_cast<long long>(std::chrono::duration_cast<std::chrono::milliseconds>(longest).count()), interrupted, lastError.c_str());
    AKK_CLUSTER_CHECK(bounded && interrupted);
}

void runClusterRpcDeadlineTests() {
    for (bool stripe : {false, true}) {
        for (bool secure : {false, true}) {
            std::fprintf(stderr, "[cluster resources] RPC admission stripe %u secure %u\n", stripe, secure);
            testRpcAdmissionDeadline(secure, stripe);
            if (stripe) {
                std::fprintf(stderr, "[cluster resources] rollback admission secure %u\n", secure);
                testRpcAdmissionDeadline(secure, true, true);
            }
        }
    }
    std::fprintf(stderr, "[cluster resources] read send and socket deadlines\n");
    testReadSendAndSocketDeadline();
    std::fprintf(stderr, "[cluster resources] STRIPE control send deadline\n");
    testStripeControlBlockedSendDeadline();
    std::fprintf(stderr, "[cluster resources] timed-out reads preserve replication ACKs\n");
    testTimedOutReadKeepsWriteConnection();
}

void runClusterQueryResourceTests() {
    std::fprintf(stderr, "[cluster resources] STRIPE ranges and quotas\n");
    testStripeStreamingQueryRanges();
    runClusterRpcDeadlineTests();
}

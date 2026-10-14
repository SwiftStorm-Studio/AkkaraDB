/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
void testDiskResultRetention() {
    using akkaradb::engine::cluster::detail::DiskResults;
    const auto dir = makeTempDir("disk-result-retention");
    auto store = std::make_shared<DiskResults>(dir, 4 * DiskResults::recordCharge(16, 1));
    std::array<uint8_t, 16> id{}; id[0] = 1;
    store->put(id, {100, 1, {9}}); store->sync();
    auto before = store->capture();
    auto imported = store->beginImport(2, 100);
    id[0] = 2; store->import(imported, id, {200, 2, {8}}); store->prepare(imported);
    store->activate(imported.generation); store->collectGarbage();
    unsigned oldCount = 0;
    AKK_CLUSTER_CHECK(before.forEach([&](auto key, const DiskResults::Record& record) {
        ++oldCount; return key[0] == 1 && record.sequence == 1 && record.value == std::vector<uint8_t>{9};
    }) && oldCount == 1);
    AKK_CLUSTER_CHECK(store->count() == 1 && store->get(id)->sequence == 2);
    for (unsigned n = 3; n <= 5; ++n) { id[0] = static_cast<uint8_t>(n); store->put(id, {200, n, {7}}); }
    store->sync(); AKK_CLUSTER_CHECK(!store->canReserve(DiskResults::recordCharge(16, 1), 199));
    AKK_CLUSTER_CHECK(store->canReserve(DiskResults::recordCharge(16, 1), 200));
    before = {}; store.reset(); store = std::make_shared<DiskResults>(dir, 4 * DiskResults::recordCharge(16, 1));
    AKK_CLUSTER_CHECK(store->count() == 4 && store->get(id)->sequence == 5);
    store->advanceFloor(200); store->sync(); AKK_CLUSTER_CHECK(store->count() == 0 && !store->get(id));
    store.reset(); store = std::make_shared<DiskResults>(dir, 4 * DiskResults::recordCharge(16, 1));
    AKK_CLUSTER_CHECK(store->count() == 0 && store->floor() == 200);
}

void testRaftFollowerApplyFailure() {
    const auto dir = makeTempDir("raft-follower-apply-failure");
    const ClusterConfig config{{node(1, 32901, 32911), node(2, 32902, 32912), node(3, 32903, 32913)},
        ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 2000}};
    ClusterRuntimeOptions options; options.raftSnapshot.enabled = false;
    std::atomic<unsigned> attempts{0}, terminalFailures{0};
    auto n1 = makeRuntime(dir / "1", config, 1, options), n2 = makeRuntime(dir / "2", config, 2, options);
    auto n3 = makeRuntime(dir / "3", config, 3, options, false, {}, [&](auto& callbacks) {
        callbacks.apply = [&](uint64_t, ReplOpType, auto, auto, uint8_t, uint64_t, uint64_t) {
            ++attempts; throw std::runtime_error("injected follower apply failure");
        };
        callbacks.fatalFailure = [&](std::exception_ptr) { ++terminalFailures; };
    });
    n1->runtime->start(); n2->runtime->start(); n3->runtime->start();
    RuntimeHarness* healthy[]{n1.get(), n2.get()};
    AKK_CLUSTER_CHECK(waitUntil([&] { return findLeader(healthy) != nullptr; }));
    auto* leader = findLeader(healthy);
    leader->runtime->shipEntry(1, ReplOpType::PUT, bytesOf("failure"), bytesOf("first"), 0, leader->nodeId);
    AKK_CLUSTER_CHECK(waitUntil([&] { return n3->runtime->raftStats().health == akkaradb::engine::ClusterHealthState::FAILED; }));
    const auto failed = n3->runtime->raftStats();
    AKK_CLUSTER_CHECK(failed.lastFailure == akkaradb::engine::ClusterFailureCode::RAFT_APPLY && failed.commitIndex > failed.appliedIndex);
    leader->runtime->shipEntry(2, ReplOpType::PUT, bytesOf("failure"), bytesOf("second"), 0, leader->nodeId);
    AKK_CLUSTER_CHECK(waitUntil([&] { return n1->hasState(2, "failure", "second") && n2->hasState(2, "failure", "second"); }));
    std::this_thread::sleep_for(std::chrono::milliseconds{250});
    AKK_CLUSTER_CHECK(attempts == 1 && terminalFailures == 1);
    n3->runtime->close(); bool restartRejected = false;
    try { n3->runtime->start(); } catch (const std::runtime_error&) { restartRejected = true; }
    AKK_CLUSTER_CHECK(restartRejected);
    n3.reset(); n3 = makeRuntime(dir / "3", config, 3, options); n3->runtime->start();
    AKK_CLUSTER_CHECK(waitUntil([&] { return n3->hasState(2, "failure", "second"); }));
    n3->runtime->close(); n2->runtime->close(); n1->runtime->close();
}

void testRaftDurabilityFailure() {
    const auto dir = makeTempDir("raft-durability-failure");
    const ClusterConfig config{{node(1, 32921, 32931)}, ReplicationMode::MIRROR, {},
        ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM}};
    std::atomic<unsigned> failures{0};
    auto runtime = makeRuntime(dir, config, 1, {}, false, {}, [&](auto& callbacks) {
        callbacks.forceDurable = [] { throw std::runtime_error("injected durable apply failure"); };
        callbacks.fatalFailure = [&](std::exception_ptr) { ++failures; };
    });
    runtime->runtime->start(); AKK_CLUSTER_CHECK(waitUntil([&] { return runtime->runtime->role() == NodeRole::PRIMARY; }));
    bool rejected = false;
    try { runtime->runtime->shipEntry(1, ReplOpType::PUT, bytesOf("durability"), bytesOf("failure"), 0, 1); }
    catch (const std::runtime_error&) { rejected = true; }
    AKK_CLUSTER_CHECK(rejected && waitUntil([&] { return failures == 1; }));
    AKK_CLUSTER_CHECK(runtime->runtime->raftStats().lastFailure == akkaradb::engine::ClusterFailureCode::RAFT_APPLY);
    runtime->runtime->close();
}

std::shared_ptr<std::atomic<unsigned>> nativeFailureAttempts;
void testNativeApplyFailureRejectsReads(bool snapshotFailure) {
    using akkaradb::engine::AkkEngine;
    std::fprintf(stderr, "[native failure] snapshot=%u\n", snapshotFailure);
    nativeFailureAttempts = std::make_shared<std::atomic<unsigned>>(0);
    AKK_CLUSTER_CHECK(registerClusterRuntimeFactory([](std::filesystem::path dir, ClusterConfig config, uint64_t id,
        ClusterEngineCallbacks callbacks, ClusterRuntimeOptions options) -> std::unique_ptr<IClusterRuntime> {
        if (id == 3) {
            callbacks.apply = [attempts = nativeFailureAttempts, apply = std::move(callbacks.apply)](
                uint64_t seq, ReplOpType op, auto key, auto value, uint8_t flags, uint64_t source, uint64_t timestamp) {
                apply(seq, op, key, value, flags, source, timestamp);
                if (textOf(key) == "poison") { ++*attempts; throw std::runtime_error("injected partial Native application"); }
            };
            callbacks.finishSnapshot = [attempts = nativeFailureAttempts](uint64_t, uint64_t) {
                ++*attempts; throw std::runtime_error("injected Native snapshot installation");
            };
        }
        return ClusterRuntime::create(std::move(dir), std::move(config), id, std::move(callbacks), std::move(options));
    }));
    auto restore = std::shared_ptr<void>{reinterpret_cast<void*>(1), [](void*) {
        nativeFailureAttempts.reset();
        (void)registerClusterRuntimeFactory([](std::filesystem::path dir, ClusterConfig config, uint64_t id,
            ClusterEngineCallbacks callbacks, ClusterRuntimeOptions options) -> std::unique_ptr<IClusterRuntime> {
            return ClusterRuntime::create(std::move(dir), std::move(config), id, std::move(callbacks), std::move(options));
        });
    }};
    const auto dir = makeTempDir(snapshotFailure ? "native-snapshot-apply-failure" : "native-partial-apply-failure");
    auto members = std::vector{node(1, 33001, 33011), node(2, 33002, 33012)};
    auto third = node(3, 33003, 33013);
    if (snapshotFailure) { third.capabilities |= RAFT_LEARNER; } else { members.push_back(third); }
    auto raft = onlineRaftMembership(); raft.membership.allowLearners = true;
    const ClusterConfig config{members, ReplicationMode::MIRROR, {},
        ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 3000}, raft};
    auto joiningMembers = members;
    if (snapshotFailure) { joiningMembers.push_back(third); }
    const ClusterConfig joining{joiningMembers, ReplicationMode::MIRROR, {}, config.consistency(), raft, {}, 0, config.clusterId()};
    std::array<akkaradb::engine::AkkEngineOptions, 3> options;
    std::array<std::unique_ptr<AkkEngine>, 3> engines;
    for (size_t n = 0; n < 3; ++n) {
        auto& opt = options[n]; opt.paths.dataDir = dir / std::to_string(n + 1);
        opt.components.clusterEnabled = true; opt.components.blobEnabled = false;
        opt.cluster.config = n == 2 ? joining : config; opt.cluster.runtime.transportMode = TransportMode::PLAIN;
        opt.cluster.runtime.raftSnapshot.enabled = snapshotFailure;
        opt.cluster.runtime.raftSnapshot.minLogEntries = 1;
        writeNodeIdFile(opt.paths.dataDir / "node.id", n + 1);
        if (!snapshotFailure || n != 2) { engines[n] = AkkEngine::open(opt); }
    }
    AkkEngine* healthy[]{engines[0].get(), engines[1].get()};
    AKK_CLUSTER_CHECK(waitUntil([&] { return findEngineLeader(healthy) != nullptr; }, std::chrono::seconds{15}));
    auto* leader = findEngineLeader(healthy);
    const auto key = snapshotFailure ? "snapshot-data" : "poison";
    leader->put(bytesOf(key), bytesOf("committed"));
    if (snapshotFailure) {
        AKK_CLUSTER_CHECK(waitUntil([&] { return leader->stats().cluster.snapshotIndex > 0; }));
        engines[2] = AkkEngine::open(options[2]); leader->addClusterLearner(third);
    }
    const bool failed = waitUntil([&] { return engines[2]->stats().cluster.lastFailure == akkaradb::engine::ClusterFailureCode::RAFT_APPLY; }, std::chrono::seconds{15});
    if (!failed) {
        const auto observed = engines[2]->stats().cluster;
        std::fprintf(stderr, "Native failure attempts=%u role=%u health=%u failure=%u applied=%llu commit=%llu snapshot=%llu\n",
            nativeFailureAttempts->load(), observed.role, static_cast<unsigned>(observed.health), static_cast<unsigned>(observed.lastFailure),
            static_cast<unsigned long long>(observed.appliedIndex), static_cast<unsigned long long>(observed.commitIndex), static_cast<unsigned long long>(observed.snapshotIndex));
    }
    AKK_CLUSTER_CHECK(failed);
    bool readRejected = false, writeRejected = false;
    try { (void)engineGet(*engines[2], key); } catch (const std::runtime_error&) { readRejected = true; }
    try { engines[2]->put(bytesOf("after-failure"), bytesOf("no")); } catch (const std::runtime_error&) { writeRejected = true; }
    AKK_CLUSTER_CHECK(readRejected && writeRejected && *nativeFailureAttempts == 1);
    leader->put(bytesOf("healthy"), bytesOf("progress"));
    engines[2]->close(); engines[2].reset();
    (void)registerClusterRuntimeFactory([](std::filesystem::path path, ClusterConfig cfg, uint64_t id,
        ClusterEngineCallbacks callbacks, ClusterRuntimeOptions runtime) -> std::unique_ptr<IClusterRuntime> {
        return ClusterRuntime::create(std::move(path), std::move(cfg), id, std::move(callbacks), std::move(runtime));
    });
    engines[2] = AkkEngine::open(options[2]);
    AKK_CLUSTER_CHECK(waitUntil([&] { try { return textOf(*engineGet(*engines[2], key)) == "committed"; } catch (...) { return false; } }));
    for (auto& engine : engines) { engine->close(); }
}

struct ClosingApplyGate {
    std::promise<void> entered, release;
    std::shared_future<void> released = release.get_future().share();
};
std::shared_ptr<ClosingApplyGate> closingApplyGate;
void testClosingCannotAcknowledgeSkippedApplication() {
    using akkaradb::engine::AkkEngine;
    closingApplyGate = std::make_shared<ClosingApplyGate>();
    AKK_CLUSTER_CHECK(registerClusterRuntimeFactory([](std::filesystem::path dir, ClusterConfig config, uint64_t id,
        ClusterEngineCallbacks callbacks, ClusterRuntimeOptions options) -> std::unique_ptr<IClusterRuntime> {
        callbacks.apply = [gate = closingApplyGate, apply = std::move(callbacks.apply)](uint64_t seq,
            ReplOpType op, auto key, auto value, uint8_t flags, uint64_t source, uint64_t timestamp) {
            gate->entered.set_value(); gate->released.wait(); apply(seq, op, key, value, flags, source, timestamp);
        };
        return ClusterRuntime::create(std::move(dir), std::move(config), id, std::move(callbacks), std::move(options));
    }));
    auto restore = std::shared_ptr<void>{reinterpret_cast<void*>(1), [](void*) {
        closingApplyGate.reset();
        (void)registerClusterRuntimeFactory([](std::filesystem::path dir, ClusterConfig config, uint64_t id,
            ClusterEngineCallbacks callbacks, ClusterRuntimeOptions options) -> std::unique_ptr<IClusterRuntime> {
            return ClusterRuntime::create(std::move(dir), std::move(config), id, std::move(callbacks), std::move(options));
        });
    }};
    const auto dir = makeTempDir("close-committed-application");
    akkaradb::engine::AkkEngineOptions options; options.paths.dataDir = dir; options.components.clusterEnabled = true;
    options.components.blobEnabled = false; options.wal.shardCount = 1;
    options.cluster.config = ClusterConfig{{node(1, 33021, 33031)}, ReplicationMode::MIRROR, {},
        ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM}};
    options.cluster.runtime.transportMode = TransportMode::PLAIN; options.cluster.runtime.requests.enabled = true;
    writeNodeIdFile(dir / "node.id", 1); auto engine = AkkEngine::open(options); const auto id = engine->newRequestId();
    auto write = std::async(std::launch::async, [&] { return engine->putWithRequest(id, bytesOf("closing"), bytesOf("committed")); });
    auto entered = closingApplyGate->entered.get_future();
    if (entered.wait_for(std::chrono::seconds{5}) != std::future_status::ready) {
        closingApplyGate->release.set_value(); throw std::runtime_error("close/apply test did not reach committed application");
    }
    auto close = std::async(std::launch::async, [&] { engine->close(); });
    std::this_thread::sleep_for(std::chrono::milliseconds{100}); closingApplyGate->release.set_value();
    bool rejected = false;
    try { (void)write.get(); } catch (const std::runtime_error&) { rejected = true; }
    close.get(); AKK_CLUSTER_CHECK(rejected);
    (void)registerClusterRuntimeFactory([](std::filesystem::path path, ClusterConfig cfg, uint64_t nodeId,
        ClusterEngineCallbacks callbacks, ClusterRuntimeOptions runtime) -> std::unique_ptr<IClusterRuntime> {
        return ClusterRuntime::create(std::move(path), std::move(cfg), nodeId, std::move(callbacks), std::move(runtime));
    });
    engine = AkkEngine::open(options);
    AKK_CLUSTER_CHECK(engine->queryRequest(id).status == ClusterRequestStatus::APPLIED);
    AKK_CLUSTER_CHECK(textOf(*engineGet(*engine, "closing")) == "committed"); engine->close();
}

void testRetainedTransactionByteAdmission() {
    using akkaradb::engine::AkkEngine;
    const auto dir = makeTempDir("transaction-result-byte-admission");
    akkaradb::engine::AkkEngineOptions options; options.paths.dataDir = dir;
    options.components.clusterEnabled = true; options.components.blobEnabled = false; options.wal.shardCount = 1;
    options.cluster.config = ClusterConfig{{node(1, 33041, 33051)}, ReplicationMode::MIRROR, {},
        ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM}};
    options.cluster.runtime.transportMode = TransportMode::PLAIN; options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
    options.cluster.runtime.transactions.mode = ClusterTransactionMode::TWO_PHASE_COMMIT;
    options.cluster.runtime.transactions.maxRetainedResultBytes = 600;
    writeNodeIdFile(dir / "node.id", 1); auto engine = AkkEngine::open(options);
    auto transaction = engine->begin(); const auto id = *transaction.clusterId();
    transaction.put(bytesOf("quota"), bytesOf("retained")); transaction.end();
    bool rejected = false;
    try { auto other = engine->begin(); } catch (const akkaradb::engine::TransactionConflict&) { rejected = true; }
    AKK_CLUSTER_CHECK(rejected && engine->transactionOutcome(id).finished);
    engine->put(bytesOf("after-quota"), bytesOf("usable"));
    AKK_CLUSTER_CHECK(textOf(*engineGet(*engine, "quota")) == "retained"); engine->close();
    auto changed = options; ++changed.cluster.runtime.transactions.maxRetainedResultBytes;
    bool policyRejected = false;
    try { (void)AkkEngine::open(changed); } catch (const std::runtime_error&) { policyRejected = true; }
    AKK_CLUSTER_CHECK(policyRejected);
    engine = AkkEngine::open(options); AKK_CLUSTER_CHECK(engine->transactionOutcome(id).finished); engine->close();
}

void testRequestResultBytePolicy() {
    const auto dir = makeTempDir("request-result-byte-policy");
    const ClusterConfig config{{node(1, 33061, 33071), node(2, 33062, 33072), node(3, 33063, 33073)},
        ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM}};
    ClusterRuntimeOptions options; options.requests.enabled = true; options.requests.maxRetainedResultBytes = 414;
    auto different = options; ++different.requests.maxRetainedResultBytes;
    auto n1 = makeRuntime(dir / "1", config, 1, options), n2 = makeRuntime(dir / "2", config, 2, options);
    auto n3 = makeRuntime(dir / "3", config, 3, different);
    n1->runtime->start(); n2->runtime->start(); n3->runtime->start();
    AKK_CLUSTER_CHECK(waitUntil([&] { return n3->runtime->raftStats().peerPolicyMismatchRejects > 0; }, std::chrono::seconds{15}));
    AKK_CLUSTER_CHECK(n3->runtime->raftStats().lastLogIndex == 0);
    n3->runtime->close(); n2->runtime->close(); n1->runtime->close(); n1.reset();
    bool policyRejected = false;
    try { (void)makeRuntime(dir / "1", config, 1, different); } catch (const std::runtime_error&) { policyRejected = true; }
    AKK_CLUSTER_CHECK(policyRejected);
}

void testRetainedRequestsBeyondPendingLimit() {
    const auto dir = makeTempDir("retained-requests-scalable");
    auto raft = onlineRaftMembership(); raft.membership.allowLearners = true;
    const ClusterConfig config{{node(1, 32941, 32951), node(2, 32942, 32952)},
        ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 5000}, raft};
    ClusterRuntimeOptions options; options.requests.enabled = true; options.requests.maxPendingRequests = 128;
    options.requests.defaultRetentionMs = options.requests.maxRetentionMs = 3'600'000;
    options.raftSnapshot.minLogEntries = 64; options.raftSnapshot.minLogBytes = 1024 * 1024;
    auto n1 = makeRuntime(dir / "1", config, 1, options), n2 = makeRuntime(dir / "2", config, 2, options);
    n1->runtime->start(); n2->runtime->start(); RuntimeHarness* nodes[]{n1.get(), n2.get()};
    AKK_CLUSTER_CHECK(waitUntil([&] { return findLeader(nodes); }, std::chrono::seconds{15}));
    auto* leader = findLeader(nodes);
    std::array<uint8_t, 32> fingerprint{}; fingerprint[0] = 1;
    std::vector<ClusterRequestId> ids; std::vector<ClusterRequestResult> results;
    const auto expires = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count()) + 3'500'000;
    const ClusterMutationFactory mutation = [](uint64_t) {
        ClusterMutation value; value.key = {'k'}; value.value = {'v'}; return value;
    };
    for (unsigned first = 1; first <= 6200; first += 128) {
        std::vector<std::shared_future<ClusterRequestResult>> pending;
        for (unsigned n = first; n < first + 128 && n <= 6200; ++n) {
            ClusterRequestId id; id.expiresAtUnixMs = expires;
            for (unsigned byte = 0; byte < 4; ++byte) { id.nonce[byte] = static_cast<uint8_t>(n >> (byte * 8)); }
            ids.push_back(id); pending.push_back(leader->runtime->submitRequest(id, fingerprint, mutation));
        }
        for (auto& future : pending) {
            results.push_back(future.get()); AKK_CLUSTER_CHECK(results.back().status == ClusterRequestStatus::APPLIED);
        }
        if (first % 1024 == 1) { std::fprintf(stderr, "[retained requests] %zu\n", results.size()); }
    }
    AKK_CLUSTER_CHECK(leader->runtime->raftStats().retainedRequestResults == 6200);
    const auto writes = leader->appliedCount.load();
    AKK_CLUSTER_CHECK(leader->runtime->submitRequest(ids.front(), fingerprint, mutation).get().sequence == results.front().sequence);
    AKK_CLUSTER_CHECK(leader->appliedCount == writes);
    AKK_CLUSTER_CHECK(waitUntil([&] { return leader->runtime->raftStats().snapshotIndex > results[5000].logIndex; }));
    auto third = node(3, 32943, 32953); third.capabilities |= RAFT_LEARNER;
    auto members = config.nodes(); members.push_back(third);
    const ClusterConfig joining{members, ReplicationMode::MIRROR, {}, config.consistency(), raft, {}, 0, config.clusterId()};
    auto n3 = makeRuntime(dir / "3", joining, 3, options); n3->runtime->start(); leader->runtime->addRaftLearner(third);
    AKK_CLUSTER_CHECK(waitUntil([&] { return n3->runtime->raftStats().retainedRequestResults == 6200 && n3->snapshotsInstalled > 0; }, std::chrono::seconds{30}));
    const auto snapshotIndex = n3->runtime->raftStats().snapshotIndex;
    AKK_CLUSTER_CHECK(waitUntil([&] {
        const auto stats = leader->runtime->raftStats();
        return std::ranges::any_of(stats.peers, [&](const auto& peer) { return peer.nodeId == 3 && peer.matchIndex == stats.lastLogIndex; });
    }, std::chrono::seconds{30}));
    leader->runtime->promoteRaftLearner(3); leader->runtime->transferRaftLeadership(3);
    AKK_CLUSTER_CHECK(waitUntil([&] { return n3->runtime->role() == NodeRole::PRIMARY; }));
    const auto replay = n3->runtime->submitRequest(ids.front(), fingerprint, mutation).get();
    AKK_CLUSTER_CHECK(replay.sequence == results.front().sequence && replay.logIndex == results.front().logIndex);
    n3->runtime->close(); n3.reset(); n3 = makeRuntime(dir / "3", joining, 3, options); n3->runtime->start();
    AKK_CLUSTER_CHECK(n3->runtime->raftStats().snapshotIndex >= snapshotIndex);
    RuntimeHarness* restarted[]{n1.get(), n2.get(), n3.get()};
    AKK_CLUSTER_CHECK(waitUntil([&] { return findLeader(restarted) != nullptr; }, std::chrono::seconds{15}));
    auto* newLeader = findLeader(restarted);
    if (newLeader != n3.get()) {
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto stats = newLeader->runtime->raftStats();
            return std::ranges::any_of(stats.peers, [&](const auto& peer) { return peer.nodeId == 3 && peer.matchIndex == stats.lastLogIndex; });
        }, std::chrono::seconds{30}));
        newLeader->runtime->transferRaftLeadership(3);
    }
    AKK_CLUSTER_CHECK(waitUntil([&] { return n3->runtime->role() == NodeRole::PRIMARY; }, std::chrono::seconds{15}));
    AKK_CLUSTER_CHECK(n3->runtime->queryRequest(ids.back()).sequence == results.back().sequence);
    n3->runtime->close(); n2->runtime->close(); n1->runtime->close();
}

void testRetainedTransactionsBeyondOldLimit() {
    using akkaradb::engine::AkkEngine;
    const auto dir = makeTempDir("retained-transactions-scalable");
    akkaradb::engine::AkkEngineOptions options; options.paths.dataDir = dir;
    options.components.clusterEnabled = true; options.components.blobEnabled = false;
    options.cluster.config = ClusterConfig{{node(1, 32961, 32971)}, ReplicationMode::MIRROR, {},
        ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM}, {}, {}, 0, testClusterId(9701)};
    options.cluster.runtime.transportMode = TransportMode::PLAIN;
    options.cluster.runtime.transactions.mode = ClusterTransactionMode::TWO_PHASE_COMMIT;
    options.cluster.runtime.transactions.recoveryIntervalMs = 1000;
    options.wal.shardCount = 1;
    options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
    options.transactions.maxLifetimeMs = 3'600'000;
    options.cluster.runtime.raftSnapshot.minLogEntries = 32;
    writeNodeIdFile(dir / "node.id", 1);
    auto engine = AkkEngine::open(options);
    AKK_CLUSTER_CHECK(waitUntil([&] { return engine->stats().cluster.role == static_cast<uint32_t>(NodeRole::PRIMARY); }));
    std::vector<ClusterTransactionId> ids;
    for (unsigned n = 0; n < 1050; ++n) {
        auto transaction = engine->begin(); ids.push_back(*transaction.clusterId());
        if (n == 0 || n == 1049) { transaction.put(bytesOf("retained"), bytesOf(std::to_string(n))); }
        transaction.end();
        AKK_CLUSTER_CHECK(engine->transactionOutcome(ids.back()).finished);
        if (n % 250 == 249) { std::fprintf(stderr, "[retained transactions] %u\n", n + 1); }
    }
    AKK_CLUSTER_CHECK(engine->transactionOutcome(ids.front()).status == ClusterTransactionStatus::COMMITTED);
    engine->close(); engine = AkkEngine::open(options);
    AKK_CLUSTER_CHECK(waitUntil([&] { try { return engine->transactionOutcome(ids.front()).finished; } catch (...) { return false; } }));
    AKK_CLUSTER_CHECK(engine->transactionOutcome(ids.back()).status == ClusterTransactionStatus::COMMITTED);
    AKK_CLUSTER_CHECK(textOf(*engineGet(*engine, "retained")) == "1049"); engine->close();
}

std::function<std::optional<ClusterSnapshot>()> memorySnapshotCapture;
void testMemorySnapshotDoesNotBlockConsensus() {
    using akkaradb::engine::AkkEngine;
    AKK_CLUSTER_CHECK(registerClusterRuntimeFactory([](std::filesystem::path dir, ClusterConfig config, uint64_t id,
        ClusterEngineCallbacks callbacks, ClusterRuntimeOptions options) -> std::unique_ptr<IClusterRuntime> {
        if (id == 1) { memorySnapshotCapture = callbacks.exportSnapshot; }
        return ClusterRuntime::create(std::move(dir), std::move(config), id, std::move(callbacks), std::move(options));
    }));
    auto restore = std::shared_ptr<void>{reinterpret_cast<void*>(1), [](void*) {
        memorySnapshotCapture = {};
        (void)registerClusterRuntimeFactory([](std::filesystem::path dir, ClusterConfig config, uint64_t id,
            ClusterEngineCallbacks callbacks, ClusterRuntimeOptions options) -> std::unique_ptr<IClusterRuntime> {
            return ClusterRuntime::create(std::move(dir), std::move(config), id, std::move(callbacks), std::move(options));
        });
    }};
    const auto dir = makeTempDir("memory-snapshot-consensus");
    const ClusterConfig config{{node(1, 32981, 32991), node(2, 32982, 32992), node(3, 32983, 32993)}, ReplicationMode::MIRROR, {},
        ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM}};
    akkaradb::engine::AkkEngineOptions options; options.paths.dataDir = dir;
    options.components.clusterEnabled = true; options.components.sstEnabled = options.components.manifestEnabled = false;
    options.components.blobEnabled = false; options.cluster.config = config;
    options.cluster.runtime.transportMode = TransportMode::PLAIN;
    options.cluster.runtime.memoryOnlySnapshot.maxPinnedGenerations = 1;
    options.cluster.runtime.raftSnapshot.minLogEntries = 1;
    options.cluster.runtime.raftSnapshot.maxIntervalMs = 100;
    options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
    std::array<std::unique_ptr<AkkEngine>, 3> engines;
    for (size_t n = 0; n < engines.size(); ++n) {
        options.paths.dataDir = dir / std::to_string(n + 1); options.wal.shardCount = 1;
        writeNodeIdFile(options.paths.dataDir / "node.id", n + 1); engines[n] = AkkEngine::open(options);
    }
    auto& engine = engines[0];
    AKK_CLUSTER_CHECK(waitUntil([&] { return engine->stats().cluster.role == static_cast<uint32_t>(NodeRole::PRIMARY); }, std::chrono::seconds{15}));
    engine->put(bytesOf("pinned"), bytesOf("old"));
    std::optional<ClusterSnapshot> held;
    AKK_CLUSTER_CHECK(waitUntil([&] { held = memorySnapshotCapture(); return held.has_value(); }));
    const auto term = engine->stats().cluster.raftTerm;
    engine->put(bytesOf("pinned"), bytesOf("new"));
    auto unavailable = std::async(std::launch::async, [] { return memorySnapshotCapture(); });
    const auto available = unavailable.wait_for(std::chrono::seconds{1}) == std::future_status::ready;
    if (!available) { held.reset(); }
    AKK_CLUSTER_CHECK(available && !unavailable.get());
    std::this_thread::sleep_for(std::chrono::milliseconds{1500});
    auto write = std::async(std::launch::async, [&] { engine->put(bytesOf("progress"), bytesOf("ok")); return engineGet(*engine, "progress"); });
    const auto progressed = write.wait_for(std::chrono::seconds{2}) == std::future_status::ready;
    if (!progressed) { held.reset(); }
    AKK_CLUSTER_CHECK(progressed && textOf(*write.get()) == "ok");
    AKK_CLUSTER_CHECK(engine->stats().cluster.health == akkaradb::engine::ClusterHealthState::HEALTHY);
    AKK_CLUSTER_CHECK(engines[1]->stats().cluster.raftTerm == term && engines[2]->stats().cluster.raftTerm == term);
    held.reset(); AKK_CLUSTER_CHECK(waitUntil([&] { return engine->stats().cluster.snapshotIndex > 1; }));
    held = memorySnapshotCapture(); engine->put(bytesOf("close-pinned"), bytesOf("ok"));
    auto close = std::async(std::launch::async, [&] { engine->close(); });
    const auto closed = close.wait_for(std::chrono::seconds{3}) == std::future_status::ready;
    held.reset(); AKK_CLUSTER_CHECK(closed); close.get();
    engines[1]->close(); engines[2]->close();
}

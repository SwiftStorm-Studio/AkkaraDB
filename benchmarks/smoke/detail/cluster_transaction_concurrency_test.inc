/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
std::vector<std::string> transactionPartitionKeys(const ClusterConfig& config) {
    std::vector<std::string> keys(config.partition().partitionCount);
    for (unsigned i = 0; i < 10000; ++i) {
        auto key = "dynamic-" + std::to_string(i);
        const auto index = akkaradb::engine::cluster::detail::partitionForKey(bytesOf(key), config.partition().partitionCount);
        if (keys[index].empty()) { keys[index] = std::move(key); }
    }
    AKK_CLUSTER_CHECK(std::ranges::all_of(keys, [](const auto& key) { return !key.empty(); })); return keys;
}

void testClusterTransactionReservations() {
    using namespace akkaradb::engine;
    const auto dir = makeTempDir("cluster-transaction-reservations");
    AkkEngineOptions options;
    options.paths.dataDir = dir;
    options.components.sstEnabled = options.components.manifestEnabled = false;
    options.components.versionLogEnabled = options.components.clusterEnabled = true;
    options.cluster.config = ClusterConfig{{node(1, 32901, 32911)}, ReplicationMode::MIRROR, {},
        ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 5000}, {}, {}, 0, testClusterId(9637)};
    options.cluster.runtime.clusterGroupId = 9637;
    options.cluster.runtime.routingMode = ClusterRoutingMode::FORWARD;
    options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
    options.cluster.runtime.raftBlobPolicy = RaftBlobPolicy::RAFT_LOG;
    options.cluster.runtime.transactions.mode = ClusterTransactionMode::TWO_PHASE_COMMIT;
    options.cluster.runtime.transactions.timeoutMs = 10000;
    options.cluster.runtime.raftHeartbeatIntervalMs = 30;
    writeNodeIdFile(dir / "node.id", 1);
    auto engine = AkkEngine::open(options);
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { engine->put(bytesOf("reserved"), bytesOf("old")); return true; }
        catch (const std::exception&) { return false; }
    }, std::chrono::seconds{15}));
    const auto rejected = [&](const std::string& key) {
        try { engine->put(bytesOf(key), bytesOf("conflict")); }
        catch (const ClusterRoutingError& error) { return error.code == ClusterRoutingCode::TRANSACTION_CONFLICT; }
        return false;
    };
    {
        auto tx = engine->begin();
        AKK_CLUSTER_CHECK(textOf(*tx.get(bytesOf("reserved"))) == "old");
        AKK_CLUSTER_CHECK(!tx.get(bytesOf("missing")));
        AKK_CLUSTER_CHECK(rejected("reserved") && rejected("missing"));
        engine->put(bytesOf("unrelated"), bytesOf("allowed"));
        engine->remove(bytesOf("unrelated"));
        auto disjoint = engine->begin();
        disjoint.put(bytesOf("unrelated"), bytesOf("parallel")); disjoint.end();
        AKK_CLUSTER_CHECK(textOf(*engine->get(bytesOf("unrelated"))) == "parallel");
        engine->remove(bytesOf("unrelated"));
        tx.put(bytesOf("reserved"), bytesOf("committed")); tx.end();
        AKK_CLUSTER_CHECK(textOf(*engine->get(bytesOf("reserved"))) == "committed");
        engine->put(bytesOf("missing"), bytesOf("released")); engine->remove(bytesOf("missing"));
    }
    {
        auto tx = engine->begin();
        AKK_CLUSTER_CHECK(tx.count(bytesOf("range/"), bytesOf("range0")) == 0);
        AKK_CLUSTER_CHECK(rejected("range/new"));
        auto disjoint = engine->begin();
        disjoint.put(bytesOf("range0"), bytesOf("boundary")); disjoint.end();
        auto overlapping = engine->begin(); bool rangeConflict = false;
        try { overlapping.put(bytesOf("range/new"), bytesOf("invalid")); }
        catch (const TransactionConflict&) { rangeConflict = true; }
        AKK_CLUSTER_CHECK(rangeConflict && !overlapping.active());
        auto overlappingRange = engine->begin(); rangeConflict = false;
        try { (void)overlappingRange.count(bytesOf("range/a"), bytesOf("range/z")); }
        catch (const TransactionConflict&) { rangeConflict = true; }
        AKK_CLUSTER_CHECK(rangeConflict && !overlappingRange.active());
        engine->put(bytesOf("range0"), bytesOf("boundary")); engine->remove(bytesOf("range0"));
        tx.rollback();
        engine->put(bytesOf("range/new"), bytesOf("released")); engine->remove(bytesOf("range/new"));
    }
    engine->close(); engine = AkkEngine::open(options);
    auto tx = engine->begin();
    AKK_CLUSTER_CHECK(textOf(*tx.get(bytesOf("reserved"))) == "committed");
    AKK_CLUSTER_CHECK(rejected("reserved"));
    engine->put(bytesOf("unrelated"), bytesOf("after-reopen"));
    tx.end();
    engine->close();
}

void testClusterTransactionConcurrency(bool secure) {
    using namespace akkaradb::engine;
    std::fprintf(stderr, "[transactions] dynamic participants and replicated concurrency %s\n", secure ? "SECURE" : "PLAIN");
    ClusterTransactionFixture fixture{makeTempDir("cluster-transaction-concurrency"), true, secure, 300000, true, 0, {}, 2};
    auto& engine = *fixture.engines[0]; const auto keys = transactionPartitionKeys(fixture.config);
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { for (const auto& key : keys) { engine.put(bytesOf(key), bytesOf("old")); } return true; }
        catch (const std::exception&) { return false; }
    }, std::chrono::seconds{20}));
    auto first = fixture.begin(), second = fixture.begin(), same = fixture.begin();
    engine.put(bytesOf(keys[0]), bytesOf("after-begin")); // begin has no partition reservations
    AKK_CLUSTER_CHECK(textOf(*first.get(bytesOf(keys[0]))) == "after-begin");
    std::string samePartition;
    for (unsigned i = 0; i < 10000; ++i) {
        auto candidate = "same-partition-" + std::to_string(i);
        if (akkaradb::engine::cluster::detail::partitionForKey(bytesOf(candidate), fixture.config.partition().partitionCount) == 0) {
            samePartition = std::move(candidate); break;
        }
    }
    AKK_CLUSTER_CHECK(!samePartition.empty());
    engine.put(bytesOf(samePartition), bytesOf("allowed"));
    AKK_CLUSTER_CHECK(textOf(*engine.get(bytesOf(samePartition))) == "allowed");
    engine.remove(bytesOf(samePartition));
    same.put(bytesOf(samePartition), bytesOf("same-partition"));
    AKK_CLUSTER_CHECK(textOf(*second.get(bytesOf(keys[1]))) == "old");
    engine.put(bytesOf(keys[2]), bytesOf("unrelated"));
    bool ordinaryBlocked = false;
    try { engine.put(bytesOf(keys[0]), bytesOf("race")); }
    catch (const ClusterRoutingError& error) { ordinaryBlocked = error.code == ClusterRoutingCode::TRANSACTION_CONFLICT; }
    AKK_CLUSTER_CHECK(ordinaryBlocked && textOf(*first.get(bytesOf(keys[0]))) == "after-begin");
    first.put(bytesOf(keys[0]), bytesOf("first")); second.put(bytesOf(keys[1]), bytesOf("second"));
    auto commitFirst = std::async(std::launch::async, [tx = std::move(first)]() mutable { tx.end(); });
    auto commitSecond = std::async(std::launch::async, [tx = std::move(second)]() mutable { tx.end(); });
    auto commitSame = std::async(std::launch::async, [tx = std::move(same)]() mutable { tx.end(); });
    commitFirst.get(); commitSecond.get(); commitSame.get();
    AKK_CLUSTER_CHECK(textOf(*engine.get(bytesOf(samePartition))) == "same-partition");
    engine.remove(bytesOf(samePartition));
    AKK_CLUSTER_CHECK(textOf(*engine.get(bytesOf(keys[0]))) == "first" && textOf(*engine.get(bytesOf(keys[1]))) == "second");
    {
        // Sparse participant indices must not index an array sized to the enlisted count.
        auto sparse = fixture.begin(); sparse.put(bytesOf(keys.back()), bytesOf("sparse")); sparse.end();
        AKK_CLUSTER_CHECK(textOf(*engine.get(bytesOf(keys.back()))) == "sparse");
    }
    {
        auto owner = fixture.begin(), contender = fixture.begin();
        (void)owner.get(bytesOf(keys[0])); (void)contender.get(bytesOf(keys[1]));
        const auto contenderId = *contender.clusterId();
        bool conflict = false;
        try { contender.put(bytesOf(keys[0]), bytesOf("invalid")); }
        catch (const TransactionConflict&) { conflict = true; }
        AKK_CLUSTER_CHECK(conflict && !contender.active());
        const auto outcome = engine.transactionOutcome(contenderId);
        AKK_CLUSTER_CHECK(outcome.status == ClusterTransactionStatus::ABORTED && outcome.finished);
        // The failed cross-enlistment releases the contender's earlier reservation.
        owner.put(bytesOf(keys[1]), bytesOf("released")); owner.end();
    }
    {
        // Read-dependent cross-partition writes cannot both commit (write skew).
        auto left = fixture.begin(), right = fixture.begin();
        (void)left.get(bytesOf(keys[0])); (void)right.get(bytesOf(keys[1]));
        bool rejected = false;
        try { left.put(bytesOf(keys[1]), bytesOf("skew")); } catch (const TransactionConflict&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected && !left.active());
        right.put(bytesOf(keys[0]), bytesOf("serial")); right.end();
        AKK_CLUSTER_CHECK(textOf(*engine.get(bytesOf(keys[0]))) == "serial" && textOf(*engine.get(bytesOf(keys[1]))) == "released");
    }
    {
        auto reader = fixture.begin();
        AKK_CLUSTER_CHECK(!reader.get(bytesOf("missing-key")));
        bool rejected = false;
        try { engine.put(bytesOf("missing-key"), bytesOf("phantom")); }
        catch (const ClusterRoutingError& error) { rejected = error.code == ClusterRoutingCode::TRANSACTION_CONFLICT; }
        AKK_CLUSTER_CHECK(rejected); reader.rollback();
        engine.put(bytesOf("missing-key"), bytesOf("released")); engine.remove(bytesOf("missing-key"));
    }
    {
        auto scanner = fixture.begin();
        AKK_CLUSTER_CHECK(scanner.count(bytesOf("bounded/"), bytesOf("bounded0")) == 0);
        bool rejected = false;
        try { engine.put(bytesOf("bounded/new"), bytesOf("phantom")); }
        catch (const ClusterRoutingError& error) { rejected = error.code == ClusterRoutingCode::TRANSACTION_CONFLICT; }
        AKK_CLUSTER_CHECK(rejected);
        engine.put(bytesOf("bounded0"), bytesOf("outside"));
        engine.remove(bytesOf("bounded0"));
        scanner.rollback();
        engine.put(bytesOf("bounded/new"), bytesOf("released")); engine.remove(bytesOf("bounded/new"));
    }
    {
        auto scanner = fixture.begin();
        AKK_CLUSTER_CHECK(scanner.count(bytesOf(keys[0]), bytesOf(keys[0])) == 0);
        engine.put(bytesOf(keys[0]), bytesOf("empty-range"));
        AKK_CLUSTER_CHECK(scanner.count() == keys.size());
        auto other = fixture.begin(); bool rejected = false;
        try { (void)other.get(bytesOf(keys.back())); } catch (const TransactionConflict&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected && !other.active());
        scanner.end();
    }
    fixture.close(); fixture.open();
    auto recovered = fixture.begin();
    AKK_CLUSTER_CHECK(recovered.count() == keys.size() && textOf(*recovered.get(bytesOf(keys.back()))) == "sparse");
    recovered.end();
}

void testClusterTransactionUnusedPartitionFailure() {
    using namespace akkaradb::engine;
    std::fprintf(stderr, "[transactions] unused partition outage and independent recovery\n");
    ClusterTransactionFixture fixture{makeTempDir("cluster-transaction-local-failure"), true, false, 300000, true, 0, {}, 2};
    const auto keys = transactionPartitionKeys(fixture.config);
    const auto members = akkaradb::engine::cluster::detail::partitionTargets(fixture.config, bytesOf(keys[0]));
    uint64_t offline = 1;
    while (std::ranges::any_of(members, [&](const auto& member) { return member.nodeId == offline; })) { ++offline; }
    const auto caller = members.front().nodeId - 1;
    auto& engine = *fixture.engines[caller];
    std::string affected;
    for (const auto& key : keys) {
        const auto targets = akkaradb::engine::cluster::detail::partitionTargets(fixture.config, bytesOf(key));
        if (std::ranges::any_of(targets, [&](const auto& member) { return member.nodeId == offline; })) { affected = key; break; }
    }
    AKK_CLUSTER_CHECK(!affected.empty());
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { engine.put(bytesOf(keys[0]), bytesOf("healthy")); engine.put(bytesOf(affected), bytesOf("before")); return true; }
        catch (const std::exception&) { return false; }
    }, std::chrono::seconds{20}));
    auto stranded = engine.begin(); const auto strandedId = *stranded.clusterId();
    stranded.put(bytesOf(affected), bytesOf("never-committed"));
    fixture.engines[offline - 1]->close(); fixture.engines[offline - 1].reset();
    // Abort persists even though release needs the unavailable participant quorum.
    {
        akkaradb::engine::cluster::detail::ReconfigurationDeadline cleanup{2000};
        stranded.rollback();
    }
    std::optional<ClusterTransactionId> healthyId;
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try {
            if (healthyId) {
                const auto outcome = engine.transactionOutcome(*healthyId);
                if (outcome.status == ClusterTransactionStatus::ABORTED && outcome.finished) { healthyId.reset(); return false; }
                return outcome.status == ClusterTransactionStatus::COMMITTED && outcome.finished;
            }
            auto healthy = engine.begin();
            healthy.put(bytesOf(keys[0]), bytesOf("independent")); healthyId = healthy.clusterId();
            healthy.end(); return true;
        } catch (const TransactionOutcomeUnknown& error) {
            healthyId = error.transactionId(); return false;
        } catch (const TransactionConflict&) { return false; }
        catch (const std::exception&) { return false; }
    }, std::chrono::seconds{20}));
    AKK_CLUSTER_CHECK(engine.transactionOutcome(strandedId).status == ClusterTransactionStatus::ABORTED);
    fixture.close(); fixture.open();
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { return fixture.engines[caller]->transactionOutcome(strandedId).finished; } catch (const std::exception&) { return false; }
    }, std::chrono::seconds{20}));
    auto proof = fixture.engines[caller]->begin();
    AKK_CLUSTER_CHECK(textOf(*proof.get(bytesOf(keys[0]))) == "independent" && textOf(*proof.get(bytesOf(affected))) == "before");
    proof.end();
}

std::shared_ptr<ClusterTransactionApplyGate> transactionPrepareForwardGate;
struct TransactionPrepareForwardGateScope {
    std::shared_ptr<ClusterTransactionApplyGate> gate = std::make_shared<ClusterTransactionApplyGate>();
    TransactionPrepareForwardGateScope() {
        transactionPrepareForwardGate = gate;
        AKK_CLUSTER_CHECK(registerClusterRuntimeFactory([](std::filesystem::path dir, ClusterConfig config,
            uint64_t id, ClusterEngineCallbacks callbacks, ClusterRuntimeOptions options) -> std::unique_ptr<IClusterRuntime> {
            const auto pause = transactionPrepareForwardGate;
            if (callbacks.forward) {
                callbacks.forward = [pause, forward = std::move(callbacks.forward)](const ForwardRequest& request) {
                    if (request.operation == ForwardOperation::TRANSACTION && request.entries.size() == 1 &&
                        akkaradb::engine::cluster::detail::decodeTransactionMessage(request.entries[0].value).action ==
                            akkaradb::engine::cluster::detail::TransactionAction::APPEND) { pause->pause(0); }
                    return forward(request);
                };
            }
            return ClusterRuntime::create(std::move(dir), std::move(config), id, std::move(callbacks), std::move(options));
        }));
    }
    ~TransactionPrepareForwardGateScope() {
        gate->release();
        transactionPrepareForwardGate.reset();
        (void)registerClusterRuntimeFactory([](std::filesystem::path dir, ClusterConfig config,
            uint64_t id, ClusterEngineCallbacks callbacks, ClusterRuntimeOptions options) -> std::unique_ptr<IClusterRuntime> {
            return ClusterRuntime::create(std::move(dir), std::move(config), id, std::move(callbacks), std::move(options));
        });
    }
};
void testClusterTransactionConcurrentCapacity() {
    using namespace akkaradb::engine;
    std::fprintf(stderr, "[transactions] MIRROR prepare capacity survives concurrent coordinator records\n");
    TransactionPrepareForwardGateScope factory;
    ClusterTransactionFixture fixture{makeTempDir("cluster-transaction-concurrent-capacity"), false, false, 300000, false, 1024};
    auto initial = fixture.begin(); initial.end();
    size_t caller = fixture.engines[0]->stats().cluster.leaderNodeId == 1 ? 1 : 0;
    auto prepared = fixture.engines[caller]->begin(); prepared.put(bytesOf("capacity"), std::vector<uint8_t>(400, 0x26));
    factory.gate->enable(0);
    auto commit = std::async(std::launch::async, [tx = std::move(prepared)]() mutable { tx.end(); });
    const bool paused = factory.gate->await();
    std::vector<ClusterTransactionFixture::Engine::Transaction> idle;
    bool capacityRejected = false; std::exception_ptr failure;
    try {
        if (paused) {
            for (unsigned i = 0; i < 24; ++i) {
                try { idle.push_back(fixture.engines[caller]->begin()); }
                catch (const TransactionConflict&) { capacityRejected = true; break; }
            }
        }
    } catch (...) { failure = std::current_exception(); }
    factory.gate->release(); commit.get();
    if (failure) { std::rethrow_exception(failure); }
    AKK_CLUSTER_CHECK(paused && capacityRejected && !idle.empty());
    for (auto& tx : idle) { tx.end(); }
    AKK_CLUSTER_CHECK(fixture.engines[0]->get(bytesOf("capacity")) == std::vector<uint8_t>(400, 0x26));
    fixture.close(); fixture.open();
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { return fixture.engines[0]->get(bytesOf("capacity")) == std::vector<uint8_t>(400, 0x26); }
        catch (const std::exception&) { return false; }
    }, std::chrono::seconds{15}));
}

void runClusterTransactionConcurrencyTests() {
    testClusterTransactionConcurrency(false); testClusterTransactionConcurrency(true);
    testClusterTransactionUnusedPartitionFailure(); testClusterTransactionConcurrentCapacity();
}

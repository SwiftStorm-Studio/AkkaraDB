/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
struct ClusterTransactionFixture {
    using Engine = akkaradb::engine::AkkEngine;
    std::filesystem::path dir;
    ClusterConfig config;
    std::vector<akkaradb::engine::AkkEngineOptions> options;
    std::vector<std::unique_ptr<Engine>> engines;
    ClusterTransactionFixture(std::filesystem::path directory, bool partitioned, bool secure = false, uint32_t lifetime = 300000, bool snapshots = false, uint64_t journalBytes = 0,
        std::function<void(size_t, akkaradb::engine::AkkEngineOptions&)> configure = {}, uint16_t replicationFactor = 1)
        : dir{std::move(directory)} {
        auto members = std::vector{node(1, 31401, 31411), node(2, 31402, 31412), node(3, 31403, 31413)};
        for (size_t i = 0; i < members.size(); ++i) { members[i].partitionReplBasePort = static_cast<uint16_t>(31500 + 16 * i); }
        if (!partitioned) { for (auto& member : members) { member.partitionReplBasePort = 0; } }
        config = ClusterConfig{members, partitioned ? ReplicationMode::PARTITIONED : ReplicationMode::MIRROR, {},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 5000}, {}, {}, 0,
            testClusterId(9601), partitioned ? PartitionOptions{.replicationFactor = replicationFactor, .partitionCount = 4} : PartitionOptions{}};
        ClusterRuntimeOptions runtime;
        runtime.clusterGroupId = 9601; runtime.routingMode = ClusterRoutingMode::FORWARD; runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
        runtime.raftHeartbeatIntervalMs = 30; runtime.forwardingTimeoutMs = 3000;
        runtime.transactions.mode = ClusterTransactionMode::TWO_PHASE_COMMIT;
        runtime.transactions.timeoutMs = 60000; runtime.transactions.recoveryIntervalMs = 50;
        if (snapshots) { runtime.raftSnapshot.minLogEntries = 2; runtime.raftSnapshot.minLogBytes = 1; runtime.raftSnapshot.maxIntervalMs = 100; }
        runtime.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
        if (secure) {
            for (size_t i = 0; i < 3; ++i) {
                const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(i + 1))}.loadOrCreate();
                runtime.secure.pinnedPeers.push_back({i + 1, identity.publicKey});
            }
        }
        options.resize(3); engines.resize(3);
        for (size_t i = 0; i < 3; ++i) {
            auto& opt = options[i]; opt.paths.dataDir = dir / std::to_string(i + 1);
            opt.components.blobEnabled = true; opt.blob.thresholdBytes = 4096;
            opt.components.sstEnabled = false; opt.components.manifestEnabled = false;
            opt.components.versionLogEnabled = true; opt.components.clusterEnabled = true;
            opt.cluster.config = config; opt.cluster.runtime = runtime; opt.cluster.runtime.raftBlobPolicy = RaftBlobPolicy::RAFT_LOG;
            opt.cluster.runtime.secure.identitySeedPath = dir / ("identity-" + std::to_string(i + 1));
            opt.transactions.maxLifetimeMs = lifetime;
            if (journalBytes) { opt.transactions.maxJournalBytes = journalBytes; }
            if (configure) { configure(i, opt); }
            writeNodeIdFile(opt.paths.dataDir / "node.id", i + 1);
        }
        open();
    }
    void open() { for (size_t i = 0; i < engines.size(); ++i) { engines[i] = Engine::open(options[i]); } }
    void close() { for (auto& engine : engines) { if (engine) { engine->close(); engine.reset(); } } }
    ~ClusterTransactionFixture() { close(); }
    Engine::Transaction begin(akkaradb::engine::TransactionIsolation mode = akkaradb::engine::TransactionIsolation::SERIALIZABLE) {
        std::optional<Engine::Transaction> result;
        std::string lastFailure;
        const bool opened = waitUntil([&] {
            try { result.emplace(engines[0]->begin({mode})); return true; }
            catch (const std::exception& error) { lastFailure = error.what(); return false; }
        }, std::chrono::seconds{15});
        if (!opened) { throw std::runtime_error("Cluster transaction begin failed: " + lastFailure); } return std::move(*result);
    }
};

std::pair<std::string, std::string> clusterTransactionKeys(const ClusterConfig& config, const std::string& prefix) {
    if (config.mode() != ReplicationMode::PARTITIONED) { return {prefix + "left", prefix + "right"}; }
    ClusterRouter router{config}; std::string first; uint64_t owner = 0;
    for (uint32_t i = 0; i < 10000; ++i) {
        const auto key = prefix + std::to_string(i); const auto targets = router.writeTargets(bytesOf(key));
        if (targets.empty()) { continue; }
        if (first.empty()) { first = key; owner = targets.front().nodeId; }
        else if (targets.front().nodeId != owner) { return {first, key}; }
    }
    throw std::runtime_error("transaction fixture must exercise different owners");
}
struct ClusterTransactionApplyGate {
    std::mutex mutex;
    std::condition_variable cv;
    uint16_t participant = 0;
    bool enabled = false, entered = false, released = false;
    std::chrono::seconds pauseTimeout{10};
    std::atomic<bool> rejectedRead{false};
    void pause(uint16_t index) {
        std::unique_lock lock{mutex};
        if (!enabled || index != participant) { return; }
        entered = true; cv.notify_all();
        if (!cv.wait_for(lock, pauseTimeout, [&] { return released; })) { throw std::runtime_error("transaction apply gate timed out"); }
    }
    void enable(uint16_t index) { std::lock_guard lock{mutex}; participant = index; enabled = true; }
    bool await(std::chrono::seconds timeout = std::chrono::seconds{10}) {
        std::unique_lock lock{mutex}; return cv.wait_for(lock, timeout, [&] { return entered; });
    }
    void release() { std::lock_guard lock{mutex}; released = true; cv.notify_all(); }
};
std::shared_ptr<ClusterTransactionApplyGate> clusterTransactionApplyGate;
struct ClusterTransactionFactoryScope {
    ClusterTransactionFactoryScope() {
        clusterTransactionApplyGate = std::make_shared<ClusterTransactionApplyGate>();
        AKK_CLUSTER_CHECK(registerClusterRuntimeFactory([](std::filesystem::path dir, ClusterConfig config,
            uint64_t id, ClusterEngineCallbacks callbacks, ClusterRuntimeOptions options) -> std::unique_ptr<IClusterRuntime> {
            if (callbacks.apply) {
                callbacks.apply = [gate = clusterTransactionApplyGate, apply = std::move(callbacks.apply)](uint64_t seq,
                    ReplOpType op, std::span<const uint8_t> key, std::span<const uint8_t> value, uint8_t flags, uint64_t source, uint64_t timestamp) {
                    if (op == ReplOpType::TRANSACTION) {
                        const auto message = akkaradb::engine::cluster::detail::decodeTransactionMessage(value);
                        if (message.action == akkaradb::engine::cluster::detail::TransactionAction::APPLY) { gate->pause(message.participant); }
                    }
                    apply(seq, op, key, value, flags, source, timestamp);
                };
            }
            if (callbacks.read) {
                callbacks.read = [gate = clusterTransactionApplyGate, read = std::move(callbacks.read)](std::span<const uint8_t> key, uint64_t seq) {
                    try { return read(key, seq); }
                    catch (const akkaradb::engine::TransactionConflict&) { gate->rejectedRead.store(true); throw; }
                };
            }
            return ClusterRuntime::create(std::move(dir), std::move(config), id, std::move(callbacks), std::move(options));
        }));
    }
    ~ClusterTransactionFactoryScope() {
        clusterTransactionApplyGate->release(); clusterTransactionApplyGate.reset();
        (void)registerClusterRuntimeFactory([](std::filesystem::path dir, ClusterConfig config,
            uint64_t id, ClusterEngineCallbacks callbacks, ClusterRuntimeOptions options) -> std::unique_ptr<IClusterRuntime> {
            return ClusterRuntime::create(std::move(dir), std::move(config), id, std::move(callbacks), std::move(options));
        });
    }
};
void testClusterTransactionVisibility() {
    std::fprintf(stderr, "[transactions] partial application stays hidden\n");
    ClusterTransactionFactoryScope factory;
    ClusterTransactionFixture fixture{makeTempDir("cluster-transactions-visibility"), true};
    auto& engine = *fixture.engines[0];
    auto [first, second] = clusterTransactionKeys(fixture.config, "visibility-");
    if (engine.revisionStreamId(bytesOf(first)) > engine.revisionStreamId(bytesOf(second))) { std::swap(first, second); }
    auto tx = fixture.begin();
    tx.put(bytesOf(first), bytesOf("new-first")); tx.put(bytesOf(second), bytesOf("new-second"));
    clusterTransactionApplyGate->enable(static_cast<uint16_t>(engine.revisionStreamId(bytesOf(second)) - 1));
    auto commit = std::async(std::launch::async, [tx = std::move(tx)]() mutable { tx.end(); });
    const bool paused = clusterTransactionApplyGate->await();
    bool rejected = false, scalarHistoryRejected = false, revisionHistoryRejected = false, historyRejected = false;
    if (paused) {
        try { (void)engine.get(bytesOf(first)); }
        catch (const std::exception&) { rejected = true; }
        try { (void)engine.getAt(bytesOf(first), UINT64_MAX); }
        catch (const std::exception&) { scalarHistoryRejected = true; }
        try { (void)engine.getAt(bytesOf(first), akkaradb::engine::Revision{engine.revisionStreamId(bytesOf(first)), UINT64_MAX}); }
        catch (const std::exception&) { revisionHistoryRejected = true; }
        try { auto history = engine.history(bytesOf(first)); (void)history.begin(); }
        catch (const std::exception&) { historyRejected = true; }
    }
    // Release before assertions or future destruction, including failure paths.
    clusterTransactionApplyGate->release(); commit.get();
    AKK_CLUSTER_CHECK(paused && rejected && scalarHistoryRejected && revisionHistoryRejected && historyRejected && clusterTransactionApplyGate->rejectedRead.load());
    AKK_CLUSTER_CHECK(textOf(*engine.getAt(bytesOf(first), UINT64_MAX)) == "new-first");
    auto committedHistory = engine.history(bytesOf(first));
    auto committedHistoryPosition = committedHistory.begin();
    AKK_CLUSTER_CHECK(committedHistoryPosition != std::default_sentinel && textOf(committedHistoryPosition->value) == "new-first");
    auto proof = fixture.begin();
    AKK_CLUSTER_CHECK(textOf(*proof.get(bytesOf(first))) == "new-first" && textOf(*proof.get(bytesOf(second))) == "new-second");
    proof.end();
}

void testClusterTransactionCapacity() {
    std::fprintf(stderr, "[transactions] capacity rejects before replicated application\n");
    ClusterTransactionFixture fixture{makeTempDir("cluster-transactions-capacity"), false, false, 300000, false, 1024,
        [](size_t, auto& options) { options.cluster.runtime.transactions.maxRetainedResultBytes = 600; }};
    auto& engine = *fixture.engines[0]; const std::vector<uint8_t> value(400, 0x27);
    bool rejected = false;
    for (unsigned i = 0; i < 24; ++i) {
        try { auto tx = i == 0 ? fixture.begin() : engine.begin(); tx.put(bytesOf("capacity"), value); tx.end(); }
        catch (const akkaradb::engine::TransactionConflict&) { rejected = true; break; }
    }
    AKK_CLUSTER_CHECK(rejected);
    // A predictable capacity limit must not poison Raft/storage application.
    AKK_CLUSTER_CHECK(waitUntil([&] { try { engine.put(bytesOf("capacity-after"), bytesOf("usable")); return true; } catch (...) { return false; } }, std::chrono::seconds{5}));
    AKK_CLUSTER_CHECK(textOf(*engine.get(bytesOf("capacity-after"))) == "usable");
    fixture.close(); fixture.open();
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { const auto stored = fixture.engines[0]->get(bytesOf("capacity-after")); return stored && textOf(*stored) == "usable"; }
        catch (...) { return false; }
    }, std::chrono::seconds{15}));
}

void testClusterTransactions(bool partitioned, bool secure = false) {
    using namespace akkaradb::engine;
    std::fprintf(stderr, "[transactions] %s %s\n", partitioned ? "PARTITIONED" : "MIRROR", secure ? "SECURE" : "PLAIN");
    ClusterTransactionFixture fixture{makeTempDir("cluster-transactions"), partitioned, secure};
    auto& engine = *fixture.engines[0];
    const auto [left, right] = clusterTransactionKeys(fixture.config, "txn-");
    std::string initialError;
    const bool writeReady = waitUntil([&] { try { engine.put(bytesOf(left), bytesOf("10")); engine.put(bytesOf(right), bytesOf("20")); return true; } catch (const std::exception& error) { initialError = error.what(); return false; } }, std::chrono::seconds{15});
    if (!writeReady) { throw std::runtime_error("Initial transaction fixture write: " + initialError); }
    auto tx = fixture.begin(); const auto id = tx.clusterId(); AKK_CLUSTER_CHECK(id.has_value());
    AKK_CLUSTER_CHECK(textOf(*tx.get(bytesOf(left))) == "10" && textOf(*tx.get(bytesOf(right))) == "20");
    bool blocked = false;
    try { engine.put(bytesOf(right), bytesOf("race")); } catch (const ClusterRoutingError& error) {
        if (error.code != ClusterRoutingCode::TRANSACTION_CONFLICT) { throw; } blocked = true;
    }
    AKK_CLUSTER_CHECK(blocked && textOf(*tx.get(bytesOf(right))) == "20");
    bool overlapping = false;
    try { auto other = fixture.engines[1]->begin(); (void)other.get(bytesOf(left)); } catch (const TransactionConflict&) { overlapping = true; }
    AKK_CLUSTER_CHECK(overlapping);
    if (textOf(*tx.get(bytesOf(left))) == "10") { tx.put(bytesOf(left), bytesOf("9")); tx.put(bytesOf(right), bytesOf("21")); }
    tx.put(bytesOf("txn-own"), bytesOf("staged"));
    AKK_CLUSTER_CHECK(!engine.exists(bytesOf("txn-own")) && textOf(*tx.get(bytesOf("txn-own"))) == "staged");
    AKK_CLUSTER_CHECK(tx.count() == 3);
    tx.end(); const auto outcome = engine.transactionOutcome(*id);
    AKK_CLUSTER_CHECK(outcome.status == ClusterTransactionStatus::COMMITTED && outcome.finished);
    AKK_CLUSTER_CHECK(textOf(*engineGet(engine, left)) == "9" && textOf(*engineGet(engine, right)) == "21");
    {
        auto abandoned = fixture.begin(); const auto abandonedId = *abandoned.clusterId();
        abandoned.remove(bytesOf(left)); abandoned.rollback();
        AKK_CLUSTER_CHECK(engine.transactionOutcome(abandonedId).status == ClusterTransactionStatus::ABORTED);
    }
    AKK_CLUSTER_CHECK(engine.exists(bytesOf(left)));
    auto snapshot = fixture.begin(TransactionIsolation::SNAPSHOT_ISOLATION);
    snapshot.remove(bytesOf("txn-own"));
    const std::vector<uint8_t> large(2 * 1024 * 1024 + 19, 0x69);
    snapshot.put(bytesOf(right), large); AKK_CLUSTER_CHECK(snapshot.get(bytesOf(right)) == large && snapshot.count() == 2);
    snapshot.end(); AKK_CLUSTER_CHECK(engine.get(bytesOf(right)) == large);
    fixture.close(); fixture.open();
    auto reopened = fixture.begin();
    AKK_CLUSTER_CHECK(textOf(*reopened.get(bytesOf(left))) == "9" && reopened.get(bytesOf(right)) == large && reopened.count() == 2);
    reopened.end();
}

int runClusterTransactionCrashWriter(const char* point, const std::filesystem::path& dir) {
    ClusterTransactionFixture fixture{dir, true, false, 60000, true};
    auto tx = fixture.begin();
    {
        const auto id = *tx.clusterId();
        std::ofstream output{dir / "transaction-id.bin", std::ios::binary};
        output.write(reinterpret_cast<const char*>(id.data()), id.size()); output.close(); AKK_CLUSTER_CHECK(output.good());
    }
    const auto [left, right] = clusterTransactionKeys(fixture.config, "crash-");
    tx.put(bytesOf(left), bytesOf("committed")); tx.put(bytesOf(right), bytesOf("committed"));
    setStorageCrashPoint(point);
    tx.end(); return 99;
}
void testClusterTransactionRecovery(const std::filesystem::path& executable) {
    for (const char* point : {"cluster_transaction.after_prepare_chunk", "cluster_transaction.after_prepare_metadata", "cluster_transaction.after_prepare_checkpoint",
        "cluster_transaction.before_decision", "cluster_transaction.after_decision", "cluster_transaction.after_apply_record"}) {
        std::fprintf(stderr, "[transactions] crash recovery %s\n", point);
        const auto dir = makeTempDir(point);
        runStorageCrashChild(executable, "--cluster-transaction-crash-writer", point, dir);
        // The crashed decision keeps its original expiry; verification
        // transactions need the normal lifetime after durable recovery.
        ClusterTransactionFixture fixture{dir, true, false, 300000, true};
        const auto [left, right] = clusterTransactionKeys(fixture.config, "crash-");
        const bool committed = std::string_view{point} == "cluster_transaction.after_decision" ||
            std::string_view{point} == "cluster_transaction.after_apply_record";
        ClusterTransactionId id{};
        { std::ifstream input{dir / "transaction-id.bin", std::ios::binary}; input.read(reinterpret_cast<char*>(id.data()), id.size()); AKK_CLUSTER_CHECK(input.good()); }
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try {
                const auto outcome = fixture.engines[0]->transactionOutcome(id);
                return outcome.finished && outcome.status == (committed ? ClusterTransactionStatus::COMMITTED : ClusterTransactionStatus::ABORTED);
            } catch (const std::exception&) { return false; }
        }, std::chrono::seconds{90}));
        auto tx = fixture.begin();
        AKK_CLUSTER_CHECK(tx.exists(bytesOf(left)) == committed && tx.exists(bytesOf(right)) == committed);
        if (committed) { AKK_CLUSTER_CHECK(textOf(*tx.get(bytesOf(left))) == "committed" && textOf(*tx.get(bytesOf(right))) == "committed"); }
        tx.end();
    }
}

void testClusterTransactionSnapshots() {
    using namespace std::chrono_literals;
    std::fprintf(stderr, "[transactions] snapshot catch-up and leader change\n");
    ClusterTransactionFixture fixture{makeTempDir("cluster-transactions-snapshot"), false, false, 300000, true};
    fixture.engines[2]->close(); fixture.engines[2].reset();
    auto tx = fixture.begin();
    const std::vector<uint8_t> large(2 * 1024 * 1024 + 7, 0x35);
    tx.put(bytesOf("snapshot-large"), large); tx.put(bytesOf("snapshot-other"), bytesOf("both"));
    tx.end();
    akkaradb::engine::AkkEngine* current[]{fixture.engines[0].get(), fixture.engines[1].get()};
    AKK_CLUSTER_CHECK(waitUntil([&] { auto* leader = findEngineLeader(current); return leader && leader->stats().cluster.snapshotIndex > 0; }, 10s));
    fixture.options[2].paths.dataDir = fixture.dir / "replacement";
    writeNodeIdFile(fixture.options[2].paths.dataDir / "node.id", 3);
    fixture.engines[2] = ClusterTransactionFixture::Engine::open(fixture.options[2]);
    AKK_CLUSTER_CHECK(waitUntil([&] { return fixture.engines[2]->stats().cluster.snapshotIndex > 0; }, 15s));
    auto* leader = findEngineLeader(current); AKK_CLUSTER_CHECK(leader);
    auto frozen = fixture.begin();
    AKK_CLUSTER_CHECK(textOf(*frozen.get(bytesOf("snapshot-other"))) == "both");
    // A nonzero snapshot index does not prove catch-up through the new key
    // reservation; wait for the replacement's applied prefix before transferring.
    const auto transferIndex = leader->stats().cluster.commitIndex;
    AKK_CLUSTER_CHECK(waitUntil([&] { return fixture.engines[2]->stats().cluster.appliedIndex >= transferIndex; }, 30s));
    leader->transferClusterLeadership(3);
    AKK_CLUSTER_CHECK(waitUntil([&] { return fixture.engines[2]->stats().cluster.role == static_cast<uint32_t>(NodeRole::PRIMARY); }, 10s));
    bool reserved = false;
    try { fixture.engines[2]->put(bytesOf("snapshot-other"), bytesOf("race")); }
    catch (const ClusterRoutingError& error) { reserved = error.code == ClusterRoutingCode::TRANSACTION_CONFLICT; }
    AKK_CLUSTER_CHECK(reserved);
    fixture.engines[2]->put(bytesOf("snapshot-unrelated"), bytesOf("allowed"));
    AKK_CLUSTER_CHECK(frozen.get(bytesOf("snapshot-large")) == large && textOf(*frozen.get(bytesOf("snapshot-other"))) == "both");
    frozen.put(bytesOf("snapshot-other"), bytesOf("leader-changed")); frozen.end();
    auto proof = fixture.begin(); AKK_CLUSTER_CHECK(textOf(*proof.get(bytesOf("snapshot-other"))) == "leader-changed"); proof.end();
}

std::shared_ptr<std::atomic<bool>> recoveryReleaseFailure;
void testClusterTransactionRecoveryDiagnostics() {
    using namespace std::chrono_literals;
    auto failRelease = std::make_shared<std::atomic<bool>>(false); recoveryReleaseFailure = failRelease;
    AKK_CLUSTER_CHECK(registerClusterRuntimeFactory([](std::filesystem::path dir, ClusterConfig config,
        uint64_t id, ClusterEngineCallbacks callbacks, ClusterRuntimeOptions options) -> std::unique_ptr<IClusterRuntime> {
        if (callbacks.forward) {
            callbacks.forward = [failRelease = recoveryReleaseFailure, forward = std::move(callbacks.forward)](const ForwardRequest& request) {
                if (failRelease->load() && request.operation == ForwardOperation::TRANSACTION && request.entries.size() == 1 &&
                    akkaradb::engine::cluster::detail::decodeTransactionMessage(request.entries[0].value).action ==
                        akkaradb::engine::cluster::detail::TransactionAction::RELEASE) {
                    throw ClusterRoutingError(ClusterRoutingCode::TRANSACTION_CONFLICT, {}, "injected recovery release failure");
                }
                return forward(request);
            };
        }
        return ClusterRuntime::create(std::move(dir), std::move(config), id, std::move(callbacks), std::move(options));
    }));
    auto restore = std::shared_ptr<void>{failRelease.get(), [](void*) {
        recoveryReleaseFailure.reset();
        (void)registerClusterRuntimeFactory([](std::filesystem::path dir, ClusterConfig config, uint64_t id,
            ClusterEngineCallbacks callbacks, ClusterRuntimeOptions options) -> std::unique_ptr<IClusterRuntime> {
            return ClusterRuntime::create(std::move(dir), std::move(config), id, std::move(callbacks), std::move(options));
        });
    }};
    ClusterTransactionFixture fixture{makeTempDir("cluster-recovery-diagnostics"), true};
    auto tx = fixture.begin(); const auto id = *tx.clusterId();
    auto [first, second] = clusterTransactionKeys(fixture.config, "recovery-diagnostic-");
    tx.put(bytesOf(first), bytesOf("one")); tx.put(bytesOf(second), bytesOf("two"));
    failRelease->store(true);
    bool unknown = false;
    try { tx.end(); } catch (const akkaradb::engine::TransactionOutcomeUnknown&) { unknown = true; }
    bool observed = waitUntil([&] {
        for (const auto& engine : fixture.engines) {
            const auto stats = engine->stats().cluster;
            if (stats.transactionRecoveryFailuresTotal && stats.transactionRecoveryConsecutiveFailedPasses &&
                stats.transactionRecoveryLastFailedId == id && stats.transactionRecoveryLastFailureAtUs &&
                stats.transactionRecoveryFailingSinceUs && stats.transactionRecoveryLastError.find("injected recovery release failure") != std::string::npos &&
                stats.pendingDistributedTransactions) { return true; }
        }
        return false;
    }, 20s);
    failRelease->store(false);
    AKK_CLUSTER_CHECK(unknown && observed);
    AKK_CLUSTER_CHECK(waitUntil([&] {
        for (const auto& engine : fixture.engines) {
            const auto stats = engine->stats().cluster;
            if (stats.transactionRecoveryFailuresTotal && !stats.transactionRecoveryConsecutiveFailedPasses &&
                !stats.transactionRecoveryFailingSinceUs && stats.transactionRecoveryLastSuccessfulPassAtUs &&
                !stats.transactionRecoveryLastError.empty() && !stats.pendingDistributedTransactions) { return true; }
        }
        return false;
    }, 20s));
}

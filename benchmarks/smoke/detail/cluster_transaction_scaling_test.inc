/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
struct TransactionScalingHooks {
    std::shared_ptr<ClusterTransactionApplyGate> seal = std::make_shared<ClusterTransactionApplyGate>();
    std::mutex mutex;
    std::map<uint64_t, std::function<ClusterSnapshot(uint64_t)>> exporters;
    std::map<uint64_t, IClusterRuntime*> runtimes;
    std::map<ClusterTransactionId, uint64_t> expiries;
    std::map<uint64_t, uint32_t> prepareCheckpointCrc;
    std::atomic<unsigned> releaseMode{2}, blockedCalls{0};
    ClusterTransactionId healthy{};
    bool pauseBeforeSeal = false;
};
std::shared_ptr<TransactionScalingHooks> transactionScalingHooks;
struct TransactionScalingHookScope {
    std::shared_ptr<TransactionScalingHooks> hooks = std::make_shared<TransactionScalingHooks>();
    TransactionScalingHookScope() {
        transactionScalingHooks = hooks;
        AKK_CLUSTER_CHECK(registerClusterRuntimeFactory([](std::filesystem::path dir, ClusterConfig config,
            uint64_t id, ClusterEngineCallbacks callbacks, ClusterRuntimeOptions options) -> std::unique_ptr<IClusterRuntime> {
            const auto hooks = transactionScalingHooks;
            const bool data = options.raftGroupKind == RaftGroupKind::DATA;
            if (callbacks.exportTransactionSnapshot && options.raftGroupKind == RaftGroupKind::DATA) {
                std::lock_guard lock{hooks->mutex}; hooks->exporters[id] = callbacks.exportTransactionSnapshot;
            }
            if (callbacks.apply) {
                callbacks.apply = [hooks, data, id, dir, apply = std::move(callbacks.apply)](uint64_t seq, ReplOpType op,
                    auto key, auto value, uint8_t flags, uint64_t source, uint64_t timestamp) {
                    if (op == ReplOpType::TRANSACTION) {
                        const auto message = akkaradb::engine::cluster::detail::decodeTransactionMessage(value);
                        if (message.action == akkaradb::engine::cluster::detail::TransactionAction::FREEZE) {
                            std::lock_guard lock{hooks->mutex}; hooks->expiries[message.id] = message.expires;
                        }
                    }
                    if (!hooks->pauseBeforeSeal && op == ReplOpType::TRANSACTION && akkaradb::engine::cluster::detail::decodeTransactionMessage(value).action ==
                        akkaradb::engine::cluster::detail::TransactionAction::SEAL) { hooks->seal->pause(0); }
                    apply(seq, op, key, value, flags, source, timestamp);
                    if (data && op == ReplOpType::TRANSACTION && akkaradb::engine::cluster::detail::decodeTransactionMessage(value).action ==
                        akkaradb::engine::cluster::detail::TransactionAction::PREPARE) {
                        std::ifstream input{dir / "transactions" / "cluster-state.akdt", std::ios::binary};
                        const std::vector<uint8_t> bytes{std::istreambuf_iterator<char>{input}, std::istreambuf_iterator<char>{}};
                        AKK_CLUSTER_CHECK(!bytes.empty());
                        std::lock_guard lock{hooks->mutex}; hooks->prepareCheckpointCrc[id] = akkaradb::engine::blob::crc32c(bytes);
                    }
                };
            }
            if (callbacks.forward) {
                callbacks.forward = [hooks, forward = std::move(callbacks.forward)](const ForwardRequest& request) {
                    if (request.operation == ForwardOperation::TRANSACTION && request.entries.size() == 1) {
                        const auto message = akkaradb::engine::cluster::detail::decodeTransactionMessage(request.entries[0].value);
                        if (hooks->pauseBeforeSeal && message.action == akkaradb::engine::cluster::detail::TransactionAction::SEAL) { hooks->seal->pause(0); }
                        if (message.action == akkaradb::engine::cluster::detail::TransactionAction::RELEASE) {
                            const auto mode = hooks->releaseMode.load();
                            if (mode == 0) { throw ClusterRoutingError(ClusterRoutingCode::TRANSACTION_CONFLICT, {}, "injected release failure"); }
                            if (mode == 1 && message.id != hooks->healthy) {
                                ++hooks->blockedCalls;
                                akkaradb::engine::cluster::detail::ReconfigurationDeadline budget{request.timeoutMs};
                                while (hooks->releaseMode.load() == 1) {
                                    std::this_thread::sleep_for(std::chrono::milliseconds{5});
                                    akkaradb::engine::cluster::detail::ReconfigurationDeadline::check();
                                }
                            }
                        }
                    }
                    return forward(request);
                };
            }
            auto runtime = ClusterRuntime::create(std::move(dir), std::move(config), id, std::move(callbacks), std::move(options));
            if (data) { std::lock_guard lock{hooks->mutex}; hooks->runtimes[id] = runtime.get(); }
            return runtime;
        }));
    }
    ~TransactionScalingHookScope() {
        hooks->releaseMode.store(2); hooks->seal->release(); transactionScalingHooks.reset();
        (void)registerClusterRuntimeFactory([](std::filesystem::path dir, ClusterConfig config,
            uint64_t id, ClusterEngineCallbacks callbacks, ClusterRuntimeOptions options) -> std::unique_ptr<IClusterRuntime> {
            return ClusterRuntime::create(std::move(dir), std::move(config), id, std::move(callbacks), std::move(options));
        });
    }
};

void testClusterTransactionPrepareScaling() {
    using namespace akkaradb::engine;
    std::fprintf(stderr, "[transactions] 32 MiB prepare and a 16 KiB key use immutable chunks and descriptor-only deltas\n");
    TransactionScalingHookScope factory;
    ClusterTransactionFixture fixture{makeTempDir("cluster-prepare-scaling"), false};
    auto tx = fixture.begin();
    std::vector<uint8_t> large(32 * 1024 * 1024);
    for (size_t i = 0; i < large.size(); ++i) { large[i] = static_cast<uint8_t>(i / (1024 * 1024)); }
    const std::string largeKey(16 * 1024, 'k');
    tx.put(bytesOf(largeKey), large);
    factory.hooks->seal->enable(0);
    auto commit = std::async(std::launch::async, [tx = std::move(tx)]() mutable { tx.end(); });
    const bool paused = factory.hooks->seal->await(std::chrono::seconds{90});
    std::optional<ClusterSnapshot> snapshot;
    std::exception_ptr failure;
    try {
        AKK_CLUSTER_CHECK(paused);
        const auto leader = fixture.engines[0]->stats().cluster.leaderNodeId;
        std::function<ClusterSnapshot(uint64_t)> capture;
        { std::lock_guard lock{factory.hooks->mutex}; capture = factory.hooks->exporters.at(leader); }
        snapshot = capture(0);
        const auto transactions = fixture.options[leader - 1].paths.dataDir / "transactions";
        std::ifstream input{transactions / "cluster-state.akdt", std::ios::binary};
        const std::vector<uint8_t> checkpoint{std::istreambuf_iterator<char>{input}, std::istreambuf_iterator<char>{}};
        AKK_CLUSTER_CHECK(checkpoint.size() < largeKey.size() + 4096);
        { std::lock_guard lock{factory.hooks->mutex}; AKK_CLUSTER_CHECK(akkaradb::engine::blob::crc32c(checkpoint) == factory.hooks->prepareCheckpointCrc.at(leader)); }
        uint64_t deltaBytes = 0; unsigned deltas = 0;
        for (const auto& entry : std::filesystem::directory_iterator(transactions / "cluster-prepare-deltas")) {
            if (entry.path().extension() == ".akdd") { deltaBytes += entry.file_size(); ++deltas; }
        }
        AKK_CLUSTER_CHECK(deltas == 33 && deltaBytes < 4096);
        uint64_t bytes = 0;
        for (const auto& entry : std::filesystem::directory_iterator(transactions / "cluster-prepare")) {
            if (entry.path().extension() == ".akdp") { bytes += entry.file_size(); }
        }
        AKK_CLUSTER_CHECK(bytes >= large.size() && bytes < large.size() + 65536);
    } catch (...) { failure = std::current_exception(); }
    factory.hooks->seal->release(); commit.get();
    if (failure) { std::rethrow_exception(failure); }
    // Captured payload must remain immutable and readable after RELEASE deletes its files.
    uint64_t payloadBytes = 0; unsigned chunks = 0; bool payload = false;
    SnapshotEntryVisitor visitor;
    visitor.beginEntry = [&](auto key, uint64_t size, uint32_t) {
        payload = akkaradb::engine::cluster::detail::decodeSnapshotKey(key).kind ==
            akkaradb::engine::cluster::detail::SnapshotRecordKind::TRANSACTION_PREPARE;
        if (payload) { ++chunks; payloadBytes += size; }
        return true;
    };
    visitor.appendValueChunk = [](uint64_t, auto) { return true; };
    visitor.finishEntry = [] { return true; };
    AKK_CLUSTER_CHECK(snapshot->forEachEntry(visitor) && chunks == 33 && payloadBytes > large.size() && payloadBytes < large.size() + largeKey.size() + 1024);
    snapshot.reset();
    AKK_CLUSTER_CHECK(fixture.engines[0]->get(bytesOf(largeKey)) == large);
    fixture.close(); fixture.open();
    AKK_CLUSTER_CHECK(waitUntil([&] { try { return fixture.engines[0]->get(bytesOf(largeKey)) == large; } catch (...) { return false; } }, std::chrono::seconds{20}));
}

void testClusterTransactionRecoveryFairness() {
    using namespace akkaradb::engine;
    std::fprintf(stderr, "[transactions] slow release recovery cannot starve a healthy decision\n");
    TransactionScalingHookScope factory;
    factory.hooks->releaseMode.store(0);
    ClusterTransactionFixture fixture{makeTempDir("cluster-recovery-fairness"), true, false, 300000, false, 0,
        [](size_t, AkkEngineOptions& options) {
            options.cluster.runtime.transactions.timeoutMs = 10000;
            options.cluster.runtime.transactions.recoveryConcurrency = 2;
            options.cluster.runtime.transactions.recoveryAttemptTimeoutMs = 2000;
            options.cluster.runtime.transactions.recoveryIntervalMs = 25;
        }};
    std::vector<ClusterTransactionId> ids;
    for (unsigned i = 0; i < 7; ++i) {
        auto tx = fixture.begin(); ids.push_back(*tx.clusterId());
        for (uint16_t partition = 0; partition < fixture.config.partition().partitionCount; ++partition) {
            for (unsigned candidate = 0;; ++candidate) {
                const auto key = "fair-" + std::to_string(i) + "-" + std::to_string(partition) + "-" + std::to_string(candidate);
                if (akkaradb::engine::cluster::detail::partitionForKey(bytesOf(key), fixture.config.partition().partitionCount) != partition) { continue; }
                tx.put(bytesOf(key), bytesOf("committed")); break;
            }
        }
        bool unknown = false;
        try { tx.end(); } catch (const TransactionOutcomeUnknown&) { unknown = true; }
        AKK_CLUSTER_CHECK(unknown && !fixture.engines[0]->transactionOutcome(ids.back()).finished);
    }
    factory.hooks->healthy = *std::ranges::max_element(ids);
    factory.hooks->releaseMode.store(1);
    const bool healthyFinished = waitUntil([&] {
        try { return fixture.engines[0]->transactionOutcome(factory.hooks->healthy).finished; } catch (...) { return false; }
    }, std::chrono::seconds{15});
    const bool blocked = factory.hooks->blockedCalls.load() != 0;
    factory.hooks->releaseMode.store(2);
    if (!healthyFinished || !blocked) {
        std::fprintf(stderr, "[recovery-fairness] finished=%d blocked_calls=%u\n", healthyFinished, factory.hooks->blockedCalls.load());
        for (const auto& engine : fixture.engines) {
            const auto stats = engine->stats().cluster;
            std::fprintf(stderr, "[recovery-fairness] pending=%llu failures=%llu last_error=%s\n",
                static_cast<unsigned long long>(stats.pendingDistributedTransactions),
                static_cast<unsigned long long>(stats.transactionRecoveryFailuresTotal), stats.transactionRecoveryLastError.c_str());
        }
    }
    AKK_CLUSTER_CHECK(healthyFinished && blocked);
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { return std::ranges::all_of(ids, [&](const auto& id) { return fixture.engines[0]->transactionOutcome(id).finished; }); }
        catch (...) { return false; }
    }, std::chrono::seconds{15}));
}

void testClusterTransactionPreparedSnapshot() {
    using namespace akkaradb::engine;
    std::fprintf(stderr, "[transactions] active prepare chunks transfer through a Raft snapshot\n");
    TransactionScalingHookScope factory;
    factory.hooks->pauseBeforeSeal = true;
    factory.hooks->seal->pauseTimeout = std::chrono::seconds{60};
    ClusterTransactionFixture fixture{makeTempDir("cluster-prepared-snapshot"), false, false, 300000, true};
    fixture.engines[2]->close(); fixture.engines[2].reset();
    auto initial = fixture.begin(); initial.end();
    const auto leaderId = fixture.engines[0]->stats().cluster.leaderNodeId;
    AKK_CLUSTER_CHECK(leaderId && leaderId <= 2);
    const auto caller = leaderId == 1 ? 1 : 0;
    auto tx = fixture.engines[caller]->begin();
    const std::vector<uint8_t> large(2 * 1024 * 1024 + 17, 0x63);
    tx.put(bytesOf("snapshot-prepared"), large);
    factory.hooks->seal->enable(0);
    auto commit = std::async(std::launch::async, [tx = std::move(tx)]() mutable { tx.end(); });
    const bool paused = factory.hooks->seal->await(std::chrono::seconds{30});
    std::exception_ptr failure;
    try {
        AKK_CLUSTER_CHECK(paused);
        const auto cut = fixture.engines[leaderId - 1]->stats().cluster.commitIndex;
        AKK_CLUSTER_CHECK(waitUntil([&] { return fixture.engines[leaderId - 1]->stats().cluster.snapshotIndex >= cut; }, std::chrono::seconds{15}));
        fixture.options[2].paths.dataDir = fixture.dir / "replacement";
        const auto oldDeltas = fixture.options[2].paths.dataDir / "transactions" / "cluster-prepare-deltas";
        std::filesystem::create_directories(oldDeltas);
        const auto oldSuffix = oldDeltas / ("9-" + std::to_string(cut + 100) + ".akdd");
        {
            // A replacement snapshot supersedes even a higher-sequence journal
            // from another local generation; its contents must never be replayed.
            std::ofstream orphan{oldSuffix, std::ios::binary}; orphan << "superseded journal";
        }
        writeNodeIdFile(fixture.options[2].paths.dataDir / "node.id", 3);
        fixture.engines[2] = ClusterTransactionFixture::Engine::open(fixture.options[2]);
        AKK_CLUSTER_CHECK(waitUntil([&] { return fixture.engines[2]->stats().cluster.snapshotIndex >= cut; }, std::chrono::seconds{20}));
        unsigned chunks = 0;
        for (const auto& entry : std::filesystem::directory_iterator(fixture.options[2].paths.dataDir / "transactions" / "cluster-prepare")) {
            if (entry.path().extension() == ".akdp") { ++chunks; }
        }
        AKK_CLUSTER_CHECK(chunks == 3);
        AKK_CLUSTER_CHECK(!std::filesystem::exists(oldSuffix));
    } catch (...) { failure = std::current_exception(); }
    factory.hooks->seal->release(); commit.get();
    if (failure) { std::rethrow_exception(failure); }
    const auto through = fixture.engines[leaderId - 1]->stats().cluster.commitIndex;
    AKK_CLUSTER_CHECK(waitUntil([&] { return fixture.engines[2]->stats().cluster.appliedIndex >= through; }, std::chrono::seconds{20}));
    fixture.engines[leaderId - 1]->transferClusterLeadership(3);
    AKK_CLUSTER_CHECK(waitUntil([&] { return fixture.engines[2]->stats().cluster.role == static_cast<uint32_t>(NodeRole::PRIMARY); }, std::chrono::seconds{15}));
    AKK_CLUSTER_CHECK(fixture.engines[2]->get(bytesOf("snapshot-prepared")) == large);
}

void testClusterTransactionPrepareIntegrity(const std::filesystem::path& executable) {
    for (unsigned damage = 0; damage < 3; ++damage) {
        const auto dir = makeTempDir("cluster-prepare-integrity-" + std::to_string(damage));
        runStorageCrashChild(executable, "--cluster-transaction-crash-writer", "cluster_transaction.after_prepare_metadata", dir);
        std::filesystem::path filePath;
        const auto extension = damage == 2 ? ".akdd" : ".akdp";
        for (const auto& entry : std::filesystem::recursive_directory_iterator(dir)) {
            if (entry.path().extension() == extension) { filePath = entry.path(); break; }
        }
        AKK_CLUSTER_CHECK(!filePath.empty());
        if (damage == 1) { AKK_CLUSTER_CHECK(std::filesystem::remove(filePath)); }
        else {
            std::fstream file{filePath, std::ios::in | std::ios::out | std::ios::binary};
            const auto offset = std::filesystem::file_size(filePath) - (damage == 2 ? 5 : 1);
            file.seekg(static_cast<std::streamoff>(offset)); char byte = 0; file.read(&byte, 1); AKK_CLUSTER_CHECK(file.good());
            byte ^= 1; file.seekp(static_cast<std::streamoff>(offset)); file.write(&byte, 1); file.close();
        }
        bool rejected = false;
        try { ClusterTransactionFixture fixture{dir, true, false, 300000, true}; }
        catch (const std::exception& error) {
            rejected = std::string_view{error.what()}.find(damage == 2 ? "prepare journal" : "prepare chunk") != std::string_view::npos;
        }
        AKK_CLUSTER_CHECK(rejected);
    }
}

void testClusterTransactionExpiredReservationFence() {
    using namespace akkaradb::engine;
    TransactionScalingHookScope factory;
    ClusterTransactionFixture fixture{makeTempDir("cluster-expired-reservation"), false, false, 10000, false, 0,
        [](size_t, AkkEngineOptions& options) { options.transactions.maxOpen = 1; }};
    auto old = fixture.begin(); const auto id = *old.clusterId();
    old.put(bytesOf("fenced"), bytesOf("aborted")); old.rollback();
    uint64_t expiry;
    { std::lock_guard lock{factory.hooks->mutex}; expiry = factory.hooks->expiries.at(id); }
    std::this_thread::sleep_for(std::chrono::milliseconds{11000});
    auto fresh = fixture.begin(); fresh.put(bytesOf("fenced"), bytesOf("fresh")); fresh.end();
    IClusterRuntime* runtime;
    { std::lock_guard lock{factory.hooks->mutex}; runtime = factory.hooks->runtimes.at(fixture.engines[0]->stats().cluster.leaderNodeId); }
    const auto command = akkaradb::engine::cluster::detail::encodeTransactionMessage({
        .action = akkaradb::engine::cluster::detail::TransactionAction::FREEZE, .id = id, .expires = expiry, .argument = expiry - 1});
    auto proposal = runtime->submitMutation([command](uint64_t) {
        ClusterMutation mutation; mutation.op = ReplOpType::TRANSACTION; mutation.recordFlags = 0; mutation.value = command; return mutation;
    });
    proposal.completion.get();
    // A resurrected stale participant would consume the only admission slot.
    auto proof = fixture.begin(); AKK_CLUSTER_CHECK(textOf(*proof.get(bytesOf("fenced"))) == "fresh"); proof.end();
}

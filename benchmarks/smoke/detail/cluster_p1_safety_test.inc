/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */

std::array<std::function<uint64_t()>, 2> partitionCheckpointCallbacks;
std::array<std::function<uint64_t()>, 2> partitionReplicationCallbacks;
std::array<std::function<std::optional<ClusterSnapshot>()>, 2> partitionSnapshotCallbacks;
struct PartitionCheckpointFactoryScope {
    PartitionCheckpointFactoryScope() {
        AKK_CLUSTER_CHECK(registerClusterRuntimeFactory([](auto dir, auto config, uint64_t id, auto callbacks, auto options) -> std::unique_ptr<IClusterRuntime> {
            partitionCheckpointCallbacks.at(id - 1) = callbacks.checkpointReplication;
            partitionReplicationCallbacks.at(id - 1) = callbacks.getCurrentSeq;
            partitionSnapshotCallbacks.at(id - 1) = callbacks.exportSnapshot;
            return ClusterRuntime::create(std::move(dir), config, id, std::move(callbacks), options);
        }));
    }
    ~PartitionCheckpointFactoryScope() {
        partitionCheckpointCallbacks = {};
        partitionReplicationCallbacks = {};
        partitionSnapshotCallbacks = {};
        (void)registerClusterRuntimeFactory([](auto dir, auto config, uint64_t id, auto callbacks, auto options) -> std::unique_ptr<IClusterRuntime> {
            return ClusterRuntime::create(std::move(dir), std::move(config), id, std::move(callbacks), options);
        });
    }
};
void testPartitionRecoveryProgressSafety(bool secure) {
    using namespace akkaradb::engine;
    PartitionCheckpointFactoryScope factory;
    const auto dir = makeTempDir(secure ? "partition-progress-safety-secure" : "partition-progress-safety-plain");
    const ClusterConfig config{{node(1, 38201, 38211), node(2, 38202, 38212)}, ReplicationMode::PARTITIONED,
        {.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::DURABLE},
        {.writeConsistency = WriteConsistency::ALL_CONFIGURED, .ackTimeoutAction = AckTimeoutAction::FAIL_WRITE, .ackTimeoutMs = 3000},
        {}, {}, 0, testClusterId(9720), {.replicationFactor = 2}};
    std::array<AkkEngineOptions, 2> options;
    std::array<std::unique_ptr<AkkEngine>, 2> engines;
    std::vector<ClusterPeerPublicKeyPin> pins;
    if (secure) { for (uint64_t id : {1, 2}) {
        const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
        pins.push_back({id, identity.publicKey});
    } }
    for (size_t i = 0; i < engines.size(); ++i) {
        auto& opt = options[i]; opt.paths.dataDir = dir / std::to_string(i + 1);
        opt.components.clusterEnabled = true;
        opt.components.blobEnabled = opt.components.sstEnabled = opt.components.manifestEnabled = false;
        opt.runtime.writeVisibility = AkkEngineOptions::WriteVisibilityMode::APPLIED;
        opt.wal.shardCount = 1; opt.cluster.config = config;
        auto& runtime = opt.cluster.runtime;
        runtime.clusterGroupId = 9720; runtime.readMode = ClusterReadMode::LOCAL_STALE_OK;
        runtime.clusterMembershipPath = opt.paths.dataDir / "members.state";
        runtime.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
        runtime.secure.identitySeedPath = dir / ("identity-" + std::to_string(i + 1));
        runtime.secure.pinnedPeers = pins;
        writeNodeIdFile(opt.paths.dataDir / "node.id", i + 1);
        engines[i] = AkkEngine::open(opt);
    }
    AKK_CLUSTER_CHECK(waitUntil([&] {
        return std::ranges::all_of(engines, [](const auto& engine) {
            const auto peers = engine->stats().cluster.peers;
            return peers.size() == 1 && peers.front().connected;
        });
    }, std::chrono::seconds{15}));
    const auto first = keyOwnedBy(config, 1, "progress-first-"), last = keyOwnedBy(config, 1, "progress-last-");
    engines[0]->put(bytesOf(first), bytesOf("first"));
    engines[0]->put(bytesOf(last), bytesOf("last"));
    AKK_CLUSTER_CHECK(textOf(*engines[1]->get(bytesOf(last))) == "last");
    const auto storedSequence = partitionCheckpointCallbacks[1]();
    bool rejectedValue = false;
    try { engines[1]->put(bytesOf(keyOwnedBy(config, 2, "invalid-local-")), std::vector<uint8_t>(size_t{UINT16_MAX} + 1, 'x')); }
    catch (const std::exception& error) { rejectedValue = std::string_view{error.what()}.find("65535-byte storage limit") != std::string_view::npos; }
    AKK_CLUSTER_CHECK(rejectedValue && partitionCheckpointCallbacks[1]() == storedSequence);
    AKK_CLUSTER_CHECK(partitionReplicationCallbacks[1]() == storedSequence);
    AKK_CLUSTER_CHECK(waitUntil([&] {
        const auto snapshot = partitionSnapshotCallbacks[1]();
        return snapshot && snapshot->seq == storedSequence;
    }));
    for (auto& engine : engines) { engine->close(); engine.reset(); }

    const auto rejects = [&](const AkkEngineOptions& opt, std::string_view reason) {
        bool rejected = false;
        try { auto opened = AkkEngine::open(opt); opened->close(); }
        catch (const std::exception& error) { rejected = std::string_view{error.what()}.find(reason) != std::string_view::npos; }
        AKK_CLUSTER_CHECK(rejected);
    };
    auto unsafe = options[1]; unsafe.runtime.recoverWal = false; rejects(unsafe, "require WAL/SST recovery");
    unsafe = options[1]; unsafe.runtime.truncateCorruptWalOnRecovery = true; rejects(unsafe, "reject corrupt-WAL truncation");
    unsafe = options[1]; unsafe.components.sstEnabled = unsafe.components.manifestEnabled = true; unsafe.runtime.recoverSst = false;
    rejects(unsafe, "require WAL/SST recovery");

    for (bool missingWal : {false, true}) {
        const auto copy = dir / (missingWal ? "missing-wal" : "corrupt-wal");
        std::filesystem::copy(options[1].paths.dataDir, copy, std::filesystem::copy_options::recursive);
        auto damaged = options[1]; damaged.paths.dataDir = copy;
        damaged.cluster.runtime.clusterMembershipPath = copy / "members.state";
        if (missingWal) {
            std::filesystem::rename(copy / "wal", copy / "retained-original-wal");
        } else {
            bool corrupted = false;
            for (const auto& file : std::filesystem::directory_iterator(copy / "wal")) {
                if (file.is_regular_file() && file.path().extension() == ".akwal" && std::filesystem::file_size(file.path()) > wal::WalSegmentHeader::SIZE) {
                    std::filesystem::resize_file(file.path(), std::filesystem::file_size(file.path()) - 1); corrupted = true;
                }
            }
            AKK_CLUSTER_CHECK(corrupted);
        }
        rejects(damaged, missingWal ? "behind durable peer progress" : "corrupt WAL");
    }
    for (bool legacy : {false, true}) {
        const auto copy = dir / (legacy ? "legacy-progress" : "corrupt-progress");
        std::filesystem::copy(options[1].paths.dataDir, copy, std::filesystem::copy_options::recursive);
        const auto progress = copy / "members.state.peer-1.progress";
        std::ifstream input{progress, std::ios::binary};
        std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(input)), {}); input.close();
        AKK_CLUSTER_CHECK(bytes.size() == 49 && bytes[4] == '2');
        if (legacy) {
            bytes[4] = '1'; bytes.erase(bytes.begin() + 37, bytes.begin() + 45);
            std::fill(bytes.end() - 4, bytes.end(), 0);
            const auto crc = snapshotValueCrc(bytes);
            for (unsigned i = 0; i < 4; ++i) { bytes[37 + i] = static_cast<uint8_t>(crc >> (8 * i)); }
        } else { bytes[37] ^= 1; }
        { std::ofstream output{progress, std::ios::binary | std::ios::trunc}; output.write(reinterpret_cast<const char*>(bytes.data()), bytes.size()); }
        auto incompatible = options[1]; incompatible.paths.dataDir = copy;
        incompatible.cluster.runtime.clusterMembershipPath = copy / "members.state";
        rejects(incompatible, legacy ? "invalid peer progress state size" : "peer progress state CRC mismatch");
    }
    for (size_t i = 0; i < engines.size(); ++i) { engines[i] = AkkEngine::open(options[i]); }
    for (auto& engine : engines) {
        AKK_CLUSTER_CHECK(textOf(*engine->get(bytesOf(first))) == "first");
        AKK_CLUSTER_CHECK(textOf(*engine->get(bytesOf(last))) == "last");
        engine->close();
    }
    std::fprintf(stderr, "[P1] PARTITIONED rejects incomplete recovery and preserves healthy restart (%s)\n", secure ? "SECURE" : "PLAIN");
}

struct TransactionTermGate {
    std::mutex mutex;
    std::condition_variable cv;
    akkaradb::engine::cluster::detail::TransactionAction action{};
    bool armed = false, entered = false, released = false;
    uint64_t term = 0;
    std::atomic<unsigned> staleRejects{0};
    std::array<ClusterRuntime*, 3> runtimes{};
    void pause(uint64_t nodeId, const ClusterMutationFactory& prepare, uint64_t validationTerm) {
        const auto mutation = prepare(0);
        if (mutation.op != ReplOpType::TRANSACTION) { return; }
        const auto command = akkaradb::engine::cluster::detail::decodeTransactionMessage(mutation.value);
        std::unique_lock lock{mutex};
        if (!armed || nodeId != 1 || command.action != action) { return; }
        armed = false; entered = true; term = validationTerm; cv.notify_all();
        if (!cv.wait_for(lock, std::chrono::seconds{45}, [&] { return released; })) {
            throw std::runtime_error("transaction term gate timed out");
        }
    }
    void release() { std::lock_guard lock{mutex}; released = true; cv.notify_all(); }
};
std::shared_ptr<TransactionTermGate> transactionTermGate;
class TransactionTermRuntime final : public IClusterRuntime {
    std::unique_ptr<ClusterRuntime> runtime_;
    std::shared_ptr<TransactionTermGate> gate_;
    uint64_t id_;
public:
    TransactionTermRuntime(std::unique_ptr<ClusterRuntime> runtime, uint64_t id, std::shared_ptr<TransactionTermGate> gate)
        : runtime_{std::move(runtime)}, gate_{std::move(gate)}, id_{id} {}
    void start() override { runtime_->start(); }
    void close() override { runtime_->close(); }
    NodeRole role() const noexcept override { return runtime_->role(); }
    RaftRuntimeStats raftStats() const override { return runtime_->raftStats(); }
    std::vector<NodeInfo> activeNodes() const override { return runtime_->activeNodes(); }
    bool ownsWriteKey(std::span<const uint8_t> key) const override { return runtime_->ownsWriteKey(key); }
    ClusterRouteTarget routeTarget(std::span<const uint8_t> key) const override { return runtime_->routeTarget(key); }
    uint64_t configurationEpoch() const noexcept override { return runtime_->configurationEpoch(); }
    uint64_t ownerNodeId(std::span<const uint8_t> key) const override { return runtime_->ownerNodeId(key); }
    uint64_t transactionLeaderNodeId() const override { return runtime_->transactionLeaderNodeId(); }
    uint64_t transactionReadBarrier() override { return runtime_->transactionReadBarrier(); }
    void queryReadBarrier() override { runtime_->queryReadBarrier(); }
    ClusterMutationSubmission submitTransactionControl(ClusterMutationFactory prepare, uint64_t expectedTerm) override {
        gate_->pause(id_, prepare, expectedTerm);
        try { return runtime_->submitTransactionControl(std::move(prepare), expectedTerm); }
        catch (const ClusterRoutingError& error) {
            if (error.code == ClusterRoutingCode::WRITE_REJECTED && runtime_->raftStats().currentTerm != expectedTerm) { ++gate_->staleRejects; }
            throw;
        }
    }
    ClusterMutationSubmission submitMutation(ClusterMutationFactory prepare, uint64_t expectedTerm = 0) override {
        return runtime_->submitMutation(std::move(prepare), expectedTerm);
    }
    std::shared_future<ClusterRequestResult> submitRequest(const ClusterRequestId& id, const std::array<uint8_t, 32>& fingerprint,
        ClusterMutationFactory prepare, uint64_t expectedTerm = 0) override {
        return runtime_->submitRequest(id, fingerprint, std::move(prepare), expectedTerm);
    }
    ClusterRequestResult queryRequest(const ClusterRequestId& id) override { return runtime_->queryRequest(id); }
    ForwardResponse forwardTo(uint64_t nodeId, ForwardRequest request) override { return runtime_->forwardTo(nodeId, std::move(request)); }
    ReadResponse linearizableReadKey(std::span<const uint8_t> key) override { return runtime_->linearizableReadKey(key); }
    ReadResponse readKey(std::span<const uint8_t> key, uint64_t seq) override { return runtime_->readKey(key, seq); }
    void shipEntry(uint64_t seq, ReplOpType op, std::span<const uint8_t> key, std::span<const uint8_t> value,
        uint8_t flags, uint64_t source) override { runtime_->shipEntry(seq, op, key, value, flags, source); }
    void shipBlob(uint64_t seq, uint64_t id, std::span<const uint8_t> value) override { runtime_->shipBlob(seq, id, value); }
};
struct TransactionTermFactoryScope {
    std::shared_ptr<TransactionTermGate> gate = std::make_shared<TransactionTermGate>();
    TransactionTermFactoryScope() {
        transactionTermGate = gate;
        AKK_CLUSTER_CHECK(registerClusterRuntimeFactory([](auto dir, auto config, uint64_t id, auto callbacks, auto options) -> std::unique_ptr<IClusterRuntime> {
            auto runtime = ClusterRuntime::create(std::move(dir), config, id, std::move(callbacks), options);
            transactionTermGate->runtimes.at(id - 1) = runtime.get();
            return std::make_unique<TransactionTermRuntime>(std::move(runtime), id, transactionTermGate);
        }));
    }
    ~TransactionTermFactoryScope() {
        gate->release(); transactionTermGate.reset();
        (void)registerClusterRuntimeFactory([](auto dir, auto config, uint64_t id, auto callbacks, auto options) -> std::unique_ptr<IClusterRuntime> {
            return ClusterRuntime::create(std::move(dir), std::move(config), id, std::move(callbacks), options);
        });
    }
};

void testTransactionDecisionTermFence(bool staleAbort) {
    using namespace akkaradb::engine;
    using namespace akkaradb::engine::cluster::detail;
    TransactionTermFactoryScope factory;
    ClusterTransactionFixture fixture{makeTempDir(staleAbort ? "transaction-stale-abort" : "transaction-stale-commit"), false,
        false, 300000, false, 0, [](size_t, AkkEngineOptions& opt) {
            opt.components.blobEnabled = false; opt.cluster.runtime.raftBlobPolicy = RaftBlobPolicy::REJECT;
            opt.cluster.runtime.raftSnapshot.enabled = false;
            opt.cluster.runtime.requests.enabled = true;
        }};
    auto tx = fixture.begin(); const auto id = *tx.clusterId();
    AKK_CLUSTER_CHECK(fixture.engines[0]->stats().cluster.leaderNodeId == 1);
    if (!staleAbort) { tx.put(bytesOf("fenced-value"), bytesOf("must-not-commit")); }
    {
        std::lock_guard lock{factory.gate->mutex};
        factory.gate->action = staleAbort ? TransactionAction::DECIDE_ABORT : TransactionAction::DECIDE_COMMIT;
        factory.gate->armed = true;
    }
    auto pending = std::async(std::launch::async, [&] {
        try { if (staleAbort) { tx.rollback(); } else { tx.end(); } return true; }
        catch (const TransactionOutcomeUnknown&) { return false; }
    });
    struct Release { std::shared_ptr<TransactionTermGate> gate; ~Release() { gate->release(); } } release{factory.gate};
    {
        std::unique_lock lock{factory.gate->mutex};
        AKK_CLUSTER_CHECK(factory.gate->cv.wait_for(lock, std::chrono::seconds{15}, [&] { return factory.gate->entered; }));
    }
    auto& runtimes = factory.gate->runtimes;
    runtimes[0]->transferRaftLeadership(2);
    AKK_CLUSTER_CHECK(waitUntil([&] { return runtimes[1]->role() == NodeRole::PRIMARY; }, std::chrono::seconds{15}));
    const auto command = [&](TransactionAction action) {
        ForwardRequest request; request.operation = ForwardOperation::TRANSACTION; request.timeoutMs = 20000;
        request.entries.push_back({{}, encodeTransactionMessage({.action = action, .id = id})});
        const auto response = runtimes[2]->forwardTo(2, std::move(request));
        if (!response.success) { throw std::runtime_error("transaction term regression command failed: " + response.message); }
    };
    if (staleAbort) { command(TransactionAction::START_PREPARE); command(TransactionAction::DECIDE_COMMIT); }
    else { command(TransactionAction::DECIDE_ABORT); }
    runtimes[1]->transferRaftLeadership(1);
    AKK_CLUSTER_CHECK(waitUntil([&] { return runtimes[0]->role() == NodeRole::PRIMARY; }, std::chrono::seconds{15}));
    const auto currentTerm = runtimes[0]->transactionReadBarrier();
    AKK_CLUSTER_CHECK(currentTerm > factory.gate->term);
    factory.gate->release();
    (void)pending.get();
    AKK_CLUSTER_CHECK(factory.gate->staleRejects == 1);
    const auto expected = staleAbort ? ClusterTransactionStatus::COMMITTED : ClusterTransactionStatus::ABORTED;
    AKK_CLUSTER_CHECK(waitUntil([&] {
        const auto outcome = fixture.engines[0]->transactionOutcome(id);
        return outcome.status == expected && outcome.finished;
    }, std::chrono::seconds{15}));
    if (!staleAbort) { AKK_CLUSTER_CHECK(!fixture.engines[0]->get(bytesOf("fenced-value"))); }

    // Participant/ordinary mutation and request admission obey the same fence.
    bool prepared = false;
    const auto prepare = [&](uint64_t) { prepared = true; ClusterMutation mutation; mutation.key = {'x'}; mutation.value = {'v'}; return mutation; };
    const auto before = runtimes[0]->raftStats();
    bool rejected = false;
    try { (void)runtimes[0]->submitMutation(prepare, factory.gate->term); }
    catch (const ClusterRoutingError& error) { rejected = error.code == ClusterRoutingCode::WRITE_REJECTED; }
    AKK_CLUSTER_CHECK(rejected && !prepared && runtimes[0]->raftStats().lastLogIndex == before.lastLogIndex);
    const auto requestId = fixture.engines[0]->newRequestId();
    rejected = false;
    try { (void)runtimes[0]->submitRequest(requestId, {}, prepare, factory.gate->term); }
    catch (const ClusterRoutingError& error) { rejected = error.code == ClusterRoutingCode::WRITE_REJECTED; }
    AKK_CLUSTER_CHECK(rejected && !prepared);
    auto valid = runtimes[0]->submitMutation(prepare, currentTerm); valid.completion.get();
    AKK_CLUSTER_CHECK(textOf(*fixture.engines[0]->get(bytesOf("x"))) == "v");
    std::fprintf(stderr, "[P1] stale %s rejected after two leader transfers; terminal outcome unchanged\n", staleAbort ? "ABORT" : "COMMIT");
}

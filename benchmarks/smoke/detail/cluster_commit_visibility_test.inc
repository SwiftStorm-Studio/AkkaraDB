/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */

void testRaftVisibilityAfterRejectedWrite(bool requests) {
    using namespace akkaradb::engine;
    const auto dir = makeTempDir(requests ? "raft-request-sequence-gap" : "raft-sequence-gap");
    const ClusterConfig config{{node(1, 34501, 34511), node(2, 34502, 34512), node(3, 34503, 34513)},
        ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 5000}};
    std::array<AkkEngineOptions, 3> options;
    std::array<std::unique_ptr<AkkEngine>, 3> engines;
    for (size_t i = 0; i < engines.size(); ++i) {
        auto& opt = options[i]; opt.paths.dataDir = dir / std::to_string(i + 1);
        opt.components.blobEnabled = opt.components.sstEnabled = opt.components.manifestEnabled = false;
        opt.components.clusterEnabled = opt.components.versionLogEnabled = true;
        opt.cluster.config = config; opt.cluster.runtime.transportMode = TransportMode::PLAIN;
        opt.cluster.runtime.readMode = ClusterReadMode::LOCAL_STALE_OK;
        opt.cluster.runtime.raftHeartbeatIntervalMs = 30;
        opt.cluster.runtime.raftSnapshot.enabled = false;
        opt.cluster.runtime.requests.enabled = requests;
        writeNodeIdFile(opt.paths.dataDir / "node.id", i + 1);
        engines[i] = AkkEngine::open(opt);
    }
    AkkEngine* leader = nullptr;
    AKK_CLUSTER_CHECK(waitUntil([&] {
        for (auto& engine : engines) {
            if (engine->stats().cluster.role == static_cast<uint32_t>(NodeRole::PRIMARY)) { leader = engine.get(); return true; }
        }
        return false;
    }, std::chrono::seconds{15}));
    leader->put(bytesOf("updated"), bytesOf("before"));
    const std::vector<uint8_t> oversized(ReplFrameHeader::MAX_RAFT_PAYLOAD_SIZE + 1, 0x71);
    const auto reject = [&] {
        bool rejected = false;
        try { leader->put(bytesOf("invalid"), oversized); }
        catch (const std::invalid_argument&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected);
    };
    reject();
    leader->put(bytesOf("updated"), bytesOf("after"));
    leader->put(bytesOf("created"), bytesOf("visible"));
    const auto createdSequence = leader->stats().currentSeq;
    reject();
    leader->remove(bytesOf("updated"));
    const auto verify = [&](AkkEngine& engine) {
        AKK_CLUSTER_CHECK(!engine.get(bytesOf("invalid")) && !engine.get(bytesOf("updated")));
        const auto created = engineGet(engine, "created");
        AKK_CLUSTER_CHECK(created && textOf(*created) == "visible");
        AKK_CLUSTER_CHECK(engine.count() == 1);
        akkaradb::core::BufferArena arena;
        size_t count = 0;
        for (const auto& row : engine.scan(arena)) {
            AKK_CLUSTER_CHECK(textOf(row.key) == "created" && textOf(row.value) == "visible"); ++count;
        }
        AKK_CLUSTER_CHECK(count == 1);
        const auto historical = engine.getAt(bytesOf("created"), createdSequence);
        AKK_CLUSTER_CHECK(historical && textOf(*historical) == "visible");
        const auto history = akk_test::collectHistory(engine.history(bytesOf("updated")));
        AKK_CLUSTER_CHECK(history.size() == 3 && textOf(history[0].value) == "before" &&
            textOf(history[1].value) == "after" && (history[2].flags & akkaradb::core::MemHdr16::FLAG_TOMBSTONE));
    };
    // LOCAL_STALE_OK checks each holder's own visibility frontier, not just the leader's routing.
    for (auto& engine : engines) {
        AKK_CLUSTER_CHECK(waitUntil([&] { return engine->stats().currentSeq >= leader->stats().currentSeq; }));
        verify(*engine);
    }
    for (auto& engine : engines) { engine->close(); engine.reset(); }
    for (size_t i = 0; i < engines.size(); ++i) { engines[i] = AkkEngine::open(options[i]); }
    for (auto& engine : engines) { verify(*engine); }
    for (auto& engine : engines) { engine->close(); }
}

struct ReadApplyOrderGate {
    std::mutex mutex;
    std::condition_variable cv;
    bool armed = false, applying = false, reading = false, released = false;
    void arm() { std::lock_guard lock{mutex}; armed = true; applying = reading = released = false; }
    void pauseApply() {
        std::unique_lock lock{mutex}; if (!armed) { return; }
        applying = true; cv.notify_all();
        if (!cv.wait_for(lock, std::chrono::seconds{10}, [&] { return released; })) {
            throw std::runtime_error("read/apply ordering gate timed out");
        }
    }
    void observeRead() { std::lock_guard lock{mutex}; if (armed) { reading = true; cv.notify_all(); } }
    bool await(bool read) {
        std::unique_lock lock{mutex};
        return cv.wait_for(lock, std::chrono::seconds{5}, [&] { return read ? reading : applying; });
    }
    void release() { std::lock_guard lock{mutex}; armed = false; released = true; cv.notify_all(); }
};
std::shared_ptr<ReadApplyOrderGate> readApplyOrderGate;

class ReadApplyOrderRuntime final : public IClusterRuntime {
    std::unique_ptr<ClusterRuntime> runtime_;
    std::shared_ptr<ReadApplyOrderGate> gate_;
public:
    ReadApplyOrderRuntime(std::unique_ptr<ClusterRuntime> runtime, std::shared_ptr<ReadApplyOrderGate> gate)
        : runtime_{std::move(runtime)}, gate_{std::move(gate)} {}
    void start() override { runtime_->start(); }
    void close() override { runtime_->close(); }
    NodeRole role() const noexcept override { return runtime_->role(); }
    RaftRuntimeStats raftStats() const override { return runtime_->raftStats(); }
    std::vector<NodeInfo> activeNodes() const override { return runtime_->activeNodes(); }
    bool ownsWriteKey(std::span<const uint8_t> key) const override { return runtime_->ownsWriteKey(key); }
    ClusterRouteTarget routeTarget(std::span<const uint8_t> key) const override { return runtime_->routeTarget(key); }
    uint64_t ownerNodeId(std::span<const uint8_t> key) const override { return runtime_->ownerNodeId(key); }
    uint64_t configurationEpoch() const noexcept override { return runtime_->configurationEpoch(); }
    uint64_t transactionLeaderNodeId() const override { return runtime_->transactionLeaderNodeId(); }
    uint64_t transactionReadBarrier() override { return runtime_->transactionReadBarrier(); }
    void queryReadBarrier() override { gate_->observeRead(); runtime_->queryReadBarrier(); }
    ClusterMutationSubmission submitTransactionControl(ClusterMutationFactory factory, uint64_t expectedTerm) override {
        return runtime_->submitTransactionControl(std::move(factory), expectedTerm);
    }
    ClusterMutationSubmission submitMutation(ClusterMutationFactory factory, uint64_t expectedTerm = 0) override {
        return runtime_->submitMutation(std::move(factory), expectedTerm);
    }
    ForwardResponse forwardTo(uint64_t node, ForwardRequest request) override { return runtime_->forwardTo(node, std::move(request)); }
    ReadResponse readKey(std::span<const uint8_t> key, uint64_t seq) override {
        gate_->observeRead(); return runtime_->readKey(key, seq);
    }
    void shipEntry(uint64_t seq, ReplOpType op, std::span<const uint8_t> key, std::span<const uint8_t> value,
        uint8_t flags, uint64_t source) override { runtime_->shipEntry(seq, op, key, value, flags, source); }
    void shipBlob(uint64_t seq, uint64_t id, std::span<const uint8_t> value) override { runtime_->shipBlob(seq, id, value); }
};

struct ReadApplyOrderFactoryScope {
    ReadApplyOrderFactoryScope() {
        readApplyOrderGate = std::make_shared<ReadApplyOrderGate>();
        AKK_CLUSTER_CHECK(registerClusterRuntimeFactory([](auto dir, auto config, uint64_t id, auto callbacks, auto options) -> std::unique_ptr<IClusterRuntime> {
            callbacks.apply = [apply = std::move(callbacks.apply), gate = readApplyOrderGate](uint64_t seq, ReplOpType op,
                auto key, auto value, uint8_t flags, uint64_t source, uint64_t timestamp) {
                if (op == ReplOpType::TRANSACTION && akkaradb::engine::cluster::detail::decodeTransactionMessage(value).action == akkaradb::engine::cluster::detail::TransactionAction::CREATE) {
                    gate->pauseApply();
                }
                apply(seq, op, key, value, flags, source, timestamp);
            };
            return std::make_unique<ReadApplyOrderRuntime>(ClusterRuntime::create(dir, config, id, std::move(callbacks), options), readApplyOrderGate);
        }));
    }
    ~ReadApplyOrderFactoryScope() {
        readApplyOrderGate->release(); readApplyOrderGate.reset();
        (void)registerClusterRuntimeFactory([](auto dir, auto config, uint64_t id, auto callbacks, auto options) -> std::unique_ptr<IClusterRuntime> {
            return ClusterRuntime::create(dir, config, id, std::move(callbacks), options);
        });
    }
};

void testClusterReadsDuringTransactionApply() {
    using namespace akkaradb::engine;
    ReadApplyOrderFactoryScope factory;
    AkkEngineOptions options; options.paths.dataDir = makeTempDir("cluster-read-apply-order");
    options.components.blobEnabled = options.components.sstEnabled = options.components.manifestEnabled = false;
    options.components.clusterEnabled = options.components.versionLogEnabled = true;
    options.cluster.config = ClusterConfig{{node(1, 34521, 34531)}, ReplicationMode::MIRROR, {},
        ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 5000}};
    options.cluster.runtime.transportMode = TransportMode::PLAIN;
    options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
    options.cluster.runtime.transactions.mode = ClusterTransactionMode::TWO_PHASE_COMMIT;
    options.cluster.runtime.transactions.timeoutMs = 10000;
    options.cluster.runtime.raftSnapshot.enabled = false;
    writeNodeIdFile(options.paths.dataDir / "node.id", 1);
    auto engine = AkkEngine::open(options);
    engine->put(bytesOf("baseline"), bytesOf("visible"));
    const auto sequence = engine->stats().currentSeq;
    const std::vector<std::pair<const char*, std::function<void()>>> reads{
        {"get", [&] { AKK_CLUSTER_CHECK(textOf(*engine->get(bytesOf("baseline"))) == "visible"); }},
        {"getBatch", [&] { const std::string key{"baseline"}; const std::array keys{bytesOf(key)}; const auto values = engine->getBatch(keys);
            AKK_CLUSTER_CHECK(values.size() == 1 && values[0].found && textOf(values[0].value) == "visible"); }},
        {"exists", [&] { AKK_CLUSTER_CHECK(engine->exists(bytesOf("baseline"))); }},
        {"getInto", [&] { std::vector<uint8_t> value; AKK_CLUSTER_CHECK(engine->getInto(bytesOf("baseline"), value) && textOf(value) == "visible"); }},
        {"getIntoArena", [&] { akkaradb::core::BufferArena arena; std::span<const uint8_t> value;
            AKK_CLUSTER_CHECK(engine->getIntoArena(bytesOf("baseline"), arena, value) && textOf(value) == "visible"); }},
        {"getAt", [&] { const auto value = engine->getAt(bytesOf("baseline"), sequence); AKK_CLUSTER_CHECK(value && textOf(*value) == "visible"); }},
        {"history", [&] { const auto history = akk_test::collectHistory(engine->history(bytesOf("baseline")));
            AKK_CLUSTER_CHECK(history.size() == 1 && textOf(history[0].value) == "visible"); }},
    };
    for (const auto& [name, read] : reads) {
        std::fprintf(stderr, "[read/apply] %s\n", name);
        readApplyOrderGate->arm();
        auto begin = std::async(std::launch::async, [&] { return engine->begin(); });
        AKK_CLUSTER_CHECK(readApplyOrderGate->await(false));
        auto result = std::async(std::launch::async, read);
        // Observe entry into the runtime, proving the public read has already acquired any outer locks.
        AKK_CLUSTER_CHECK(readApplyOrderGate->await(true));
        readApplyOrderGate->release();
        if (result.wait_for(std::chrono::seconds{5}) != std::future_status::ready) {
            akkaradb::test::failFastExit("READ/APPLY DEADLOCK", name);
        }
        result.get();
        AKK_CLUSTER_CHECK(begin.wait_for(std::chrono::seconds{5}) == std::future_status::ready);
        auto transaction = begin.get(); transaction.rollback();
    }
    engine->close();
}

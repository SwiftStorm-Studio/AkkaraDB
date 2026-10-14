/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
struct IntegrityWriteGate {
    std::mutex mutex;
    std::condition_variable cv;
    std::string key;
    bool entered = false, released = false;
    bool reclamationArmed = false;
    std::array<ClusterRuntime*, 6> runtimes{};
    void release() { std::lock_guard lock{mutex}; released = true; cv.notify_all(); }
};
std::shared_ptr<IntegrityWriteGate> integrityWriteGate;
class IntegrityRuntime final : public IClusterRuntime {
    std::unique_ptr<ClusterRuntime> runtime_;
    std::shared_ptr<IntegrityWriteGate> gate_;
    uint64_t node_;
public:
    IntegrityRuntime(std::unique_ptr<ClusterRuntime> runtime, uint64_t node, std::shared_ptr<IntegrityWriteGate> gate)
        : runtime_{std::move(runtime)}, gate_{std::move(gate)}, node_{node} {}
    void start() override { runtime_->start(); }
    void close() override { runtime_->close(); }
    RaftRuntimeStats raftStats() const override { return runtime_->raftStats(); }
    NodeRole role() const noexcept override { return runtime_->role(); }
    std::vector<NodeInfo> activeNodes() const override { return runtime_->activeNodes(); }
    bool ownsWriteKey(std::span<const uint8_t> key) const override { return runtime_->ownsWriteKey(key); }
    ClusterRouteTarget routeTarget(std::span<const uint8_t> key) const override { return runtime_->routeTarget(key); }
    uint64_t ownerNodeId(std::span<const uint8_t> key) const override { return runtime_->ownerNodeId(key); }
    uint64_t configurationEpoch() const noexcept override { return runtime_->configurationEpoch(); }
    void executePrimaryWrite(const std::function<void()>& write) override { runtime_->executePrimaryWrite(write); }
    ReadResponse readKey(std::span<const uint8_t> key, uint64_t seq) override { return runtime_->readKey(key, seq); }
    ForwardResponse forwardTo(uint64_t node, ForwardRequest request) override { return runtime_->forwardTo(node, std::move(request)); }
    void queryReadBarrier() override { runtime_->queryReadBarrier(); }
    void shipBlob(uint64_t seq, uint64_t id, std::span<const uint8_t> value) override { runtime_->shipBlob(seq, id, value); }
    void shipEntry(uint64_t seq, ReplOpType op, std::span<const uint8_t> key, std::span<const uint8_t> value,
        uint8_t flags, uint64_t source) override {
        if (node_ == 1 && !gate_->key.empty() && textOf(key) == gate_->key) {
            std::unique_lock lock{gate_->mutex}; gate_->entered = true; gate_->cv.notify_all();
            if (!gate_->cv.wait_for(lock, std::chrono::seconds{15}, [&] { return gate_->released; })) {
                throw std::runtime_error("integrity regression: replication gate timed out");
            }
        }
        runtime_->shipEntry(seq, op, key, value, flags, source);
    }
};
struct IntegrityFactoryScope {
    IntegrityFactoryScope() {
        integrityWriteGate = std::make_shared<IntegrityWriteGate>();
        AKK_CLUSTER_CHECK(registerClusterRuntimeFactory([](auto dir, auto config, uint64_t id, auto callbacks, auto options) -> std::unique_ptr<IClusterRuntime> {
            if (config.mode() == ReplicationMode::STRIPE && callbacks.reclaimStripeGeneration) {
                callbacks.reclaimStripeGeneration = [reclaim = std::move(callbacks.reclaimStripeGeneration), gate = integrityWriteGate]
                    (auto key, auto metadata, const std::function<bool()>& retainedByPeers) {
                    {
                        std::unique_lock lock{gate->mutex};
                        if (gate->reclamationArmed && textOf(key) == gate->key) {
                            gate->reclamationArmed = false; gate->entered = true; gate->cv.notify_all();
                            if (!gate->cv.wait_for(lock, std::chrono::seconds{10}, [&] { return gate->released; })) {
                                throw std::runtime_error("integrity regression: reclamation gate timed out");
                            }
                        }
                    }
                    return reclaim(key, metadata, retainedByPeers);
                };
            }
            auto runtime = ClusterRuntime::create(std::move(dir), config, id, std::move(callbacks), std::move(options));
            integrityWriteGate->runtimes.at(id - 1) = runtime.get();
            if (config.mode() == ReplicationMode::PARTITIONED && !config.usesDataConsensus()) {
                return std::make_unique<IntegrityRuntime>(std::move(runtime), id, integrityWriteGate);
            }
            return runtime;
        }));
    }
    ~IntegrityFactoryScope() {
        integrityWriteGate->release(); integrityWriteGate.reset();
        (void)registerClusterRuntimeFactory([](auto dir, auto config, uint64_t id, auto callbacks, auto options) -> std::unique_ptr<IClusterRuntime> {
            return ClusterRuntime::create(std::move(dir), std::move(config), id, std::move(callbacks), std::move(options));
        });
    }
};

void testNonRaftPartitionConcurrentReplicationOrder(bool secure) {
    using namespace akkaradb::engine;
    IntegrityFactoryScope factory;
    const auto dir = makeTempDir(secure ? "integrity-partition-secure" : "integrity-partition-plain");
    const ClusterConfig config{{node(1, 33901, 33911), node(2, 33902, 33912)}, ReplicationMode::PARTITIONED,
        {.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::DURABLE},
        {.writeConsistency = WriteConsistency::ALL_CONFIGURED, .ackTimeoutMs = 5000}, {}, {}, 0,
        testClusterId(9981), {.replicationFactor = 2}};
    std::array<AkkEngineOptions, 2> options;
    std::array<std::unique_ptr<AkkEngine>, 2> engines;
    std::vector<ClusterPeerPublicKeyPin> pins;
    if (secure) { for (uint64_t id : {1, 2}) {
        auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
        pins.push_back({id, identity.publicKey});
    } }
    for (size_t i = 0; i < engines.size(); ++i) {
        auto& opt = options[i]; opt.paths.dataDir = dir / std::to_string(i + 1);
        opt.components.blobEnabled = opt.components.sstEnabled = opt.components.manifestEnabled = false;
        opt.components.clusterEnabled = true; opt.cluster.config = config;
        opt.cluster.runtime.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
        opt.cluster.runtime.clusterGroupId = 9981; opt.cluster.runtime.readMode = ClusterReadMode::LOCAL_STALE_OK;
        opt.cluster.runtime.secure.pinnedPeers = pins; opt.cluster.runtime.secure.identitySeedPath = dir / ("identity-" + std::to_string(i + 1));
        writeNodeIdFile(opt.paths.dataDir / "node.id", i + 1); engines[i] = AkkEngine::open(opt);
    }
    const auto warmup = keyOwnedBy(config, 1, "integrity-warmup"), firstKey = keyOwnedBy(config, 1, "integrity-first"),
        secondKey = keyOwnedBy(config, 1, "integrity-second");
    AKK_CLUSTER_CHECK(waitUntil([&] { try { engines[0]->put(bytesOf(warmup), bytesOf("ready")); return true; } catch (...) { return false; } }, std::chrono::seconds{20}));
    integrityWriteGate->key = firstKey;
    auto first = std::async(std::launch::async, [&] { engines[0]->put(bytesOf(firstKey), bytesOf("first")); });
    bool paused;
    { std::unique_lock lock{integrityWriteGate->mutex}; paused = integrityWriteGate->cv.wait_for(lock, std::chrono::seconds{5}, [&] { return integrityWriteGate->entered; }); }
    auto second = std::async(std::launch::async, [&] { engines[0]->put(bytesOf(secondKey), bytesOf("second")); });
    (void)second.wait_for(std::chrono::milliseconds{500});
    integrityWriteGate->release(); first.get(); second.get();
    AKK_CLUSTER_CHECK(paused);
    const auto verify = [&] { for (const auto& engine : engines) {
        const auto firstValue = engine->get(bytesOf(firstKey)), secondValue = engine->get(bytesOf(secondKey));
        AKK_CLUSTER_CHECK(firstValue && secondValue && textOf(*firstValue) == "first" && textOf(*secondValue) == "second");
    } };
    verify();
    for (auto& engine : engines) { engine->close(); engine.reset(); }
    for (size_t i = 0; i < engines.size(); ++i) { engines[i] = AkkEngine::open(options[i]); }
    verify();
}

// 0: no history (reclamation must progress); 1: leader history; 2: future leader history.
void testStripeAuthorityHistoryReclamation(bool secure, unsigned historyPlacement) {
    using namespace akkaradb::engine;
    IntegrityFactoryScope factory;
    const auto dir = makeTempDir("integrity-stripe-" + std::to_string(secure) + "-" + std::to_string(historyPlacement));
    std::vector<NodeInfo> members;
    for (uint64_t id = 1; id <= 6; ++id) { members.push_back(node(id, static_cast<uint16_t>(34000 + id), static_cast<uint16_t>(34010 + id),
        id <= 3 ? DATA_AND_COORDINATOR : uint32_t{COORDINATOR_ELIGIBLE | STRIPE_METADATA_VOTER})); }
    const ClusterConfig config{members, ReplicationMode::STRIPE, {}, {.ackTimeoutMs = 1000}, {},
        {.dataShards = 2, .parityShards = 1, .metadataPlacement = StripeMetadataPlacement::EXPLICIT_VOTERS}, 0, testClusterId(9982)};
    std::array<std::unique_ptr<AkkEngine>, 6> engines;
    std::vector<ClusterPeerPublicKeyPin> pins;
    if (secure) { for (uint64_t id = 1; id <= 6; ++id) {
        auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate(); pins.push_back({id, identity.publicKey});
    } }
    for (size_t i = 0; i < engines.size(); ++i) {
        AkkEngineOptions opt; opt.paths.dataDir = dir / std::to_string(i + 1);
        opt.components.blobEnabled = opt.components.sstEnabled = opt.components.manifestEnabled = false;
        opt.components.versionLogEnabled = historyPlacement == 1 ? i == 3 : historyPlacement == 2 && i >= 4;
        opt.components.clusterEnabled = true; opt.cluster.config = config;
        auto& runtime = opt.cluster.runtime;
        runtime.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN; runtime.clusterGroupId = 9982;
        runtime.routingMode = ClusterRoutingMode::FORWARD; runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
        runtime.stripeAutoRebuild = false; runtime.queryResultMode = QueryResultMode::STREAMING;
        runtime.secure.pinnedPeers = pins; runtime.secure.identitySeedPath = dir / ("identity-" + std::to_string(i + 1));
        writeNodeIdFile(opt.paths.dataDir / "node.id", i + 1); engines[i] = AkkEngine::open(opt);
    }
    const auto key = keyOwnedBy(config, 1, "integrity-stripe-history");
    AKK_CLUSTER_CHECK(waitUntil([&] { try { engines[0]->put(bytesOf(key), bytesOf("old-value")); return true; } catch (...) { return false; } }, std::chrono::seconds{20}));
    AKK_CLUSTER_CHECK(engines[0]->stats().cluster.stripeMetadataLeaderNodeId == 4);
    const auto revision = Revision{0, integrityWriteGate->runtimes[3]->stripeMetadataRaftStats().appliedStateMachineSeq};
    akkaradb::core::BufferArena arena;
    auto pinned = historyPlacement == 0 ? engines[0]->scan(arena) : akkaradb::core::ArenaGenerator<AkkEngine::ScanRecordView>{};
    engines[0]->put(bytesOf(key), bytesOf("new-value"));
    if (historyPlacement == 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds{600});
        for (size_t i = 0; i < 3; ++i) { AKK_CLUSTER_CHECK(stripeInternalCounts(*engines[i]).first == 2); }
        auto position = pinned.begin();
        AKK_CLUSTER_CHECK(position != std::default_sentinel && textOf(position->value) == "old-value");
        pinned = {};
    }
    const bool collected = waitUntil([&] { return stripeInternalCounts(*engines[0]).second == 0; }, std::chrono::seconds{15});
    if (!collected) { for (size_t i = 0; i < engines.size(); ++i) {
        const auto state = integrityWriteGate->runtimes[i]->stripeMetadataRaftStats();
        const auto counts = stripeInternalCounts(*engines[i]);
        std::fprintf(stderr, "[integrity] node=%zu seq=%llu health=%u failure=%u shards=%zu intents=%zu\n", i + 1,
            static_cast<unsigned long long>(state.appliedStateMachineSeq), unsigned(state.health), unsigned(state.lastFailure), counts.first, counts.second);
    } }
    AKK_CLUSTER_CHECK(collected);
    for (size_t i = 0; i < 3; ++i) { AKK_CLUSTER_CHECK(stripeInternalCounts(*engines[i]).first == (historyPlacement ? 2 : 1)); }
    if (historyPlacement == 2) {
        engines[3]->close(); engines[3].reset();
        AKK_CLUSTER_CHECK(waitUntil([&] { const auto leader = engines[0]->stats().cluster.stripeMetadataLeaderNodeId; return leader == 5 || leader == 6; }, std::chrono::seconds{20}));
    }
    if (historyPlacement) {
        const auto historical = engines[0]->getAt(bytesOf(key), revision);
        AKK_CLUSTER_CHECK(historical && textOf(*historical) == "old-value");
        auto history = engines[0]->history(bytesOf(key)); auto position = history.begin();
        AKK_CLUSTER_CHECK(position != std::default_sentinel && textOf(position->value) == "old-value");
    }
    AKK_CLUSTER_CHECK(textOf(*engines[0]->get(bytesOf(key))) == "new-value");
}

void testStripeOwnerAuthorityReclamationAdmission() {
    using namespace akkaradb::engine;
    IntegrityFactoryScope factory;
    const auto dir = makeTempDir("integrity-stripe-owner-authority");
    std::array<std::unique_ptr<AkkEngine>, 3> engines;
    for (size_t i = 0; i < engines.size(); ++i) {
        auto options = stripeStorageOptions(dir, i + 1);
        options.components.versionLogEnabled = false;
        options.cluster.runtime.stripeAutoRebuild = false;
        engines[i] = AkkEngine::open(options);
    }
    const auto key = keyOwnedBy(stripeStorageConfig(), 1, "integrity-owner-authority");
    AKK_CLUSTER_CHECK(waitUntil([&] { try { engines[0]->put(bytesOf(key), bytesOf("first")); return true; } catch (...) { return false; } }, std::chrono::seconds{20}));
    AKK_CLUSTER_CHECK(engines[0]->stats().cluster.stripeMetadataLeaderNodeId == 1);
    { std::lock_guard lock{integrityWriteGate->mutex}; integrityWriteGate->key = key; integrityWriteGate->reclamationArmed = true; }
    engines[0]->put(bytesOf(key), bytesOf("second"));
    bool paused;
    { std::unique_lock lock{integrityWriteGate->mutex}; paused = integrityWriteGate->cv.wait_for(lock, std::chrono::seconds{5}, [&] { return integrityWriteGate->entered; }); }
    auto writer = std::async(std::launch::async, [&] { engines[0]->put(bytesOf(key), bytesOf("third")); });
    (void)writer.wait_for(std::chrono::milliseconds{200});
    integrityWriteGate->release(); writer.get();
    AKK_CLUSTER_CHECK(paused);
    AKK_CLUSTER_CHECK(textOf(*engines[0]->get(bytesOf(key))) == "third");
    AKK_CLUSTER_CHECK(waitUntil([&] { return stripeInternalCounts(*engines[0]).second == 0; }, std::chrono::seconds{15}));
}

void runClusterIntegrityRegressionTests() {
    testStripeControlFraming();
    testStripeOwnerAuthorityReclamationAdmission();
    for (bool secure : {false, true}) {
        std::fprintf(stderr, "[integrity] non-Raft ordered replication secure=%u\n", secure);
        testNonRaftPartitionConcurrentReplicationOrder(secure);
        for (unsigned history = 0; history < 3; ++history) {
            std::fprintf(stderr, "[integrity] STRIPE GC secure=%u history=%u\n", secure, history);
            testStripeAuthorityHistoryReclamation(secure, history);
        }
    }
}

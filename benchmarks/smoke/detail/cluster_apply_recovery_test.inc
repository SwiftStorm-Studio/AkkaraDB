/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */

struct ReplicaCloseGate {
    std::mutex mutex;
    std::condition_variable cv;
    bool armed = false, entered = false, released = false;
    std::function<void()> forceDurable;
    std::function<void()> apply;
    void release() { std::lock_guard lock{mutex}; released = true; cv.notify_all(); }
};
std::shared_ptr<ReplicaCloseGate> replicaCloseGate;

class ReplicaCloseRuntime final : public IClusterRuntime {
    std::unique_ptr<ClusterRuntime> runtime_;
    std::shared_ptr<ReplicaCloseGate> gate_;
public:
    ReplicaCloseRuntime(std::unique_ptr<ClusterRuntime> runtime, std::shared_ptr<ReplicaCloseGate> gate)
        : runtime_{std::move(runtime)}, gate_{std::move(gate)} {}
    void start() override { runtime_->start(); }
    void close() override {
        std::unique_lock lock{gate_->mutex};
        if (gate_->armed) {
            gate_->entered = true; gate_->cv.notify_all();
            gate_->cv.wait(lock, [&] { return gate_->released; });
        }
        lock.unlock(); runtime_->close();
    }
    NodeRole role() const noexcept override { return runtime_->role(); }
    RaftRuntimeStats raftStats() const override { return runtime_->raftStats(); }
    bool ownsWriteKey(std::span<const uint8_t> key) const override { return runtime_->ownsWriteKey(key); }
    ClusterRouteTarget routeTarget(std::span<const uint8_t> key) const override { return runtime_->routeTarget(key); }
    uint64_t ownerNodeId(std::span<const uint8_t> key) const override { return runtime_->ownerNodeId(key); }
    ReadResponse readKey(std::span<const uint8_t> key, uint64_t seq) override { return runtime_->readKey(key, seq); }
    void shipEntry(uint64_t seq, ReplOpType op, std::span<const uint8_t> key, std::span<const uint8_t> value,
        uint8_t flags, uint64_t source) override { runtime_->shipEntry(seq, op, key, value, flags, source); }
    void shipBlob(uint64_t seq, uint64_t id, std::span<const uint8_t> value) override { runtime_->shipBlob(seq, id, value); }
};
struct ReplicaCloseFactoryScope {
    ReplicaCloseFactoryScope() {
        replicaCloseGate = std::make_shared<ReplicaCloseGate>();
        AKK_CLUSTER_CHECK(registerClusterRuntimeFactory([](auto dir, auto config, uint64_t id, auto callbacks, auto options) -> std::unique_ptr<IClusterRuntime> {
            if (id != 2) { return ClusterRuntime::create(std::move(dir), std::move(config), id, std::move(callbacks), std::move(options)); }
            replicaCloseGate->forceDurable = callbacks.forceDurable;
            replicaCloseGate->apply = [apply = callbacks.apply] { apply(1000, ReplOpType::PUT, bytesOf("closing-direct"), bytesOf("invalid"), 0, 1, 0); };
            return std::make_unique<ReplicaCloseRuntime>(ClusterRuntime::create(std::move(dir), std::move(config), id,
                std::move(callbacks), std::move(options)), replicaCloseGate);
        }));
    }
    ~ReplicaCloseFactoryScope() {
        replicaCloseGate->release(); replicaCloseGate.reset();
        (void)registerClusterRuntimeFactory([](auto dir, auto config, uint64_t id, auto callbacks, auto options) -> std::unique_ptr<IClusterRuntime> {
            return ClusterRuntime::create(std::move(dir), std::move(config), id, std::move(callbacks), std::move(options));
        });
    }
};

void testNonRaftClosingRejectsAcknowledgements(bool partitioned, AckStage stage) {
    using namespace akkaradb::engine;
    ReplicaCloseFactoryScope factory;
    const auto dir = makeTempDir("closing-replica-" + std::to_string(partitioned) + "-" + std::to_string(static_cast<unsigned>(stage)));
    const ClusterConfig config{{node(1, 38001, 38011), node(2, 38002, 38012)},
        partitioned ? ReplicationMode::PARTITIONED : ReplicationMode::MIRROR,
        {.mode = AckPolicyMode::ALL_TARGETS, .stage = stage},
        {.writeConsistency = WriteConsistency::ALL_CONFIGURED, .ackTimeoutAction = AckTimeoutAction::FAIL_WRITE, .ackTimeoutMs = 1000},
        {}, {}, partitioned ? uint64_t{0} : uint64_t{1}, testClusterId(9791), {.replicationFactor = 2}};
    std::array<AkkEngineOptions, 2> options;
    std::array<std::unique_ptr<AkkEngine>, 2> engines;
    for (size_t i = 0; i < engines.size(); ++i) {
        auto& opt = options[i]; opt.paths.dataDir = dir / std::to_string(i + 1);
        opt.components.clusterEnabled = true;
        opt.components.blobEnabled = opt.components.sstEnabled = opt.components.manifestEnabled = false;
        opt.cluster.config = config; opt.cluster.runtime.transportMode = TransportMode::PLAIN;
        opt.cluster.runtime.clusterGroupId = 9791;
        opt.cluster.runtime.startupRole = partitioned || i == 0 ? NodeStartupRole::PRIMARY : NodeStartupRole::REPLICA;
        writeNodeIdFile(opt.paths.dataDir / "node.id", i + 1); engines[i] = AkkEngine::open(opt);
    }
    const auto warmup = partitioned ? keyOwnedBy(config, 1, "closing-warmup-") : "closing-warmup";
    const auto key = partitioned ? keyOwnedBy(config, 1, "closing-rejected-") : "closing-rejected";
    AKK_CLUSTER_CHECK(waitUntil([&] { try { engines[0]->put(bytesOf(warmup), bytesOf("ready")); return true; } catch (...) { return false; } }));
    { std::lock_guard lock{replicaCloseGate->mutex}; replicaCloseGate->armed = true; }
    auto close = std::async(std::launch::async, [&] { engines[1]->close(); });
    bool entered;
    { std::unique_lock lock{replicaCloseGate->mutex}; entered = replicaCloseGate->cv.wait_for(lock, std::chrono::seconds{5}, [&] { return replicaCloseGate->entered; }); }
    bool rejected = false, applyRejected = false, syncRejected = false;
    try { engines[0]->put(bytesOf(key), bytesOf("retry-on-reconnect")); } catch (const std::runtime_error&) { rejected = true; }
    try { replicaCloseGate->apply(); } catch (const std::runtime_error&) { applyRejected = true; }
    try { replicaCloseGate->forceDurable(); } catch (const std::runtime_error&) { syncRejected = true; }
    // Release before checking so failures cannot leave the closing future blocked.
    replicaCloseGate->release(); close.get(); engines[1].reset();
    AKK_CLUSTER_CHECK(entered && rejected && applyRejected && syncRejected);
    engines[1] = AkkEngine::open(options[1]);
    AKK_CLUSTER_CHECK(waitUntil([&] { const auto value = engines[1]->get(bytesOf(key)); return value && textOf(*value) == "retry-on-reconnect"; }));
    // The failed ACK does not roll back the owner; reconnect must recover its write.
    engines[0]->put(bytesOf(key), bytesOf("after-reconnect"));
    AKK_CLUSTER_CHECK(textOf(*engines[1]->get(bytesOf(key))) == "after-reconnect");
    for (auto& engine : engines) { engine->close(); }
}
void runNonRaftCloseAcknowledgementTests() {
    for (bool partitioned : {false, true}) { for (auto stage : {AckStage::APPLIED, AckStage::DURABLE}) {
        testNonRaftClosingRejectsAcknowledgements(partitioned, stage);
    } }
}

akkaradb::engine::AkkEngineOptions raftApplyRecoveryOptions(const std::filesystem::path& dir, bool blobs) {
    using namespace akkaradb::engine;
    AkkEngineOptions options; options.paths.dataDir = dir;
    options.components.clusterEnabled = options.components.versionLogEnabled = true;
    options.components.blobEnabled = blobs;
    options.components.sstEnabled = options.components.manifestEnabled = false;
    options.wal.syncPolicy = wal::WalSyncPolicy::ALWAYS; options.wal.shardCount = 1;
    options.vlog.groupMicros = 5'000'000;
    options.cluster.config = ClusterConfig{{node(1, 38101, 38111)}, ReplicationMode::MIRROR, {},
        {.mode = ConsistencyMode::RAFT_QUORUM}, {}, {}, 0, testClusterId(9792)};
    options.cluster.runtime.transportMode = TransportMode::PLAIN;
    options.cluster.runtime.raftSnapshot.enabled = false;
    options.cluster.runtime.requests.enabled = true;
    options.cluster.runtime.raftBlobPolicy = blobs ? RaftBlobPolicy::RAFT_LOG : RaftBlobPolicy::REJECT;
    writeNodeIdFile(dir / "node.id", 1); return options;
}
int runRaftApplyRecoveryWriter(const char* point, const std::filesystem::path& dir) {
    using namespace akkaradb::engine;
    const bool blobs = std::filesystem::exists(dir / "blob-case");
    const bool remove = std::filesystem::exists(dir / "remove-case");
    auto engine = AkkEngine::open(raftApplyRecoveryOptions(dir, blobs));
    engine->put(bytesOf("key"), bytesOf("before"));
    const auto id = engine->newRequestId(60000);
    { std::ofstream token{dir / "token", std::ios::binary};
        token.write(reinterpret_cast<const char*>(id.nonce.data()), id.nonce.size());
        token.write(reinterpret_cast<const char*>(&id.expiresAtUnixMs), sizeof(id.expiresAtUnixMs)); }
    setStorageCrashPoint(point);
    if (remove) { (void)engine->removeWithRequest(id, bytesOf("key")); }
    else { (void)engine->putWithRequest(id, bytesOf("key"), bytesOf(blobs ? std::string(512 * 1024, 'b') : "after")); }
    return 1;
}
void testRaftApplyRecoveryHistory(const std::filesystem::path& executable) {
    using namespace akkaradb::engine;
    for (const auto point : {"raft.apply.before_durable", "raft.request.after_apply"}) {
        for (unsigned variant = 0; variant < 3; ++variant) {
            const bool blobs = variant == 1, remove = variant == 2;
            const auto dir = makeTempDir("raft-apply-history-" + std::string{point} + "-" + std::to_string(variant));
            if (blobs) { std::ofstream marker{dir / "blob-case"}; }
            if (remove) { std::ofstream marker{dir / "remove-case"}; }
            runStorageCrashChild(executable, "--raft-apply-recovery-writer", point, dir);
            ClusterRequestId id;
            { std::ifstream token{dir / "token", std::ios::binary};
                token.read(reinterpret_cast<char*>(id.nonce.data()), id.nonce.size());
                token.read(reinterpret_cast<char*>(&id.expiresAtUnixMs), sizeof(id.expiresAtUnixMs)); AKK_CLUSTER_CHECK(token.good()); }
            const auto value = blobs ? std::string(512 * 1024, 'b') : "after";
            auto options = raftApplyRecoveryOptions(dir, blobs);
            for (unsigned reopen = 0; reopen < 2; ++reopen) {
                auto engine = AkkEngine::open(options);
                const auto history = akk_test::collectHistory(engine->history(bytesOf("key")));
                AKK_CLUSTER_CHECK(history.size() == 2 && textOf(history[0].value) == "before");
                AKK_CLUSTER_CHECK(history[1].sourceNodeId == 1 && history[1].timestampNs != 0);
                const auto before = engine->getAt(bytesOf("key"), history[0].seq);
                AKK_CLUSTER_CHECK(before && textOf(*before) == "before");
                const auto current = engine->get(bytesOf("key"));
                const auto historical = engine->getAt(bytesOf("key"), history[1].seq);
                if (remove) {
                    AKK_CLUSTER_CHECK(!current && !historical && (history[1].flags & akkaradb::core::MemHdr16::FLAG_TOMBSTONE));
                } else {
                    AKK_CLUSTER_CHECK(current && historical && textOf(*current) == value && textOf(*historical) == value && textOf(history[1].value) == value);
                }
                const auto result = engine->queryRequest(id); AKK_CLUSTER_CHECK(result.status == ClusterRequestStatus::APPLIED);
                const auto retry = remove ? engine->removeWithRequest(id, bytesOf("key")) : engine->putWithRequest(id, bytesOf("key"), bytesOf(value));
                AKK_CLUSTER_CHECK(retry.sequence == result.sequence && akk_test::collectHistory(engine->history(bytesOf("key"))).size() == 2);
                engine->close();
            }
        }
    }
}

/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
void testClusterNodeTransferResources() {
    using namespace akkaradb::engine::cluster::detail;
    auto limits = ClusterNodeResourceLimits{};
    limits.transferMemoryBytes = 128 * 1024; limits.concurrentTransfers = 1;
    auto nodeBudget = std::make_shared<NodeResourceBudget>(limits);
    ReplicationTransferOptions options; options.resumeEnabled = false;
    auto first = std::make_shared<TransferBudget>(options, nodeBudget);
    auto second = std::make_shared<TransferBudget>(options, nodeBudget);
    auto memory = first->reserve(TransferBudget::Resource::MEMORY, 96 * 1024);
    bool rejected = false;
    try { (void)second->reserve(TransferBudget::Resource::MEMORY, 64 * 1024); } catch (const std::runtime_error&) { rejected = true; }
    AKK_CLUSTER_CHECK(rejected && second->used(TransferBudget::Resource::MEMORY) == 0);
    auto active = first->reserve(TransferBudget::Resource::ACTIVE, 1);
    rejected = false;
    try { (void)second->reserve(TransferBudget::Resource::ACTIVE, 1); } catch (const std::runtime_error&) { rejected = true; }
    AKK_CLUSTER_CHECK(rejected);
    memory.reset(); active.reset();
    auto recovered = second->reserve(TransferBudget::Resource::MEMORY, 128 * 1024);
    AKK_CLUSTER_CHECK(nodeBudget->transferMemory->used() == 128 * 1024);
    recovered.reset();
    AKK_CLUSTER_CHECK(nodeBudget->transferMemory->used() == 0 && nodeBudget->transfers->used() == 0);
    auto quota = std::make_shared<NodeResourceQuota>(1);
    auto held = quota->reserve(1);
    auto timedOut = quota->waitReserve(1, std::chrono::steady_clock::now() + std::chrono::milliseconds{30}, [] { return false; });
    AKK_CLUSTER_CHECK(!timedOut);
    std::atomic<bool> cancelled = false;
    auto waiting = std::async(std::launch::async, [&] {
        return quota->waitReserve(1, std::chrono::steady_clock::now() + std::chrono::seconds{5}, [&] { return cancelled.load(); });
    });
    cancelled = true; AKK_CLUSTER_CHECK(!waiting.get());
    held.reset(); AKK_CLUSTER_CHECK(quota->reserve(1) != nullptr);
    ClusterRuntimeOptions invalid; invalid.nodeResources.outgoingForwardRequests = 0;
    rejected = false;
    try { ensureNodeResourceBudget(invalid); } catch (const std::invalid_argument&) { rejected = true; }
    AKK_CLUSTER_CHECK(rejected);
}

std::map<uint64_t, std::pair<std::shared_ptr<akkaradb::engine::cluster::detail::NodeResourceBudget>, unsigned>> observedNodeBudgets;
struct NodeBudgetFactoryScope {
    NodeBudgetFactoryScope() {
        observedNodeBudgets.clear();
        AKK_CLUSTER_CHECK(registerClusterRuntimeFactory([](std::filesystem::path dir, ClusterConfig config, uint64_t id,
            ClusterEngineCallbacks callbacks, ClusterRuntimeOptions options) -> std::unique_ptr<IClusterRuntime> {
            AKK_CLUSTER_CHECK(options.nodeResourceBudget);
            auto& observed = observedNodeBudgets[id];
            if (observed.first) { AKK_CLUSTER_CHECK(observed.first == options.nodeResourceBudget); }
            else { observed.first = options.nodeResourceBudget; }
            ++observed.second;
            return ClusterRuntime::create(std::move(dir), std::move(config), id, std::move(callbacks), std::move(options));
        }));
    }
    ~NodeBudgetFactoryScope() {
        (void)registerClusterRuntimeFactory([](std::filesystem::path dir, ClusterConfig config, uint64_t id,
            ClusterEngineCallbacks callbacks, ClusterRuntimeOptions options) -> std::unique_ptr<IClusterRuntime> {
            return ClusterRuntime::create(std::move(dir), std::move(config), id, std::move(callbacks), std::move(options));
        });
        observedNodeBudgets.clear();
    }
};

void testClusterNodePartitionResources(bool secure) {
    std::fprintf(stderr, "[node-resources] PARTITIONED shared budgets and single RPC slot %s\n", secure ? "SECURE" : "PLAIN");
    NodeBudgetFactoryScope factory;
    ClusterTransactionFixture fixture{makeTempDir("cluster-node-budgets"), true, secure, 300000, false, 0,
        [](size_t, akkaradb::engine::AkkEngineOptions& options) {
            options.cluster.runtime.nodeResources.outgoingForwardRequests = 1;
            options.cluster.runtime.nodeResources.incomingForwardRequests = 1;
        }, 2};
    AKK_CLUSTER_CHECK(observedNodeBudgets.size() == 3);
    for (const auto& [id, observed] : observedNodeBudgets) { (void)id; AKK_CLUSTER_CHECK(observed.second > 1); }
    AKK_CLUSTER_CHECK(observedNodeBudgets.at(1).first != observedNodeBudgets.at(2).first);
    const auto keys = transactionPartitionKeys(fixture.config);
    auto transaction = fixture.begin();
    for (const auto& key : keys) { transaction.put(bytesOf(key), bytesOf("shared")); }
    transaction.end();
    auto proof = fixture.begin(); AKK_CLUSTER_CHECK(proof.count() == keys.size()); proof.end();
    auto budget = observedNodeBudgets.at(1).first;
    auto memory = budget->forwardReceive->reserve(4096);
    auto transfer = budget->transferMemory->reserve(1024);
    auto active = budget->transfers->reserve(1);
    const auto stats = fixture.engines[0]->stats().cluster;
    AKK_CLUSTER_CHECK(stats.nodeForwardReceiveMemoryBytes >= 4096 && stats.nodeTransferMemoryBytes == 1024 && stats.nodeActiveTransfers == 1);
    memory.reset(); transfer.reset(); active.reset();
    AKK_CLUSTER_CHECK(waitUntil([&] { const auto sampled = fixture.engines[0]->stats().cluster;
        return sampled.nodeIncomingForwardRequests == 0 && sampled.nodeOutgoingForwardRequests == 0;
    }));
}

void testClusterNodeIncomingResources() {
    using namespace akkaradb::engine::cluster::detail;
    std::fprintf(stderr, "[node-resources] independent Raft groups share incoming slots and receive memory\n");
    const auto dir = makeTempDir("node-incoming-budgets");
    ClusterRuntimeOptions options; options.nodeResources.incomingForwardRequests = 1;
    options.nodeResources.outgoingForwardRequests = 1;
    options.nodeResourceBudget = std::make_shared<NodeResourceBudget>(options.nodeResources);
    const ClusterConfig a{{node(1, 31701, 31711), node(2, 31702, 31712)}, ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM}};
    const ClusterConfig b{{node(1, 31721, 31731), node(2, 31722, 31732)}, ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM}};
    auto first = makeRuntime(dir / "a", a, 1, options), second = makeRuntime(dir / "b", b, 1, options);
    first->runtime->start(); second->runtime->start();
    RaftTestSocket held; openRaftTestPeer(held, 31711, 2, a, '3', options, 1);
    AKK_CLUSTER_CHECK(waitUntil([&] { return first->runtime->raftStats().nodeIncomingForwardRequests == 1; }));
    RaftTestSocket rejected; openRaftTestPeer(rejected, 31731, 2, b, '3', options, 1, 4); rejected.close();
    AKK_CLUSTER_CHECK(second->runtime->raftStats().incomingForwardConnections == 0);
    RaftTestSocket control; openRaftTestPeer(control, 31731, 2, b, '3', options, 2); control.close();
    RaftTestSocket consensus; openRaftTestPeer(consensus, 31731, 2, b, '3', options, 0); consensus.close();
    auto allMemory = options.nodeResourceBudget->forwardReceive->reserve(options.nodeResources.forwardReceiveMemoryBytes);
    const auto wire = encodeFrame(ReplMsgType::FORWARD_REQUEST, std::vector<uint8_t>(1024));
    AKK_CLUSTER_CHECK(held.send(std::span<const uint8_t>{wire}.first(ReplFrameHeader::SIZE)));
    std::array<uint8_t, 1> response{}; AKK_CLUSTER_CHECK(!held.receive(response));
    allMemory.reset(); held.close();
    AKK_CLUSTER_CHECK(waitUntil([&] { return options.nodeResourceBudget->incomingForward->used() == 0; }));
    RaftTestSocket reusable; openRaftTestPeer(reusable, 31731, 2, b, '3', options, 1); reusable.close();
    auto outgoing = options.nodeResourceBudget->outgoingForward->reserve(1);
    for (auto* runtime : {first->runtime.get(), second->runtime.get()}) {
        ForwardRequest request; request.operation = ForwardOperation::GET; request.timeoutMs = 30; request.entries.push_back({{1}, {}});
        bool expired = false;
        try { (void)runtime->forwardTo(2, request); }
        catch (const ClusterRoutingError& error) { expired = error.code == ClusterRoutingCode::FORWARD_UNAVAILABLE && !error.outcomeUnknown(); }
        AKK_CLUSTER_CHECK(expired && options.nodeResourceBudget->outgoingForward->used() == 1);
    }
    outgoing.reset();
    second->runtime->close(); first->runtime->close();
    AKK_CLUSTER_CHECK(options.nodeResourceBudget->incomingForward->used() == 0 && options.nodeResourceBudget->forwardReceive->used() == 0);
}
void runClusterNodeResourceTests() {
    const ClusterRuntimeOptions defaults;
    AKK_CLUSTER_CHECK(defaults.nodeResources.raftReceiveMemoryBytes == 128ull * 1024 * 1024 &&
        defaults.nodeResources.forwardReceiveMemoryBytes == 128ull * 1024 * 1024 &&
        defaults.nodeResources.outgoingForwardRequests == 128 && defaults.nodeResources.incomingForwardRequests == 128 &&
        defaults.nodeResources.transferMemoryBytes == 8ull * 1024 * 1024 && defaults.nodeResources.concurrentTransfers == 8);
    testClusterNodeTransferResources(); testClusterNodeIncomingResources();
    testClusterNodePartitionResources(false); testClusterNodePartitionResources(true);
}

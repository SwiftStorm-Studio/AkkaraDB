/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */

void testClusterSharedSchedulingAndIdleAdmission() {
    using namespace akkaradb::engine::cluster::detail;
    auto scheduler = std::make_shared<NodeScheduler>(2);
    std::mutex mutex;
    std::set<std::thread::id> workers;
    std::atomic<unsigned> completed{0};
    std::vector<std::shared_ptr<NodeScheduler::Task>> groups;
    for (unsigned i = 0; i < 64; ++i) {
        groups.push_back(scheduler->task([&] {
            { std::lock_guard lock{mutex}; workers.insert(std::this_thread::get_id()); }
            ++completed;
        }));
        groups.back()->notify();
    }
    AKK_CLUSTER_CHECK(waitUntil([&] { return completed == groups.size(); }));
    for (auto& group : groups) { group->cancel(); }
    AKK_CLUSTER_CHECK(workers.size() <= 2);
    ParallelismGate gate;
    ReleaseParallelismGate release{gate};
    std::atomic<unsigned> calls{0};
    auto blocked = scheduler->task([&] { if (++calls == 1) { gate.pause(); } });
    blocked->notify(); AKK_CLUSTER_CHECK(gate.await());
    for (unsigned i = 0; i < 1000; ++i) { blocked->notify(); }
    std::promise<void> independent;
    auto another = scheduler->task([&] { independent.set_value(); });
    another->notify();
    AKK_CLUSTER_CHECK(independent.get_future().wait_for(std::chrono::seconds{1}) == std::future_status::ready);
    ParallelismGate secondGate;
    auto secondBlocked = scheduler->task([&] { secondGate.pause(); });
    ReleaseParallelismGate releaseSecond{secondGate};
    secondBlocked->notify(); AKK_CLUSTER_CHECK(secondGate.await());
    std::promise<void> heartbeat;
    auto timer = scheduler->task([&] { heartbeat.set_value(); }, true);
    timer->notify();
    AKK_CLUSTER_CHECK(heartbeat.get_future().wait_for(std::chrono::seconds{1}) == std::future_status::ready);
    secondGate.release(); secondBlocked->cancel(); timer->cancel();
    auto cancelling = std::async(std::launch::async, [&] { blocked->cancel(); });
    AKK_CLUSTER_CHECK(cancelling.wait_for(std::chrono::milliseconds{30}) == std::future_status::timeout);
    gate.release(); cancelling.get();
    AKK_CLUSTER_CHECK(calls == 1);
    another->cancel();

    auto quota = std::make_shared<NodeResourceQuota>(1);
    auto cached = quota->reserve(1);
    AKK_CLUSTER_CHECK(!quota->tryReserve(1));
    bool evicted = false;
    quota->park(cached, [&] { evicted = true; });
    auto active = quota->reserve(1);
    AKK_CLUSTER_CHECK(evicted && !quota->activate(cached) && quota->used() == 1);
    cached.reset(); AKK_CLUSTER_CHECK(quota->used() == 1);
    quota->park(active, [] {});
    AKK_CLUSTER_CHECK(quota->activate(active) && !quota->tryReserve(1));
    active.reset(); AKK_CLUSTER_CHECK(quota->used() == 0);
}

void testClusterBufferedSpool() {
    const auto dir = makeTempDir("buffered-query-spool") / "scratch";
    std::atomic<uint64_t> used{0};
    {
        akkaradb::engine::detail::ClusterQuerySpool spool{dir, used, 200000};
        std::vector<uint8_t> expected(4096, 17);
        spool.append(expected);
        AKK_CLUSTER_CHECK(spool.read(0, expected.size()) == expected && !std::filesystem::exists(dir));
        const std::vector<uint8_t> large(70000, 31);
        spool.append(large); expected.insert(expected.end(), large.begin(), large.end());
        AKK_CLUSTER_CHECK(spool.read(4000, 66000) == std::vector<uint8_t>(expected.begin() + 4000, expected.begin() + 70000));
        spool.append(bytesOf("tail")); expected.insert(expected.end(), {'t', 'a', 'i', 'l'});
        AKK_CLUSTER_CHECK(spool.read(0, expected.size()) == expected && used == expected.size());
        bool rejected = false;
        try { spool.append(std::vector<uint8_t>(200000)); } catch (const std::runtime_error&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected && used == expected.size());
        AKK_CLUSTER_CHECK(spool.read(spool.size(), 0).empty());
    }
    AKK_CLUSTER_CHECK(used == 0 && std::filesystem::is_empty(dir));
}

void testClusterMultiplexedReads(bool secure) {
    const auto dir = makeTempDir("multiplexed-peer-reads");
    const ClusterConfig config{{node(1, 39601, 39611), node(2, 39602, 39612)}, ReplicationMode::MIRROR,
        {}, {}, {}, {}, 1, testClusterId(9960)};
    ClusterRuntimeOptions options; options.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
    options.clusterGroupId = 9960; options.replicationClusterId = config.clusterId();
    options.replicationConfigFingerprint = config.replicationFingerprint();
    if (secure) {
        for (uint64_t id = 1; id <= 2; ++id) {
            const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
            options.secure.pinnedPeers.push_back({id, identity.publicKey});
        }
    }
    auto serverOptions = options, clientOptions = options;
    serverOptions.secure.identitySeedPath = dir / "identity-1";
    clientOptions.secure.identitySeedPath = dir / "identity-2";
    clientOptions.secure.expectedPrimaryNodeId = 1;
    auto server = ReplicationServer::create(39611, 1, [] { return uint64_t{0}; }, config.ackPolicy(), config.consistency(), 1, {2}, serverOptions);
    auto client = ReplicationClient::create("127.0.0.1", 39611, 2, [] { return uint64_t{0}; }, config.ackPolicy(), clientOptions, true);
    ParallelismGate gate;
    server->setReadCallback([&](const ReadRequest& request) {
        if (textOf(request.key) == "slow") { gate.pause(); }
        ReadResponse response; response.status = ReadStatus::FOUND; response.value = request.key; return response;
    });
    server->start(); client->start();
    AKK_CLUSTER_CHECK(waitUntil([&] { return client->connected() && server->replicaCount() == 1; }));
    auto slow = std::async(std::launch::async, [&] {
        try { (void)client->readKey(bytesOf("slow"), 0, 200); return false; } catch (const std::runtime_error&) { return true; }
    });
    ReleaseParallelismGate release{gate};
    AKK_CLUSTER_CHECK(gate.await() && slow.get());
    const auto fast = client->readKey(bytesOf("fast"), 0, 2000);
    AKK_CLUSTER_CHECK(fast.status == ReadStatus::FOUND && textOf(fast.value) == "fast");
    gate.release();
    for (unsigned i = 0; i < 8; ++i) {
        const auto result = client->readKey(bytesOf("next"), 0, 2000);
        AKK_CLUSTER_CHECK(textOf(result.value) == "next");
    }
    client->close(); server->close();
}

void testClusterForwardReuseAndBatch(bool secure) {
    using Engine = akkaradb::engine::AkkEngine;
    const auto dir = makeTempDir("forward-reuse-batch");
    auto raft = onlineRaftMembership(); raft.membership.allowLearners = true;
    const ClusterConfig config{{node(1, 39621, 39631), node(2, 39622, 39632, DATA_AND_COORDINATOR | RAFT_LEARNER), node(3, 39623, 39633)},
        ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 3000}, raft};
    ClusterRuntimeOptions options; options.routingMode = ClusterRoutingMode::FORWARD;
    options.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
    if (secure) {
        for (uint64_t id = 1; id <= 3; ++id) {
            const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
            options.secure.pinnedPeers.push_back({id, identity.publicKey});
        }
    }
    std::mutex mutex; std::condition_variable cv; unsigned active = 0;
    std::atomic<bool> batch{false};
    ParallelismGate pageGate;
    std::vector<uint8_t> queryBytes;
    const std::string queryValue(32768, 'p');
    for (unsigned i = 0; i < 40; ++i) {
        const auto key = "row-" + std::to_string(1000 + i);
        appendRaftTestInteger(queryBytes, key.size()); appendRaftTestInteger(queryBytes, queryValue.size());
        queryBytes.insert(queryBytes.end(), key.begin(), key.end());
        queryBytes.insert(queryBytes.end(), queryValue.begin(), queryValue.end());
    }
    const auto execute = [&](const ForwardRequest& request) {
        if (request.operation == ForwardOperation::READ_QUERY) {
            ForwardResponse result; result.success = true;
            const auto& command = request.entries.front().value;
            if (command.front() == 0) { // SCAN opens a prepared cursor.
                result.value.assign(16, 1); appendRaftTestInteger(result.value, queryBytes.size()); appendRaftTestInteger(result.value, 40);
            } else if (command.front() == 4) { // NEXT, with the requested byte offset.
                uint64_t offset = 0;
                for (size_t i = 0; i < 8; ++i) { offset |= uint64_t{command.at(i + 1)} << (i * 8); }
                if (offset) { pageGate.pause(); }
                const auto end = std::min<uint64_t>(queryBytes.size(), offset + 1024 * 1024);
                result.value.assign(queryBytes.begin() + static_cast<std::ptrdiff_t>(offset),
                    queryBytes.begin() + static_cast<std::ptrdiff_t>(end));
            }
            return result;
        }
        if (batch.load()) {
            std::unique_lock lock{mutex}; ++active; cv.notify_all();
            if (!cv.wait_for(lock, std::chrono::seconds{2}, [&] { return active >= 2; })) {
                throw std::runtime_error("batch point reads did not overlap");
            }
        }
        ForwardResponse result; result.success = result.found = true; result.value = request.entries.front().key; return result;
    };
    options.secure.identitySeedPath = dir / "identity-1";
    auto one = makeRuntime(dir / "n1", config, 1, options, secure, execute);
    options.secure.identitySeedPath = dir / "identity-3";
    auto three = makeRuntime(dir / "n3", config, 3, options, secure, execute);
    akkaradb::engine::AkkEngineOptions engineOptions;
    engineOptions.paths.dataDir = dir / "n2";
    engineOptions.components.clusterEnabled = true; engineOptions.components.blobEnabled = false;
    engineOptions.cluster.config = config; engineOptions.cluster.runtime = options;
    engineOptions.cluster.runtime.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
    engineOptions.cluster.runtime.secure.identitySeedPath = dir / "identity-2";
    writeNodeIdFile(dir / "n2" / "node.id", 2);
    one->runtime->start(); three->runtime->start(); auto two = Engine::open(engineOptions);
    AKK_CLUSTER_CHECK(waitUntil([&] { return one->runtime->role() == NodeRole::PRIMARY || three->runtime->role() == NodeRole::PRIMARY; }, std::chrono::seconds{20}));
    AKK_CLUSTER_CHECK(waitUntil([&] { try { return two->get(bytesOf("warm")).has_value(); } catch (const std::runtime_error&) { return false; } }));
    for (unsigned i = 0; i < 2; ++i) { AKK_CLUSTER_CHECK(two->get(bytesOf("warm"))); }
    const auto before = two->stats().cluster.outboundConnectionsTotal;
    for (unsigned i = 0; i < 16; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds{25});
        AKK_CLUSTER_CHECK(textOf(*two->get(bytesOf("reuse"))) == "reuse");
    }
    const auto connections = two->stats().cluster.outboundConnectionsTotal - before;
    std::fprintf(stderr, "[cluster performance] 16 forwarding GETs: %llu new connections (%s)\n",
        static_cast<unsigned long long>(connections), secure ? "SECURE" : "PLAIN");
    AKK_CLUSTER_CHECK(connections == 0);
    batch.store(true);
    const std::array<std::string, 3> ownedKeys{"first", "second", "first"};
    const std::array keys{bytesOf(ownedKeys[0]), bytesOf(ownedKeys[1]), bytesOf(ownedKeys[2])};
    const auto results = two->getBatch(keys);
    AKK_CLUSTER_CHECK(results.size() == keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        AKK_CLUSTER_CHECK(results[i].found && textOf(results[i].value) == textOf(keys[i]));
    }
    { std::lock_guard lock{mutex}; active = 0; }
    std::this_thread::sleep_for(std::chrono::milliseconds{50});
    const auto batchConnections = two->stats().cluster.outboundConnectionsTotal;
    const auto again = two->getBatch(keys);
    for (size_t i = 0; i < keys.size(); ++i) { AKK_CLUSTER_CHECK(again[i].found && textOf(again[i].value) == textOf(keys[i])); }
    AKK_CLUSTER_CHECK(two->stats().cluster.outboundConnectionsTotal == batchConnections);
    batch.store(false);
    {
        akkaradb::core::BufferArena arena;
        auto scan = two->scan(arena);
        ReleaseParallelismGate releasePage{pageGate};
        auto position = scan.begin();
        AKK_CLUSTER_CHECK(position != std::default_sentinel && textOf(position->key) == "row-1000");
        // Page two must already be requested while the caller still owns page one's first record.
        AKK_CLUSTER_CHECK(pageGate.await());
        pageGate.release();
        unsigned count = 0;
        for (; position != std::default_sentinel; ++position) {
            AKK_CLUSTER_CHECK(textOf(position->key) == "row-" + std::to_string(1000 + count));
            AKK_CLUSTER_CHECK(textOf(position->value) == queryValue); ++count;
        }
        AKK_CLUSTER_CHECK(count == 40);
    }
    AKK_CLUSTER_CHECK(waitUntil([&] { return two->stats().cluster.nodeOutgoingForwardRequests == 0; }));
    two->close(); three->runtime->close(); one->runtime->close();
}

void testClusterApplyDoesNotBlockReplication() {
    const auto dir = makeTempDir("async-raft-apply");
    const ClusterConfig config{{node(1, 39641, 39651), node(2, 39642, 39652)}, ReplicationMode::MIRROR,
        {}, ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 2000}};
    ClusterRuntimeOptions options; options.raftSnapshot.enabled = false;
    auto one = makeRuntime(dir / "n1", config, 1, options);
    ParallelismGate gate;
    auto two = makeRuntime(dir / "n2", config, 2, options, false, {}, [&](auto& callbacks) {
        const auto apply = callbacks.apply;
        callbacks.apply = [&, apply](auto seq, auto op, auto key, auto value, auto flags, auto source, auto timestamp) {
            if (textOf(key) == "blocked") { gate.pause(); }
            apply(seq, op, key, value, flags, source, timestamp);
        };
    });
    one->runtime->start(); two->runtime->start();
    std::array<RuntimeHarness*, 2> nodes{one.get(), two.get()}; ensurePreferredLeader(nodes, std::chrono::seconds{20});
    one->runtime->shipEntry(1, ReplOpType::PUT, bytesOf("blocked"), bytesOf("one"), 0, 1);
    ReleaseParallelismGate release{gate};
    AKK_CLUSTER_CHECK(gate.await());
    one->runtime->shipEntry(2, ReplOpType::PUT, bytesOf("independent"), bytesOf("two"), 0, 1);
    AKK_CLUSTER_CHECK(one->runtime->raftStats().commitIndex >= 2);
    auto backpressured = std::async(std::launch::async, [&] {
        one->runtime->shipEntry(3, ReplOpType::PUT, bytesOf("bounded"), bytesOf("three"), 0, 1);
    });
    AKK_CLUSTER_CHECK(backpressured.wait_for(std::chrono::milliseconds{100}) == std::future_status::timeout);
    gate.release();
    backpressured.get();
    AKK_CLUSTER_CHECK(waitUntil([&] { return two->lastSeq == 3; }));
    two->runtime->close(); one->runtime->close();
}

void testClusterRaftWindowCatchup(bool secure) {
    const auto dir = makeTempDir("raft-window-catchup");
    const ClusterConfig config{{node(1, 39661, 39671), node(2, 39662, 39672), node(3, 39663, 39673)},
        ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 5000}};
    ClusterRuntimeOptions options; options.raftSnapshot.enabled = false;
    if (secure) {
        for (uint64_t id = 1; id <= 3; ++id) {
            const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
            options.secure.pinnedPeers.push_back({id, identity.publicKey});
        }
    }
    const auto make = [&](uint64_t id) {
        auto peerOptions = options; peerOptions.secure.identitySeedPath = dir / ("identity-" + std::to_string(id));
        return makeRuntime(dir / ("n" + std::to_string(id)), config, id, peerOptions, secure);
    };
    auto one = make(1), two = make(2), three = make(3);
    one->runtime->start(); two->runtime->start(); three->runtime->start();
    std::array<RuntimeHarness*, 3> nodes{one.get(), two.get(), three.get()}; ensurePreferredLeader(nodes);
    two->runtime->close();
    const std::string payload(48000, 'p');
    std::vector<std::future<void>> writes;
    for (uint64_t seq = 1; seq <= 384; ++seq) {
        writes.push_back(one->runtime->submitEntry(seq, ReplOpType::PUT, bytesOf("window"), bytesOf(payload), 0, 1));
    }
    for (auto& write : writes) { write.get(); }
    two = make(2); two->runtime->start();
    AKK_CLUSTER_CHECK(waitUntil([&] { return two->hasState(384, "window", payload); }, std::chrono::seconds{30}));
    two->runtime->close(); three->runtime->close(); one->runtime->close();
    two = make(2);
    AKK_CLUSTER_CHECK(two->hasState(384, "window", payload));
    two->runtime->close();
}

void testClusterManySmallSnapshotChunks() {
    const auto dir = makeTempDir("small-snapshot-chunks");
    const ClusterConfig config{{node(1, 39681, 39691), node(2, 39682, 39692), node(3, 39683, 39693)},
        ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 5000}};
    ClusterRuntimeOptions options;
    options.raftSnapshot.minLogEntries = 2;
    options.raftBlobChunkSizeBytes = 37;
    constexpr unsigned records = 12000;
    const std::string value(64, 's');
    const auto exporter = [&](ClusterEngineCallbacks& callbacks) {
        callbacks.exportSnapshot = [getSequence = callbacks.getLastSeq, value]() -> std::optional<ClusterSnapshot> {
            const auto sequence = getSequence();
            if (!sequence) { return {}; }
            ClusterSnapshot snapshot; snapshot.seq = sequence;
            snapshot.forEachEntry = [value](const ClusterSnapshot::EntryVisitor& visitor) {
                for (unsigned i = 0; i < records; ++i) {
                    const auto key = "tiny-" + std::to_string(i);
                    if (!visitor.beginEntry(bytesOf(key), value.size(), snapshotValueCrc(bytesOf(value))) ||
                        !visitor.appendValueChunk(0, bytesOf(value)) || !visitor.finishEntry()) { return false; }
                }
                return true;
            };
            return snapshot;
        };
    };
    auto one = makeRuntime(dir / "n1", config, 1, options, false, {}, exporter);
    auto three = makeRuntime(dir / "n3", config, 3, options, false, {}, exporter);
    one->runtime->start(); three->runtime->start();
    std::array<RuntimeHarness*, 2> voters{one.get(), three.get()}; ensurePreferredLeader(voters);
    one->runtime->shipEntry(1, ReplOpType::PUT, bytesOf("tiny-11999"), bytesOf(value), 0, 1);
    AKK_CLUSTER_CHECK(waitUntil([&] { return one->runtime->raftStats().snapshotIndex >= 2; }));
    AKK_CLUSTER_CHECK(waitUntil([&] { return three->hasState(1, "tiny-11999", value); }));
    // Discard pre-compaction AppendEntries already waiting to connect. Those
    // would legitimately catch up a late listener without installing a snapshot.
    one->runtime->close(); three->runtime->close();
    one = makeRuntime(dir / "n1", config, 1, options, false, {}, exporter);
    three = makeRuntime(dir / "n3", config, 3, options, false, {}, exporter);
    one->runtime->start(); three->runtime->start();
    voters = {one.get(), three.get()}; ensurePreferredLeader(voters);
    std::atomic<unsigned> installed{0};
    auto two = makeRuntime(dir / "n2", config, 2, options, false, {}, [&](auto& callbacks) {
        callbacks.finishSnapshotEntry = [&] { ++installed; };
        callbacks.finishSnapshot = [finish = callbacks.finishSnapshot](uint64_t seq, uint64_t count) {
            AKK_CLUSTER_CHECK(count == records); finish(seq, 1);
        };
    });
    const auto started = std::chrono::steady_clock::now();
    two->runtime->start();
    AKK_CLUSTER_CHECK(waitUntil([&] { return two->hasState(1, "tiny-11999", value); }, std::chrono::seconds{20}));
    AKK_CLUSTER_CHECK(installed == records);
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started).count();
    std::fprintf(stderr, "[cluster performance] 24,000 small snapshot chunks: %lld ms (loopback)\n", static_cast<long long>(elapsed));
    two->runtime->close(); three->runtime->close(); one->runtime->close();
}

void runClusterPerformanceTests() {
    std::fprintf(stderr, "[cluster performance] shared scheduling and reclaimable idle admission\n");
    testClusterSharedSchedulingAndIdleAdmission();
    std::fprintf(stderr, "[cluster performance] buffered spool\n");
    testClusterBufferedSpool();
    std::fprintf(stderr, "[cluster performance] multiplexed reads PLAIN/SECURE\n");
    testClusterMultiplexedReads(false); testClusterMultiplexedReads(true);
    std::fprintf(stderr, "[cluster performance] forwarding reuse and batch reads PLAIN/SECURE\n");
    testClusterForwardReuseAndBatch(false); testClusterForwardReuseAndBatch(true);
    std::fprintf(stderr, "[cluster performance] consensus independent of application\n");
    testClusterApplyDoesNotBlockReplication();
    std::fprintf(stderr, "[cluster performance] multi-frame catch-up PLAIN/SECURE\n");
    testClusterRaftWindowCatchup(false); testClusterRaftWindowCatchup(true);
    std::fprintf(stderr, "[cluster performance] many small snapshot chunks\n");
    testClusterManySmallSnapshotChunks();
}

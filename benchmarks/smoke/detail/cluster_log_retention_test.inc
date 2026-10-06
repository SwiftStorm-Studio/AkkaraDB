akkaradb::engine::AkkEngineOptions retainedHistorySnapshotOptions(const std::filesystem::path& dir, uint64_t id) {
    const ClusterConfig config{{node(1, 28601, 28611), node(2, 28602, 28612), node(3, 28603, 28613)},
        ReplicationMode::MIRROR, {}, {.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 2000}, {}, {}, 0, testClusterId(9567)};
    akkaradb::engine::AkkEngineOptions opt;
    opt.paths.dataDir = dir / std::to_string(id);
    opt.components.clusterEnabled = true; opt.components.versionLogEnabled = true;
    opt.components.sstEnabled = false; opt.components.manifestEnabled = false;
    opt.components.blobEnabled = true; opt.blob.thresholdBytes = 16;
    opt.cluster.config = config; opt.cluster.runtime.transportMode = TransportMode::PLAIN;
    opt.cluster.runtime.raftBlobPolicy = RaftBlobPolicy::RAFT_LOG;
    opt.cluster.runtime.requests.enabled = true;
    opt.cluster.runtime.raftSnapshot.minLogEntries = 1;
    opt.cluster.runtime.raftHeartbeatIntervalMs = 30;
    writeNodeIdFile(opt.paths.dataDir / "node.id", id);
    return opt;
}

int runRetainedHistorySnapshotReceiver(const std::filesystem::path& dir, const char* point) {
    setStorageCrashPoint(point);
    auto engine = akkaradb::engine::AkkEngine::open(retainedHistorySnapshotOptions(dir, 3));
    std::this_thread::sleep_for(std::chrono::seconds{20});
    return 1;
}

void testRaftEmptyStateCompaction() {
    using akkaradb::engine::AkkEngine;
    using namespace std::chrono_literals;
    const auto dir = makeTempDir("raft-empty-state-compaction");
    std::array<std::unique_ptr<AkkEngine>, 3> engines;
    for (size_t i = 0; i < 2; ++i) { engines[i] = AkkEngine::open(retainedHistorySnapshotOptions(dir, i + 1)); }
    AKK_CLUSTER_CHECK(waitUntil([&] {
        return engines[0]->stats().cluster.snapshotIndex != 0 && engines[1]->stats().cluster.snapshotIndex != 0;
    }, 10s));
    engines[2] = AkkEngine::open(retainedHistorySnapshotOptions(dir, 3));
    AKK_CLUSTER_CHECK(waitUntil([&] { return engines[2]->stats().cluster.snapshotIndex != 0; }, 10s));
    AKK_CLUSTER_CHECK(engines[2]->count() == 0 && engines[2]->stats().vlog.indexedEntries == 0);
    for (auto& engine : engines) { engine->close(); engine.reset(); }
    for (size_t i = 0; i < engines.size(); ++i) { engines[i] = AkkEngine::open(retainedHistorySnapshotOptions(dir, i + 1)); }
    AkkEngine* live[] = {engines[0].get(), engines[1].get(), engines[2].get()};
    runOnEngineLeader(live, [&](auto& leader) { leader.put(bytesOf("first-after-empty-snapshot"), bytesOf("value")); });
    auto* leader = findEngineLeader(live);
    AKK_CLUSTER_CHECK(leader && akk_test::collectHistory(leader->history(bytesOf("first-after-empty-snapshot"))).size() == 1);
    for (auto& engine : engines) { engine->close(); engine.reset(); }
    std::filesystem::remove_all(dir);
}

void testRaftRetainedHistorySnapshot(const std::filesystem::path& executable) {
    using akkaradb::engine::AkkEngine;
    using akkaradb::engine::AkkEngineOptions;
    using namespace std::chrono_literals;
    const auto dir = makeTempDir("raft-retained-history-snapshot");
    std::array<AkkEngineOptions, 3> options;
    std::array<std::unique_ptr<AkkEngine>, 3> engines;
    for (size_t i = 0; i < options.size(); ++i) { options[i] = retainedHistorySnapshotOptions(dir, i + 1); }
    engines[0] = AkkEngine::open(options[0]); engines[1] = AkkEngine::open(options[1]);
    AkkEngine* live[] = {engines[0].get(), engines[1].get()};
    const std::string keyText{"AKES1\0history\xff", 14};
    const auto key = bytesOf(keyText);
    const auto firstValue = deterministicBytes(257, 1234), secondValue = deterministicBytes(263, 5678);
    ClusterRequestId request;
    ClusterRequestResult receipt;
    runOnEngineLeader(live, [&](auto& leader) {
        request = leader.newRequestId(120000);
        receipt = leader.putWithRequest(request, key, firstValue);
    });
    auto* leader = findEngineLeader(live);
    AKK_CLUSTER_CHECK(leader != nullptr);
    std::fprintf(stderr, "[retention] history mutations\n");
    leader->put(key, secondValue); leader->remove(key);
    const auto initial = akk_test::collectHistory(leader->history(key));
    AKK_CLUSTER_CHECK(initial.size() == 3 && (initial.back().flags & akkaradb::core::MemHdr16::FLAG_TOMBSTONE) != 0);
    const auto rollback = leader->rollbackKey(key, {.streamId = leader->revisionStreamId(key), .seq = initial.front().seq},
        {.conflict = akkaradb::engine::RollbackConflictPolicy::OVERWRITE_LATEST});
    AKK_CLUSTER_CHECK(rollback.complete());
    const auto expected = akk_test::collectHistory(leader->history(key));
    AKK_CLUSTER_CHECK(expected.size() == 4 && (expected.back().flags & akkaradb::engine::vlog::VLOG_FLAG_ROLLBACK) != 0);
    // Repeat compaction, verifying reclamation on the caught-up follower too.
    for (unsigned cycle = 0; cycle < 3; ++cycle) {
        std::fprintf(stderr, "[retention] compaction cycle %u\n", cycle);
        for (unsigned i = 0; i < 8; ++i) {
            try { leader->put(bytesOf("overwritten"), bytesOf(std::to_string(cycle * 8 + i))); }
            catch (...) {
                for (auto* engine : live) {
                    const auto s = engine->stats().cluster;
                    std::fprintf(stderr, "[retention diagnostic] cycle=%u i=%u role=%u leader=%llu commit=%llu applied=%llu included=%llu last=%llu failure=%u\n",
                        cycle, i, s.role, s.leaderNodeId, s.commitIndex, s.appliedIndex, s.snapshotIndex, s.lastLogIndex, s.lastFailure);
                    for (const auto& peer : s.peers) {
                        std::fprintf(stderr, "[retention peer] id=%llu match=%llu next=%llu failures=%llu\n", peer.nodeId, peer.matchIndex,
                            peer.nextIndex, peer.consecutiveRoundTripFailures);
                    }
                }
                throw;
            }
        }
        AKK_CLUSTER_CHECK(waitUntil([&] {
            for (auto* engine : live) {
                const auto stats = engine->stats().cluster;
                if (stats.snapshotIndex == 0 || stats.snapshotIndex != stats.lastLogIndex) { return false; }
            }
            return true;
        }, 10s));
        for (unsigned i = 1; i <= 2; ++i) {
            const auto segments = dir / std::to_string(i) / "cluster-raft.log.segments";
            AKK_CLUSTER_CHECK(!std::filesystem::exists(segments) || std::filesystem::is_empty(segments));
        }
    }
    runStorageCrashChild(executable, "--retention-snapshot-receiver", "snapshot.finish.after_wal_records", dir);
    engines[2] = AkkEngine::open(options[2]);
    std::fprintf(stderr, "[retention] history snapshot join\n");
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { return akk_test::collectHistory(engines[2]->history(key)).size() == expected.size() && engineGet(*engines[2], "overwritten") == engineGet(*leader, "overwritten"); }
        catch (...) { return false; }
    }, 20s));
    AKK_CLUSTER_CHECK(engines[2]->stats().vlog.indexedEntries == leader->stats().vlog.indexedEntries);
    const auto verify = [&](AkkEngine& engine) {
        const auto actual = akk_test::collectHistory(engine.history(key));
        AKK_CLUSTER_CHECK(actual.size() == expected.size());
        for (size_t i = 0; i < actual.size(); ++i) {
            AKK_CLUSTER_CHECK(actual[i].seq == expected[i].seq && actual[i].sourceNodeId == expected[i].sourceNodeId &&
                actual[i].timestampNs == expected[i].timestampNs && actual[i].flags == expected[i].flags && actual[i].value == expected[i].value);
        }
        AKK_CLUSTER_CHECK(engine.getAt(key, expected.front().seq) == std::optional<std::vector<uint8_t>>{firstValue});
        AKK_CLUSTER_CHECK(!engine.getAt(key, expected[2].seq));
        const auto retried = engine.putWithRequest(request, key, firstValue);
        AKK_CLUSTER_CHECK(retried.sequence == receipt.sequence && retried.logIndex == receipt.logIndex);
        AKK_CLUSTER_CHECK(akk_test::collectHistory(engine.history(key)).size() == expected.size());
    };
    AkkEngine* all[] = {engines[0].get(), engines[1].get(), engines[2].get()};
    runOnEngineLeader(all, [&](auto& current) { current.transferClusterLeadership(3); });
    std::fprintf(stderr, "[retention] transfer to snapshot receiver\n");
    AKK_CLUSTER_CHECK(waitUntil([&] { return findEngineLeader(all) == engines[2].get(); }, 20s));
    verify(*engines[2]);
    for (auto& engine : engines) { engine->close(); engine.reset(); }
    for (size_t i = 0; i < engines.size(); ++i) { engines[i] = AkkEngine::open(options[i]); }
    std::fprintf(stderr, "[retention] restart\n");
    AKK_CLUSTER_CHECK(waitUntil([&] {
        for (const auto& engine : engines) { if (engine->stats().cluster.role == static_cast<uint32_t>(NodeRole::PRIMARY)) { return true; } }
        return false;
    }, 20s));
    AkkEngine* restarted[] = {engines[0].get(), engines[1].get(), engines[2].get()};
    runOnEngineLeader(restarted, verify);
    for (auto& engine : engines) { engine->close(); engine.reset(); }
    std::filesystem::remove_all(dir);
}

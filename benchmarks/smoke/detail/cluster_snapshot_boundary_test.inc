/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
// Independent wire fixtures exercise receiver reconciliation, including a
// durable install interrupted after the state machine has stored its snapshot.
std::vector<uint8_t> snapshotBoundaryMutation(uint64_t term, uint64_t index, uint8_t key, uint8_t value) {
    std::vector<uint8_t> entry;
    for (const uint64_t integer : {term, index, index}) { appendRaftTestInteger(entry, integer); }
    entry.insert(entry.end(), {0, static_cast<uint8_t>(ReplOpType::PUT), 0, 1});
    appendRaftTestInteger(entry, 2); appendRaftTestInteger(entry, 1);
    appendRaftTestInteger(entry, 1, 4); appendRaftTestInteger(entry, 1, 4);
    entry.insert(entry.end(), {0, key, value});
    return entry;
}

std::vector<uint8_t> snapshotBoundaryAppend(uint64_t term, uint64_t previousIndex, uint64_t previousTerm,
    uint64_t committed, const std::vector<std::vector<uint8_t>>& entries = {}) {
    std::vector<uint8_t> request;
    for (const uint64_t integer : {term, uint64_t{2}, previousIndex, previousTerm, committed}) { appendRaftTestInteger(request, integer); }
    appendRaftTestInteger(request, entries.size(), 4);
    for (const auto& entry : entries) {
        appendRaftTestInteger(request, entry.size(), 4); request.insert(request.end(), entry.begin(), entry.end());
    }
    return encodeFrame(ReplMsgType::RAFT_APPEND_ENTRIES, request);
}

struct SnapshotBoundaryOptions {
    uint64_t term = 3;
    bool done = true, includeEntry = true;
    uint8_t value = 'v';
};
std::vector<uint8_t> snapshotBoundaryMembers(const ClusterConfig& config) {
    std::vector<uint8_t> members;
    appendRaftTestInteger(members, config.nodes().size(), 4);
    for (const auto& member : config.nodes()) {
        appendRaftTestInteger(members, member.nodeId); appendRaftTestInteger(members, member.capabilities, 4);
        appendRaftTestInteger(members, member.host.size(), 4); appendRaftTestInteger(members, member.dataPort, 4);
        appendRaftTestInteger(members, member.replPort, 4);
        members.insert(members.end(), member.host.begin(), member.host.end());
        for (const auto& label : {member.hostId, member.siteId}) {
            appendRaftTestInteger(members, label.size(), 4); members.insert(members.end(), label.begin(), label.end());
        }
    }
    return members;
}
std::vector<uint8_t> snapshotBoundaryInstall(const ClusterConfig& config, uint64_t index, uint64_t boundaryTerm,
    SnapshotBoundaryOptions options = {}) {
    std::vector<uint8_t> request;
    for (const uint64_t integer : {options.term, uint64_t{2}, index, boundaryTerm, index, uint64_t{options.done}, uint64_t{0}}) {
        appendRaftTestInteger(request, integer);
    }
    const auto members = snapshotBoundaryMembers(config);
    appendRaftTestInteger(request, members.size(), 4); request.insert(request.end(), members.begin(), members.end());
    request.insert(request.end(), {0, 0}); // No joint membership.
    for (unsigned i = 0; i < 5; ++i) { appendRaftTestInteger(request, 0); } // Empty request state.
    appendRaftTestInteger(request, 0, 4); // No retained request results.
    request.push_back(static_cast<uint8_t>(options.done)); appendRaftTestInteger(request, options.includeEntry ? 1 : 0, 4);
    if (!options.includeEntry) { return encodeFrame(ReplMsgType::RAFT_INSTALL_SNAPSHOT, request); }
    for (const uint64_t integer : {uint64_t{0}, uint64_t{0}, uint64_t{1}}) { appendRaftTestInteger(request, integer); }
    const std::vector<uint8_t> value{options.value};
    appendRaftTestInteger(request, snapshotValueCrc(value), 4);
    appendRaftTestInteger(request, 1, 4); appendRaftTestInteger(request, 1, 4); request.insert(request.end(), {'k', options.value});
    return encodeFrame(ReplMsgType::RAFT_INSTALL_SNAPSHOT, request);
}

void testRaftSnapshotBoundary(unsigned boundary, bool interrupted) {
    using namespace akkaradb::engine;
    const auto dir = makeTempDir("raft-snapshot-boundary-" + std::to_string(boundary) + "-" + std::to_string(interrupted));
    const ClusterConfig config{{node(1, 34101, 34111), node(2, 34102, 34112), node(3, 34103, 34113)},
        ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 1000}};
    ClusterRuntimeOptions options;
    options.raftHeartbeatIntervalMs = 1000;
    options.raftSnapshot.minLogEntries = 1'000'000; options.raftSnapshot.minLogBytes = 1024ull * 1024 * 1024;
    const auto armed = std::make_shared<std::atomic<bool>>(false);
    auto runtime = makeRuntime(dir / "n1", config, 1, options, false, {}, [&](ClusterEngineCallbacks& callbacks) {
        if (!interrupted) { return; }
        callbacks.forceDurable = [force = std::move(callbacks.forceDurable), armed] {
            force();
            if (armed->exchange(false)) { throw std::runtime_error("snapshot boundary test: interrupted metadata finalization"); }
        };
    });
    runtime->runtime->start();
    RaftTestSocket peer; openRaftTestPeer(peer, 34111, 2, config, '3', options);
    AKK_CLUSTER_CHECK(peer.send(snapshotBoundaryAppend(2, 0, 0, 0,
        {snapshotBoundaryMutation(2, 1, 'b', '1'), snapshotBoundaryMutation(2, 2, 'c', '2')})));
    auto response = peer.receiveFrame();
    AKK_CLUSTER_CHECK(response.type == ReplMsgType::RAFT_APPEND_ENTRIES_RESPONSE && response.payload.at(8) == 1);
    AKK_CLUSTER_CHECK(runtime->runtime->raftStats().lastLogIndex == 2 && runtime->lastSeq.load() == 0);
    const uint64_t index = boundary == 2 ? 3 : 1, boundaryTerm = boundary == 0 ? 2 : 3;
    armed->store(interrupted);
    AKK_CLUSTER_CHECK(peer.send(snapshotBoundaryInstall(config, index, boundaryTerm)));
    response = peer.receiveFrame();
    AKK_CLUSTER_CHECK(response.type == ReplMsgType::RAFT_INSTALL_SNAPSHOT_RESPONSE && response.payload.at(8) == !interrupted);
    AKK_CLUSTER_CHECK(runtime->hasState(index, "k", "v"));
    if (interrupted) { AKK_CLUSTER_CHECK(runtime->runtime->raftStats().health == ClusterHealthState::FAILED); }
    peer.close(); runtime->runtime->close(); runtime.reset();

    runtime = makeRuntime(dir / "n1", config, 1, options, false, {}, [](ClusterEngineCallbacks& callbacks) {
        // This harness persisted the completed state-machine install before the
        // injected failure; recovery only needs to finalize its Raft metadata.
        callbacks.recoverSnapshot = [](uint64_t) {};
    });
    runtime->runtime->start();
    const uint64_t expectedLast = boundary == 0 ? 2 : index;
    AKK_CLUSTER_CHECK(runtime->runtime->raftStats().lastLogIndex == expectedLast);
    AKK_CLUSTER_CHECK(readRaftLogLastIncludedIndex(dir / "n1" / "cluster-raft.log") == index);
    AKK_CLUSTER_CHECK(runtime->runtime->raftStats().appliedIndex == index && runtime->hasState(index, "k", "v"));
    openRaftTestPeer(peer, 34111, 2, config, '3', options);
    if (boundary == 0) {
        // A matching suffix remains usable, rather than only present on disk.
        AKK_CLUSTER_CHECK(peer.send(snapshotBoundaryAppend(3, 2, 2, 2)));
        response = peer.receiveFrame();
        AKK_CLUSTER_CHECK(response.payload.at(8) == 1 && runtime->hasState(2, "c", "2"));
    } else {
        // The replaced prefix cannot be used to commit an old mutation.
        AKK_CLUSTER_CHECK(peer.send(snapshotBoundaryAppend(3, 2, 2, 2)));
        response = peer.receiveFrame(); AKK_CLUSTER_CHECK(response.payload.at(8) == 0);
        AKK_CLUSTER_CHECK(runtime->hasState(index, "k", "v"));
        AKK_CLUSTER_CHECK(peer.send(snapshotBoundaryAppend(3, index, boundaryTerm, index + 1,
            {snapshotBoundaryMutation(3, index + 1, 'n', '3')})));
        response = peer.receiveFrame();
        AKK_CLUSTER_CHECK(response.payload.at(8) == 1 && runtime->hasState(index + 1, "n", "3"));
    }
    peer.close(); runtime->runtime->close();
}

void testRaftSnapshotPreservesCommittedPrefix(bool streamed, bool equalBoundary) {
    const auto dir = makeTempDir("raft-snapshot-committed-" + std::to_string(streamed) + "-" + std::to_string(equalBoundary));
    const ClusterConfig config{{node(1, 34101, 34111), node(2, 34102, 34112), node(3, 34103, 34113)},
        ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 1000}};
    ClusterRuntimeOptions options; options.raftHeartbeatIntervalMs = 1000;
    options.raftSnapshot.minLogEntries = 1'000'000; options.raftSnapshot.minLogBytes = 1024ull * 1024 * 1024;
    auto runtime = makeRuntime(dir / "n1", config, 1, options);
    runtime->runtime->start();
    RaftTestSocket peer; openRaftTestPeer(peer, 34111, 2, config, '3', options);
    AKK_CLUSTER_CHECK(peer.send(snapshotBoundaryAppend(2, 0, 0, streamed ? 0 : 2,
        {snapshotBoundaryMutation(2, 1, 'k', '1'), snapshotBoundaryMutation(2, 2, 'k', '2')})));
    AKK_CLUSTER_CHECK(peer.receiveFrame().payload.at(8) == 1);
    const uint64_t boundary = equalBoundary ? 2 : 1;
    if (streamed) {
        AKK_CLUSTER_CHECK(peer.send(snapshotBoundaryInstall(config, boundary, 2, {.done = false, .value = '1'})));
        AKK_CLUSTER_CHECK(peer.receiveFrame().payload.at(8) == 1);
        AKK_CLUSTER_CHECK(peer.send(snapshotBoundaryAppend(3, 2, 2, 2)));
        AKK_CLUSTER_CHECK(peer.receiveFrame().payload.at(8) == 1 && runtime->hasState(2, "k", "2"));
    }
    AKK_CLUSTER_CHECK(peer.send(snapshotBoundaryInstall(config, boundary, 2, {.includeEntry = !streamed, .value = '1'})));
    const auto response = peer.receiveFrame();
    AKK_CLUSTER_CHECK(response.type == ReplMsgType::RAFT_INSTALL_SNAPSHOT_RESPONSE && response.payload.at(8) == 1);
    uint64_t acknowledged = 0;
    for (unsigned i = 0; i < 8; ++i) { acknowledged |= uint64_t{response.payload.at(9 + i)} << (8 * i); }
    AKK_CLUSTER_CHECK(acknowledged >= boundary && runtime->snapshotsInstalled == 0);
    AKK_CLUSTER_CHECK(runtime->runtime->raftStats().appliedIndex == 2 && runtime->hasState(2, "k", "2"));
    AKK_CLUSTER_CHECK(peer.send(snapshotBoundaryAppend(3, 2, 2, 2)));
    AKK_CLUSTER_CHECK(peer.receiveFrame().payload.at(8) == 1 && runtime->hasState(2, "k", "2"));
    AKK_CLUSTER_CHECK(peer.send(snapshotBoundaryInstall(config, 3, 3, {.done = false})));
    AKK_CLUSTER_CHECK(peer.receiveFrame().payload.at(8) == 1 && runtime->snapshotsInstalled == 0);
    peer.close(); runtime->runtime->close(); runtime.reset();
    runtime = makeRuntime(dir / "n1", config, 1, options); runtime->runtime->start();
    AKK_CLUSTER_CHECK(runtime->runtime->raftStats().appliedIndex == 2 && runtime->hasState(2, "k", "2"));
    // A superseded staged transfer must not prevent a later snapshot.
    openRaftTestPeer(peer, 34111, 2, config, '3', options);
    AKK_CLUSTER_CHECK(peer.send(snapshotBoundaryInstall(config, 3, 3)));
    AKK_CLUSTER_CHECK(peer.receiveFrame().payload.at(8) == 1 && runtime->hasState(3, "k", "v"));
    peer.close(); runtime->runtime->close();
}

void testRaftSnapshotAdmissionRace(bool changesTerm) {
    const auto dir = makeTempDir("raft-snapshot-admission-" + std::to_string(changesTerm));
    const ClusterConfig config{{node(1, 34101, 34111), node(2, 34102, 34112), node(3, 34103, 34113), node(4, 34104, 34114)},
        ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 1000}};
    ClusterRuntimeOptions options; options.raftHeartbeatIntervalMs = 1000;
    options.raftSnapshot.minLogEntries = 1'000'000; options.raftSnapshot.minLogBytes = 1024ull * 1024 * 1024;
    ParallelismGate gate;
    auto runtime = makeRuntime(dir / "n1", config, 1, options, false, {}, [&](ClusterEngineCallbacks& callbacks) {
        if (changesTerm) {
            callbacks.apply = [apply = std::move(callbacks.apply), &gate](auto seq, auto op, auto key, auto value, auto flags, auto source, auto timestamp) {
                if (seq == 2) { gate.pause(); }
                apply(seq, op, key, value, flags, source, timestamp);
            };
        } else { callbacks.prepareSnapshot = [&](uint64_t, uint64_t) { gate.pause(); }; }
    });
    runtime->runtime->start();
    RaftTestSocket peer; openRaftTestPeer(peer, 34111, 2, config, '3', options);
    AKK_CLUSTER_CHECK(peer.send(snapshotBoundaryAppend(2, 0, 0, 0,
        {snapshotBoundaryMutation(2, 1, 'k', '1'), snapshotBoundaryMutation(2, 2, 'k', '2')})));
    AKK_CLUSTER_CHECK(peer.receiveFrame().payload.at(8) == 1);
    RaftTestSocket other; openRaftTestPeer(other, 34111, 3, config, '3', options);
    auto competing = std::async(std::launch::async, [&] {
        if (changesTerm) {
            AKK_CLUSTER_CHECK(peer.send(snapshotBoundaryAppend(3, 2, 2, 2)));
        } else { AKK_CLUSTER_CHECK(peer.send(snapshotBoundaryInstall(config, 1, 2, {.value = '1'}))); }
        return peer.receiveFrame();
    });
    ReleaseParallelismGate release{gate};
    AKK_CLUSTER_CHECK(gate.await());
    auto snapshot = std::async(std::launch::async, [&] {
        if (changesTerm) { AKK_CLUSTER_CHECK(other.send(snapshotBoundaryInstall(config, 3, 3, {.term = 4}))); }
        else { AKK_CLUSTER_CHECK(other.send(snapshotBoundaryAppend(3, 2, 2, 2))); }
        return other.receiveFrame();
    });
    AKK_CLUSTER_CHECK(waitUntil([&] {
        const auto stats = runtime->runtime->raftStats();
        return changesTerm ? stats.currentTerm == 4 : stats.commitIndex == 2;
    }));
    if (changesTerm) {
        RaftTestSocket voter; openRaftTestPeer(voter, 34111, 4, config, '3', options);
        const auto vote = probeRaftTestPeer(voter, 5, 4, 2, 2, ReplMsgType::RAFT_REQUEST_VOTE);
        AKK_CLUSTER_CHECK(vote.payload.at(8) == 1); voter.close();
    }
    gate.release();
    const auto first = competing.get(), second = snapshot.get();
    AKK_CLUSTER_CHECK(first.payload.at(8) == 1 && second.payload.at(8) == (changesTerm ? 0 : 1));
    AKK_CLUSTER_CHECK(runtime->snapshotsInstalled == 0 && runtime->runtime->raftStats().appliedIndex == 2 && runtime->hasState(2, "k", "2"));
    peer.close(); other.close(); runtime->runtime->close();
}

void testRaftSnapshotRecoveryReplaysMembership() {
    const auto dir = makeTempDir("raft-snapshot-membership-recovery");
    const ClusterConfig config{{node(1, 34101, 34111), node(2, 34102, 34112), node(3, 34103, 34113)},
        ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 1000}, onlineRaftMembership()};
    ClusterRuntimeOptions options; options.raftHeartbeatIntervalMs = 1000;
    options.raftSnapshot.minLogEntries = 1'000'000; options.raftSnapshot.minLogBytes = 1024ull * 1024 * 1024;
    std::vector<uint8_t> configuration;
    for (const uint64_t integer : {uint64_t{2}, uint64_t{2}, uint64_t{0}}) { appendRaftTestInteger(configuration, integer); }
    configuration.insert(configuration.end(), {2, static_cast<uint8_t>(ReplOpType::PUT), 0, 1}); // CONFIG_FINAL
    appendRaftTestInteger(configuration, 7); appendRaftTestInteger(configuration, 0);
    const auto members = snapshotBoundaryMembers(config);
    appendRaftTestInteger(configuration, 0, 4); appendRaftTestInteger(configuration, members.size(), 4);
    configuration.push_back(0); configuration.insert(configuration.end(), members.begin(), members.end());
    ParallelismGate gate;
    auto runtime = makeRuntime(dir / "n1", config, 1, options, false, {}, [&](ClusterEngineCallbacks& callbacks) {
        callbacks.forceDurable = [&] { gate.pause(); throw std::runtime_error("snapshot test: interrupted before metadata finalization"); };
    });
    runtime->runtime->start();
    RaftTestSocket peer; openRaftTestPeer(peer, 34111, 2, config, '3', options);
    AKK_CLUSTER_CHECK(peer.send(snapshotBoundaryAppend(2, 0, 0, 0,
        {snapshotBoundaryMutation(2, 1, 'k', '1'), configuration})));
    AKK_CLUSTER_CHECK(peer.receiveFrame().payload.at(8) == 1);
    auto installing = std::async(std::launch::async, [&] {
        AKK_CLUSTER_CHECK(peer.send(snapshotBoundaryInstall(config, 1, 2, {.value = '1'})));
        return peer.receiveFrame();
    });
    ReleaseParallelismGate release{gate};
    AKK_CLUSTER_CHECK(gate.await());
    // This suffix commits after the durable install intent. Its membership
    // must survive recovery even though it has no state-machine sequence.
    RaftTestSocket other; openRaftTestPeer(other, 34111, 3, config, '3', options);
    AKK_CLUSTER_CHECK(other.send(snapshotBoundaryAppend(3, 2, 2, 2)));
    AKK_CLUSTER_CHECK(waitUntil([&] { return runtime->runtime->raftStats().commitIndex == 2; }));
    gate.release();
    AKK_CLUSTER_CHECK(installing.get().payload.at(8) == 0 && runtime->hasState(1, "k", "1"));
    peer.close(); other.close(); runtime->runtime->close(); runtime.reset();
    runtime = makeRuntime(dir / "n1", config, 1, options, false, {}, [](ClusterEngineCallbacks& callbacks) {
        callbacks.recoverSnapshot = [](uint64_t) {};
    });
    runtime->runtime->start();
    const auto recovered = runtime->runtime->raftStats();
    AKK_CLUSTER_CHECK(recovered.configurationGeneration == 7 && recovered.appliedIndex == 2 && runtime->hasState(1, "k", "1"));
    runtime->runtime->close();
}

void testRaftSnapshotStagingSynchronization(bool fails) {
    const auto dir = makeTempDir("raft-snapshot-staging-sync-" + std::to_string(fails));
    const ClusterConfig config{{node(1, 34101, 34111), node(2, 34102, 34112), node(3, 34103, 34113)},
        ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 1000}};
    ClusterRuntimeOptions options; options.raftHeartbeatIntervalMs = 1000;
    options.raftSnapshot.minLogEntries = 1;
    ParallelismGate gate;
    std::atomic<bool> exportAllowed{false};
    auto runtime = makeRuntime(dir / "n1", config, 1, options, false, {}, [&](ClusterEngineCallbacks& callbacks) {
        callbacks.exportSnapshot = [exportSnapshot = std::move(callbacks.exportSnapshot), &exportAllowed]() -> std::optional<ClusterSnapshot> {
            return exportAllowed.load() ? exportSnapshot() : std::nullopt;
        };
        callbacks.beginSnapshot = [begin = std::move(callbacks.beginSnapshot), &gate, fails](uint64_t seq, uint64_t count) {
            gate.pause();
            if (fails) { throw std::runtime_error("snapshot staging test: begin failed"); }
            begin(seq, count);
        };
    });
    runtime->runtime->start();
    RaftTestSocket peer; openRaftTestPeer(peer, 34111, 2, config, '3', options);
    AKK_CLUSTER_CHECK(peer.send(snapshotBoundaryAppend(2, 0, 0, 1, {snapshotBoundaryMutation(2, 1, 'b', '1')})));
    AKK_CLUSTER_CHECK(peer.receiveFrame().payload.at(8) == 1 && runtime->hasState(1, "b", "1"));
    auto staged = std::async(std::launch::async, [&] {
        AKK_CLUSTER_CHECK(peer.send(snapshotBoundaryInstall(config, 3, 3, {.done = false})));
        return peer.receiveFrame();
    });
    ReleaseParallelismGate release{gate};
    AKK_CLUSTER_CHECK(gate.await());
    exportAllowed.store(true);
    // Storage staging must not hold the consensus lock needed by observers.
    auto observed = std::async(std::launch::async, [&] { return runtime->runtime->raftStats(); });
    const auto ready = observed.wait_for(std::chrono::seconds{1}) == std::future_status::ready;
    if (!ready) { gate.release(); }
    AKK_CLUSTER_CHECK(ready);
    AKK_CLUSTER_CHECK(observed.get().commitIndex == 1 && runtime->snapshotsExported == 0);
    gate.release();
    AKK_CLUSTER_CHECK(staged.get().payload.at(8) == !fails);
    if (fails) {
        // Clearing a failed transfer must permit local compaction again.
        AKK_CLUSTER_CHECK(waitUntil([&] { return runtime->runtime->raftStats().snapshotIndex == 1; }));
        AKK_CLUSTER_CHECK(runtime->snapshotsInstalled == 0 && runtime->hasState(1, "b", "1"));
    } else {
        // Between chunks application is unlocked, but compaction must still
        // respect the active transfer. Keep leader contact fresh while polling.
        const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds{1400};
        do {
            AKK_CLUSTER_CHECK(peer.send(snapshotBoundaryAppend(3, 1, 2, 1)));
            AKK_CLUSTER_CHECK(peer.receiveFrame().payload.at(8) == 1);
            const auto stats = runtime->runtime->raftStats();
            AKK_CLUSTER_CHECK(stats.snapshotIndex == 0 && stats.appliedIndex == 1 && runtime->snapshotsExported == 0);
            std::this_thread::sleep_for(std::chrono::milliseconds{50});
        } while (std::chrono::steady_clock::now() < until);
        AKK_CLUSTER_CHECK(peer.send(snapshotBoundaryInstall(config, 3, 3, {.includeEntry = false})));
        AKK_CLUSTER_CHECK(peer.receiveFrame().payload.at(8) == 1 && runtime->hasState(3, "k", "v"));
        AKK_CLUSTER_CHECK(peer.send(snapshotBoundaryAppend(3, 3, 3, 4, {snapshotBoundaryMutation(3, 4, 'n', '4')})));
        AKK_CLUSTER_CHECK(peer.receiveFrame().payload.at(8) == 1);
        // Successful completion must clear the same guard as the failure path.
        AKK_CLUSTER_CHECK(waitUntil([&] { return runtime->runtime->raftStats().snapshotIndex == 4; }));
        AKK_CLUSTER_CHECK(runtime->snapshotsInstalled == 1 && runtime->hasState(4, "n", "4"));
    }
    AKK_CLUSTER_CHECK(runtime->snapshotsExported > 0);
    peer.close(); runtime->runtime->close();
}

void runRaftSnapshotIntegrityTests() {
    for (bool streamed : {false, true}) {
        for (bool equal : {false, true}) { testRaftSnapshotPreservesCommittedPrefix(streamed, equal); }
    }
    testRaftSnapshotAdmissionRace(false); testRaftSnapshotAdmissionRace(true);
    testRaftSnapshotRecoveryReplaysMembership();
    testRaftSnapshotStagingSynchronization(false); testRaftSnapshotStagingSynchronization(true);
}

void runRaftSnapshotBoundaryTests() {
    for (bool interrupted : {false, true}) {
        for (unsigned boundary = 0; boundary < 3; ++boundary) {
            std::fprintf(stderr, "[snapshot-boundary] boundary=%u interrupted=%u\n", boundary, interrupted);
            testRaftSnapshotBoundary(boundary, interrupted);
        }
    }
}

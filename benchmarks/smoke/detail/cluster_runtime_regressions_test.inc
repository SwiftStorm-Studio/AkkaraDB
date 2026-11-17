/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */

std::vector<uint8_t> runtimeTestAppendPrefix(uint32_t count, uint64_t commit, uint64_t previous = 0) {
    std::vector<uint8_t> payload;
    for (auto value : {uint64_t{1}, uint64_t{2}, previous, previous ? uint64_t{1} : uint64_t{0}, commit}) { appendRaftTestInteger(payload, value); }
    appendRaftTestInteger(payload, count, 4);
    for (uint64_t index = previous + 1; index <= previous + count; ++index) {
        // Independent MUTATION fixture matching the persisted/wire entry contract.
        std::vector<uint8_t> entry;
        for (auto value : {uint64_t{1}, index, index}) { appendRaftTestInteger(entry, value); }
        entry.push_back(0);
        entry.push_back(static_cast<uint8_t>(ReplOpType::PUT));
        entry.push_back(0);
        entry.push_back(1);
        appendRaftTestInteger(entry, 2);
        appendRaftTestInteger(entry, 0);
        appendRaftTestInteger(entry, 1, 4);
        appendRaftTestInteger(entry, 1, 4);
        entry.push_back(0);
        entry.push_back('k');
        entry.push_back(static_cast<uint8_t>(index));
        appendRaftTestInteger(payload, entry.size(), 4);
        payload.insert(payload.end(), entry.begin(), entry.end());
    }
    return encodeFrame(ReplMsgType::RAFT_APPEND_ENTRIES, payload);
}

void testRaftCommitMonotonicity(bool applying) {
    const auto dir = makeTempDir(applying ? "raft-commit-applying" : "raft-commit-applied");
    const ClusterConfig config{
        {node(1, 28101, 28111), node(2, 28102, 28112), node(3, 28103, 28113)},
        ReplicationMode::MIRROR,
        {},
        ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 500}
    };
    ClusterRuntimeOptions options;
    std::promise<void> entered, release;
    auto enteredFuture = entered.get_future();
    auto released = release.get_future().share();
    auto follower = makeRuntime(
        dir / "n1",
        config,
        1,
        options,
        false,
        {},
        [&](auto& callbacks) {
            if (!applying) { return; }
            callbacks.apply = [apply = callbacks.apply, &entered, released](auto seq, auto op, auto key, auto value, auto flags, auto source, auto time) {
                if (seq == 1) {
                    entered.set_value();
                    released.wait();
                }
                apply(seq, op, key, value, flags, source, time);
            };
        }
    );
    struct ReleaseApplication {
        std::promise<void>& gate;

        ~ReleaseApplication() {
            try { gate.set_value(); }
            catch (const std::future_error&) {}
        }
    } unblock{release};
    follower->runtime->start();
    RaftTestSocket leader;
    openRaftTestPeer(leader, 28111, 2, config);
    AKK_CLUSTER_CHECK(leader.send(runtimeTestAppendPrefix(3, 3)));
    AKK_CLUSTER_CHECK(leader.receiveFrame().payload[8] == 1);
    if (applying) { AKK_CLUSTER_CHECK(enteredFuture.wait_for(std::chrono::seconds{2}) == std::future_status::ready); }
    else { AKK_CLUSTER_CHECK(waitUntil([&] { return follower->runtime->raftStats().appliedIndex == 3; })); }
    // A leader may resend an earlier prefix with a newer commit watermark.
    AKK_CLUSTER_CHECK(leader.send(runtimeTestAppendPrefix(1, 4)));
    AKK_CLUSTER_CHECK(leader.receiveFrame().payload[8] == 1);
    AKK_CLUSTER_CHECK(follower->runtime->raftStats().commitIndex == 3);
    AKK_CLUSTER_CHECK(leader.send(runtimeTestAppendPrefix(0, 4, 1)));
    AKK_CLUSTER_CHECK(leader.receiveFrame().payload[8] == 1);
    AKK_CLUSTER_CHECK(follower->runtime->raftStats().commitIndex == 3);
    release.set_value();
    AKK_CLUSTER_CHECK(waitUntil([&] { return follower->runtime->raftStats().appliedIndex == 3; }));
    AKK_CLUSTER_CHECK(follower->runtime->raftStats().health != akkaradb::engine::ClusterHealthState::FAILED);
    leader.shutdown();
    follower->runtime->close();
    follower.reset();
    follower = makeRuntime(dir / "n1", config, 1, options);
    AKK_CLUSTER_CHECK(follower->runtime->raftStats().commitIndex == 3);
    AKK_CLUSTER_CHECK(follower->runtime->raftStats().appliedIndex == 3);
}

void testMirrorPrimaryHostOverride() {
    const auto dir = makeTempDir("mirror-primary-host-override");
    auto first = node(1, 28401, 28411);
    first.host = "127.0.0.2";
    const ClusterConfig config{
        {first, node(2, 28402, 28412)},
        ReplicationMode::MIRROR,
        {.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::DURABLE},
        ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 1000}
    };
    ClusterRuntimeOptions options;
    options.startupRole = NodeStartupRole::PRIMARY;
    options.clusterGroupId = 9987;
    options.clusterGroupEpoch = 1;
    options.replBindHost = "127.0.0.1";
    auto primary = makeRuntime(dir / "n1", config, 1, options);
    primary->runtime->start();
    options.startupRole = NodeStartupRole::REPLICA;
    options.primaryHost = "127.0.0.1";
    options.primaryReplPort = 28411;
    auto replica = makeRuntime(dir / "n2", config, 2, options);
    replica->runtime->start();
    AKK_CLUSTER_CHECK(
        waitUntil(
            [&] {
                const auto stats = primary->runtime->raftStats();
                return std::ranges::any_of(stats.peers, [](const auto& peer) { return peer.connected; });
            }
        )
    );
    primary->setState(1, bytesOf("override"), bytesOf("replicated"));
    primary->runtime->shipEntry(1, ReplOpType::PUT, bytesOf("override"), bytesOf("replicated"), 0, 1);
    AKK_CLUSTER_CHECK(replica->hasState(1, "override", "replicated"));
    replica->runtime->close();
    primary->runtime->close();
}

std::optional<DecodedFrame> runtimeTestReceiveFrame(const RaftTestSocket& peer) {
    std::vector<uint8_t> wire(ReplFrameHeader::SIZE);
    if (!peer.receive(wire)) { return std::nullopt; }
    uint32_t length = 0;
    for (size_t i = 0; i < 4; ++i) { length |= uint32_t{wire[6 + i]} << (8 * i); }
    AKK_CLUSTER_CHECK(length <= ReplFrameHeader::MAX_RAFT_PAYLOAD_SIZE);
    wire.resize(ReplFrameHeader::SIZE + length);
    if (!peer.receive(std::span{wire}.subspan(ReplFrameHeader::SIZE))) { return std::nullopt; }
    DecodedFrame frame;
    AKK_CLUSTER_CHECK(decodeFrame(wire, frame));
    return frame;
}

void testRaftElectionIgnoresSlowMinority(bool probe, bool higherTerm = false) {
    using Clock = std::chrono::steady_clock;
    const auto dir = makeTempDir(higherTerm ? "raft-late-higher-vote" : probe ? "raft-slow-prevote" : "raft-slow-vote");
    const ClusterConfig config{
        {node(1, 28201, 28211), node(2, 28202, 28212), node(3, 28203, 28213)},
        ReplicationMode::MIRROR,
        {},
        ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 500}
    };
    RaftTestSocket fastListener, slowListener;
    fastListener.listen(28212);
    slowListener.listen(28213);
    std::atomic<bool> stopping{false};
    std::promise<void> slowEntered;
    auto slowReady = slowEntered.get_future().share();
    std::promise<Clock::time_point> fastGranted;
    auto grantedAt = fastGranted.get_future();
    std::promise<void> leaderObserved;
    auto leaderReady = leaderObserved.get_future().share();
    auto candidate = makeRuntime(dir / "n1", config, 1);
    auto fast = std::async(
        std::launch::async,
        [&] {
            RaftTestSocket peer{::accept(fastListener.handle, nullptr, nullptr)};
            if (peer.handle == RaftTestSocket::INVALID) { return; }
            const auto hello = runtimeTestReceiveFrame(peer);
            if (!hello) { return; }
            AKK_CLUSTER_CHECK(hello->type == ReplMsgType::RAFT_PEER_HELLO);
            AKK_CLUSTER_CHECK(peer.send(encodeFrame(ReplMsgType::RAFT_PEER_HELLO_RESPONSE, std::array<uint8_t, 1>{0})));
            bool first = true;
            while (const auto request = runtimeTestReceiveFrame(peer)) {
                std::vector<uint8_t> response;
                if (request->type == ReplMsgType::RAFT_PRE_VOTE || request->type == ReplMsgType::RAFT_REQUEST_VOTE) {
                    if (first) { AKK_CLUSTER_CHECK(slowReady.wait_for(std::chrono::seconds{5}) == std::future_status::ready); }
                    const bool preVote = request->type == ReplMsgType::RAFT_PRE_VOTE;
                    appendRaftTestInteger(response, preVote ? readU64Le(request->payload, 0) - 1 : readU64Le(request->payload, 0));
                    response.push_back(1);
                    if (!peer.send(encodeFrame(preVote ? ReplMsgType::RAFT_PRE_VOTE_RESPONSE : ReplMsgType::RAFT_REQUEST_VOTE_RESPONSE, response))) { break; }
                    if (first) {
                        first = false;
                        fastGranted.set_value(Clock::now());
                    }
                }
                else if (request->type == ReplMsgType::RAFT_APPEND_ENTRIES) {
                    appendRaftTestInteger(response, readU64Le(request->payload, 0));
                    response.push_back(1);
                    uint32_t count = 0;
                    for (size_t i = 0; i < 4; ++i) { count |= uint32_t{request->payload[40 + i]} << (8 * i); }
                    appendRaftTestInteger(response, readU64Le(request->payload, 16) + count);
                    appendRaftTestInteger(response, 0);
                    appendRaftTestInteger(response, 0);
                    if (!peer.send(encodeFrame(ReplMsgType::RAFT_APPEND_ENTRIES_RESPONSE, response))) { break; }
                }
                else { throw std::runtime_error("unexpected election test request"); }
            }
        }
    );
    auto slow = std::async(
        std::launch::async,
        [&] {
            RaftTestSocket peer{::accept(slowListener.handle, nullptr, nullptr)};
            if (peer.handle == RaftTestSocket::INVALID) { return; }
            if (!runtimeTestReceiveFrame(peer)) { return; }
            AKK_CLUSTER_CHECK(peer.send(encodeFrame(ReplMsgType::RAFT_PEER_HELLO_RESPONSE, std::array<uint8_t, 1>{0})));
            const auto request = runtimeTestReceiveFrame(peer);
            if (!request) { return; }
            AKK_CLUSTER_CHECK(request->type == (probe ? ReplMsgType::RAFT_PRE_VOTE : ReplMsgType::RAFT_REQUEST_VOTE));
            slowEntered.set_value();
            std::vector<uint8_t> response;
            appendRaftTestInteger(response, higherTerm ? 10 : probe ? 0 : readU64Le(request->payload, 0));
            response.push_back(higherTerm ? 0 : 1);
            const auto wire = encodeFrame(probe ? ReplMsgType::RAFT_PRE_VOTE_RESPONSE : ReplMsgType::RAFT_REQUEST_VOTE_RESPONSE, response);
            if (higherTerm) {
                if (leaderReady.wait_for(std::chrono::seconds{2}) == std::future_status::ready && !stopping) { (void)peer.send(wire); }
                return;
            }
            for (size_t i = 0; i < wire.size() && !stopping; ++i) {
                if (!peer.send(std::span{wire}.subspan(i, 1))) { break; }
                std::this_thread::sleep_for(std::chrono::milliseconds{100});
            }
        }
    );
    const auto cleanup = [&] {
        stopping = true;
        candidate->runtime->close();
        try { leaderObserved.set_value(); }
        catch (const std::future_error&) {}
        fastListener.shutdown();
        slowListener.shutdown();
        fastListener.close();
        slowListener.close();
        fast.wait();
        slow.wait();
    };
    try {
        candidate->runtime->start();
        if (!probe) { candidate->runtime->campaignLeadership(); }
        AKK_CLUSTER_CHECK(grantedAt.wait_for(std::chrono::seconds{10}) == std::future_status::ready);
        const auto grantTime = grantedAt.get();
        AKK_CLUSTER_CHECK(waitUntil([&] { return candidate->runtime->role() == NodeRole::PRIMARY; }, std::chrono::milliseconds{350}));
        AKK_CLUSTER_CHECK(Clock::now() - grantTime < std::chrono::milliseconds{450});
        if (higherTerm) {
            leaderObserved.set_value();
            AKK_CLUSTER_CHECK(waitUntil([&] { return candidate->runtime->raftStats().currentTerm == 10 && candidate->runtime->role() == NodeRole::REPLICA; }));
        }
        else {
            // The minority RPC must also stop at its absolute deadline,
            // even while response bytes keep arriving within SO_RCVTIMEO.
            AKK_CLUSTER_CHECK(
                waitUntil(
                    [&] {
                        const auto peers = candidate->runtime->raftStats().peers;
                        const auto slowPeer = std::ranges::find(peers, uint64_t{3}, &RaftPeerStats::nodeId);
                        return slowPeer != peers.end() && slowPeer->roundTripsFailed != 0;
                    },
                    std::chrono::milliseconds{1200}
                )
            );
        }
    }
    catch (...) {
        cleanup();
        throw;
    }
    cleanup();
    fast.get();
    slow.get();
}

void runClusterRuntimeRegressionTests() {
    testRaftCommitMonotonicity(false);
    testRaftCommitMonotonicity(true);
    testMirrorPrimaryHostOverride();
    testRaftElectionIgnoresSlowMinority(true);
    testRaftElectionIgnoresSlowMinority(false);
    testRaftElectionIgnoresSlowMinority(true, true);
}

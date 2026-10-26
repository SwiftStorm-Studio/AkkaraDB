/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */


// Included inside RaftConsensusRuntime::Impl.
std::shared_ptr<PeerWorker> peerWorker(uint64_t peerId) {
    std::lock_guard lock{workersMutex_};
    if (workersStopping_) { throw std::runtime_error("Raft transport is closing"); }
    auto& worker = workers_[peerId];
    if (!worker) {
        worker = std::make_shared<PeerWorker>();
        ++peerWorkersStarted_;
        worker->thread = std::thread(
            [this, peerId, worker] {
                for (;;) {
                    std::function<void()> task;
                    uint64_t target = 0, round = 0;
                    bool heartbeat = false;
                    {
                        std::unique_lock lock{worker->mutex};
                        worker->cv.wait(lock, [&] { return worker->stopping || !worker->tasks.empty() || worker->replicate; });
                        if (worker->stopping) { return; }
                        if (!worker->tasks.empty()) {
                            task = std::move(worker->tasks.front());
                            worker->tasks.pop_front();
                        }
                        else {
                            target = worker->target;
                            round = worker->readRound;
                            heartbeat = worker->heartbeat;
                            worker->replicate = worker->heartbeat = false;
                        }
                    }
                    try {
                        if (task) { task(); }
                        else {
                            (void)replicatePeerTo(peerId, target, heartbeat, round);
                            advanceLeaderCommit();
                        }
                    }
                    catch (...) {
                        std::lock_guard lock{mutex_};
                        stopRuntimeLocked();
                    }
                }
            }
        );
    }
    return worker;
}

void scheduleReplication(const NodeInfo& peer, uint64_t target, bool heartbeat, uint64_t round = 0) {
    auto worker = peerWorker(peer.nodeId);
    std::lock_guard lock{worker->mutex};
    worker->target = target;
    worker->readRound = std::max(worker->readRound, round);
    worker->heartbeat = worker->heartbeat || heartbeat;
    worker->replicate = true;
    worker->cv.notify_one();
}

std::future<void> schedulePeerTask(uint64_t peerId, std::function<void()> fn) {
    auto task = std::make_shared<std::packaged_task<void()>>(std::move(fn));
    auto result = task->get_future();
    auto worker = peerWorker(peerId);
    std::lock_guard lock{worker->mutex};
    if (worker->tasks.size() >= 16) { throw std::runtime_error("Raft peer work queue is full"); }
    worker->tasks.emplace_back([task] { (*task)(); });
    worker->cv.notify_one();
    return result;
}

void retirePeerWorkers() {
    std::vector<NodeInfo> peers;
    {
        std::lock_guard lock{mutex_};
        peers = peers_;
    }
    std::vector<std::shared_ptr<PeerWorker>> retired;
    {
        std::lock_guard lock{workersMutex_};
        std::erase_if(
            workers_,
            [&](const auto& item) {
                const auto& [peerId, worker] = item;
                if (containsNode(peers, peerId)) { return false; }
                retired.push_back(worker);
                return true;
            }
        );
    }
    for (auto& worker : retired) {
        {
            std::lock_guard lock{worker->mutex};
            worker->stopping = true;
            worker->tasks.clear();
            worker->cv.notify_all();
        }
        {
            std::lock_guard lock{worker->socketMutex};
            shutdownSocket(worker->socket);
        }
        if (worker->thread.joinable()) { worker->thread.join(); }
    }
}

bool exchangeRpc(
    const NodeInfo& peer,
    const std::vector<uint8_t>& wire,
    ReplMsgType expected,
    DecodedFrame& frame,
    int timeoutMs,
    bool* submitted = nullptr,
    ForwardRequest* forwardRequest = nullptr
) {
    const auto deadline = detail::ReconfigurationDeadline::cap(Clock::now() + std::chrono::milliseconds(timeoutMs));
    auto worker = forwardRequest ? std::make_shared<PeerWorker>() : peerWorker(peer.nodeId);
    struct ForwardChannelGuard {
        Impl* owner = nullptr;
        std::shared_ptr<PeerWorker> channel;
        bool placementRead = false;
        std::shared_ptr<void> nodeLease;

        ~ForwardChannelGuard() {
            if (!owner) { return; }
            std::lock_guard lock{owner->workersMutex_};
            auto& channels = placementRead ? owner->placementReadChannels_ : owner->forwardChannels_;
            std::erase_if(channels, [&](const auto& weak) { return weak.expired() || weak.lock() == channel; });
            owner->forwardChannelsCv_.notify_all();
        }
    } forwardGuard;
    if (forwardRequest) {
        // A long management call can trigger a callback which itself
        // reads the authority. Keep RPC channels separate from consensus
        // and from each other so those cycles do not share one socket lock.
        std::unique_lock lock{workersMutex_};
        // Placement/2PC reads and receipt commands are leaf RPCs used
        // by callbacks of an already forwarded data operation. Reserve one bounded channel
        // so saturated forwarding admission cannot deadlock that cycle.
        forwardGuard.placementRead = isReservedControlForward(*forwardRequest, runtimeOptions_.raftGroupKind);
        auto& channels = forwardGuard.placementRead ? placementReadChannels_ : forwardChannels_;
        const size_t limit = forwardGuard.placementRead ? 1 : runtimeOptions_.raftMaxForwardRequests;
        if (!forwardChannelsCv_.wait_until(lock, deadline, [&] { return workersStopping_ || channels.size() < limit; }) || workersStopping_) {
            return false;
        }
        channels.push_back(worker);
        forwardGuard.owner = this;
        forwardGuard.channel = worker;
        lock.unlock();
        {
            const auto quota = forwardGuard.placementRead
                                   ? runtimeOptions_.nodeResourceBudget->outgoingControl
                                   : runtimeOptions_.nodeResourceBudget->outgoingForward;
            forwardGuard.nodeLease = quota->waitReserve(
                1,
                deadline,
                [this] {
                    std::lock_guard stoppingLock{workersMutex_};
                    return workersStopping_;
                }
            );
            if (!forwardGuard.nodeLease) { return false; }
        }
    }
    std::unique_lock ioLock{worker->ioMutex, std::defer_lock};
    if (!ioLock.try_lock_until(deadline)) { return false; }
    std::unique_ptr<detail::ForwardDeadline> forwardDeadline;
    if (submitted || detail::ReconfigurationDeadline::current().deadline != Clock::time_point::max()) {
        forwardDeadline = std::make_unique<detail::ForwardDeadline>(
            deadline,
            [worker] {
                std::lock_guard lock{worker->socketMutex};
                shutdownSocket(worker->socket);
            }
        );
    }
    const auto reset = [&] {
        std::lock_guard socketLock{worker->socketMutex};
        closeSocket(worker->socket);
        worker->socket = BAD_SOCKET;
        worker->secure.reset();
    };
    if (wire.empty()) { return false; }
    const auto channelKind = !forwardRequest
                                 ? RaftChannelKind::CONSENSUS
                                 : forwardGuard.placementRead
                                 ? RaftChannelKind::PLACEMENT_READ
                                 : RaftChannelKind::FORWARD;
    const auto& channelBudget = channelKind == RaftChannelKind::CONSENSUS
                                    ? receiveBudget_
                                    : channelKind == RaftChannelKind::FORWARD
                                    ? forwardReceiveBudget_
                                    : controlReceiveBudget_;
    PeerRoundTripObservation observation{worker};
    if (worker->endpoint.host != peer.host || worker->endpoint.replPort != peer.replPort) { reset(); }
    if (!socketOk(worker->socket)) {
        auto socket = detail::connectWithDeadline(peer.host, peer.replPort, deadline, [worker] {
            std::lock_guard lock{worker->mutex};
            return worker->stopping;
        });
        if (!socketOk(socket)) { return false; }
        ++outboundConnections_;
        {
            std::lock_guard socketLock{worker->socketMutex};
            std::lock_guard workLock{worker->mutex};
            if (worker->stopping || Clock::now() >= deadline) {
                closeSocket(socket);
                return false;
            }
            worker->socket = socket;
            worker->endpoint = peer;
        }
        const auto handshakeBudget = std::chrono::ceil<std::chrono::milliseconds>(deadline - Clock::now()).count();
        if (handshakeBudget <= 0) { reset(); return false; }
        setTimeouts(socket, static_cast<int>(handshakeBudget));
        if (runtimeOptions_.transportMode == TransportMode::SECURE) {
            worker->secure = openSecureSession(socket, peer.nodeId);
            if (!worker->secure) {
                reset();
                return false;
            }
        }
        const RaftPeerHello hello{
            .nodeId = selfNodeId_,
            .clusterId = config_.clusterId(),
            .compatibilityFingerprint = compatibilityFingerprint_,
            .blobPolicy = runtimeOptions_.raftBlobPolicy,
            .requestsEnabled = runtimeOptions_.requests.enabled,
            .requestMaxRetentionMs = runtimeOptions_.requests.maxRetentionMs,
            .requestCapacity = runtimeOptions_.requests.maxPendingRequests,
            .groupIdentity = groupIdentity_,
            .channelKind = channelKind,
        };
        DecodedFrame helloFrame;
        RaftPeerHelloResponse helloResponse;
        if (!sendFrame(socket, worker->secure.get(), encodeRaftPeerHello(hello)) || !
            recvFrame(socket, worker->secure.get(), helloFrame, handshakeReceiveBudget_, 1) || helloFrame.type !=
            ReplMsgType::RAFT_PEER_HELLO_RESPONSE || !decodeRaftPeerHelloResponse(helloFrame.payload, helloResponse) || helloResponse.status !=
            RaftPeerHelloStatus::ACCEPTED) {
            if (helloResponse.status == RaftPeerHelloStatus::CLUSTER_MISMATCH) {
                ++foreignClusterRejects_;
                recordFailure(ClusterFailureCode::FOREIGN_CLUSTER, ClusterHealthState::DEGRADED);
            }
            else if (helloResponse.status == RaftPeerHelloStatus::POLICY_MISMATCH) {
                ++peerPolicyMismatchRejects_;
                recordFailure(ClusterFailureCode::POLICY_MISMATCH, ClusterHealthState::DEGRADED);
            }
            reset();
            return false;
        }
    }
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
    if (remaining <= 0) { return false; }
    std::vector<uint8_t> forwardWire;
    if (forwardRequest) {
        // Admission and connection setup consume the caller's
        // budget, not a second full budget at the destination.
        forwardRequest->timeoutMs = static_cast<uint32_t>(remaining);
        forwardWire = encodeForwardRequest(*forwardRequest);
        if (forwardWire.empty() || Clock::now() >= deadline) { return false; }
    }
    setTimeouts(worker->socket, static_cast<int>(remaining));
    if (submitted) { *submitted = true; }
    if (!sendFrame(worker->socket, worker->secure.get(), forwardRequest ? forwardWire : wire) || !recvFrame(
        worker->socket,
        worker->secure.get(),
        frame,
        channelBudget
    ) || frame.type != expected || Clock::now() >= deadline) {
        reset();
        return false;
    }
    worker->lastSuccessfulContactAtUs.store(observationNowUs(), std::memory_order_relaxed);
    observation.succeeded = true;
    return true;
}

bool requestVote(const NodeInfo& peer, const RequestVote& request, RequestVoteResponse& response) {
    DecodedFrame frame;
    return exchangeRpc(peer, encodeRequestVote(request), ReplMsgType::RAFT_REQUEST_VOTE_RESPONSE, frame, rpcTimeoutMs(500)) &&
        decodeRequestVoteResponse(frame.payload, response);
}

bool preVote(const NodeInfo& peer, const RequestVote& request, RequestVoteResponse& response) {
    DecodedFrame frame;
    return exchangeRpc(
        peer,
        encodeRequestVote(request, ReplMsgType::RAFT_PRE_VOTE),
        ReplMsgType::RAFT_PRE_VOTE_RESPONSE,
        frame,
        rpcTimeoutMs(500)
    ) && decodeRequestVoteResponse(frame.payload, response);
}

bool appendEntries(const NodeInfo& peer, const AppendEntries& request, AppendEntriesResponse& response) {
    DecodedFrame frame;
    return exchangeRpc(peer, encodeAppendEntries(request), ReplMsgType::RAFT_APPEND_ENTRIES_RESPONSE, frame, rpcTimeoutMs(1000)) &&
        decodeAppendEntriesResponse(frame.payload, response);
}

bool installSnapshot(const NodeInfo& peer, const InstallSnapshot& request, InstallSnapshotResponse& response) {
    DecodedFrame frame;
    return exchangeRpc(peer, encodeInstallSnapshot(request), ReplMsgType::RAFT_INSTALL_SNAPSHOT_RESPONSE, frame, rpcTimeoutMs(2000)) &&
        decodeInstallSnapshotResponse(frame.payload, response);
}

bool timeoutNow(const NodeInfo& peer, const TimeoutNow& request, TimeoutNowResponse& response) {
    DecodedFrame frame;
    return exchangeRpc(peer, encodeTimeoutNow(request), ReplMsgType::RAFT_TIMEOUT_NOW_RESPONSE, frame, rpcTimeoutMs(500)) &&
        decodeTimeoutNowResponse(frame.payload, response);
}

std::unique_ptr<crypto::SecureSession> openSecureSession(SocketHandle socket, uint64_t peerNodeId) {
    try {
        crypto::NoiseInitiator initiator{localIdentity_};
        if (!writeSecureClientHello(socket, initiator.hello())) { return nullptr; }
        crypto::ServerHello serverHello{};
        if (!readSecureServerHello(socket, serverHello)) { return nullptr; }
        const auto expected = pinnedPeerKey(runtimeOptions_, peerNodeId);
        if (!expected) { return nullptr; }
        auto session = initiator.finish(serverHello, expected);
        return std::make_unique<crypto::SecureSession>(std::move(session));
    }
    catch (...) { return nullptr; }
}

std::unique_ptr<crypto::SecureSession> acceptSecureSession(SocketHandle socket, crypto::PublicKey& remotePublicKey) {
    try {
        crypto::ClientHello hello{};
        if (!readSecureClientHello(socket, hello)) { return nullptr; }
        auto accepted = crypto::acceptResponder(localIdentity_, hello);
        remotePublicKey = accepted.remoteStaticPublicKey;
        if (!writeSecureServerHello(socket, accepted.hello)) { return nullptr; }
        return std::make_unique<crypto::SecureSession>(std::move(accepted.session));
    }
    catch (...) { return nullptr; }
}

bool verifySecurePeer(uint64_t nodeId, const crypto::PublicKey& remotePublicKey) const {
    if (runtimeOptions_.transportMode != TransportMode::SECURE) { return true; }
    const auto expected = pinnedPeerKey(runtimeOptions_, nodeId);
    return expected && remotePublicKey == *expected;
}


/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */


// Included inside RaftConsensusRuntime::Impl.
void acceptLoop(SocketHandle listenSock) {
    while (true) {
        {
            std::lock_guard lock{mutex_};
            if (!running_) { return; }
        }
        SocketHandle client = ::accept(listenSock, nullptr, nullptr);
        if (!socketOk(client)) {
            if (acceptInterrupted()) { continue; }
            std::lock_guard lock{mutex_};
            if (!running_) { return; }
            return;
        }
        configureNoSigPipe(client);
        setTimeouts(client, 1000);
        auto handshakeLease = runtimeOptions_.nodeResourceBudget->handshakes->tryReserve(1);
        if (!handshakeLease) {
            closeSocket(client);
            continue;
        }
        const auto pending = pendingClientHandlers_.fetch_add(1, std::memory_order_acq_rel);
        if (pending >= MAX_RAFT_PENDING_HANDSHAKES) {
            pendingClientHandlers_.fetch_sub(1, std::memory_order_acq_rel);
            closeSocket(client);
            continue;
        }
        activeClientHandlers_.fetch_add(1, std::memory_order_acq_rel);
        {
            std::lock_guard lock{clientHandlersMutex_};
            clientSockets_.insert(client);
        }
        try {
            std::thread(
                [this, client, handshakeLease = std::move(handshakeLease)]() mutable {
                    {
                        IncomingChannel channel{*this};
                        channel.handshakeLease = std::move(handshakeLease);
                        try { handleClient(client, channel); }
                        catch (...) {}
                    }
                    std::lock_guard lock{clientHandlersMutex_};
                    clientSockets_.erase(client);
                    closeSocket(client);
                    activeClientHandlers_.fetch_sub(1, std::memory_order_acq_rel);
                    clientHandlersCv_.notify_all();
                }
            ).detach();
        }
        catch (...) {
            std::lock_guard lock{clientHandlersMutex_};
            clientSockets_.erase(client);
            closeSocket(client);
            activeClientHandlers_.fetch_sub(1, std::memory_order_acq_rel);
            pendingClientHandlers_.fetch_sub(1, std::memory_order_acq_rel);
            clientHandlersCv_.notify_all();
            return;
        }
    }
}

void handleClient(SocketHandle client, IncomingChannel& channel) {
    std::unique_ptr<crypto::SecureSession> secure;
    crypto::PublicKey remotePublicKey{};
    if (runtimeOptions_.transportMode == TransportMode::SECURE) {
        secure = acceptSecureSession(client, remotePublicKey);
        if (!secure) { return; }
    }
    DecodedFrame helloFrame;
    RaftPeerHello hello;
    RaftPeerHelloResponse helloResponse;
    if (recvFrame(client, secure.get(), helloFrame, handshakeReceiveBudget_, 107) && helloFrame.type == ReplMsgType::RAFT_PEER_HELLO &&
        decodeRaftPeerHello(helloFrame.payload, hello)) {
        bool knownNode = false;
        {
            std::lock_guard lock{mutex_};
            knownNode = hello.nodeId != selfNodeId_ && (isReplicationMemberLocked(hello.nodeId) ||
                (runtimeOptions_.raftGroupKind == RaftGroupKind::REQUEST_AUTHORITY && hello.channelKind != RaftChannelKind::CONSENSUS &&
                 std::ranges::contains(requestForwardClients_, hello.nodeId)));
        }
        if (!knownNode || !verifySecurePeer(hello.nodeId, remotePublicKey)) { helloResponse.status = RaftPeerHelloStatus::UNKNOWN_NODE; }
        else if (hello.clusterId != config_.clusterId() || hello.groupIdentity != groupIdentity_) {
            helloResponse.status = RaftPeerHelloStatus::CLUSTER_MISMATCH;
            ++foreignClusterRejects_;
            recordFailure(ClusterFailureCode::FOREIGN_CLUSTER, ClusterHealthState::DEGRADED);
        }
        else if (hello.compatibilityFingerprint != compatibilityFingerprint_ || hello.blobPolicy != runtimeOptions_.raftBlobPolicy || hello.
            requestsEnabled != runtimeOptions_.requests.enabled || hello.requestMaxRetentionMs != runtimeOptions_.requests.maxRetentionMs || hello.
            requestCapacity != runtimeOptions_.requests.maxPendingRequests) {
            helloResponse.status = RaftPeerHelloStatus::POLICY_MISMATCH;
            ++peerPolicyMismatchRejects_;
            recordFailure(ClusterFailureCode::POLICY_MISMATCH, ClusterHealthState::DEGRADED);
        }
        else if (channel.admit(hello)) { helloResponse.status = RaftPeerHelloStatus::ACCEPTED; }
    }
    if (!sendFrame(client, secure.get(), encodeRaftPeerHelloResponse(helloResponse)) || helloResponse.status != RaftPeerHelloStatus::ACCEPTED) {
        return;
    }
    helloFrame = {};
    const auto& channelBudget = hello.channelKind == RaftChannelKind::CONSENSUS
                                    ? receiveBudget_
                                    : hello.channelKind == RaftChannelKind::FORWARD
                                    ? forwardReceiveBudget_
                                    : controlReceiveBudget_;
    for (;;) {
        {
            std::lock_guard lock{mutex_};
            if (!running_) { return; }
        }
        #ifdef _WIN32
        fd_set readable;
        FD_ZERO(&readable);
        FD_SET(client, &readable);
        timeval timeout{0, 100000};
        const auto ready = ::select(0, &readable, nullptr, nullptr, &timeout);
        #else
        pollfd readable{client, POLLIN, 0}; const auto ready = ::poll(&readable, 1, 100);
        #endif
        if (ready == 0) { continue; }
        if (ready < 0) { return; }
        DecodedFrame frame;
        if (!recvFrame(client, secure.get(), frame, channelBudget)) { return; }
        if ((frame.type == ReplMsgType::FORWARD_REQUEST) == (hello.channelKind == RaftChannelKind::CONSENSUS)) { return; }
        if (frame.type == ReplMsgType::RAFT_PRE_VOTE) {
            RequestVote request;
            RequestVoteResponse response;
            if (decodeRequestVote(frame.payload, request) && request.candidateId == hello.nodeId && verifySecurePeer(
                request.candidateId,
                remotePublicKey
            )) { response = handlePreVote(request); }
            if (!sendFrame(client, secure.get(), encodeRequestVoteResponse(response, ReplMsgType::RAFT_PRE_VOTE_RESPONSE))) { return; }
        }
        else if (frame.type == ReplMsgType::RAFT_REQUEST_VOTE) {
            RequestVote request;
            RequestVoteResponse response;
            if (decodeRequestVote(frame.payload, request) && verifySecurePeer(request.candidateId, remotePublicKey)) {
                response = handleRequestVote(request);
            }
            if (!sendFrame(client, secure.get(), encodeRequestVoteResponse(response))) { return; }
        }
        else if (frame.type == ReplMsgType::RAFT_APPEND_ENTRIES) {
            AppendEntries request;
            AppendEntriesResponse response;
            if (decodeAppendEntries(frame.payload, request) && verifySecurePeer(request.leaderId, remotePublicKey)) {
                response = handleAppendEntries(request);
            }
            if (!sendFrame(client, secure.get(), encodeAppendEntriesResponse(response))) { return; }
        }
        else if (frame.type == ReplMsgType::RAFT_INSTALL_SNAPSHOT) {
            InstallSnapshot request;
            InstallSnapshotResponse response;
            if (decodeInstallSnapshot(frame.payload, request) && verifySecurePeer(request.leaderId, remotePublicKey)) {
                response = handleInstallSnapshot(request);
            }
            if (!sendFrame(client, secure.get(), encodeInstallSnapshotResponse(response))) { return; }
        }
        else if (frame.type == ReplMsgType::RAFT_TIMEOUT_NOW) {
            TimeoutNow request;
            TimeoutNowResponse response;
            if (decodeTimeoutNow(frame.payload, request) && verifySecurePeer(request.leaderId, remotePublicKey)) {
                response = handleTimeoutNow(request);
            }
            if (!sendFrame(client, secure.get(), encodeTimeoutNowResponse(response))) { return; }
        }
        else if (frame.type == ReplMsgType::FORWARD_REQUEST) {
            {
                std::lock_guard lock{mutex_};
                if (!isReplicationMemberLocked(hello.nodeId) &&
                    !(runtimeOptions_.raftGroupKind == RaftGroupKind::REQUEST_AUTHORITY &&
                      std::ranges::contains(requestForwardClients_, hello.nodeId))) { return; }
            }
            ForwardRequest request;
            auto requestReservation = channelBudget->reserve(forwardRequestMemoryBytes(frame.payload));
            if (!decodeForwardRequest(frame.payload, request)) { return; }
            const auto placementRead = isReservedControlForward(request, runtimeOptions_.raftGroupKind);
            if ((hello.channelKind == RaftChannelKind::PLACEMENT_READ) != placementRead) { return; }
            ForwardResponse response;
            response.errorCode = ClusterRoutingCode::FORWARD_UNAVAILABLE;
            if (callbacks_.forward) {
                try { response = callbacks_.forward(request); }
                catch (const ClusterRoutingError& error) {
                    response.errorCode = error.code;
                    response.target = error.target;
                    response.message = error.what();
                }
                catch (const std::exception& error) {
                    response.errorCode = forwardMayMutate(request.operation)
                                             ? ClusterRoutingCode::OUTCOME_UNKNOWN
                                             : ClusterRoutingCode::FORWARD_UNAVAILABLE;
                    response.message = error.what();
                }
                catch (...) {
                    response.errorCode = forwardMayMutate(request.operation)
                                             ? ClusterRoutingCode::OUTCOME_UNKNOWN
                                             : ClusterRoutingCode::FORWARD_UNAVAILABLE;
                }
            }
            response.requestId = request.requestId;
            response.message.resize(std::min<size_t>(response.message.size(), 4096));
            auto wire = encodeForwardResponse(response);
            if (wire.empty()) {
                response = {};
                response.requestId = request.requestId;
                response.errorCode = forwardMayMutate(request.operation)
                                         ? ClusterRoutingCode::OUTCOME_UNKNOWN
                                         : ClusterRoutingCode::PAYLOAD_TOO_LARGE;
                wire = encodeForwardResponse(response);
            }
            if (!sendFrame(client, secure.get(), wire)) { return; }
        }
        else { return; }
    }
}


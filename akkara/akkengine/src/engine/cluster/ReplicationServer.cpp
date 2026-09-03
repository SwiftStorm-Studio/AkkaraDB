/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/cluster/ReplicationServer.cpp
#include "akk/engine/cluster/ReplicationServer.hpp"
#include "akk/crypto/SecureChannel.hpp"

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_set>
#include <algorithm>
#include <vector>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#else
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace akkaradb::engine::cluster {
    namespace {
        constexpr int HANDSHAKE_TIMEOUT_MS = 5'000;
        #ifdef _WIN32
        using SocketHandle = SOCKET;
        constexpr SocketHandle BAD_SOCKET = INVALID_SOCKET;
        void shutdownSocket(SocketHandle s) noexcept { if (s != BAD_SOCKET) { ::shutdown(s, SD_BOTH); } }
        void closeSocket(SocketHandle s) noexcept { if (s != BAD_SOCKET) { ::closesocket(s); } }
        bool socketOk(SocketHandle s) noexcept { return s != INVALID_SOCKET; }

        void netInit() {
            static std::once_flag once;
            std::call_once(
                once,
                [] {
                    WSADATA wsa{};
                    ::WSAStartup(MAKEWORD(2, 2), &wsa);
                }
            );
        }
        #else
        using SocketHandle = int; constexpr SocketHandle BAD_SOCKET = -1; void
        shutdownSocket(SocketHandle s) noexcept { if (s >= 0) { ::shutdown(s, SHUT_RDWR); } } void closeSocket(SocketHandle s) noexcept {
            if (s >= 0) { ::close(s); }
        } bool socketOk(SocketHandle s) noexcept { return s >= 0; } void netInit() {}
        #endif

        void configureNoSigPipe(SocketHandle s) noexcept {
            #if !defined(_WIN32) && defined(SO_NOSIGPIPE)
            int enabled = 1; (void)::setsockopt(s, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled));
            #else
            (void)s;
            #endif
        }

        bool sendAll(SocketHandle s, const uint8_t* data, size_t size) {
            size_t sent = 0;
            while (sent < size) {
                #ifdef _WIN32
                const int rc = ::send(s, reinterpret_cast<const char*>(data + sent), static_cast<int>(size - sent), 0);
                if (rc < 0 && ::WSAGetLastError() == WSAEINTR) { continue; }
                #else
                int flags = 0;
                #ifdef MSG_NOSIGNAL
                flags |= MSG_NOSIGNAL;
                #endif
                const ssize_t rc = ::send(s, data + sent, size - sent, flags); if (rc < 0 && errno == EINTR) { continue; }
                #endif
                if (rc <= 0) { return false; }
                sent += static_cast<size_t>(rc);
            }
            return true;
        }

        bool recvAll(SocketHandle s, uint8_t* data, size_t size) {
            size_t got = 0;
            while (got < size) {
                #ifdef _WIN32
                const int rc = ::recv(s, reinterpret_cast<char*>(data + got), static_cast<int>(size - got), 0);
                if (rc < 0 && ::WSAGetLastError() == WSAEINTR) { continue; }
                #else
                const ssize_t rc = ::recv(s, data + got, size - got, 0); if (rc < 0 && errno == EINTR) { continue; }
                #endif
                if (rc <= 0) { return false; }
                got += static_cast<size_t>(rc);
            }
            return true;
        }

        void setSocketTimeouts(SocketHandle s, int timeoutMs) {
            #ifdef _WIN32
            const auto timeout = static_cast<DWORD>(timeoutMs);
            (void)::setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&timeout), sizeof(timeout));
            (void)::setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, reinterpret_cast<const char*>(&timeout), sizeof(timeout));
            #else
            timeval timeout{};
            timeout.tv_sec = timeoutMs / 1000;
            timeout.tv_usec = (timeoutMs % 1000) * 1000;
            (void)::setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
            (void)::setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
            #endif
        }

        uint32_t readU32(const uint8_t* b) noexcept {
            return static_cast<uint32_t>(b[0]) | (static_cast<uint32_t>(b[1]) << 8) | (static_cast<uint32_t>(b[2]) << 16) | (static_cast<
                uint32_t>(b[3]) << 24);
        }

        bool recvFrame(SocketHandle s, DecodedFrame& out) {
            uint8_t header[ReplFrameHeader::SIZE];
            if (!recvAll(s, header, sizeof(header))) { return false; }
            const uint32_t payloadLen = readU32(header + 6);
            if (payloadLen > ReplFrameHeader::MAX_PAYLOAD_SIZE) { return false; }
            std::vector<uint8_t> wire(sizeof(header) + payloadLen);
            std::memcpy(wire.data(), header, sizeof(header));
            if (payloadLen > 0 && !recvAll(s, wire.data() + sizeof(header), payloadLen)) { return false; }
            return decodeFrame(wire, out);
        }

        constexpr uint32_t SECURE_HELLO_MAGIC = 0x48434B41; // "AKCH"
        constexpr uint32_t SECURE_FRAME_MAGIC = 0x46434B41; // "AKCF"
        constexpr uint8_t SECURE_VERSION = 1;
        constexpr uint8_t SECURE_CLIENT_HELLO = 1;
        constexpr uint8_t SECURE_SERVER_HELLO = 2;
        constexpr size_t SECURE_HELLO_HEADER_SIZE = 6;
        constexpr size_t SECURE_CLIENT_HELLO_SIZE = SECURE_HELLO_HEADER_SIZE + 64;
        constexpr size_t SECURE_SERVER_HELLO_SIZE = SECURE_HELLO_HEADER_SIZE + 80;
        constexpr size_t SECURE_FRAME_HEADER_SIZE = 34;
        constexpr uint32_t SECURE_MAX_CIPHERTEXT_SIZE = ReplFrameHeader::SIZE + ReplFrameHeader::MAX_PAYLOAD_SIZE;

        void writeU32Le(uint8_t* out, uint32_t value) noexcept {
            for (size_t i = 0; i < 4; ++i) { out[i] = static_cast<uint8_t>(value >> (i * 8)); }
        }

        void writeU64Le(uint8_t* out, uint64_t value) noexcept {
            for (size_t i = 0; i < 8; ++i) { out[i] = static_cast<uint8_t>(value >> (i * 8)); }
        }

        uint32_t readU32Le(const uint8_t* in) noexcept {
            return static_cast<uint32_t>(in[0]) | (static_cast<uint32_t>(in[1]) << 8) | (static_cast<uint32_t>(in[2]) << 16) | (static_cast<
                uint32_t>(in[3]) << 24);
        }

        uint64_t readU64Le(const uint8_t* in) noexcept {
            uint64_t out = 0;
            for (size_t i = 0; i < 8; ++i) { out |= static_cast<uint64_t>(in[i]) << (i * 8); }
            return out;
        }

        crypto::NodeIdentity loadSecureIdentity(const ClusterRuntimeOptions& options) {
            if (options.secure.identitySeedPath.empty()) { return crypto::generateNodeIdentity(); }
            return crypto::IdentityStore{options.secure.identitySeedPath}.loadOrCreate();
        }

        std::optional<crypto::PublicKey> pinnedPeerKey(const ClusterRuntimeOptions& options, uint64_t nodeId) {
            if (nodeId == 0) { return std::nullopt; }
            for (const auto& pin : options.secure.pinnedPeers) { if (pin.nodeId == nodeId) { return pin.publicKey; } }
            return std::nullopt;
        }

        bool readSecureClientHello(SocketHandle socket, crypto::ClientHello& hello) {
            std::array<uint8_t, SECURE_CLIENT_HELLO_SIZE> wire{};
            if (!recvAll(socket, wire.data(), wire.size())) { return false; }
            if (readU32Le(wire.data()) != SECURE_HELLO_MAGIC || wire[4] != SECURE_VERSION || wire[5] != SECURE_CLIENT_HELLO) {
                return false;
            }
            std::memcpy(hello.staticPublicKey.data(), wire.data() + SECURE_HELLO_HEADER_SIZE, hello.staticPublicKey.size());
            std::memcpy(
                hello.ephemeralPublicKey.data(),
                wire.data() + SECURE_HELLO_HEADER_SIZE + hello.staticPublicKey.size(),
                hello.ephemeralPublicKey.size()
            );
            return true;
        }

        bool writeSecureServerHello(SocketHandle socket, const crypto::ServerHello& hello) {
            std::array<uint8_t, SECURE_SERVER_HELLO_SIZE> wire{};
            writeU32Le(wire.data(), SECURE_HELLO_MAGIC);
            wire[4] = SECURE_VERSION;
            wire[5] = SECURE_SERVER_HELLO;
            std::memcpy(wire.data() + SECURE_HELLO_HEADER_SIZE, hello.staticPublicKey.data(), hello.staticPublicKey.size());
            std::memcpy(
                wire.data() + SECURE_HELLO_HEADER_SIZE + hello.staticPublicKey.size(),
                hello.ephemeralPublicKey.data(),
                hello.ephemeralPublicKey.size()
            );
            std::memcpy(
                wire.data() + SECURE_HELLO_HEADER_SIZE + hello.staticPublicKey.size() + hello.ephemeralPublicKey.size(),
                hello.authenticator.data(),
                hello.authenticator.size()
            );
            return sendAll(socket, wire.data(), wire.size());
        }

        bool sendSecureFrame(SocketHandle socket, crypto::SecureSession& session, const uint8_t* data, size_t size) {
            if (size > SECURE_MAX_CIPHERTEXT_SIZE) { return false; }
            const auto encrypted = session.seal(std::span<const uint8_t>{data, size});
            if (encrypted.ciphertext.size() > SECURE_MAX_CIPHERTEXT_SIZE) { return false; }

            std::array<uint8_t, SECURE_FRAME_HEADER_SIZE> header{};
            writeU32Le(header.data(), SECURE_FRAME_MAGIC);
            header[4] = SECURE_VERSION;
            header[5] = 0;
            writeU64Le(header.data() + 6, encrypted.counter);
            writeU32Le(header.data() + 14, static_cast<uint32_t>(encrypted.ciphertext.size()));
            std::memcpy(header.data() + 18, encrypted.tag.data(), encrypted.tag.size());

            return sendAll(socket, header.data(), header.size()) && (encrypted.ciphertext.empty() || sendAll(
                socket,
                encrypted.ciphertext.data(),
                encrypted.ciphertext.size()
            ));
        }

        bool recvSecureFrame(SocketHandle socket, crypto::SecureSession& session, DecodedFrame& out) {
            std::array<uint8_t, SECURE_FRAME_HEADER_SIZE> header{};
            if (!recvAll(socket, header.data(), header.size())) { return false; }
            if (readU32Le(header.data()) != SECURE_FRAME_MAGIC || header[4] != SECURE_VERSION) { return false; }

            crypto::EncryptedFrame encrypted;
            encrypted.counter = readU64Le(header.data() + 6);
            const uint32_t ciphertextSize = readU32Le(header.data() + 14);
            if (ciphertextSize > SECURE_MAX_CIPHERTEXT_SIZE) { return false; }
            std::memcpy(encrypted.tag.data(), header.data() + 18, encrypted.tag.size());
            encrypted.ciphertext.resize(ciphertextSize);
            if (ciphertextSize > 0 && !recvAll(socket, encrypted.ciphertext.data(), ciphertextSize)) { return false; }

            std::vector<uint8_t> plaintext;
            if (!session.open(encrypted, plaintext)) { return false; }
            return decodeFrame(plaintext, out);
        }

        SocketHandle listenOn(const std::string& host, uint16_t port) {
            netInit();

            addrinfo hints{};
            hints.ai_family = AF_UNSPEC;
            hints.ai_socktype = SOCK_STREAM;
            hints.ai_flags = AI_PASSIVE;

            addrinfo* result = nullptr;
            const auto portText = std::to_string(port);
            const char* hostArg = host.empty() ? nullptr : host.c_str();
            if (::getaddrinfo(hostArg, portText.c_str(), &hints, &result) != 0) {
                throw std::runtime_error(
                    "ReplicationServer: getaddrinfo failed for " + (host.empty() ? std::string{"0.0.0.0"} : host) + ":" + portText
                );
            }

            SocketHandle out = BAD_SOCKET;
            for (addrinfo* it = result; it != nullptr; it = it->ai_next) {
                SocketHandle s = ::socket(it->ai_family, it->ai_socktype, it->ai_protocol);
                if (!socketOk(s)) { continue; }

                int reuse = 1;
                ::setsockopt(s, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&reuse), sizeof(reuse));
                if (::bind(s, it->ai_addr, static_cast<int>(it->ai_addrlen)) == 0 && ::listen(s, 16) == 0) {
                    out = s;
                    break;
                }
                closeSocket(s);
            }

            ::freeaddrinfo(result);
            if (!socketOk(out)) {
                throw std::runtime_error(
                    "ReplicationServer: bind/listen failed on " + (host.empty() ? std::string{"0.0.0.0"} : host) + ":" + portText
                );
            }
            return out;
        }

        struct BufferedWire {
            uint64_t seq = 0;
            std::vector<uint8_t> wire;
        };

        struct ReplicaState {
            SocketHandle sock = BAD_SOCKET;
            std::unique_ptr<crypto::SecureSession> secure;
            uint64_t nodeId = 0;
            std::atomic<uint64_t> lastAckedSeq{0};
            std::atomic<uint8_t> lastAckStage{static_cast<uint8_t>(AckStage::DURABLE)};
            std::atomic<bool> dead{false};
            std::atomic<bool> shutdownStarted{false};
            std::mutex queueMutex;
            std::condition_variable queueCv;
            std::deque<std::vector<uint8_t>> queue;
            uint64_t queuedBytes = 0;
            std::mutex sendMutex;
            std::thread sendThread;
            std::thread recvThread;

            ~ReplicaState() { closeSocket(sock); }
        };
    } // namespace

    class ReplicationServer::Impl {
        public:
            uint16_t replPort = 0;
            uint64_t selfNodeId = 0;
            std::function<uint64_t()> getCurrentSeq;
            HistoryProvider historyProvider;
            SnapshotProvider snapshotProvider;
            ReadCallback readCallback;
            AckPolicy ackPolicy;
            ConsistencyOptions consistency;
            uint16_t configuredReplicaCount = 0;
            ClusterRuntimeOptions runtimeOptions;
            std::vector<uint64_t> configuredReplicaNodeIds;
            crypto::NodeIdentity localIdentity;

            SocketHandle listenSock = BAD_SOCKET;
            std::atomic<bool> running{false};
            std::thread acceptThread;
            std::atomic<bool> reaperStopping{false};
            std::thread reaperThread;
            std::mutex reaperMutex;
            std::condition_variable reaperCv;
            std::mutex handshakeSocketsMutex;
            std::unordered_set<SocketHandle> handshakeSockets;

            mutable std::mutex replicasMutex;
            std::vector<std::shared_ptr<ReplicaState>> replicas;
            std::deque<BufferedWire> entryBuffer;

            std::mutex ackMutex;
            std::condition_variable ackCv;
            std::unordered_set<uint64_t> lagBlockedReplicas;

            bool queueHasCapacityLocked(const ReplicaState& replica, size_t wireSize) const noexcept {
                if (runtimeOptions.maxReplicaQueueFrames != 0 && replica.queue.size() >= runtimeOptions.maxReplicaQueueFrames) { return false; }
                if (runtimeOptions.maxReplicaQueueBytes != 0) {
                    const uint64_t bytes = static_cast<uint64_t>(wireSize);
                    if (bytes > runtimeOptions.maxReplicaQueueBytes || replica.queuedBytes > runtimeOptions.maxReplicaQueueBytes - bytes) { return false; }
                }
                return true;
            }

            bool queueInitialWire(const std::shared_ptr<ReplicaState>& replica, std::vector<uint8_t> wire) {
                if (wire.empty()) { return false; }
                if (!queueHasCapacityLocked(*replica, wire.size())) { return false; }
                replica->queuedBytes += static_cast<uint64_t>(wire.size());
                replica->queue.push_back(std::move(wire));
                return true;
            }

            bool queueLiveWireLocked(const std::shared_ptr<ReplicaState>& replica, const std::vector<uint8_t>& wire) {
                if (wire.empty()) { return false; }
                std::lock_guard qlock{replica->queueMutex};
                if (!queueHasCapacityLocked(*replica, wire.size())) {
                    requestReplicaStop(replica);
                    if (consistency.replicaLagAction == ReplicaLagAction::BLOCK_WRITES) { lagBlockedReplicas.insert(replica->nodeId); }
                    return false;
                }
                replica->queuedBytes += static_cast<uint64_t>(wire.size());
                replica->queue.push_back(wire);
                replica->queueCv.notify_one();
                return true;
            }

            void start() {
                listenSock = listenOn(runtimeOptions.replBindHost, replPort);
                running.store(true);
                reaperStopping.store(false);
                reaperThread = std::thread([this] { reapLoop(); });
                acceptThread = std::thread([this] { acceptLoop(); });
            }

            void close() {
                running.store(false);
                shutdownSocket(listenSock);
                closeSocket(listenSock);
                listenSock = BAD_SOCKET;
                {
                    std::lock_guard lock{handshakeSocketsMutex};
                    for (const auto socket : handshakeSockets) {
                        shutdownSocket(socket);
                    }
                }
                if (acceptThread.joinable()) { acceptThread.join(); }
                {
                    std::lock_guard lock{handshakeSocketsMutex};
                    for (const auto socket : handshakeSockets) { closeSocket(socket); }
                    handshakeSockets.clear();
                }
                {
                    std::lock_guard lock{replicasMutex};
                    for (const auto& replica : replicas) { requestReplicaStop(replica); }
                }
                reaperStopping.store(true);
                reaperCv.notify_all();
                if (reaperThread.joinable()) { reaperThread.join(); }
            }

            void reapLoop() {
                while (true) {
                    std::vector<std::shared_ptr<ReplicaState>> deadReplicas;
                    bool done = false;
                    {
                        std::lock_guard lock{replicasMutex};
                        for (auto it = replicas.begin(); it != replicas.end();) {
                            if ((*it)->dead.load()) {
                                deadReplicas.push_back(std::move(*it));
                                it = replicas.erase(it);
                            }
                            else { ++it; }
                        }
                        done = reaperStopping.load() && replicas.empty();
                    }
                    for (const auto& replica : deadReplicas) { finalizeReplica(replica); }
                    if (done) { return; }

                    std::unique_lock lock{reaperMutex};
                    reaperCv.wait_for(lock, std::chrono::milliseconds{100});
                }
            }

            void acceptLoop() {
                while (running.load()) {
                    SocketHandle client = ::accept(listenSock, nullptr, nullptr);
                    if (!socketOk(client)) { break; }
                    configureNoSigPipe(client);
                    setSocketTimeouts(client, HANDSHAKE_TIMEOUT_MS);
                    {
                        std::lock_guard lock{handshakeSocketsMutex};
                        if (!running.load()) {
                            closeSocket(client);
                            break;
                        }
                        handshakeSockets.insert(client);
                    }
                    const auto finishHandshake = [&] {
                        std::lock_guard lock{handshakeSocketsMutex};
                        handshakeSockets.erase(client);
                    };

                    std::unique_ptr<crypto::SecureSession> secure;
                    crypto::PublicKey secureRemotePublicKey{};
                    if (runtimeOptions.transportMode == TransportMode::SECURE) {
                        try {
                            crypto::ClientHello cryptoHello{};
                            if (!readSecureClientHello(client, cryptoHello)) { throw std::runtime_error("secure client hello failed"); }
                            auto accepted = crypto::acceptResponder(localIdentity, cryptoHello);
                            secureRemotePublicKey = accepted.remoteStaticPublicKey;
                            if (!writeSecureServerHello(client, accepted.hello)) { throw std::runtime_error("secure server hello failed"); }
                            secure = std::make_unique<crypto::SecureSession>(std::move(accepted.session));
                        }
                        catch (...) {
                            finishHandshake();
                            closeSocket(client);
                            continue;
                        }
                    }

                    DecodedFrame frame;
                    ClientHello hello;
                    if (!recvFrameFrom(client, secure.get(), frame) || frame.type != ReplMsgType::CLIENT_HELLO || !decodeClientHello(
                        frame.payload,
                        hello
                    )) {
                        finishHandshake();
                        closeSocket(client);
                        continue;
                    }
                    if (hello.role != NodeRole::REPLICA) {
                        finishHandshake();
                        closeSocket(client);
                        continue;
                    }
                    if (hello.groupId != 0 && hello.groupId != runtimeOptions.clusterGroupId) {
                        finishHandshake();
                        closeSocket(client);
                        continue;
                    }
                    if (hello.groupEpoch != 0 && hello.groupEpoch != runtimeOptions.clusterGroupEpoch) {
                        finishHandshake();
                        closeSocket(client);
                        continue;
                    }
                    if (!configuredReplicaNodeIds.empty() && std::ranges::find(configuredReplicaNodeIds, hello.nodeId) ==
                        configuredReplicaNodeIds.end()) {
                        finishHandshake();
                        closeSocket(client);
                        continue;
                    }
                    if (secure) {
                        const auto expected = pinnedPeerKey(runtimeOptions, hello.nodeId);
                        if (!expected || secureRemotePublicKey != *expected) {
                            finishHandshake();
                            closeSocket(client);
                            continue;
                        }
                    }

                    auto replica = std::make_shared<ReplicaState>();
                    replica->sock = client;
                    replica->secure = std::move(secure);
                    replica->nodeId = hello.nodeId;
                    replica->lastAckedSeq.store(hello.lastSeq);
                    replica->lastAckStage.store(static_cast<uint8_t>(AckStage::DURABLE));

                    ServerHello response{};
                    response.nodeId = selfNodeId;
                    response.role = NodeRole::PRIMARY;
                    bool resyncRequired = false;
                    {
                        std::lock_guard lock{replicasMutex};
                        const auto duplicate = std::ranges::find_if(
                            replicas,
                            [&](const std::shared_ptr<ReplicaState>& existing) {
                                return !existing->dead.load() && existing->nodeId == hello.nodeId;
                            }
                        );
                        if (duplicate != replicas.end()) {
                            finishHandshake();
                            closeSocket(client);
                            continue;
                        }
                        response.currentSeq = getCurrentSeq ? getCurrentSeq() : 0;
                        response.groupId = runtimeOptions.clusterGroupId;
                        response.groupEpoch = runtimeOptions.clusterGroupEpoch;
                        if (historyProvider && hello.lastSeq < response.currentSeq) {
                            const auto entries = historyProvider(hello.lastSeq, response.currentSeq);
                            if (!entries && consistency.replicaLagAction == ReplicaLagAction::ASYNC_RESYNC && snapshotProvider) {
                                const auto snapshot = snapshotProvider();
                                if (!snapshot) { resyncRequired = true; }
                                else {
                                    response.currentSeq = snapshot->seq;
                                    if (!queueInitialWire(
                                        replica,
                                        encodeSnapshotBegin(
                                            ReplSnapshotBegin{.snapshotSeq = snapshot->seq, .entryCount = snapshot->entries.size(),}
                                        )
                                    )) {
                                        resyncRequired = true;
                                    }
                                    for (const auto& entry : snapshot->entries) {
                                        if (resyncRequired || !queueInitialWire(replica, encodeSnapshotEntry(entry))) {
                                            resyncRequired = true;
                                            break;
                                        }
                                    }
                                    if (!resyncRequired && !queueInitialWire(replica, encodeSnapshotEnd(snapshot->seq))) { resyncRequired = true; }
                                }
                            }
                            else if (!entries) { resyncRequired = true; }
                            else {
                                for (const auto& entry : *entries) {
                                    if (!queueInitialWire(replica, encodeEntry(entry))) {
                                        resyncRequired = true;
                                        break;
                                    }
                                }
                            }
                        }
                        else if (!historyProvider) {
                            for (const auto& buffered : entryBuffer) {
                                if (buffered.seq > hello.lastSeq && !queueInitialWire(replica, buffered.wire)) {
                                    resyncRequired = true;
                                    break;
                                }
                            }
                        }
                        if (resyncRequired && consistency.replicaLagAction == ReplicaLagAction::BLOCK_WRITES) {
                            lagBlockedReplicas.insert(hello.nodeId);
                        }
                        if (!resyncRequired) {
                            lagBlockedReplicas.erase(hello.nodeId);
                            replicas.push_back(replica);
                        }
                    }
                    const auto helloWire = encodeServerHello(response);
                    if (!sendTo(client, replica->secure.get(), helloWire.data(), helloWire.size())) {
                        finishHandshake();
                        requestReplicaStop(replica);
                        continue;
                    }
                    if (resyncRequired) {
                        const auto resync = encodeFrame(ReplMsgType::RESYNC_REQUIRED, {});
                        (void)sendTo(client, replica->secure.get(), resync.data(), resync.size());
                        finishHandshake();
                        requestReplicaStop(replica);
                        continue;
                    }
                    finishHandshake();
                    setSocketTimeouts(client, 0);
                    replica->sendThread = std::thread([this, replica] { sendLoop(replica); });
                    replica->recvThread = std::thread([this, replica] { recvLoop(replica); });
                    replica->queueCv.notify_one();
                }
            }

            static bool sendTo(SocketHandle sock, crypto::SecureSession* secure, const uint8_t* data, size_t size) {
                try {
                    if (secure != nullptr) { return sendSecureFrame(sock, *secure, data, size); }
                    return sendAll(sock, data, size);
                }
                catch (...) { return false; }
            }

            static bool recvFrameFrom(SocketHandle sock, crypto::SecureSession* secure, DecodedFrame& out) {
                if (secure != nullptr) { return recvSecureFrame(sock, *secure, out); }
                return recvFrame(sock, out);
            }

            void requestReplicaStop(const std::shared_ptr<ReplicaState>& replica) {
                replica->dead.store(true);
                if (!replica->shutdownStarted.exchange(true)) { shutdownSocket(replica->sock); }
                replica->queueCv.notify_all();
                reaperCv.notify_all();
            }

            void finalizeReplica(const std::shared_ptr<ReplicaState>& replica) {
                requestReplicaStop(replica);
                if (replica->sendThread.joinable()) { replica->sendThread.join(); }
                if (replica->recvThread.joinable()) { replica->recvThread.join(); }
                closeSocket(replica->sock);
                replica->sock = BAD_SOCKET;
            }

            void setReadCallback(ReadCallback callback) { readCallback = std::move(callback); }

            void sendLoop(const std::shared_ptr<ReplicaState>& replica) {
                while (!replica->dead.load()) {
                    std::vector<uint8_t> wire;
                    {
                        std::unique_lock lock{replica->queueMutex};
                        replica->queueCv.wait(lock, [&] { return replica->dead.load() || !replica->queue.empty(); });
                        if (replica->dead.load() && replica->queue.empty()) { break; }
                        const uint64_t wireBytes = static_cast<uint64_t>(replica->queue.front().size());
                        wire = std::move(replica->queue.front());
                        replica->queue.pop_front();
                        replica->queuedBytes = replica->queuedBytes > wireBytes ? replica->queuedBytes - wireBytes : 0;
                    }
                    std::lock_guard sendLock{replica->sendMutex};
                    if (!sendTo(replica->sock, replica->secure.get(), wire.data(), wire.size())) {
                        requestReplicaStop(replica);
                        break;
                    }
                }
            }

            bool handleReadRequest(const std::shared_ptr<ReplicaState>& replica, const DecodedFrame& frame) {
                ReadRequest request;
                if (!decodeReadRequest(frame.payload, request)) { return false; }
                ReadResponse response;
                if (readCallback) {
                    try { response = readCallback(request); }
                    catch (...) {
                        response.requestId = request.requestId;
                        response.status = ReadStatus::ERROR_STATUS;
                    }
                }
                else {
                    response.requestId = request.requestId;
                    response.status = ReadStatus::ERROR_STATUS;
                }
                response.requestId = request.requestId;
                const auto wire = encodeReadResponse(response);
                std::lock_guard sendLock{replica->sendMutex};
                return sendTo(replica->sock, replica->secure.get(), wire.data(), wire.size());
            }

            void recvLoop(const std::shared_ptr<ReplicaState>& replica) {
                while (!replica->dead.load()) {
                    DecodedFrame frame;
                    if (!recvFrameFrom(replica->sock, replica->secure.get(), frame)) { break; }
                    if (frame.type == ReplMsgType::READ_REQUEST) {
                        if (!handleReadRequest(replica, frame)) { break; }
                        continue;
                    }
                    if (frame.type != ReplMsgType::ACK) { break; }
                    ReplAck ack;
                    if (!decodeAck(frame.payload, ack)) { break; }
                    replica->lastAckedSeq.store(ack.seq);
                    replica->lastAckStage.store(static_cast<uint8_t>(ack.stage));
                    ackCv.notify_all();
                }
                requestReplicaStop(replica);
            }

            void enqueueWire(uint64_t seq, std::vector<uint8_t> wire, bool retainForCatchup) {
                if (wire.empty()) { throw std::runtime_error("ReplicationServer: encoded replication frame exceeds maximum size"); }
                {
                    std::lock_guard lock{replicasMutex};
                    if (seq != 0 && consistency.replicaLagAction == ReplicaLagAction::BLOCK_WRITES && !lagBlockedReplicas.empty()) {
                        throw std::runtime_error("ReplicationServer: write blocked by lagging replica");
                    }
                    if (retainForCatchup) {
                        entryBuffer.push_back(BufferedWire{seq, wire});
                        while (entryBuffer.size() > ENTRY_BUFFER_SIZE) { entryBuffer.pop_front(); }
                    }
                    for (auto& replica : replicas) {
                        if (!replica->dead.load()) { (void)queueLiveWireLocked(replica, wire); }
                    }
                }
                if (!waitForAcks(seq) && (consistency.ackTimeoutAction == AckTimeoutAction::FAIL_ACK || consistency.ackTimeoutAction ==
                    AckTimeoutAction::FAIL_WRITE)) { throw std::runtime_error("ReplicationServer: write acknowledgement timeout"); }
            }

            void enqueueWireTo(uint64_t targetNodeId, uint64_t seq, std::vector<uint8_t> wire, bool waitForAck) {
                if (wire.empty()) { throw std::runtime_error("ReplicationServer: encoded replication frame exceeds maximum size"); }
                {
                    std::lock_guard lock{replicasMutex};
                    const auto it = std::ranges::find_if(
                        replicas,
                        [targetNodeId](const std::shared_ptr<ReplicaState>& replica) {
                            return !replica->dead.load() && replica->nodeId == targetNodeId;
                        }
                    );
                    if (it == replicas.end()) { throw std::runtime_error("ReplicationServer: target replica is not connected"); }
                    if (!queueLiveWireLocked(*it, wire)) { throw std::runtime_error("ReplicationServer: target replica queue is full"); }
                }
                if (!waitForAck) { return; }
                if (!waitForTargetAck(targetNodeId, seq) && (consistency.ackTimeoutAction == AckTimeoutAction::FAIL_ACK || consistency.
                    ackTimeoutAction == AckTimeoutAction::FAIL_WRITE)) {
                    throw std::runtime_error("ReplicationServer: write acknowledgement timeout");
                }
            }

            bool waitForAcks(uint64_t seq) {
                if (ackPolicy.mode == AckPolicyMode::NONE || seq == 0) { return true; }
                const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(consistency.ackTimeoutMs);
                while (std::chrono::steady_clock::now() < deadline) {
                    size_t live = 0;
                    size_t acked = 0;
                    {
                        std::lock_guard lock{replicasMutex};
                        for (const auto& replica : replicas) {
                            if (!replica->dead.load()) {
                                ++live;
                                const uint64_t ackSeq = replica->lastAckedSeq.load();
                                const auto ackStage = static_cast<AckStage>(replica->lastAckStage.load());
                                if (ackSeq > seq || (ackSeq == seq && ackStage >= ackPolicy.stage)) { ++acked; }
                            }
                        }
                    }

                    const bool ok = ackPolicy.mode == AckPolicyMode::ALL_TARGETS
                                        ? (consistency.writeConsistency == WriteConsistency::ALL_CONFIGURED
                                               ? acked >= configuredReplicaCount
                                               : live > 0 && acked >= live)
                                        : acked >= ackPolicy.quorum;
                    if (ok) { return true; }

                    std::unique_lock lock{ackMutex};
                    ackCv.wait_for(lock, std::chrono::milliseconds(50));
                }
                return false;
            }

            bool waitForTargetAck(uint64_t targetNodeId, uint64_t seq) {
                if (ackPolicy.mode == AckPolicyMode::NONE || seq == 0) { return true; }
                const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(consistency.ackTimeoutMs);
                while (std::chrono::steady_clock::now() < deadline) {
                    {
                        std::lock_guard lock{replicasMutex};
                        for (const auto& replica : replicas) {
                            if (replica->dead.load() || replica->nodeId != targetNodeId) { continue; }
                            const uint64_t ackSeq = replica->lastAckedSeq.load();
                            const auto ackStage = static_cast<AckStage>(replica->lastAckStage.load());
                            if (ackSeq > seq || (ackSeq == seq && ackStage >= ackPolicy.stage)) { return true; }
                        }
                    }

                    std::unique_lock lock{ackMutex};
                    ackCv.wait_for(lock, std::chrono::milliseconds(50));
                }
                return false;
            }
    };

    ReplicationServer::ReplicationServer(std::unique_ptr<Impl> impl) : impl_{std::move(impl)} {}
    ReplicationServer::~ReplicationServer() { close(); }

    std::unique_ptr<ReplicationServer> ReplicationServer::create(
        uint16_t replPort,
        uint64_t selfNodeId,
        std::function<uint64_t()> getCurrentSeq,
        AckPolicy ackPolicy,
        ConsistencyOptions consistency,
        uint16_t configuredReplicaCount,
        std::vector<uint64_t> configuredReplicaNodeIds,
        ClusterRuntimeOptions runtimeOptions,
        HistoryProvider historyProvider,
        SnapshotProvider snapshotProvider
    ) {
        auto impl = std::make_unique<Impl>();
        impl->replPort = replPort;
        impl->selfNodeId = selfNodeId;
        impl->getCurrentSeq = std::move(getCurrentSeq);
        impl->ackPolicy = ackPolicy;
        impl->consistency = consistency;
        impl->configuredReplicaCount = configuredReplicaCount;
        impl->configuredReplicaNodeIds = std::move(configuredReplicaNodeIds);
        impl->runtimeOptions = std::move(runtimeOptions);
        impl->historyProvider = std::move(historyProvider);
        impl->snapshotProvider = std::move(snapshotProvider);
        if (impl->runtimeOptions.transportMode == TransportMode::SECURE) { impl->localIdentity = loadSecureIdentity(impl->runtimeOptions); }
        return std::unique_ptr<ReplicationServer>(new ReplicationServer(std::move(impl)));
    }

    void ReplicationServer::start() { impl_->start(); }
    void ReplicationServer::close() { if (impl_) { impl_->close(); } }
    void ReplicationServer::setReadCallback(ReadCallback callback) { impl_->setReadCallback(std::move(callback)); }

    void ReplicationServer::shipEntry(
        uint64_t seq,
        ReplOpType op,
        std::span<const uint8_t> key,
        std::span<const uint8_t> value,
        uint8_t recordFlags,
        uint64_t sourceNodeId
    ) {
        ReplEntry entry;
        entry.seq = seq;
        entry.sourceNodeId = sourceNodeId;
        entry.op = op;
        entry.recordFlags = recordFlags;
        entry.key.assign(key.begin(), key.end());
        entry.value.assign(value.begin(), value.end());
        impl_->enqueueWire(seq, encodeEntry(entry), true);
    }

    void ReplicationServer::shipEntryTo(
        uint64_t targetNodeId,
        uint64_t seq,
        ReplOpType op,
        std::span<const uint8_t> key,
        std::span<const uint8_t> value,
        uint8_t recordFlags,
        uint64_t sourceNodeId,
        bool waitForAck
    ) {
        ReplEntry entry;
        entry.seq = seq;
        entry.sourceNodeId = sourceNodeId;
        entry.op = op;
        entry.recordFlags = recordFlags;
        entry.key.assign(key.begin(), key.end());
        entry.value.assign(value.begin(), value.end());
        impl_->enqueueWireTo(targetNodeId, seq, encodeEntry(entry), waitForAck);
    }

    void ReplicationServer::shipBlob(uint64_t seq, uint64_t blobId, std::span<const uint8_t> content) {
        ReplBlob blob;
        blob.seq = seq;
        blob.blobId = blobId;
        blob.content.assign(content.begin(), content.end());
        impl_->enqueueWire(0, encodeBlob(blob), false);
    }

    size_t ReplicationServer::replicaCount() const noexcept {
        std::lock_guard lock{impl_->replicasMutex};
        size_t count = 0;
        for (const auto& replica : impl_->replicas) { if (!replica->dead.load()) { ++count; } }
        return count;
    }
} // namespace akkaradb::engine::cluster

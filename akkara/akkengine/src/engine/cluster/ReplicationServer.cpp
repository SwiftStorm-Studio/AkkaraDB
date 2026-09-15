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
#include "akk/engine/cluster/detail/ReplicationTransfer.hpp"
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
#include <unordered_map>
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
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace akkaradb::engine::cluster {
    namespace {
        constexpr int HANDSHAKE_TIMEOUT_MS = 5'000;
        uint64_t contactNowUs() noexcept {
            return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count());
        }
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

        bool recvAll(SocketHandle s, uint8_t* data, size_t size, const std::atomic<bool>* running = nullptr) {
            const auto deadline = running != nullptr
                ? std::chrono::steady_clock::now() + std::chrono::milliseconds{HANDSHAKE_TIMEOUT_MS}
                : std::chrono::steady_clock::time_point{};
            size_t got = 0;
            while (got < size) {
                if (running != nullptr) {
                    if (!running->load() || std::chrono::steady_clock::now() >= deadline) { return false; }
#ifdef _WIN32
                    fd_set readable;
                    FD_ZERO(&readable);
                    FD_SET(s, &readable);
                    timeval timeout{0, 50'000};
                    const int ready = ::select(0, &readable, nullptr, nullptr, &timeout);
#else
                    pollfd readable{s, POLLIN, 0};
                    const int ready = ::poll(&readable, 1, 50);
#endif
                    if (!running->load() || ready < 0) { return false; }
                    if (ready == 0) { continue; }
                }
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

        bool recvFrame(SocketHandle s, DecodedFrame& out, const std::shared_ptr<detail::TransferBudget>& budget, const std::atomic<bool>* running = nullptr) {
            uint8_t header[ReplFrameHeader::SIZE];
            if (!recvAll(s, header, sizeof(header), running)) { return false; }
            const uint32_t payloadLen = readU32(header + 6);
            if (payloadLen > detail::TRANSFER_FRAME_LIMIT) { return false; }
            auto wireMemory = budget->reserve(detail::TransferBudget::Resource::MEMORY, ReplFrameHeader::SIZE + payloadLen);
            out.memoryReservation = budget->reserve(detail::TransferBudget::Resource::MEMORY, payloadLen);
            std::vector<uint8_t> wire(sizeof(header) + payloadLen);
            std::memcpy(wire.data(), header, sizeof(header));
            if (payloadLen > 0 && !recvAll(s, wire.data() + sizeof(header), payloadLen, running)) { return false; }
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
        constexpr uint32_t SECURE_MAX_CIPHERTEXT_SIZE = ReplFrameHeader::SIZE + detail::TRANSFER_FRAME_LIMIT;

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

        bool readSecureClientHello(SocketHandle socket, crypto::ClientHello& hello, const std::atomic<bool>* running) {
            std::array<uint8_t, SECURE_CLIENT_HELLO_SIZE> wire{};
            if (!recvAll(socket, wire.data(), wire.size(), running)) { return false; }
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

        bool sendSecureFrame(SocketHandle socket, crypto::SecureSession& session, const uint8_t* data, size_t size,
            const std::shared_ptr<detail::TransferBudget>& budget) {
            if (size > SECURE_MAX_CIPHERTEXT_SIZE) { return false; }
            auto encryptedMemory = budget->reserve(detail::TransferBudget::Resource::MEMORY, size);
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

        bool recvSecureFrame(SocketHandle socket, crypto::SecureSession& session, DecodedFrame& out, const std::shared_ptr<detail::TransferBudget>& budget, const std::atomic<bool>* running = nullptr) {
            std::array<uint8_t, SECURE_FRAME_HEADER_SIZE> header{};
            if (!recvAll(socket, header.data(), header.size(), running)) { return false; }
            if (readU32Le(header.data()) != SECURE_FRAME_MAGIC || header[4] != SECURE_VERSION) { return false; }

            crypto::EncryptedFrame encrypted;
            encrypted.counter = readU64Le(header.data() + 6);
            const uint32_t ciphertextSize = readU32Le(header.data() + 14);
            if (ciphertextSize > SECURE_MAX_CIPHERTEXT_SIZE) { return false; }
            std::memcpy(encrypted.tag.data(), header.data() + 18, encrypted.tag.size());
            auto decryptMemory = budget->reserve(detail::TransferBudget::Resource::MEMORY, 2ull * ciphertextSize);
            out.memoryReservation = budget->reserve(detail::TransferBudget::Resource::MEMORY,
                ciphertextSize > ReplFrameHeader::SIZE ? ciphertextSize - ReplFrameHeader::SIZE : 0);
            encrypted.ciphertext.resize(ciphertextSize);
            if (ciphertextSize > 0 && !recvAll(socket, encrypted.ciphertext.data(), ciphertextSize, running)) { return false; }

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
            detail::MessagePtr wire;
        };

        enum class ReplicaPhase : uint8_t {
            BOOTSTRAPPING,
            LIVE,
        };

        struct ReplicaState {
            SocketHandle sock = BAD_SOCKET;
            std::unique_ptr<crypto::SecureSession> secure;
            uint64_t nodeId = 0;
            std::atomic<uint64_t> lastAckedSeq{0};
            std::atomic<uint8_t> lastAckStage{static_cast<uint8_t>(AckStage::DURABLE)};
            std::atomic<uint64_t> lastSuccessfulContactAtUs{0};
            std::atomic<bool> dead{false};
            std::atomic<bool> shutdownStarted{false};
            std::atomic<bool> workersReady{false};
            ReplicaPhase phase = ReplicaPhase::BOOTSTRAPPING; // Guarded by replicasMutex.
            std::mutex queueMutex;
            std::condition_variable queueCv;
            std::deque<detail::MessagePtr> queue;
            uint64_t queuedBytes = 0;
            std::mutex messageMutex;
            std::mutex sendMutex;
            std::shared_ptr<detail::TransferSession> transferSession = std::make_shared<detail::TransferSession>();
            std::thread sendThread;
            std::thread recvThread;
            std::thread readThread;
            std::thread bootstrapThread;
            std::mutex readMutex;
            std::condition_variable readCv;
            struct PendingRead {
                ReadRequest request;
                std::shared_ptr<void> reservation;
            };
            std::optional<PendingRead> pendingRead;
            struct PendingStripeControl {
                StripeControlRequest request;
                std::shared_ptr<void> reservation;
            };
            std::optional<PendingStripeControl> pendingStripeControl;

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
            StripeControlCallback stripeControlCallback;
            AckPolicy ackPolicy;
            ConsistencyOptions consistency;
            uint16_t configuredReplicaCount = 0;
            ClusterRuntimeOptions runtimeOptions;
            std::shared_ptr<detail::TransferBudget> transferBudget;
            std::vector<uint64_t> configuredReplicaNodeIds;
            crypto::NodeIdentity localIdentity;

            SocketHandle listenSock = BAD_SOCKET;
            std::recursive_mutex lifecycleMutex;
            std::atomic<bool> running{false};
            std::thread acceptThread;
            static constexpr size_t HANDSHAKE_WORKERS = 4;
            static constexpr size_t MAX_PENDING_HANDSHAKES = 64;
            std::vector<std::thread> handshakeWorkers;
            std::deque<SocketHandle> pendingHandshakes;
            std::condition_variable handshakeCv;
            std::atomic<bool> reaperStopping{false};
            std::thread reaperThread;
            std::mutex reaperMutex;
            std::condition_variable reaperCv;
            std::mutex handshakeSocketsMutex;
            std::unordered_set<SocketHandle> handshakeSockets;

            mutable std::mutex replicasMutex;
            std::vector<std::shared_ptr<ReplicaState>> replicas;
            std::unordered_set<uint64_t> bootstrappingReplicaNodeIds;
            std::unordered_map<uint64_t, uint64_t> lastSuccessfulContactByNode;
            std::deque<BufferedWire> entryBuffer;
            uint64_t entryBufferBytes = 0;

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

            bool bootstrapQueueHasCapacityLocked(const ReplicaState& replica, size_t wireSize) const noexcept {
                if (!queueHasCapacityLocked(replica, wireSize)) { return false; }
                // Keep headroom in the shared transfer budget for constructing
                // the next catch-up message and for the receiver/control paths.
                const uint64_t largestHeapMessage = static_cast<uint64_t>(transferBudget->options.thresholdBytes) + ReplFrameHeader::SIZE;
                const uint64_t resourceWindow = std::max<uint64_t>(1, transferBudget->options.maxMemoryBytes / (2 * largestHeapMessage));
                return replica.queue.size() < std::min<uint64_t>(64, resourceWindow);
            }

            bool queueBootstrapWire(const std::shared_ptr<ReplicaState>& replica, detail::MessagePtr wire) {
                if (!wire) { return false; }
                if (runtimeOptions.maxReplicaQueueBytes != 0 && wire->size() > runtimeOptions.maxReplicaQueueBytes) { return false; }
                std::unique_lock lock{replica->queueMutex};
                replica->queueCv.wait(lock, [&] {
                    return replica->dead.load() || !running.load() || bootstrapQueueHasCapacityLocked(*replica, wire->size());
                });
                if (replica->dead.load() || !running.load()) { return false; }
                replica->queuedBytes += static_cast<uint64_t>(wire->size());
                replica->queue.push_back(std::move(wire));
                replica->queueCv.notify_one();
                return true;
            }

            bool queueLiveWireLocked(const std::shared_ptr<ReplicaState>& replica, const detail::MessagePtr& wire) {
                if (!wire) { return false; }
                std::lock_guard qlock{replica->queueMutex};
                if (!queueHasCapacityLocked(*replica, wire->size())) {
                    requestReplicaStop(replica);
                    if (consistency.replicaLagAction == ReplicaLagAction::BLOCK_WRITES) { lagBlockedReplicas.insert(replica->nodeId); }
                    return false;
                }
                replica->queuedBytes += static_cast<uint64_t>(wire->size());
                replica->queue.push_back(wire);
                replica->queueCv.notify_one();
                return true;
            }

            bool queueBootstrapWire(const std::shared_ptr<ReplicaState>& replica, std::vector<uint8_t> wire) {
                if (wire.empty()) { return false; }
                return queueBootstrapWire(replica, detail::messageFromWire(std::move(wire), transferBudget));
            }

            void start() {
                std::lock_guard lock{lifecycleMutex};
                if (running.load()) { return; }
                listenSock = listenOn(runtimeOptions.replBindHost, replPort);
                running.store(true);
                reaperStopping.store(false);
                try {
                    reaperThread = std::thread([this] { reapLoop(); });
                    for (size_t i = 0; i < HANDSHAKE_WORKERS; ++i) {
                        handshakeWorkers.emplace_back([this] { handshakeLoop(); });
                    }
                    acceptThread = std::thread([this, listener = listenSock] { acceptLoop(listener); });
                }
                catch (...) {
                    close();
                    throw;
                }
            }

            void close() {
                std::lock_guard lifecycleLock{lifecycleMutex};
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
                handshakeCv.notify_all();
                if (acceptThread.joinable()) { acceptThread.join(); }
                for (auto& worker : handshakeWorkers) { if (worker.joinable()) { worker.join(); } }
                handshakeWorkers.clear();
                {
                    std::lock_guard lock{handshakeSocketsMutex};
                    for (const auto socket : pendingHandshakes) { closeSocket(socket); }
                    pendingHandshakes.clear();
                    handshakeSockets.clear();
                }
                {
                    std::lock_guard lock{replicasMutex};
                    entryBuffer.clear();
                    entryBufferBytes = 0;
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
                            if ((*it)->dead.load() && (*it)->workersReady.load()) {
                                const auto nodeId = (*it)->nodeId;
                                const auto contact = (*it)->lastSuccessfulContactAtUs.load(std::memory_order_relaxed);
                                auto& retained = lastSuccessfulContactByNode[nodeId];
                                retained = std::max(retained, contact);
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

            void acceptLoop(SocketHandle listener) {
                while (running.load()) {
                    const auto client = ::accept(listener, nullptr, nullptr);
                    if (!socketOk(client)) { break; }
                    configureNoSigPipe(client);
                    setSocketTimeouts(client, HANDSHAKE_TIMEOUT_MS);
                    std::lock_guard lock{handshakeSocketsMutex};
                    if (!running.load() || pendingHandshakes.size() >= MAX_PENDING_HANDSHAKES) {
                        closeSocket(client);
                        continue;
                    }
                    try {
                        handshakeSockets.insert(client);
                        pendingHandshakes.push_back(client);
                    }
                    catch (...) {
                        handshakeSockets.erase(client);
                        closeSocket(client);
                        continue;
                    }
                    handshakeCv.notify_one();
                }
            }

            void handshakeLoop() {
                while (true) {
                    SocketHandle client;
                    {
                        std::unique_lock lock{handshakeSocketsMutex};
                        handshakeCv.wait(lock, [this] { return !running.load() || !pendingHandshakes.empty(); });
                        if (!running.load()) { return; }
                        client = pendingHandshakes.front();
                        pendingHandshakes.pop_front();
                    }
                    std::shared_ptr<ReplicaState> replica;
                    bool connected = false;
                    try {
                        replica = std::make_shared<ReplicaState>();
                        replica->sock = client;
                        connected = processHandshake(replica);
                    }
                    catch (...) {}
                    // Detach before releasing ownership: close() must never shut
                    // down a descriptor that has already been closed and reused.
                    {
                        std::lock_guard lock{handshakeSocketsMutex};
                        handshakeSockets.erase(client);
                    }
                    if (replica) {
                        {
                            std::lock_guard lock{replicasMutex};
                            bootstrappingReplicaNodeIds.erase(replica->nodeId);
                        }
                        if (!connected) {
                            requestReplicaStop(replica);
                            finalizeReplica(replica);
                        }
                        else {
                            replica->workersReady.store(true);
                            replica->queueCv.notify_one();
                            reaperCv.notify_one();
                        }
                    }
                    else { closeSocket(client); }
                }
            }

            bool processHandshake(const std::shared_ptr<ReplicaState>& replica) {
                const auto client = replica->sock;
                std::unique_ptr<crypto::SecureSession> secure;
                crypto::PublicKey secureRemotePublicKey{};
                if (runtimeOptions.transportMode == TransportMode::SECURE) {
                    try {
                        crypto::ClientHello cryptoHello{};
                        if (!readSecureClientHello(client, cryptoHello, &running)) { throw std::runtime_error("secure client hello failed"); }
                        auto accepted = crypto::acceptResponder(localIdentity, cryptoHello);
                        secureRemotePublicKey = accepted.remoteStaticPublicKey;
                        if (!writeSecureServerHello(client, accepted.hello)) { throw std::runtime_error("secure server hello failed"); }
                        secure = std::make_unique<crypto::SecureSession>(std::move(accepted.session));
                    }
                    catch (...) {
                        return false;
                    }
                }

                DecodedFrame frame;
                ClientHello hello;
                if (!recvFrameFrom(client, secure.get(), frame, &running) || frame.type != ReplMsgType::CLIENT_HELLO || !decodeClientHello(
                    frame.payload,
                    hello
                )) {
                    return false;
                }
                if (hello.role != NodeRole::REPLICA) {
                    return false;
                }
                if (hello.groupId != 0 && hello.groupId != runtimeOptions.clusterGroupId) {
                    return false;
                }
                if (hello.groupEpoch != 0 && hello.groupEpoch != runtimeOptions.clusterGroupEpoch) {
                    return false;
                }
                if (!configuredReplicaNodeIds.empty() && std::ranges::find(configuredReplicaNodeIds, hello.nodeId) ==
                    configuredReplicaNodeIds.end()) {
                    return false;
                }
                if (secure) {
                    const auto expected = pinnedPeerKey(runtimeOptions, hello.nodeId);
                    if (!expected || secureRemotePublicKey != *expected) {
                        return false;
                    }
                }

                replica->secure = std::move(secure);
                replica->nodeId = hello.nodeId;
                replica->transferSession->setScope(runtimeOptions.clusterGroupId, selfNodeId, hello.nodeId);
                replica->lastAckedSeq.store(hello.lastSeq);
                replica->lastAckStage.store(static_cast<uint8_t>(AckStage::DURABLE));

                ServerHello response{};
                response.nodeId = selfNodeId;
                response.role = NodeRole::PRIMARY;
                {
                    std::lock_guard lock{replicasMutex};
                    const auto duplicate = std::ranges::find_if(
                        replicas,
                        [&](const std::shared_ptr<ReplicaState>& existing) {
                            return !existing->dead.load() && existing->nodeId == hello.nodeId;
                        }
                    );
                    if (duplicate != replicas.end() || bootstrappingReplicaNodeIds.contains(hello.nodeId)) {
                        return false;
                    }
                    bootstrappingReplicaNodeIds.insert(hello.nodeId);
                    response.currentSeq = getCurrentSeq ? getCurrentSeq() : 0;
                    response.groupId = runtimeOptions.clusterGroupId;
                    response.groupEpoch = runtimeOptions.clusterGroupEpoch;
                }
                const auto helloWire = encodeServerHello(response);
                if (!sendTo(client, replica->secure.get(), helloWire.data(), helloWire.size())) {
                    requestReplicaStop(replica);
                    return false;
                }
                replica->lastSuccessfulContactAtUs.store(contactNowUs(), std::memory_order_relaxed);
                setSocketTimeouts(client, 0);
                try {
                    replica->sendThread = std::thread([this, replica] { sendLoop(replica); });
                    replica->readThread = std::thread([this, replica] { readLoop(replica); });
                    replica->recvThread = std::thread([this, replica] { recvLoop(replica); });
                }
                catch (...) {
                    requestReplicaStop(replica);
                    return false;
                }
                {
                    std::lock_guard lock{replicasMutex};
                    bootstrappingReplicaNodeIds.erase(hello.nodeId);
                    replicas.push_back(replica);
                }
                try {
                    replica->bootstrapThread = std::thread([this, replica, lastSeq = hello.lastSeq] {
                        bootstrapLoop(replica, lastSeq);
                    });
                }
                catch (...) { requestReplicaStop(replica); }
                return true;
            }

            void bootstrapLoop(const std::shared_ptr<ReplicaState>& replica, uint64_t lastSeq) noexcept {
                bool resyncRequired = false;
                try {
                    uint64_t cursor = lastSeq;
                    while (!replica->dead.load() && running.load()) {
                        if (historyProvider) {
                            uint64_t through = 0;
                            {
                                std::lock_guard lock{replicasMutex};
                                through = getCurrentSeq ? getCurrentSeq() : 0;
                                if (through <= cursor) {
                                    replica->phase = ReplicaPhase::LIVE;
                                    lagBlockedReplicas.erase(replica->nodeId);
                                    ackCv.notify_all();
                                    return;
                                }
                            }

                            const auto entries = historyProvider(cursor, through);
                            if (!entries) {
                                if (consistency.replicaLagAction != ReplicaLagAction::ASYNC_RESYNC || !snapshotProvider) {
                                    resyncRequired = true;
                                    break;
                                }
                                const auto snapshot = snapshotProvider();
                                if (!snapshot || !snapshot->forEachEntry || snapshot->seq < through || !queueBootstrapWire(
                                    replica,
                                    encodeSnapshotBegin(ReplSnapshotBegin{
                                        .snapshotSeq = snapshot->seq,
                                        .entryCount = snapshot->entryCount,
                                    })
                                )) {
                                    resyncRequired = true;
                                    break;
                                }
                                uint64_t streamedEntries = 0;
                                const bool streamed = snapshot->forEachEntry([&](std::span<const uint8_t> key, std::span<const uint8_t> value) {
                                    if (streamedEntries == snapshot->entryCount || !queueBootstrapWire(
                                        replica,
                                        detail::snapshotMessage(key, value, transferBudget)
                                    )) { return false; }
                                    ++streamedEntries;
                                    return true;
                                });
                                if (!streamed || streamedEntries != snapshot->entryCount) { resyncRequired = true; }
                                if (resyncRequired || !queueBootstrapWire(replica, encodeSnapshotEnd(snapshot->seq))) {
                                    resyncRequired = true;
                                    break;
                                }
                                cursor = snapshot->seq;
                                continue;
                            }

                            uint64_t expected = cursor;
                            for (const auto& entry : *entries) {
                                if (expected == UINT64_MAX || entry.seq != expected + 1 || entry.seq > through || !queueBootstrapWire(
                                    replica,
                                    detail::entryMessage(
                                        entry.seq,
                                        entry.sourceNodeId,
                                        entry.op,
                                        entry.recordFlags,
                                        entry.key,
                                        entry.value,
                                        transferBudget
                                    )
                                )) {
                                    resyncRequired = true;
                                    break;
                                }
                                expected = entry.seq;
                            }
                            if (resyncRequired || expected != through) {
                                resyncRequired = true;
                                break;
                            }
                            cursor = through;
                            continue;
                        }

                        std::vector<BufferedWire> buffered;
                        {
                            std::lock_guard lock{replicasMutex};
                            for (const auto& entry : entryBuffer) {
                                if (entry.seq > cursor) { buffered.push_back(entry); }
                            }
                            if (buffered.empty()) {
                                replica->phase = ReplicaPhase::LIVE;
                                lagBlockedReplicas.erase(replica->nodeId);
                                ackCv.notify_all();
                                return;
                            }
                        }
                        for (const auto& entry : buffered) {
                            if (entry.seq <= cursor) { continue; }
                            if (!queueBootstrapWire(replica, entry.wire)) {
                                resyncRequired = true;
                                break;
                            }
                            cursor = entry.seq;
                        }
                        if (resyncRequired) { break; }
                    }
                }
                catch (...) { resyncRequired = true; }

                const bool reportResync = resyncRequired && running.load() && !replica->dead.load();
                if (reportResync && consistency.replicaLagAction == ReplicaLagAction::BLOCK_WRITES) {
                    std::lock_guard lock{replicasMutex};
                    lagBlockedReplicas.insert(replica->nodeId);
                }
                if (reportResync) {
                    try {
                        const auto resync = encodeFrame(ReplMsgType::RESYNC_REQUIRED, {});
                        const auto message = detail::messageFromWire(resync, transferBudget);
                        (void)sendTo(replica, *message);
                    }
                    catch (...) {}
                }
                requestReplicaStop(replica);
            }

            bool sendTo(SocketHandle sock, crypto::SecureSession* secure, const detail::TransferMessage& message) {
                try {
                    return detail::sendMessage(message, transferBudget, [&](std::span<const uint8_t> wire) {
                        return secure ? sendSecureFrame(sock, *secure, wire.data(), wire.size(), transferBudget) : sendAll(sock, wire.data(), wire.size());
                    });
                } catch (...) { return false; }
            }
            bool sendFrame(const std::shared_ptr<ReplicaState>& replica, std::span<const uint8_t> wire) {
                std::lock_guard lock{replica->sendMutex};
                try {
                    return replica->secure ? sendSecureFrame(replica->sock, *replica->secure, wire.data(), wire.size(), transferBudget) :
                           sendAll(replica->sock, wire.data(), wire.size());
                }
                catch (...) { return false; }
            }
            bool sendTo(const std::shared_ptr<ReplicaState>& replica, const detail::TransferMessage& message) {
                std::lock_guard messageLock{replica->messageMutex};
                try {
                    return detail::sendMessage(message, transferBudget, replica->transferSession,
                        [&](std::span<const uint8_t> wire) { return sendFrame(replica, wire); });
                }
                catch (...) { return false; }
            }
            bool sendTo(SocketHandle sock, crypto::SecureSession* secure, const uint8_t* data, size_t size) {
                try {
                    const auto message = detail::messageFromWire(std::vector<uint8_t>{data, data + size}, transferBudget);
                    return sendTo(sock, secure, *message);
                }
                catch (...) { return false; }
            }

            bool recvFrameFrom(SocketHandle sock, crypto::SecureSession* secure, DecodedFrame& out, const std::atomic<bool>* running = nullptr) {
                if (secure != nullptr) { return recvSecureFrame(sock, *secure, out, transferBudget, running); }
                return recvFrame(sock, out, transferBudget, running);
            }

            void requestReplicaStop(const std::shared_ptr<ReplicaState>& replica) {
                replica->dead.store(true);
                replica->transferSession->cancel();
                if (!replica->shutdownStarted.exchange(true)) { shutdownSocket(replica->sock); }
                replica->queueCv.notify_all();
                replica->readCv.notify_all();
                reaperCv.notify_all();
            }

            void finalizeReplica(const std::shared_ptr<ReplicaState>& replica) {
                requestReplicaStop(replica);
                if (replica->bootstrapThread.joinable()) { replica->bootstrapThread.join(); }
                if (replica->sendThread.joinable()) { replica->sendThread.join(); }
                if (replica->recvThread.joinable()) { replica->recvThread.join(); }
                if (replica->readThread.joinable()) { replica->readThread.join(); }
                closeSocket(replica->sock);
                replica->sock = BAD_SOCKET;
            }

            void setReadCallback(ReadCallback callback) { readCallback = std::move(callback); }
            void setStripeControlCallback(StripeControlCallback callback) { stripeControlCallback = std::move(callback); }

            void sendLoop(const std::shared_ptr<ReplicaState>& replica) {
                while (!replica->dead.load()) {
                    detail::MessagePtr wire;
                    {
                        std::unique_lock lock{replica->queueMutex};
                        replica->queueCv.wait(lock, [&] { return replica->dead.load() || !replica->queue.empty(); });
                        if (replica->dead.load() && replica->queue.empty()) { break; }
                        const uint64_t wireBytes = static_cast<uint64_t>(replica->queue.front()->size());
                        wire = std::move(replica->queue.front());
                        replica->queue.pop_front();
                        replica->queuedBytes = replica->queuedBytes > wireBytes ? replica->queuedBytes - wireBytes : 0;
                        replica->queueCv.notify_all();
                    }
                    if (!sendTo(replica, *wire)) {
                        requestReplicaStop(replica);
                        break;
                    }
                }
            }

            bool handleReadRequest(const std::shared_ptr<ReplicaState>& replica, const ReadRequest& request) {
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
                const auto wire = detail::readResponseMessage(response, transferBudget);
                return sendTo(replica, *wire);
            }

            void readLoop(const std::shared_ptr<ReplicaState>& replica) {
                while (!replica->dead.load()) {
                    std::optional<ReplicaState::PendingRead> pendingRead;
                    std::optional<ReplicaState::PendingStripeControl> pendingControl;
                    {
                        std::unique_lock lock{replica->readMutex};
                        replica->readCv.wait(lock, [&] {
                            return replica->dead.load() || replica->pendingRead.has_value() || replica->pendingStripeControl.has_value();
                        });
                        if (replica->dead.load()) { return; }
                        if (replica->pendingRead) {
                            pendingRead = std::move(replica->pendingRead);
                            replica->pendingRead.reset();
                        }
                        else {
                            pendingControl = std::move(replica->pendingStripeControl);
                            replica->pendingStripeControl.reset();
                        }
                    }
                    try {
                        if (pendingRead && handleReadRequest(replica, pendingRead->request)) { continue; }
                        if (pendingControl) {
                            StripeControlResponse response;
                            if (stripeControlCallback) {
                                response = stripeControlCallback(replica->nodeId, pendingControl->request);
                            }
                            response.requestId = pendingControl->request.requestId;
                            const auto wire = encodeStripeControlResponse(response);
                            if (sendFrame(replica, wire)) { continue; }
                        }
                    }
                    catch (...) {}
                    requestReplicaStop(replica);
                }
            }

            void recvLoop(const std::shared_ptr<ReplicaState>& replica) {
                while (!replica->dead.load()) {
                    detail::MessagePtr message;
                    try { message = detail::receiveMessage(transferBudget, replica->transferSession, [&](DecodedFrame& part) {
                        return recvFrameFrom(replica->sock, replica->secure.get(), part);
                    }, [&](std::span<const uint8_t> wire) { return sendFrame(replica, wire); }); } catch (...) {}
                    if (!message) { break; }
                    const auto& frame = *message;
                    replica->lastSuccessfulContactAtUs.store(contactNowUs(), std::memory_order_relaxed);
                    if (frame.type == ReplMsgType::READ_REQUEST) {
                        ReplicaState::PendingRead pending;
                        try {
                            pending.reservation = transferBudget->reserve(detail::TransferBudget::Resource::MEMORY, frame.payload.size());
                            if (!decodeReadRequest(frame.payload, pending.request)) { break; }
                        }
                        catch (...) { break; }
                        {
                            std::lock_guard lock{replica->readMutex};
                            if (replica->pendingRead) { break; } // Bound pipelined work per connection.
                            replica->pendingRead = std::move(pending);
                        }
                        replica->readCv.notify_one();
                        continue;
                    }
                    if (frame.type == ReplMsgType::STRIPE_CONTROL_REQUEST) {
                        ReplicaState::PendingStripeControl pending;
                        try {
                            pending.reservation = transferBudget->reserve(detail::TransferBudget::Resource::MEMORY, frame.payload.size());
                            if (!decodeStripeControlRequest(frame.payload, pending.request)) { break; }
                        }
                        catch (...) { break; }
                        {
                            std::lock_guard lock{replica->readMutex};
                            if (replica->pendingStripeControl) { break; }
                            replica->pendingStripeControl = std::move(pending);
                        }
                        replica->readCv.notify_one();
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

            void enqueueWire(uint64_t seq, detail::MessagePtr wire, bool retainForCatchup) {
                if (!wire) { throw std::runtime_error("ReplicationServer: encoded replication frame exceeds maximum size"); }
                {
                    std::lock_guard lock{replicasMutex};
                    if (seq != 0 && consistency.replicaLagAction == ReplicaLagAction::BLOCK_WRITES && !lagBlockedReplicas.empty()) {
                        throw std::runtime_error("ReplicationServer: write blocked by lagging replica");
                    }
                    if (retainForCatchup && !historyProvider) {
                        entryBuffer.push_back(BufferedWire{seq, wire});
                        entryBufferBytes += wire->size();
                        const auto limit = std::min(transferBudget->options.maxMemoryBytes, transferBudget->options.maxSpoolBytes) / 4;
                        while (entryBuffer.size() > ENTRY_BUFFER_SIZE || entryBufferBytes > limit) {
                            entryBufferBytes -= entryBuffer.front().wire->size();
                            entryBuffer.pop_front();
                        }
                    }
                    for (auto& replica : replicas) {
                        if (!replica->dead.load() && replica->phase == ReplicaPhase::LIVE) {
                            (void)queueLiveWireLocked(replica, wire);
                        }
                    }
                }
                if (!waitForAcks(seq) && (consistency.ackTimeoutAction == AckTimeoutAction::FAIL_ACK || consistency.ackTimeoutAction ==
                    AckTimeoutAction::FAIL_WRITE)) { throw std::runtime_error("ReplicationServer: write acknowledgement timeout"); }
            }

            void enqueueWireTo(uint64_t targetNodeId, uint64_t seq, detail::MessagePtr wire, bool waitForAck) {
                if (!wire) { throw std::runtime_error("ReplicationServer: encoded replication frame exceeds maximum size"); }
                {
                    std::lock_guard lock{replicasMutex};
                    const auto it = std::ranges::find_if(
                        replicas,
                        [targetNodeId](const std::shared_ptr<ReplicaState>& replica) {
                            return !replica->dead.load() && replica->phase == ReplicaPhase::LIVE && replica->nodeId == targetNodeId;
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
                            if (!replica->dead.load() && replica->phase == ReplicaPhase::LIVE) {
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
                            if (replica->dead.load() || replica->phase != ReplicaPhase::LIVE || replica->nodeId != targetNodeId) { continue; }
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
        SnapshotProvider snapshotProvider,
        std::shared_ptr<detail::TransferBudget> transferBudget
    ) {
        if (replPort == 0 || selfNodeId == 0) { throw std::invalid_argument("ReplicationServer: nonzero port and node id are required"); }
        runtimeOptions.secure.validatePins();
        if (runtimeOptions.transportMode == TransportMode::SECURE) {
            if (runtimeOptions.secure.pinnedPeers.empty()) { throw std::invalid_argument("ReplicationServer: SECURE requires peer pins"); }
            runtimeOptions.secure.validatePins(configuredReplicaNodeIds);
        }
        if (!transferBudget) { transferBudget = std::make_shared<detail::TransferBudget>(runtimeOptions.transfer); }
        auto impl = std::make_unique<Impl>();
        impl->transferBudget = std::move(transferBudget);
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
    void ReplicationServer::setStripeControlCallback(StripeControlCallback callback) {
        impl_->setStripeControlCallback(std::move(callback));
    }

    void ReplicationServer::shipEntry(
        uint64_t seq,
        ReplOpType op,
        std::span<const uint8_t> key,
        std::span<const uint8_t> value,
        uint8_t recordFlags,
        uint64_t sourceNodeId
    ) {
        impl_->enqueueWire(seq, detail::entryMessage(seq, sourceNodeId, op, recordFlags, key, value, impl_->transferBudget), true);
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
        impl_->enqueueWireTo(targetNodeId, seq, detail::entryMessage(seq, sourceNodeId, op, recordFlags, key, value, impl_->transferBudget), waitForAck);
    }

    void ReplicationServer::shipBlob(uint64_t seq, uint64_t blobId, std::span<const uint8_t> content) {
        impl_->enqueueWire(0, detail::blobMessage(seq, blobId, content, impl_->transferBudget), false);
    }

    size_t ReplicationServer::replicaCount() const noexcept {
        std::lock_guard lock{impl_->replicasMutex};
        size_t count = 0;
        for (const auto& replica : impl_->replicas) {
            if (!replica->dead.load() && replica->phase == ReplicaPhase::LIVE) { ++count; }
        }
        return count;
    }

    ReplicationServer::Stats ReplicationServer::stats() const noexcept {
        Stats out;
        std::lock_guard lock{impl_->replicasMutex};
        out.peers.reserve(std::max(impl_->configuredReplicaNodeIds.size(), impl_->lastSuccessfulContactByNode.size()));
        const auto findPeer = [&](uint64_t nodeId) {
            return std::ranges::find(out.peers, nodeId, &Stats::Peer::nodeId);
        };
        for (const auto nodeId : impl_->configuredReplicaNodeIds) {
            const auto retained = impl_->lastSuccessfulContactByNode.find(nodeId);
            out.peers.push_back(Stats::Peer{
                .nodeId = nodeId,
                .lastSuccessfulContactAtUs = retained == impl_->lastSuccessfulContactByNode.end() ? 0 : retained->second,
            });
        }
        for (const auto& [nodeId, contact] : impl_->lastSuccessfulContactByNode) {
            if (findPeer(nodeId) == out.peers.end()) {
                out.peers.push_back(Stats::Peer{.nodeId = nodeId, .lastSuccessfulContactAtUs = contact});
            }
        }
        for (const auto& replica : impl_->replicas) {
            if (replica->dead.load()) { continue; }
            auto peer = findPeer(replica->nodeId);
            if (peer == out.peers.end()) {
                out.peers.push_back(Stats::Peer{.nodeId = replica->nodeId});
                peer = std::prev(out.peers.end());
            }
            if (replica->phase == ReplicaPhase::LIVE) {
                ++out.connectedReplicas;
                peer->connected = true;
            }
            peer->lastSuccessfulContactAtUs = std::max(
                peer->lastSuccessfulContactAtUs,
                replica->lastSuccessfulContactAtUs.load(std::memory_order_relaxed)
            );
            std::lock_guard queueLock{replica->queueMutex};
            out.queuedFrames += replica->queue.size();
            out.queuedBytes += replica->queuedBytes;
        }
        return out;
    }
} // namespace akkaradb::engine::cluster

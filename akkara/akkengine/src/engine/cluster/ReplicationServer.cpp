/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

// akkengine/src/engine/cluster/ReplicationServer.cpp
#include "akk/engine/cluster/ReplicationServer.hpp"
#include "akk/crypto/SecureChannel.hpp"

#include <array>
#include <atomic>
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
        #ifdef _WIN32
        using SocketHandle = SOCKET; constexpr SocketHandle BAD_SOCKET = INVALID_SOCKET; void shutdownSocket(SocketHandle s) noexcept {
            if (s != BAD_SOCKET) { ::shutdown(s, SD_BOTH); }
        } void closeSocket(SocketHandle s) noexcept { if (s != BAD_SOCKET) { ::closesocket(s); } } bool socketOk(SocketHandle s) noexcept {
            return s != INVALID_SOCKET;
        } void netInit() {
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
        using SocketHandle = int;
        constexpr SocketHandle BAD_SOCKET = -1;
        void shutdownSocket(SocketHandle s) noexcept { if (s >= 0) { ::shutdown(s, SHUT_RDWR); } }
        void closeSocket(SocketHandle s) noexcept { if (s >= 0) { ::close(s); } }
        bool socketOk(SocketHandle s) noexcept { return s >= 0; }
        void netInit() {}
        #endif

        bool sendAll(SocketHandle s, const uint8_t* data, size_t size) {
            size_t sent = 0;
            while (sent < size) {
                #ifdef _WIN32
                const int rc = ::send(s, reinterpret_cast<const char*>(data + sent), static_cast<int>(size - sent), 0);
                #else
                const ssize_t rc = ::send(s, data + sent, size - sent, 0);
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
                #else
                const ssize_t rc = ::recv(s, data + got, size - got, 0);
                #endif
                if (rc <= 0) { return false; }
                got += static_cast<size_t>(rc);
            }
            return true;
        }

        uint32_t readU32(const uint8_t* b) noexcept {
            return static_cast<uint32_t>(b[0]) | (static_cast<uint32_t>(b[1]) << 8) | (static_cast<uint32_t>(b[2]) << 16) | (static_cast<
                uint32_t>(b[3]) << 24);
        }

        bool recvFrame(SocketHandle s, DecodedFrame& out) {
            uint8_t header[ReplFrameHeader::SIZE];
            if (!recvAll(s, header, sizeof(header))) { return false; }
            const uint32_t payloadLen = readU32(header + 6);
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
        constexpr uint32_t SECURE_MAX_CIPHERTEXT_SIZE = 128u * 1024u * 1024u;

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
            std::atomic<bool> dead{false};
            std::mutex queueMutex;
            std::condition_variable queueCv;
            std::deque<std::vector<uint8_t>> queue;
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
            AckPolicy ackPolicy;
            ClusterRuntimeOptions runtimeOptions;
            crypto::NodeIdentity localIdentity;

            SocketHandle listenSock = BAD_SOCKET;
            std::atomic<bool> running{false};
            std::thread acceptThread;

            mutable std::mutex replicasMutex;
            std::vector<std::shared_ptr<ReplicaState>> replicas;
            std::deque<BufferedWire> entryBuffer;

            std::mutex ackMutex;
            std::condition_variable ackCv;

            void start() {
                listenSock = listenOn(runtimeOptions.replBindHost, replPort);
                running.store(true);
                acceptThread = std::thread([this] { acceptLoop(); });
            }

            void close() {
                running.store(false);
                shutdownSocket(listenSock);
                closeSocket(listenSock);
                listenSock = BAD_SOCKET;

                std::vector<std::shared_ptr<ReplicaState>> copy;
                {
                    std::lock_guard lock{replicasMutex};
                    copy = replicas;
                    replicas.clear();
                }
                for (auto& replica : copy) {
                    replica->dead.store(true);
                    closeReplica(replica);
                    replica->queueCv.notify_all();
                }
                if (acceptThread.joinable()) { acceptThread.join(); }
                for (auto& replica : copy) {
                    if (replica->sendThread.joinable()) { replica->sendThread.join(); }
                    if (replica->recvThread.joinable()) { replica->recvThread.join(); }
                }
            }

            void acceptLoop() {
                while (running.load()) {
                    SocketHandle client = ::accept(listenSock, nullptr, nullptr);
                    if (!socketOk(client)) { break; }

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
                        closeSocket(client);
                        continue;
                    }
                    if (hello.role != NodeRole::REPLICA) {
                        closeSocket(client);
                        continue;
                    }
                    if (secure) {
                        if (const auto expected = pinnedPeerKey(runtimeOptions, hello.nodeId); expected && secureRemotePublicKey != *
                            expected) {
                            closeSocket(client);
                            continue;
                        }
                    }

                    ServerHello response{};
                    response.nodeId = selfNodeId;
                    response.currentSeq = getCurrentSeq ? getCurrentSeq() : 0;
                    response.role = NodeRole::PRIMARY;
                    auto helloWire = encodeServerHello(response);
                    if (!sendTo(client, secure.get(), helloWire.data(), helloWire.size())) {
                        closeSocket(client);
                        continue;
                    }

                    auto replica = std::make_shared<ReplicaState>();
                    replica->sock = client;
                    replica->secure = std::move(secure);
                    replica->nodeId = hello.nodeId;
                    replica->lastAckedSeq.store(hello.lastSeq);

                    {
                        std::lock_guard lock{replicasMutex};
                        for (const auto& buffered : entryBuffer) {
                            if (buffered.seq == 0 || buffered.seq > hello.lastSeq) { replica->queue.push_back(buffered.wire); }
                        }
                        replicas.push_back(replica);
                    }
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

            static void closeReplica(const std::shared_ptr<ReplicaState>& replica) {
                shutdownSocket(replica->sock);
                closeSocket(replica->sock);
                replica->sock = BAD_SOCKET;
            }

            void sendLoop(const std::shared_ptr<ReplicaState>& replica) {
                while (!replica->dead.load()) {
                    std::vector<uint8_t> wire;
                    {
                        std::unique_lock lock{replica->queueMutex};
                        replica->queueCv.wait(lock, [&] { return replica->dead.load() || !replica->queue.empty(); });
                        if (replica->dead.load() && replica->queue.empty()) { break; }
                        wire = std::move(replica->queue.front());
                        replica->queue.pop_front();
                    }
                    if (!sendTo(replica->sock, replica->secure.get(), wire.data(), wire.size())) {
                        replica->dead.store(true);
                        closeReplica(replica);
                        break;
                    }
                }
            }

            void recvLoop(const std::shared_ptr<ReplicaState>& replica) {
                while (!replica->dead.load()) {
                    DecodedFrame frame;
                    if (!recvFrameFrom(replica->sock, replica->secure.get(), frame)) { break; }
                    if (frame.type != ReplMsgType::ACK) { break; }
                    ReplAck ack;
                    if (!decodeAck(frame.payload, ack)) { break; }
                    replica->lastAckedSeq.store(ack.seq);
                    ackCv.notify_all();
                }
                replica->dead.store(true);
                closeReplica(replica);
                replica->queueCv.notify_all();
            }

            void enqueueWire(uint64_t seq, std::vector<uint8_t> wire) {
                {
                    std::lock_guard lock{replicasMutex};
                    entryBuffer.push_back(BufferedWire{seq, wire});
                    while (entryBuffer.size() > ENTRY_BUFFER_SIZE) { entryBuffer.pop_front(); }
                    for (auto& replica : replicas) {
                        if (!replica->dead.load()) {
                            std::lock_guard qlock{replica->queueMutex};
                            replica->queue.push_back(wire);
                            replica->queueCv.notify_one();
                        }
                    }
                }
                waitForAcks(seq);
            }

            void waitForAcks(uint64_t seq) {
                if (ackPolicy.mode == AckPolicyMode::ASYNC || seq == 0) { return; }
                const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
                while (std::chrono::steady_clock::now() < deadline) {
                    size_t live = 0;
                    size_t acked = 0;
                    {
                        std::lock_guard lock{replicasMutex};
                        for (const auto& replica : replicas) {
                            if (!replica->dead.load()) {
                                ++live;
                                if (replica->lastAckedSeq.load() >= seq) { ++acked; }
                            }
                        }
                    }

                    const bool ok = ackPolicy.mode == AckPolicyMode::ALL ? acked >= live : acked >= ackPolicy.quorum;
                    if (ok) { return; }

                    std::unique_lock lock{ackMutex};
                    ackCv.wait_for(lock, std::chrono::milliseconds(50));
                }
            }
    };

    ReplicationServer::ReplicationServer(std::unique_ptr<Impl> impl) : impl_{std::move(impl)} {}
    ReplicationServer::~ReplicationServer() { close(); }

    std::unique_ptr<ReplicationServer> ReplicationServer::create(
        uint16_t replPort,
        uint64_t selfNodeId,
        std::function<uint64_t()> getCurrentSeq,
        AckPolicy ackPolicy,
        ClusterRuntimeOptions runtimeOptions
    ) {
        auto impl = std::make_unique<Impl>();
        impl->replPort = replPort;
        impl->selfNodeId = selfNodeId;
        impl->getCurrentSeq = std::move(getCurrentSeq);
        impl->ackPolicy = ackPolicy;
        impl->runtimeOptions = std::move(runtimeOptions);
        if (impl->runtimeOptions.transportMode == TransportMode::SECURE) { impl->localIdentity = loadSecureIdentity(impl->runtimeOptions); }
        return std::unique_ptr < ReplicationServer > (new ReplicationServer(std::move(impl)));
    }

    void ReplicationServer::start() { impl_->start(); }
    void ReplicationServer::close() { if (impl_) { impl_->close(); } }

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
        impl_->enqueueWire(seq, encodeEntry(entry));
    }

    void ReplicationServer::shipBlob(uint64_t seq, uint64_t blobId, std::span<const uint8_t> content) {
        ReplBlob blob;
        blob.seq = seq;
        blob.blobId = blobId;
        blob.content.assign(content.begin(), content.end());
        impl_->enqueueWire(0, encodeBlob(blob));
    }

    size_t ReplicationServer::replicaCount() const noexcept {
        std::lock_guard lock{impl_->replicasMutex};
        size_t count = 0;
        for (const auto& replica : impl_->replicas) { if (!replica->dead.load()) { ++count; } }
        return count;
    }
} // namespace akkaradb::engine::cluster

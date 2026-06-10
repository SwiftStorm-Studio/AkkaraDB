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
#include "akk/net/tls/TlsStream.hpp"

#include <atomic>
#include <chrono>
#include <cstring>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

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

        bool sendAll(net::TlsStream& stream, const uint8_t* data, size_t size) {
            size_t sent = 0;
            while (sent < size) { sent += stream.send(data + sent, size - sent); }
            return true;
        }

        bool recvAll(net::TlsStream& stream, uint8_t* data, size_t size) {
            size_t got = 0;
            while (got < size) { got += stream.recv(data + got, size - got); }
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

        bool recvFrame(net::TlsStream& stream, DecodedFrame& out) {
            uint8_t header[ReplFrameHeader::SIZE];
            try {
                if (!recvAll(stream, header, sizeof(header))) { return false; }
                const uint32_t payloadLen = readU32(header + 6);
                std::vector<uint8_t> wire(sizeof(header) + payloadLen);
                std::memcpy(wire.data(), header, sizeof(header));
                if (payloadLen > 0 && !recvAll(stream, wire.data() + sizeof(header), payloadLen)) { return false; }
                return decodeFrame(wire, out);
            }
            catch (...) { return false; }
        }

        struct TlsConfigStorage {
            std::string certPath;
            std::string keyPath;
            std::string caPath;
            net::TlsConfig config{};
        };

        TlsConfigStorage makeTlsConfig(const ClusterRuntimeOptions& options) {
            TlsConfigStorage storage;
            storage.certPath = options.tls.certPath.string();
            storage.keyPath = options.tls.keyPath.string();
            storage.caPath = options.tls.caPath.string();
            storage.config.certPath = storage.certPath.empty() ? nullptr : storage.certPath.c_str();
            storage.config.keyPath = storage.keyPath.empty() ? nullptr : storage.keyPath.c_str();
            storage.config.caPath = storage.caPath.empty() ? nullptr : storage.caPath.c_str();
            storage.config.verifyPeer = options.tls.verifyPeer;
            return storage;
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
            std::unique_ptr<net::TlsStream> tls;
            uint64_t nodeId = 0;
            std::atomic<uint64_t> lastAckedSeq{0};
            std::atomic<bool> dead{false};
            std::mutex queueMutex;
            std::condition_variable queueCv;
            std::deque<std::vector<uint8_t>> queue;
            std::thread sendThread;
            std::thread recvThread;

            ~ReplicaState() {
                if (tls) { tls->close(); }
                closeSocket(sock);
            }
        };
    } // namespace

    class ReplicationServer::Impl {
        public:
            uint16_t replPort = 0;
            uint64_t selfNodeId = 0;
            std::function<uint64_t()> getCurrentSeq;
            AckPolicy ackPolicy;
            ClusterRuntimeOptions runtimeOptions;

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

                    std::unique_ptr<net::TlsStream> tls;
                    if (runtimeOptions.transportMode == TransportMode::TLS) {
                        try {
                            auto storage = makeTlsConfig(runtimeOptions);
                            tls = std::make_unique<net::TlsStream>();
                            tls->accept(static_cast<std::uintptr_t>(client), storage.config);
                            client = BAD_SOCKET;
                        }
                        catch (...) {
                            closeSocket(client);
                            continue;
                        }
                    }

                    DecodedFrame frame;
                    ClientHello hello;
                    if (!recvFrameFrom(client, tls.get(), frame) || frame.type != ReplMsgType::CLIENT_HELLO || !decodeClientHello(
                        frame.payload,
                        hello
                    )) {
                        if (tls) { tls->close(); }
                        closeSocket(client);
                        continue;
                    }

                    ServerHello response{};
                    response.nodeId = selfNodeId;
                    response.currentSeq = getCurrentSeq ? getCurrentSeq() : 0;
                    response.role = NodeRole::PRIMARY;
                    auto helloWire = encodeServerHello(response);
                    if (!sendTo(client, tls.get(), helloWire.data(), helloWire.size())) {
                        if (tls) { tls->close(); }
                        closeSocket(client);
                        continue;
                    }

                    auto replica = std::make_shared<ReplicaState>();
                    replica->sock = client;
                    replica->tls = std::move(tls);
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

            static bool sendTo(SocketHandle sock, net::TlsStream* tls, const uint8_t* data, size_t size) {
                try { return tls != nullptr ? sendAll(*tls, data, size) : sendAll(sock, data, size); }
                catch (...) { return false; }
            }

            static bool recvFrameFrom(SocketHandle sock, net::TlsStream* tls, DecodedFrame& out) {
                return tls != nullptr ? recvFrame(*tls, out) : recvFrame(sock, out);
            }

            static void closeReplica(const std::shared_ptr<ReplicaState>& replica) {
                if (replica->tls) { replica->tls->shutdown(); }
                else {
                    shutdownSocket(replica->sock);
                    closeSocket(replica->sock);
                }
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
                    if (!sendTo(replica->sock, replica->tls.get(), wire.data(), wire.size())) {
                        replica->dead.store(true);
                        closeReplica(replica);
                        break;
                    }
                }
            }

            void recvLoop(const std::shared_ptr<ReplicaState>& replica) {
                while (!replica->dead.load()) {
                    DecodedFrame frame;
                    if (!recvFrameFrom(replica->sock, replica->tls.get(), frame)) { break; }
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
        return std::unique_ptr<ReplicationServer>(new ReplicationServer(std::move(impl)));
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

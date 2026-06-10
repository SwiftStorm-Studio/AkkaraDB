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

// akkengine/src/engine/cluster/ReplicationClient.cpp
#include "akk/engine/cluster/ReplicationClient.hpp"
#include "akk/net/tls/TlsStream.hpp"

#include <atomic>
#include <chrono>
#include <cstring>
#include <mutex>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace akkaradb::engine::cluster {
    namespace {
        #ifdef _WIN32
        using SocketHandle = SOCKET;
        constexpr SocketHandle INVALID_SOCKET_HANDLE = INVALID_SOCKET;
        void shutdownSocket(SocketHandle s) noexcept { if (s != INVALID_SOCKET_HANDLE) { ::shutdown(s, SD_BOTH); } }
        #else
        using SocketHandle = int; constexpr SocketHandle INVALID_SOCKET_HANDLE = -1; void shutdownSocket(SocketHandle s) noexcept {
            if (s >= 0) { ::shutdown(s, SHUT_RDWR); }
        }
        #endif

        void ensureSocketRuntime() {
            #ifdef _WIN32
            static std::once_flag once;
            std::call_once(
                once,
                [] {
                    WSADATA data{};
                    if (::WSAStartup(MAKEWORD(2, 2), &data) != 0) { throw std::runtime_error("ReplicationClient: WSAStartup failed"); }
                }
            );
            #endif
        }

        void closeSocket(SocketHandle s) noexcept {
            if (s == INVALID_SOCKET_HANDLE) { return; }
            #ifdef _WIN32
            ::closesocket(s);
            #else
            ::close(s);
            #endif
        }

        bool sendAll(SocketHandle s, const uint8_t* data, size_t size) {
            size_t sent = 0;
            while (sent < size) {
                #ifdef _WIN32
                const int n = ::send(s, reinterpret_cast<const char*>(data + sent), static_cast<int>(size - sent), 0);
                #else
                const ssize_t n = ::send(s, data + sent, size - sent, 0);
                #endif
                if (n <= 0) { return false; }
                sent += static_cast<size_t>(n);
            }
            return true;
        }

        bool recvAll(SocketHandle s, uint8_t* data, size_t size) {
            size_t received = 0;
            while (received < size) {
                #ifdef _WIN32
                const int n = ::recv(s, reinterpret_cast<char*>(data + received), static_cast<int>(size - received), 0);
                #else
                const ssize_t n = ::recv(s, data + received, size - received, 0);
                #endif
                if (n <= 0) { return false; }
                received += static_cast<size_t>(n);
            }
            return true;
        }

        bool sendAll(net::TlsStream& stream, const uint8_t* data, size_t size) {
            size_t sent = 0;
            while (sent < size) { sent += stream.send(data + sent, size - sent); }
            return true;
        }

        bool recvAll(net::TlsStream& stream, uint8_t* data, size_t size) {
            size_t received = 0;
            while (received < size) { received += stream.recv(data + received, size - received); }
            return true;
        }

        bool recvFrame(SocketHandle s, DecodedFrame& out) {
            uint8_t header[ReplFrameHeader::SIZE];
            if (!recvAll(s, header, sizeof(header))) { return false; }

            const uint32_t payloadLen = static_cast<uint32_t>(header[6]) | (static_cast<uint32_t>(header[7]) << 8) | (static_cast<uint32_t>(
                header[8]) << 16) | (static_cast<uint32_t>(header[9]) << 24);

            std::vector<uint8_t> wire(sizeof(header) + payloadLen);
            std::memcpy(wire.data(), header, sizeof(header));
            if (payloadLen > 0 && !recvAll(s, wire.data() + sizeof(header), payloadLen)) { return false; }
            return decodeFrame(wire, out);
        }

        bool recvFrame(net::TlsStream& stream, DecodedFrame& out) {
            uint8_t header[ReplFrameHeader::SIZE];
            try {
                if (!recvAll(stream, header, sizeof(header))) { return false; }

                const uint32_t payloadLen = static_cast<uint32_t>(header[6]) | (static_cast<uint32_t>(header[7]) << 8) | (static_cast<
                    uint32_t>(header[8]) << 16) | (static_cast<uint32_t>(header[9]) << 24);

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

        SocketHandle connectTo(const std::string& host, uint16_t port) {
            ensureSocketRuntime();

            addrinfo hints{};
            hints.ai_family = AF_UNSPEC;
            hints.ai_socktype = SOCK_STREAM;

            addrinfo* result = nullptr;
            const auto portText = std::to_string(port);
            if (::getaddrinfo(host.c_str(), portText.c_str(), &hints, &result) != 0) { return INVALID_SOCKET_HANDLE; }

            SocketHandle socket = INVALID_SOCKET_HANDLE;
            for (addrinfo* it = result; it != nullptr; it = it->ai_next) {
                socket = ::socket(it->ai_family, it->ai_socktype, it->ai_protocol);
                if (socket == INVALID_SOCKET_HANDLE) { continue; }
                if (::connect(socket, it->ai_addr, static_cast<int>(it->ai_addrlen)) == 0) { break; }
                closeSocket(socket);
                socket = INVALID_SOCKET_HANDLE;
            }

            ::freeaddrinfo(result);
            return socket;
        }
    } // namespace

    class ReplicationClient::Impl {
        public:
            Impl(
                std::string primaryHost,
                uint16_t primaryReplPort,
                uint64_t selfNodeId,
                std::function<uint64_t()> getLastSeq,
                ClusterRuntimeOptions runtimeOptions
            )
                : primaryHost_{std::move(primaryHost)},
                  primaryReplPort_{primaryReplPort},
                  selfNodeId_{selfNodeId},
                  getLastSeq_{std::move(getLastSeq)},
                  runtimeOptions_{std::move(runtimeOptions)} {}

            ~Impl() { close(); }

            void setApplyCallback(ApplyCallback callback) {
                std::lock_guard lock{callbackMutex_};
                applyCallback_ = std::move(callback);
            }

            void setBlobCallback(BlobCallback callback) {
                std::lock_guard lock{callbackMutex_};
                blobCallback_ = std::move(callback);
            }

            void start() {
                if (running_.exchange(true)) { return; }
                worker_ = std::thread([this] { run(); });
            }

            void close() {
                running_ = false;
                {
                    std::lock_guard lock{socketMutex_};
                    if (tls_) { tls_->shutdown(); }
                    shutdownSocket(socket_);
                    closeSocket(socket_);
                    socket_ = INVALID_SOCKET_HANDLE;
                }
                if (worker_.joinable()) { worker_.join(); }
                {
                    std::lock_guard lock{socketMutex_};
                    if (tls_) {
                        tls_->close();
                        tls_.reset();
                    }
                }
                connected_ = false;
            }

            bool connected() const noexcept { return connected_; }

        private:
            void run() {
                while (running_) {
                    SocketHandle socket = INVALID_SOCKET_HANDLE;
                    net::TlsStream* tls = nullptr;

                    if (runtimeOptions_.transportMode == TransportMode::TLS) {
                        try {
                            auto storage = makeTlsConfig(runtimeOptions_);
                            auto stream = std::make_unique<net::TlsStream>();
                            stream->connect(primaryHost_.c_str(), primaryReplPort_, storage.config);
                            tls = stream.get();
                            std::lock_guard lock{socketMutex_};
                            tls_ = std::move(stream);
                        }
                        catch (...) {
                            std::this_thread::sleep_for(std::chrono::milliseconds(200));
                            continue;
                        }
                    }
                    else {
                        socket = connectTo(primaryHost_, primaryReplPort_);
                        if (socket == INVALID_SOCKET_HANDLE) {
                            std::this_thread::sleep_for(std::chrono::milliseconds(200));
                            continue;
                        }

                        {
                            std::lock_guard lock{socketMutex_};
                            socket_ = socket;
                        }
                    }

                    if (handshake(socket, tls)) {
                        connected_ = true;
                        receiveLoop(socket, tls);
                    }

                    connected_ = false;
                    {
                        std::lock_guard lock{socketMutex_};
                        if (tls_ && tls_.get() == tls) {
                            tls_->close();
                            tls_.reset();
                        }
                        if (socket_ == socket) { socket_ = INVALID_SOCKET_HANDLE; }
                    }
                    closeSocket(socket);

                    if (running_) { std::this_thread::sleep_for(std::chrono::milliseconds(200)); }
                }
            }

            bool handshake(SocketHandle socket, net::TlsStream* tls) {
                const ClientHello hello{.nodeId = selfNodeId_, .lastSeq = getLastSeq_ ? getLastSeq_() : 0, .role = NodeRole::REPLICA,};
                const auto wire = encodeClientHello(hello);
                if (!sendTo(socket, tls, wire.data(), wire.size())) { return false; }

                DecodedFrame frame;
                if (!recvFrameFrom(socket, tls, frame) || frame.type != ReplMsgType::SERVER_HELLO) { return false; }
                ServerHello serverHello;
                return decodeServerHello(frame.payload, serverHello);
            }

            static bool sendTo(SocketHandle socket, net::TlsStream* tls, const uint8_t* data, size_t size) {
                try { return tls != nullptr ? sendAll(*tls, data, size) : sendAll(socket, data, size); }
                catch (...) { return false; }
            }

            static bool recvFrameFrom(SocketHandle socket, net::TlsStream* tls, DecodedFrame& frame) {
                return tls != nullptr ? recvFrame(*tls, frame) : recvFrame(socket, frame);
            }

            void receiveLoop(SocketHandle socket, net::TlsStream* tls) {
                while (running_) {
                    DecodedFrame frame;
                    if (!recvFrameFrom(socket, tls, frame)) { return; }

                    if (frame.type == ReplMsgType::ENTRY) {
                        ReplEntry entry;
                        if (!decodeEntry(frame.payload, entry)) { return; }
                        ApplyCallback callback;
                        {
                            std::lock_guard lock{callbackMutex_};
                            callback = applyCallback_;
                        }
                        if (callback) { callback(entry.seq, entry.op, entry.key, entry.value, entry.recordFlags, entry.sourceNodeId); }
                        const auto ack = encodeAck(ReplAck{.seq = entry.seq});
                        if (!sendTo(socket, tls, ack.data(), ack.size())) { return; }
                    }
                    else if (frame.type == ReplMsgType::BLOB_PUT) {
                        ReplBlob blob;
                        if (!decodeBlob(frame.payload, blob)) { return; }
                        BlobCallback callback;
                        {
                            std::lock_guard lock{callbackMutex_};
                            callback = blobCallback_;
                        }
                        if (callback) { callback(blob.seq, blob.blobId, blob.content); }
                    }
                    else if (frame.type != ReplMsgType::READ_RESPONSE) { return; }
                }
            }

            std::string primaryHost_;
            uint16_t primaryReplPort_;
            uint64_t selfNodeId_;
            std::function<uint64_t()> getLastSeq_;
            ClusterRuntimeOptions runtimeOptions_;

            std::atomic<bool> running_{false};
            std::atomic<bool> connected_{false};
            std::thread worker_;

            mutable std::mutex socketMutex_;
            SocketHandle socket_ = INVALID_SOCKET_HANDLE;
            std::unique_ptr<net::TlsStream> tls_;

            mutable std::mutex callbackMutex_;
            ApplyCallback applyCallback_;
            BlobCallback blobCallback_;
    };

    std::unique_ptr<ReplicationClient> ReplicationClient::create(
        std::string primaryHost,
        uint16_t primaryReplPort,
        uint64_t selfNodeId,
        std::function<uint64_t()> getLastSeq,
        ClusterRuntimeOptions runtimeOptions
    ) {
        return std::unique_ptr<ReplicationClient>(
            new ReplicationClient(
                std::make_unique<Impl>(
                    std::move(primaryHost),
                    primaryReplPort,
                    selfNodeId,
                    std::move(getLastSeq),
                    std::move(runtimeOptions)
                )
            )
        );
    }

    ReplicationClient::ReplicationClient(std::unique_ptr<Impl> impl) : impl_{std::move(impl)} {}

    ReplicationClient::~ReplicationClient() = default;

    void ReplicationClient::setApplyCallback(ApplyCallback callback) { impl_->setApplyCallback(std::move(callback)); }

    void ReplicationClient::setBlobCallback(BlobCallback callback) { impl_->setBlobCallback(std::move(callback)); }

    void ReplicationClient::start() { impl_->start(); }

    void ReplicationClient::close() { impl_->close(); }

    bool ReplicationClient::connected() const noexcept { return impl_->connected(); }
} // namespace akkaradb::engine::cluster

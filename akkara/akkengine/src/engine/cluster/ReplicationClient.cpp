/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/cluster/ReplicationClient.cpp
#include "akk/engine/cluster/ReplicationClient.hpp"
#include "akk/crypto/SecureChannel.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <mutex>
#include <memory>
#include <optional>
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
        using SocketHandle = SOCKET; constexpr SocketHandle INVALID_SOCKET_HANDLE = INVALID_SOCKET; void shutdownSocket(
            SocketHandle s
        ) noexcept { if (s != INVALID_SOCKET_HANDLE) { ::shutdown(s, SD_BOTH); } }
        #else
        using SocketHandle = int;
        constexpr SocketHandle INVALID_SOCKET_HANDLE = -1;
        void shutdownSocket(SocketHandle s) noexcept { if (s >= 0) { ::shutdown(s, SHUT_RDWR); } }
        #endif

        void ensureSocketRuntime() {
            #ifdef _WIN32
            static std::once_flag once; std::call_once(
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

        bool recvFrame(SocketHandle s, DecodedFrame& out) {
            uint8_t header[ReplFrameHeader::SIZE];
            if (!recvAll(s, header, sizeof(header))) { return false; }

            const uint32_t payloadLen = static_cast<uint32_t>(header[6]) | (static_cast<uint32_t>(header[7]) << 8) | (static_cast<uint32_t>(
                header[8]) << 16) | (static_cast<uint32_t>(header[9]) << 24);
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
        constexpr uint32_t SECURE_MAX_CIPHERTEXT_SIZE = ReplFrameHeader::MAX_PAYLOAD_SIZE;

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

        bool writeSecureClientHello(SocketHandle socket, const crypto::ClientHello& hello) {
            std::array<uint8_t, SECURE_CLIENT_HELLO_SIZE> wire{};
            writeU32Le(wire.data(), SECURE_HELLO_MAGIC);
            wire[4] = SECURE_VERSION;
            wire[5] = SECURE_CLIENT_HELLO;
            std::memcpy(wire.data() + SECURE_HELLO_HEADER_SIZE, hello.staticPublicKey.data(), hello.staticPublicKey.size());
            std::memcpy(
                wire.data() + SECURE_HELLO_HEADER_SIZE + hello.staticPublicKey.size(),
                hello.ephemeralPublicKey.data(),
                hello.ephemeralPublicKey.size()
            );
            return sendAll(socket, wire.data(), wire.size());
        }

        bool readSecureServerHello(SocketHandle socket, crypto::ServerHello& hello) {
            std::array<uint8_t, SECURE_SERVER_HELLO_SIZE> wire{};
            if (!recvAll(socket, wire.data(), wire.size())) { return false; }
            if (readU32Le(wire.data()) != SECURE_HELLO_MAGIC || wire[4] != SECURE_VERSION || wire[5] != SECURE_SERVER_HELLO) {
                return false;
            }
            std::memcpy(hello.staticPublicKey.data(), wire.data() + SECURE_HELLO_HEADER_SIZE, hello.staticPublicKey.size());
            std::memcpy(
                hello.ephemeralPublicKey.data(),
                wire.data() + SECURE_HELLO_HEADER_SIZE + hello.staticPublicKey.size(),
                hello.ephemeralPublicKey.size()
            );
            std::memcpy(
                hello.authenticator.data(),
                wire.data() + SECURE_HELLO_HEADER_SIZE + hello.staticPublicKey.size() + hello.ephemeralPublicKey.size(),
                hello.authenticator.size()
            );
            return true;
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
                AckPolicy ackPolicy,
                ClusterRuntimeOptions runtimeOptions
            )
                : primaryHost_{std::move(primaryHost)},
                  primaryReplPort_{primaryReplPort},
                  selfNodeId_{selfNodeId},
                  getLastSeq_{std::move(getLastSeq)},
                  ackPolicy_{ackPolicy},
                  runtimeOptions_{std::move(runtimeOptions)},
                  localIdentity_{
                      runtimeOptions_.transportMode == TransportMode::SECURE ? loadSecureIdentity(runtimeOptions_) : crypto::NodeIdentity{}
                  } {}

            ~Impl() { close(); }

            void setApplyCallback(ApplyCallback callback) {
                std::lock_guard lock{callbackMutex_};
                applyCallback_ = std::move(callback);
            }

            void setBlobCallback(BlobCallback callback) {
                std::lock_guard lock{callbackMutex_};
                blobCallback_ = std::move(callback);
            }

            void setForceDurableCallback(std::function<void()> callback) {
                std::lock_guard lock{callbackMutex_};
                forceDurableCallback_ = std::move(callback);
            }

            void setSnapshotCallbacks(SnapshotBeginCallback begin, SnapshotEntryCallback entry, SnapshotEndCallback end) {
                std::lock_guard lock{callbackMutex_};
                snapshotBeginCallback_ = std::move(begin);
                snapshotEntryCallback_ = std::move(entry);
                snapshotEndCallback_ = std::move(end);
            }

            void start() {
                if (running_.exchange(true)) { return; }
                worker_ = std::thread([this] { run(); });
            }

            void close() {
                running_ = false;
                {
                    std::lock_guard lock{socketMutex_};
                    shutdownSocket(socket_);
                    closeSocket(socket_);
                    socket_ = INVALID_SOCKET_HANDLE;
                }
                if (worker_.joinable()) { worker_.join(); }
                connected_ = false;
            }

            bool connected() const noexcept { return connected_; }

        private:
            void run() {
                while (running_) {
                    SocketHandle socket = INVALID_SOCKET_HANDLE;
                    std::unique_ptr<crypto::SecureSession> secure;
                    crypto::PublicKey secureRemotePublicKey{};

                    socket = connectTo(primaryHost_, primaryReplPort_);
                    if (socket == INVALID_SOCKET_HANDLE) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(200));
                        continue;
                    }

                    {
                        std::lock_guard lock{socketMutex_};
                        socket_ = socket;
                    }

                    if (runtimeOptions_.transportMode == TransportMode::SECURE) {
                        try {
                            auto opened = openSecureSession(socket);
                            secureRemotePublicKey = opened.remotePublicKey;
                            secure = std::make_unique<crypto::SecureSession>(std::move(opened.session));
                        }
                        catch (...) {
                            closeSocket(socket);
                            std::lock_guard lock{socketMutex_};
                            if (socket_ == socket) { socket_ = INVALID_SOCKET_HANDLE; }
                            std::this_thread::sleep_for(std::chrono::milliseconds(200));
                            continue;
                        }
                    }

                    if (handshake(socket, secure.get(), secureRemotePublicKey)) {
                        connected_ = true;
                        receiveLoop(socket, secure.get());
                    }

                    connected_ = false;
                    {
                        std::lock_guard lock{socketMutex_};
                        if (socket_ == socket) { socket_ = INVALID_SOCKET_HANDLE; }
                    }
                    closeSocket(socket);

                    if (running_) { std::this_thread::sleep_for(std::chrono::milliseconds(200)); }
                }
            }

            struct OpenSecureSession {
                crypto::SecureSession session;
                crypto::PublicKey remotePublicKey{};
            };

            OpenSecureSession openSecureSession(SocketHandle socket) {
                crypto::NoiseInitiator initiator{localIdentity_};
                if (!writeSecureClientHello(socket, initiator.hello())) {
                    throw std::runtime_error("ReplicationClient: secure hello send failed");
                }

                crypto::ServerHello serverHello{};
                if (!readSecureServerHello(socket, serverHello)) {
                    throw std::runtime_error("ReplicationClient: secure hello receive failed");
                }

                const auto expected = pinnedPeerKey(runtimeOptions_, runtimeOptions_.secure.expectedPrimaryNodeId);
                auto session = initiator.finish(serverHello, expected);
                return OpenSecureSession{std::move(session), serverHello.staticPublicKey};
            }

            bool handshake(SocketHandle socket, crypto::SecureSession* secure, const crypto::PublicKey& secureRemotePublicKey) {
                const ClientHello hello{.nodeId = selfNodeId_, .lastSeq = getLastSeq_ ? getLastSeq_() : 0, .role = NodeRole::REPLICA,};
                const auto wire = encodeClientHello(hello);
                if (!sendTo(socket, secure, wire.data(), wire.size())) { return false; }

                DecodedFrame frame;
                if (!recvFrameFrom(socket, secure, frame) || frame.type != ReplMsgType::SERVER_HELLO) { return false; }
                ServerHello serverHello;
                if (!decodeServerHello(frame.payload, serverHello) || serverHello.role != NodeRole::PRIMARY) { return false; }
                if (runtimeOptions_.secure.expectedPrimaryNodeId != 0 && serverHello.nodeId != runtimeOptions_.secure.
                    expectedPrimaryNodeId) { return false; }
                if (secure != nullptr) {
                    if (const auto expected = pinnedPeerKey(runtimeOptions_, serverHello.nodeId); expected && secureRemotePublicKey != *
                        expected) { return false; }
                }
                return true;
            }

            static bool sendTo(SocketHandle socket, crypto::SecureSession* secure, const uint8_t* data, size_t size) {
                try {
                    if (secure != nullptr) { return sendSecureFrame(socket, *secure, data, size); }
                    return sendAll(socket, data, size);
                }
                catch (...) { return false; }
            }

            static bool recvFrameFrom(SocketHandle socket, crypto::SecureSession* secure, DecodedFrame& frame) {
                if (secure != nullptr) { return recvSecureFrame(socket, *secure, frame); }
                return recvFrame(socket, frame);
            }

            bool sendAck(SocketHandle socket, crypto::SecureSession* secure, uint64_t seq, AckStage stage) {
                if (ackPolicy_.mode == AckPolicyMode::NONE || ackPolicy_.stage != stage) { return true; }
                const auto ack = encodeAck(ReplAck{.seq = seq, .stage = stage});
                return sendTo(socket, secure, ack.data(), ack.size());
            }

            void receiveLoop(SocketHandle socket, crypto::SecureSession* secure) {
                uint64_t expectedSeq = getLastSeq_ ? getLastSeq_() + 1 : 1;
                bool receivingSnapshot = false;
                uint64_t snapshotSeq = 0;
                while (running_) {
                    DecodedFrame frame;
                    if (!recvFrameFrom(socket, secure, frame)) { return; }

                    if (frame.type == ReplMsgType::ENTRY) {
                        ReplEntry entry;
                        if (!decodeEntry(frame.payload, entry)) { return; }
                        if (receivingSnapshot) { return; }
                        if (entry.seq < expectedSeq) {
                            if (!sendAck(socket, secure, entry.seq, AckStage::APPLIED)) { return; }
                            continue;
                        }
                        if (entry.seq != expectedSeq) { return; }
                        ApplyCallback applyCallback;
                        std::function < void() > forceDurableCallback;
                        {
                            std::lock_guard lock{callbackMutex_};
                            applyCallback = applyCallback_;
                            forceDurableCallback = forceDurableCallback_;
                        }
                        if (!sendAck(socket, secure, entry.seq, AckStage::RECEIVED)) { return; }
                        if (applyCallback) {
                            applyCallback(entry.seq, entry.op, entry.key, entry.value, entry.recordFlags, entry.sourceNodeId);
                        }
                        ++expectedSeq;
                        if (!sendAck(socket, secure, entry.seq, AckStage::APPLIED)) { return; }
                        if (ackPolicy_.mode != AckPolicyMode::NONE && ackPolicy_.stage == AckStage::DURABLE) {
                            if (forceDurableCallback) { forceDurableCallback(); }
                            if (!sendAck(socket, secure, entry.seq, AckStage::DURABLE)) { return; }
                        }
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
                    else if (frame.type == ReplMsgType::SNAPSHOT_BEGIN) {
                        ReplSnapshotBegin begin;
                        if (receivingSnapshot || !decodeSnapshotBegin(frame.payload, begin)) { return; }
                        SnapshotBeginCallback callback;
                        {
                            std::lock_guard lock{callbackMutex_};
                            callback = snapshotBeginCallback_;
                        }
                        if (!callback) { return; }
                        callback(begin.snapshotSeq, begin.entryCount);
                        receivingSnapshot = true;
                        snapshotSeq = begin.snapshotSeq;
                    }
                    else if (frame.type == ReplMsgType::SNAPSHOT_ENTRY) {
                        ReplSnapshotEntry entry;
                        if (!receivingSnapshot || !decodeSnapshotEntry(frame.payload, entry)) { return; }
                        SnapshotEntryCallback callback;
                        {
                            std::lock_guard lock{callbackMutex_};
                            callback = snapshotEntryCallback_;
                        }
                        if (!callback) { return; }
                        callback(entry.key, entry.value);
                    }
                    else if (frame.type == ReplMsgType::SNAPSHOT_END) {
                        uint64_t endSeq = 0;
                        if (!receivingSnapshot || !decodeSnapshotEnd(frame.payload, endSeq) || endSeq != snapshotSeq) { return; }
                        SnapshotEndCallback callback;
                        {
                            std::lock_guard lock{callbackMutex_};
                            callback = snapshotEndCallback_;
                        }
                        if (!callback) { return; }
                        callback(endSeq);
                        receivingSnapshot = false;
                        expectedSeq = endSeq + 1;
                    }
                    else if (frame.type == ReplMsgType::RESYNC_REQUIRED) { return; }
                    else if (frame.type != ReplMsgType::READ_RESPONSE) { return; }
                }
            }

            std::string primaryHost_;
            uint16_t primaryReplPort_;
            uint64_t selfNodeId_;
            std::function<uint64_t()> getLastSeq_;
            AckPolicy ackPolicy_;
            ClusterRuntimeOptions runtimeOptions_;
            crypto::NodeIdentity localIdentity_;

            std::atomic<bool> running_{false};
            std::atomic<bool> connected_{false};
            std::thread worker_;

            mutable std::mutex socketMutex_;
            SocketHandle socket_ = INVALID_SOCKET_HANDLE;

            mutable std::mutex callbackMutex_;
            ApplyCallback applyCallback_;
            BlobCallback blobCallback_;
            std::function<void()> forceDurableCallback_;
            SnapshotBeginCallback snapshotBeginCallback_;
            SnapshotEntryCallback snapshotEntryCallback_;
            SnapshotEndCallback snapshotEndCallback_;
    };

    std::unique_ptr<ReplicationClient> ReplicationClient::create(
        std::string primaryHost,
        uint16_t primaryReplPort,
        uint64_t selfNodeId,
        std::function<uint64_t()> getLastSeq,
        AckPolicy ackPolicy,
        ClusterRuntimeOptions runtimeOptions
    ) {
        return std::unique_ptr<ReplicationClient>(
            new ReplicationClient(
                std::make_unique<Impl>(
                    std::move(primaryHost),
                    primaryReplPort,
                    selfNodeId,
                    std::move(getLastSeq),
                    ackPolicy,
                    std::move(runtimeOptions)
                )
            )
        );
    }

    ReplicationClient::ReplicationClient(std::unique_ptr<Impl> impl) : impl_{std::move(impl)} {}

    ReplicationClient::~ReplicationClient() = default;

    void ReplicationClient::setApplyCallback(ApplyCallback callback) { impl_->setApplyCallback(std::move(callback)); }

    void ReplicationClient::setBlobCallback(BlobCallback callback) { impl_->setBlobCallback(std::move(callback)); }

    void ReplicationClient::setForceDurableCallback(std::function<void()> callback) { impl_->setForceDurableCallback(std::move(callback)); }

    void ReplicationClient::setSnapshotCallbacks(SnapshotBeginCallback begin, SnapshotEntryCallback entry, SnapshotEndCallback end) {
        impl_->setSnapshotCallbacks(std::move(begin), std::move(entry), std::move(end));
    }

    void ReplicationClient::start() { impl_->start(); }

    void ReplicationClient::close() { impl_->close(); }

    bool ReplicationClient::connected() const noexcept { return impl_->connected(); }
} // namespace akkaradb::engine::cluster

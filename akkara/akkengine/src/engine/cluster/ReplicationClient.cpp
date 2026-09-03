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
#include "akk/cpu/CRC32C.hpp"
#include "akk/crypto/SecureChannel.hpp"

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#else
#include <arpa/inet.h>
#include <fcntl.h>
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

        void configureNoSigPipe(SocketHandle s) noexcept {
            #if !defined(_WIN32) && defined(SO_NOSIGPIPE)
            int enabled = 1; (void)::setsockopt(s, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled));
            #else
            (void)s;
            #endif
        }

        void setTimeouts(SocketHandle s, int timeoutMs) {
            #ifdef _WIN32
            const auto timeout = static_cast<DWORD>(timeoutMs);
            ::setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&timeout), sizeof(timeout));
            ::setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, reinterpret_cast<const char*>(&timeout), sizeof(timeout));
            #else
            timeval tv{}; tv.tv_sec = timeoutMs / 1000; tv.tv_usec = (timeoutMs % 1000) * 1000; ::setsockopt(
                s,
                SOL_SOCKET,
                SO_RCVTIMEO,
                &tv,
                sizeof(tv)
            ); ::setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
            #endif
        }

        bool setBlocking(SocketHandle s, bool blocking) noexcept {
            #ifdef _WIN32
            u_long mode = blocking ? 0u : 1u;
            return ::ioctlsocket(s, FIONBIO, &mode) == 0;
            #else
            const int flags = ::fcntl(s, F_GETFL, 0); if (flags < 0) { return false; } const int nextFlags =
                blocking ? (flags & ~O_NONBLOCK) : (flags | O_NONBLOCK); return ::fcntl(s, F_SETFL, nextFlags) == 0;
            #endif
        }

        bool waitForConnect(SocketHandle s, int timeoutMs) {
            fd_set writeSet;
            FD_ZERO(&writeSet);
            FD_SET(s, &writeSet);
            timeval tv{};
            tv.tv_sec = timeoutMs / 1000;
            tv.tv_usec = (timeoutMs % 1000) * 1000;
            #ifdef _WIN32
            const int rc = ::select(0, nullptr, &writeSet, nullptr, &tv);
            #else
            int rc = 0; do { rc = ::select(s + 1, nullptr, &writeSet, nullptr, &tv); }
            while (rc < 0 && errno == EINTR);
            #endif
            if (rc <= 0) { return false; }
            int error = 0;
            #ifdef _WIN32
            int len = sizeof(error);
            return ::getsockopt(s, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&error), &len) == 0 && error == 0;
            #else
            socklen_t len = sizeof(error); return ::getsockopt(s, SOL_SOCKET, SO_ERROR, &error, &len) == 0 && error == 0;
            #endif
        }

        bool sendAll(SocketHandle s, const uint8_t* data, size_t size) {
            size_t sent = 0;
            while (sent < size) {
                #ifdef _WIN32
                const int n = ::send(s, reinterpret_cast<const char*>(data + sent), static_cast<int>(size - sent), 0);
                if (n < 0 && ::WSAGetLastError() == WSAEINTR) { continue; }
                #else
                int flags = 0;
                #ifdef MSG_NOSIGNAL
                flags |= MSG_NOSIGNAL;
                #endif
                const ssize_t n = ::send(s, data + sent, size - sent, flags); if (n < 0 && errno == EINTR) { continue; }
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
                if (n < 0 && ::WSAGetLastError() == WSAEINTR) { continue; }
                #else
                const ssize_t n = ::recv(s, data + received, size - received, 0); if (n < 0 && errno == EINTR) { continue; }
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
            constexpr int CONNECT_TIMEOUT_MS = 1000;
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
                configureNoSigPipe(socket);
                setTimeouts(socket, CONNECT_TIMEOUT_MS);
                if (!setBlocking(socket, false)) {
                    closeSocket(socket);
                    socket = INVALID_SOCKET_HANDLE;
                    continue;
                }
                const int rc = ::connect(socket, it->ai_addr, static_cast<int>(it->ai_addrlen));
                #ifdef _WIN32
                const bool inProgress = rc != 0 && (::WSAGetLastError() == WSAEWOULDBLOCK || ::WSAGetLastError() == WSAEINPROGRESS ||
                    ::WSAGetLastError() == WSAEINVAL);
                #else
                const bool inProgress = rc != 0 && errno == EINPROGRESS;
                #endif
                if (rc == 0 || (inProgress && waitForConnect(socket, CONNECT_TIMEOUT_MS))) {
                    (void)setBlocking(socket, true);
                    setTimeouts(socket, CONNECT_TIMEOUT_MS);
                    break;
                }
                closeSocket(socket);
                socket = INVALID_SOCKET_HANDLE;
            }

            ::freeaddrinfo(result);
            return socket;
        }

        struct MembershipState {
            uint64_t groupId = 0;
            uint64_t primaryNodeId = 0;
            uint64_t groupEpoch = 0;
        };

        void writeLe32(std::vector<uint8_t>& out, uint32_t value) {
            for (size_t i = 0; i < 4; ++i) { out.push_back(static_cast<uint8_t>(value >> (i * 8))); }
        }

        void writeLe64(std::vector<uint8_t>& out, uint64_t value) {
            for (size_t i = 0; i < 8; ++i) { out.push_back(static_cast<uint8_t>(value >> (i * 8))); }
        }

        void writeLe32At(std::vector<uint8_t>& out, size_t off, uint32_t value) {
            for (size_t i = 0; i < 4; ++i) { out[off + i] = static_cast<uint8_t>(value >> (i * 8)); }
        }

        uint32_t readLe32(std::span<const uint8_t> in, size_t off) {
            return static_cast<uint32_t>(in[off]) | (static_cast<uint32_t>(in[off + 1]) << 8) | (static_cast<uint32_t>(in[off + 2]) << 16) |
                (static_cast<uint32_t>(in[off + 3]) << 24);
        }

        uint64_t readLe64(std::span<const uint8_t> in, size_t off) {
            uint64_t out = 0;
            for (size_t i = 0; i < 8; ++i) { out |= static_cast<uint64_t>(in[off + i]) << (i * 8); }
            return out;
        }

        uint32_t crcWithZeroedField(std::vector<uint8_t> bytes, size_t crcOffset) {
            if (crcOffset + 4 > bytes.size()) { throw std::runtime_error("ReplicationClient: invalid CRC field"); }
            writeLe32At(bytes, crcOffset, 0);
            return cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size());
        }

        std::filesystem::path corruptBackupPath(const std::filesystem::path& path) {
            for (uint32_t i = 0; i < 10000; ++i) {
                auto candidate = path;
                candidate += i == 0 ? ".corrupt" : ".corrupt." + std::to_string(i);
                if (!std::filesystem::exists(candidate)) { return candidate; }
            }
            throw std::runtime_error("ReplicationClient: cannot allocate corrupt state backup path");
        }

        bool handleCorruptMembershipFile(const std::filesystem::path& path, CorruptClusterStateAction action, const std::exception& cause) {
            if (action == CorruptClusterStateAction::FAIL_STARTUP) {
                throw std::runtime_error(std::string{"ReplicationClient: "} + cause.what());
            }
            std::error_code ec;
            if (action == CorruptClusterStateAction::BACKUP_AND_RECREATE) {
                std::filesystem::rename(path, corruptBackupPath(path), ec);
                if (ec) { throw std::runtime_error("ReplicationClient: cannot back up corrupt membership state: " + ec.message()); }
                return true;
            }
            if (action == CorruptClusterStateAction::DELETE_AND_RECREATE) {
                std::filesystem::remove(path, ec);
                if (ec) { throw std::runtime_error("ReplicationClient: cannot delete corrupt membership state: " + ec.message()); }
                return true;
            }
            throw std::runtime_error("ReplicationClient: invalid corrupt state action");
        }

        void syncFile(const std::filesystem::path& path) {
            #ifdef _WIN32
            const int fd = _wopen(path.c_str(), _O_RDWR | _O_BINARY);
            if (fd < 0) { throw std::runtime_error("ReplicationClient: cannot reopen temp membership state for sync"); }
            const int rc = _commit(fd);
            const int closeRc = _close(fd);
            if (rc != 0 || closeRc != 0) { throw std::runtime_error("ReplicationClient: temp membership state sync failed"); }
            #else
            const int fd = ::open(path.c_str(), O_RDONLY); if (fd < 0) {
                throw std::runtime_error("ReplicationClient: cannot reopen temp membership state for sync");
            } const int rc = ::fsync(fd); const int closeRc = ::close(fd); if (rc != 0 || closeRc != 0) {
                throw std::runtime_error("ReplicationClient: temp membership state sync failed");
            }
            #endif
        }

        std::filesystem::path makeTempPath(const std::filesystem::path& path) {
            static std::atomic<uint64_t> sequence{0};
            const auto parent = path.parent_path();
            const auto stem = path.filename().string();
            #ifdef _WIN32
            const auto pid = static_cast<uint64_t>(::GetCurrentProcessId());
            #else
            const auto pid = static_cast<uint64_t>(::getpid());
            #endif
            for (uint32_t attempt = 0; attempt < 1024; ++attempt) {
                const auto suffix = ".tmp." + std::to_string(pid) + "." + std::to_string(sequence.fetch_add(1)) + "." + std::to_string(
                    attempt
                );
                auto candidate = parent / (stem + suffix);
                if (!std::filesystem::exists(candidate)) { return candidate; }
            }
            throw std::runtime_error("ReplicationClient: cannot allocate temp membership state name");
        }

        #ifndef _WIN32
        void syncParentDirectory(const std::filesystem::path& path) {
            const auto parent = path.parent_path().empty() ? std::filesystem::path{"."} : path.parent_path();
            int flags = O_RDONLY;
        #ifdef O_DIRECTORY
        flags|= O_DIRECTORY;
        #endif
        const int fd = ::open(parent.c_str(), flags);if (fd<0) {
            throw std::runtime_error("ReplicationClient: cannot open membership parent directory for sync");
        } const int rc = ::fsync(fd); const int closeRc = ::close(fd);if (rc!= 0 || closeRc
!= 0) { throw std::runtime_error("ReplicationClient: membership parent directory sync failed"); }
        }
        #endif

        void replaceFileAtomically(const std::filesystem::path& tmp, const std::filesystem::path& path) {
            #ifdef _WIN32
            if (!::MoveFileExW(tmp.wstring().c_str(), path.wstring().c_str(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
                throw std::runtime_error("ReplicationClient: atomic membership state replace failed");
            }
            #else
            std::filesystem::rename(tmp, path); syncParentDirectory(path);
            #endif
        }

        std::optional<MembershipState> loadMembership(const std::filesystem::path& path, CorruptClusterStateAction corruptAction) {
            if (path.empty() || !std::filesystem::exists(path)) { return std::nullopt; }
            try {
                std::ifstream in(path, std::ios::binary);
                if (!in) { throw std::runtime_error("cannot open cluster membership state"); }
                std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
                constexpr size_t expectedSize = 5 + 8 + 8 + 8 + 4;
                constexpr size_t crcOffset = expectedSize - 4;
                if (bytes.size() != expectedSize) { throw std::runtime_error("invalid cluster membership state size"); }
                if (std::string_view{reinterpret_cast<const char*>(bytes.data()), 5} != "AKCG2") {
                    throw std::runtime_error("bad cluster membership state magic");
                }
                if (readLe32(bytes, crcOffset) != crcWithZeroedField(bytes, crcOffset)) {
                    throw std::runtime_error("cluster membership state CRC mismatch");
                }
                return MembershipState{
                    .groupId = readLe64(bytes, 5),
                    .primaryNodeId = readLe64(bytes, 13),
                    .groupEpoch = readLe64(bytes, 21),
                };
            }
            catch (const std::exception& ex) {
                (void)handleCorruptMembershipFile(path, corruptAction, ex);
                return std::nullopt;
            }
        }

        void saveMembership(const std::filesystem::path& path, MembershipState state) {
            if (path.empty()) { return; }
            if (path.has_parent_path()) { std::filesystem::create_directories(path.parent_path()); }
            std::vector<uint8_t> bytes;
            bytes.insert(bytes.end(), {'A', 'K', 'C', 'G', '2'});
            writeLe64(bytes, state.groupId);
            writeLe64(bytes, state.primaryNodeId);
            writeLe64(bytes, state.groupEpoch);
            const size_t crcOffset = bytes.size();
            writeLe32(bytes, 0);
            writeLe32At(bytes, crcOffset, crcWithZeroedField(bytes, crcOffset));

            const auto tmpPath = makeTempPath(path);
            {
                std::ofstream out(tmpPath, std::ios::binary | std::ios::trunc);
                if (!out) { throw std::runtime_error("ReplicationClient: cannot create cluster membership state"); }
                out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
                out.flush();
                if (!out) { throw std::runtime_error("ReplicationClient: cluster membership state write failed"); }
            }
            syncFile(tmpPath);
            replaceFileAtomically(tmpPath, path);
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

            void setBlobCallbacks(BlobBeginCallback begin, BlobChunkCallback chunk, BlobEndCallback end) {
                std::lock_guard lock{callbackMutex_};
                blobBeginCallback_ = std::move(begin);
                blobChunkCallback_ = std::move(chunk);
                blobEndCallback_ = std::move(end);
            }

            void setForceDurableCallback(std::function<void()> callback) {
                std::lock_guard lock{callbackMutex_};
                forceDurableCallback_ = std::move(callback);
            }

            void setSnapshotCallbacks(
                SnapshotBeginCallback begin,
                SnapshotEntryBeginCallback beginEntry,
                SnapshotEntryChunkCallback chunk,
                SnapshotEntryEndCallback endEntry,
                SnapshotEndCallback end
            ) {
                std::lock_guard lock{callbackMutex_};
                snapshotBeginCallback_ = std::move(begin);
                snapshotEntryBeginCallback_ = std::move(beginEntry);
                snapshotEntryChunkCallback_ = std::move(chunk);
                snapshotEntryEndCallback_ = std::move(endEntry);
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
                failPendingRead();
            }

            bool connected() const noexcept { return connected_; }

            ReadResponse readKey(std::span<const uint8_t> key, uint64_t snapshotSeq, uint32_t timeoutMs) {
                std::unique_lock readLock{readMutex_};
                const uint64_t requestId = nextReadRequestId_++;
                pendingReadRequestId_ = requestId;
                pendingReadResponse_.reset();
                pendingReadFailed_ = false;

                ReadRequest request;
                request.requestId = requestId;
                request.snapshotSeq = snapshotSeq;
                request.key.assign(key.begin(), key.end());
                const auto wire = encodeReadRequest(request);

                bool sent = false;
                {
                    std::lock_guard sendLock{sendMutex_};
                    std::lock_guard socketLock{socketMutex_};
                    if (connected_.load(std::memory_order_acquire) && socket_ != INVALID_SOCKET_HANDLE) {
                        sent = sendTo(socket_, activeSecure_, wire.data(), wire.size());
                    }
                }
                if (!sent) {
                    pendingReadRequestId_ = 0;
                    throw std::runtime_error("ReplicationClient: owner read request send failed");
                }

                const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
                while (!pendingReadResponse_.has_value() && !pendingReadFailed_) {
                    if (readCv_.wait_until(readLock, deadline) == std::cv_status::timeout) { break; }
                }
                if (!pendingReadResponse_.has_value()) {
                    pendingReadRequestId_ = 0;
                    if (pendingReadFailed_) { throw std::runtime_error("ReplicationClient: owner read connection closed"); }
                    throw std::runtime_error("ReplicationClient: owner read request timeout");
                }
                ReadResponse response = std::move(*pendingReadResponse_);
                pendingReadResponse_.reset();
                pendingReadRequestId_ = 0;
                return response;
            }

        private:
            void completePendingRead(ReadResponse response) {
                std::lock_guard lock{readMutex_};
                if (response.requestId == pendingReadRequestId_) {
                    pendingReadResponse_ = std::move(response);
                    readCv_.notify_all();
                }
            }

            void failPendingRead() {
                std::lock_guard lock{readMutex_};
                pendingReadFailed_ = true;
                readCv_.notify_all();
            }

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
                        {
                            std::lock_guard lock{socketMutex_};
                            if (socket_ == socket) { activeSecure_ = secure.get(); }
                        }
                        connected_ = true;
                        receiveLoop(socket, secure.get());
                    }

                    connected_ = false;
                    failPendingRead();
                    {
                        std::lock_guard lock{socketMutex_};
                        if (socket_ == socket) {
                            activeSecure_ = nullptr;
                            socket_ = INVALID_SOCKET_HANDLE;
                        }
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
                if (!expected) { throw std::runtime_error("ReplicationClient: missing secure primary peer pin"); }
                auto session = initiator.finish(serverHello, expected);
                return OpenSecureSession{std::move(session), serverHello.staticPublicKey};
            }

            bool handshake(SocketHandle socket, crypto::SecureSession* secure, const crypto::PublicKey& secureRemotePublicKey) {
                const ClientHello hello{
                    .nodeId = selfNodeId_,
                    .lastSeq = getLastSeq_ ? getLastSeq_() : 0,
                    .role = NodeRole::REPLICA,
                    .groupId = runtimeOptions_.clusterGroupId,
                    .groupEpoch = runtimeOptions_.clusterGroupEpoch,
                };
                const auto wire = encodeClientHello(hello);
                if (!sendTo(socket, secure, wire.data(), wire.size())) { return false; }

                DecodedFrame frame;
                if (!recvFrameFrom(socket, secure, frame) || frame.type != ReplMsgType::SERVER_HELLO) { return false; }
                ServerHello serverHello;
                if (!decodeServerHello(frame.payload, serverHello) || serverHello.role != NodeRole::PRIMARY) { return false; }
                if (runtimeOptions_.clusterGroupId != 0 && serverHello.groupId != runtimeOptions_.clusterGroupId) { return false; }
                if (runtimeOptions_.clusterGroupEpoch != 0 && serverHello.groupEpoch != runtimeOptions_.clusterGroupEpoch) { return false; }
                if (runtimeOptions_.secure.expectedPrimaryNodeId != 0 && serverHello.nodeId != runtimeOptions_.secure.
                    expectedPrimaryNodeId) { return false; }
                if (secure != nullptr) {
                    const auto expected = pinnedPeerKey(runtimeOptions_, serverHello.nodeId);
                    if (!expected || secureRemotePublicKey != *expected) { return false; }
                }
                const MembershipState incoming{
                    .groupId = serverHello.groupId,
                    .primaryNodeId = serverHello.nodeId,
                    .groupEpoch = serverHello.groupEpoch,
                };
                try {
                    if (const auto existing = loadMembership(runtimeOptions_.clusterMembershipPath, runtimeOptions_.corruptStateAction);
                        existing && !runtimeOptions_.resetClusterMembership && (existing->groupId != incoming.groupId || existing->
                            primaryNodeId != incoming.primaryNodeId || existing->groupEpoch != incoming.groupEpoch)) { return false; }
                    saveMembership(runtimeOptions_.clusterMembershipPath, incoming);
                }
                catch (...) { return false; }
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
                std::lock_guard lock{sendMutex_};
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
                        std::function<void()> forceDurableCallback;
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
                        BlobBeginCallback beginCallback;
                        BlobChunkCallback chunkCallback;
                        BlobEndCallback endCallback;
                        {
                            std::lock_guard lock{callbackMutex_};
                            beginCallback = blobBeginCallback_;
                            chunkCallback = blobChunkCallback_;
                            endCallback = blobEndCallback_;
                        }
                        if (beginCallback && chunkCallback && endCallback) {
                            const uint32_t contentCrc32c = cpu::CRC32C(
                                reinterpret_cast<const std::byte*>(blob.content.data()),
                                blob.content.size()
                            );
                            beginCallback(blob.seq, blob.blobId, static_cast<uint64_t>(blob.content.size()), contentCrc32c);
                            chunkCallback(blob.seq, blob.blobId, 0, blob.content);
                            endCallback(blob.seq, blob.blobId);
                        }
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
                        SnapshotEntryBeginCallback beginCallback;
                        SnapshotEntryChunkCallback chunkCallback;
                        SnapshotEntryEndCallback endCallback;
                        {
                            std::lock_guard lock{callbackMutex_};
                            beginCallback = snapshotEntryBeginCallback_;
                            chunkCallback = snapshotEntryChunkCallback_;
                            endCallback = snapshotEntryEndCallback_;
                        }
                        if (!beginCallback || !chunkCallback || !endCallback) { return; }
                        const uint32_t valueCrc32c = cpu::CRC32C(
                            reinterpret_cast<const std::byte*>(entry.value.data()),
                            entry.value.size()
                        );
                        beginCallback(entry.key, static_cast<uint64_t>(entry.value.size()), valueCrc32c);
                        chunkCallback(0, entry.value);
                        endCallback();
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
                    else if (frame.type == ReplMsgType::READ_RESPONSE) {
                        ReadResponse response;
                        if (!decodeReadResponse(frame.payload, response)) { return; }
                        completePendingRead(std::move(response));
                    }
                    else { return; }
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
            crypto::SecureSession* activeSecure_ = nullptr;
            std::mutex sendMutex_;

            mutable std::mutex readMutex_;
            std::condition_variable readCv_;
            uint64_t nextReadRequestId_ = 1;
            uint64_t pendingReadRequestId_ = 0;
            std::optional<ReadResponse> pendingReadResponse_;
            bool pendingReadFailed_ = false;

            mutable std::mutex callbackMutex_;
            ApplyCallback applyCallback_;
            BlobBeginCallback blobBeginCallback_;
            BlobChunkCallback blobChunkCallback_;
            BlobEndCallback blobEndCallback_;
            std::function<void()> forceDurableCallback_;
            SnapshotBeginCallback snapshotBeginCallback_;
            SnapshotEntryBeginCallback snapshotEntryBeginCallback_;
            SnapshotEntryChunkCallback snapshotEntryChunkCallback_;
            SnapshotEntryEndCallback snapshotEntryEndCallback_;
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

    void ReplicationClient::setBlobCallbacks(BlobBeginCallback begin, BlobChunkCallback chunk, BlobEndCallback end) {
        impl_->setBlobCallbacks(std::move(begin), std::move(chunk), std::move(end));
    }

    void ReplicationClient::setForceDurableCallback(std::function<void()> callback) { impl_->setForceDurableCallback(std::move(callback)); }

    void ReplicationClient::setSnapshotCallbacks(
        SnapshotBeginCallback begin,
        SnapshotEntryBeginCallback beginEntry,
        SnapshotEntryChunkCallback chunk,
        SnapshotEntryEndCallback endEntry,
        SnapshotEndCallback end
    ) { impl_->setSnapshotCallbacks(std::move(begin), std::move(beginEntry), std::move(chunk), std::move(endEntry), std::move(end)); }

    void ReplicationClient::start() { impl_->start(); }

    void ReplicationClient::close() { impl_->close(); }

    bool ReplicationClient::connected() const noexcept { return impl_->connected(); }

    ReadResponse ReplicationClient::readKey(std::span<const uint8_t> key, uint64_t snapshotSeq, uint32_t timeoutMs) {
        return impl_->readKey(key, snapshotSeq, timeoutMs);
    }
} // namespace akkaradb::engine::cluster

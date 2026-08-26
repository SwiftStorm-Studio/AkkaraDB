/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/cluster/RaftConsensusRuntime.cpp
#include "akk/engine/cluster/detail/RaftConsensusRuntime.hpp"
#include "akk/cpu/CRC32C.hpp"
#include "akk/crypto/SecureChannel.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <limits>
#include <mutex>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#include <fcntl.h>
#include <io.h>
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace akkaradb::engine::cluster {
    namespace {
        using Clock = std::chrono::steady_clock;
        constexpr size_t MAX_RAFT_CLIENT_HANDLERS = 128;

        #ifdef _WIN32
        using SocketHandle = SOCKET; constexpr SocketHandle BAD_SOCKET = INVALID_SOCKET; void netInit() {
            static std::once_flag once;
            std::call_once(
                once,
                [] {
                    WSADATA wsa{};
                    if (::WSAStartup(MAKEWORD(2, 2), &wsa) != 0) { throw std::runtime_error("RaftConsensusRuntime: WSAStartup failed"); }
                }
            );
        } void closeSocket(SocketHandle s) noexcept { if (s != BAD_SOCKET) { ::closesocket(s); } } void
        shutdownSocket(SocketHandle s) noexcept { if (s != BAD_SOCKET) { ::shutdown(s, SD_BOTH); } } bool
        socketOk(SocketHandle s) noexcept { return s != INVALID_SOCKET; }
        #else
        using SocketHandle = int;
        constexpr SocketHandle BAD_SOCKET = -1;
        void netInit() {}
        void closeSocket(SocketHandle s) noexcept { if (s >= 0) { ::close(s); } }
        void shutdownSocket(SocketHandle s) noexcept { if (s >= 0) { ::shutdown(s, SHUT_RDWR); } }
        bool socketOk(SocketHandle s) noexcept { return s >= 0; }
        #endif

        void writeU8(std::vector<uint8_t>& out, uint8_t value) { out.push_back(value); }

        void writeU32(std::vector<uint8_t>& out, uint32_t value) {
            for (size_t i = 0; i < 4; ++i) { out.push_back(static_cast<uint8_t>(value >> (8 * i))); }
        }

        void writeU64(std::vector<uint8_t>& out, uint64_t value) {
            for (size_t i = 0; i < 8; ++i) { out.push_back(static_cast<uint8_t>(value >> (8 * i))); }
        }

        void writeU32At(std::vector<uint8_t>& out, size_t off, uint32_t value) {
            for (size_t i = 0; i < 4; ++i) { out[off + i] = static_cast<uint8_t>(value >> (8 * i)); }
        }

        uint32_t readU32(std::span<const uint8_t> in, size_t off) {
            return static_cast<uint32_t>(in[off]) | (static_cast<uint32_t>(in[off + 1]) << 8) | (static_cast<uint32_t>(in[off + 2]) << 16) |
                (static_cast<uint32_t>(in[off + 3]) << 24);
        }

        uint64_t readU64(std::span<const uint8_t> in, size_t off) {
            uint64_t out = 0;
            for (size_t i = 0; i < 8; ++i) { out |= static_cast<uint64_t>(in[off + i]) << (8 * i); }
            return out;
        }

        bool readBytes(std::span<const uint8_t> in, size_t& cursor, size_t len, std::vector<uint8_t>& out) {
            if (cursor + len > in.size()) { return false; }
            out.assign(in.begin() + static_cast<std::ptrdiff_t>(cursor), in.begin() + static_cast<std::ptrdiff_t>(cursor + len));
            cursor += len;
            return true;
        }

        uint32_t crcBytes(std::span<const uint8_t> bytes) {
            return cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size());
        }

        class Crc32cStream {
            public:
                void update(std::span<const uint8_t> bytes) noexcept {
                    for (const auto byte : bytes) {
                        crc_ = (crc_ >> 8u) ^ table()[(crc_ ^ byte) & 0xffu];
                    }
                }

                [[nodiscard]] uint32_t finish() const noexcept { return ~crc_; }

            private:
                static const std::array<uint32_t, 256>& table() noexcept {
                    static const std::array<uint32_t, 256> values = [] {
                        std::array<uint32_t, 256> out{};
                        for (uint32_t i = 0; i < out.size(); ++i) {
                            uint32_t crc = i;
                            for (uint32_t bit = 0; bit < 8; ++bit) {
                                crc = (crc >> 1u) ^ (0x82F63B78u & (0u - (crc & 1u)));
                            }
                            out[i] = crc;
                        }
                        return out;
                    }();
                    return values;
                }

                uint32_t crc_ = 0xFFFFFFFFu;
        };

        uint32_t crcWithZeroedField(std::vector<uint8_t> bytes, size_t crcOffset) {
            if (crcOffset + 4 > bytes.size()) { throw std::runtime_error("RaftConsensusRuntime: invalid CRC field"); }
            writeU32At(bytes, crcOffset, 0);
            return crcBytes(bytes);
        }

        std::vector<uint8_t> readWholeFile(const std::filesystem::path& path, const char* context) {
            std::ifstream in(path, std::ios::binary);
            if (!in) { throw std::runtime_error(std::string{context} + ": cannot open file"); }
            return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
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
                const auto suffix = ".tmp." + std::to_string(pid) + "." + std::to_string(sequence.fetch_add(1)) + "." +
                                    std::to_string(attempt);
                auto candidate = parent / (stem + suffix);
                if (!std::filesystem::exists(candidate)) { return candidate; }
            }
            throw std::runtime_error("RaftConsensusRuntime: cannot allocate temp file name");
        }

        #ifndef _WIN32
        void syncParentDirectory(const std::filesystem::path& path, const char* context) {
            const auto parent = path.parent_path().empty() ? std::filesystem::path{"."} : path.parent_path();
            int flags = O_RDONLY;
            #ifdef O_DIRECTORY
            flags |= O_DIRECTORY;
            #endif
            const int fd = ::open(parent.c_str(), flags);
            if (fd < 0) { throw std::runtime_error(std::string{context} + ": cannot open parent directory for sync"); }
            const int rc = ::fsync(fd);
            const int closeRc = ::close(fd);
            if (rc != 0 || closeRc != 0) { throw std::runtime_error(std::string{context} + ": parent directory sync failed"); }
        }
        #endif

        void replaceFileAtomically(const std::filesystem::path& tmp, const std::filesystem::path& path, const char* context) {
            #ifdef _WIN32
            if (!::MoveFileExW(tmp.wstring().c_str(), path.wstring().c_str(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
                throw std::runtime_error(std::string{context} + ": atomic file replace failed");
            }
            #else
            std::filesystem::rename(tmp, path);
            syncParentDirectory(path, context);
            #endif
        }

        void writeFileAtomically(const std::filesystem::path& path, std::span<const uint8_t> bytes, const char* context) {
            if (path.has_parent_path()) { std::filesystem::create_directories(path.parent_path()); }
            const auto tmp = makeTempPath(path);
            {
                std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
                if (!out) { throw std::runtime_error(std::string{context} + ": cannot create temp file"); }
                out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
                out.flush();
                if (!out) { throw std::runtime_error(std::string{context} + ": write failed"); }
            }
            #ifdef _WIN32
            const int fd = _wopen(tmp.c_str(), _O_RDWR | _O_BINARY);
            if (fd < 0) { throw std::runtime_error(std::string{context} + ": cannot reopen temp file for sync"); }
            const int rc = _commit(fd);
            const int closeRc = _close(fd);
            if (rc != 0 || closeRc != 0) { throw std::runtime_error(std::string{context} + ": temp file sync failed"); }
            #else
            const int fd = ::open(tmp.c_str(), O_RDONLY);
            if (fd < 0) { throw std::runtime_error(std::string{context} + ": cannot reopen temp file for sync"); }
            const int rc = ::fsync(fd);
            const int closeRc = ::close(fd);
            if (rc != 0 || closeRc != 0) { throw std::runtime_error(std::string{context} + ": temp file sync failed"); }
            #endif
            replaceFileAtomically(tmp, path, context);
        }

        void configureNoSigPipe(SocketHandle s) noexcept {
            #if !defined(_WIN32) && defined(SO_NOSIGPIPE)
            int enabled = 1;
            (void)::setsockopt(s, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled));
            #else
            (void)s;
            #endif
        }

        bool setBlocking(SocketHandle s, bool blocking) noexcept {
            #ifdef _WIN32
            u_long mode = blocking ? 0u : 1u;
            return ::ioctlsocket(s, FIONBIO, &mode) == 0;
            #else
            const int flags = ::fcntl(s, F_GETFL, 0);
            if (flags < 0) { return false; }
            const int nextFlags = blocking ? (flags & ~O_NONBLOCK) : (flags | O_NONBLOCK);
            return ::fcntl(s, F_SETFL, nextFlags) == 0;
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
            int rc = 0;
            do { rc = ::select(s + 1, nullptr, &writeSet, nullptr, &tv); }
            while (rc < 0 && errno == EINTR);
            #endif
            if (rc <= 0) { return false; }
            int error = 0;
            #ifdef _WIN32
            int len = sizeof(error);
            return ::getsockopt(s, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&error), &len) == 0 && error == 0;
            #else
            socklen_t len = sizeof(error);
            return ::getsockopt(s, SOL_SOCKET, SO_ERROR, &error, &len) == 0 && error == 0;
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
                const ssize_t rc = ::send(s, data + sent, size - sent, flags);
                if (rc < 0 && errno == EINTR) { continue; }
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
                const ssize_t rc = ::recv(s, data + got, size - got, 0);
                if (rc < 0 && errno == EINTR) { continue; }
                #endif
                if (rc <= 0) { return false; }
                got += static_cast<size_t>(rc);
            }
            return true;
        }

        void setTimeouts(SocketHandle s, int timeoutMs) {
            #ifdef _WIN32
            const auto timeout = static_cast<DWORD>(timeoutMs); ::setsockopt(
                s,
                SOL_SOCKET,
                SO_RCVTIMEO,
                reinterpret_cast<const char*>(&timeout),
                sizeof(timeout)
            ); ::setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, reinterpret_cast<const char*>(&timeout), sizeof(timeout));
            #else
            timeval tv{};
            tv.tv_sec = timeoutMs / 1000;
            tv.tv_usec = (timeoutMs % 1000) * 1000;
            ::setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            ::setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
            #endif
        }

        SocketHandle listenOn(const std::string& host, uint16_t port) {
            netInit();
            addrinfo hints{};
            hints.ai_family = AF_UNSPEC;
            hints.ai_socktype = SOCK_STREAM;
            hints.ai_flags = AI_PASSIVE;

            addrinfo* result = nullptr;
            const auto portText = std::to_string(port);
            if (::getaddrinfo(host.empty() ? nullptr : host.c_str(), portText.c_str(), &hints, &result) != 0) {
                throw std::runtime_error("RaftConsensusRuntime: getaddrinfo failed for listener");
            }

            SocketHandle out = BAD_SOCKET;
            for (addrinfo* it = result; it != nullptr; it = it->ai_next) {
                SocketHandle s = ::socket(it->ai_family, it->ai_socktype, it->ai_protocol);
                if (!socketOk(s)) { continue; }
                configureNoSigPipe(s);
                int reuse = 1;
                ::setsockopt(s, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&reuse), sizeof(reuse));
                if (::bind(s, it->ai_addr, static_cast<int>(it->ai_addrlen)) == 0 && ::listen(s, 32) == 0) {
                    out = s;
                    break;
                }
                closeSocket(s);
            }
            ::freeaddrinfo(result);
            if (!socketOk(out)) { throw std::runtime_error("RaftConsensusRuntime: bind/listen failed"); }
            return out;
        }

        SocketHandle connectTo(const std::string& host, uint16_t port, int timeoutMs) {
            netInit();
            addrinfo hints{};
            hints.ai_family = AF_UNSPEC;
            hints.ai_socktype = SOCK_STREAM;
            addrinfo* result = nullptr;
            const auto portText = std::to_string(port);
            if (::getaddrinfo(host.c_str(), portText.c_str(), &hints, &result) != 0) { return BAD_SOCKET; }
            SocketHandle out = BAD_SOCKET;
            for (addrinfo* it = result; it != nullptr; it = it->ai_next) {
                SocketHandle s = ::socket(it->ai_family, it->ai_socktype, it->ai_protocol);
                if (!socketOk(s)) { continue; }
                configureNoSigPipe(s);
                setTimeouts(s, timeoutMs);
                if (!setBlocking(s, false)) {
                    closeSocket(s);
                    continue;
                }
                const int rc = ::connect(s, it->ai_addr, static_cast<int>(it->ai_addrlen));
                #ifdef _WIN32
                const bool inProgress = rc != 0 && (::WSAGetLastError() == WSAEWOULDBLOCK || ::WSAGetLastError() == WSAEINPROGRESS ||
                                                     ::WSAGetLastError() == WSAEINVAL);
                #else
                const bool inProgress = rc != 0 && errno == EINPROGRESS;
                #endif
                if (rc == 0 || (inProgress && waitForConnect(s, timeoutMs))) {
                    (void)setBlocking(s, true);
                    setTimeouts(s, timeoutMs);
                    out = s;
                    break;
                }
                closeSocket(s);
            }
            ::freeaddrinfo(result);
            return out;
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

        bool sendFrame(SocketHandle s, const std::vector<uint8_t>& wire) { return sendAll(s, wire.data(), wire.size()); }

        constexpr uint32_t SECURE_HELLO_MAGIC = 0x48434B41;
        constexpr uint32_t SECURE_FRAME_MAGIC = 0x46434B41;
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

        bool sendFrame(SocketHandle s, crypto::SecureSession* secure, const std::vector<uint8_t>& wire) {
            if (secure != nullptr) { return sendSecureFrame(s, *secure, wire.data(), wire.size()); }
            return sendFrame(s, wire);
        }

        bool recvFrame(SocketHandle s, crypto::SecureSession* secure, DecodedFrame& out) {
            if (secure != nullptr) { return recvSecureFrame(s, *secure, out); }
            return recvFrame(s, out);
        }

        enum class RaftRole : uint8_t {
            FOLLOWER, CANDIDATE, LEADER,
        };

        enum class RaftEntryKind : uint8_t {
            MUTATION = 0, CONFIG_JOINT = 1, CONFIG_FINAL = 2, BLOB = 3, NOOP = 4,
        };

        struct RaftLogEntry {
            uint64_t term = 0;
            uint64_t index = 0;
            uint64_t clientSeq = 0;
            RaftEntryKind kind = RaftEntryKind::MUTATION;
            ReplOpType op = ReplOpType::PUT;
            uint8_t flags = 0;
            uint64_t sourceNodeId = 0;
            std::vector<uint8_t> key;
            std::vector<uint8_t> value;
        };

        struct RequestVote {
            uint64_t term = 0;
            uint64_t candidateId = 0;
            uint64_t lastLogIndex = 0;
            uint64_t lastLogTerm = 0;
        };

        struct RequestVoteResponse {
            uint64_t term = 0;
            bool voteGranted = false;
        };

        struct AppendEntries {
            uint64_t term = 0;
            uint64_t leaderId = 0;
            uint64_t prevLogIndex = 0;
            uint64_t prevLogTerm = 0;
            uint64_t leaderCommit = 0;
            std::vector<RaftLogEntry> entries;
        };

        struct AppendEntriesResponse {
            uint64_t term = 0;
            bool success = false;
            uint64_t matchIndex = 0;
            uint64_t conflictIndex = 0;
            uint64_t conflictTerm = 0;
        };

        struct SnapshotKvChunk {
            uint64_t entryIndex = 0;
            uint64_t valueOffset = 0;
            uint64_t valueSize = 0;
            uint32_t valueCrc32c = 0;
            std::vector<uint8_t> key;
            std::vector<uint8_t> value;
        };

        struct InstallSnapshot {
            uint64_t term = 0;
            uint64_t leaderId = 0;
            uint64_t lastIncludedIndex = 0;
            uint64_t lastIncludedTerm = 0;
            uint64_t snapshotSeq = 0;
            uint64_t entryCount = 0;
            std::vector<NodeInfo> committedVoters;
            std::optional<std::vector<NodeInfo>> jointOldVoters;
            std::optional<std::vector<NodeInfo>> jointNewVoters;
            bool done = false;
            std::vector<SnapshotKvChunk> chunks;
        };

        struct InstallSnapshotResponse {
            uint64_t term = 0;
            bool success = false;
            uint64_t lastIncludedIndex = 0;
        };

        struct TimeoutNow {
            uint64_t term = 0;
            uint64_t leaderId = 0;
            uint64_t targetId = 0;
        };

        struct TimeoutNowResponse {
            uint64_t term = 0;
            bool accepted = false;
        };

        struct PeerReplicationState {
            NodeInfo node;
            uint64_t nextIndex = 1;
            uint64_t matchIndex = 0;
        };

        void writeNodeInfo(std::vector<uint8_t>& out, const NodeInfo& node) {
            writeU64(out, node.nodeId);
            writeU32(out, node.capabilities);
            writeU32(out, node.host.size() > UINT32_MAX ? UINT32_MAX : static_cast<uint32_t>(node.host.size()));
            writeU32(out, node.dataPort);
            writeU32(out, node.replPort);
            out.insert(out.end(), node.host.begin(), node.host.end());
        }

        bool readNodeInfo(std::span<const uint8_t> in, size_t& cursor, NodeInfo& node) {
            constexpr size_t FIXED_NODE_INFO_SIZE = 24;
            if (cursor + FIXED_NODE_INFO_SIZE > in.size()) { return false; }
            node.nodeId = readU64(in, cursor);
            cursor += 8;
            node.capabilities = readU32(in, cursor);
            cursor += 4;
            const uint32_t hostLen = readU32(in, cursor);
            cursor += 4;
            node.dataPort = static_cast<uint16_t>(readU32(in, cursor));
            cursor += 4;
            node.replPort = static_cast<uint16_t>(readU32(in, cursor));
            cursor += 4;
            std::vector<uint8_t> host;
            if (!readBytes(in, cursor, hostLen, host)) { return false; }
            node.host.assign(host.begin(), host.end());
            return true;
        }

        std::vector<uint8_t> encodeNodeSet(const std::vector<NodeInfo>& nodes) {
            std::vector<uint8_t> out;
            writeU32(out, static_cast<uint32_t>(nodes.size()));
            for (const auto& node : nodes) { writeNodeInfo(out, node); }
            return out;
        }

        bool decodeNodeSet(std::span<const uint8_t> in, std::vector<NodeInfo>& nodes) {
            if (in.size() < 4) { return false; }
            const uint32_t count = readU32(in, 0);
            size_t cursor = 4;
            constexpr size_t MIN_NODE_INFO_SIZE = 24;
            if (count > (in.size() - cursor) / MIN_NODE_INFO_SIZE) { return false; }
            nodes.clear();
            nodes.reserve(count);
            for (uint32_t i = 0; i < count; ++i) {
                NodeInfo node;
                if (!readNodeInfo(in, cursor, node)) { return false; }
                nodes.push_back(std::move(node));
            }
            return cursor == in.size();
        }

        void writeNodeSetField(std::vector<uint8_t>& out, const std::vector<NodeInfo>& nodes) {
            const auto encoded = encodeNodeSet(nodes);
            writeU32(out, static_cast<uint32_t>(encoded.size()));
            out.insert(out.end(), encoded.begin(), encoded.end());
        }

        bool readNodeSetField(std::span<const uint8_t> in, size_t& cursor, std::vector<NodeInfo>& nodes) {
            if (cursor + 4 > in.size()) { return false; }
            const uint32_t len = readU32(in, cursor);
            cursor += 4;
            if (cursor + len > in.size()) { return false; }
            const auto encoded = in.subspan(cursor, len);
            cursor += len;
            return decodeNodeSet(encoded, nodes);
        }

        void writeOptionalNodeSetField(std::vector<uint8_t>& out, const std::optional<std::vector<NodeInfo>>& nodes) {
            writeU8(out, nodes ? 1 : 0);
            if (nodes) { writeNodeSetField(out, *nodes); }
        }

        bool readOptionalNodeSetField(std::span<const uint8_t> in, size_t& cursor, std::optional<std::vector<NodeInfo>>& nodes) {
            if (cursor + 1 > in.size()) { return false; }
            const bool hasValue = in[cursor++] != 0;
            if (!hasValue) {
                nodes.reset();
                return true;
            }
            std::vector<NodeInfo> decoded;
            if (!readNodeSetField(in, cursor, decoded)) { return false; }
            nodes = std::move(decoded);
            return true;
        }

        std::vector<uint8_t> encodeEntryPayload(const RaftLogEntry& entry) {
            std::vector<uint8_t> out;
            writeU64(out, entry.term);
            writeU64(out, entry.index);
            writeU64(out, entry.clientSeq);
            writeU8(out, static_cast<uint8_t>(entry.kind));
            writeU8(out, static_cast<uint8_t>(entry.op));
            writeU8(out, entry.flags);
            writeU64(out, entry.sourceNodeId);
            writeU32(out, static_cast<uint32_t>(entry.key.size()));
            writeU32(out, static_cast<uint32_t>(entry.value.size()));
            out.insert(out.end(), entry.key.begin(), entry.key.end());
            out.insert(out.end(), entry.value.begin(), entry.value.end());
            return out;
        }

        bool decodeEntryPayload(std::span<const uint8_t> in, size_t& cursor, RaftLogEntry& out) {
            if (cursor + 43 > in.size()) { return false; }
            out.term = readU64(in, cursor);
            cursor += 8;
            out.index = readU64(in, cursor);
            cursor += 8;
            out.clientSeq = readU64(in, cursor);
            cursor += 8;
            out.kind = static_cast<RaftEntryKind>(in[cursor++]);
            out.op = static_cast<ReplOpType>(in[cursor++]);
            out.flags = in[cursor++];
            out.sourceNodeId = readU64(in, cursor);
            cursor += 8;
            const uint32_t keyLen = readU32(in, cursor);
            cursor += 4;
            const uint32_t valueLen = readU32(in, cursor);
            cursor += 4;
            return readBytes(in, cursor, keyLen, out.key) && readBytes(in, cursor, valueLen, out.value);
        }

        std::vector<uint8_t> encodeRequestVote(const RequestVote& rpc) {
            std::vector<uint8_t> out;
            writeU64(out, rpc.term);
            writeU64(out, rpc.candidateId);
            writeU64(out, rpc.lastLogIndex);
            writeU64(out, rpc.lastLogTerm);
            return encodeFrame(ReplMsgType::RAFT_REQUEST_VOTE, out);
        }

        bool decodeRequestVote(std::span<const uint8_t> in, RequestVote& out) {
            if (in.size() != 32) { return false; }
            out.term = readU64(in, 0);
            out.candidateId = readU64(in, 8);
            out.lastLogIndex = readU64(in, 16);
            out.lastLogTerm = readU64(in, 24);
            return true;
        }

        std::vector<uint8_t> encodeRequestVoteResponse(const RequestVoteResponse& rpc) {
            std::vector<uint8_t> out;
            writeU64(out, rpc.term);
            writeU8(out, rpc.voteGranted ? 1 : 0);
            return encodeFrame(ReplMsgType::RAFT_REQUEST_VOTE_RESPONSE, out);
        }

        bool decodeRequestVoteResponse(std::span<const uint8_t> in, RequestVoteResponse& out) {
            if (in.size() != 9) { return false; }
            out.term = readU64(in, 0);
            out.voteGranted = in[8] != 0;
            return true;
        }

        std::vector<uint8_t> encodeAppendEntries(const AppendEntries& rpc) {
            std::vector<uint8_t> out;
            writeU64(out, rpc.term);
            writeU64(out, rpc.leaderId);
            writeU64(out, rpc.prevLogIndex);
            writeU64(out, rpc.prevLogTerm);
            writeU64(out, rpc.leaderCommit);
            writeU32(out, static_cast<uint32_t>(rpc.entries.size()));
            for (const auto& entry : rpc.entries) {
                const auto payload = encodeEntryPayload(entry);
                writeU32(out, static_cast<uint32_t>(payload.size()));
                out.insert(out.end(), payload.begin(), payload.end());
            }
            return encodeFrame(ReplMsgType::RAFT_APPEND_ENTRIES, out);
        }

        bool decodeAppendEntries(std::span<const uint8_t> in, AppendEntries& out) {
            if (in.size() < 44) { return false; }
            out.term = readU64(in, 0);
            out.leaderId = readU64(in, 8);
            out.prevLogIndex = readU64(in, 16);
            out.prevLogTerm = readU64(in, 24);
            out.leaderCommit = readU64(in, 32);
            const uint32_t count = readU32(in, 40);
            size_t cursor = 44;
            if (count > (in.size() - cursor) / 4) { return false; }
            out.entries.clear();
            out.entries.reserve(count);
            for (uint32_t i = 0; i < count; ++i) {
                if (cursor + 4 > in.size()) { return false; }
                const uint32_t entryLen = readU32(in, cursor);
                cursor += 4;
                if (cursor + entryLen > in.size()) { return false; }
                size_t entryCursor = 0;
                RaftLogEntry entry;
                if (!decodeEntryPayload(in.subspan(cursor, entryLen), entryCursor, entry) || entryCursor != entryLen) { return false; }
                cursor += entryLen;
                out.entries.push_back(std::move(entry));
            }
            return cursor == in.size();
        }

        std::vector<uint8_t> encodeAppendEntriesResponse(const AppendEntriesResponse& rpc) {
            std::vector<uint8_t> out;
            writeU64(out, rpc.term);
            writeU8(out, rpc.success ? 1 : 0);
            writeU64(out, rpc.matchIndex);
            writeU64(out, rpc.conflictIndex);
            writeU64(out, rpc.conflictTerm);
            return encodeFrame(ReplMsgType::RAFT_APPEND_ENTRIES_RESPONSE, out);
        }

        bool decodeAppendEntriesResponse(std::span<const uint8_t> in, AppendEntriesResponse& out) {
            if (in.size() != 33) { return false; }
            out.term = readU64(in, 0);
            out.success = in[8] != 0;
            out.matchIndex = readU64(in, 9);
            out.conflictIndex = readU64(in, 17);
            out.conflictTerm = readU64(in, 25);
            return true;
        }

        std::vector<uint8_t> encodeInstallSnapshot(const InstallSnapshot& rpc) {
            std::vector<uint8_t> out;
            writeU64(out, rpc.term);
            writeU64(out, rpc.leaderId);
            writeU64(out, rpc.lastIncludedIndex);
            writeU64(out, rpc.lastIncludedTerm);
            writeU64(out, rpc.snapshotSeq);
            writeU64(out, rpc.entryCount);
            writeNodeSetField(out, rpc.committedVoters);
            writeOptionalNodeSetField(out, rpc.jointOldVoters);
            writeOptionalNodeSetField(out, rpc.jointNewVoters);
            writeU8(out, rpc.done ? 1 : 0);
            writeU32(out, static_cast<uint32_t>(rpc.chunks.size()));
            for (const auto& chunk : rpc.chunks) {
                writeU64(out, chunk.entryIndex);
                writeU64(out, chunk.valueOffset);
                writeU64(out, chunk.valueSize);
                writeU32(out, chunk.valueCrc32c);
                writeU32(out, static_cast<uint32_t>(chunk.key.size()));
                writeU32(out, static_cast<uint32_t>(chunk.value.size()));
                out.insert(out.end(), chunk.key.begin(), chunk.key.end());
                out.insert(out.end(), chunk.value.begin(), chunk.value.end());
            }
            return encodeFrame(ReplMsgType::RAFT_INSTALL_SNAPSHOT, out);
        }

        bool decodeInstallSnapshot(std::span<const uint8_t> in, InstallSnapshot& out) {
            if (in.size() < 53) { return false; }
            out.term = readU64(in, 0);
            out.leaderId = readU64(in, 8);
            out.lastIncludedIndex = readU64(in, 16);
            out.lastIncludedTerm = readU64(in, 24);
            out.snapshotSeq = readU64(in, 32);
            out.entryCount = readU64(in, 40);
            size_t cursor = 48;
            if (!readNodeSetField(in, cursor, out.committedVoters) ||
                !readOptionalNodeSetField(in, cursor, out.jointOldVoters) ||
                !readOptionalNodeSetField(in, cursor, out.jointNewVoters)) {
                return false;
            }
            if (cursor + 5 > in.size()) { return false; }
            out.done = in[cursor++] != 0;
            const uint32_t count = readU32(in, cursor);
            cursor += 4;
            if (count > (in.size() - cursor) / 36) { return false; }
            out.chunks.clear();
            out.chunks.reserve(count);
            for (uint32_t i = 0; i < count; ++i) {
                if (cursor + 36 > in.size()) { return false; }
                SnapshotKvChunk chunk;
                chunk.entryIndex = readU64(in, cursor);
                cursor += 8;
                chunk.valueOffset = readU64(in, cursor);
                cursor += 8;
                chunk.valueSize = readU64(in, cursor);
                cursor += 8;
                chunk.valueCrc32c = readU32(in, cursor);
                cursor += 4;
                const uint32_t keyLen = readU32(in, cursor);
                cursor += 4;
                const uint32_t valueLen = readU32(in, cursor);
                cursor += 4;
                if (chunk.valueOffset > chunk.valueSize || valueLen > chunk.valueSize - chunk.valueOffset) { return false; }
                if (!readBytes(in, cursor, keyLen, chunk.key) || !readBytes(in, cursor, valueLen, chunk.value)) { return false; }
                out.chunks.push_back(std::move(chunk));
            }
            return cursor == in.size();
        }

        std::vector<uint8_t> encodeInstallSnapshotResponse(const InstallSnapshotResponse& rpc) {
            std::vector<uint8_t> out;
            writeU64(out, rpc.term);
            writeU8(out, rpc.success ? 1 : 0);
            writeU64(out, rpc.lastIncludedIndex);
            return encodeFrame(ReplMsgType::RAFT_INSTALL_SNAPSHOT_RESPONSE, out);
        }

        bool decodeInstallSnapshotResponse(std::span<const uint8_t> in, InstallSnapshotResponse& out) {
            if (in.size() != 17) { return false; }
            out.term = readU64(in, 0);
            out.success = in[8] != 0;
            out.lastIncludedIndex = readU64(in, 9);
            return true;
        }

        std::vector<uint8_t> encodeTimeoutNow(const TimeoutNow& rpc) {
            std::vector<uint8_t> out;
            writeU64(out, rpc.term);
            writeU64(out, rpc.leaderId);
            writeU64(out, rpc.targetId);
            return encodeFrame(ReplMsgType::RAFT_TIMEOUT_NOW, out);
        }

        bool decodeTimeoutNow(std::span<const uint8_t> in, TimeoutNow& out) {
            if (in.size() != 24) { return false; }
            out.term = readU64(in, 0);
            out.leaderId = readU64(in, 8);
            out.targetId = readU64(in, 16);
            return true;
        }

        std::vector<uint8_t> encodeTimeoutNowResponse(const TimeoutNowResponse& rpc) {
            std::vector<uint8_t> out;
            writeU64(out, rpc.term);
            writeU8(out, rpc.accepted ? 1 : 0);
            return encodeFrame(ReplMsgType::RAFT_TIMEOUT_NOW_RESPONSE, out);
        }

        bool decodeTimeoutNowResponse(std::span<const uint8_t> in, TimeoutNowResponse& out) {
            if (in.size() != 9) { return false; }
            out.term = readU64(in, 0);
            out.accepted = in[8] != 0;
            return true;
        }

        bool sameEntry(const RaftLogEntry& lhs, const RaftLogEntry& rhs) {
            return lhs.term == rhs.term && lhs.index == rhs.index && lhs.clientSeq == rhs.clientSeq && lhs.kind == rhs.kind && lhs.op ==
                rhs.op && lhs.flags == rhs.flags && lhs.sourceNodeId == rhs.sourceNodeId && lhs.key == rhs.key && lhs.value == rhs.value;
        }

        struct RaftBlobChunk {
            uint64_t blobId = 0;
            uint64_t offset = 0;
            uint64_t totalSize = 0;
            uint32_t contentCrc32c = 0;
        };

        static constexpr size_t RAFT_APPEND_ENTRIES_BASE_SIZE = 44;
        static constexpr size_t RAFT_ENTRY_LENGTH_PREFIX_SIZE = 4;
        static constexpr size_t RAFT_ENTRY_PAYLOAD_BASE_SIZE = 43;
        static constexpr size_t RAFT_BLOB_CHUNK_KEY_SIZE = 28;

        uint32_t maxRaftBlobChunkSizeBytes() {
            return ReplFrameHeader::MAX_PAYLOAD_SIZE -
                   static_cast<uint32_t>(
                       RAFT_APPEND_ENTRIES_BASE_SIZE + RAFT_ENTRY_LENGTH_PREFIX_SIZE + RAFT_ENTRY_PAYLOAD_BASE_SIZE +
                       RAFT_BLOB_CHUNK_KEY_SIZE
                   );
        }

        std::vector<uint8_t> encodeBlobChunkKey(const RaftBlobChunk& chunk) {
            std::vector<uint8_t> out;
            writeU64(out, chunk.blobId);
            writeU64(out, chunk.offset);
            writeU64(out, chunk.totalSize);
            writeU32(out, chunk.contentCrc32c);
            return out;
        }

        bool decodeBlobChunkKey(std::span<const uint8_t> in, RaftBlobChunk& chunk) {
            if (in.size() != RAFT_BLOB_CHUNK_KEY_SIZE) { return false; }
            chunk.blobId = readU64(in, 0);
            chunk.offset = readU64(in, 8);
            chunk.totalSize = readU64(in, 16);
            chunk.contentCrc32c = readU32(in, 24);
            if (chunk.offset > chunk.totalSize) { return false; }
            return true;
        }
    }

    class RaftConsensusRuntime::Impl {
        private:
            struct PendingSnapshotInstall {
                bool active = false;
                uint64_t snapshotSeq = 0;
                uint64_t lastIncludedIndex = 0;
                uint64_t lastIncludedTerm = 0;
                uint64_t entryCount = 0;
                std::vector<NodeInfo> committedVoters;
                std::optional<std::vector<NodeInfo>> jointOldVoters;
                std::optional<std::vector<NodeInfo>> jointNewVoters;
                uint64_t nextEntryIndex = 0;
                bool hasCurrentEntry = false;
                uint64_t currentEntryIndex = 0;
                uint64_t currentValueSize = 0;
                uint64_t currentValueOffset = 0;
                uint32_t currentValueCrc32c = 0;
                std::vector<uint8_t> currentKey;
                Crc32cStream currentValueCrc;
            };

            std::filesystem::path dbDir_;
            std::filesystem::path statePath_;
            std::filesystem::path logPath_;
            ClusterConfig config_;
            ClusterRouter router_;
            uint64_t selfNodeId_ = 0;
            const NodeInfo* self_ = nullptr;
            std::vector<NodeInfo> peers_;
            ClusterEngineCallbacks callbacks_;
            ClusterRuntimeOptions runtimeOptions_;
            crypto::NodeIdentity localIdentity_{};

            mutable std::mutex mutex_;
            std::condition_variable cv_;
            bool running_ = false;
            uint64_t currentTerm_ = 0;
            uint64_t votedFor_ = 0;
            uint64_t commitIndex_ = 0;
            uint64_t lastApplied_ = 0;
            uint64_t lastIncludedIndex_ = 0;
            uint64_t lastIncludedTerm_ = 0;
            std::vector<RaftLogEntry> log_;
            std::vector<NodeInfo> committedVoters_;
            std::optional<std::vector<NodeInfo>> jointOldVoters_;
            std::optional<std::vector<NodeInfo>> jointNewVoters_;
            std::vector<PeerReplicationState> peerReplication_;
            Clock::time_point electionDeadline_{};
            std::atomic<RaftRole> role_{RaftRole::FOLLOWER};
            std::mutex applyMutex_;
            PendingSnapshotInstall pendingSnapshotInstall_;

            SocketHandle listenSock_ = BAD_SOCKET;
            std::thread acceptThread_;
            std::thread timerThread_;
            std::atomic<size_t> activeClientHandlers_{0};
            std::mutex clientHandlersMutex_;
            std::condition_variable clientHandlersCv_;

            Clock::time_point nextElectionDeadline() {
                const auto configuredAckTimeoutMs = config_.consistency().ackTimeoutMs;
                const auto baseTimeoutMs = std::clamp<uint32_t>(std::max<uint32_t>(3000u, configuredAckTimeoutMs * 2u), 3000u, 6000u);
                // Test hook: keep production Raft randomized, but let smoke tests make leadership reproducible.
                if (const char* deterministic = std::getenv("AKKARADB_TEST_DETERMINISTIC_RAFT_ELECTION");
                    deterministic != nullptr && std::string_view{deterministic} == "1") {
                    uint32_t rank = 0;
                    for (const auto& node : config_.dataNodes()) {
                        if (node.nodeId < selfNodeId_) { ++rank; }
                    }
                    return Clock::now() + std::chrono::milliseconds{baseTimeoutMs + rank * 250u};
                }
                static thread_local std::mt19937_64 rng{std::random_device{}()};
                std::uniform_int_distribution<int> dist(static_cast<int>(baseTimeoutMs), static_cast<int>(baseTimeoutMs * 2u));
                return Clock::now() + std::chrono::milliseconds(dist(rng));
            }

            bool onlineMembershipChangeEnabled() const noexcept {
                const auto raft = config_.raft();
                return raft.membership.mode == RaftMembershipMode::JOINT_CONSENSUS && raft.membership.allowOnlineVoterChanges;
            }

            static bool containsNode(const std::vector<NodeInfo>& nodes, uint64_t nodeId) {
                for (const auto& node : nodes) { if (node.nodeId == nodeId) { return true; } }
                return false;
            }

            static std::vector<NodeInfo> sortedUniqueVoters(std::vector<NodeInfo> nodes) {
                std::ranges::sort(nodes, [](const NodeInfo& lhs, const NodeInfo& rhs) { return lhs.nodeId < rhs.nodeId; });
                nodes.erase(
                    std::ranges::unique(
                        nodes,
                        [](const NodeInfo& lhs, const NodeInfo& rhs) { return lhs.nodeId == rhs.nodeId; }
                    ).begin(),
                    nodes.end()
                );
                return nodes;
            }

            std::vector<NodeInfo> replicationTargetsLocked() const {
                std::vector<NodeInfo> out = committedVoters_;
                if (jointNewVoters_) { out.insert(out.end(), jointNewVoters_->begin(), jointNewVoters_->end()); }
                out = sortedUniqueVoters(std::move(out));
                return out;
            }

            void refreshPeersFromMembershipLocked() {
                peers_.clear();
                for (const auto& node : replicationTargetsLocked()) { if (node.nodeId != selfNodeId_) { peers_.push_back(node); } }
            }

            void ensurePeerReplicationTargetsLocked(const std::vector<NodeInfo>* targetsOverride = nullptr) {
                if (targetsOverride != nullptr) {
                    peers_.clear();
                    for (const auto& node : *targetsOverride) { if (node.nodeId != selfNodeId_) { peers_.push_back(node); } }
                }
                else { refreshPeersFromMembershipLocked(); }
                for (const auto& peer : peers_) {
                    if (this->peerState(peer.nodeId) == nullptr) {
                        peerReplication_.push_back(PeerReplicationState{.node = peer, .nextIndex = 1, .matchIndex = 0});
                    }
                }
                std::erase_if(
                    peerReplication_,
                    [&](const PeerReplicationState& state) { return !containsNode(peers_, state.node.nodeId); }
                );
            }

            bool replicatedByMajorityLocked(const std::vector<NodeInfo>& voters, uint64_t index) {
                size_t replicated = 0;
                for (const auto& voter : voters) {
                    if (voter.nodeId == selfNodeId_) { if (index <= this->lastLogIndex()) { ++replicated; } }
                    else if (const auto* state = this->peerState(voter.nodeId); state != nullptr && state->matchIndex >= index) { ++replicated; }
                }
                return replicated >= (voters.size() / 2) + 1;
            }

            static bool votedByMajority(const std::vector<NodeInfo>& voters, const std::vector<uint64_t>& grantedVotes) {
                size_t granted = 0;
                for (const auto& voter : voters) {
                    if (std::find(grantedVotes.begin(), grantedVotes.end(), voter.nodeId) != grantedVotes.end()) { ++granted; }
                }
                return granted >= (voters.size() / 2) + 1;
            }

            bool hasElectionQuorumLocked(const std::vector<uint64_t>& grantedVotes) const {
                if (jointOldVoters_ && jointNewVoters_) {
                    return votedByMajority(*jointOldVoters_, grantedVotes) && votedByMajority(*jointNewVoters_, grantedVotes);
                }
                return votedByMajority(committedVoters_, grantedVotes);
            }

            bool hasCommitQuorumLocked(uint64_t index, const RaftLogEntry* entry = nullptr) {
                if (entry != nullptr && entry->kind == RaftEntryKind::CONFIG_JOINT) {
                    std::vector<NodeInfo> oldVoters;
                    std::vector<NodeInfo> newVoters;
                    if (!decodeNodeSet(entry->key, oldVoters) || !decodeNodeSet(entry->value, newVoters)) { return false; }
                    return replicatedByMajorityLocked(oldVoters, index) && replicatedByMajorityLocked(newVoters, index);
                }
                if (entry != nullptr && entry->kind == RaftEntryKind::CONFIG_FINAL && !entry->key.empty()) {
                    std::vector<NodeInfo> oldVoters;
                    std::vector<NodeInfo> newVoters;
                    if (!decodeNodeSet(entry->key, oldVoters) || !decodeNodeSet(entry->value, newVoters)) { return false; }
                    return replicatedByMajorityLocked(oldVoters, index) && replicatedByMajorityLocked(newVoters, index);
                }
                if (jointOldVoters_ && jointNewVoters_) {
                    return replicatedByMajorityLocked(*jointOldVoters_, index) && replicatedByMajorityLocked(*jointNewVoters_, index);
                }
                return replicatedByMajorityLocked(committedVoters_, index);
            }

            bool canCommitEntryLocked(const RaftLogEntry& entry) {
                return entry.term == currentTerm_ && hasCommitQuorumLocked(entry.index, &entry);
            }

            static bool sameNodeSet(const std::vector<NodeInfo>& lhs, const std::vector<NodeInfo>& rhs) {
                return encodeNodeSet(lhs) == encodeNodeSet(rhs);
            }

            static bool sameOptionalNodeSet(
                const std::optional<std::vector<NodeInfo>>& lhs,
                const std::optional<std::vector<NodeInfo>>& rhs
            ) {
                if (lhs.has_value() != rhs.has_value()) { return false; }
                return !lhs || sameNodeSet(*lhs, *rhs);
            }

            std::optional<std::vector<NodeInfo>> replicationTargetsForEntry(const RaftLogEntry& entry) const {
                if (entry.kind == RaftEntryKind::CONFIG_JOINT) {
                    std::vector<NodeInfo> oldVoters;
                    std::vector<NodeInfo> newVoters;
                    if (!decodeNodeSet(entry.key, oldVoters) || !decodeNodeSet(entry.value, newVoters)) { return std::nullopt; }
                    oldVoters.insert(oldVoters.end(), newVoters.begin(), newVoters.end());
                    return sortedUniqueVoters(std::move(oldVoters));
                }
                if (entry.kind == RaftEntryKind::CONFIG_FINAL) {
                    std::vector<NodeInfo> oldVoters;
                    std::vector<NodeInfo> newVoters;
                    if (!decodeNodeSet(entry.value, newVoters)) { return std::nullopt; }
                    if (!entry.key.empty()) {
                        if (!decodeNodeSet(entry.key, oldVoters)) { return std::nullopt; }
                        oldVoters.insert(oldVoters.end(), newVoters.begin(), newVoters.end());
                        return sortedUniqueVoters(std::move(oldVoters));
                    }
                    return sortedUniqueVoters(std::move(newVoters));
                }
                return std::nullopt;
            }

            bool isVotingMemberLocked(uint64_t nodeId) const {
                if (containsNode(committedVoters_, nodeId)) { return true; }
                if (jointOldVoters_ && containsNode(*jointOldVoters_, nodeId)) { return true; }
                if (jointNewVoters_ && containsNode(*jointNewVoters_, nodeId)) { return true; }
                return false;
            }

            int rpcTimeoutMs(int fallbackMs) const {
                const auto configured = config_.consistency().ackTimeoutMs;
                if (configured == 0) { return fallbackMs; }
                return static_cast<int>(std::clamp<uint32_t>(configured, 100, static_cast<uint32_t>(fallbackMs)));
            }

            uint64_t lastLogIndex() const noexcept { return log_.empty() ? lastIncludedIndex_ : log_.back().index; }
            uint64_t lastLogTerm() const noexcept { return log_.empty() ? lastIncludedTerm_ : log_.back().term; }

            std::optional<RaftLogEntry> entryAt(uint64_t index) const {
                if (index <= lastIncludedIndex_) { return std::nullopt; }
                for (const auto& entry : log_) { if (entry.index == index) { return entry; } }
                return std::nullopt;
            }

            uint64_t termAt(uint64_t index) const {
                if (index == 0) { return 0; }
                if (index == lastIncludedIndex_) { return lastIncludedTerm_; }
                if (index < lastIncludedIndex_) { return 0; }
                const auto entry = entryAt(index);
                return entry ? entry->term : 0;
            }

            std::vector<RaftLogEntry> entriesFrom(uint64_t nextIndex) const {
                std::vector<RaftLogEntry> entries;
                size_t payloadSize = RAFT_APPEND_ENTRIES_BASE_SIZE;
                for (const auto& entry : log_) {
                    if (entry.index < nextIndex) { continue; }
                    const auto entryPayload = encodeEntryPayload(entry);
                    const size_t entrySize = RAFT_ENTRY_LENGTH_PREFIX_SIZE + entryPayload.size();
                    if (payloadSize + entrySize > ReplFrameHeader::MAX_PAYLOAD_SIZE) {
                        if (entries.empty()) { return {}; }
                        break;
                    }
                    payloadSize += entrySize;
                    entries.push_back(entry);
                }
                return entries;
            }

            std::optional<uint64_t> compactableLogIndexForSnapshotSeq(uint64_t snapshotSeq) const {
                std::optional<uint64_t> target;
                for (const auto& entry : log_) {
                    if ((entry.kind == RaftEntryKind::MUTATION || entry.kind == RaftEntryKind::BLOB) && entry.clientSeq <= snapshotSeq) {
                        target = entry.index;
                        continue;
                    }
                    if (entry.kind == RaftEntryKind::MUTATION || entry.kind == RaftEntryKind::BLOB) {
                        return target;
                    }
                    if (entry.kind == RaftEntryKind::CONFIG_JOINT || entry.kind == RaftEntryKind::CONFIG_FINAL ||
                        entry.kind == RaftEntryKind::NOOP) {
                        target = entry.index;
                    }
                }
                return target;
            }

            std::optional<uint64_t> appliedLogIndexForClientSeq(uint64_t clientSeq) const {
                std::optional<uint64_t> target = lastIncludedIndex_;
                for (const auto& entry : log_) {
                    if (entry.index > commitIndex_) { break; }
                    if (entry.kind == RaftEntryKind::MUTATION || entry.kind == RaftEntryKind::BLOB) {
                        if (entry.clientSeq <= clientSeq) {
                            target = entry.index;
                            continue;
                        }
                        return target;
                    }
                    target = entry.index;
                }
                return target;
            }

            uint64_t firstIndexOfTermLocked(uint64_t term, uint64_t atOrBeforeIndex) const {
                if (term == 0) { return 0; }
                if (lastIncludedIndex_ != 0 && lastIncludedIndex_ <= atOrBeforeIndex && lastIncludedTerm_ == term) { return lastIncludedIndex_; }
                for (const auto& entry : log_) {
                    if (entry.index > atOrBeforeIndex) { break; }
                    if (entry.term == term) { return entry.index; }
                }
                return 0;
            }

            uint64_t lastIndexOfTermLocked(uint64_t term) const {
                if (term == 0) { return 0; }
                uint64_t out = 0;
                if (lastIncludedIndex_ != 0 && lastIncludedTerm_ == term) { out = lastIncludedIndex_; }
                for (const auto& entry : log_) {
                    if (entry.term == term) { out = entry.index; }
                    else if (out != 0 && entry.term > term) { break; }
                }
                return out;
            }

            AppendEntriesResponse conflictResponseLocked(uint64_t prevLogIndex) const {
                if (prevLogIndex > lastLogIndex()) {
                    return AppendEntriesResponse{
                        .term = currentTerm_,
                        .success = false,
                        .matchIndex = lastLogIndex(),
                        .conflictIndex = lastLogIndex() + 1,
                        .conflictTerm = 0
                    };
                }
                if (prevLogIndex <= lastIncludedIndex_) {
                    return AppendEntriesResponse{
                        .term = currentTerm_,
                        .success = false,
                        .matchIndex = lastIncludedIndex_,
                        .conflictIndex = lastIncludedIndex_ + 1,
                        .conflictTerm = lastIncludedTerm_
                    };
                }
                const uint64_t conflictTerm = termAt(prevLogIndex);
                const uint64_t firstConflictIndex = firstIndexOfTermLocked(conflictTerm, prevLogIndex);
                return AppendEntriesResponse{
                    .term = currentTerm_,
                    .success = false,
                    .matchIndex = firstConflictIndex == 0 ? 0 : firstConflictIndex - 1,
                    .conflictIndex = firstConflictIndex == 0 ? prevLogIndex : firstConflictIndex,
                    .conflictTerm = conflictTerm
                };
            }

            void updateNextIndexAfterConflictLocked(PeerReplicationState& state, const AppendEntriesResponse& response) const {
                uint64_t nextIndex = 1;
                if (response.conflictTerm != 0) {
                    if (const uint64_t leaderLastIndexForTerm = lastIndexOfTermLocked(response.conflictTerm); leaderLastIndexForTerm != 0) {
                        nextIndex = leaderLastIndexForTerm + 1;
                    }
                    else if (response.conflictIndex != 0) { nextIndex = response.conflictIndex; }
                    else { nextIndex = response.matchIndex + 1; }
                }
                else if (response.conflictIndex != 0) { nextIndex = response.conflictIndex; }
                else { nextIndex = response.matchIndex + 1; }
                if (nextIndex >= state.nextIndex && state.nextIndex > 1) { nextIndex = state.nextIndex - 1; }
                state.nextIndex = std::max<uint64_t>(1, nextIndex);
            }

            RaftLogEntry makeNoopEntryLocked() const {
                RaftLogEntry entry;
                entry.term = currentTerm_;
                entry.index = lastLogIndex() + 1;
                entry.clientSeq = 0;
                entry.kind = RaftEntryKind::NOOP;
                entry.op = ReplOpType::PUT;
                entry.sourceNodeId = selfNodeId_;
                return entry;
            }

            void persistState() {
                std::vector<uint8_t> bytes;
                bytes.insert(bytes.end(), {'A', 'K', 'R', 'S', '2'});
                writeU64(bytes, currentTerm_);
                writeU64(bytes, votedFor_);
                const size_t crcOffset = bytes.size();
                writeU32(bytes, 0);
                writeU32At(bytes, crcOffset, crcWithZeroedField(bytes, crcOffset));
                writeFileAtomically(statePath_, bytes, "RaftConsensusRuntime state");
            }

            void enforceLogInvariantsLocked(const char* context) const {
                if (committedVoters_.empty()) { throw std::runtime_error(std::string{context} + ": empty committed voter set"); }
                if (lastIncludedIndex_ > commitIndex_) {
                    throw std::runtime_error(std::string{context} + ": lastIncludedIndex is ahead of commitIndex");
                }
                if (lastApplied_ < lastIncludedIndex_) {
                    throw std::runtime_error(std::string{context} + ": lastApplied is behind lastIncludedIndex");
                }
                if (lastApplied_ > commitIndex_) { throw std::runtime_error(std::string{context} + ": lastApplied is ahead of commitIndex"); }
                if (commitIndex_ > lastLogIndex()) { throw std::runtime_error(std::string{context} + ": commitIndex is ahead of log"); }
                uint64_t expectedIndex = lastIncludedIndex_ + 1;
                for (const auto& entry : log_) {
                    if (entry.index != expectedIndex) { throw std::runtime_error(std::string{context} + ": non-contiguous log"); }
                    ++expectedIndex;
                }
                if (jointOldVoters_.has_value() != jointNewVoters_.has_value()) {
                    throw std::runtime_error(std::string{context} + ": incomplete joint voter set");
                }
            }

            void recoverState() {
                if (!std::filesystem::exists(statePath_)) { return; }
                try {
                    const auto bytes = readWholeFile(statePath_, "RaftConsensusRuntime state");
                    constexpr size_t expectedSize = 5 + 8 + 8 + 4;
                    constexpr size_t crcOffset = expectedSize - 4;
                    if (bytes.size() != expectedSize) { throw std::runtime_error("invalid state file size"); }
                    if (std::string_view{reinterpret_cast<const char*>(bytes.data()), 5} != "AKRS2") {
                        throw std::runtime_error("bad state file magic");
                    }
                    if (readU32(bytes, crcOffset) != crcWithZeroedField(bytes, crcOffset)) {
                        throw std::runtime_error("state file CRC mismatch");
                    }
                    currentTerm_ = readU64(bytes, 5);
                    votedFor_ = readU64(bytes, 13);
                }
                catch (const std::exception& ex) {
                    throw std::runtime_error(std::string{"RaftConsensusRuntime: corrupt hard state: "} + ex.what());
                }
            }

            void persistLog() {
                enforceLogInvariantsLocked("RaftConsensusRuntime log");
                std::vector<uint8_t> bytes;
                bytes.insert(bytes.end(), {'A', 'K', 'R', 'L', '1'});
                writeU64(bytes, commitIndex_);
                writeU64(bytes, lastApplied_);
                writeU64(bytes, lastIncludedIndex_);
                writeU64(bytes, lastIncludedTerm_);
                writeU64(bytes, static_cast<uint64_t>(log_.size()));
                writeNodeSetField(bytes, committedVoters_);
                writeOptionalNodeSetField(bytes, jointOldVoters_);
                writeOptionalNodeSetField(bytes, jointNewVoters_);
                const size_t headerCrcOffset = bytes.size();
                writeU32(bytes, 0);
                writeU32At(bytes, headerCrcOffset, crcBytes(std::span<const uint8_t>{bytes.data(), headerCrcOffset}));
                for (const auto& entry : log_) {
                    const auto payload = encodeEntryPayload(entry);
                    writeU64(bytes, static_cast<uint64_t>(payload.size()));
                    writeU32(bytes, crcBytes(payload));
                    bytes.insert(bytes.end(), payload.begin(), payload.end());
                }
                writeFileAtomically(logPath_, bytes, "RaftConsensusRuntime log");
            }

            void recoverLog() {
                if (!std::filesystem::exists(logPath_)) { return; }
                const auto bytes = readWholeFile(logPath_, "RaftConsensusRuntime log");
                constexpr size_t fixedHeaderSize = 5 + 8 + 8 + 8 + 8 + 8;
                if (bytes.size() < fixedHeaderSize + 4) { throw std::runtime_error("RaftConsensusRuntime: truncated log file"); }
                if (std::string_view{reinterpret_cast<const char*>(bytes.data()), 5} != "AKRL1") {
                    throw std::runtime_error("RaftConsensusRuntime: bad log file");
                }
                commitIndex_ = readU64(bytes, 5);
                lastApplied_ = readU64(bytes, 13);
                lastIncludedIndex_ = readU64(bytes, 21);
                lastIncludedTerm_ = readU64(bytes, 29);
                const uint64_t count = readU64(bytes, 37);
                size_t cursor = fixedHeaderSize;
                if (!readNodeSetField(bytes, cursor, committedVoters_) ||
                    !readOptionalNodeSetField(bytes, cursor, jointOldVoters_) ||
                    !readOptionalNodeSetField(bytes, cursor, jointNewVoters_)) {
                    throw std::runtime_error("RaftConsensusRuntime: corrupt log membership header");
                }
                if (committedVoters_.empty() || cursor + 4 > bytes.size()) {
                    throw std::runtime_error("RaftConsensusRuntime: invalid log membership header");
                }
                const size_t headerCrcOffset = cursor;
                if (readU32(bytes, headerCrcOffset) != crcBytes(std::span<const uint8_t>{bytes.data(), headerCrcOffset})) {
                    throw std::runtime_error("RaftConsensusRuntime: log header CRC mismatch");
                }
                cursor += 4;

                uint64_t lastGoodIndex = lastIncludedIndex_;
                bool truncatedTail = false;
                const auto canTruncateTail = [&] {
                    return runtimeOptions_.raftLogRecoveryAction == RaftLogRecoveryAction::TRUNCATE_UNCOMMITTED_TAIL &&
                           commitIndex_ <= lastGoodIndex;
                };
                const auto failOrTruncate = [&](const char* message) {
                    if (canTruncateTail()) {
                        truncatedTail = true;
                        return;
                    }
                    throw std::runtime_error(std::string{"RaftConsensusRuntime: "} + message);
                };

                for (uint64_t i = 0; i < count; ++i) {
                    if (cursor + 12 > bytes.size()) {
                        failOrTruncate("truncated log entry header");
                        break;
                    }
                    const uint64_t len = readU64(bytes, cursor);
                    cursor += 8;
                    const uint32_t storedCrc = readU32(bytes, cursor);
                    cursor += 4;
                    if (len > ReplFrameHeader::MAX_PAYLOAD_SIZE || len > bytes.size() - cursor) {
                        failOrTruncate("truncated log entry payload");
                        break;
                    }
                    const auto payload = std::span<const uint8_t>{bytes.data() + cursor, static_cast<size_t>(len)};
                    cursor += static_cast<size_t>(len);
                    if (storedCrc != crcBytes(payload)) {
                        failOrTruncate("log entry CRC mismatch");
                        break;
                    }
                    size_t payloadCursor = 0;
                    RaftLogEntry entry;
                    if (!decodeEntryPayload(payload, payloadCursor, entry) || payloadCursor != payload.size()) {
                        failOrTruncate("corrupt log entry");
                        break;
                    }
                    if (entry.index <= lastIncludedIndex_ || entry.index != lastGoodIndex + 1) {
                        failOrTruncate("non-contiguous log entry");
                        break;
                    }
                    if (entry.kind != RaftEntryKind::MUTATION && entry.kind != RaftEntryKind::CONFIG_JOINT && entry.kind !=
                        RaftEntryKind::CONFIG_FINAL && entry.kind != RaftEntryKind::BLOB && entry.kind != RaftEntryKind::NOOP) {
                        failOrTruncate("invalid log entry kind");
                        break;
                    }
                    if (entry.kind == RaftEntryKind::BLOB) {
                        RaftBlobChunk chunk;
                        if (entry.op != ReplOpType::PUT || entry.flags != 0 || !decodeBlobChunkKey(entry.key, chunk) ||
                            chunk.offset + entry.value.size() > chunk.totalSize) {
                            failOrTruncate("invalid blob log entry");
                            break;
                        }
                    }
                    else if (entry.kind != RaftEntryKind::NOOP && entry.op != ReplOpType::PUT && entry.op != ReplOpType::REMOVE) {
                        failOrTruncate("invalid log entry operation");
                        break;
                    }
                    lastGoodIndex = entry.index;
                    log_.push_back(std::move(entry));
                }
                if (!truncatedTail && cursor != bytes.size()) {
                    failOrTruncate("trailing log bytes");
                }
                if (commitIndex_ > lastGoodIndex) {
                    throw std::runtime_error("RaftConsensusRuntime: committed log entry is missing or corrupt");
                }
                if (lastIncludedIndex_ > commitIndex_ || lastApplied_ > commitIndex_) {
                    throw std::runtime_error("RaftConsensusRuntime: invalid applied/commit index metadata");
                }
                if (lastApplied_ < lastIncludedIndex_) { lastApplied_ = lastIncludedIndex_; }
                if (truncatedTail) { persistLog(); }
            }

            void recoverAppliedProgressFromStateMachine() {
                if (!callbacks_.getLastSeq) { return; }
                const uint64_t durableClientSeq = callbacks_.getLastSeq();
                bool advanced = false;
                {
                    std::lock_guard lock{mutex_};
                    const auto recoveredAppliedIndex = appliedLogIndexForClientSeq(durableClientSeq);
                    if (recoveredAppliedIndex && *recoveredAppliedIndex > lastApplied_) {
                        lastApplied_ = std::min(*recoveredAppliedIndex, commitIndex_);
                        advanced = true;
                    }
                }
                if (advanced) {
                    std::lock_guard lock{mutex_};
                    persistLog();
                }
            }

            std::optional<NodeRole> setRole(RaftRole role) {
                const auto old = role_.exchange(role);
                if (old == role) { return std::nullopt; }
                return role == RaftRole::LEADER ? NodeRole::PRIMARY : NodeRole::REPLICA;
            }

            void notifyRoleChange(std::optional<NodeRole> role) {
                if (role && callbacks_.roleChange) { callbacks_.roleChange(*role); }
            }

            void setRoleAndNotify(RaftRole role) { notifyRoleChange(setRole(role)); }

            void becomeFollower(uint64_t term) {
                bool changed = false;
                {
                    std::lock_guard lock{mutex_};
                    if (term > currentTerm_) {
                        currentTerm_ = term;
                        votedFor_ = 0;
                        persistState();
                        changed = true;
                    }
                    electionDeadline_ = nextElectionDeadline();
                }
                setRoleAndNotify(RaftRole::FOLLOWER);
                (void)changed;
            }

            void becomeLeader(uint64_t term) {
                {
                    std::lock_guard lock{mutex_};
                    currentTerm_ = std::max(currentTerm_, term);
                    votedFor_ = selfNodeId_;
                    persistState();
                    resetLeaderReplicationState();
                    log_.push_back(makeNoopEntryLocked());
                    persistLog();
                    electionDeadline_ = Clock::now() + std::chrono::hours(24);
                }
                setRoleAndNotify(RaftRole::LEADER);
                (void)commitOutstandingEntry(std::chrono::milliseconds{0});
            }

            void resetLeaderReplicationState() {
                refreshPeersFromMembershipLocked();
                peerReplication_.clear();
                peerReplication_.reserve(peers_.size());
                const uint64_t next = lastLogIndex() + 1;
                for (const auto& peer : peers_) {
                    peerReplication_.push_back(PeerReplicationState{.node = peer, .nextIndex = next, .matchIndex = 0});
                }
            }

            PeerReplicationState* peerState(uint64_t nodeId) {
                for (auto& state : peerReplication_) { if (state.node.nodeId == nodeId) { return &state; } }
                return nullptr;
            }

            void timerLoop() {
                while (true) {
                    {
                        std::unique_lock lock{mutex_};
                        if (!running_) { return; }
                        if (role_.load() == RaftRole::LEADER) {
                            lock.unlock();
                            sendHeartbeats();
                            std::this_thread::sleep_for(std::chrono::milliseconds(100));
                            continue;
                        }
                        if (Clock::now() < electionDeadline_) {
                            cv_.wait_until(lock, electionDeadline_);
                            continue;
                        }
                    }
                    startElection();
                }
            }

            void startElection() {
                uint64_t term = 0;
                uint64_t lastIndex = 0;
                uint64_t lastTerm = 0;
                std::vector<NodeInfo> electionPeers;
                {
                    std::lock_guard lock{mutex_};
                    if (!running_) { return; }
                    if (!self_->coordinatorEligible() || !isVotingMemberLocked(selfNodeId_)) {
                        electionDeadline_ = nextElectionDeadline();
                        return;
                    }
                    role_.store(RaftRole::CANDIDATE);
                    ++currentTerm_;
                    term = currentTerm_;
                    votedFor_ = selfNodeId_;
                    lastIndex = lastLogIndex();
                    lastTerm = lastLogTerm();
                    electionDeadline_ = nextElectionDeadline();
                    refreshPeersFromMembershipLocked();
                    electionPeers = peers_;
                    persistState();
                }

                std::mutex votesMutex;
                std::vector<uint64_t> grantedVotes{selfNodeId_};
                std::vector<std::thread> workers;
                for (const auto& peer : electionPeers) {
                    workers.emplace_back(
                        [this, peer, term, lastIndex, lastTerm, &votesMutex, &grantedVotes] {
                            RequestVoteResponse response;
                            if (requestVote(peer, RequestVote{term, selfNodeId_, lastIndex, lastTerm}, response)) {
                                if (response.term > term) {
                                    becomeFollower(response.term);
                                    return;
                                }
                                if (response.voteGranted) {
                                    std::lock_guard lock{votesMutex};
                                    grantedVotes.push_back(peer.nodeId);
                                }
                            }
                        }
                    );
                }
                for (auto& worker : workers) { if (worker.joinable()) { worker.join(); } }

                bool won = false;
                {
                    std::lock_guard lock{mutex_};
                    std::lock_guard votesLock{votesMutex};
                    won = running_ && currentTerm_ == term && role_.load() == RaftRole::CANDIDATE && hasElectionQuorumLocked(grantedVotes);
                }
                if (won) { becomeLeader(term); }
            }

            bool requestVote(const NodeInfo& peer, const RequestVote& request, RequestVoteResponse& response) {
                SocketHandle socket = connectTo(peer.host, peer.replPort, rpcTimeoutMs(500));
                if (!socketOk(socket)) { return false; }
                const auto close = [&] { closeSocket(socket); };
                std::unique_ptr<crypto::SecureSession> secure;
                if (runtimeOptions_.transportMode == TransportMode::SECURE) {
                    secure = openSecureSession(socket, peer.nodeId);
                    if (!secure) {
                        close();
                        return false;
                    }
                }
                if (!sendFrame(socket, secure.get(), encodeRequestVote(request))) {
                    close();
                    return false;
                }
                DecodedFrame frame;
                if (!recvFrame(socket, secure.get(), frame) || frame.type != ReplMsgType::RAFT_REQUEST_VOTE_RESPONSE) {
                    close();
                    return false;
                }
                close();
                return decodeRequestVoteResponse(frame.payload, response);
            }

            bool appendEntries(const NodeInfo& peer, const AppendEntries& request, AppendEntriesResponse& response) {
                SocketHandle socket = connectTo(peer.host, peer.replPort, rpcTimeoutMs(1000));
                if (!socketOk(socket)) { return false; }
                const auto close = [&] { closeSocket(socket); };
                std::unique_ptr<crypto::SecureSession> secure;
                if (runtimeOptions_.transportMode == TransportMode::SECURE) {
                    secure = openSecureSession(socket, peer.nodeId);
                    if (!secure) {
                        close();
                        return false;
                    }
                }
                const auto wire = encodeAppendEntries(request);
                if (wire.empty() || !sendFrame(socket, secure.get(), wire)) {
                    close();
                    return false;
                }
                DecodedFrame frame;
                if (!recvFrame(socket, secure.get(), frame) || frame.type != ReplMsgType::RAFT_APPEND_ENTRIES_RESPONSE) {
                    close();
                    return false;
                }
                close();
                return decodeAppendEntriesResponse(frame.payload, response);
            }

            bool installSnapshot(const NodeInfo& peer, const InstallSnapshot& request, InstallSnapshotResponse& response) {
                SocketHandle socket = connectTo(peer.host, peer.replPort, rpcTimeoutMs(2000));
                if (!socketOk(socket)) { return false; }
                const auto close = [&] { closeSocket(socket); };
                std::unique_ptr<crypto::SecureSession> secure;
                if (runtimeOptions_.transportMode == TransportMode::SECURE) {
                    secure = openSecureSession(socket, peer.nodeId);
                    if (!secure) {
                        close();
                        return false;
                    }
                }
                const auto wire = encodeInstallSnapshot(request);
                if (wire.empty() || !sendFrame(socket, secure.get(), wire)) {
                    close();
                    return false;
                }
                DecodedFrame frame;
                if (!recvFrame(socket, secure.get(), frame) || frame.type != ReplMsgType::RAFT_INSTALL_SNAPSHOT_RESPONSE) {
                    close();
                    return false;
                }
                close();
                return decodeInstallSnapshotResponse(frame.payload, response);
            }

            bool timeoutNow(const NodeInfo& peer, const TimeoutNow& request, TimeoutNowResponse& response) {
                SocketHandle socket = connectTo(peer.host, peer.replPort, rpcTimeoutMs(500));
                if (!socketOk(socket)) { return false; }
                const auto close = [&] { closeSocket(socket); };
                std::unique_ptr<crypto::SecureSession> secure;
                if (runtimeOptions_.transportMode == TransportMode::SECURE) {
                    secure = openSecureSession(socket, peer.nodeId);
                    if (!secure) {
                        close();
                        return false;
                    }
                }
                if (!sendFrame(socket, secure.get(), encodeTimeoutNow(request))) {
                    close();
                    return false;
                }
                DecodedFrame frame;
                if (!recvFrame(socket, secure.get(), frame) || frame.type != ReplMsgType::RAFT_TIMEOUT_NOW_RESPONSE) {
                    close();
                    return false;
                }
                close();
                return decodeTimeoutNowResponse(frame.payload, response);
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

            bool buildInstallSnapshotRequests(uint64_t term, std::vector<InstallSnapshot>& out) {
                if (!callbacks_.exportSnapshot) { return false; }
                const auto snapshot = callbacks_.exportSnapshot();
                if (!snapshot || snapshot->seq == 0) { return false; }
                uint64_t logIndexValue = 0;
                uint64_t logTermValue = 0;
                std::vector<NodeInfo> committedVoters;
                std::optional<std::vector<NodeInfo>> jointOldVoters;
                std::optional<std::vector<NodeInfo>> jointNewVoters;
                {
                    std::lock_guard lock{mutex_};
                    auto logIndex = compactableLogIndexForSnapshotSeq(snapshot->seq);
                    if (!logIndex && lastIncludedIndex_ > 0) { logIndex = lastIncludedIndex_; }
                    if (!logIndex || *logIndex > commitIndex_) { return false; }
                    logIndexValue = *logIndex;
                    logTermValue = termAt(logIndexValue);
                    committedVoters = committedVoters_;
                    jointOldVoters = jointOldVoters_;
                    jointNewVoters = jointNewVoters_;
                }
                if (logTermValue == 0) { return false; }

                const auto makeRequest = [&] {
                    InstallSnapshot request;
                    request.term = term;
                    request.leaderId = selfNodeId_;
                    request.lastIncludedIndex = logIndexValue;
                    request.lastIncludedTerm = logTermValue;
                    request.snapshotSeq = snapshot->seq;
                    request.entryCount = snapshot->entries.size();
                    request.committedVoters = committedVoters;
                    request.jointOldVoters = jointOldVoters;
                    request.jointNewVoters = jointNewVoters;
                    return request;
                };

                out.clear();
                InstallSnapshot current = makeRequest();
                const size_t chunkSize = runtimeOptions_.raftBlobChunkSizeBytes;
                for (uint64_t entryIndex = 0; entryIndex < snapshot->entries.size(); ++entryIndex) {
                    const auto& entry = snapshot->entries[static_cast<size_t>(entryIndex)];
                    const uint32_t valueCrc32c = crcBytes(entry.value);
                    size_t offset = 0;
                    bool emittedEntryChunk = false;
                    do {
                        const size_t remaining = entry.value.size() - offset;
                        size_t currentChunkSize = std::min(chunkSize, remaining);
                        auto makeChunk = [&](size_t valueBytes) {
                            SnapshotKvChunk chunk;
                            chunk.entryIndex = entryIndex;
                            chunk.valueOffset = offset;
                            chunk.valueSize = entry.value.size();
                            chunk.valueCrc32c = valueCrc32c;
                            if (offset == 0) { chunk.key = entry.key; }
                            chunk.value.assign(
                                entry.value.begin() + static_cast<std::ptrdiff_t>(offset),
                                entry.value.begin() + static_cast<std::ptrdiff_t>(offset + valueBytes)
                            );
                            return chunk;
                        };
                        SnapshotKvChunk chunk = makeChunk(currentChunkSize);
                        InstallSnapshot candidate = current;
                        candidate.chunks.push_back(chunk);
                        if (encodeInstallSnapshot(candidate).empty() && !current.chunks.empty()) {
                            out.push_back(std::move(current));
                            current = makeRequest();
                            candidate = current;
                            candidate.chunks.push_back(chunk);
                        }
                        if (encodeInstallSnapshot(candidate).empty()) {
                            if (currentChunkSize == 0) { return false; }
                            size_t low = 1;
                            size_t high = currentChunkSize;
                            size_t best = 0;
                            while (low <= high) {
                                const size_t mid = low + (high - low) / 2;
                                chunk = makeChunk(mid);
                                candidate = current;
                                candidate.chunks.push_back(chunk);
                                if (encodeInstallSnapshot(candidate).empty()) {
                                    high = mid - 1;
                                }
                                else {
                                    best = mid;
                                    low = mid + 1;
                                }
                            }
                            if (best == 0) { return false; }
                            currentChunkSize = best;
                            chunk = makeChunk(currentChunkSize);
                            candidate = current;
                            candidate.chunks.push_back(std::move(chunk));
                        }
                        current = std::move(candidate);
                        offset += currentChunkSize;
                        emittedEntryChunk = true;
                    }
                    while (offset < entry.value.size() || !emittedEntryChunk);
                }
                if (!current.chunks.empty() || snapshot->entries.empty()) { out.push_back(std::move(current)); }
                if (out.empty()) { return false; }
                out.back().done = true;
                return true;
            }

            void maybeCompactLog() {
                if (!callbacks_.exportSnapshot) { return; }
                const auto snapshot = callbacks_.exportSnapshot();
                if (!snapshot || snapshot->seq == 0) { return; }
                std::lock_guard lock{mutex_};
                const auto logIndex = compactableLogIndexForSnapshotSeq(snapshot->seq);
                if (!logIndex || *logIndex <= lastIncludedIndex_ || *logIndex > commitIndex_) { return; }
                const uint64_t includedTerm = termAt(*logIndex);
                if (includedTerm == 0) { return; }
                compactLogThrough(*logIndex, includedTerm);
                persistLog();
            }

            void compactLogThrough(uint64_t index, uint64_t term) {
                if (index <= lastIncludedIndex_) { return; }
                log_.erase(
                    std::ranges::remove_if(log_, [&](const RaftLogEntry& entry) { return entry.index <= index; }).begin(),
                    log_.end()
                );
                lastIncludedIndex_ = index;
                lastIncludedTerm_ = term;
                if (commitIndex_ < lastIncludedIndex_) { commitIndex_ = lastIncludedIndex_; }
                if (lastApplied_ < lastIncludedIndex_) { lastApplied_ = lastIncludedIndex_; }
            }

            void proposeMembershipChange(const std::vector<NodeInfo>& oldVoters, const std::vector<NodeInfo>& newVoters) {
                RaftLogEntry joint = makeConfigEntry(RaftEntryKind::CONFIG_JOINT, oldVoters, newVoters);
                appendReplicateAndCommitConfig(joint);
                RaftLogEntry final = makeConfigEntry(RaftEntryKind::CONFIG_FINAL, oldVoters, newVoters);
                appendReplicateAndCommitConfig(final);
            }

            RaftLogEntry makeConfigEntry(
                RaftEntryKind kind,
                const std::vector<NodeInfo>& oldVoters,
                const std::vector<NodeInfo>& newVoters
            ) {
                std::lock_guard lock{mutex_};
                if (commitIndex_ != lastLogIndex()) {
                    throw std::runtime_error("RaftConsensusRuntime: cannot change membership while a prior entry is uncommitted");
                }
                RaftLogEntry entry;
                entry.term = currentTerm_;
                entry.index = lastLogIndex() + 1;
                entry.clientSeq = 0;
                entry.kind = kind;
                entry.key = encodeNodeSet(oldVoters);
                entry.value = encodeNodeSet(newVoters);
                return entry;
            }

            void appendReplicateAndCommitConfig(const RaftLogEntry& entry, bool appendEntry = true) {
                uint64_t term = 0;
                {
                    std::lock_guard lock{mutex_};
                    if (role_.load() != RaftRole::LEADER) {
                        throw std::runtime_error("RaftConsensusRuntime: local node is not Raft leader");
                    }
                    term = currentTerm_;
                    if (appendEntry) { log_.push_back(entry); }
                    if (entry.kind == RaftEntryKind::CONFIG_JOINT) {
                        std::vector<NodeInfo> oldVoters;
                        std::vector<NodeInfo> newVoters;
                        if (!decodeNodeSet(entry.key, oldVoters) || !decodeNodeSet(entry.value, newVoters)) {
                            throw std::runtime_error("RaftConsensusRuntime: corrupt joint membership entry");
                        }
                        auto targets = oldVoters;
                        targets.insert(targets.end(), newVoters.begin(), newVoters.end());
                        targets = sortedUniqueVoters(std::move(targets));
                        for (const auto& node : targets) {
                            if (node.nodeId != selfNodeId_ && peerState(node.nodeId) == nullptr) {
                                peerReplication_.push_back(PeerReplicationState{.node = node, .nextIndex = 1, .matchIndex = 0});
                            }
                        }
                        peers_.clear();
                        for (const auto& node : targets) { if (node.nodeId != selfNodeId_) { peers_.push_back(node); } }
                    }
                    persistLog();
                }
                if (!replicateEntryToMajority(entry, std::chrono::milliseconds{20000})) {
                    throw std::runtime_error("RaftConsensusRuntime: failed to replicate membership change to Raft quorum");
                }
                {
                    std::lock_guard lock{mutex_};
                    if (role_.load() != RaftRole::LEADER || currentTerm_ != term) {
                        throw std::runtime_error("RaftConsensusRuntime: leadership changed before membership commit");
                    }
                    commitIndex_ = entry.index;
                    persistLog();
                }
                applyCommitted();
                sendHeartbeats();
            }

            void applyConfigEntryLocked(const RaftLogEntry& entry) {
                if (entry.kind == RaftEntryKind::CONFIG_JOINT) {
                    std::vector<NodeInfo> oldVoters;
                    std::vector<NodeInfo> newVoters;
                    if (!decodeNodeSet(entry.key, oldVoters) || !decodeNodeSet(entry.value, newVoters)) {
                        throw std::runtime_error("RaftConsensusRuntime: corrupt joint membership entry");
                    }
                    jointOldVoters_ = sortedUniqueVoters(std::move(oldVoters));
                    jointNewVoters_ = sortedUniqueVoters(std::move(newVoters));
                }
                else if (entry.kind == RaftEntryKind::CONFIG_FINAL) {
                    std::vector<NodeInfo> newVoters;
                    if (!decodeNodeSet(entry.value, newVoters)) {
                        throw std::runtime_error("RaftConsensusRuntime: corrupt final membership entry");
                    }
                    committedVoters_ = sortedUniqueVoters(std::move(newVoters));
                    jointOldVoters_.reset();
                    jointNewVoters_.reset();
                }
                refreshPeersFromMembershipLocked();
                ensurePeerReplicationTargetsLocked();
            }

            bool applySnapshotMembershipLocked(const InstallSnapshot& request) {
                if (request.committedVoters.empty() || request.jointOldVoters.has_value() != request.jointNewVoters.has_value()) {
                    return false;
                }
                committedVoters_ = sortedUniqueVoters(request.committedVoters);
                if (request.jointOldVoters && request.jointNewVoters) {
                    jointOldVoters_ = sortedUniqueVoters(*request.jointOldVoters);
                    jointNewVoters_ = sortedUniqueVoters(*request.jointNewVoters);
                    if (jointOldVoters_->empty() || jointNewVoters_->empty()) { return false; }
                }
                else {
                    jointOldVoters_.reset();
                    jointNewVoters_.reset();
                }
                refreshPeersFromMembershipLocked();
                ensurePeerReplicationTargetsLocked();
                return true;
            }

            void replayCommittedMembership() {
                for (const auto& entry : log_) {
                    if (entry.index > commitIndex_) { break; }
                    if (entry.kind == RaftEntryKind::CONFIG_JOINT || entry.kind == RaftEntryKind::CONFIG_FINAL) {
                        applyConfigEntryLocked(entry);
                    }
                }
            }

            bool replicateEntryToMajority(const RaftLogEntry& entry, std::chrono::milliseconds retryWindow = std::chrono::milliseconds{0}) {
                const auto deadline = Clock::now() + retryWindow;
                const auto entryTargets = replicationTargetsForEntry(entry);
                do {
                    std::vector<std::thread> workers;
                    std::vector<uint64_t> peerIds;
                    {
                        std::lock_guard lock{mutex_};
                        if (!running_ || role_.load() != RaftRole::LEADER) { return false; }
                        if (peerReplication_.empty()) { resetLeaderReplicationState(); }
                        ensurePeerReplicationTargetsLocked(entryTargets ? &*entryTargets : nullptr);
                        for (const auto& state : peerReplication_) { peerIds.push_back(state.node.nodeId); }
                    }
                    for (const auto peerId : peerIds) {
                        workers.emplace_back([this, peerId, targetIndex = entry.index] { (void)replicatePeerTo(peerId, targetIndex); });
                    }
                    for (auto& worker : workers) { if (worker.joinable()) { worker.join(); } }
                    {
                        std::lock_guard lock{mutex_};
                        if (hasCommitQuorumLocked(entry.index, &entry)) { return true; }
                    }
                    if (retryWindow.count() == 0 || Clock::now() >= deadline) { return false; }
                    std::this_thread::sleep_for(std::chrono::milliseconds{50});
                }
                while (true);
            }

            bool commitOutstandingEntry(std::chrono::milliseconds retryWindow) {
                RaftLogEntry entry;
                uint64_t term = 0;
                {
                    std::lock_guard lock{mutex_};
                    if (!running_ || role_.load() != RaftRole::LEADER || commitIndex_ == lastLogIndex() || log_.empty()) { return true; }
                    entry = log_.back();
                    term = currentTerm_;
                }
                if (!replicateEntryToMajority(entry, retryWindow)) { return false; }
                {
                    std::lock_guard lock{mutex_};
                    if (role_.load() != RaftRole::LEADER || currentTerm_ != term) { return false; }
                    if (entry.index > commitIndex_ && canCommitEntryLocked(entry)) {
                        commitIndex_ = entry.index;
                        persistLog();
                    }
                }
                applyCommitted();
                sendHeartbeats();
                return true;
            }

            bool replicatePeerTo(uint64_t peerId, uint64_t targetIndex, bool forceHeartbeat = false) {
                while (true) {
                    NodeInfo peer;
                    uint64_t requestTerm = 0;
                    auto needsSnapshot = false;
                    AppendEntries appendRequest;
                    {
                        std::lock_guard lock{mutex_};
                        if (!running_ || role_.load() != RaftRole::LEADER) { return false; }
                        auto* state = peerState(peerId);
                        if (state == nullptr) { return false; }
                        if (state->matchIndex >= targetIndex && !forceHeartbeat) { return true; }

                        peer = state->node;
                        requestTerm = currentTerm_;
                        if (state->nextIndex <= lastIncludedIndex_) { needsSnapshot = true; }
                        else {
                            appendRequest.term = currentTerm_;
                            appendRequest.leaderId = selfNodeId_;
                            appendRequest.prevLogIndex = state->nextIndex - 1;
                            appendRequest.prevLogTerm = termAt(appendRequest.prevLogIndex);
                            appendRequest.leaderCommit = commitIndex_;
                            if (state->matchIndex < targetIndex) {
                                appendRequest.entries = entriesFrom(state->nextIndex);
                                if (appendRequest.entries.empty()) { return false; }
                            }
                        }
                    }
                    forceHeartbeat = false;

                    if (needsSnapshot) {
                        std::vector<InstallSnapshot> snapshotRequests;
                        if (!buildInstallSnapshotRequests(requestTerm, snapshotRequests)) { return false; }
                        InstallSnapshotResponse response;
                        for (const auto& snapshotRequest : snapshotRequests) {
                            if (!installSnapshot(peer, snapshotRequest, response)) { return false; }
                            if (response.term > requestTerm) {
                                becomeFollower(response.term);
                                return false;
                            }
                            if (!response.success) { return false; }
                        }
                        std::lock_guard lock{mutex_};
                        if (auto* state = peerState(peerId); state != nullptr) {
                            state->matchIndex = std::max(state->matchIndex, response.lastIncludedIndex);
                            state->nextIndex = state->matchIndex + 1;
                        }
                        continue;
                    }

                    AppendEntriesResponse response;
                    if (!appendEntries(peer, appendRequest, response)) { return false; }
                    if (response.term > appendRequest.term) {
                        becomeFollower(response.term);
                        return false;
                    }
                    std::lock_guard lock{mutex_};
                    auto* state = peerState(peerId);
                    if (state == nullptr) { return false; }
                    if (response.success) {
                        state->matchIndex = std::max(state->matchIndex, response.matchIndex);
                        state->nextIndex = state->matchIndex + 1;
                    }
                    else { updateNextIndexAfterConflictLocked(*state, response); }
                }
            }

            void sendHeartbeats() {
                maybeCompactLog();
                std::vector<uint64_t> peerIds;
                uint64_t targetCommit = 0;
                {
                    std::lock_guard lock{mutex_};
                    if (!running_ || role_.load() != RaftRole::LEADER) { return; }
                    if (peerReplication_.empty()) { resetLeaderReplicationState(); }
                    ensurePeerReplicationTargetsLocked();
                    targetCommit = commitIndex_;
                    for (const auto& state : peerReplication_) { peerIds.push_back(state.node.nodeId); }
                }
                std::vector<std::thread> workers;
                workers.reserve(peerIds.size());
                for (const uint64_t peerId : peerIds) {
                    workers.emplace_back([this, peerId, targetCommit] { (void)replicatePeerTo(peerId, targetCommit, true); });
                }
                for (auto& worker : workers) { if (worker.joinable()) { worker.join(); } }
            }

            void acceptLoop() {
                while (true) {
                    {
                        std::lock_guard lock{mutex_};
                        if (!running_) { return; }
                    }
                    SocketHandle client = ::accept(listenSock_, nullptr, nullptr);
                    if (!socketOk(client)) { return; }
                    configureNoSigPipe(client);
                    setTimeouts(client, 1000);
                    const size_t active = activeClientHandlers_.fetch_add(1, std::memory_order_acq_rel);
                    if (active >= MAX_RAFT_CLIENT_HANDLERS) {
                        activeClientHandlers_.fetch_sub(1, std::memory_order_acq_rel);
                        closeSocket(client);
                        continue;
                    }
                    std::thread(
                        [this, client] {
                            try { handleClient(client); }
                            catch (...) { closeSocket(client); }
                            if (activeClientHandlers_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                                std::lock_guard lock{clientHandlersMutex_};
                                clientHandlersCv_.notify_all();
                            }
                        }
                    ).detach();
                }
            }

            void handleClient(SocketHandle client) {
                std::unique_ptr<crypto::SecureSession> secure;
                crypto::PublicKey remotePublicKey{};
                if (runtimeOptions_.transportMode == TransportMode::SECURE) {
                    secure = acceptSecureSession(client, remotePublicKey);
                    if (!secure) {
                        closeSocket(client);
                        return;
                    }
                }
                DecodedFrame frame;
                if (!recvFrame(client, secure.get(), frame)) {
                    closeSocket(client);
                    return;
                }
                if (frame.type == ReplMsgType::RAFT_REQUEST_VOTE) {
                    RequestVote request;
                    RequestVoteResponse response;
                    if (decodeRequestVote(frame.payload, request) && verifySecurePeer(request.candidateId, remotePublicKey)) {
                        response = handleRequestVote(request);
                    }
                    (void)sendFrame(client, secure.get(), encodeRequestVoteResponse(response));
                }
                else if (frame.type == ReplMsgType::RAFT_APPEND_ENTRIES) {
                    AppendEntries request;
                    AppendEntriesResponse response;
                    if (decodeAppendEntries(frame.payload, request) && verifySecurePeer(request.leaderId, remotePublicKey)) {
                        response = handleAppendEntries(request);
                    }
                    (void)sendFrame(client, secure.get(), encodeAppendEntriesResponse(response));
                }
                else if (frame.type == ReplMsgType::RAFT_INSTALL_SNAPSHOT) {
                    InstallSnapshot request;
                    InstallSnapshotResponse response;
                    if (decodeInstallSnapshot(frame.payload, request) && verifySecurePeer(request.leaderId, remotePublicKey)) {
                        response = handleInstallSnapshot(request);
                    }
                    (void)sendFrame(client, secure.get(), encodeInstallSnapshotResponse(response));
                }
                else if (frame.type == ReplMsgType::RAFT_TIMEOUT_NOW) {
                    TimeoutNow request;
                    TimeoutNowResponse response;
                    if (decodeTimeoutNow(frame.payload, request) && verifySecurePeer(request.leaderId, remotePublicKey)) {
                        response = handleTimeoutNow(request);
                    }
                    (void)sendFrame(client, secure.get(), encodeTimeoutNowResponse(response));
                }
                closeSocket(client);
            }

            RequestVoteResponse handleRequestVote(const RequestVote& request) {
                std::optional<NodeRole> roleChange;
                RequestVoteResponse response;
                {
                    std::lock_guard lock{mutex_};
                    if (!isVotingMemberLocked(selfNodeId_) || !isVotingMemberLocked(request.candidateId)) {
                        return RequestVoteResponse{.term = currentTerm_, .voteGranted = false};
                    }
                    if (request.term < currentTerm_) { return RequestVoteResponse{.term = currentTerm_, .voteGranted = false}; }
                    if (request.term > currentTerm_) {
                        currentTerm_ = request.term;
                        votedFor_ = 0;
                        roleChange = setRole(RaftRole::FOLLOWER);
                    }
                    const bool upToDate = request.lastLogTerm > lastLogTerm() || (request.lastLogTerm == lastLogTerm() && request.lastLogIndex
                        >= lastLogIndex());
                    const bool canVote = votedFor_ == 0 || votedFor_ == request.candidateId;
                    const bool granted = canVote && upToDate;
                    if (granted) {
                        votedFor_ = request.candidateId;
                        electionDeadline_ = nextElectionDeadline();
                    }
                    persistState();
                    response = RequestVoteResponse{.term = currentTerm_, .voteGranted = granted};
                }
                notifyRoleChange(roleChange);
                return response;
            }

            TimeoutNowResponse handleTimeoutNow(const TimeoutNow& request) {
                uint64_t responseTerm = 0;
                std::optional<NodeRole> roleChange;
                {
                    std::lock_guard lock{mutex_};
                    if (request.targetId != selfNodeId_ || !isVotingMemberLocked(selfNodeId_) || !isVotingMemberLocked(request.leaderId)) {
                        return TimeoutNowResponse{.term = currentTerm_, .accepted = false};
                    }
                    if (request.term < currentTerm_) { return TimeoutNowResponse{.term = currentTerm_, .accepted = false}; }
                    if (request.term > currentTerm_) {
                        currentTerm_ = request.term;
                        votedFor_ = 0;
                        persistState();
                    }
                    roleChange = setRole(RaftRole::FOLLOWER);
                    electionDeadline_ = Clock::now();
                    responseTerm = currentTerm_;
                }
                notifyRoleChange(roleChange);
                cv_.notify_all();
                return TimeoutNowResponse{.term = responseTerm, .accepted = true};
            }

            bool applySnapshotChunkRequestLocked(const InstallSnapshot& request) {
                if (request.chunks.empty() && !request.done) { return false; }
                if (request.committedVoters.empty() || request.jointOldVoters.has_value() != request.jointNewVoters.has_value()) {
                    return false;
                }
                if (request.jointOldVoters && (request.jointOldVoters->empty() || request.jointNewVoters->empty())) { return false; }
                const bool sameSnapshot = pendingSnapshotInstall_.active && pendingSnapshotInstall_.snapshotSeq == request.snapshotSeq &&
                                          pendingSnapshotInstall_.lastIncludedIndex == request.lastIncludedIndex &&
                                          pendingSnapshotInstall_.lastIncludedTerm == request.lastIncludedTerm &&
                                          pendingSnapshotInstall_.entryCount == request.entryCount &&
                                          sameNodeSet(pendingSnapshotInstall_.committedVoters, request.committedVoters) &&
                                          sameOptionalNodeSet(pendingSnapshotInstall_.jointOldVoters, request.jointOldVoters) &&
                                          sameOptionalNodeSet(pendingSnapshotInstall_.jointNewVoters, request.jointNewVoters);
                if (pendingSnapshotInstall_.active && !sameSnapshot) { return false; }
                const bool restartsSnapshot = sameSnapshot && !request.chunks.empty() &&
                                              request.chunks.front().entryIndex == 0 && request.chunks.front().valueOffset == 0 &&
                                              (pendingSnapshotInstall_.nextEntryIndex != 0 || pendingSnapshotInstall_.hasCurrentEntry);
                if (!sameSnapshot || restartsSnapshot) {
                    pendingSnapshotInstall_ = PendingSnapshotInstall{};
                    pendingSnapshotInstall_.active = true;
                    pendingSnapshotInstall_.snapshotSeq = request.snapshotSeq;
                    pendingSnapshotInstall_.lastIncludedIndex = request.lastIncludedIndex;
                    pendingSnapshotInstall_.lastIncludedTerm = request.lastIncludedTerm;
                    pendingSnapshotInstall_.entryCount = request.entryCount;
                    pendingSnapshotInstall_.committedVoters = request.committedVoters;
                    pendingSnapshotInstall_.jointOldVoters = request.jointOldVoters;
                    pendingSnapshotInstall_.jointNewVoters = request.jointNewVoters;
                    if (callbacks_.beginSnapshot) { callbacks_.beginSnapshot(request.snapshotSeq, request.entryCount); }
                }

                for (const auto& chunk : request.chunks) {
                    if (chunk.entryIndex >= pendingSnapshotInstall_.entryCount || chunk.valueOffset > chunk.valueSize ||
                        chunk.value.size() > chunk.valueSize - chunk.valueOffset) {
                        return false;
                    }
                    if (!pendingSnapshotInstall_.hasCurrentEntry) {
                        if (chunk.entryIndex != pendingSnapshotInstall_.nextEntryIndex || chunk.valueOffset != 0) { return false; }
                        pendingSnapshotInstall_.hasCurrentEntry = true;
                        pendingSnapshotInstall_.currentEntryIndex = chunk.entryIndex;
                        pendingSnapshotInstall_.currentValueSize = chunk.valueSize;
                        pendingSnapshotInstall_.currentValueOffset = 0;
                        pendingSnapshotInstall_.currentValueCrc32c = chunk.valueCrc32c;
                        pendingSnapshotInstall_.currentKey = chunk.key;
                        pendingSnapshotInstall_.currentValueCrc = Crc32cStream{};
                        if (callbacks_.beginSnapshotEntry) {
                            callbacks_.beginSnapshotEntry(
                                pendingSnapshotInstall_.currentKey,
                                pendingSnapshotInstall_.currentValueSize,
                                pendingSnapshotInstall_.currentValueCrc32c
                            );
                        }
                    }
                    else if (chunk.entryIndex != pendingSnapshotInstall_.currentEntryIndex || !chunk.key.empty()) {
                        return false;
                    }

                    if (pendingSnapshotInstall_.currentValueSize != chunk.valueSize ||
                        pendingSnapshotInstall_.currentValueCrc32c != chunk.valueCrc32c ||
                        pendingSnapshotInstall_.currentValueOffset != chunk.valueOffset) {
                        return false;
                    }
                    pendingSnapshotInstall_.currentValueCrc.update(chunk.value);
                    if (callbacks_.appendSnapshotEntryChunk) {
                        callbacks_.appendSnapshotEntryChunk(chunk.valueOffset, chunk.value);
                    }
                    pendingSnapshotInstall_.currentValueOffset += chunk.value.size();
                    if (pendingSnapshotInstall_.currentValueOffset == pendingSnapshotInstall_.currentValueSize) {
                        if (pendingSnapshotInstall_.currentValueCrc.finish() != pendingSnapshotInstall_.currentValueCrc32c) { return false; }
                        if (callbacks_.finishSnapshotEntry) {
                            callbacks_.finishSnapshotEntry();
                        }
                        ++pendingSnapshotInstall_.nextEntryIndex;
                        pendingSnapshotInstall_.currentKey.clear();
                        pendingSnapshotInstall_.currentValueCrc = Crc32cStream{};
                        pendingSnapshotInstall_.hasCurrentEntry = false;
                    }
                }

                if (!request.done) { return true; }
                if (pendingSnapshotInstall_.hasCurrentEntry || pendingSnapshotInstall_.nextEntryIndex != pendingSnapshotInstall_.entryCount) {
                    return false;
                }
                if (callbacks_.finishSnapshot) { callbacks_.finishSnapshot(request.snapshotSeq); }
                if (callbacks_.forceDurable) { callbacks_.forceDurable(); }
                return true;
            }

            InstallSnapshotResponse handleInstallSnapshot(const InstallSnapshot& request) {
                std::optional<NodeRole> roleChange;
                std::optional<InstallSnapshotResponse> earlyResponse;
                {
                    std::lock_guard lock{mutex_};
                    if (request.term < currentTerm_) {
                        return InstallSnapshotResponse{.term = currentTerm_, .success = false, .lastIncludedIndex = lastIncludedIndex_};
                    }
                    if (request.term > currentTerm_) {
                        currentTerm_ = request.term;
                        votedFor_ = 0;
                        persistState();
                    }
                    roleChange = setRole(RaftRole::FOLLOWER);
                    electionDeadline_ = nextElectionDeadline();
                    if (request.lastIncludedIndex <= lastIncludedIndex_) {
                        earlyResponse = InstallSnapshotResponse{.term = currentTerm_, .success = true, .lastIncludedIndex = lastIncludedIndex_};
                    }
                }
                notifyRoleChange(roleChange);
                if (earlyResponse) { return *earlyResponse; }

                try {
                    std::lock_guard applyLock{applyMutex_};
                    if (!applySnapshotChunkRequestLocked(request)) {
                        std::lock_guard lock{mutex_};
                        return InstallSnapshotResponse{.term = currentTerm_, .success = false, .lastIncludedIndex = lastIncludedIndex_};
                    }
                    if (!request.done) {
                        std::lock_guard lock{mutex_};
                        return InstallSnapshotResponse{.term = currentTerm_, .success = true, .lastIncludedIndex = lastIncludedIndex_};
                    }
                    std::lock_guard lock{mutex_};
                    if (!applySnapshotMembershipLocked(request)) {
                        return InstallSnapshotResponse{.term = currentTerm_, .success = false, .lastIncludedIndex = lastIncludedIndex_};
                    }
                    compactLogThrough(request.lastIncludedIndex, request.lastIncludedTerm);
                    persistLog();
                    pendingSnapshotInstall_ = PendingSnapshotInstall{};
                    return InstallSnapshotResponse{.term = currentTerm_, .success = true, .lastIncludedIndex = lastIncludedIndex_};
                }
                catch (...) {
                    {
                        std::lock_guard applyLock{applyMutex_};
                        pendingSnapshotInstall_ = PendingSnapshotInstall{};
                    }
                    std::lock_guard lock{mutex_};
                    return InstallSnapshotResponse{.term = currentTerm_, .success = false, .lastIncludedIndex = lastIncludedIndex_};
                }
            }

            AppendEntriesResponse handleAppendEntries(const AppendEntries& request) {
                AppendEntriesResponse response;
                std::optional<NodeRole> roleChange;
                std::optional<AppendEntriesResponse> earlyResponse;
                {
                    std::lock_guard lock{mutex_};
                    if (request.term < currentTerm_) {
                        return AppendEntriesResponse{.term = currentTerm_, .success = false, .matchIndex = lastLogIndex()};
                    }
                    if (request.term > currentTerm_) {
                        currentTerm_ = request.term;
                        votedFor_ = 0;
                        persistState();
                    }
                    roleChange = setRole(RaftRole::FOLLOWER);
                    electionDeadline_ = nextElectionDeadline();

                    if (request.prevLogIndex < lastIncludedIndex_) {
                        earlyResponse = conflictResponseLocked(request.prevLogIndex);
                    }
                    else if (request.prevLogIndex > lastLogIndex()) {
                        earlyResponse = conflictResponseLocked(request.prevLogIndex);
                    }
                    else if (termAt(request.prevLogIndex) != request.prevLogTerm) {
                        earlyResponse = conflictResponseLocked(request.prevLogIndex);
                    }
                    if (earlyResponse) { response = *earlyResponse; }
                    else {

                        uint64_t expectedIndex = request.prevLogIndex + 1;
                        uint64_t acceptedMatchIndex = request.prevLogIndex;
                        for (const auto& entry : request.entries) {
                            if (entry.index != expectedIndex++) {
                                earlyResponse = AppendEntriesResponse{.term = currentTerm_, .success = false, .matchIndex = acceptedMatchIndex};
                                response = *earlyResponse;
                                break;
                            }
                            if (entry.index <= lastIncludedIndex_) { continue; }
                            const auto existing = entryAt(entry.index);
                            if (!existing || !sameEntry(*existing, entry)) {
                                std::erase_if(log_, [&](const RaftLogEntry& item) { return item.index >= entry.index; });
                                log_.push_back(entry);
                            }
                            acceptedMatchIndex = entry.index;
                        }

                        if (!earlyResponse) {
                            if (request.leaderCommit > commitIndex_) { commitIndex_ = std::min(request.leaderCommit, acceptedMatchIndex); }
                            persistLog();
                            response = AppendEntriesResponse{.term = currentTerm_, .success = true, .matchIndex = acceptedMatchIndex};
                        }
                    }
                }
                notifyRoleChange(roleChange);
                if (earlyResponse) { return response; }
                applyCommitted();
                return response;
            }

            struct BlobApplyBatch {
                bool complete = false;
                size_t nextEntryOffset = 0;
                uint64_t finalLogIndex = 0;
                uint64_t seq = 0;
                uint64_t blobId = 0;
                uint64_t totalSize = 0;
                uint32_t contentCrc32c = 0;
            };

            BlobApplyBatch applyBlobBatch(const std::vector<RaftLogEntry>& entries, size_t start) const {
                BlobApplyBatch batch;
                batch.nextEntryOffset = start;
                if (start >= entries.size()) { return batch; }
                const auto& first = entries[start];
                RaftBlobChunk chunk;
                if (first.kind != RaftEntryKind::BLOB || !decodeBlobChunkKey(first.key, chunk) || chunk.offset != 0 ||
                    first.value.size() > chunk.totalSize) {
                    throw std::runtime_error("RaftConsensusRuntime: corrupt Blob log entry");
                }
                const uint64_t seq = first.clientSeq == 0 ? first.index : first.clientSeq;
                Crc32cStream contentCrc;

                uint64_t expectedOffset = 0;
                size_t pos = start;
                while (pos < entries.size()) {
                    const auto& entry = entries[pos];
                    RaftBlobChunk current;
                    const uint64_t entrySeq = entry.clientSeq == 0 ? entry.index : entry.clientSeq;
                    if (entry.kind != RaftEntryKind::BLOB || !decodeBlobChunkKey(entry.key, current) || current.blobId != chunk.blobId ||
                        entrySeq != seq || current.totalSize != chunk.totalSize || current.contentCrc32c != chunk.contentCrc32c ||
                        current.offset != expectedOffset || entry.value.size() > current.totalSize - current.offset) {
                        throw std::runtime_error("RaftConsensusRuntime: out-of-order Blob log chunk");
                    }
                    contentCrc.update(entry.value);
                    expectedOffset += entry.value.size();
                    batch.finalLogIndex = entry.index;
                    ++pos;
                    if (expectedOffset == chunk.totalSize) {
                        if (contentCrc.finish() != chunk.contentCrc32c) {
                            throw std::runtime_error("RaftConsensusRuntime: Blob log payload CRC mismatch");
                        }
                        bool blobStarted = false;
                        try {
                            if (callbacks_.beginBlob) {
                                callbacks_.beginBlob(seq, chunk.blobId, chunk.totalSize, chunk.contentCrc32c);
                                blobStarted = true;
                            }
                            for (size_t callbackPos = start; callbackPos < pos; ++callbackPos) {
                                RaftBlobChunk callbackChunk;
                                if (!decodeBlobChunkKey(entries[callbackPos].key, callbackChunk)) {
                                    throw std::runtime_error("RaftConsensusRuntime: corrupt Blob log entry");
                                }
                                if (callbacks_.appendBlobChunk) {
                                    callbacks_.appendBlobChunk(seq, chunk.blobId, callbackChunk.offset, entries[callbackPos].value);
                                }
                            }
                            if (callbacks_.finishBlob) { callbacks_.finishBlob(seq, chunk.blobId); }
                        }
                        catch (...) {
                            if (blobStarted && callbacks_.abortBlob) { callbacks_.abortBlob(seq, chunk.blobId); }
                            throw;
                        }
                        batch.complete = true;
                        batch.nextEntryOffset = pos;
                        batch.seq = seq;
                        batch.blobId = chunk.blobId;
                        batch.totalSize = chunk.totalSize;
                        batch.contentCrc32c = chunk.contentCrc32c;
                        return batch;
                    }
                }
                if (expectedOffset > chunk.totalSize) {
                    throw std::runtime_error("RaftConsensusRuntime: Blob log payload exceeds declared size");
                }
                batch.nextEntryOffset = pos;
                return batch;
            }

            void applyCommitted() {
                std::lock_guard applyLock{applyMutex_};
                std::vector<RaftLogEntry> toApply;
                {
                    std::lock_guard lock{mutex_};
                    for (const auto& entry : log_) {
                        if (entry.index > lastApplied_ && entry.index <= commitIndex_) { toApply.push_back(entry); }
                    }
                }

                uint64_t appliedThrough = 0;
                bool appliedDurableMutation = false;
                for (size_t i = 0; i < toApply.size();) {
                    const auto& entry = toApply[i];
                    if (entry.kind == RaftEntryKind::CONFIG_JOINT || entry.kind == RaftEntryKind::CONFIG_FINAL) {
                        std::lock_guard lock{mutex_};
                        applyConfigEntryLocked(entry);
                        appliedThrough = entry.index;
                        ++i;
                    }
                    else if (entry.kind == RaftEntryKind::BLOB) {
                        auto batch = applyBlobBatch(toApply, i);
                        if (!batch.complete) { break; }
                        appliedDurableMutation = true;
                        appliedThrough = batch.finalLogIndex;
                        i = batch.nextEntryOffset;
                    }
                    else {
                        if (entry.kind != RaftEntryKind::NOOP && callbacks_.apply) {
                            callbacks_.apply(
                                entry.clientSeq == 0 ? entry.index : entry.clientSeq,
                                entry.op,
                                entry.key,
                                entry.value,
                                entry.flags,
                                entry.sourceNodeId
                            );
                            appliedDurableMutation = true;
                        }
                        appliedThrough = entry.index;
                        ++i;
                    }
                }

                if (appliedThrough == 0) { return; }
                if (appliedDurableMutation && callbacks_.forceDurable) { callbacks_.forceDurable(); }
                std::lock_guard lock{mutex_};
                if (appliedThrough > lastApplied_) {
                    lastApplied_ = appliedThrough;
                    persistLog();
                }
            }

        public:
            Impl(
                std::filesystem::path dbDir,
                ClusterConfig config,
                uint64_t selfNodeId,
                ClusterEngineCallbacks callbacks,
                ClusterRuntimeOptions runtimeOptions
            )
                : dbDir_{std::move(dbDir)},
                  statePath_{dbDir_.empty() ? std::filesystem::path{"cluster-raft.state"} : dbDir_ / "cluster-raft.state"},
                  logPath_{dbDir_.empty() ? std::filesystem::path{"cluster-raft.log"} : dbDir_ / "cluster-raft.log"},
                  config_{std::move(config)},
                  router_{config_},
                  selfNodeId_{selfNodeId},
                  callbacks_{std::move(callbacks)},
                  runtimeOptions_{std::move(runtimeOptions)} {
                config_.validate();
                if (runtimeOptions_.raftBlobChunkSizeBytes == 0 ||
                    runtimeOptions_.raftBlobChunkSizeBytes > maxRaftBlobChunkSizeBytes()) {
                    throw std::invalid_argument("RaftConsensusRuntime: invalid Raft Blob chunk size");
                }
                self_ = config_.findById(selfNodeId_);
                if (self_ == nullptr || !self_->dataBearing()) {
                    throw std::invalid_argument("RaftConsensusRuntime: local node must be data-bearing");
                }
                if (runtimeOptions_.transportMode == TransportMode::SECURE) { localIdentity_ = loadSecureIdentity(runtimeOptions_); }
                committedVoters_ = config_.dataNodes();
                refreshPeersFromMembershipLocked();
                if (config_.dataNodes().empty()) { throw std::invalid_argument("RaftConsensusRuntime: no data-bearing nodes"); }
                recoverState();
                recoverLog();
                recoverAppliedProgressFromStateMachine();
                this->replayCommittedMembership();
            }

            ~Impl() { this->close(); }

            void start() {
                {
                    std::lock_guard lock{mutex_};
                    if (running_) { return; }
                    running_ = true;
                    electionDeadline_ = this->nextElectionDeadline();
                }
                listenSock_ = listenOn(runtimeOptions_.replBindHost, self_->replPort);
                acceptThread_ = std::thread([this] { this->acceptLoop(); });
                timerThread_ = std::thread([this] { this->timerLoop(); });
                this->applyCommitted();
                if (peers_.empty()) { this->becomeLeader(currentTerm_ == 0 ? 1 : currentTerm_); }
            }

            void close() {
                {
                    std::lock_guard lock{mutex_};
                    running_ = false;
                }
                shutdownSocket(listenSock_);
                closeSocket(listenSock_);
                listenSock_ = BAD_SOCKET;
                cv_.notify_all();
                if (acceptThread_.joinable()) { acceptThread_.join(); }
                if (timerThread_.joinable()) { timerThread_.join(); }
                std::unique_lock clientLock{clientHandlersMutex_};
                clientHandlersCv_.wait(clientLock, [this] { return activeClientHandlers_.load() == 0; });
            }

            NodeRole role() const noexcept {
                const auto role = role_.load();
                if (role == RaftRole::LEADER) { return NodeRole::PRIMARY; }
                return NodeRole::REPLICA;
            }

            std::vector<NodeInfo> activeNodes() const {
                std::lock_guard lock{mutex_};
                return this->replicationTargetsLocked();
            }

            const ClusterRouter& router() const noexcept { return router_; }

            void shipEntry(
                uint64_t seq,
                ReplOpType op,
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint8_t recordFlags,
                uint64_t sourceNodeId
            ) {
                this->maybeCompactLog();
                (void)this->commitOutstandingEntry(std::chrono::milliseconds{20000});
                RaftLogEntry entry;
                uint64_t term = 0;
                {
                    std::lock_guard lock{mutex_};
                    if (role_.load() != RaftRole::LEADER) {
                        throw std::runtime_error("RaftConsensusRuntime: local node is not Raft leader");
                    }
                    if (commitIndex_ != lastLogIndex()) {
                        throw std::runtime_error("RaftConsensusRuntime: cannot accept a new proposal while a prior entry is uncommitted");
                    }
                    term = currentTerm_;
                    entry.term = term;
                    entry.index = this->lastLogIndex() + 1;
                    entry.clientSeq = seq;
                    entry.kind = RaftEntryKind::MUTATION;
                    entry.op = op;
                    entry.flags = recordFlags;
                    entry.sourceNodeId = sourceNodeId;
                    entry.key.assign(key.begin(), key.end());
                    entry.value.assign(value.begin(), value.end());
                    log_.push_back(entry);
                    this->persistLog();
                }

                if (!this->replicateEntryToMajority(entry)) {
                    throw std::runtime_error("RaftConsensusRuntime: failed to replicate entry to Raft majority");
                }

                {
                    std::lock_guard lock{mutex_};
                    if (role_.load() != RaftRole::LEADER || currentTerm_ != term) {
                        throw std::runtime_error("RaftConsensusRuntime: leadership changed before commit");
                    }
                    if (entry.index > commitIndex_) {
                        commitIndex_ = entry.index;
                        this->persistLog();
                    }
                }
                this->applyCommitted();
                this->sendHeartbeats();
                this->maybeCompactLog();
            }

            void shipBlob(uint64_t seq, uint64_t blobId, std::span<const uint8_t> content) {
                if (runtimeOptions_.raftBlobPolicy == RaftBlobPolicy::REJECT) {
                    throw std::runtime_error("RaftConsensusRuntime: RAFT_QUORUM does not support Blob payload replication");
                }
                if (runtimeOptions_.raftBlobPolicy == RaftBlobPolicy::PRIMARY_SIDE_ONLY) {
                    if (role_.load() != RaftRole::LEADER) {
                        throw std::runtime_error("RaftConsensusRuntime: local node is not Raft leader");
                    }
                    return;
                }
                if (runtimeOptions_.raftBlobPolicy == RaftBlobPolicy::RAFT_LOG) {
                    std::vector<RaftLogEntry> entries;
                    uint64_t term = 0;
                    {
                        std::lock_guard lock{mutex_};
                        if (role_.load() != RaftRole::LEADER) {
                            throw std::runtime_error("RaftConsensusRuntime: local node is not Raft leader");
                        }
                        term = currentTerm_;
                        const uint32_t contentCrc32c = crcBytes(content);
                        const size_t chunkSize = runtimeOptions_.raftBlobChunkSizeBytes;
                        size_t offset = 0;
                        do {
                            const size_t remaining = content.size() - offset;
                            const size_t currentChunkSize = std::min(chunkSize, remaining);
                            RaftLogEntry entry;
                            entry.term = term;
                            entry.index = this->lastLogIndex() + entries.size() + 1;
                            entry.clientSeq = seq;
                            entry.kind = RaftEntryKind::BLOB;
                            entry.op = ReplOpType::PUT;
                            entry.flags = 0;
                            entry.sourceNodeId = selfNodeId_;
                            entry.key = encodeBlobChunkKey(
                                RaftBlobChunk{
                                    .blobId = blobId,
                                    .offset = static_cast<uint64_t>(offset),
                                    .totalSize = static_cast<uint64_t>(content.size()),
                                    .contentCrc32c = contentCrc32c,
                                }
                            );
                            entry.value.assign(content.begin() + static_cast<std::ptrdiff_t>(offset), content.begin() + static_cast<std::ptrdiff_t>(
                                offset + currentChunkSize
                            ));
                            entries.push_back(std::move(entry));
                            offset += currentChunkSize;
                        }
                        while (offset < content.size());
                        log_.insert(log_.end(), entries.begin(), entries.end());
                        this->persistLog();
                    }

                    const RaftLogEntry& lastEntry = entries.back();
                    if (!this->replicateEntryToMajority(lastEntry)) {
                        throw std::runtime_error("RaftConsensusRuntime: failed to replicate Blob payload to Raft majority");
                    }

                    {
                        std::lock_guard lock{mutex_};
                        if (role_.load() != RaftRole::LEADER || currentTerm_ != term) {
                            throw std::runtime_error("RaftConsensusRuntime: leadership changed before Blob payload commit");
                        }
                        if (lastEntry.index > commitIndex_) {
                            commitIndex_ = lastEntry.index;
                            this->persistLog();
                        }
                    }
                    this->applyCommitted();
                    this->sendHeartbeats();
                    this->maybeCompactLog();
                    return;
                }
                throw std::runtime_error("RaftConsensusRuntime: unsupported Raft Blob policy");
            }

            void addVotingNode(const NodeInfo& node) {
                if (!node.dataBearing()) { throw std::invalid_argument("RaftConsensusRuntime: Raft voting node must be data-bearing"); }
                if (!onlineMembershipChangeEnabled()) {
                    throw std::runtime_error("RaftConsensusRuntime: online Raft membership change is disabled");
                }
                (void)this->commitOutstandingEntry(std::chrono::milliseconds{20000});
                std::vector<NodeInfo> oldVoters;
                std::vector<NodeInfo> newVoters;
                bool finishExistingJoint = false;
                std::optional<RaftLogEntry> pendingConfigEntry;
                {
                    std::lock_guard lock{mutex_};
                    if (role_.load() != RaftRole::LEADER) {
                        throw std::runtime_error("RaftConsensusRuntime: local node is not Raft leader");
                    }
                    if (commitIndex_ != lastLogIndex() && !log_.empty() && (log_.back().kind == RaftEntryKind::CONFIG_JOINT || log_.back().
                        kind == RaftEntryKind::CONFIG_FINAL)) {
                        std::vector<NodeInfo> pendingNewVoters;
                        if (decodeNodeSet(log_.back().value, pendingNewVoters) && containsNode(pendingNewVoters, node.nodeId)) {
                            pendingConfigEntry = log_.back();
                        }
                        else {
                            throw std::runtime_error("RaftConsensusRuntime: cannot change membership while a prior entry is uncommitted");
                        }
                    }
                    else if (jointOldVoters_ || jointNewVoters_) {
                        if (jointNewVoters_ && containsNode(*jointNewVoters_, node.nodeId) && !
                            containsNode(committedVoters_, node.nodeId)) {
                            oldVoters = jointOldVoters_.value_or(committedVoters_);
                            newVoters = *jointNewVoters_;
                            finishExistingJoint = true;
                        }
                        else { throw std::runtime_error("RaftConsensusRuntime: membership change already in progress"); }
                    }
                    else if (containsNode(committedVoters_, node.nodeId)) {
                        throw std::invalid_argument("RaftConsensusRuntime: node is already a voting member");
                    }
                    else {
                        oldVoters = committedVoters_;
                        newVoters = committedVoters_;
                        newVoters.push_back(node);
                        newVoters = sortedUniqueVoters(std::move(newVoters));
                    }
                }
                if (pendingConfigEntry) {
                    this->appendReplicateAndCommitConfig(*pendingConfigEntry, false);
                    if (pendingConfigEntry->kind == RaftEntryKind::CONFIG_JOINT) {
                        std::vector<NodeInfo> oldFinalVoters;
                        std::vector<NodeInfo> finalVoters;
                        if (!decodeNodeSet(pendingConfigEntry->key, oldFinalVoters) ||
                            !decodeNodeSet(pendingConfigEntry->value, finalVoters)) {
                            throw std::runtime_error("RaftConsensusRuntime: corrupt joint membership entry");
                        }
                        RaftLogEntry final = this->makeConfigEntry(RaftEntryKind::CONFIG_FINAL, oldFinalVoters, finalVoters);
                        this->appendReplicateAndCommitConfig(final);
                    }
                    return;
                }
                if (finishExistingJoint) {
                    RaftLogEntry final = this->makeConfigEntry(RaftEntryKind::CONFIG_FINAL, oldVoters, newVoters);
                    this->appendReplicateAndCommitConfig(final);
                    return;
                }
                this->proposeMembershipChange(oldVoters, newVoters);
            }

            void removeVotingNode(uint64_t nodeId) {
                if (!onlineMembershipChangeEnabled()) {
                    throw std::runtime_error("RaftConsensusRuntime: online Raft membership change is disabled");
                }
                (void)this->commitOutstandingEntry(std::chrono::milliseconds{20000});
                if (nodeId == selfNodeId_) {
                    throw std::runtime_error("RaftConsensusRuntime: removing the local leader is not supported by this API");
                }
                std::vector<NodeInfo> oldVoters;
                std::vector<NodeInfo> newVoters;
                bool finishExistingJoint = false;
                std::optional<RaftLogEntry> pendingConfigEntry;
                {
                    std::lock_guard lock{mutex_};
                    if (role_.load() != RaftRole::LEADER) {
                        throw std::runtime_error("RaftConsensusRuntime: local node is not Raft leader");
                    }
                    if (commitIndex_ != this->lastLogIndex() && !log_.empty() && (log_.back().kind == RaftEntryKind::CONFIG_JOINT || log_.back().
                        kind == RaftEntryKind::CONFIG_FINAL)) {
                        std::vector<NodeInfo> pendingNewVoters;
                        if (decodeNodeSet(log_.back().value, pendingNewVoters) && !containsNode(pendingNewVoters, nodeId)) {
                            pendingConfigEntry = log_.back();
                        }
                        else {
                            throw std::runtime_error("RaftConsensusRuntime: cannot change membership while a prior entry is uncommitted");
                        }
                    }
                    else if (jointOldVoters_ || jointNewVoters_) {
                        if (jointOldVoters_ && jointNewVoters_ && containsNode(*jointOldVoters_, nodeId) && !containsNode(
                            *jointNewVoters_,
                            nodeId
                        )) {
                            oldVoters = *jointOldVoters_;
                            newVoters = *jointNewVoters_;
                            finishExistingJoint = true;
                        }
                        else { throw std::runtime_error("RaftConsensusRuntime: membership change already in progress"); }
                    }
                    else if (!containsNode(committedVoters_, nodeId)) {
                        throw std::invalid_argument("RaftConsensusRuntime: node is not a voting member");
                    }
                    else {
                        oldVoters = committedVoters_;
                        for (const auto& node : committedVoters_) { if (node.nodeId != nodeId) { newVoters.push_back(node); } }
                        if (newVoters.empty()) {
                            throw std::invalid_argument("RaftConsensusRuntime: cannot remove the last voting member");
                        }
                    }
                }
                if (pendingConfigEntry) {
                    this->appendReplicateAndCommitConfig(*pendingConfigEntry, false);
                    if (pendingConfigEntry->kind == RaftEntryKind::CONFIG_JOINT) {
                        std::vector<NodeInfo> oldFinalVoters;
                        std::vector<NodeInfo> finalVoters;
                        if (!decodeNodeSet(pendingConfigEntry->key, oldFinalVoters) ||
                            !decodeNodeSet(pendingConfigEntry->value, finalVoters)) {
                            throw std::runtime_error("RaftConsensusRuntime: corrupt joint membership entry");
                        }
                        RaftLogEntry final = this->makeConfigEntry(RaftEntryKind::CONFIG_FINAL, oldFinalVoters, finalVoters);
                        this->appendReplicateAndCommitConfig(final);
                    }
                    return;
                }
                if (finishExistingJoint) {
                    RaftLogEntry final = this->makeConfigEntry(RaftEntryKind::CONFIG_FINAL, oldVoters, newVoters);
                    this->appendReplicateAndCommitConfig(final);
                    return;
                }
                this->proposeMembershipChange(oldVoters, newVoters);
            }

            void transferLeadership(uint64_t targetNodeId) {
                if (targetNodeId == selfNodeId_) { return; }
                NodeInfo target;
                uint64_t targetIndex = 0;
                uint64_t term = 0;
                {
                    std::lock_guard lock{mutex_};
                    if (role_.load() != RaftRole::LEADER) {
                        throw std::runtime_error("RaftConsensusRuntime: local node is not Raft leader");
                    }
                    if (!isVotingMemberLocked(targetNodeId)) {
                        throw std::invalid_argument("RaftConsensusRuntime: transfer target is not a voting member");
                    }
                    auto* state = this->peerState(targetNodeId);
                    if (state == nullptr) {
                        throw std::invalid_argument("RaftConsensusRuntime: transfer target is not a replication peer");
                    }
                    target = state->node;
                    targetIndex = this->lastLogIndex();
                    term = currentTerm_;
                }

                if (!this->replicatePeerTo(targetNodeId, targetIndex)) {
                    throw std::runtime_error("RaftConsensusRuntime: failed to catch up transfer target");
                }
                {
                    std::lock_guard lock{mutex_};
                    const auto* state = this->peerState(targetNodeId);
                    if (role_.load() != RaftRole::LEADER || currentTerm_ != term) {
                        throw std::runtime_error("RaftConsensusRuntime: leadership changed before transfer");
                    }
                    if (state == nullptr || state->matchIndex < targetIndex) {
                        throw std::runtime_error("RaftConsensusRuntime: transfer target is not caught up");
                    }
                }

                TimeoutNowResponse response;
                if (!this->timeoutNow(target, TimeoutNow{.term = term, .leaderId = selfNodeId_, .targetId = targetNodeId}, response)) {
                    throw std::runtime_error("RaftConsensusRuntime: failed to send leader transfer request");
                }
                if (response.term > term) {
                    this->becomeFollower(response.term);
                    throw std::runtime_error("RaftConsensusRuntime: leadership changed during transfer");
                }
                if (!response.accepted) { throw std::runtime_error("RaftConsensusRuntime: leader transfer target rejected request"); }
                {
                    std::lock_guard lock{mutex_};
                    if (role_.load() == RaftRole::LEADER && currentTerm_ == term) { electionDeadline_ = this->nextElectionDeadline(); }
                }
                this->setRoleAndNotify(RaftRole::FOLLOWER);
            }

    };

    std::unique_ptr<RaftConsensusRuntime> RaftConsensusRuntime::create(
        std::filesystem::path dbDir,
        ClusterConfig config,
        uint64_t selfNodeId,
        ClusterEngineCallbacks callbacks,
        ClusterRuntimeOptions runtimeOptions
    ) {
        return std::unique_ptr<RaftConsensusRuntime>(
            new RaftConsensusRuntime(
                std::make_unique<Impl>(std::move(dbDir), std::move(config), selfNodeId, std::move(callbacks), std::move(runtimeOptions))
            )
        );
    }

    RaftConsensusRuntime::RaftConsensusRuntime(std::unique_ptr<Impl> impl) : impl_{std::move(impl)} {}
    RaftConsensusRuntime::~RaftConsensusRuntime() = default;
    void RaftConsensusRuntime::start() { impl_->start(); }
    void RaftConsensusRuntime::close() { impl_->close(); }
    NodeRole RaftConsensusRuntime::role() const noexcept { return impl_->role(); }
    std::vector<NodeInfo> RaftConsensusRuntime::activeNodes() const { return impl_->activeNodes(); }
    const ClusterRouter& RaftConsensusRuntime::router() const noexcept { return impl_->router(); }

    void RaftConsensusRuntime::shipEntry(
        uint64_t seq,
        ReplOpType op,
        std::span<const uint8_t> key,
        std::span<const uint8_t> value,
        uint8_t recordFlags,
        uint64_t sourceNodeId
    ) { impl_->shipEntry(seq, op, key, value, recordFlags, sourceNodeId); }

    void RaftConsensusRuntime::shipBlob(uint64_t seq, uint64_t blobId, std::span<const uint8_t> content) {
        impl_->shipBlob(seq, blobId, content);
    }

    void RaftConsensusRuntime::addVotingNode(const NodeInfo& node) { impl_->addVotingNode(node); }
    void RaftConsensusRuntime::removeVotingNode(uint64_t nodeId) { impl_->removeVotingNode(nodeId); }
    void RaftConsensusRuntime::transferLeadership(uint64_t targetNodeId) { impl_->transferLeadership(targetNodeId); }
}

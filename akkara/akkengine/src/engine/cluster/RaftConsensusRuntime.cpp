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
#include "akk/core/record/MemHdr16.hpp"
#include "akk/cpu/CRC32C.hpp"
#include "akk/crypto/Random.hpp"
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
#include <deque>
#include <future>
#include <functional>
#include <limits>
#include <mutex>
#include <map>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <unordered_map>
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
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace akkaradb::engine::cluster {
    namespace {
        using Clock = std::chrono::steady_clock;
        constexpr size_t MAX_RAFT_CLIENT_HANDLERS = 128;

        uint64_t observationNowUs() noexcept {
            return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count());
        }

        struct AtomicLatencyHistogram {
            std::array<std::atomic<uint64_t>, 8> bucketCounts{};
            std::atomic<uint64_t> sampleCount{0};
            std::atomic<uint64_t> totalUs{0};
            std::atomic<uint64_t> maxUs{0};

            void observe(uint64_t latencyUs) noexcept {
                ClusterLatencyHistogram layout;
                ++sampleCount;
                totalUs.fetch_add(latencyUs, std::memory_order_relaxed);
                auto previousMax = maxUs.load(std::memory_order_relaxed);
                while (previousMax < latencyUs &&
                       !maxUs.compare_exchange_weak(previousMax, latencyUs, std::memory_order_relaxed)) {}
                for (size_t i = 0; i < layout.bucketUpperBoundsUs.size(); ++i) {
                    if (latencyUs <= layout.bucketUpperBoundsUs[i]) {
                        bucketCounts[i].fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }

            [[nodiscard]] ClusterLatencyHistogram snapshot() const noexcept {
                ClusterLatencyHistogram out;
                out.sampleCount = sampleCount.load(std::memory_order_relaxed);
                out.totalUs = totalUs.load(std::memory_order_relaxed);
                out.maxUs = maxUs.load(std::memory_order_relaxed);
                for (size_t i = 0; i < out.bucketCounts.size(); ++i) {
                    out.bucketCounts[i] = bucketCounts[i].load(std::memory_order_relaxed);
                }
                return out;
            }
        };

        void crashAtLogTestPoint(const char* point) noexcept {
            const char* requested = std::getenv("AKKARADB_TEST_CRASH_POINT");
            if (requested != nullptr && std::strcmp(requested, point) == 0) { std::quick_exit(86); }
        }

        uint64_t requestJournalCompactRecords() noexcept {
            const char* requested = std::getenv("AKKARADB_TEST_REQUEST_JOURNAL_COMPACT_RECORDS");
            if (requested == nullptr || *requested == '\0') { return 0; }
            char* end = nullptr;
            const auto value = std::strtoull(requested, &end, 10);
            return end != requested && *end == '\0' && value > 0 ? value : 0;
        }

        #ifdef _WIN32
        using SocketHandle = SOCKET;
        constexpr auto BAD_SOCKET = INVALID_SOCKET;

        void netInit() {
            static std::once_flag once;
            std::call_once(
                once,
                [] {
                    WSADATA wsa{};
                    if (::WSAStartup(MAKEWORD(2, 2), &wsa) != 0) { throw std::runtime_error("RaftConsensusRuntime: WSAStartup failed"); }
                }
            );
        }

        void closeSocket(SocketHandle s) noexcept { if (s != BAD_SOCKET) { ::closesocket(s); } }

        void shutdownSocket(SocketHandle s) noexcept { if (s != BAD_SOCKET) { ::shutdown(s, SD_BOTH); } }

        bool socketOk(SocketHandle s) noexcept { return s != INVALID_SOCKET; }

        bool acceptInterrupted() noexcept {
            const int err = ::WSAGetLastError();
            return err == WSAEINTR || err == WSAECONNABORTED || err == WSAECONNRESET;
        }
        #else
        using SocketHandle = int; constexpr SocketHandle BAD_SOCKET = -1; void netInit() {} void closeSocket(SocketHandle s) noexcept {
            if (s >= 0) { ::close(s); }
        } void shutdownSocket(SocketHandle s) noexcept { if (s >= 0) { ::shutdown(s, SHUT_RDWR); } } bool socketOk(SocketHandle s) noexcept { return s >= 0; }

        bool acceptInterrupted() noexcept { return errno == EINTR || errno == ECONNABORTED; }
        #endif

        void writeU8(std::vector<uint8_t>& out, uint8_t value) { out.push_back(value); }

        void writeU32(std::vector<uint8_t>& out, uint32_t value) { for (size_t i = 0; i < 4; ++i) { out.push_back(static_cast<uint8_t>(value >> (8 * i))); } }

        void writeU64(std::vector<uint8_t>& out, uint64_t value) { for (size_t i = 0; i < 8; ++i) { out.push_back(static_cast<uint8_t>(value >> (8 * i))); } }

        void writeU32At(std::vector<uint8_t>& out, size_t off, uint32_t value) {
            for (size_t i = 0; i < 4; ++i) { out[off + i] = static_cast<uint8_t>(value >> (8 * i)); }
        }

        uint32_t readU32(std::span<const uint8_t> in, size_t off) {
            return static_cast<uint32_t>(in[off]) | (static_cast<uint32_t>(in[off + 1]) << 8) | (static_cast<uint32_t>(in[off + 2]) << 16) | (static_cast<
                uint32_t>(in[off + 3]) << 24);
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

        uint32_t crcBytes(std::span<const uint8_t> bytes) { return cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size()); }

        class Crc32cStream {
            public:
                void update(std::span<const uint8_t> bytes) noexcept { for (const auto byte : bytes) { crc_ = (crc_ >> 8u) ^ table()[(crc_ ^ byte) & 0xffu]; } }

                [[nodiscard]] uint32_t finish() const noexcept { return ~crc_; }

            private:
                static const std::array<uint32_t, 256>& table() noexcept {
                    static constexpr std::array<uint32_t, 256> values = [] {
                        std::array<uint32_t, 256> out{};
                        for (uint32_t i = 0; i < out.size(); ++i) {
                            uint32_t crc = i;
                            for (uint32_t bit = 0; bit < 8; ++bit) { crc = (crc >> 1u) ^ (0x82F63B78u & (0u - (crc & 1u))); }
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
            return {std::istreambuf_iterator(in), std::istreambuf_iterator<char>()};
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
                const auto suffix = ".tmp." + std::to_string(pid) + "." + std::to_string(sequence.fetch_add(1)) + "." + std::to_string(attempt);
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
        flags|= O_DIRECTORY;
        #endif
        const int fd = ::open(parent.c_str(), flags);if (fd<0) { throw std::runtime_error(std::string{context} + ": cannot open parent directory for sync"); }
        const int rc = ::fsync(fd); const int closeRc = ::close(fd);if (rc!= 0 || closeRc!= 0) {
                throw std::runtime_error(std::string{context} + ": parent directory sync failed"); }
            }
        #endif

        void replaceFileAtomically(const std::filesystem::path& tmp, const std::filesystem::path& path, const char* context) {
            #ifdef _WIN32
            if (!MoveFileExW(tmp.wstring().c_str(), path.wstring().c_str(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
                throw std::runtime_error(std::string{context} + ": atomic file replace failed");
            }
            #else
            std::filesystem::rename(tmp, path); syncParentDirectory(path, context);
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
            const int fd = ::open(tmp.c_str(), O_RDONLY); if (fd < 0) { throw std::runtime_error(std::string{context} + ": cannot reopen temp file for sync"); }
            const int rc = ::fsync(fd); const int closeRc = ::close(fd); if (rc != 0 || closeRc != 0) {
                throw std::runtime_error(std::string{context} + ": temp file sync failed");
            }
            #endif
            replaceFileAtomically(tmp, path, context);
        }

        void configureNoSigPipe(SocketHandle s) noexcept {
            #if !defined(_WIN32) && defined(SO_NOSIGPIPE)
            int enabled = 1; (void)::setsockopt(s, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled));
            #else
            (void)s;
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

        void setTimeouts(SocketHandle s, int timeoutMs) {
            #ifdef _WIN32
            const auto timeout = static_cast<DWORD>(timeoutMs);
            ::setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&timeout), sizeof(timeout));
            ::setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, reinterpret_cast<const char*>(&timeout), sizeof(timeout));
            #else
            timeval tv{}; tv.tv_sec = timeoutMs / 1000; tv.tv_usec = (timeoutMs % 1000) * 1000; ::setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
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
                const bool inProgress = rc != 0 && (::WSAGetLastError() == WSAEWOULDBLOCK || ::WSAGetLastError() == WSAEINPROGRESS || ::WSAGetLastError() ==
                    WSAEINVAL);
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
            const uint32_t payloadLen = static_cast<uint32_t>(header[6]) | (static_cast<uint32_t>(header[7]) << 8) | (static_cast<uint32_t>(header[8]) << 16) |
                (static_cast<uint32_t>(header[9]) << 24);
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

        void writeU32Le(uint8_t* out, uint32_t value) noexcept { for (size_t i = 0; i < 4; ++i) { out[i] = static_cast<uint8_t>(value >> (i * 8)); } }

        void writeU64Le(uint8_t* out, uint64_t value) noexcept { for (size_t i = 0; i < 8; ++i) { out[i] = static_cast<uint8_t>(value >> (i * 8)); } }

        uint32_t readU32Le(const uint8_t* in) noexcept {
            return static_cast<uint32_t>(in[0]) | (static_cast<uint32_t>(in[1]) << 8) | (static_cast<uint32_t>(in[2]) << 16) | (static_cast<uint32_t>(in[3]) <<
                24);
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
            if (readU32Le(wire.data()) != SECURE_HELLO_MAGIC || wire[4] != SECURE_VERSION || wire[5] != SECURE_CLIENT_HELLO) { return false; }
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
            if (readU32Le(wire.data()) != SECURE_HELLO_MAGIC || wire[4] != SECURE_VERSION || wire[5] != SECURE_SERVER_HELLO) { return false; }
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

        struct RequestRecord {
            std::array<uint8_t, 32> fingerprint{};
            uint64_t sequence = 0;
            uint64_t index = 0;
            bool operator==(const RequestRecord&) const = default;
        };
        struct RequestState {
            uint64_t expiryFloor = 0;
            std::map<ClusterRequestId, RequestRecord> results;
            bool operator==(const RequestState&) const = default;
        };
        constexpr uint32_t MAX_REQUEST_RECORDS = 65'536;

        void writeRequestId(std::vector<uint8_t>& out, const ClusterRequestId& id) {
            out.insert(out.end(), id.nonce.begin(), id.nonce.end());
            writeU64(out, id.expiresAtUnixMs);
        }
        bool readRequestId(std::span<const uint8_t> in, size_t& cursor, ClusterRequestId& id) {
            if (cursor > in.size() || in.size() - cursor < 24) { return false; }
            std::copy_n(in.data() + cursor, 16, id.nonce.begin());
            id.expiresAtUnixMs = readU64(in, cursor + 16);
            cursor += 24;
            return id.expiresAtUnixMs != 0 && std::ranges::any_of(id.nonce, [](uint8_t b) { return b != 0; });
        }
        void writeRequestState(std::vector<uint8_t>& out, const RequestState& state) {
            writeU64(out, state.expiryFloor);
            writeU32(out, static_cast<uint32_t>(state.results.size()));
            for (const auto& [id, record] : state.results) {
                writeRequestId(out, id);
                out.insert(out.end(), record.fingerprint.begin(), record.fingerprint.end());
                writeU64(out, record.sequence);
                writeU64(out, record.index);
            }
        }
        bool readRequestState(std::span<const uint8_t> in, size_t& cursor, RequestState& state) {
            if (cursor > in.size() || in.size() - cursor < 12) { return false; }
            state = {};
            state.expiryFloor = readU64(in, cursor);
            const auto count = readU32(in, cursor + 8);
            cursor += 12;
            if (count > MAX_REQUEST_RECORDS || count > (in.size() - cursor) / 72) { return false; }
            for (uint32_t i = 0; i < count; ++i) {
                ClusterRequestId id;
                RequestRecord record;
                if (!readRequestId(in, cursor, id)) { return false; }
                std::copy_n(in.data() + cursor, 32, record.fingerprint.begin());
                record.sequence = readU64(in, cursor + 32);
                record.index = readU64(in, cursor + 40);
                cursor += 48;
                if (record.sequence == 0 || record.index == 0 || id.expiresAtUnixMs <= state.expiryFloor ||
                    !state.results.emplace(id, record).second) { return false; }
            }
            return true;
        }

        struct RaftLogEntry {
            std::optional<ClusterRequestId> requestId;
            std::array<uint8_t, 32> requestFingerprint{};
            uint64_t requestTime = 0;
            uint64_t term = 0;
            uint64_t index = 0;
            // Global state-machine sequence for MUTATION/BLOB entries. Metadata entries keep this at 0.
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

        struct RaftPeerHello {
            static constexpr uint32_t VERSION = 2;
            uint64_t nodeId = 0;
            ClusterId clusterId{};
            std::array<uint8_t, 32> compatibilityFingerprint{};
            RaftBlobPolicy blobPolicy = RaftBlobPolicy::REJECT;
            bool requestsEnabled = false;
            uint64_t requestMaxRetentionMs = 0;
            uint32_t requestCapacity = 0;
        };

        enum class RaftPeerHelloStatus : uint8_t {
            ACCEPTED = 0,
            UNKNOWN_NODE = 1,
            CLUSTER_MISMATCH = 2,
            POLICY_MISMATCH = 3,
            INVALID = 4,
        };

        struct RaftPeerHelloResponse {
            RaftPeerHelloStatus status = RaftPeerHelloStatus::INVALID;
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
            RequestState requestState;
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
            bool inFlight = false;
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
            writeU8(out, entry.requestId ? 1 : 0);
            if (entry.requestId) {
                writeRequestId(out, *entry.requestId);
                out.insert(out.end(), entry.requestFingerprint.begin(), entry.requestFingerprint.end());
                writeU64(out, entry.requestTime);
            }
            out.insert(out.end(), entry.key.begin(), entry.key.end());
            out.insert(out.end(), entry.value.begin(), entry.value.end());
            return out;
        }

        bool decodeEntryPayload(std::span<const uint8_t> in, size_t& cursor, RaftLogEntry& out) {
            if (cursor + 44 > in.size()) { return false; }
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
            const uint8_t hasRequest = in[cursor++];
            if (hasRequest > 1) { return false; }
            out.requestId.reset();
            if (hasRequest) {
                ClusterRequestId id;
                if (!readRequestId(in, cursor, id) || in.size() - cursor < 40) { return false; }
                out.requestId = id;
                std::copy_n(in.data() + cursor, 32, out.requestFingerprint.begin());
                out.requestTime = readU64(in, cursor + 32);
                cursor += 40;
            }
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

        std::vector<uint8_t> encodeRaftPeerHello(const RaftPeerHello& hello) {
            std::vector<uint8_t> out;
            writeU32(out, RaftPeerHello::VERSION);
            writeU64(out, hello.nodeId);
            out.insert(out.end(), hello.clusterId.begin(), hello.clusterId.end());
            out.insert(out.end(), hello.compatibilityFingerprint.begin(), hello.compatibilityFingerprint.end());
            writeU8(out, static_cast<uint8_t>(hello.blobPolicy));
            writeU8(out, hello.requestsEnabled ? 1 : 0);
            writeU64(out, hello.requestMaxRetentionMs);
            writeU32(out, hello.requestCapacity);
            return encodeFrame(ReplMsgType::RAFT_PEER_HELLO, out);
        }

        bool decodeRaftPeerHello(std::span<const uint8_t> in, RaftPeerHello& out) {
            constexpr size_t EXPECTED_SIZE = 74;
            if (in.size() != EXPECTED_SIZE || readU32(in, 0) != RaftPeerHello::VERSION ||
                in[60] > static_cast<uint8_t>(RaftBlobPolicy::RAFT_LOG) || in[61] > 1) { return false; }
            out.nodeId = readU64(in, 4);
            std::copy_n(in.data() + 12, out.clusterId.size(), out.clusterId.begin());
            std::copy_n(in.data() + 28, out.compatibilityFingerprint.size(), out.compatibilityFingerprint.begin());
            out.blobPolicy = static_cast<RaftBlobPolicy>(in[60]);
            out.requestsEnabled = in[61] != 0;
            out.requestMaxRetentionMs = readU64(in, 62);
            out.requestCapacity = readU32(in, 70);
            return out.nodeId != 0;
        }

        std::vector<uint8_t> encodeRaftPeerHelloResponse(const RaftPeerHelloResponse& response) {
            const std::array<uint8_t, 1> payload{static_cast<uint8_t>(response.status)};
            return encodeFrame(ReplMsgType::RAFT_PEER_HELLO_RESPONSE, payload);
        }

        bool decodeRaftPeerHelloResponse(std::span<const uint8_t> in, RaftPeerHelloResponse& out) {
            if (in.size() != 1 || in[0] > static_cast<uint8_t>(RaftPeerHelloStatus::INVALID)) { return false; }
            out.status = static_cast<RaftPeerHelloStatus>(in[0]);
            return true;
        }

        std::array<uint8_t, 32> raftCompatibilityFingerprint(
            const ClusterConfig& config,
            const ClusterRuntimeOptions& runtimeOptions
        ) {
            std::vector<uint8_t> bytes{'A', 'K', 'R', 'P', '2'};
            const auto consistency = config.consistency();
            const auto raft = config.raft();
            writeU8(bytes, static_cast<uint8_t>(config.mode()));
            writeU8(bytes, static_cast<uint8_t>(consistency.mode));
            writeU8(bytes, static_cast<uint8_t>(consistency.writeConsistency));
            writeU8(bytes, static_cast<uint8_t>(consistency.ackTimeoutAction));
            writeU8(bytes, static_cast<uint8_t>(consistency.replicaLagAction));
            writeU8(bytes, static_cast<uint8_t>(raft.membership.mode));
            writeU8(bytes, raft.membership.allowOnlineVoterChanges ? 1 : 0);
            writeU8(bytes, raft.membership.allowLearners ? 1 : 0);
            writeU8(bytes, static_cast<uint8_t>(runtimeOptions.raftBlobPolicy));
            const std::array parts{std::span<const uint8_t>{bytes}};
            return crypto::hash256(parts);
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
            writeRequestState(out, rpc.requestState);
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
            if (!readNodeSetField(in, cursor, out.committedVoters) || !readOptionalNodeSetField(in, cursor, out.jointOldVoters) || !readOptionalNodeSetField(
                in,
                cursor,
                out.jointNewVoters
            )) { return false; }
            if (!readRequestState(in, cursor, out.requestState) || cursor + 5 > in.size()) { return false; }
            out.done = in[cursor++] != 0;
            if (!out.done && (!out.requestState.results.empty() || out.requestState.expiryFloor != 0)) { return false; }
            for (const auto& [id, result] : out.requestState.results) {
                if (result.index > out.lastIncludedIndex || result.sequence > out.snapshotSeq) { return false; }
            }
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
            return lhs.term == rhs.term && lhs.index == rhs.index && lhs.clientSeq == rhs.clientSeq && lhs.kind == rhs.kind && lhs.op == rhs.op && lhs.flags ==
                rhs.flags && lhs.sourceNodeId == rhs.sourceNodeId && lhs.key == rhs.key && lhs.value == rhs.value &&
                lhs.requestId == rhs.requestId && lhs.requestFingerprint == rhs.requestFingerprint && lhs.requestTime == rhs.requestTime;
        }

        bool isStateMachineEntry(RaftEntryKind kind) noexcept { return kind == RaftEntryKind::MUTATION || kind == RaftEntryKind::BLOB; }

        bool isKnownRaftEntryKind(RaftEntryKind kind) noexcept {
            return kind == RaftEntryKind::MUTATION || kind == RaftEntryKind::CONFIG_JOINT || kind == RaftEntryKind::CONFIG_FINAL || kind ==
                RaftEntryKind::BLOB || kind == RaftEntryKind::NOOP;
        }

        bool isValidRecordFlags(uint8_t flags) noexcept {
            constexpr uint8_t validFlags = core::MemHdr16::FLAG_TOMBSTONE | core::MemHdr16::FLAG_BLOB;
            return (flags & ~validFlags) == 0;
        }

        bool advanceStateMachineSequenceTracker(const RaftLogEntry& entry, uint64_t& lastSeq, bool& seqHasMutation) noexcept {
            if (!isStateMachineEntry(entry.kind)) { return entry.clientSeq == 0; }
            if (entry.clientSeq == 0 || entry.clientSeq < lastSeq) { return false; }
            if (entry.clientSeq != lastSeq) {
                lastSeq = entry.clientSeq;
                seqHasMutation = false;
            }
            if (entry.kind == RaftEntryKind::MUTATION) {
                if (seqHasMutation) { return false; }
                seqHasMutation = true;
            }
            return true;
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
            return ReplFrameHeader::MAX_PAYLOAD_SIZE - static_cast<uint32_t>(RAFT_APPEND_ENTRIES_BASE_SIZE + RAFT_ENTRY_LENGTH_PREFIX_SIZE +
                RAFT_ENTRY_PAYLOAD_BASE_SIZE + RAFT_BLOB_CHUNK_KEY_SIZE);
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

        bool isValidRaftLogEntryShape(const RaftLogEntry& entry) {
            if (!isKnownRaftEntryKind(entry.kind)) { return false; }
            if (entry.requestId && (entry.kind != RaftEntryKind::MUTATION || entry.requestTime == 0 ||
                entry.requestTime >= entry.requestId->expiresAtUnixMs)) { return false; }
            if (entry.kind == RaftEntryKind::BLOB) {
                RaftBlobChunk chunk;
                return entry.op == ReplOpType::PUT && entry.flags == 0 && decodeBlobChunkKey(entry.key, chunk) && entry.value.size() <= chunk.totalSize -
                    chunk.offset;
            }
            if (entry.kind == RaftEntryKind::CONFIG_JOINT) {
                std::vector<NodeInfo> oldVoters;
                std::vector<NodeInfo> newVoters;
                return entry.clientSeq == 0 && entry.op == ReplOpType::PUT && entry.flags == 0 && decodeNodeSet(entry.key, oldVoters) && decodeNodeSet(
                    entry.value,
                    newVoters
                ) && !oldVoters.empty() && !newVoters.empty();
            }
            if (entry.kind == RaftEntryKind::CONFIG_FINAL) {
                std::vector<NodeInfo> oldVoters;
                std::vector<NodeInfo> newVoters;
                return entry.clientSeq == 0 && entry.op == ReplOpType::PUT && entry.flags == 0 && (entry.key.empty() || decodeNodeSet(entry.key, oldVoters))
                    && decodeNodeSet(entry.value, newVoters) && !newVoters.empty();
            }
            if (entry.kind == RaftEntryKind::NOOP) {
                return entry.clientSeq == 0 && entry.op == ReplOpType::PUT && entry.flags == 0 && entry.key.empty() && entry.value.empty();
            }
            return (entry.op == ReplOpType::PUT || entry.op == ReplOpType::REMOVE) && isValidRecordFlags(entry.flags);
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

            struct SnapshotInstallTransaction {
                RequestState requestState;
                uint64_t snapshotSeq = 0;
                uint64_t lastIncludedIndex = 0;
                uint64_t lastIncludedTerm = 0;
                uint64_t entryCount = 0;
                std::vector<NodeInfo> committedVoters;
                std::optional<std::vector<NodeInfo>> jointOldVoters;
                std::optional<std::vector<NodeInfo>> jointNewVoters;
            };

            struct DurableLogState {
                RequestState requestState;
                RequestState journalState;
                uint64_t requestJournalGeneration = 0;
                uint64_t requestJournalBytes = 0;
                uint64_t requestJournalRecords = 0;
                uint64_t commitIndex = 0;
                uint64_t lastApplied = 0;
                uint64_t lastIncludedIndex = 0;
                uint64_t lastIncludedTerm = 0;
                uint64_t lastIncludedStateMachineSeq = 0;
                std::vector<RaftLogEntry> log;
                std::vector<NodeInfo> committedVoters;
                std::optional<std::vector<NodeInfo>> jointOldVoters;
                std::optional<std::vector<NodeInfo>> jointNewVoters;
                std::vector<NodeInfo> peers;
                std::vector<PeerReplicationState> peerReplication;
                std::optional<SnapshotInstallTransaction> durableSnapshotInstall;
            };

            struct DurableHardState {
                uint64_t currentTerm = 0;
                uint64_t votedFor = 0;
            };

            RequestState requestState_;
            std::mutex requestAdmissionMutex_;
            std::filesystem::path dbDir_;
            std::filesystem::path statePath_;
            std::filesystem::path logPath_;
            std::filesystem::path requestJournalBasePath_;
            ClusterConfig config_;
            ClusterRouter router_;
            uint64_t selfNodeId_ = 0;
            const NodeInfo* self_ = nullptr;
            std::vector<NodeInfo> peers_;
            ClusterEngineCallbacks callbacks_;
            ClusterRuntimeOptions runtimeOptions_;
            std::array<uint8_t, 32> compatibilityFingerprint_{};
            crypto::NodeIdentity localIdentity_{};

            mutable std::mutex mutex_;
            std::recursive_mutex lifecycleMutex_;
            std::condition_variable cv_;
            bool running_ = false;
            uint64_t currentTerm_ = 0;
            uint64_t votedFor_ = 0;
            uint64_t commitIndex_ = 0;
            uint64_t lastApplied_ = 0;
            uint64_t lastIncludedIndex_ = 0;
            uint64_t lastIncludedTerm_ = 0;
            uint64_t lastIncludedStateMachineSeq_ = 0;
            std::vector<RaftLogEntry> log_;
            struct LogLocation {
                uint64_t index;
                uint64_t term;
                uint64_t segment;
                uint64_t begin;
                uint64_t end;
            };
            struct LogExtent {
                uint64_t segment;
                uint64_t begin;
                uint64_t end;
            };
            std::vector<LogLocation> persistedLogLocations_;
            uint64_t nextLogSegment_ = 1;
            RequestState journalState_;
            uint64_t requestJournalGeneration_ = 0;
            uint64_t requestJournalBytes_ = 0;
            uint64_t requestJournalRecords_ = 0;
            bool logIoFailed_ = false;
            static constexpr uint64_t LOG_SEGMENT_BYTES = 16ull * 1024ull * 1024ull;
            static constexpr uint64_t REQUEST_JOURNAL_COMPACT_BYTES = 1024ull * 1024ull;
            std::vector<NodeInfo> committedVoters_;
            std::optional<std::vector<NodeInfo>> jointOldVoters_;
            std::optional<std::vector<NodeInfo>> jointNewVoters_;
            std::vector<PeerReplicationState> peerReplication_;
            std::condition_variable peerReplicationCv_;
            Clock::time_point electionDeadline_{};
            bool forceElection_ = false;
            std::atomic<RaftRole> role_{RaftRole::FOLLOWER};
            std::mutex applyMutex_;
            PendingSnapshotInstall pendingSnapshotInstall_;
            std::optional<SnapshotInstallTransaction> durableSnapshotInstall_;
            uint64_t observedLeaderTerm_ = 0;
            uint64_t observedLeaderId_ = 0;
            Clock::time_point nextCompactionCheck_{};
            Clock::time_point nextCompactionDeadline_{};

            SocketHandle listenSock_ = BAD_SOCKET;
            std::thread acceptThread_;
            std::thread timerThread_;
            std::atomic<size_t> activeClientHandlers_{0};
            std::mutex clientHandlersMutex_;
            std::condition_variable clientHandlersCv_;
            std::unordered_set<SocketHandle> clientSockets_;

            struct PeerWorker {
                std::mutex mutex;
                std::condition_variable cv;
                bool stopping = false;
                bool replicate = false;
                bool heartbeat = false;
                uint64_t target = 0;
                uint64_t readRound = 0;
                std::deque<std::function<void()>> tasks;
                std::thread thread;
                std::mutex ioMutex;
                std::mutex socketMutex;
                SocketHandle socket = BAD_SOCKET;
                NodeInfo endpoint;
                std::unique_ptr<crypto::SecureSession> secure;
                std::atomic<uint64_t> lastSuccessfulContactAtUs{0};
                std::atomic<uint64_t> roundTripsSucceeded{0};
                std::atomic<uint64_t> roundTripsFailed{0};
                std::atomic<uint64_t> consecutiveRoundTripFailures{0};
                std::atomic<uint64_t> lastRoundTripAtUs{0};
                std::atomic<uint64_t> lastRoundTripFailureAtUs{0};
                AtomicLatencyHistogram roundTripLatencyUs;
                ~PeerWorker() { closeSocket(socket); }
            };

            struct PeerRoundTripObservation {
                std::shared_ptr<PeerWorker> worker;
                Clock::time_point started = Clock::now();
                bool succeeded = false;

                ~PeerRoundTripObservation() {
                    const uint64_t nowUs = observationNowUs();
                    worker->lastRoundTripAtUs.store(nowUs, std::memory_order_relaxed);
                    if (succeeded) {
                        ++worker->roundTripsSucceeded;
                        worker->consecutiveRoundTripFailures.store(0, std::memory_order_relaxed);
                        const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - started).count();
                        worker->roundTripLatencyUs.observe(static_cast<uint64_t>(std::max<int64_t>(0, elapsed)));
                    }
                    else {
                        ++worker->roundTripsFailed;
                        ++worker->consecutiveRoundTripFailures;
                        worker->lastRoundTripFailureAtUs.store(nowUs, std::memory_order_relaxed);
                    }
                }
            };
            mutable std::mutex workersMutex_;
            std::atomic<uint64_t> outboundConnections_{0}, peerWorkersStarted_{0}, proposalBatches_{0};
            std::atomic<uint64_t> peerPolicyMismatchRejects_{0}, foreignClusterRejects_{0}, endpointStartFailures_{0};
            std::atomic<uint64_t> expiredRequests_{0}, rejectedRequests_{0};
            std::atomic<uint64_t> requestJournalBytesWritten_{0}, requestJournalCompactions_{0};
            std::atomic<ClusterHealthState> health_{ClusterHealthState::HEALTHY};
            std::atomic<ClusterFailureCode> lastFailure_{ClusterFailureCode::NONE};
            std::atomic<uint64_t> lastFailureAtUs_{0};
            std::atomic<uint64_t> runtimeStartedAtUs_{0};
            bool workersStopping_ = false;
            std::unordered_map<uint64_t, std::shared_ptr<PeerWorker>> workers_;
            uint64_t nextReadRound_ = 0;
            std::unordered_map<uint64_t, std::pair<uint64_t, uint64_t>> readAcks_;

            void recordFailure(ClusterFailureCode failure, ClusterHealthState health) noexcept {
                lastFailureAtUs_.store(observationNowUs(), std::memory_order_relaxed);
                lastFailure_.store(failure, std::memory_order_relaxed);
                if (health == ClusterHealthState::FAILED || health_.load(std::memory_order_relaxed) != ClusterHealthState::FAILED) {
                    health_.store(health, std::memory_order_relaxed);
                }
            }

            struct RequestCompletion {
                ClusterRequestId id;
                std::array<uint8_t, 32> fingerprint{};
                uint64_t sequence = 0;
                std::promise<ClusterRequestResult> promise;
                std::shared_future<ClusterRequestResult> future = promise.get_future().share();
            };
            struct Proposal {
                std::shared_ptr<RequestCompletion> request;
                uint64_t entryTerm = 0;
                std::vector<RaftLogEntry> entries;
                uint64_t term = 0;
                uint64_t index = 0;
                Clock::time_point deadline;
                std::promise<void> completion;
                size_t bytes = 0;
                std::exception_ptr error;
            };
            std::deque<std::shared_ptr<Proposal>> proposalQueue_;
            std::vector<std::shared_ptr<Proposal>> proposals_;
            std::thread proposalThread_;
            bool administrativeChange_ = false;
            std::mutex administrationMutex_;

            std::shared_ptr<PeerWorker> peerWorker(uint64_t peerId) {
                std::lock_guard lock{workersMutex_};
                if (workersStopping_) { throw std::runtime_error("Raft transport is closing"); }
                auto& worker = workers_[peerId];
                if (!worker) {
                    worker = std::make_shared<PeerWorker>();
                    ++peerWorkersStarted_;
                    worker->thread = std::thread([this, peerId, worker] {
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
                    });
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
                worker->tasks.push_back([task] { (*task)(); });
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
                    std::erase_if(workers_, [&](const auto& item) {
                        if (containsNode(peers, item.first)) { return false; }
                        retired.push_back(item.second);
                        return true;
                    });
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

            bool exchangeRpc(const NodeInfo& peer, const std::vector<uint8_t>& wire, ReplMsgType expected, DecodedFrame& frame, int timeoutMs) {
                auto worker = peerWorker(peer.nodeId);
                std::lock_guard ioLock{worker->ioMutex};
                const auto reset = [&] {
                    std::lock_guard socketLock{worker->socketMutex};
                    closeSocket(worker->socket);
                    worker->socket = BAD_SOCKET;
                    worker->secure.reset();
                };
                if (wire.empty()) { return false; }
                PeerRoundTripObservation observation{worker};
                if (worker->endpoint.host != peer.host || worker->endpoint.replPort != peer.replPort) { reset(); }
                if (!socketOk(worker->socket)) {
                    auto socket = connectTo(peer.host, peer.replPort, timeoutMs);
                    if (!socketOk(socket)) { return false; }
                    ++outboundConnections_;
                    {
                        std::lock_guard socketLock{worker->socketMutex};
                        std::lock_guard workLock{worker->mutex};
                        if (worker->stopping) { closeSocket(socket); return false; }
                        worker->socket = socket;
                        worker->endpoint = peer;
                    }
                    if (runtimeOptions_.transportMode == TransportMode::SECURE) {
                        worker->secure = openSecureSession(socket, peer.nodeId);
                        if (!worker->secure) { reset(); return false; }
                    }
                    const RaftPeerHello hello{
                        .nodeId = selfNodeId_,
                        .clusterId = config_.clusterId(),
                        .compatibilityFingerprint = compatibilityFingerprint_,
                        .blobPolicy = runtimeOptions_.raftBlobPolicy,
                        .requestsEnabled = runtimeOptions_.requests.enabled,
                        .requestMaxRetentionMs = runtimeOptions_.requests.maxRetentionMs,
                        .requestCapacity = runtimeOptions_.requests.maxTrackedRequests,
                    };
                    DecodedFrame helloFrame;
                    RaftPeerHelloResponse helloResponse;
                    if (!sendFrame(socket, worker->secure.get(), encodeRaftPeerHello(hello)) ||
                        !recvFrame(socket, worker->secure.get(), helloFrame) || helloFrame.type != ReplMsgType::RAFT_PEER_HELLO_RESPONSE ||
                        !decodeRaftPeerHelloResponse(helloFrame.payload, helloResponse) || helloResponse.status != RaftPeerHelloStatus::ACCEPTED) {
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
                setTimeouts(worker->socket, timeoutMs);
                if (!sendFrame(worker->socket, worker->secure.get(), wire) ||
                    !recvFrame(worker->socket, worker->secure.get(), frame) || frame.type != expected) {
                    reset();
                    return false;
                }
                worker->lastSuccessfulContactAtUs.store(observationNowUs(), std::memory_order_relaxed);
                observation.succeeded = true;
                return true;
            }

            std::future<void> submitProposal(std::vector<RaftLogEntry> entries, std::shared_ptr<Proposal> proposal = {}) {
                if (!proposal) { proposal = std::make_shared<Proposal>(); }
                proposal->entries = std::move(entries);
                for (const auto& entry : proposal->entries) { proposal->bytes += entry.key.size() + entry.value.size() + sizeof(RaftLogEntry); }
                auto future = proposal->completion.get_future();
                std::lock_guard lock{mutex_};
                if (!running_ || role_.load() != RaftRole::LEADER || administrativeChange_ ||
                    (proposal->term != 0 && proposal->term != currentTerm_)) {
                    throw std::runtime_error("RaftConsensusRuntime: proposal requires an active leader without a membership/transfer operation");
                }
                constexpr size_t limit = 256ull * 1024 * 1024;
                size_t pendingBytes = 0;
                // Timed-out entries may still commit. Keep counting their retained
                // payloads so a partition cannot bypass admission backpressure.
                for (auto it = log_.rbegin(); it != log_.rend() && it->index > lastApplied_; ++it) {
                    pendingBytes += it->key.size() + it->value.size() + sizeof(RaftLogEntry);
                }
                for (const auto& queued : proposalQueue_) { if (!queued->error) { pendingBytes += queued->bytes; } }
                if (proposals_.size() >= 4096 || proposal->bytes > limit || pendingBytes > limit - proposal->bytes) {
                    throw std::runtime_error("RaftConsensusRuntime: proposal queue is full");
                }
                proposal->term = currentTerm_;
                proposal->deadline = Clock::now() + std::chrono::milliseconds{config_.consistency().ackTimeoutMs};
                proposals_.push_back(proposal);
                proposalQueue_.push_back(proposal);
                cv_.notify_all();
                return future;
            }

            void finishProposalsLocked() {
                std::erase_if(proposals_, [&](const auto& proposal) {
                    const bool applied = currentTerm_ == proposal->term && role_.load() == RaftRole::LEADER &&
                        proposal->index != 0 && lastApplied_ >= proposal->index &&
                        (proposal->index <= lastIncludedIndex_ || termAt(proposal->index) == (proposal->entryTerm ? proposal->entryTerm : proposal->term));
                    if (!proposal->error && !applied && running_ && role_.load() == RaftRole::LEADER &&
                        currentTerm_ == proposal->term && Clock::now() < proposal->deadline) { return false; }
                    if (!proposal->error && applied) {
                        proposal->completion.set_value();
                        if (proposal->request) { proposal->request->promise.set_value({ClusterRequestStatus::APPLIED, proposal->request->sequence, proposal->index}); }
                    }
                    else {
                        if (!proposal->error) { proposal->error = std::make_exception_ptr(std::runtime_error("RaftConsensusRuntime: proposal interrupted or quorum timed out; outcome may be unknown")); }
                        proposal->completion.set_exception(proposal->error);
                        if (proposal->request) { proposal->request->promise.set_exception(proposal->error); }
                    }
                    return true;
                });
            }

            void advanceLeaderCommit() {
                bool advanced = false;
                {
                    std::lock_guard lock{mutex_};
                    if (!running_ || role_.load() != RaftRole::LEADER) { return; }
                    for (auto it = log_.rbegin(); it != log_.rend() && it->index > commitIndex_; ++it) {
                        if (!canCommitEntryLocked(*it)) { continue; }
                        commitIndex_ = it->index;
                        persistLog();
                        advanced = true;
                        break;
                    }
                }
                applyCommitted();
                {
                    std::lock_guard lock{mutex_};
                    finishProposalsLocked();
                    peerReplicationCv_.notify_all();
                }
                if (advanced) { sendHeartbeats(); }
            }

            void proposalLoop() {
                for (;;) {
                    try {
                        bool appended = false;
                        {
                            std::unique_lock lock{mutex_};
                            cv_.wait_for(lock, std::chrono::milliseconds{10}, [&] { return !running_ || !proposalQueue_.empty(); });
                            if (!running_) { finishProposalsLocked(); proposalQueue_.clear(); return; }
                            if (!proposalQueue_.empty()) {
                                // Coalesce concurrent admissions without waiting for prior quorum I/O.
                                cv_.wait_for(lock, std::chrono::milliseconds{1}, [&] { return !running_ || proposalQueue_.size() >= 256; });
                            }
                            size_t batchBytes = 0;
                            for (size_t count = 0; running_ && !proposalQueue_.empty() && count < 256; ++count) {
                                auto proposal = proposalQueue_.front();
                                proposalQueue_.pop_front();
                                if (proposal->error) { continue; }
                                if (role_.load() != RaftRole::LEADER || currentTerm_ != proposal->term || Clock::now() >= proposal->deadline) {
                                    proposal->error = std::make_exception_ptr(std::runtime_error("Raft proposal expired before append"));
                                    continue;
                                }
                                const size_t previousSize = log_.size();
                                try {
                                    for (auto& entry : proposal->entries) {
                                        entry.term = currentTerm_;
                                        entry.index = lastLogIndex() + 1;
                                        if (!isValidRaftLogEntryShape(entry) || !entryPreservesStateMachineSequenceLocked(entry)) {
                                            throw std::invalid_argument("RaftConsensusRuntime: invalid proposal or state-machine sequence");
                                        }
                                        log_.push_back(std::move(entry));
                                    }
                                    proposal->index = lastLogIndex();
                                    proposal->entries.clear();
                                    appended = true;
                                    batchBytes += proposal->bytes;
                                }
                                catch (...) { log_.resize(previousSize); proposal->error = std::current_exception(); }
                                if (batchBytes >= 4 * 1024 * 1024) { break; }
                            }
                            if (appended) { persistLog(); ++proposalBatches_; }
                            finishProposalsLocked();
                        }
                        if (appended) { sendHeartbeats(); }
                        advanceLeaderCommit();
                        if (appended) { maybeCompactLog(); }
                    }
                    catch (...) {
                        std::lock_guard lock{mutex_};
                        stopRuntimeLocked();
                        finishProposalsLocked();
                        proposalQueue_.clear();
                        return;
                    }
                }
            }

            struct AdministrativeGuard {
                Impl& runtime;
                std::unique_lock<std::mutex> serialization;
                explicit AdministrativeGuard(Impl& owner) : runtime{owner}, serialization{owner.administrationMutex_} {
                    std::lock_guard lock{runtime.mutex_};
                    if (!runtime.proposals_.empty()) { throw std::runtime_error("RaftConsensusRuntime: proposals are in flight; retry administrative change"); }
                    runtime.administrativeChange_ = true;
                }
                ~AdministrativeGuard() {
                    std::lock_guard lock{runtime.mutex_};
                    runtime.administrativeChange_ = false;
                }
            };

            Clock::time_point nextElectionDeadline() {
                const auto configuredAckTimeoutMs = config_.consistency().ackTimeoutMs;
                const auto baseTimeoutMs = std::clamp<uint32_t>(std::max<uint32_t>(3000u, configuredAckTimeoutMs * 2u), 3000u, 6000u);
                // Test hook: keep production Raft randomized, but let smoke tests make leadership reproducible.
                if (const char* deterministic = std::getenv("AKKARADB_TEST_DETERMINISTIC_RAFT_ELECTION"); deterministic != nullptr && std::string_view{
                    deterministic
                } == "1") {
                    uint32_t rank = 0;
                    for (const auto& node : config_.dataNodes()) { if (node.nodeId < selfNodeId_) { ++rank; } }
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
                nodes.erase(std::ranges::unique(nodes, [](const NodeInfo& lhs, const NodeInfo& rhs) { return lhs.nodeId == rhs.nodeId; }).begin(), nodes.end());
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
                        peerReplication_.push_back(PeerReplicationState{.node = peer, .nextIndex = 1, .matchIndex = 0, .inFlight = false});
                    }
                }
                std::erase_if(peerReplication_, [&](const PeerReplicationState& state) { return !containsNode(peers_, state.node.nodeId); });
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
                for (const auto& voter : voters) { if (std::ranges::find(grantedVotes, voter.nodeId) != grantedVotes.end()) { ++granted; } }
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
                for (const auto& pending : log_) {
                    if (pending.index <= commitIndex_ || pending.index > index) { continue; }
                    if (pending.kind == RaftEntryKind::CONFIG_JOINT) {
                        std::vector<NodeInfo> oldVoters;
                        std::vector<NodeInfo> newVoters;
                        if (!decodeNodeSet(pending.key, oldVoters) || !decodeNodeSet(pending.value, newVoters)) { return false; }
                        return replicatedByMajorityLocked(oldVoters, index) && replicatedByMajorityLocked(newVoters, index);
                    }
                    if (pending.kind == RaftEntryKind::CONFIG_FINAL && !pending.key.empty()) {
                        std::vector<NodeInfo> oldVoters;
                        std::vector<NodeInfo> newVoters;
                        if (!decodeNodeSet(pending.key, oldVoters) || !decodeNodeSet(pending.value, newVoters)) { return false; }
                        return replicatedByMajorityLocked(oldVoters, index) && replicatedByMajorityLocked(newVoters, index);
                    }
                }
                return replicatedByMajorityLocked(committedVoters_, index);
            }

            bool canCommitEntryLocked(const RaftLogEntry& entry) { return entry.term == currentTerm_ && hasCommitQuorumLocked(entry.index, &entry); }

            static bool sameNodeSet(const std::vector<NodeInfo>& lhs, const std::vector<NodeInfo>& rhs) { return encodeNodeSet(lhs) == encodeNodeSet(rhs); }

            static bool sameOptionalNodeSet(const std::optional<std::vector<NodeInfo>>& lhs, const std::optional<std::vector<NodeInfo>>& rhs) {
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
                const uint64_t offset = index - lastIncludedIndex_ - 1;
                if (offset >= log_.size()) { return std::nullopt; }
                const auto& entry = log_[static_cast<size_t>(offset)];
                return entry.index == index ? std::optional<RaftLogEntry>{entry} : std::nullopt;
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

            bool entryPreservesStateMachineSequenceLocked(const RaftLogEntry& incoming) const {
                if (isStateMachineEntry(incoming.kind) && incoming.clientSeq <= lastIncludedStateMachineSeq_) { return false; }
                uint64_t lastStateMachineSeq = lastIncludedStateMachineSeq_;
                bool seqHasMutation = false;
                for (const auto& entry : log_) {
                    if (entry.index >= incoming.index) { break; }
                    if (!advanceStateMachineSequenceTracker(entry, lastStateMachineSeq, seqHasMutation)) { return false; }
                }
                return advanceStateMachineSequenceTracker(incoming, lastStateMachineSeq, seqHasMutation);
            }

            std::optional<uint64_t> compactableLogIndexForSnapshotSeq(uint64_t snapshotSeq) const {
                std::optional<uint64_t> target;
                for (const auto& entry : log_) {
                    if ((entry.kind == RaftEntryKind::MUTATION || entry.kind == RaftEntryKind::BLOB) && entry.clientSeq <= snapshotSeq) {
                        target = entry.index;
                        continue;
                    }
                    if (entry.kind == RaftEntryKind::MUTATION || entry.kind == RaftEntryKind::BLOB) { return target; }
                    if (entry.kind == RaftEntryKind::CONFIG_JOINT || entry.kind == RaftEntryKind::CONFIG_FINAL || entry.kind == RaftEntryKind::NOOP) {
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
                bytes.insert(bytes.end(), {'A', 'K', 'R', 'S', '3'});
                bytes.insert(bytes.end(), config_.clusterId().begin(), config_.clusterId().end());
                writeU64(bytes, currentTerm_);
                writeU64(bytes, votedFor_);
                const size_t crcOffset = bytes.size();
                writeU32(bytes, 0);
                writeU32At(bytes, crcOffset, crcWithZeroedField(bytes, crcOffset));
                writeFileAtomically(statePath_, bytes, "RaftConsensusRuntime state");
            }

            void enforceLogInvariantsLocked(const char* context) const {
                if (committedVoters_.empty()) { throw std::runtime_error(std::string{context} + ": empty committed voter set"); }
                if (lastIncludedIndex_ > commitIndex_) { throw std::runtime_error(std::string{context} + ": lastIncludedIndex is ahead of commitIndex"); }
                if (lastApplied_ < lastIncludedIndex_) { throw std::runtime_error(std::string{context} + ": lastApplied is behind lastIncludedIndex"); }
                if (lastApplied_ > commitIndex_) { throw std::runtime_error(std::string{context} + ": lastApplied is ahead of commitIndex"); }
                if (commitIndex_ > lastLogIndex()) { throw std::runtime_error(std::string{context} + ": commitIndex is ahead of log"); }
                for (const auto& [id, result] : requestState_.results) {
                    if (result.index > lastApplied_) { throw std::runtime_error(std::string{context} + ": request result is ahead of applied log"); }
                }
                uint64_t expectedIndex = lastIncludedIndex_ + 1;
                for (const auto& entry : log_) {
                    if (entry.index != expectedIndex) { throw std::runtime_error(std::string{context} + ": non-contiguous log"); }
                    ++expectedIndex;
                }
                if (jointOldVoters_.has_value() != jointNewVoters_.has_value()) {
                    throw std::runtime_error(std::string{context} + ": incomplete joint voter set");
                }
                uint64_t lastStateMachineSeq = lastIncludedStateMachineSeq_;
                bool seqHasMutation = false;
                for (const auto& entry : log_) {
                    if (!isValidRaftLogEntryShape(entry)) { throw std::runtime_error(std::string{context} + ": invalid log entry shape"); }
                    if (isStateMachineEntry(entry.kind) && entry.clientSeq <= lastIncludedStateMachineSeq_) {
                        throw std::runtime_error(std::string{context} + ": state-machine sequence is behind compacted snapshot");
                    }
                    if (!advanceStateMachineSequenceTracker(entry, lastStateMachineSeq, seqHasMutation)) {
                        throw std::runtime_error(std::string{context} + ": invalid state-machine sequence metadata");
                    }
                }
            }

            void recoverState() {
                if (!std::filesystem::exists(statePath_)) { return; }
                try {
                    const auto bytes = readWholeFile(statePath_, "RaftConsensusRuntime state");
                    constexpr size_t expectedSize = 5 + 16 + 8 + 8 + 4;
                    constexpr size_t crcOffset = expectedSize - 4;
                    if (bytes.size() != expectedSize) { throw std::runtime_error("invalid state file size"); }
                    if (std::string_view{reinterpret_cast<const char*>(bytes.data()), 5} != "AKRS3") { throw std::runtime_error("bad state file magic"); }
                    if (readU32(bytes, crcOffset) != crcWithZeroedField(bytes, crcOffset)) { throw std::runtime_error("state file CRC mismatch"); }
                    if (!std::equal(config_.clusterId().begin(), config_.clusterId().end(), bytes.begin() + 5)) {
                        throw std::runtime_error("hard state belongs to a different cluster");
                    }
                    currentTerm_ = readU64(bytes, 21);
                    votedFor_ = readU64(bytes, 29);
                }
                catch (const std::exception& ex) { throw std::runtime_error(std::string{"RaftConsensusRuntime: corrupt hard state: "} + ex.what()); }
            }

            void writeSnapshotInstallTransactionField(std::vector<uint8_t>& out) const {
                writeU8(out, durableSnapshotInstall_ ? 1 : 0);
                if (!durableSnapshotInstall_) { return; }
                writeU64(out, durableSnapshotInstall_->snapshotSeq);
                writeU64(out, durableSnapshotInstall_->lastIncludedIndex);
                writeU64(out, durableSnapshotInstall_->lastIncludedTerm);
                writeU64(out, durableSnapshotInstall_->entryCount);
                writeNodeSetField(out, durableSnapshotInstall_->committedVoters);
                writeOptionalNodeSetField(out, durableSnapshotInstall_->jointOldVoters);
                writeOptionalNodeSetField(out, durableSnapshotInstall_->jointNewVoters);
                writeRequestState(out, durableSnapshotInstall_->requestState);
            }

            static bool readSnapshotInstallTransactionField(
                std::span<const uint8_t> in,
                size_t& cursor,
                std::optional<SnapshotInstallTransaction>& transaction
            ) {
                if (cursor + 1 > in.size()) { return false; }
                const bool hasTransaction = in[cursor++] != 0;
                if (!hasTransaction) {
                    transaction.reset();
                    return true;
                }
                if (cursor + 32 > in.size()) { return false; }
                SnapshotInstallTransaction decoded;
                decoded.snapshotSeq = readU64(in, cursor);
                cursor += 8;
                decoded.lastIncludedIndex = readU64(in, cursor);
                cursor += 8;
                decoded.lastIncludedTerm = readU64(in, cursor);
                cursor += 8;
                decoded.entryCount = readU64(in, cursor);
                cursor += 8;
                if (!readNodeSetField(in, cursor, decoded.committedVoters) || !readOptionalNodeSetField(in, cursor, decoded.jointOldVoters) || !
                    readOptionalNodeSetField(in, cursor, decoded.jointNewVoters)) {
                    return false;
                }
                if (!readRequestState(in, cursor, decoded.requestState)) { return false; }
                if (decoded.committedVoters.empty() || decoded.jointOldVoters.has_value() != decoded.jointNewVoters.has_value()) { return false; }
                if (decoded.jointOldVoters && (decoded.jointOldVoters->empty() || decoded.jointNewVoters->empty())) { return false; }
                transaction = std::move(decoded);
                return true;
            }

            [[nodiscard]] std::filesystem::path logSegmentDir() const {
                auto path = logPath_;
                path += ".segments";
                return path;
            }

            [[nodiscard]] std::filesystem::path logSegmentPath(uint64_t id) const {
                return logSegmentDir() / ("segment-" + std::to_string(id) + ".akrl");
            }

            [[nodiscard]] std::filesystem::path requestJournalPath(uint64_t generation) const {
                auto path = requestJournalBasePath_;
                path += "." + std::to_string(generation);
                return path;
            }

            static std::vector<uint8_t> requestJournalRecord(uint8_t type, const RequestState& state,
                const std::vector<std::pair<ClusterRequestId, RequestRecord>>& additions = {}) {
                std::vector<uint8_t> payload;
                writeU8(payload, type);
                if (type == 0) { writeRequestState(payload, state); }
                else {
                    writeU64(payload, state.expiryFloor);
                    writeU32(payload, static_cast<uint32_t>(additions.size()));
                    for (const auto& [id, record] : additions) {
                        writeRequestId(payload, id);
                        payload.insert(payload.end(), record.fingerprint.begin(), record.fingerprint.end());
                        writeU64(payload, record.sequence);
                        writeU64(payload, record.index);
                    }
                }
                std::vector<uint8_t> framed;
                writeU32(framed, static_cast<uint32_t>(payload.size()));
                writeU32(framed, crcBytes(payload));
                framed.insert(framed.end(), payload.begin(), payload.end());
                return framed;
            }

            static bool requestStateDelta(const RequestState& before, const RequestState& after,
                std::vector<std::pair<ClusterRequestId, RequestRecord>>& additions) {
                if (after.expiryFloor < before.expiryFloor) { return false; }
                for (const auto& [id, record] : before.results) {
                    const auto found = after.results.find(id);
                    if (id.expiresAtUnixMs > after.expiryFloor && (found == after.results.end() || found->second != record)) { return false; }
                    if (found != after.results.end() && found->second != record) { return false; }
                }
                additions.clear();
                for (const auto& [id, record] : after.results) {
                    if (id.expiresAtUnixMs <= after.expiryFloor) { return false; }
                    const auto found = before.results.find(id);
                    if (found == before.results.end()) { additions.emplace_back(id, record); }
                    else if (found->second != record) { return false; }
                }
                return true;
            }

            void writeRequestJournalCheckpointLocked() {
                if (requestJournalGeneration_ == UINT64_MAX) { throw std::runtime_error("Raft request journal: generations exhausted"); }
                uint64_t generation = requestJournalGeneration_ + 1;
                while (std::filesystem::exists(requestJournalPath(generation))) {
                    if (generation == UINT64_MAX) { throw std::runtime_error("Raft request journal: generations exhausted"); }
                    ++generation;
                }
                std::vector<uint8_t> bytes{'A', 'K', 'R', 'Q', '1'};
                const auto record = requestJournalRecord(0, requestState_);
                bytes.insert(bytes.end(), record.begin(), record.end());
                writeFileAtomically(requestJournalPath(generation), bytes, "Raft request journal checkpoint");
                crashAtLogTestPoint("raft.request.after_journal_sync");
                requestJournalBytesWritten_.fetch_add(bytes.size(), std::memory_order_relaxed);
                requestJournalCompactions_.fetch_add(1, std::memory_order_relaxed);
                requestJournalGeneration_ = generation;
                requestJournalBytes_ = bytes.size();
                requestJournalRecords_ = 1;
                journalState_ = requestState_;
            }

            void prepareRequestJournalLocked() {
                if (journalState_ == requestState_) { return; }
                std::vector<std::pair<ClusterRequestId, RequestRecord>> additions;
                const uint64_t forcedRecordLimit = requestJournalCompactRecords();
                const uint64_t checkpointEstimate = 5 + 8 + 1 + 12 + 72 * requestState_.results.size();
                const bool staleJournal = requestJournalBytes_ >= REQUEST_JOURNAL_COMPACT_BYTES &&
                    requestJournalBytes_ > checkpointEstimate * 2;
                const bool compact = requestJournalGeneration_ == 0 || staleJournal ||
                    (forcedRecordLimit != 0 && requestJournalRecords_ >= forcedRecordLimit) ||
                    !requestStateDelta(journalState_, requestState_, additions);
                if (compact) {
                    writeRequestJournalCheckpointLocked();
                    return;
                }
                const auto path = requestJournalPath(requestJournalGeneration_);
                if (!std::filesystem::exists(path) || std::filesystem::file_size(path) < requestJournalBytes_) {
                    throw std::runtime_error("Raft request journal: referenced journal is missing or truncated");
                }
                if (std::filesystem::file_size(path) != requestJournalBytes_) {
                    std::filesystem::resize_file(path, requestJournalBytes_);
                }
                const auto record = requestJournalRecord(1, requestState_, additions);
                {
                    std::ofstream out(path, std::ios::binary | std::ios::app);
                    if (!out) { throw std::runtime_error("Raft request journal: cannot append delta"); }
                    out.write(reinterpret_cast<const char*>(record.data()), static_cast<std::streamsize>(record.size()));
                    out.flush();
                    if (!out) { throw std::runtime_error("Raft request journal: delta write failed"); }
                }
                syncLogSegment(path);
                crashAtLogTestPoint("raft.request.after_journal_sync");
                requestJournalBytesWritten_.fetch_add(record.size(), std::memory_order_relaxed);
                requestJournalBytes_ += record.size();
                ++requestJournalRecords_;
                journalState_ = requestState_;
            }

            void recoverRequestJournalLocked() {
                requestState_ = {};
                journalState_ = {};
                if (requestJournalGeneration_ == 0) {
                    if (requestJournalBytes_ != 0 || requestJournalRecords_ != 0) {
                        throw std::runtime_error("Raft request journal: invalid empty reference");
                    }
                    return;
                }
                const auto path = requestJournalPath(requestJournalGeneration_);
                if (!std::filesystem::exists(path)) { throw std::runtime_error("Raft request journal: referenced file is missing"); }
                const auto bytes = readWholeFile(path, "Raft request journal");
                if (requestJournalBytes_ < 5 || bytes.size() < requestJournalBytes_ ||
                    std::string_view{reinterpret_cast<const char*>(bytes.data()), 5} != "AKRQ1") {
                    throw std::runtime_error("Raft request journal: invalid header or referenced size");
                }
                size_t cursor = 5;
                uint64_t records = 0;
                while (cursor < requestJournalBytes_) {
                    if (requestJournalBytes_ - cursor < 8) { throw std::runtime_error("Raft request journal: truncated record header"); }
                    const uint32_t len = readU32(bytes, cursor);
                    const uint32_t crc = readU32(bytes, cursor + 4);
                    cursor += 8;
                    if (len == 0 || len > requestJournalBytes_ - cursor) { throw std::runtime_error("Raft request journal: truncated record"); }
                    const auto payload = std::span<const uint8_t>{bytes.data() + cursor, len};
                    cursor += len;
                    if (crcBytes(payload) != crc) { throw std::runtime_error("Raft request journal: record CRC mismatch"); }
                    size_t payloadCursor = 1;
                    if (payload[0] == 0) {
                        if (!readRequestState(payload, payloadCursor, requestState_) || payloadCursor != payload.size()) {
                            throw std::runtime_error("Raft request journal: invalid checkpoint");
                        }
                    }
                    else if (payload[0] == 1) {
                        if (payload.size() < 13) { throw std::runtime_error("Raft request journal: invalid delta"); }
                        const uint64_t floor = readU64(payload, 1);
                        const uint32_t count = readU32(payload, 9);
                        payloadCursor = 13;
                        if (floor < requestState_.expiryFloor || count > MAX_REQUEST_RECORDS || count > (payload.size() - payloadCursor) / 72) {
                            throw std::runtime_error("Raft request journal: invalid delta bounds");
                        }
                        requestState_.expiryFloor = floor;
                        std::erase_if(requestState_.results, [&](const auto& item) { return item.first.expiresAtUnixMs <= floor; });
                        for (uint32_t i = 0; i < count; ++i) {
                            ClusterRequestId id;
                            RequestRecord record;
                            if (!readRequestId(payload, payloadCursor, id) || payload.size() - payloadCursor < 48) {
                                throw std::runtime_error("Raft request journal: truncated delta result");
                            }
                            std::copy_n(payload.data() + payloadCursor, 32, record.fingerprint.begin());
                            record.sequence = readU64(payload, payloadCursor + 32);
                            record.index = readU64(payload, payloadCursor + 40);
                            payloadCursor += 48;
                            if (id.expiresAtUnixMs <= floor || record.sequence == 0 || record.index == 0 ||
                                !requestState_.results.emplace(id, record).second) {
                                throw std::runtime_error("Raft request journal: invalid delta result");
                            }
                        }
                        if (payloadCursor != payload.size()) { throw std::runtime_error("Raft request journal: trailing delta bytes"); }
                    }
                    else { throw std::runtime_error("Raft request journal: unknown record type"); }
                    ++records;
                }
                if (cursor != requestJournalBytes_ || records != requestJournalRecords_) {
                    throw std::runtime_error("Raft request journal: record count mismatch");
                }
                for (const auto& [id, record] : requestState_.results) {
                    (void)id;
                    if (record.index > commitIndex_) { throw std::runtime_error("Raft request journal: result is ahead of commit index"); }
                    lastApplied_ = std::max(lastApplied_, record.index);
                }
                journalState_ = requestState_;
                if (bytes.size() != requestJournalBytes_) { std::filesystem::resize_file(path, requestJournalBytes_); }
            }

            void collectUnusedRequestJournals() const noexcept {
                try {
                    const auto parent = requestJournalBasePath_.parent_path().empty() ? std::filesystem::path{"."} : requestJournalBasePath_.parent_path();
                    const auto prefix = requestJournalBasePath_.filename().string() + ".";
                    if (!std::filesystem::exists(parent)) { return; }
                    for (const auto& file : std::filesystem::directory_iterator(parent)) {
                        const auto name = file.path().filename().string();
                        if (!file.is_regular_file() || !name.starts_with(prefix)) { continue; }
                        const auto generation = name.substr(prefix.size());
                        if (generation.empty() || generation.find_first_not_of("0123456789") != std::string::npos ||
                            file.path() == requestJournalPath(requestJournalGeneration_)) { continue; }
                        std::error_code ec;
                        std::filesystem::remove(file.path(), ec);
                    }
                }
                catch (...) { /* Unreferenced generations are harmless and can be collected after the next publish. */ }
            }

            static void syncLogSegment(const std::filesystem::path& path) {
#ifdef _WIN32
                const int fd = _wopen(path.c_str(), _O_RDWR | _O_BINARY);
                if (fd < 0) { throw std::runtime_error("Raft log: cannot open segment for sync"); }
                const int rc = _commit(fd);
                const int closeRc = _close(fd);
#else
                const int fd = ::open(path.c_str(), O_RDONLY);
                if (fd < 0) { throw std::runtime_error("Raft log: cannot open segment for sync"); }
                const int rc = ::fsync(fd);
                const int closeRc = ::close(fd);
#endif
                if (rc != 0 || closeRc != 0) { throw std::runtime_error("Raft log: segment sync failed"); }
            }

            void collectUnusedLogSegments() const noexcept {
                // The metadata is already durable. Unreferenced files, including
                // tails left by interrupted writes, are never part of recovery.
                try {
                    std::unordered_set<std::string> live;
                    for (const auto& entry : persistedLogLocations_) {
                        live.insert(logSegmentPath(entry.segment).filename().string());
                    }
                    if (!std::filesystem::exists(logSegmentDir())) { return; }
                    for (const auto& file : std::filesystem::directory_iterator(logSegmentDir())) {
                        const auto name = file.path().filename().string();
                        if (!file.is_regular_file() || !name.starts_with("segment-") || !name.ends_with(".akrl") || live.contains(name)) { continue; }
                        const auto number = name.substr(8, name.size() - 13);
                        if (number.empty() || number.find_first_not_of("0123456789") != std::string::npos) { continue; }
                        std::error_code ec;
                        std::filesystem::remove(file.path(), ec);
                    }
                }
                catch (...) { /* Cleanup is retryable; it cannot invalidate durable log state. */ }
            }

            void persistLog() {
                enforceLogInvariantsLocked("RaftConsensusRuntime log");
                std::vector<LogLocation> locations;
                locations.reserve(log_.size());
                size_t common = 0;
                if (!log_.empty()) {
                    auto previous = std::lower_bound(
                        persistedLogLocations_.begin(), persistedLogLocations_.end(), log_.front().index,
                        [](const LogLocation& location, uint64_t index) { return location.index < index; }
                    );
                    // Raft entries are immutable within an (index, term) pair.
                    while (common < log_.size() && previous != persistedLogLocations_.end() &&
                           previous->index == log_[common].index && previous->term == log_[common].term) {
                        locations.push_back(*previous++);
                        ++common;
                    }
                }

                try {
                    if (common < log_.size()) {
                        std::filesystem::create_directories(logSegmentDir());
                        uint64_t segment = 0;
                        uint64_t offset = 0;
                        if (!locations.empty() && !persistedLogLocations_.empty() &&
                            locations.back().index == persistedLogLocations_.back().index &&
                            std::filesystem::file_size(logSegmentPath(locations.back().segment)) == locations.back().end) {
                            segment = locations.back().segment;
                            offset = locations.back().end;
                        }
                        std::ofstream out;
                        const auto finishSegment = [&] {
                            if (!out.is_open()) { return; }
                            out.flush();
                            if (!out) { throw std::runtime_error("Raft log: segment write failed"); }
                            out.close();
                            if (out.fail()) { throw std::runtime_error("Raft log: segment close failed"); }
                            syncLogSegment(logSegmentPath(segment));
#ifndef _WIN32
                            syncParentDirectory(logSegmentPath(segment), "Raft log segment");
#endif
                        };
                        for (size_t i = common; i < log_.size(); ++i) {
                            const auto payload = encodeEntryPayload(log_[i]);
                            std::vector<uint8_t> record;
                            writeU64(record, payload.size());
                            writeU32(record, crcBytes(payload));
                            record.insert(record.end(), payload.begin(), payload.end());
                            if (segment == 0 || (offset != 0 && offset + record.size() > LOG_SEGMENT_BYTES)) {
                                finishSegment();
                                do {
                                    if (nextLogSegment_ == UINT64_MAX) { throw std::runtime_error("Raft log: segment ids exhausted"); }
                                    segment = nextLogSegment_++;
                                } while (std::filesystem::exists(logSegmentPath(segment)));
                                offset = 0;
                            }
                            if (!out.is_open()) {
                                out.open(logSegmentPath(segment), std::ios::binary | std::ios::app);
                                if (!out) { throw std::runtime_error("Raft log: cannot append segment"); }
                            }
                            out.write(reinterpret_cast<const char*>(record.data()), static_cast<std::streamsize>(record.size()));
                            locations.push_back(LogLocation{log_[i].index, log_[i].term, segment, offset, offset + record.size()});
                            offset += record.size();
                        }
                        finishSegment();
#ifndef _WIN32
                        syncParentDirectory(logSegmentDir(), "Raft log segment directory");
#endif
                        crashAtLogTestPoint("raft.after_segment_sync");
                    }

                    std::vector<LogExtent> extents;
                    for (const auto& entry : locations) {
                        if (!extents.empty() && extents.back().segment == entry.segment && extents.back().end == entry.begin) {
                            extents.back().end = entry.end;
                        }
                        else { extents.push_back(LogExtent{entry.segment, entry.begin, entry.end}); }
                    }
                    prepareRequestJournalLocked();
                    std::vector<uint8_t> bytes{'A', 'K', 'R', 'L', '1'};
                    writeU64(bytes, commitIndex_);
                    writeU64(bytes, lastApplied_);
                    writeU64(bytes, lastIncludedIndex_);
                    writeU64(bytes, lastIncludedTerm_);
                    writeU64(bytes, lastIncludedStateMachineSeq_);
                    writeU64(bytes, static_cast<uint64_t>(log_.size()));
                    writeNodeSetField(bytes, committedVoters_);
                    writeOptionalNodeSetField(bytes, jointOldVoters_);
                    writeOptionalNodeSetField(bytes, jointNewVoters_);
                    writeSnapshotInstallTransactionField(bytes);
                    writeU64(bytes, requestJournalGeneration_);
                    writeU64(bytes, requestJournalBytes_);
                    writeU64(bytes, requestJournalRecords_);
                    writeU64(bytes, extents.size());
                    for (const auto& extent : extents) {
                        writeU64(bytes, extent.segment);
                        writeU64(bytes, extent.begin);
                        writeU64(bytes, extent.end);
                    }
                    writeU32(bytes, crcBytes(bytes));
                    // Publish only after every referenced segment has reached disk.
                    writeFileAtomically(logPath_, bytes, "RaftConsensusRuntime log metadata");
                    crashAtLogTestPoint("raft.after_log_metadata");
                    persistedLogLocations_ = std::move(locations);
                }
                catch (...) {
                    // An atomic replacement can have succeeded before a sync error.
                    // Never continue appending against an uncertain durable boundary.
                    logIoFailed_ = true;
                    recordFailure(ClusterFailureCode::RAFT_PERSISTENCE, ClusterHealthState::FAILED);
                    stopRuntimeLocked();
                    throw;
                }
                collectUnusedLogSegments();
                collectUnusedRequestJournals();
            }

            void recoverLog() {
                if (!std::filesystem::exists(logPath_)) { return; }
                const auto bytes = readWholeFile(logPath_, "RaftConsensusRuntime log metadata");
                constexpr size_t fixedHeaderSize = 5 + 8 * 6;
                if (bytes.size() < fixedHeaderSize + 12) { throw std::runtime_error("RaftConsensusRuntime: truncated log metadata"); }
                if (std::string_view{reinterpret_cast<const char*>(bytes.data()), 5} != "AKRL1" ||
                    readU32(bytes, bytes.size() - 4) != crcBytes(std::span<const uint8_t>{bytes.data(), bytes.size() - 4})) {
                    throw std::runtime_error("RaftConsensusRuntime: log metadata CRC or magic mismatch");
                }
                commitIndex_ = readU64(bytes, 5);
                lastApplied_ = readU64(bytes, 13);
                lastIncludedIndex_ = readU64(bytes, 21);
                lastIncludedTerm_ = readU64(bytes, 29);
                lastIncludedStateMachineSeq_ = readU64(bytes, 37);
                const uint64_t count = readU64(bytes, 45);
                size_t cursor = fixedHeaderSize;
                if (!readNodeSetField(bytes, cursor, committedVoters_) || !readOptionalNodeSetField(bytes, cursor, jointOldVoters_) ||
                    !readOptionalNodeSetField(bytes, cursor, jointNewVoters_) ||
                    !readSnapshotInstallTransactionField(bytes, cursor, durableSnapshotInstall_) ||
                    committedVoters_.empty() || cursor + 32 > bytes.size() - 4) {
                    throw std::runtime_error("RaftConsensusRuntime: corrupt log membership metadata");
                }
                requestJournalGeneration_ = readU64(bytes, cursor);
                requestJournalBytes_ = readU64(bytes, cursor + 8);
                requestJournalRecords_ = readU64(bytes, cursor + 16);
                cursor += 24;
                const uint64_t extentCount = readU64(bytes, cursor);
                cursor += 8;
                if (extentCount > count || extentCount > (bytes.size() - 4 - cursor) / 24 ||
                    extentCount * 24 != bytes.size() - 4 - cursor) {
                    throw std::runtime_error("RaftConsensusRuntime: invalid log segment metadata");
                }

                uint64_t lastGoodIndex = lastIncludedIndex_;
                uint64_t recoveredStateMachineSeq = lastIncludedStateMachineSeq_;
                bool recoveredSeqHasMutation = false;
                bool truncatedTail = false;
                const auto failOrTruncate = [&](const char* message) {
                    if (runtimeOptions_.raftLogRecoveryAction == RaftLogRecoveryAction::TRUNCATE_UNCOMMITTED_TAIL &&
                        commitIndex_ <= lastGoodIndex) {
                        truncatedTail = true;
                        return;
                    }
                    throw std::runtime_error(std::string{"RaftConsensusRuntime: "} + message);
                };
                for (uint64_t e = 0; e < extentCount && !truncatedTail; ++e) {
                    const LogExtent extent{readU64(bytes, cursor), readU64(bytes, cursor + 8), readU64(bytes, cursor + 16)};
                    cursor += 24;
                    if (extent.segment == 0 || extent.begin >= extent.end) {
                        throw std::runtime_error("RaftConsensusRuntime: invalid segment extent");
                    }
                    if (!std::filesystem::exists(logSegmentPath(extent.segment))) {
                        failOrTruncate("missing log segment");
                        break;
                    }
                    const auto segmentBytes = readWholeFile(logSegmentPath(extent.segment), "Raft log segment");
                    uint64_t offset = extent.begin;
                    while (offset < extent.end && !truncatedTail) {
                        const uint64_t begin = offset;
                        if (offset > segmentBytes.size() || segmentBytes.size() - offset < 12 || extent.end - offset < 12) {
                            failOrTruncate("truncated log entry header");
                            break;
                        }
                        const uint64_t len = readU64(segmentBytes, static_cast<size_t>(offset));
                        const uint32_t crc = readU32(segmentBytes, static_cast<size_t>(offset + 8));
                        offset += 12;
                        if (len > ReplFrameHeader::MAX_PAYLOAD_SIZE || len > segmentBytes.size() - offset || len > extent.end - offset) {
                            failOrTruncate("truncated log entry payload");
                            break;
                        }
                        const auto payload = std::span<const uint8_t>{segmentBytes.data() + offset, static_cast<size_t>(len)};
                        offset += len;
                        size_t payloadCursor = 0;
                        RaftLogEntry entry;
                        if (crc != crcBytes(payload) || !decodeEntryPayload(payload, payloadCursor, entry) || payloadCursor != payload.size() ||
                            entry.index != lastGoodIndex + 1 || !isValidRaftLogEntryShape(entry) ||
                            (isStateMachineEntry(entry.kind) && entry.clientSeq <= lastIncludedStateMachineSeq_) ||
                            !advanceStateMachineSequenceTracker(entry, recoveredStateMachineSeq, recoveredSeqHasMutation)) {
                            failOrTruncate("corrupt log entry");
                            break;
                        }
                        lastGoodIndex = entry.index;
                        persistedLogLocations_.push_back(LogLocation{entry.index, entry.term, extent.segment, begin, offset});
                        log_.push_back(std::move(entry));
                    }
                }
                if (!truncatedTail && log_.size() != count) { failOrTruncate("log entry count mismatch"); }
                if (commitIndex_ > lastGoodIndex || lastIncludedIndex_ > commitIndex_ || lastApplied_ > commitIndex_) {
                    throw std::runtime_error("RaftConsensusRuntime: committed log entry is missing or corrupt");
                }
                if (durableSnapshotInstall_ && durableSnapshotInstall_->lastIncludedIndex < lastIncludedIndex_) {
                    throw std::runtime_error("RaftConsensusRuntime: snapshot install transaction does not match log metadata");
                }
                if (lastApplied_ < lastIncludedIndex_) { lastApplied_ = lastIncludedIndex_; }
                recoverRequestJournalLocked();
                if (truncatedTail) { persistLog(); }
                collectUnusedLogSegments();
                collectUnusedRequestJournals();
            }

            void recoverAppliedProgressFromStateMachine() {
                if (!callbacks_.getLastSeq) { return; }
                const uint64_t durableClientSeq = callbacks_.getLastSeq();
                std::lock_guard lock{mutex_};
                const auto recoveredAppliedIndex = appliedLogIndexForClientSeq(durableClientSeq);
                if (recoveredAppliedIndex && *recoveredAppliedIndex > lastApplied_) {
                    const auto backup = captureDurableLogStateLocked();
                    lastApplied_ = std::min(*recoveredAppliedIndex, commitIndex_);
                    try { persistLog(); }
                    catch (...) {
                        restoreDurableLogStateLocked(backup);
                        throw;
                    }
                }
            }

            void recoverDurableSnapshotInstall() {
                std::optional<SnapshotInstallTransaction> transaction;
                {
                    std::lock_guard lock{mutex_};
                    transaction = durableSnapshotInstall_;
                }
                if (!transaction) { return; }

                std::lock_guard applyLock{applyMutex_};
                if (callbacks_.recoverSnapshot) { callbacks_.recoverSnapshot(transaction->snapshotSeq); }
                else if (callbacks_.finishSnapshot) { callbacks_.finishSnapshot(transaction->snapshotSeq); }
                if (callbacks_.forceDurable) { callbacks_.forceDurable(); }

                std::lock_guard lock{mutex_};
                if (durableSnapshotInstall_ && durableSnapshotInstall_->snapshotSeq == transaction->snapshotSeq && durableSnapshotInstall_->lastIncludedIndex ==
                    transaction->lastIncludedIndex && durableSnapshotInstall_->lastIncludedTerm == transaction->lastIncludedTerm) {
                    const auto backup = captureDurableLogStateLocked();
                    try {
                        if (!finalizeSnapshotInstallMetadataLocked(*transaction)) {
                            throw std::runtime_error("RaftConsensusRuntime: failed to recover snapshot install membership");
                        }
                        durableSnapshotInstall_.reset();
                        persistLog();
                    }
                    catch (...) {
                        restoreDurableLogStateLocked(backup);
                        throw;
                    }
                }
            }

            std::optional<NodeRole> setRole(RaftRole role) {
                const auto old = role_.exchange(role);
                if (old == role) { return std::nullopt; }
                peerReplicationCv_.notify_all();
                return role == RaftRole::LEADER ? NodeRole::PRIMARY : NodeRole::REPLICA;
            }

            void notifyRoleChange(std::optional<NodeRole> role) { if (role && callbacks_.roleChange) { callbacks_.roleChange(*role); } }

            void setRoleAndNotify(RaftRole role) { notifyRoleChange(setRole(role)); }

            void becomeFollower(uint64_t term) {
                bool changed = false;
                {
                    std::lock_guard lock{mutex_};
                    if (term > currentTerm_) {
                        const auto hardBackup = captureDurableHardStateLocked();
                        currentTerm_ = term;
                        votedFor_ = 0;
                        clearObservedLeaderLocked();
                        try { persistState(); }
                        catch (...) {
                            restoreDurableHardStateLocked(hardBackup);
                            stopRuntimeLocked();
                            throw;
                        }
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
                    const auto role = role_.load();
                    const bool directSingleNodeElection =
                        role == RaftRole::FOLLOWER && peers_.empty() && term >= currentTerm_;
                    if (!running_ || term < currentTerm_ || (!directSingleNodeElection && (currentTerm_ != term || role != RaftRole::CANDIDATE)) ||
                        !self_->coordinatorEligible() || !isVotingMemberLocked(selfNodeId_)) {
                        return;
                    }
                    const auto hardBackup = captureDurableHardStateLocked();
                    const auto logBackup = captureDurableLogStateLocked();
                    currentTerm_ = std::max(currentTerm_, term);
                    votedFor_ = selfNodeId_;
                    observeLeaderLocked(currentTerm_, selfNodeId_);
                    bool hardStateDurable = false;
                    try {
                        persistState();
                        hardStateDurable = true;
                        resetLeaderReplicationState();
                        log_.push_back(makeNoopEntryLocked());
                        persistLog();
                    }
                    catch (...) {
                        if (!hardStateDurable) { restoreDurableHardStateLocked(hardBackup); }
                        restoreDurableLogStateLocked(logBackup);
                        stopRuntimeLocked();
                        throw;
                    }
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
                    peerReplication_.push_back(PeerReplicationState{.node = peer, .nextIndex = next, .matchIndex = 0, .inFlight = false});
                }
            }

            PeerReplicationState* peerState(uint64_t nodeId) {
                for (auto& state : peerReplication_) { if (state.node.nodeId == nodeId) { return &state; } }
                return nullptr;
            }

            DurableLogState captureDurableLogStateLocked() const {
                return DurableLogState{
                    .requestState = requestState_,
                    .journalState = journalState_,
                    .requestJournalGeneration = requestJournalGeneration_,
                    .requestJournalBytes = requestJournalBytes_,
                    .requestJournalRecords = requestJournalRecords_,
                    .commitIndex = commitIndex_,
                    .lastApplied = lastApplied_,
                    .lastIncludedIndex = lastIncludedIndex_,
                    .lastIncludedTerm = lastIncludedTerm_,
                    .lastIncludedStateMachineSeq = lastIncludedStateMachineSeq_,
                    .log = log_,
                    .committedVoters = committedVoters_,
                    .jointOldVoters = jointOldVoters_,
                    .jointNewVoters = jointNewVoters_,
                    .peers = peers_,
                    .peerReplication = peerReplication_,
                    .durableSnapshotInstall = durableSnapshotInstall_
                };
            }

            void restoreDurableLogStateLocked(DurableLogState state) {
                requestState_ = std::move(state.requestState);
                journalState_ = std::move(state.journalState);
                requestJournalGeneration_ = state.requestJournalGeneration;
                requestJournalBytes_ = state.requestJournalBytes;
                requestJournalRecords_ = state.requestJournalRecords;
                commitIndex_ = state.commitIndex;
                lastApplied_ = state.lastApplied;
                lastIncludedIndex_ = state.lastIncludedIndex;
                lastIncludedTerm_ = state.lastIncludedTerm;
                lastIncludedStateMachineSeq_ = state.lastIncludedStateMachineSeq;
                log_ = std::move(state.log);
                committedVoters_ = std::move(state.committedVoters);
                jointOldVoters_ = std::move(state.jointOldVoters);
                jointNewVoters_ = std::move(state.jointNewVoters);
                peers_ = std::move(state.peers);
                peerReplication_ = std::move(state.peerReplication);
                durableSnapshotInstall_ = std::move(state.durableSnapshotInstall);
                peerReplicationCv_.notify_all();
            }

            DurableHardState captureDurableHardStateLocked() const noexcept { return DurableHardState{.currentTerm = currentTerm_, .votedFor = votedFor_}; }

            void restoreDurableHardStateLocked(DurableHardState state) noexcept {
                currentTerm_ = state.currentTerm;
                votedFor_ = state.votedFor;
            }

            void stopRuntimeLocked() noexcept {
                running_ = false;
                forceElection_ = false;
                role_.store(RaftRole::FOLLOWER);
                cv_.notify_all();
                peerReplicationCv_.notify_all();
            }

            void observeLeaderLocked(uint64_t term, uint64_t leaderId) noexcept {
                if (term >= observedLeaderTerm_) {
                    observedLeaderTerm_ = term;
                    observedLeaderId_ = leaderId;
                }
            }

            void clearObservedLeaderLocked() noexcept {
                observedLeaderTerm_ = 0;
                observedLeaderId_ = 0;
            }

            bool observedLeaderLocked(uint64_t term, uint64_t leaderId) const noexcept {
                return observedLeaderTerm_ == term && observedLeaderId_ == leaderId;
            }

            bool claimPeerReplication(uint64_t peerId) {
                std::unique_lock lock{mutex_};
                peerReplicationCv_.wait(
                    lock,
                    [&] {
                        const auto* state = peerState(peerId);
                        return !running_ || role_.load() != RaftRole::LEADER || state == nullptr || !state->inFlight;
                    }
                );
                if (!running_ || role_.load() != RaftRole::LEADER) { return false; }
                auto* state = peerState(peerId);
                if (state == nullptr) { return false; }
                state->inFlight = true;
                return true;
            }

            void releasePeerReplication(uint64_t peerId) noexcept {
                {
                    std::lock_guard lock{mutex_};
                    if (auto* state = peerState(peerId); state != nullptr) { state->inFlight = false; }
                }
                peerReplicationCv_.notify_all();
            }

            void timerLoop() {
                try {
                    for (;;) {
                        retirePeerWorkers();
                        std::unique_lock lock{mutex_};
                        if (!running_) { return; }
                        if (role_.load() == RaftRole::LEADER) {
                            lock.unlock();
                            sendHeartbeats();
                            maybeCompactLog();
                            lock.lock();
                            cv_.wait_for(lock, std::chrono::milliseconds{100}, [&] { return !running_; });
                            continue;
                        }
                        if (!forceElection_ && Clock::now() < electionDeadline_) {
                            cv_.wait_until(lock, std::min(electionDeadline_, Clock::now() + std::chrono::milliseconds{100}));
                            continue;
                        }
                        lock.unlock();
                        startElection();
                    }
                }
                catch (...) {
                    std::lock_guard lock{mutex_};
                    stopRuntimeLocked();
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
                        forceElection_ = false;
                        electionDeadline_ = nextElectionDeadline();
                        return;
                    }
                    forceElection_ = false;
                    const auto hardBackup = captureDurableHardStateLocked();
                    role_.store(RaftRole::CANDIDATE);
                    peerReplicationCv_.notify_all();
                    ++currentTerm_;
                    term = currentTerm_;
                    votedFor_ = selfNodeId_;
                    clearObservedLeaderLocked();
                    lastIndex = lastLogIndex();
                    lastTerm = lastLogTerm();
                    electionDeadline_ = nextElectionDeadline();
                    refreshPeersFromMembershipLocked();
                    electionPeers = peers_;
                    try { persistState(); }
                    catch (...) {
                        restoreDurableHardStateLocked(hardBackup);
                        role_.store(RaftRole::FOLLOWER);
                        stopRuntimeLocked();
                        throw;
                    }
                }

                struct Votes { std::mutex mutex; std::vector<uint64_t> granted; };
                auto votes = std::make_shared<Votes>();
                votes->granted.push_back(selfNodeId_);
                std::vector<std::future<void>> workers;
                for (const auto& peer : electionPeers) {
                    workers.push_back(schedulePeerTask(peer.nodeId,
                        [this, peer, term, lastIndex, lastTerm, votes] {
                            RequestVoteResponse response;
                            if (requestVote(peer, RequestVote{term, selfNodeId_, lastIndex, lastTerm}, response)) {
                                if (response.term > term) {
                                    becomeFollower(response.term);
                                    return;
                                }
                                if (response.voteGranted) {
                                    std::lock_guard lock{votes->mutex};
                                    votes->granted.push_back(peer.nodeId);
                                }
                            }
                        }
                    ));
                }
                for (auto& worker : workers) { worker.wait(); }
                for (auto& worker : workers) { worker.get(); }

                bool won = false;
                {
                    std::lock_guard lock{mutex_};
                    std::lock_guard votesLock{votes->mutex};
                    won = running_ && currentTerm_ == term && role_.load() == RaftRole::CANDIDATE && hasElectionQuorumLocked(votes->granted);
                }
                if (won) { becomeLeader(term); }
            }

            bool requestVote(const NodeInfo& peer, const RequestVote& request, RequestVoteResponse& response) {
                DecodedFrame frame;
                return exchangeRpc(peer, encodeRequestVote(request), ReplMsgType::RAFT_REQUEST_VOTE_RESPONSE, frame, rpcTimeoutMs(500)) &&
                    decodeRequestVoteResponse(frame.payload, response);
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

            bool installSnapshotStream(const NodeInfo& peer, uint64_t term, InstallSnapshotResponse& response) {
                if (!callbacks_.exportSnapshot) { return false; }
                std::optional<ClusterSnapshot> snapshot;
                RequestState snapshotRequests;
                uint64_t logIndexValue = 0;
                uint64_t logTermValue = 0;
                std::vector<NodeInfo> committedVoters;
                std::optional<std::vector<NodeInfo>> jointOldVoters;
                std::optional<std::vector<NodeInfo>> jointNewVoters;
                {
                    std::lock_guard applyLock{applyMutex_};
                    snapshot = callbacks_.exportSnapshot();
                    if (!snapshot || snapshot->seq == 0 || !snapshot->forEachEntry) { return false; }
                    std::lock_guard lock{mutex_};
                    auto logIndex = compactableLogIndexForSnapshotSeq(snapshot->seq);
                    if (!logIndex && lastIncludedIndex_ > 0) { logIndex = lastIncludedIndex_; }
                    if (!logIndex || *logIndex > commitIndex_) { return false; }
                    logIndexValue = *logIndex;
                    snapshotRequests = requestState_;
                    std::erase_if(snapshotRequests.results, [&](const auto& item) { return item.second.index > logIndexValue; });
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
                    request.entryCount = snapshot->entryCount;
                    request.committedVoters = committedVoters;
                    request.jointOldVoters = jointOldVoters;
                    request.jointNewVoters = jointNewVoters;
                    return request;
                };

                InstallSnapshot current = makeRequest();
                const auto sendRequest = [&](const InstallSnapshot& request) {
                    return installSnapshot(peer, request, response) && response.term <= term && response.success;
                };
                const size_t chunkSize = runtimeOptions_.raftBlobChunkSizeBytes;
                uint64_t entryIndex = 0;
                const bool streamed = snapshot->forEachEntry([&](std::span<const uint8_t> key, std::span<const uint8_t> value) {
                    if (entryIndex >= snapshot->entryCount) { return false; }
                    const uint32_t valueCrc32c = crcBytes(value);
                    size_t offset = 0;
                    bool emittedEntryChunk = false;
                    do {
                        const size_t remaining = value.size() - offset;
                        size_t currentChunkSize = std::min(chunkSize, remaining);
                        auto makeChunk = [&](size_t valueBytes) {
                            SnapshotKvChunk chunk;
                            chunk.entryIndex = entryIndex;
                            chunk.valueOffset = offset;
                            chunk.valueSize = value.size();
                            chunk.valueCrc32c = valueCrc32c;
                            if (offset == 0) { chunk.key.assign(key.begin(), key.end()); }
                            chunk.value.assign(
                                value.begin() + static_cast<std::ptrdiff_t>(offset),
                                value.begin() + static_cast<std::ptrdiff_t>(offset + valueBytes)
                            );
                            return chunk;
                        };
                        SnapshotKvChunk chunk = makeChunk(currentChunkSize);
                        InstallSnapshot candidate = current;
                        candidate.chunks.push_back(chunk);
                        if (encodeInstallSnapshot(candidate).empty() && !current.chunks.empty()) {
                            if (!sendRequest(current)) { return false; }
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
                                if (encodeInstallSnapshot(candidate).empty()) { high = mid - 1; }
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
                    while (offset < value.size() || !emittedEntryChunk);
                    ++entryIndex;
                    return true;
                });
                if (!streamed || entryIndex != snapshot->entryCount) { return false; }

                current.requestState = std::move(snapshotRequests);
                current.done = true;
                if (encodeInstallSnapshot(current).empty()) {
                    auto final = makeRequest();
                    final.requestState = std::move(current.requestState);
                    final.done = true;
                    current.done = false;
                    if (!current.chunks.empty() && !sendRequest(current)) { return false; }
                    if (encodeInstallSnapshot(final).empty()) { return false; }
                    return sendRequest(final);
                }
                return sendRequest(current);
            }

            bool committedLogBytesAtLeastLocked(uint64_t threshold) const {
                uint64_t bytes = 0;
                for (const auto& entry : log_) {
                    if (entry.index > commitIndex_) { break; }
                    const uint64_t entryBytes = sizeof(RaftLogEntry) + static_cast<uint64_t>(entry.key.size()) +
                        static_cast<uint64_t>(entry.value.size());
                    if (entryBytes >= threshold - bytes) { return true; }
                    bytes += entryBytes;
                }
                return false;
            }

            bool shouldCompactLogLocked(Clock::time_point now) const {
                if (commitIndex_ <= lastIncludedIndex_) { return false; }
                if (commitIndex_ - lastIncludedIndex_ >= runtimeOptions_.raftSnapshot.minLogEntries) { return true; }
                if (now >= nextCompactionDeadline_) { return true; }
                return committedLogBytesAtLeastLocked(runtimeOptions_.raftSnapshot.minLogBytes);
            }

            void maybeCompactLog(bool force = false) {
                if (!callbacks_.exportSnapshot) { return; }
                {
                    std::lock_guard lock{mutex_};
                    if (!running_ || role_.load() != RaftRole::LEADER) { return; }
                    const auto now = Clock::now();
                    if (!force && now < nextCompactionCheck_) { return; }
                    nextCompactionCheck_ = now + std::chrono::seconds{1};
                    if (!force && !shouldCompactLogLocked(now)) { return; }
                }
                std::lock_guard applyLock{applyMutex_};
                {
                    std::lock_guard lock{mutex_};
                    if (!running_ || role_.load() != RaftRole::LEADER) { return; }
                    if (!force && !shouldCompactLogLocked(Clock::now())) { return; }
                }
                const auto snapshot = callbacks_.exportSnapshot();
                if (!snapshot || snapshot->seq == 0) { return; }
                std::lock_guard lock{mutex_};
                if (!running_ || role_.load() != RaftRole::LEADER) { return; }
                const auto logIndex = compactableLogIndexForSnapshotSeq(snapshot->seq);
                if (!logIndex || *logIndex <= lastIncludedIndex_ || *logIndex > commitIndex_) { return; }
                const uint64_t includedTerm = termAt(*logIndex);
                if (includedTerm == 0) { return; }
                const auto backup = captureDurableLogStateLocked();
                compactLogThrough(*logIndex, includedTerm, snapshot->seq);
                try { persistLog(); }
                catch (...) {
                    restoreDurableLogStateLocked(backup);
                    throw;
                }
                nextCompactionDeadline_ = Clock::now() + std::chrono::milliseconds{runtimeOptions_.raftSnapshot.maxIntervalMs};
            }

            void compactLogThrough(uint64_t index, uint64_t term, uint64_t stateMachineSeq) {
                if (index <= lastIncludedIndex_) { return; }
                log_.erase(std::ranges::remove_if(log_, [&](const RaftLogEntry& entry) { return entry.index <= index; }).begin(), log_.end());
                lastIncludedIndex_ = index;
                lastIncludedTerm_ = term;
                lastIncludedStateMachineSeq_ = std::max(lastIncludedStateMachineSeq_, stateMachineSeq);
                if (commitIndex_ < lastIncludedIndex_) { commitIndex_ = lastIncludedIndex_; }
                if (lastApplied_ < lastIncludedIndex_) { lastApplied_ = lastIncludedIndex_; }
            }

            void proposeMembershipChange(const std::vector<NodeInfo>& oldVoters, const std::vector<NodeInfo>& newVoters) {
                RaftLogEntry joint = makeConfigEntry(RaftEntryKind::CONFIG_JOINT, oldVoters, newVoters);
                appendReplicateAndCommitConfig(joint);
                RaftLogEntry final = makeConfigEntry(RaftEntryKind::CONFIG_FINAL, oldVoters, newVoters);
                appendReplicateAndCommitConfig(final);
            }

            RaftLogEntry makeConfigEntry(RaftEntryKind kind, const std::vector<NodeInfo>& oldVoters, const std::vector<NodeInfo>& newVoters) {
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
                if (!isValidRaftLogEntryShape(entry)) { throw std::runtime_error("RaftConsensusRuntime: invalid membership log entry"); }
                return entry;
            }

            void appendReplicateAndCommitConfig(const RaftLogEntry& entry, bool appendEntry = true) {
                uint64_t term = 0;
                {
                    std::lock_guard lock{mutex_};
                    if (role_.load() != RaftRole::LEADER) { throw std::runtime_error("RaftConsensusRuntime: local node is not Raft leader"); }
                    term = currentTerm_;
                    if (!isValidRaftLogEntryShape(entry)) { throw std::runtime_error("RaftConsensusRuntime: invalid membership log entry"); }
                    const auto backup = captureDurableLogStateLocked();
                    try {
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
                                    peerReplication_.push_back(PeerReplicationState{.node = node, .nextIndex = 1, .matchIndex = 0, .inFlight = false});
                                }
                            }
                            peers_.clear();
                            for (const auto& node : targets) { if (node.nodeId != selfNodeId_) { peers_.push_back(node); } }
                        }
                        persistLog();
                    }
                    catch (...) {
                        restoreDurableLogStateLocked(backup);
                        throw;
                    }
                }
                if (!replicateEntryToMajority(entry, std::chrono::milliseconds{20000})) {
                    throw std::runtime_error("RaftConsensusRuntime: failed to replicate membership change to Raft quorum");
                }
                {
                    std::lock_guard lock{mutex_};
                    if (role_.load() != RaftRole::LEADER || currentTerm_ != term) {
                        throw std::runtime_error("RaftConsensusRuntime: leadership changed before membership commit");
                    }
                    const auto backup = captureDurableLogStateLocked();
                    try {
                        commitIndex_ = std::max(commitIndex_, entry.index);
                        persistLog();
                    }
                    catch (...) {
                        restoreDurableLogStateLocked(backup);
                        throw;
                    }
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
                    if (!decodeNodeSet(entry.value, newVoters)) { throw std::runtime_error("RaftConsensusRuntime: corrupt final membership entry"); }
                    committedVoters_ = sortedUniqueVoters(std::move(newVoters));
                    jointOldVoters_.reset();
                    jointNewVoters_.reset();
                }
                refreshPeersFromMembershipLocked();
                ensurePeerReplicationTargetsLocked();
            }

            bool applySnapshotMembershipLocked(const InstallSnapshot& request) {
                if (request.committedVoters.empty() || request.jointOldVoters.has_value() != request.jointNewVoters.has_value()) { return false; }
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

            bool applySnapshotMembershipLocked(const SnapshotInstallTransaction& transaction) {
                if (transaction.committedVoters.empty() || transaction.jointOldVoters.has_value() != transaction.jointNewVoters.has_value()) { return false; }
                committedVoters_ = sortedUniqueVoters(transaction.committedVoters);
                if (transaction.jointOldVoters && transaction.jointNewVoters) {
                    jointOldVoters_ = sortedUniqueVoters(*transaction.jointOldVoters);
                    jointNewVoters_ = sortedUniqueVoters(*transaction.jointNewVoters);
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

            bool finalizeSnapshotInstallMetadataLocked(const SnapshotInstallTransaction& transaction) {
                if (!applySnapshotMembershipLocked(transaction)) { return false; }
                for (const auto& [id, record] : transaction.requestState.results) {
                    if (record.index > transaction.lastIncludedIndex) { return false; }
                }
                const auto floor = std::max(requestState_.expiryFloor, transaction.requestState.expiryFloor);
                requestState_ = transaction.requestState;
                expireRequestsLocked(floor);
                compactLogThrough(transaction.lastIncludedIndex, transaction.lastIncludedTerm, transaction.snapshotSeq);
                return true;
            }

            void replayCommittedMembership() {
                for (const auto& entry : log_) {
                    if (entry.index > commitIndex_) { break; }
                    if (entry.kind == RaftEntryKind::CONFIG_JOINT || entry.kind == RaftEntryKind::CONFIG_FINAL) { applyConfigEntryLocked(entry); }
                }
            }

            bool replicateEntryToMajority(const RaftLogEntry& entry, std::chrono::milliseconds retryWindow = std::chrono::milliseconds{0}) {
                const auto deadline = Clock::now() + (retryWindow.count() == 0 ? std::chrono::milliseconds{config_.consistency().ackTimeoutMs} : retryWindow);
                const auto entryTargets = replicationTargetsForEntry(entry);
                do {
                    std::vector<NodeInfo> targets;
                    {
                        std::lock_guard lock{mutex_};
                        if (!running_ || role_.load() != RaftRole::LEADER || currentTerm_ != entry.term) { return false; }
                        if (peerReplication_.empty()) { resetLeaderReplicationState(); }
                        ensurePeerReplicationTargetsLocked(entryTargets ? &*entryTargets : nullptr);
                        if (hasCommitQuorumLocked(entry.index, &entry)) { return true; }
                        targets = peers_;
                    }
                    for (const auto& peer : targets) { scheduleReplication(peer, entry.index, false); }
                    std::unique_lock lock{mutex_};
                    peerReplicationCv_.wait_until(lock, std::min(deadline, Clock::now() + std::chrono::milliseconds{50}), [&] {
                        return !running_ || role_.load() != RaftRole::LEADER || currentTerm_ != entry.term || hasCommitQuorumLocked(entry.index, &entry);
                    });
                    if (!running_ || role_.load() != RaftRole::LEADER || currentTerm_ != entry.term) { return false; }
                    if (hasCommitQuorumLocked(entry.index, &entry)) { return true; }
                    if (Clock::now() >= deadline) { return false; }
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
                        const auto backup = captureDurableLogStateLocked();
                        try {
                            commitIndex_ = entry.index;
                            persistLog();
                        }
                        catch (...) {
                            restoreDurableLogStateLocked(backup);
                            throw;
                        }
                    }
                }
                applyCommitted();
                sendHeartbeats();
                return true;
            }

        public:
            void linearizableReadBarrier() {
                uint64_t term = 0, readIndex = 0, round = 0;
                std::vector<NodeInfo> targets, voters;
                std::optional<std::vector<NodeInfo>> oldVoters, newVoters;
                // A newly elected leader must commit its current-term NOOP once.
                bool established = false;
                {
                    std::lock_guard lock{mutex_};
                    established = commitIndex_ != 0 && termAt(commitIndex_) == currentTerm_;
                }
                if (!established && !commitOutstandingEntry(std::chrono::milliseconds{config_.consistency().ackTimeoutMs})) {
                    throw std::runtime_error("RaftConsensusRuntime: current-term commit required for ReadIndex");
                }
                {
                    std::lock_guard lock{mutex_};
                    if (!running_ || role_.load() != RaftRole::LEADER || termAt(commitIndex_) != currentTerm_) {
                        throw std::runtime_error("RaftConsensusRuntime: local node has no established leadership");
                    }
                    term = currentTerm_;
                    readIndex = commitIndex_;
                    if (nextReadRound_ == UINT64_MAX) { throw std::runtime_error("Raft read rounds exhausted"); }
                    round = ++nextReadRound_;
                    targets = peers_;
                    voters = committedVoters_;
                    oldVoters = jointOldVoters_;
                    newVoters = jointNewVoters_;
                }
                const auto deadline = Clock::now() + std::chrono::milliseconds{config_.consistency().ackTimeoutMs};
                for (;;) {
                    for (const auto& peer : targets) { scheduleReplication(peer, readIndex, true, round); }
                    std::unique_lock lock{mutex_};
                    const auto confirmed = [&] {
                        std::vector<uint64_t> acknowledgers{selfNodeId_};
                        for (const auto& [id, ack] : readAcks_) { if (ack.first == term && ack.second >= round) { acknowledgers.push_back(id); } }
                        return oldVoters && newVoters
                            ? votedByMajority(*oldVoters, acknowledgers) && votedByMajority(*newVoters, acknowledgers)
                            : votedByMajority(voters, acknowledgers);
                    };
                    peerReplicationCv_.wait_until(lock, std::min(deadline, Clock::now() + std::chrono::milliseconds{50}), [&] {
                        return !running_ || currentTerm_ != term || role_.load() != RaftRole::LEADER || confirmed();
                    });
                    if (!running_ || currentTerm_ != term || role_.load() != RaftRole::LEADER ||
                        !sameNodeSet(voters, committedVoters_) || !sameOptionalNodeSet(oldVoters, jointOldVoters_) ||
                        !sameOptionalNodeSet(newVoters, jointNewVoters_)) {
                        throw std::runtime_error("RaftConsensusRuntime: leadership or membership changed during ReadIndex");
                    }
                    if (confirmed()) { break; }
                    if (Clock::now() >= deadline) { throw std::runtime_error("RaftConsensusRuntime: ReadIndex quorum timed out"); }
                }
                applyCommitted();
                std::lock_guard lock{mutex_};
                if (!running_ || currentTerm_ != term || role_.load() != RaftRole::LEADER || lastApplied_ < readIndex) {
                    throw std::runtime_error("RaftConsensusRuntime: ReadIndex is not applied under current leadership");
                }
            }

        private:
            bool replicatePeerTo(uint64_t peerId, uint64_t targetIndex, bool forceHeartbeat = false, uint64_t readRound = 0) {
                if (!claimPeerReplication(peerId)) { return false; }
                struct PeerReplicationGuard {
                    Impl* runtime;
                    uint64_t peerId;
                    ~PeerReplicationGuard() {
                        if (runtime != nullptr) { runtime->releasePeerReplication(peerId); }
                    }
                } guard{this, peerId};

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
                    if (needsSnapshot) {
                        InstallSnapshotResponse response;
                        if (!installSnapshotStream(peer, requestTerm, response)) {
                            if (response.term > requestTerm) { becomeFollower(response.term); }
                            return false;
                        }
                        std::lock_guard lock{mutex_};
                        if (!running_ || role_.load() != RaftRole::LEADER || currentTerm_ != requestTerm || response.term != requestTerm) { return false; }
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
                    if (!running_ || role_.load() != RaftRole::LEADER || currentTerm_ != requestTerm || response.term != requestTerm) { return false; }
                    auto* state = peerState(peerId);
                    if (state == nullptr) { return false; }
                    if (response.success) {
                        state->matchIndex = std::max(state->matchIndex, response.matchIndex);
                        state->nextIndex = state->matchIndex + 1;
                        forceHeartbeat = false;
                        auto& ack = readAcks_[peerId];
                        if (ack.first != requestTerm) { ack = {requestTerm, readRound}; }
                        else { ack.second = std::max(ack.second, readRound); }
                        peerReplicationCv_.notify_all();
                    }
                    else { updateNextIndexAfterConflictLocked(*state, response); }
                }
            }

            void sendHeartbeats() {
                std::vector<NodeInfo> targets;
                uint64_t targetCommit = 0;
                {
                    std::lock_guard lock{mutex_};
                    if (!running_ || role_.load() != RaftRole::LEADER) { return; }
                    if (peerReplication_.empty()) { resetLeaderReplicationState(); }
                    ensurePeerReplicationTargetsLocked();
                    targetCommit = lastLogIndex();
                    targets = peers_;
                }
                for (const auto& peer : targets) { scheduleReplication(peer, targetCommit, true); }
            }

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
                    const size_t active = activeClientHandlers_.fetch_add(1, std::memory_order_acq_rel);
                    if (active >= MAX_RAFT_CLIENT_HANDLERS) {
                        activeClientHandlers_.fetch_sub(1, std::memory_order_acq_rel);
                        closeSocket(client);
                        continue;
                    }
                    {
                        std::lock_guard lock{clientHandlersMutex_};
                        clientSockets_.insert(client);
                    }
                    try {
                        std::thread(
                            [this, client] {
                                try { handleClient(client); }
                                catch (...) {}
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
                        clientHandlersCv_.notify_all();
                        return;
                    }
                }
            }

            void handleClient(SocketHandle client) {
                std::unique_ptr<crypto::SecureSession> secure;
                crypto::PublicKey remotePublicKey{};
                if (runtimeOptions_.transportMode == TransportMode::SECURE) {
                    secure = acceptSecureSession(client, remotePublicKey);
                    if (!secure) { return; }
                }
                DecodedFrame helloFrame;
                RaftPeerHello hello;
                RaftPeerHelloResponse helloResponse;
                if (recvFrame(client, secure.get(), helloFrame) && helloFrame.type == ReplMsgType::RAFT_PEER_HELLO &&
                    decodeRaftPeerHello(helloFrame.payload, hello)) {
                    bool knownNode = false;
                    {
                        std::lock_guard lock{mutex_};
                        knownNode = isVotingMemberLocked(hello.nodeId);
                    }
                    if (!knownNode || !verifySecurePeer(hello.nodeId, remotePublicKey)) {
                        helloResponse.status = RaftPeerHelloStatus::UNKNOWN_NODE;
                    }
                    else if (hello.clusterId != config_.clusterId()) {
                        helloResponse.status = RaftPeerHelloStatus::CLUSTER_MISMATCH;
                        ++foreignClusterRejects_;
                        recordFailure(ClusterFailureCode::FOREIGN_CLUSTER, ClusterHealthState::DEGRADED);
                    }
                    else if (hello.compatibilityFingerprint != compatibilityFingerprint_ ||
                             hello.blobPolicy != runtimeOptions_.raftBlobPolicy ||
                             hello.requestsEnabled != runtimeOptions_.requests.enabled ||
                             hello.requestMaxRetentionMs != runtimeOptions_.requests.maxRetentionMs ||
                             hello.requestCapacity != runtimeOptions_.requests.maxTrackedRequests) {
                        helloResponse.status = RaftPeerHelloStatus::POLICY_MISMATCH;
                        ++peerPolicyMismatchRejects_;
                        recordFailure(ClusterFailureCode::POLICY_MISMATCH, ClusterHealthState::DEGRADED);
                    }
                    else { helloResponse.status = RaftPeerHelloStatus::ACCEPTED; }
                }
                if (!sendFrame(client, secure.get(), encodeRaftPeerHelloResponse(helloResponse)) ||
                    helloResponse.status != RaftPeerHelloStatus::ACCEPTED) { return; }
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
                    const int ready = ::select(0, &readable, nullptr, nullptr, &timeout);
#else
                    pollfd readable{client, POLLIN, 0};
                    const int ready = ::poll(&readable, 1, 100);
#endif
                    if (ready == 0) { continue; }
                    if (ready < 0) { return; }
                    DecodedFrame frame;
                    if (!recvFrame(client, secure.get(), frame)) { return; }
                    if (frame.type == ReplMsgType::RAFT_REQUEST_VOTE) {
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
                    else { return; }
                }
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
                        const auto hardBackup = captureDurableHardStateLocked();
                        currentTerm_ = request.term;
                        votedFor_ = 0;
                        clearObservedLeaderLocked();
                        roleChange = setRole(RaftRole::FOLLOWER);
                        try { persistState(); }
                        catch (...) {
                            restoreDurableHardStateLocked(hardBackup);
                            stopRuntimeLocked();
                            throw;
                        }
                    }
                    const bool upToDate = request.lastLogTerm > lastLogTerm() || (request.lastLogTerm == lastLogTerm() && request.lastLogIndex >=
                        lastLogIndex());
                    const bool canVote = votedFor_ == 0 || votedFor_ == request.candidateId;
                    const bool granted = canVote && upToDate;
                    const auto voteBackup = captureDurableHardStateLocked();
                    if (granted) {
                        votedFor_ = request.candidateId;
                        electionDeadline_ = nextElectionDeadline();
                        try { persistState(); }
                        catch (...) {
                            restoreDurableHardStateLocked(voteBackup);
                            stopRuntimeLocked();
                            throw;
                        }
                    }
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
                        const auto hardBackup = captureDurableHardStateLocked();
                        currentTerm_ = request.term;
                        votedFor_ = 0;
                        clearObservedLeaderLocked();
                        try { persistState(); }
                        catch (...) {
                            restoreDurableHardStateLocked(hardBackup);
                            stopRuntimeLocked();
                            throw;
                        }
                    }
                    if (request.term != currentTerm_ || !observedLeaderLocked(request.term, request.leaderId)) {
                        return TimeoutNowResponse{.term = currentTerm_, .accepted = false};
                    }
                    roleChange = setRole(RaftRole::FOLLOWER);
                    forceElection_ = true;
                    electionDeadline_ = Clock::now();
                    responseTerm = currentTerm_;
                }
                notifyRoleChange(roleChange);
                cv_.notify_all();
                return TimeoutNowResponse{.term = responseTerm, .accepted = true};
            }

            bool applySnapshotChunkRequestLocked(const InstallSnapshot& request) {
                if (request.chunks.empty() && !request.done) { return false; }
                if (request.committedVoters.empty() || request.jointOldVoters.has_value() != request.jointNewVoters.has_value()) { return false; }
                if (request.jointOldVoters && (request.jointOldVoters->empty() || request.jointNewVoters->empty())) { return false; }
                const bool sameSnapshot = pendingSnapshotInstall_.active && pendingSnapshotInstall_.snapshotSeq == request.snapshotSeq &&
                    pendingSnapshotInstall_.lastIncludedIndex == request.lastIncludedIndex && pendingSnapshotInstall_.lastIncludedTerm == request.
                    lastIncludedTerm && pendingSnapshotInstall_.entryCount == request.entryCount && sameNodeSet(
                        pendingSnapshotInstall_.committedVoters,
                        request.committedVoters
                    ) && sameOptionalNodeSet(pendingSnapshotInstall_.jointOldVoters, request.jointOldVoters) && sameOptionalNodeSet(
                        pendingSnapshotInstall_.jointNewVoters,
                        request.jointNewVoters
                    );
                if (pendingSnapshotInstall_.active && !sameSnapshot) { return false; }
                const bool restartsSnapshot = sameSnapshot && !request.chunks.empty() && request.chunks.front().entryIndex == 0 && request.chunks.front().
                    valueOffset == 0 && (pendingSnapshotInstall_.nextEntryIndex != 0 || pendingSnapshotInstall_.hasCurrentEntry);
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
                    if (chunk.entryIndex >= pendingSnapshotInstall_.entryCount || chunk.valueOffset > chunk.valueSize || chunk.value.size() > chunk.valueSize -
                        chunk.valueOffset) { return false; }
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
                    else if (chunk.entryIndex != pendingSnapshotInstall_.currentEntryIndex || !chunk.key.empty()) { return false; }

                    if (pendingSnapshotInstall_.currentValueSize != chunk.valueSize || pendingSnapshotInstall_.currentValueCrc32c != chunk.valueCrc32c ||
                        pendingSnapshotInstall_.currentValueOffset != chunk.valueOffset) { return false; }
                    pendingSnapshotInstall_.currentValueCrc.update(chunk.value);
                    if (callbacks_.appendSnapshotEntryChunk) { callbacks_.appendSnapshotEntryChunk(chunk.valueOffset, chunk.value); }
                    pendingSnapshotInstall_.currentValueOffset += chunk.value.size();
                    if (pendingSnapshotInstall_.currentValueOffset == pendingSnapshotInstall_.currentValueSize) {
                        if (pendingSnapshotInstall_.currentValueCrc.finish() != pendingSnapshotInstall_.currentValueCrc32c) { return false; }
                        if (callbacks_.finishSnapshotEntry) { callbacks_.finishSnapshotEntry(); }
                        ++pendingSnapshotInstall_.nextEntryIndex;
                        pendingSnapshotInstall_.currentKey.clear();
                        pendingSnapshotInstall_.currentValueCrc = Crc32cStream{};
                        pendingSnapshotInstall_.hasCurrentEntry = false;
                    }
                }

                if (!request.done) { return true; }
                if (pendingSnapshotInstall_.hasCurrentEntry || pendingSnapshotInstall_.nextEntryIndex != pendingSnapshotInstall_.entryCount) { return false; }
                return true;
            }

            InstallSnapshotResponse handleInstallSnapshot(const InstallSnapshot& request) {
                std::optional<NodeRole> roleChange;
                std::optional<InstallSnapshotResponse> earlyResponse;
                {
                    std::lock_guard lock{mutex_};
                    if (!isVotingMemberLocked(selfNodeId_) || !isVotingMemberLocked(request.leaderId)) {
                        return InstallSnapshotResponse{.term = currentTerm_, .success = false, .lastIncludedIndex = lastIncludedIndex_};
                    }
                    if (request.term < currentTerm_) {
                        return InstallSnapshotResponse{.term = currentTerm_, .success = false, .lastIncludedIndex = lastIncludedIndex_};
                    }
                    if (request.term > currentTerm_) {
                        const auto hardBackup = captureDurableHardStateLocked();
                        currentTerm_ = request.term;
                        votedFor_ = 0;
                        try { persistState(); }
                        catch (...) {
                            restoreDurableHardStateLocked(hardBackup);
                            stopRuntimeLocked();
                            throw;
                        }
                    }
                    roleChange = setRole(RaftRole::FOLLOWER);
                    electionDeadline_ = nextElectionDeadline();
                    if (request.lastIncludedIndex <= lastIncludedIndex_) {
                        earlyResponse = InstallSnapshotResponse{.term = currentTerm_, .success = true, .lastIncludedIndex = lastIncludedIndex_};
                    }
                }
                notifyRoleChange(roleChange);
                if (earlyResponse) { return *earlyResponse; }

                bool snapshotInstallIntentDurable = false;
                bool stateMachineSnapshotFinished = false;
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
                    SnapshotInstallTransaction transaction{
                        .requestState = request.requestState,
                        .snapshotSeq = request.snapshotSeq,
                        .lastIncludedIndex = request.lastIncludedIndex,
                        .lastIncludedTerm = request.lastIncludedTerm,
                        .entryCount = request.entryCount,
                        .committedVoters = request.committedVoters,
                        .jointOldVoters = request.jointOldVoters,
                        .jointNewVoters = request.jointNewVoters
                    };
                    {
                        std::lock_guard lock{mutex_};
                        const auto backup = captureDurableLogStateLocked();
                        durableSnapshotInstall_ = transaction;
                        try { persistLog(); }
                        catch (...) {
                            restoreDurableLogStateLocked(backup);
                            return InstallSnapshotResponse{.term = currentTerm_, .success = false, .lastIncludedIndex = lastIncludedIndex_};
                        }
                        snapshotInstallIntentDurable = true;
                    }
                    if (callbacks_.finishSnapshot) { callbacks_.finishSnapshot(request.snapshotSeq); }
                    stateMachineSnapshotFinished = true;
                    if (callbacks_.forceDurable) { callbacks_.forceDurable(); }
                    {
                        std::lock_guard lock{mutex_};
                        if (currentTerm_ != request.term || !isVotingMemberLocked(request.leaderId)) {
                            if (snapshotInstallIntentDurable) { stopRuntimeLocked(); }
                            return InstallSnapshotResponse{.term = currentTerm_, .success = false, .lastIncludedIndex = lastIncludedIndex_};
                        }
                        const auto backup = captureDurableLogStateLocked();
                        if (!finalizeSnapshotInstallMetadataLocked(transaction)) {
                            restoreDurableLogStateLocked(backup);
                            return InstallSnapshotResponse{.term = currentTerm_, .success = false, .lastIncludedIndex = lastIncludedIndex_};
                        }
                        observeLeaderLocked(request.term, request.leaderId);
                        durableSnapshotInstall_.reset();
                        pendingSnapshotInstall_ = PendingSnapshotInstall{};
                        try { persistLog(); }
                        catch (...) {
                            restoreDurableLogStateLocked(backup);
                            running_ = false;
                            role_.store(RaftRole::FOLLOWER);
                            cv_.notify_all();
                            peerReplicationCv_.notify_all();
                            return InstallSnapshotResponse{.term = currentTerm_, .success = false, .lastIncludedIndex = lastIncludedIndex_};
                        }
                        return InstallSnapshotResponse{.term = currentTerm_, .success = true, .lastIncludedIndex = lastIncludedIndex_};
                    }
                }
                catch (...) {
                    const bool stateMachineSnapshotDurable = stateMachineSnapshotFinished || (snapshotInstallIntentDurable && callbacks_.isSnapshotDurable &&
                        callbacks_.isSnapshotDurable(request.snapshotSeq));
                    {
                        std::lock_guard applyLock{applyMutex_};
                        pendingSnapshotInstall_ = PendingSnapshotInstall{};
                    }
                    std::lock_guard lock{mutex_};
                    if (snapshotInstallIntentDurable && stateMachineSnapshotDurable) { stopRuntimeLocked(); }
                    return InstallSnapshotResponse{.term = currentTerm_, .success = false, .lastIncludedIndex = lastIncludedIndex_};
                }
            }

            AppendEntriesResponse handleAppendEntries(const AppendEntries& request) {
                AppendEntriesResponse response;
                std::optional<NodeRole> roleChange;
                std::optional<AppendEntriesResponse> earlyResponse;
                {
                    std::lock_guard lock{mutex_};
                    if (!isVotingMemberLocked(selfNodeId_) || !isVotingMemberLocked(request.leaderId)) {
                        return AppendEntriesResponse{.term = currentTerm_, .success = false, .matchIndex = lastLogIndex()};
                    }
                    if (request.term < currentTerm_) { return AppendEntriesResponse{.term = currentTerm_, .success = false, .matchIndex = lastLogIndex()}; }
                    if (request.term > currentTerm_) {
                        const auto hardBackup = captureDurableHardStateLocked();
                        currentTerm_ = request.term;
                        votedFor_ = 0;
                        try { persistState(); }
                        catch (...) {
                            restoreDurableHardStateLocked(hardBackup);
                            stopRuntimeLocked();
                            throw;
                        }
                    }
                    roleChange = setRole(RaftRole::FOLLOWER);
                    electionDeadline_ = nextElectionDeadline();

                    if (request.prevLogIndex < lastIncludedIndex_) { earlyResponse = conflictResponseLocked(request.prevLogIndex); }
                    else if (request.prevLogIndex > lastLogIndex()) { earlyResponse = conflictResponseLocked(request.prevLogIndex); }
                    else if (termAt(request.prevLogIndex) != request.prevLogTerm) { earlyResponse = conflictResponseLocked(request.prevLogIndex); }
                    if (earlyResponse) { response = *earlyResponse; }
                    else {
                        const auto backup = captureDurableLogStateLocked();
                        bool mutatedLogState = false;
                        uint64_t expectedIndex = request.prevLogIndex + 1;
                        uint64_t acceptedMatchIndex = request.prevLogIndex;
                        try {
                            for (const auto& entry : request.entries) {
                                if (entry.index != expectedIndex++) {
                                    earlyResponse = AppendEntriesResponse{.term = currentTerm_, .success = false, .matchIndex = acceptedMatchIndex};
                                    response = *earlyResponse;
                                    break;
                                }
                                if (entry.index <= lastIncludedIndex_) { continue; }
                                if (!isValidRaftLogEntryShape(entry)) {
                                    earlyResponse = AppendEntriesResponse{.term = currentTerm_, .success = false, .matchIndex = acceptedMatchIndex};
                                    response = *earlyResponse;
                                    break;
                                }
                                if (!entryPreservesStateMachineSequenceLocked(entry)) {
                                    earlyResponse = AppendEntriesResponse{.term = currentTerm_, .success = false, .matchIndex = acceptedMatchIndex};
                                    response = *earlyResponse;
                                    break;
                                }
                                const auto existing = entryAt(entry.index);
                                if (!existing || !sameEntry(*existing, entry)) {
                                    if (entry.index <= commitIndex_) {
                                        earlyResponse = AppendEntriesResponse{.term = currentTerm_, .success = false, .matchIndex = acceptedMatchIndex};
                                        response = *earlyResponse;
                                        break;
                                    }
                                    std::erase_if(log_, [&](const RaftLogEntry& item) { return item.index >= entry.index; });
                                    log_.push_back(entry);
                                    mutatedLogState = true;
                                }
                                acceptedMatchIndex = entry.index;
                            }

                            if (earlyResponse && mutatedLogState) {
                                restoreDurableLogStateLocked(backup);
                            }
                            if (!earlyResponse) {
                                if (request.leaderCommit > commitIndex_) {
                                    commitIndex_ = std::min(request.leaderCommit, acceptedMatchIndex);
                                    mutatedLogState = true;
                                }
                                if (mutatedLogState) { persistLog(); }
                                observeLeaderLocked(request.term, request.leaderId);
                                response = AppendEntriesResponse{.term = currentTerm_, .success = true, .matchIndex = acceptedMatchIndex};
                            }
                        }
                        catch (...) {
                            if (mutatedLogState) { restoreDurableLogStateLocked(backup); }
                            throw;
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
                if (first.kind != RaftEntryKind::BLOB || !decodeBlobChunkKey(first.key, chunk) || chunk.offset != 0 || first.value.size() > chunk.totalSize) {
                    throw std::runtime_error("RaftConsensusRuntime: corrupt Blob log entry");
                }
                const uint64_t seq = first.clientSeq;
                Crc32cStream contentCrc;

                uint64_t expectedOffset = 0;
                size_t pos = start;
                while (pos < entries.size()) {
                    const auto& entry = entries[pos];
                    RaftBlobChunk current;
                    const uint64_t entrySeq = entry.clientSeq;
                    if (entry.kind != RaftEntryKind::BLOB || !decodeBlobChunkKey(entry.key, current) || current.blobId != chunk.blobId || entrySeq != seq ||
                        current.totalSize != chunk.totalSize || current.contentCrc32c != chunk.contentCrc32c || current.offset != expectedOffset || entry.value.
                        size() > current.totalSize - current.offset) { throw std::runtime_error("RaftConsensusRuntime: out-of-order Blob log chunk"); }
                    contentCrc.update(entry.value);
                    expectedOffset += entry.value.size();
                    batch.finalLogIndex = entry.index;
                    ++pos;
                    if (expectedOffset == chunk.totalSize) {
                        if (contentCrc.finish() != chunk.contentCrc32c) { throw std::runtime_error("RaftConsensusRuntime: Blob log payload CRC mismatch"); }
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
                if (expectedOffset > chunk.totalSize) { throw std::runtime_error("RaftConsensusRuntime: Blob log payload exceeds declared size"); }
                batch.nextEntryOffset = pos;
                return batch;
            }

            void applyCommitted() {
                std::lock_guard applyLock{applyMutex_};
                std::vector<RaftLogEntry> toApply;
                {
                    std::lock_guard lock{mutex_};
                    for (const auto& entry : log_) { if (entry.index > lastApplied_ && entry.index <= commitIndex_) { toApply.push_back(entry); } }
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
                                entry.clientSeq,
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
                if (appliedDurableMutation && callbacks_.forceDurable) {
                    try { callbacks_.forceDurable(); }
                    catch (...) {
                        std::lock_guard lock{mutex_};
                        stopRuntimeLocked();
                        throw;
                    }
                }
                crashAtLogTestPoint("raft.request.after_apply");
                std::lock_guard lock{mutex_};
                if (appliedThrough > lastApplied_) {
                    const auto backup = captureDurableLogStateLocked();
                    lastApplied_ = appliedThrough;
                    rebuildRequestResultsLocked(backup.lastApplied);
                    try { persistLog(); }
                    catch (...) {
                        restoreDurableLogStateLocked(backup);
                        stopRuntimeLocked();
                        throw;
                    }
                }
            }

        public:
            Impl(std::filesystem::path dbDir, ClusterConfig config, uint64_t selfNodeId, ClusterEngineCallbacks callbacks, ClusterRuntimeOptions runtimeOptions)
                : dbDir_{std::move(dbDir)},
                  statePath_{dbDir_.empty() ? std::filesystem::path{"cluster-raft.state"} : dbDir_ / "cluster-raft.state"},
                  logPath_{dbDir_.empty() ? std::filesystem::path{"cluster-raft.log"} : dbDir_ / "cluster-raft.log"},
                  requestJournalBasePath_{dbDir_.empty() ? std::filesystem::path{"cluster-raft.requests"} : dbDir_ / "cluster-raft.requests"},
                  config_{std::move(config)},
                  router_{config_},
                  selfNodeId_{selfNodeId},
                  callbacks_{std::move(callbacks)},
                  runtimeOptions_{std::move(runtimeOptions)} {
                config_.validate();
                compatibilityFingerprint_ = raftCompatibilityFingerprint(config_, runtimeOptions_);
                if (runtimeOptions_.raftBlobChunkSizeBytes == 0 || runtimeOptions_.raftBlobChunkSizeBytes > maxRaftBlobChunkSizeBytes()) {
                    throw std::invalid_argument("RaftConsensusRuntime: invalid Raft Blob chunk size");
                }
                self_ = config_.findById(selfNodeId_);
                if (self_ == nullptr || !self_->dataBearing()) { throw std::invalid_argument("RaftConsensusRuntime: local node must be data-bearing"); }
                if (runtimeOptions_.transportMode == TransportMode::SECURE) { localIdentity_ = loadSecureIdentity(runtimeOptions_); }
                committedVoters_ = config_.dataNodes();
                refreshPeersFromMembershipLocked();
                if (config_.dataNodes().empty()) { throw std::invalid_argument("RaftConsensusRuntime: no data-bearing nodes"); }
                recoverState();
                recoverLog();
                recoverAppliedProgressFromStateMachine();
                this->replayCommittedMembership();
                rebuildRequestResultsLocked();
                validatePeerConfiguration(replicationTargetsLocked());
            }

            ~Impl() { this->close(); }

            void start() {
                std::lock_guard lifecycle{lifecycleMutex_};
                {
                    std::lock_guard lock{workersMutex_};
                    workersStopping_ = false;
                }
                if (logIoFailed_) { throw std::runtime_error("Raft log: reopen required after persistence failure"); }
                this->recoverDurableSnapshotInstall();
                {
                    std::lock_guard lock{mutex_};
                    if (running_) { return; }
                    if (timerThread_.joinable() || proposalThread_.joinable()) {
                        throw std::runtime_error("RaftConsensusRuntime: close the stopped runtime before restarting");
                    }
                    running_ = true;
                    electionDeadline_ = this->nextElectionDeadline();
                    nextCompactionCheck_ = Clock::now();
                    nextCompactionDeadline_ = Clock::now() + std::chrono::milliseconds{runtimeOptions_.raftSnapshot.maxIntervalMs};
                }
                bool endpointStarted = false;
                runtimeStartedAtUs_.store(observationNowUs(), std::memory_order_relaxed);
                health_.store(ClusterHealthState::HEALTHY, std::memory_order_relaxed);
                try {
                    const SocketHandle listenSock = listenOn(runtimeOptions_.replBindHost, self_->replPort);
                    {
                        std::lock_guard lock{mutex_};
                        listenSock_ = listenSock;
                    }
                    acceptThread_ = std::thread([this, listenSock] { this->acceptLoop(listenSock); });
                    timerThread_ = std::thread([this] { this->timerLoop(); });
                    proposalThread_ = std::thread([this] { this->proposalLoop(); });
                    endpointStarted = true;
                    this->applyCommitted();
                    bool shouldBecomeSingleNodeLeader = false;
                    uint64_t singleNodeTerm = 0;
                    {
                        std::lock_guard lock{mutex_};
                        shouldBecomeSingleNodeLeader = peers_.empty() && self_->coordinatorEligible() && isVotingMemberLocked(selfNodeId_);
                        singleNodeTerm = currentTerm_ == 0 ? 1 : currentTerm_;
                    }
                    if (shouldBecomeSingleNodeLeader) { this->becomeLeader(singleNodeTerm); }
                }
                catch (...) {
                    if (!endpointStarted) {
                        ++endpointStartFailures_;
                        recordFailure(ClusterFailureCode::ENDPOINT_START, ClusterHealthState::FAILED);
                    }
                    close();
                    throw;
                }
            }

            void close() {
                std::lock_guard lifecycle{lifecycleMutex_};
                SocketHandle listener = BAD_SOCKET;
                {
                    std::lock_guard lock{mutex_};
                    stopRuntimeLocked();
                    listener = listenSock_;
                    listenSock_ = BAD_SOCKET;
                    finishProposalsLocked();
                    proposalQueue_.clear();
                }
                shutdownSocket(listener);
                closeSocket(listener);
                std::vector<std::shared_ptr<PeerWorker>> workers;
                {
                    std::lock_guard lock{workersMutex_};
                    workersStopping_ = true;
                    for (auto& [_, worker] : workers_) { workers.push_back(worker); }
                }
                for (auto& worker : workers) {
                    {
                        std::lock_guard lock{worker->mutex};
                        worker->stopping = true;
                        worker->tasks.clear();
                        worker->cv.notify_all();
                    }
                    std::lock_guard socketLock{worker->socketMutex};
                    shutdownSocket(worker->socket);
                }
                if (acceptThread_.joinable()) { acceptThread_.join(); }
                {
                    std::lock_guard lock{clientHandlersMutex_};
                    for (auto socket : clientSockets_) { shutdownSocket(socket); }
                }
                if (timerThread_.joinable()) { timerThread_.join(); }
                if (proposalThread_.joinable()) { proposalThread_.join(); }
                for (auto& worker : workers) { if (worker->thread.joinable()) { worker->thread.join(); } }
                {
                    std::lock_guard lock{workersMutex_};
                    workers_.clear();
                }
                std::unique_lock clientLock{clientHandlersMutex_};
                clientHandlersCv_.wait(clientLock, [this] { return activeClientHandlers_.load() == 0; });
            }

            RaftRuntimeStats stats() const {
                RaftRuntimeStats out;
                out.enabled = true;
                out.sampledAtUs = observationNowUs();
                out.runtimeStartedAtUs = runtimeStartedAtUs_.load(std::memory_order_relaxed);
                out.clusterGroupId = runtimeOptions_.clusterGroupId;
                out.clusterGroupEpoch = runtimeOptions_.clusterGroupEpoch;
                out.health = health_.load(std::memory_order_relaxed);
                out.lastFailure = lastFailure_.load(std::memory_order_relaxed);
                out.lastFailureAtUs = lastFailureAtUs_.load(std::memory_order_relaxed);
                out.outboundConnections = outboundConnections_.load(std::memory_order_relaxed);
                out.peerWorkers = peerWorkersStarted_.load(std::memory_order_relaxed);
                out.proposalBatches = proposalBatches_.load(std::memory_order_relaxed);
                out.expiredRequests = expiredRequests_.load(std::memory_order_relaxed);
                out.rejectedRequests = rejectedRequests_.load(std::memory_order_relaxed);
                out.peerPolicyMismatchRejects = peerPolicyMismatchRejects_.load(std::memory_order_relaxed);
                out.foreignClusterRejects = foreignClusterRejects_.load(std::memory_order_relaxed);
                out.endpointStartFailures = endpointStartFailures_.load(std::memory_order_relaxed);
                out.requestJournalBytesWritten = requestJournalBytesWritten_.load(std::memory_order_relaxed);
                out.requestJournalCompactions = requestJournalCompactions_.load(std::memory_order_relaxed);
                {
                    std::lock_guard lock{mutex_};
                    out.currentTerm = currentTerm_;
                    out.leaderNodeId = role_.load() == RaftRole::LEADER ? selfNodeId_ :
                        (observedLeaderTerm_ == currentTerm_ ? observedLeaderId_ : 0);
                    out.commitIndex = commitIndex_;
                    out.appliedIndex = lastApplied_;
                    out.lastLogIndex = lastLogIndex();
                    out.snapshotIndex = lastIncludedIndex_;
                    out.proposalQueueDepth = proposalQueue_.size();
                    out.pendingProposals = proposals_.size();
                    out.retainedRequestResults = requestState_.results.size();
                    out.requestCapacity = runtimeOptions_.requests.maxTrackedRequests;
                    out.requestJournalBytes = requestJournalBytes_;
                    out.requestJournalRecords = requestJournalRecords_;
                    for (const auto& proposal : proposals_) { if (proposal->request && !proposal->error) { ++out.pendingRequests; } }
                    for (const auto& proposal : proposalQueue_) { out.replicationQueueBytes += proposal->bytes; }
                    out.peers.reserve(peerReplication_.size());
                    for (const auto& peer : peerReplication_) {
                        RaftPeerStats peerStats;
                        peerStats.nodeId = peer.node.nodeId;
                        peerStats.matchIndex = peer.matchIndex;
                        peerStats.nextIndex = peer.nextIndex;
                        peerStats.replicationLag = out.lastLogIndex > peer.matchIndex ? out.lastLogIndex - peer.matchIndex : 0;
                        out.peers.push_back(std::move(peerStats));
                    }
                }
                std::lock_guard workersLock{workersMutex_};
                for (const auto& [nodeId, worker] : workers_) {
                    {
                        std::lock_guard workLock{worker->mutex};
                        out.replicationQueueFrames += worker->tasks.size() + (worker->replicate ? 1u : 0u);
                    }
                    const auto peer = std::ranges::find(out.peers, nodeId, &RaftPeerStats::nodeId);
                    if (peer != out.peers.end()) {
                        std::lock_guard socketLock{worker->socketMutex};
                        peer->connected = socketOk(worker->socket);
                        peer->lastSuccessfulContactAtUs = worker->lastSuccessfulContactAtUs.load(std::memory_order_relaxed);
                        peer->roundTripsSucceeded = worker->roundTripsSucceeded.load(std::memory_order_relaxed);
                        peer->roundTripsFailed = worker->roundTripsFailed.load(std::memory_order_relaxed);
                        peer->consecutiveRoundTripFailures = worker->consecutiveRoundTripFailures.load(std::memory_order_relaxed);
                        peer->lastRoundTripAtUs = worker->lastRoundTripAtUs.load(std::memory_order_relaxed);
                        peer->lastRoundTripFailureAtUs = worker->lastRoundTripFailureAtUs.load(std::memory_order_relaxed);
                        peer->roundTripLatencyUs = worker->roundTripLatencyUs.snapshot();
                    }
                }
                out.sampledAtUs = observationNowUs();
                return out;
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

            static uint64_t requestNow() {
                return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count());
            }

            void expireRequestsLocked(uint64_t now) {
                requestState_.expiryFloor = std::max(requestState_.expiryFloor, now);
                const auto before = requestState_.results.size();
                std::erase_if(requestState_.results, [&](const auto& item) {
                    return item.first.expiresAtUnixMs <= requestState_.expiryFloor;
                });
                expiredRequests_.fetch_add(before - requestState_.results.size(), std::memory_order_relaxed);
            }

            void rebuildRequestResultsLocked(uint64_t afterIndex = 0) {
                const auto first = std::upper_bound(log_.begin(), log_.end(), afterIndex,
                    [](uint64_t index, const RaftLogEntry& entry) { return index < entry.index; });
                for (auto it = first; it != log_.end() && it->index <= lastApplied_; ++it) {
                    if (!it->requestId) { continue; }
                    expireRequestsLocked(it->requestTime);
                    if (it->requestId->expiresAtUnixMs <= requestState_.expiryFloor) { continue; }
                    const RequestRecord record{it->requestFingerprint, it->clientSeq, it->index};
                    const auto [existing, inserted] = requestState_.results.emplace(*it->requestId, record);
                    if (!inserted && (existing->second.index != record.index || existing->second.fingerprint != record.fingerprint)) {
                        throw std::runtime_error("Raft: conflicting committed request identity");
                    }
                    if (requestState_.results.size() > MAX_REQUEST_RECORDS) { throw std::runtime_error("Raft: request result limit exceeded"); }
                }
            }

            static std::shared_future<ClusterRequestResult> readyRequest(ClusterRequestResult result) {
                std::promise<ClusterRequestResult> promise;
                promise.set_value(result);
                return promise.get_future().share();
            }

            uint64_t validateRequestLocked(const ClusterRequestId& id) {
                if (!runtimeOptions_.requests.enabled) {
                    rejectedRequests_.fetch_add(1, std::memory_order_relaxed);
                    throw std::logic_error("Raft: retry-safe requests are disabled");
                }
                if (id.expiresAtUnixMs == 0 || !std::ranges::any_of(id.nonce, [](uint8_t b) { return b != 0; })) {
                    rejectedRequests_.fetch_add(1, std::memory_order_relaxed);
                    throw std::invalid_argument("Raft: invalid request identity");
                }
                const uint64_t now = std::max(requestNow(), requestState_.expiryFloor);
                if (id.expiresAtUnixMs > now && id.expiresAtUnixMs - now > runtimeOptions_.requests.maxRetentionMs) {
                    rejectedRequests_.fetch_add(1, std::memory_order_relaxed);
                    throw std::invalid_argument("Raft: request exceeds maximum retry retention");
                }
                return now;
            }

            std::shared_future<ClusterRequestResult> submitRequest(
                const ClusterRequestId& id, const std::array<uint8_t, 32>& fingerprint,
                std::function<ClusterHistoryEntry()> prepare
            ) {
                std::lock_guard admission{requestAdmissionMutex_};
                auto proposal = std::make_shared<Proposal>();
                proposal->request = std::make_shared<RequestCompletion>();
                proposal->request->id = id;
                proposal->request->fingerprint = fingerprint;
                uint64_t now = 0, term = 0;
                {
                    std::lock_guard lock{mutex_};
                    if (!running_ || role_.load() != RaftRole::LEADER || administrativeChange_) {
                        throw std::runtime_error("Raft: retry-safe write requires an active leader");
                    }
                    now = validateRequestLocked(id);
                    if (id.expiresAtUnixMs <= now) { return readyRequest({ClusterRequestStatus::EXPIRED}); }
                    const auto checkFingerprint = [&](const auto& value) {
                        if (value != fingerprint) { throw std::invalid_argument("Raft: request id reused for a different mutation"); }
                    };
                    if (const auto it = requestState_.results.find(id); it != requestState_.results.end()) {
                        checkFingerprint(it->second.fingerprint);
                        return readyRequest({ClusterRequestStatus::APPLIED, it->second.sequence, it->second.index});
                    }
                    for (const auto& pending : proposals_) {
                        if (pending->request && pending->request->id == id && !pending->error) {
                            checkFingerprint(pending->request->fingerprint);
                            return pending->request->future;
                        }
                    }
                    for (const auto& entry : log_) {
                        if (entry.requestId != id) { continue; }
                        checkFingerprint(entry.requestFingerprint);
                        proposal->index = entry.index;
                        proposal->entryTerm = entry.term;
                        proposal->request->sequence = entry.clientSeq;
                        proposal->term = currentTerm_;
                        proposal->deadline = Clock::now() + std::chrono::milliseconds{config_.consistency().ackTimeoutMs};
                        proposals_.push_back(proposal);
                        cv_.notify_all();
                        return proposal->request->future;
                    }
                    size_t tracked = 0;
                    for (const auto& [token, result] : requestState_.results) { if (token.expiresAtUnixMs > now) { ++tracked; } }
                    for (const auto& entry : log_) { if (entry.index > lastApplied_ && entry.requestId && entry.requestId->expiresAtUnixMs > now) { ++tracked; } }
                    for (const auto& pending : proposalQueue_) { if (pending->request && !pending->error) { ++tracked; } }
                    if (tracked >= runtimeOptions_.requests.maxTrackedRequests) {
                        rejectedRequests_.fetch_add(1, std::memory_order_relaxed);
                        throw std::runtime_error("Raft: request result capacity exhausted; unexpired results are never evicted");
                    }
                    term = currentTerm_;
                }
                const auto mutation = prepare();
                RaftLogEntry entry;
                entry.requestId = id;
                entry.requestFingerprint = fingerprint;
                entry.requestTime = now;
                entry.clientSeq = mutation.seq;
                entry.op = static_cast<ReplOpType>(mutation.op);
                entry.flags = mutation.recordFlags;
                entry.sourceNodeId = mutation.sourceNodeId;
                entry.key = mutation.key;
                entry.value = mutation.value;
                proposal->request->sequence = mutation.seq;
                // Preparation can include Blob I/O. Never admit its mutation in a
                // different leadership term than the deduplication check.
                {
                    std::lock_guard lock{mutex_};
                    if (!running_ || role_.load() != RaftRole::LEADER || currentTerm_ != term) {
                        throw std::runtime_error("Raft: leadership changed during request preparation");
                    }
                }
                proposal->term = term;
                (void)submitProposal({std::move(entry)}, proposal);
                return proposal->request->future;
            }

            ClusterRequestResult queryRequest(const ClusterRequestId& id) {
                linearizableReadBarrier();
                std::lock_guard lock{mutex_};
                const auto now = validateRequestLocked(id);
                if (id.expiresAtUnixMs <= now) { return {ClusterRequestStatus::EXPIRED}; }
                if (const auto it = requestState_.results.find(id); it != requestState_.results.end()) {
                    return {ClusterRequestStatus::APPLIED, it->second.sequence, it->second.index};
                }
                for (const auto& entry : log_) { if (entry.requestId == id) { return {ClusterRequestStatus::PENDING}; } }
                for (const auto& proposal : proposalQueue_) {
                    if (proposal->request && proposal->request->id == id && !proposal->error) { return {ClusterRequestStatus::PENDING}; }
                }
                return {ClusterRequestStatus::NOT_FOUND};
            }

            std::future<void> submitEntry(
                uint64_t seq, ReplOpType op, std::span<const uint8_t> key,
                std::span<const uint8_t> value, uint8_t flags, uint64_t source
            ) {
                if (seq == 0) { throw std::invalid_argument("RaftConsensusRuntime: state-machine sequence must be non-zero"); }
                RaftLogEntry entry;
                entry.clientSeq = seq;
                entry.kind = RaftEntryKind::MUTATION;
                entry.op = op;
                entry.flags = flags;
                entry.sourceNodeId = source;
                entry.key.assign(key.begin(), key.end());
                entry.value.assign(value.begin(), value.end());
                return submitProposal({std::move(entry)});
            }

            void shipEntry(uint64_t seq, ReplOpType op, std::span<const uint8_t> key, std::span<const uint8_t> value, uint8_t flags, uint64_t source) {
                submitEntry(seq, op, key, value, flags, source).get();
            }

            void shipBlob(uint64_t seq, uint64_t blobId, std::span<const uint8_t> content) {
                if (seq == 0) { throw std::invalid_argument("RaftConsensusRuntime: Blob state-machine sequence must be non-zero"); }
                if (runtimeOptions_.raftBlobPolicy == RaftBlobPolicy::PRIMARY_SIDE_ONLY) {
                    if (role_.load() != RaftRole::LEADER) { throw std::runtime_error("RaftConsensusRuntime: local node is not Raft leader"); }
                    return;
                }
                if (runtimeOptions_.raftBlobPolicy != RaftBlobPolicy::RAFT_LOG) {
                    throw std::runtime_error("RaftConsensusRuntime: RAFT_QUORUM does not support Blob payload replication");
                }
                std::vector<RaftLogEntry> entries;
                const uint32_t contentCrc32c = crcBytes(content);
                const size_t chunkSize = runtimeOptions_.raftBlobChunkSizeBytes;
                size_t offset = 0;
                do {
                    const size_t remaining = content.size() - offset;
                    const size_t currentChunkSize = std::min(chunkSize, remaining);
                    RaftLogEntry entry;
                    entry.term = 0;
                    entry.index = entries.size() + 1;
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
                    entry.value.assign(
                        content.begin() + static_cast<std::ptrdiff_t>(offset),
                        content.begin() + static_cast<std::ptrdiff_t>(offset + currentChunkSize)
                    );
                    entries.push_back(std::move(entry));
                    offset += currentChunkSize;
                }
                while (offset < content.size());

                submitProposal(std::move(entries)).get();
            }

            void validatePeerConfiguration(const std::vector<NodeInfo>& nodes) const {
                // Recovered/online membership can differ from the original config.
                (void)ClusterConfig{nodes, ReplicationMode::STANDALONE, {},
                                    ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM}};
                if (runtimeOptions_.transportMode == TransportMode::SECURE) {
                    std::vector<uint64_t> required;
                    for (const auto& peer : nodes) { if (peer.nodeId != selfNodeId_) { required.push_back(peer.nodeId); } }
                    runtimeOptions_.secure.validatePins(required);
                }
            }

            void addVotingNode(const NodeInfo& node) {
                AdministrativeGuard admission{*this};
                if (!node.dataBearing()) { throw std::invalid_argument("RaftConsensusRuntime: Raft voting node must be data-bearing"); }
                if (!onlineMembershipChangeEnabled()) { throw std::runtime_error("RaftConsensusRuntime: online Raft membership change is disabled"); }
                {
                    std::lock_guard lock{mutex_};
                    auto targets = replicationTargetsLocked();
                    if (!containsNode(targets, node.nodeId)) { targets.push_back(node); }
                    validatePeerConfiguration(targets);
                }
                (void)this->commitOutstandingEntry(std::chrono::milliseconds{20000});
                std::vector<NodeInfo> oldVoters;
                std::vector<NodeInfo> newVoters;
                bool finishExistingJoint = false;
                std::optional<RaftLogEntry> pendingConfigEntry;
                {
                    std::lock_guard lock{mutex_};
                    if (role_.load() != RaftRole::LEADER) { throw std::runtime_error("RaftConsensusRuntime: local node is not Raft leader"); }
                    if (commitIndex_ != lastLogIndex() && !log_.empty() && (log_.back().kind == RaftEntryKind::CONFIG_JOINT || log_.back().kind ==
                        RaftEntryKind::CONFIG_FINAL)) {
                        std::vector<NodeInfo> pendingNewVoters;
                        if (decodeNodeSet(log_.back().value, pendingNewVoters) && containsNode(pendingNewVoters, node.nodeId)) {
                            pendingConfigEntry = log_.back();
                        }
                        else { throw std::runtime_error("RaftConsensusRuntime: cannot change membership while a prior entry is uncommitted"); }
                    }
                    else if (jointOldVoters_ || jointNewVoters_) {
                        if (jointNewVoters_ && containsNode(*jointNewVoters_, node.nodeId) && !containsNode(committedVoters_, node.nodeId)) {
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
                        if (!decodeNodeSet(pendingConfigEntry->key, oldFinalVoters) || !decodeNodeSet(pendingConfigEntry->value, finalVoters)) {
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
                AdministrativeGuard admission{*this};
                if (!onlineMembershipChangeEnabled()) { throw std::runtime_error("RaftConsensusRuntime: online Raft membership change is disabled"); }
                (void)this->commitOutstandingEntry(std::chrono::milliseconds{20000});
                if (nodeId == selfNodeId_) { throw std::runtime_error("RaftConsensusRuntime: removing the local leader is not supported by this API"); }
                std::vector<NodeInfo> oldVoters;
                std::vector<NodeInfo> newVoters;
                bool finishExistingJoint = false;
                std::optional<RaftLogEntry> pendingConfigEntry;
                {
                    std::lock_guard lock{mutex_};
                    if (role_.load() != RaftRole::LEADER) { throw std::runtime_error("RaftConsensusRuntime: local node is not Raft leader"); }
                    if (commitIndex_ != this->lastLogIndex() && !log_.empty() && (log_.back().kind == RaftEntryKind::CONFIG_JOINT || log_.back().kind ==
                        RaftEntryKind::CONFIG_FINAL)) {
                        std::vector<NodeInfo> pendingNewVoters;
                        if (decodeNodeSet(log_.back().value, pendingNewVoters) && !containsNode(pendingNewVoters, nodeId)) { pendingConfigEntry = log_.back(); }
                        else { throw std::runtime_error("RaftConsensusRuntime: cannot change membership while a prior entry is uncommitted"); }
                    }
                    else if (jointOldVoters_ || jointNewVoters_) {
                        if (jointOldVoters_ && jointNewVoters_ && containsNode(*jointOldVoters_, nodeId) && !containsNode(*jointNewVoters_, nodeId)) {
                            oldVoters = *jointOldVoters_;
                            newVoters = *jointNewVoters_;
                            finishExistingJoint = true;
                        }
                        else { throw std::runtime_error("RaftConsensusRuntime: membership change already in progress"); }
                    }
                    else if (!containsNode(committedVoters_, nodeId)) { throw std::invalid_argument("RaftConsensusRuntime: node is not a voting member"); }
                    else {
                        oldVoters = committedVoters_;
                        for (const auto& node : committedVoters_) { if (node.nodeId != nodeId) { newVoters.push_back(node); } }
                        if (newVoters.empty()) { throw std::invalid_argument("RaftConsensusRuntime: cannot remove the last voting member"); }
                    }
                }
                if (pendingConfigEntry) {
                    this->appendReplicateAndCommitConfig(*pendingConfigEntry, false);
                    if (pendingConfigEntry->kind == RaftEntryKind::CONFIG_JOINT) {
                        std::vector<NodeInfo> oldFinalVoters;
                        std::vector<NodeInfo> finalVoters;
                        if (!decodeNodeSet(pendingConfigEntry->key, oldFinalVoters) || !decodeNodeSet(pendingConfigEntry->value, finalVoters)) {
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
                AdministrativeGuard admission{*this};
                if (targetNodeId == selfNodeId_) { return; }
                NodeInfo target;
                uint64_t targetIndex = 0;
                uint64_t term = 0;
                {
                    std::lock_guard lock{mutex_};
                    if (role_.load() != RaftRole::LEADER) { throw std::runtime_error("RaftConsensusRuntime: local node is not Raft leader"); }
                    if (!isVotingMemberLocked(targetNodeId)) { throw std::invalid_argument("RaftConsensusRuntime: transfer target is not a voting member"); }
                    auto* state = this->peerState(targetNodeId);
                    if (state == nullptr) { throw std::invalid_argument("RaftConsensusRuntime: transfer target is not a replication peer"); }
                    target = state->node;
                    targetIndex = this->lastLogIndex();
                    term = currentTerm_;
                }

                if (!this->replicatePeerTo(targetNodeId, targetIndex, true)) {
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
        config.validateRuntime(selfNodeId, runtimeOptions);
        return std::unique_ptr<RaftConsensusRuntime>(
            new RaftConsensusRuntime(std::make_unique<Impl>(std::move(dbDir), std::move(config), selfNodeId, std::move(callbacks), std::move(runtimeOptions)))
        );
    }

    RaftConsensusRuntime::RaftConsensusRuntime(std::unique_ptr<Impl> impl) : impl_{std::move(impl)} {}
    RaftConsensusRuntime::~RaftConsensusRuntime() = default;
    std::shared_future<ClusterRequestResult> RaftConsensusRuntime::submitRequest(
        const ClusterRequestId& id, const std::array<uint8_t, 32>& fingerprint, std::function<ClusterHistoryEntry()> prepare
    ) { return impl_->submitRequest(id, fingerprint, std::move(prepare)); }
    ClusterRequestResult RaftConsensusRuntime::queryRequest(const ClusterRequestId& id) { return impl_->queryRequest(id); }

    void RaftConsensusRuntime::start() { impl_->start(); }

    void RaftConsensusRuntime::close() { impl_->close(); }
    RaftRuntimeStats RaftConsensusRuntime::stats() const { return impl_->stats(); }

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

    std::future<void> RaftConsensusRuntime::submitEntry(uint64_t seq, ReplOpType op, std::span<const uint8_t> key,
        std::span<const uint8_t> value, uint8_t flags, uint64_t source) {
        return impl_->submitEntry(seq, op, key, value, flags, source);
    }

    void RaftConsensusRuntime::shipBlob(uint64_t seq, uint64_t blobId, std::span<const uint8_t> content) { impl_->shipBlob(seq, blobId, content); }

    void RaftConsensusRuntime::linearizableReadBarrier() { impl_->linearizableReadBarrier(); }

    void RaftConsensusRuntime::addVotingNode(const NodeInfo& node) { impl_->addVotingNode(node); }

    void RaftConsensusRuntime::removeVotingNode(uint64_t nodeId) { impl_->removeVotingNode(nodeId); }

    void RaftConsensusRuntime::transferLeadership(uint64_t targetNodeId) { impl_->transferLeadership(targetNodeId); }
}

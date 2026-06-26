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

// akkengine/include/akk/engine/server/ApiTransport.hpp
#pragma once

#include "akk/engine/AkkEngine.hpp"
#include "akk/net/tls/TlsStream.hpp"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <cerrno>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
#endif

#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace akkaradb::engine::server::detail {
    #ifdef _WIN32
    using SocketHandle = SOCKET; inline constexpr SocketHandle BAD_SOCKET_VALUE = INVALID_SOCKET; inline void netInit() {
        static std::once_flag flag;
        std::call_once(
            flag,
            [] {
                WSADATA data{};
                if (::WSAStartup(MAKEWORD(2, 2), &data) != 0) { throw std::runtime_error("ApiServer: WSAStartup failed"); }
            }
        );
    } inline bool socketOk(SocketHandle s) noexcept { return s != INVALID_SOCKET; } inline void closeSocket(SocketHandle s) noexcept {
        if (socketOk(s)) { ::closesocket(s); }
    } inline void shutdownSocket(SocketHandle s) noexcept { if (socketOk(s)) { ::shutdown(s, SD_BOTH); } }
    #else
    using SocketHandle = int;
    inline constexpr SocketHandle BAD_SOCKET_VALUE = -1;
    inline void netInit() {}
    inline bool socketOk(SocketHandle s) noexcept { return s >= 0; }
    inline void closeSocket(SocketHandle s) noexcept { if (socketOk(s)) { ::close(s); } }
    inline void shutdownSocket(SocketHandle s) noexcept { if (socketOk(s)) { ::shutdown(s, SHUT_RDWR); } }
    #endif

    inline bool lastAcceptErrorIsTransient() noexcept {
        #ifdef _WIN32
        const int err = WSAGetLastError(); return err == WSAEINTR || err == WSAECONNRESET;
        #else
        const int err = errno;
        if (err == EINTR || err == ECONNABORTED) { return true; }
        #ifdef EPROTO
        if (err == EPROTO) { return true; }
        #endif
        return false;
        #endif
    }

    inline int sendNoSigpipeFlags() noexcept {
        #ifdef _WIN32
        return 0;
        #else
        #ifdef MSG_NOSIGNAL
        return MSG_NOSIGNAL;
        #else
        return 0;
        #endif
        #endif
    }

    struct TlsConfigStorage {
        std::string certPath;
        std::string keyPath;
        std::string caPath;
        std::string pskIdentity;
        std::vector<uint8_t> psk;
        net::TlsConfig config{};
    };

    inline TlsConfigStorage makeTlsConfig(const AkkEngineOptions::ApiTlsOptions& options) {
        TlsConfigStorage storage;
        storage.certPath = options.certPath.string();
        storage.keyPath = options.keyPath.string();
        storage.caPath = options.caPath.string();
        storage.pskIdentity = options.pskIdentity;
        storage.psk = options.psk;
        storage.config.certPath = storage.certPath.empty() ? nullptr : storage.certPath.c_str();
        storage.config.keyPath = storage.keyPath.empty() ? nullptr : storage.keyPath.c_str();
        storage.config.caPath = storage.caPath.empty() ? nullptr : storage.caPath.c_str();
        storage.config.psk = storage.psk.empty() ? nullptr : storage.psk.data();
        storage.config.pskLen = storage.psk.size();
        storage.config.pskIdentity = storage.pskIdentity.empty() ? nullptr : storage.pskIdentity.c_str();
        storage.config.verifyPeer = options.verifyPeer;
        return storage;
    }

    struct SocketTuningOptions {
        uint32_t listenBacklog = 16;
        uint32_t recvBufferBytes = 0;
        uint32_t sendBufferBytes = 0;
        uint32_t readTimeoutMs = 0;
        uint32_t writeTimeoutMs = 0;
        bool noDelay = false;
        bool keepAlive = false;
    };

    inline void setSocketTimeout(SocketHandle s, int option, uint32_t timeoutMs) noexcept {
        if (!socketOk(s) || timeoutMs == 0) { return; }

        #ifdef _WIN32
        const DWORD value = timeoutMs; (void)::setsockopt(s, SOL_SOCKET, option, reinterpret_cast<const char*>(&value), sizeof(value));
        #else
        timeval value{};
        value.tv_sec = static_cast<time_t>(timeoutMs / 1000u);
        value.tv_usec = static_cast<suseconds_t>((timeoutMs % 1000u) * 1000u);
        (void)::setsockopt(s, SOL_SOCKET, option, &value, static_cast<socklen_t>(sizeof(value)));
        #endif
    }

    inline void applySocketTuning(SocketHandle s, const SocketTuningOptions& options, bool acceptedSocket) noexcept {
        if (!socketOk(s)) { return; }

        if (options.recvBufferBytes > 0) {
            const int value = static_cast<int>(options.recvBufferBytes);
            (void)::setsockopt(s, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<const char*>(&value), sizeof(value));
        }
        if (options.sendBufferBytes > 0) {
            const int value = static_cast<int>(options.sendBufferBytes);
            (void)::setsockopt(s, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<const char*>(&value), sizeof(value));
        }
        if (acceptedSocket && options.keepAlive) {
            const int value = 1;
            (void)::setsockopt(s, SOL_SOCKET, SO_KEEPALIVE, reinterpret_cast<const char*>(&value), sizeof(value));
        }
        if (acceptedSocket) {
            setSocketTimeout(s, SO_RCVTIMEO, options.readTimeoutMs);
            setSocketTimeout(s, SO_SNDTIMEO, options.writeTimeoutMs);
        }
        if (acceptedSocket && options.noDelay) {
            const int value = 1;
            (void)::setsockopt(s, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&value), sizeof(value));
        }
    }

    inline SocketTuningOptions makeSocketTuning(const AkkEngineOptions::ApiOptions& options) noexcept {
        return SocketTuningOptions{
            options.tcpListenBacklog,
            options.tcpRecvBufferBytes,
            options.tcpSendBufferBytes,
            options.tcpReadTimeoutMs,
            options.tcpWriteTimeoutMs,
            options.tcpNoDelay,
            options.tcpKeepAlive
        };
    }

    inline SocketHandle listenOn(
        const std::string& host,
        uint16_t port,
        const char* label,
        SocketTuningOptions tuning = SocketTuningOptions{}
    ) {
        netInit();

        addrinfo hints{};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_protocol = IPPROTO_TCP;
        hints.ai_flags = AI_PASSIVE;

        const std::string portString = std::to_string(port);
        addrinfo* results = nullptr;
        const int gai = ::getaddrinfo(host.c_str(), portString.c_str(), &hints, &results);
        if (gai != 0) { throw std::runtime_error(std::string(label) + ": getaddrinfo failed for " + host + ":" + portString); }

        SocketHandle out = BAD_SOCKET_VALUE;
        for (addrinfo* it = results; it != nullptr; it = it->ai_next) {
            SocketHandle s = ::socket(it->ai_family, it->ai_socktype, it->ai_protocol);
            if (!socketOk(s)) { continue; }

            int reuse = 1;
            ::setsockopt(s, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&reuse), sizeof(reuse));
            applySocketTuning(s, tuning, false);

            const int backlog = tuning.listenBacklog == 0 ? 16 : static_cast<int>(tuning.listenBacklog);
            if (::bind(s, it->ai_addr, static_cast<int>(it->ai_addrlen)) == 0 && ::listen(s, backlog) == 0) {
                out = s;
                break;
            }
            closeSocket(s);
        }
        ::freeaddrinfo(results);

        if (!socketOk(out)) { throw std::runtime_error(std::string(label) + ": bind/listen failed on " + host + ":" + portString); }
        return out;
    }

    inline bool sendAll(SocketHandle s, const uint8_t* data, size_t size) {
        size_t sent = 0;
        while (sent < size) {
            #ifdef _WIN32
            const int rc = ::send(s, reinterpret_cast<const char*>(data + sent), static_cast<int>(size - sent), 0); if (rc < 0 &&
                WSAGetLastError() == WSAEINTR) { continue; }
            #else
            const ssize_t rc = ::send(s, data + sent, size - sent, sendNoSigpipeFlags());
            if (rc < 0 && errno == EINTR) { continue; }
            #endif
            if (rc <= 0) { return false; }
            sent += static_cast<size_t>(rc);
        }
        return true;
    }

    inline bool recvAll(SocketHandle s, uint8_t* data, size_t size) {
        size_t got = 0;
        while (got < size) {
            #ifdef _WIN32
            const int rc = ::recv(s, reinterpret_cast<char*>(data + got), static_cast<int>(size - got), 0); if (rc < 0 && WSAGetLastError()
                == WSAEINTR) { continue; }
            #else
            const ssize_t rc = ::recv(s, data + got, size - got, 0);
            if (rc < 0 && errno == EINTR) { continue; }
            #endif
            if (rc <= 0) { return false; }
            got += static_cast<size_t>(rc);
        }
        return true;
    }

    inline size_t recvSome(SocketHandle s, uint8_t* data, size_t size) {
        for (;;) {
            #ifdef _WIN32
            const int rc = ::recv(s, reinterpret_cast<char*>(data), static_cast<int>(size), 0); if (rc < 0 && WSAGetLastError() ==
                WSAEINTR) { continue; }
            #else
            const ssize_t rc = ::recv(s, data, size, 0);
            if (rc < 0 && errno == EINTR) { continue; }
            #endif
            return rc > 0 ? static_cast<size_t>(rc) : 0;
        }
    }

    inline bool sendAll(net::TlsStream& stream, const uint8_t* data, size_t size) {
        size_t sent = 0;
        while (sent < size) { sent += stream.send(data + sent, size - sent); }
        return true;
    }

    inline bool recvAll(net::TlsStream& stream, uint8_t* data, size_t size) {
        size_t got = 0;
        while (got < size) { got += stream.recv(data + got, size - got); }
        return true;
    }

    inline size_t recvSome(net::TlsStream& stream, uint8_t* data, size_t size) { return stream.recv(data, size); }

    class Connection {
        public:
            explicit Connection(SocketHandle socket) : socket_{socket} {}

            Connection(const Connection&) = delete;
            Connection& operator=(const Connection&) = delete;

            ~Connection() { close(); }

            void enableTls(const AkkEngineOptions::ApiTlsOptions& options) {
                auto storage = makeTlsConfig(options);
                auto stream = std::make_unique<net::TlsStream>();
                const SocketHandle rawSocket = socket_;
                socket_ = BAD_SOCKET_VALUE;
                stream->accept(static_cast<std::uintptr_t>(rawSocket), storage.config);
                tls_ = std::move(stream);
            }

            [[nodiscard]] bool recvAll(uint8_t* data, size_t size) {
                try { return tls_ ? detail::recvAll(*tls_, data, size) : detail::recvAll(socket_, data, size); }
                catch (...) { return false; }
            }

            [[nodiscard]] size_t recvSome(uint8_t* data, size_t size) {
                try { return tls_ ? detail::recvSome(*tls_, data, size) : detail::recvSome(socket_, data, size); }
                catch (...) { return 0; }
            }

            [[nodiscard]] bool sendAll(const uint8_t* data, size_t size) {
                try { return tls_ ? detail::sendAll(*tls_, data, size) : detail::sendAll(socket_, data, size); }
                catch (...) { return false; }
            }

            void shutdown() noexcept {
                if (tls_) { tls_->shutdown(); }
                else { shutdownSocket(socket_); }
            }

            void close() noexcept {
                if (tls_) {
                    tls_->close();
                    tls_.reset();
                }
                closeSocket(socket_);
                socket_ = BAD_SOCKET_VALUE;
            }

        private:
            SocketHandle socket_ = BAD_SOCKET_VALUE;
            std::unique_ptr<net::TlsStream> tls_;
    };
}

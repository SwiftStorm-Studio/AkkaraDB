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
#include <sys/socket.h>
#include <unistd.h>
#endif

#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

namespace akkaradb::engine::server::detail {
    #ifdef _WIN32
    using socket_t = SOCKET; inline constexpr socket_t BAD_SOCKET_VALUE = INVALID_SOCKET; inline void net_init() {
        static std::once_flag flag;
        std::call_once(
            flag,
            [] {
                WSADATA data{};
                if (::WSAStartup(MAKEWORD(2, 2), &data) != 0) { throw std::runtime_error("ApiServer: WSAStartup failed"); }
            }
        );
    } inline bool socket_ok(socket_t s) noexcept { return s != INVALID_SOCKET; } inline void close_socket(socket_t s) noexcept {
        if (socket_ok(s)) { ::closesocket(s); }
    } inline void shutdown_socket(socket_t s) noexcept { if (socket_ok(s)) { ::shutdown(s, SD_BOTH); } }
    #else
    using socket_t = int;
    inline constexpr socket_t BAD_SOCKET_VALUE = -1;

    inline void net_init() {}
    inline bool socket_ok(socket_t s) noexcept { return s >= 0; }
    inline void close_socket(socket_t s) noexcept { if (socket_ok(s)) { ::close(s); } }
    inline void shutdown_socket(socket_t s) noexcept { if (socket_ok(s)) { ::shutdown(s, SHUT_RDWR); } }
    #endif

    struct TlsConfigStorage {
        std::string cert_path;
        std::string key_path;
        std::string ca_path;
        std::string psk_identity;
        std::vector<uint8_t> psk;
        net::TlsConfig config{};
    };

    inline TlsConfigStorage make_tls_config(const AkkEngineOptions::ApiTlsOptions& options) {
        TlsConfigStorage storage;
        storage.cert_path = options.cert_path.string();
        storage.key_path = options.key_path.string();
        storage.ca_path = options.ca_path.string();
        storage.psk_identity = options.psk_identity;
        storage.psk = options.psk;
        storage.config.cert_path = storage.cert_path.empty() ? nullptr : storage.cert_path.c_str();
        storage.config.key_path = storage.key_path.empty() ? nullptr : storage.key_path.c_str();
        storage.config.ca_path = storage.ca_path.empty() ? nullptr : storage.ca_path.c_str();
        storage.config.psk = storage.psk.empty() ? nullptr : storage.psk.data();
        storage.config.psk_len = storage.psk.size();
        storage.config.psk_identity = storage.psk_identity.empty() ? nullptr : storage.psk_identity.c_str();
        storage.config.verify_peer = options.verify_peer;
        return storage;
    }

    inline socket_t listen_on(const std::string& host, uint16_t port, const char* label) {
        net_init();

        addrinfo hints{};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_protocol = IPPROTO_TCP;
        hints.ai_flags = AI_PASSIVE;

        const std::string port_string = std::to_string(port);
        addrinfo* results = nullptr;
        const int gai = ::getaddrinfo(host.c_str(), port_string.c_str(), &hints, &results);
        if (gai != 0) { throw std::runtime_error(std::string(label) + ": getaddrinfo failed for " + host + ":" + port_string); }

        socket_t out = BAD_SOCKET_VALUE;
        for (addrinfo* it = results; it != nullptr; it = it->ai_next) {
            socket_t s = ::socket(it->ai_family, it->ai_socktype, it->ai_protocol);
            if (!socket_ok(s)) { continue; }

            int reuse = 1;
            ::setsockopt(s, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&reuse), sizeof(reuse));

            if (::bind(s, it->ai_addr, static_cast<int>(it->ai_addrlen)) == 0 && ::listen(s, 16) == 0) {
                out = s;
                break;
            }
            close_socket(s);
        }
        ::freeaddrinfo(results);

        if (!socket_ok(out)) { throw std::runtime_error(std::string(label) + ": bind/listen failed on " + host + ":" + port_string); }
        return out;
    }

    inline bool send_all(socket_t s, const uint8_t* data, size_t size) {
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

    inline bool recv_all(socket_t s, uint8_t* data, size_t size) {
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

    inline size_t recv_some(socket_t s, uint8_t* data, size_t size) {
        #ifdef _WIN32
        const int rc = ::recv(s, reinterpret_cast<char*>(data), static_cast<int>(size), 0);
        #else
        const ssize_t rc = ::recv(s, data, size, 0);
        #endif
        return rc > 0 ? static_cast<size_t>(rc) : 0;
    }

    inline bool send_all(net::TlsStream& stream, const uint8_t* data, size_t size) {
        size_t sent = 0;
        while (sent < size) { sent += stream.send(data + sent, size - sent); }
        return true;
    }

    inline bool recv_all(net::TlsStream& stream, uint8_t* data, size_t size) {
        size_t got = 0;
        while (got < size) { got += stream.recv(data + got, size - got); }
        return true;
    }

    inline size_t recv_some(net::TlsStream& stream, uint8_t* data, size_t size) { return stream.recv(data, size); }

    class Connection {
        public:
            explicit Connection(socket_t socket) : socket_{socket} {}

            Connection(const Connection&) = delete;
            Connection& operator=(const Connection&) = delete;

            ~Connection() { close(); }

            void enable_tls(const AkkEngineOptions::ApiTlsOptions& options) {
                auto storage = make_tls_config(options);
                tls_ = std::make_unique<net::TlsStream>();
                tls_->accept(static_cast<std::uintptr_t>(socket_), storage.config);
                socket_ = BAD_SOCKET_VALUE;
            }

            [[nodiscard]] bool recv_all(uint8_t* data, size_t size) {
                try { return tls_ ? detail::recv_all(*tls_, data, size) : detail::recv_all(socket_, data, size); }
                catch (...) { return false; }
            }

            [[nodiscard]] size_t recv_some(uint8_t* data, size_t size) {
                try { return tls_ ? detail::recv_some(*tls_, data, size) : detail::recv_some(socket_, data, size); }
                catch (...) { return 0; }
            }

            [[nodiscard]] bool send_all(const uint8_t* data, size_t size) {
                try { return tls_ ? detail::send_all(*tls_, data, size) : detail::send_all(socket_, data, size); }
                catch (...) { return false; }
            }

            void shutdown() noexcept {
                if (tls_) { tls_->shutdown(); }
                else { shutdown_socket(socket_); }
            }

            void close() noexcept {
                if (tls_) {
                    tls_->close();
                    tls_.reset();
                }
                close_socket(socket_);
                socket_ = BAD_SOCKET_VALUE;
            }

        private:
            socket_t socket_ = BAD_SOCKET_VALUE;
            std::unique_ptr<net::TlsStream> tls_;
    };
}

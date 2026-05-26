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

// akkengine/src/platform/socket/SocketWindows.cpp
#include "akk/platform/socket/Socket.hpp"

#if defined(_WIN32)

#include <winsock2.h>
#include <ws2tcpip.h>

#include <algorithm>
#include <climits>
#include <cstdio>
#include <cstdint>
#include <mutex>
#include <stdexcept>
#include <string>
#include <system_error>

#pragma comment(lib, "Ws2_32.lib")

namespace akkaradb::platform {
    namespace {
        using native_handle_t = std::uintptr_t;

        /**
         * @brief Returns the invalid socket sentinel.
         *
         * @return Sentinel value representing an invalid socket.
         */
        [[nodiscard]] native_handle_t invalid_handle() noexcept { return INVALID_SOCKET; }

        /**
         * @brief Converts the native handle to a SOCKET.
         *
         * @param handle Native socket handle.
         * @return SOCKET value.
         */
        [[nodiscard]] SOCKET to_socket(native_handle_t handle) noexcept { return handle; }

        /**
         * @brief Converts a SOCKET to the native handle type.
         *
         * @param s SOCKET value.
         * @return Native socket handle.
         */
        [[nodiscard]] native_handle_t from_socket(SOCKET s) noexcept { return s; }

        /**
         * @brief Ensures Winsock is initialized exactly once.
         *
         * @throws std::runtime_error if WSAStartup fails.
         */
        void ensure_wsa() {
            static std::once_flag once;
            std::call_once(
                once,
                [] {
                    WSADATA wsa{};
                    const int rc = WSAStartup(MAKEWORD(2, 2), &wsa);
                    if (rc != 0) { throw std::runtime_error("WSAStartup failed: " + std::to_string(rc)); }
                }
            );
        }

        /**
         * @brief Sets or clears non-blocking mode.
         *
         * @param s Socket descriptor.
         * @param enabled true to enable non-blocking mode.
         * @return true on success.
         */
        [[nodiscard]] bool set_nonblocking(SOCKET s, bool enabled) noexcept {
            u_long mode = enabled ? 1UL : 0UL;
            return ioctlsocket(s, FIONBIO, &mode) == 0;
        }

        /**
         * @brief Waits until a non-blocking connect completes.
         *
         * @param s Socket descriptor.
         * @return 0 on success, otherwise a Winsock error code.
         */
        [[nodiscard]] int wait_connect_complete(SOCKET s) noexcept {
            for (;;) {
                fd_set wfds;
                fd_set efds;
                FD_ZERO(&wfds);
                FD_ZERO(&efds);
                FD_SET(s, &wfds);
                FD_SET(s, &efds);

                const int rc = select(0, nullptr, &wfds, &efds, nullptr);
                if (rc == SOCKET_ERROR) {
                    const int err = WSAGetLastError();
                    if (err == WSAEINTR) { continue; }
                    return err;
                }

                int so_error = 0;
                int len = sizeof(so_error);
                if (getsockopt(s, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&so_error), &len) != 0) { return WSAGetLastError(); }

                return so_error;
            }
        }

        /**
         * @brief Returns the portable would-block error code.
         *
         * @return operation_would_block.
         */
        [[nodiscard]] std::error_code would_block() noexcept { return std::make_error_code(std::errc::operation_would_block); }

        /**
         * @brief Builds a detailed connect failure.
         *
         * @param host Host name or address.
         * @param port Port number.
         * @param err Winsock or resolver error code.
         * @return Exception object.
         */
        [[nodiscard]] std::system_error make_connect_error(const char* host, uint16_t port, int err) {
            std::string message = "Socket::connect(";
            message += (host != nullptr) ? host : "(null)";
            message += ':';
            message += std::to_string(port);
            message += ") failed";
            return {err, std::system_category(), message};
        }
    } // namespace

    Socket::Socket() noexcept : handle_(invalid_handle()) {}

    Socket::~Socket() { close(); }

    Socket::Socket(Socket&& other) noexcept : handle_(other.handle_) { other.handle_ = invalid_handle(); }

    Socket& Socket::operator=(Socket&& other) noexcept {
        if (this != &other) {
            close();
            handle_ = other.handle_;
            other.handle_ = invalid_handle();
        }
        return *this;
    }

    bool Socket::valid() const noexcept { return handle_ != invalid_handle(); }

    void Socket::close() noexcept {
        if (valid()) {
            closesocket(to_socket(handle_));
            handle_ = invalid_handle();
        }
    }

    /**
     * @brief Resolves a host and establishes a TCP connection.
     *
     * The socket is configured as non-blocking for connect and remains
     * non-blocking after this function returns.
     *
     * @param host Host name or numeric address.
     * @param port TCP port.
     * @return Connected socket.
     * @throws std::invalid_argument if host is null.
     * @throws std::runtime_error on resolver failure.
     * @throws std::system_error on socket or connect failure.
     */
    Socket Socket::connect(const char* host, uint16_t port) {
        if (host == nullptr) { throw std::invalid_argument("Socket::connect: host is null"); }

        ensure_wsa();

        addrinfo hints{};
        addrinfo* result = nullptr;

        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_protocol = IPPROTO_TCP;
#if defined(AI_ADDRCONFIG)
hints.ai_flags= AI_ADDRCONFIG;
#endif

char port_str[6]; const int port_len = std::snprintf(port_str, sizeof(port_str), "%u", static_cast<unsigned>(port));if (port_len<0 || port_len >= static_cast<
    int>(sizeof(port_str))) { throw std::invalid_argument("Socket::connect: invalid port"); } const int gai = getaddrinfo(host, port_str, &hints, &result);if (
    gai!= 0) {
            std::string message = "Socket::connect(";
            message += host;
            message += ':';
            message += port_str;
            message += ") getaddrinfo failed: ";
            message += gai_strerrorA(gai);
            throw std::runtime_error(message);
        }

native_handle_t connected = invalid_handle(); int last_error = 0;for (auto* rp = result; rp!= nullptr; rp= rp->ai_next) {
            const SOCKET s = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
            if (s == INVALID_SOCKET) {
                last_error = WSAGetLastError();
                continue;
            }

            if (!set_nonblocking(s, true)) {
                last_error = WSAGetLastError();
                closesocket(s);
                continue;
            }

            const int rc = ::connect(s, rp->ai_addr, static_cast<int>(rp->ai_addrlen));
            if (rc == 0) {
                connected = from_socket(s);
                break;
            }

            const int err = WSAGetLastError();
            if (err == WSAEWOULDBLOCK || err == WSAEINPROGRESS || err == WSAEALREADY) {
                const int wait_rc = wait_connect_complete(s);
                if (wait_rc == 0) {
                    connected = from_socket(s);
                    break;
                }

                last_error = wait_rc;
                closesocket(s);
                continue;
            }

            last_error = err;
            closesocket(s);
        }

freeaddrinfo (result);if (connected== invalid_handle()) {
            if (last_error != 0) { throw make_connect_error(host, port, last_error); }
            throw std::runtime_error("Socket::connect failed");
        }

Socket sock; sock.handle_= connected;return sock;}

/**
     * @brief Sends up to @p size bytes.
     *
     * @param data Input buffer.
     * @param size Number of bytes to send.
     * @param out_sent Number of bytes actually sent.
     * @return Empty error_code on success, or a portable error on failure.
     */
std::error_code Socket::send_some(const void* data, std::size_t size, std::size_t& out_sent) noexcept {
    out_sent = 0;

    if (!valid()) { return std::make_error_code(std::errc::bad_file_descriptor); }

    if (size == 0) { return {}; }

    if (data == nullptr) { return std::make_error_code(std::errc::invalid_argument); }

    const auto* ptr = static_cast<const char*>(data);
    const std::size_t chunk_size = std::min<std::size_t>(size, INT_MAX);

    for (;;) {
        const int n = send(to_socket(handle_), ptr, static_cast<int>(chunk_size), 0);

        if (n >= 0) {
            out_sent = static_cast<std::size_t>(n);
            return {};
        }

        const int err = WSAGetLastError();
        if (err == WSAEINTR) { continue; }
        if (err == WSAEWOULDBLOCK) { return would_block(); }
        if (err == WSAECONNRESET || err == WSAECONNABORTED || err == WSAENOTCONN) { return std::make_error_code(std::errc::connection_reset); }

        return {err, std::system_category()};
    }
}

/**
     * @brief Receives up to @p size bytes.
     *
     * @param data Output buffer.
     * @param size Maximum number of bytes to receive.
     * @param out_recv Number of bytes actually received.
     * @return Empty error_code on success, or a portable error on failure.
     */
std::error_code Socket::recv_some(void* data, std::size_t size, std::size_t& out_recv) noexcept {
    out_recv = 0;

    if (!valid()) { return std::make_error_code(std::errc::bad_file_descriptor); }

    if (size == 0) { return {}; }

    if (data == nullptr) { return std::make_error_code(std::errc::invalid_argument); }

    auto* ptr = static_cast<char*>(data);
    const std::size_t chunk_size = std::min<std::size_t>(size, INT_MAX);

    for (;;) {
        const int n = recv(to_socket(handle_), ptr, static_cast<int>(chunk_size), 0);

        if (n > 0) {
            out_recv = static_cast<std::size_t>(n);
            return {};
        }

        if (n == 0) { return std::make_error_code(std::errc::connection_reset); }

        const int err = WSAGetLastError();
        if (err == WSAEINTR) { continue; }
        if (err == WSAEWOULDBLOCK) { return would_block(); }
        if (err == WSAECONNRESET || err == WSAECONNABORTED || err == WSAENOTCONN) { return std::make_error_code(std::errc::connection_reset); }

        return {err, std::system_category()};
    }
}} // namespace akkaradb::platform

#endif

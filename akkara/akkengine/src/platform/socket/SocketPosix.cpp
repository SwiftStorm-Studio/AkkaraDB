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

// akkengine/src/platform/socket/SocketPosix.cpp
#include "akk/platform/socket/Socket.hpp"

#if !defined(_WIN32)

#include <arpa/inet.h>
#include <cerrno>
#include <cstdio>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <stdexcept>
#include <string>
#include <string.h>
#include <system_error>
#include <sys/socket.h>
#include <unistd.h>

namespace akkaradb::platform {
    namespace {
        /**
         * @brief Returns the invalid native socket handle.
         *
         * @return Sentinel value that represents an invalid socket.
         */
        [[nodiscard]] static native_handle_t invalid_handle() noexcept { return static_cast<native_handle_t>(~static_cast<native_handle_t>(0)); }

        /**
         * @brief Converts a native handle to a POSIX file descriptor.
         *
         * @param handle Native socket handle.
         * @return POSIX file descriptor.
         */
        [[nodiscard]] static int to_fd(native_handle_t handle) noexcept { return static_cast<int>(handle); }

        /**
         * @brief Converts a POSIX file descriptor to a native handle.
         *
         * @param fd POSIX file descriptor.
         * @return Native socket handle.
         */
        [[nodiscard]] static native_handle_t from_fd(int fd) noexcept { return static_cast<native_handle_t>(fd); }

        /**
         * @brief Sets or clears O_NONBLOCK on a file descriptor.
         *
         * @param fd File descriptor.
         * @param enabled true to enable non-blocking mode, false to disable it.
         * @return true on success.
         */
        [[nodiscard]] static bool set_nonblocking(int fd, bool enabled) noexcept {
            int flags = ::fcntl(fd, F_GETFL, 0);
            if (flags < 0) { return false; }

            if (enabled) { flags |= O_NONBLOCK; }
            else { flags &= ~O_NONBLOCK; }

            return ::fcntl(fd, F_SETFL, flags) == 0;
        }

        /**
         * @brief Sets FD_CLOEXEC on a file descriptor.
         *
         * @param fd File descriptor.
         * @return true on success.
         */
        [[nodiscard]] static bool set_close_on_exec(int fd) noexcept {
            int flags = ::fcntl(fd, F_GETFD, 0);
            if (flags < 0) { return false; }

            return ::fcntl(fd, F_SETFD, flags | FD_CLOEXEC) == 0;
        }

        /**
         * @brief Waits until a non-blocking connect completes.
         *
         * @param fd Connected socket candidate.
         * @return 0 on success, otherwise an errno-compatible error value.
         */
        [[nodiscard]] static int wait_connect_complete(int fd) noexcept {
            for (;;) {
                struct pollfd pfd{};
                pfd.fd = fd;
                pfd.events = POLLOUT;

                const int rc = ::poll(&pfd, 1, -1);
                if (rc < 0) {
                    if (errno == EINTR) { continue; }
                    return errno;
                }

                int so_error = 0;
                socklen_t len = static_cast<socklen_t>(sizeof(so_error));
                if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_error, &len) < 0) { return errno; }

                return so_error;
            }
        }

        /**
         * @brief Returns a POSIX would-block error code.
         *
         * @return operation_would_block.
         */
        [[nodiscard]] static std::error_code would_block() noexcept { return std::make_error_code(std::errc::operation_would_block); }

        /**
         * @brief Returns the platform send flags.
         *
         * @return MSG_NOSIGNAL when available, otherwise 0.
         */
        [[nodiscard]] static int send_flags() noexcept {
            #if defined(MSG_NOSIGNAL)
            return MSG_NOSIGNAL;
            #else
            return 0;
            #endif
        }

        /**
 * @brief Formats a connect failure as an exception.
 *
 * @param host Host name or address string.
 * @param port Port number.
 * @param err Error code.
 * @return Exception object.
 */
        [[nodiscard]] static std::system_error make_connect_error(const char* host, uint16_t port, int err) {
            std::string message = "Socket::connect(";
            message += (host != nullptr) ? host : "(null)";
            message += ':';
            message += std::to_string(port);
            message += ") failed";
            return std::system_error(err, std::generic_category(), message);
        }
    } // namespace

    /**
 * @brief Constructs an invalid socket.
 */
    Socket::Socket() noexcept : handle_(invalid_handle()) {}

    /**
 * @brief Destroys the socket and closes the underlying handle.
 */
    Socket::~Socket() { close(); }

    /**
 * @brief Move-constructs a socket.
 *
 * @param other Source socket.
 */
    Socket::Socket(Socket&& other) noexcept : handle_(other.handle_) { other.handle_ = invalid_handle(); }

    /**
 * @brief Move-assigns a socket.
 *
 * @param other Source socket.
 * @return This socket.
 */
    Socket& Socket::operator=(Socket&& other) noexcept {
        if (this != &other) {
            close();
            handle_ = other.handle_;
            other.handle_ = invalid_handle();
        }
        return *this;
    }

    /**
 * @brief Checks whether the socket is valid.
 *
 * @return true when the socket owns a live file descriptor.
 */
    bool Socket::valid() const noexcept { return handle_ != invalid_handle(); }

    /**
 * @brief Closes the socket if it is valid.
 */
    void Socket::close() noexcept {
        if (valid()) {
            ::close(to_fd(handle_));
            handle_ = invalid_handle();
        }
    }

    /**
 * @brief Resolves host and establishes a TCP connection.
 *
 * The socket is configured as non-blocking for connect and remains
 * non-blocking after this function returns.
 *
 * @param host Host name or numeric address.
 * @param port TCP port.
 * @return Connected socket.
 * @throws std::runtime_error on DNS failure.
 * @throws std::system_error on socket or connect failure.
 */
    Socket Socket::connect(const char* host, uint16_t port) {
        if (host == nullptr) { throw std::invalid_argument("Socket::connect: host is null"); }

        struct addrinfo hints{};
        struct addrinfo* result = nullptr;

        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_protocol = IPPROTO_TCP;
        #if defined(AI_ADDRCONFIG)
        hints.ai_flags = AI_ADDRCONFIG;
        #endif

        char port_str[6];
        const int port_len = std::snprintf(port_str, sizeof(port_str), "%u", static_cast<unsigned>(port));
        if (port_len < 0 || port_len >= static_cast<int>(sizeof(port_str))) { throw std::invalid_argument("Socket::connect: invalid port"); }
        const int gai = ::getaddrinfo(host, port_str, &hints, &result);
        if (gai != 0) {
            std::string message = "Socket::connect(";
            message += host;
            message += ':';
            message += port_str;
            message += ") getaddrinfo failed: ";
            message += ::gai_strerror(gai);
            throw std::runtime_error(message);
        }

        native_handle_t connected = invalid_handle();
        int last_error = 0;
        for (struct addrinfo* rp = result; rp != nullptr; rp = rp->ai_next) {
            const int fd = ::socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
            if (fd < 0) {
                last_error = errno;
                continue;
            }

            if (!set_close_on_exec(fd)) {
                last_error = errno;
                ::close(fd);
                continue;
            }

            if (!set_nonblocking(fd, true)) {
                last_error = errno;
                ::close(fd);
                continue;
            }

            #if defined(SO_NOSIGPIPE)
            {
                int one = 1;
                (void)::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, static_cast<socklen_t>(sizeof(one)));
            }
            #endif

            const int rc = ::connect(fd, rp->ai_addr, rp->ai_addrlen);
            if (rc == 0) {
                connected = from_fd(fd);
                break;
            }

            if (errno == EINPROGRESS || errno == EALREADY || errno == EWOULDBLOCK) {
                const int wait_rc = wait_connect_complete(fd);
                if (wait_rc == 0) {
                    connected = from_fd(fd);
                    break;
                }

                last_error = wait_rc;
                ::close(fd);
                continue;
            }

            last_error = errno;
            ::close(fd);
        }

        ::freeaddrinfo(result);
        if (connected == invalid_handle()) {
            if (last_error != 0) { throw make_connect_error(host, port, last_error); }
            throw std::runtime_error("Socket::connect failed");
        }

        Socket sock;
        sock.handle_ = connected;
        return sock;
    }

    /**
 * @brief Sends bytes on the socket.
 *
 * @param data Buffer to send.
 * @param size Number of bytes to send.
 * @param out_sent Number of bytes actually sent.
 * @return Empty error_code on success, would-block or a POSIX error otherwise.
 */
    std::error_code Socket::send_some(const void* data, std::size_t size, std::size_t& out_sent) noexcept {
        out_sent = 0;

        if (!valid()) { return std::make_error_code(std::errc::bad_file_descriptor); }

        if (size == 0) { return {}; }

        if (data == nullptr) { return std::make_error_code(std::errc::invalid_argument); }

        const auto* ptr = static_cast<const unsigned char*>(data);

        for (;;) {
            const ssize_t n = ::send(to_fd(handle_), ptr, size, send_flags());
            if (n >= 0) {
                out_sent = static_cast<std::size_t>(n);
                return {};
            }

            if (errno == EINTR) { continue; }

            if (errno == EAGAIN || errno == EWOULDBLOCK) { return would_block(); }

            if (errno == EPIPE) { return std::make_error_code(std::errc::broken_pipe); }

            return std::error_code(errno, std::generic_category());
        }
    }

    /**
 * @brief Receives bytes from the socket.
 *
 * @param data Destination buffer.
 * @param size Maximum number of bytes to receive.
 * @param out_recv Number of bytes actually received.
 * @return Empty error_code on success, would-block or a POSIX error otherwise.
 */
    std::error_code Socket::recv_some(void* data, std::size_t size, std::size_t& out_recv) noexcept {
        out_recv = 0;

        if (!valid()) { return std::make_error_code(std::errc::bad_file_descriptor); }

        if (size == 0) { return {}; }

        if (data == nullptr) { return std::make_error_code(std::errc::invalid_argument); }

        auto* ptr = static_cast<unsigned char*>(data);

        for (;;) {
            const ssize_t n = ::recv(to_fd(handle_), ptr, size, 0);

            if (n > 0) {
                out_recv = static_cast<std::size_t>(n);
                return {};
            }

            if (n == 0) { return std::make_error_code(std::errc::connection_reset); }

            if (errno == EINTR) { continue; }

            if (errno == EAGAIN || errno == EWOULDBLOCK) { return would_block(); }

            return std::error_code(errno, std::generic_category());
        }
    }
} // namespace akkaradb::platform

#endif

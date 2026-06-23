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
        [[nodiscard]] static NativeHandle invalidHandle() noexcept { return static_cast<NativeHandle>(~static_cast<NativeHandle>(0)); }

        /**
         * @brief Converts a native handle to a POSIX file descriptor.
         *
         * @param handle Native socket handle.
         * @return POSIX file descriptor.
         */
        [[nodiscard]] static int toFd(NativeHandle handle) noexcept { return static_cast<int>(handle); }

        /**
         * @brief Converts a POSIX file descriptor to a native handle.
         *
         * @param fd POSIX file descriptor.
         * @return Native socket handle.
         */
        [[nodiscard]] static NativeHandle fromFd(int fd) noexcept { return static_cast<NativeHandle>(fd); }

        /**
         * @brief Sets or clears O_NONBLOCK on a file descriptor.
         *
         * @param fd File descriptor.
         * @param enabled true to enable non-blocking mode, false to disable it.
         * @return true on success.
         */
        [[nodiscard]] static bool setNonblocking(int fd, bool enabled) noexcept {
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
        [[nodiscard]] static bool setCloseOnExec(int fd) noexcept {
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
        [[nodiscard]] static int waitConnectComplete(int fd) noexcept {
            for (;;) {
                struct pollfd pfd{};
                pfd.fd = fd;
                pfd.events = POLLOUT;

                const int rc = ::poll(&pfd, 1, -1);
                if (rc < 0) {
                    if (errno == EINTR) { continue; }
                    return errno;
                }

                int soError = 0;
                socklen_t len = static_cast<socklen_t>(sizeof(soError));
                if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &soError, &len) < 0) { return errno; }

                return soError;
            }
        }

        /**
         * @brief Returns a POSIX would-block error code.
         *
         * @return operationWouldBlock.
         */
        [[nodiscard]] static std::error_code wouldBlock() noexcept { return std::make_error_code(std::errc::operation_would_block); }

        /**
         * @brief Returns the platform send flags.
         *
         * @return MSG_NOSIGNAL when available, otherwise 0.
         */
        [[nodiscard]] static int sendFlags() noexcept {
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
        [[nodiscard]] static std::system_error makeConnectError(const char* host, uint16_t port, int err) {
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
    Socket::Socket() noexcept : handle_(invalidHandle()) {}

    /**
* @brief Destroys the socket and closes the underlying handle.
*/
    Socket::~Socket() { close(); }

    /**
* @brief Move-constructs a socket.
*
* @param other Source socket.
*/
    Socket::Socket(Socket&& other) noexcept : handle_(other.handle_) { other.handle_ = invalidHandle(); }

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
            other.handle_ = invalidHandle();
        }
        return *this;
    }

    /**
* @brief Checks whether the socket is valid.
*
* @return true when the socket owns a live file descriptor.
*/
    bool Socket::valid() const noexcept { return handle_ != invalidHandle(); }

    /**
* @brief Closes the socket if it is valid.
*/
    void Socket::close() noexcept {
        if (valid()) {
            ::close(toFd(handle_));
            handle_ = invalidHandle();
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

        char portStr[6];
        const int portLen = std::snprintf(portStr, sizeof(portStr), "%u", static_cast<unsigned>(port));
        if (portLen < 0 || portLen >= static_cast<int>(sizeof(portStr))) { throw std::invalid_argument("Socket::connect: invalid port"); }
        const int gai = ::getaddrinfo(host, portStr, &hints, &result);
        if (gai != 0) {
            std::string message = "Socket::connect(";
            message += host;
            message += ':';
            message += portStr;
            message += ") getaddrinfo failed: ";
            message += ::gai_strerror(gai);
            throw std::runtime_error(message);
        }

        NativeHandle connected = invalidHandle();
        int lastError = 0;
        for (struct addrinfo* rp = result; rp != nullptr; rp = rp->ai_next) {
            const int fd = ::socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
            if (fd < 0) {
                lastError = errno;
                continue;
            }

            if (!setCloseOnExec(fd)) {
                lastError = errno;
                ::close(fd);
                continue;
            }

            if (!setNonblocking(fd, true)) {
                lastError = errno;
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
                connected = fromFd(fd);
                break;
            }

            if (errno == EINPROGRESS || errno == EALREADY || errno == EWOULDBLOCK) {
                const int waitRc = waitConnectComplete(fd);
                if (waitRc == 0) {
                    connected = fromFd(fd);
                    break;
                }

                lastError = waitRc;
                ::close(fd);
                continue;
            }

            lastError = errno;
            ::close(fd);
        }

        ::freeaddrinfo(result);
        if (connected == invalidHandle()) {
            if (lastError != 0) { throw makeConnectError(host, port, lastError); }
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
* @param outSent Number of bytes actually sent.
* @return Empty error_code on success, would-block or a POSIX error otherwise.
*/
    std::error_code Socket::sendSome(const void* data, std::size_t size, std::size_t& outSent) noexcept {
        outSent = 0;

        if (!valid()) { return std::make_error_code(std::errc::bad_file_descriptor); }

        if (size == 0) { return {}; }

        if (data == nullptr) { return std::make_error_code(std::errc::invalid_argument); }

        const auto* ptr = static_cast<const unsigned char*>(data);

        for (;;) {
            const ssize_t n = ::send(toFd(handle_), ptr, size, sendFlags());
            if (n >= 0) {
                outSent = static_cast<std::size_t>(n);
                return {};
            }

            if (errno == EINTR) { continue; }

            if (errno == EAGAIN || errno == EWOULDBLOCK) { return wouldBlock(); }

            if (errno == EPIPE) { return std::make_error_code(std::errc::broken_pipe); }

            return std::error_code(errno, std::generic_category());
        }
    }

    /**
* @brief Receives bytes from the socket.
*
* @param data Destination buffer.
* @param size Maximum number of bytes to receive.
* @param outRecv Number of bytes actually received.
* @return Empty error_code on success, would-block or a POSIX error otherwise.
*/
    std::error_code Socket::recvSome(void* data, std::size_t size, std::size_t& outRecv) noexcept {
        outRecv = 0;

        if (!valid()) { return std::make_error_code(std::errc::bad_file_descriptor); }

        if (size == 0) { return {}; }

        if (data == nullptr) { return std::make_error_code(std::errc::invalid_argument); }

        auto* ptr = static_cast<unsigned char*>(data);

        for (;;) {
            const ssize_t n = ::recv(toFd(handle_), ptr, size, 0);

            if (n > 0) {
                outRecv = static_cast<std::size_t>(n);
                return {};
            }

            if (n == 0) { return std::make_error_code(std::errc::connection_reset); }

            if (errno == EINTR) { continue; }

            if (errno == EAGAIN || errno == EWOULDBLOCK) { return wouldBlock(); }

            return std::error_code(errno, std::generic_category());
        }
    }
} // namespace akkaradb::platform

#endif

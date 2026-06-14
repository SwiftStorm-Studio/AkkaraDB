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

// akkengine/include/akk/platform/socket/Socket.hpp
#pragma once

#include "akkaradb/Export.hpp"

#include <cstddef>
#include <cstdint>
#include <system_error>

namespace akkaradb::platform {
    /**
     * @brief Minimal TCP socket wrapper.
     *
     * The socket is configured for non-blocking I/O after connection establishment.
     * sendSome() and recvSome() never block; they report would-block via std::error_code.
     */
    class AKDB_API Socket {
        public:
            /**
             * @brief Construct an invalid socket.
             */
            Socket() noexcept;

            /**
             * @brief Destroy the socket and close the underlying handle if needed.
             */
            ~Socket();

            Socket(Socket&& other) noexcept;
            Socket& operator=(Socket&& other) noexcept;

            Socket(const Socket&) = delete;
            Socket& operator=(const Socket&) = delete;

            /**
             * @brief Create a TCP connection and leave the socket in non-blocking mode.
             *
             * @param host Hostname or IP address.
             * @param port TCP port.
             * @return Connected socket.
             *
             * @throws std::runtime_error on failure.
             */
            static Socket connect(const char* host, uint16_t port);

            /**
             * @brief Check whether the socket is valid.
             */
            [[nodiscard]] bool valid() const noexcept;

            /**
             * @brief Close the socket.
             */
            void close() noexcept;

            /**
             * @brief Send up to @p size bytes without blocking.
             *
             * @param data Input buffer.
             * @param size Maximum number of bytes to send.
             * @param outSent Number of bytes sent on success.
             * @return std::error_code{} on success, operationWouldBlock if the socket would block.
             */
            std::error_code sendSome(const void* data, std::size_t size, std::size_t& outSent) noexcept;

            /**
             * @brief Receive up to @p size bytes without blocking.
             *
             * @param data Output buffer.
             * @param size Maximum number of bytes to receive.
             * @param outRecv Number of bytes received on success.
             * @return std::error_code{} on success, operationWouldBlock if the socket would block.
             */
            std::error_code recvSome(void* data, std::size_t size, std::size_t& outRecv) noexcept;

        private:
            typedef std::uintptr_t NativeHandle;

            NativeHandle handle_;
    };
} //akkaradb::socket

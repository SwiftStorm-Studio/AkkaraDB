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

// akkengine/include/akk/net/tls/TlsStream.hpp
#pragma once

#include <cstddef>
#include <cstdint>

namespace akkaradb::net {
    struct TlsConfig {
        const char* certPath = nullptr;
        const char* keyPath = nullptr;
        const char* caPath = nullptr;
        const unsigned char* psk = nullptr;
        std::size_t pskLen = 0;
        const char* pskIdentity = nullptr;
        bool verifyPeer = true;
    };

    /**
     * @brief TLS stream over a TCP socket.
     *
     * Provides blocking TLS communication over a connected socket.
     * TLS implementation details are hidden in the source file.
     */
    class TlsStream {
        public:
            TlsStream() = default;
            ~TlsStream();

            TlsStream(TlsStream&&) noexcept;
            TlsStream& operator=(TlsStream&&) noexcept;

            TlsStream(const TlsStream&) = delete;
            TlsStream& operator=(const TlsStream&) = delete;

            /**
             * @brief Establish a TLS connection.
             *
             * @param host Hostname
             * @param port Port
             *
             * @throws std::runtime_error on failure
             */
            void connect(const char* host, uint16_t port, const TlsConfig& config = {});

            void connect(const char* host, uint16_t port, const TlsConfig& config, uint32_t readTimeoutMs, uint32_t writeTimeoutMs);

            /**
             * @brief Adopt an accepted TCP socket and complete a server-side TLS handshake.
             *
             * The stream owns @p nativeSocket after this call starts, even if the handshake fails.
             */
            void accept(std::uintptr_t nativeSocket, const TlsConfig& config = {});

            /**
             * @brief Send data over TLS.
             *
             * @return Number of bytes sent
             */
            std::size_t send(const void* data, std::size_t size);

            /**
             * @brief Receive data over TLS.
             *
             * @return Number of bytes received
             */
            std::size_t recv(void* data, std::size_t size);

            /**
             * @brief Close the connection.
             */
            void close() noexcept;

            /**
             * @brief Shutdown the underlying TCP socket without destroying TLS state.
             *
             * This is used to unblock another thread currently waiting in recv().
             */
            void shutdown() noexcept;

            /**
             * @brief Check if stream is connected.
             */
            [[nodiscard]] bool valid() const noexcept;

        private:
            // Opaque TLS state (implementation-specific)
            struct Impl;
            Impl* impl_ = nullptr;

            void setup(const TlsConfig& config, int endpoint, const char* hostname);
    };
} //akkaradb::net

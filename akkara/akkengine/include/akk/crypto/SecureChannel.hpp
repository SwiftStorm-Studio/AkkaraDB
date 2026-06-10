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

// akkengine/include/akk/crypto/SecureChannel.hpp
#pragma once

#include "akk/crypto/Identity.hpp"

#include <array>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

namespace akkaradb::crypto {
    using BytesView = std::span<const std::uint8_t>;
    using AeadTag = std::array<std::uint8_t, 16>;

    struct ClientHello {
        PublicKey static_public_key{};
        PublicKey ephemeral_public_key{};
    };

    struct ServerHello {
        PublicKey static_public_key{};
        PublicKey ephemeral_public_key{};
        AeadTag authenticator{};
    };

    struct EncryptedFrame {
        std::uint64_t counter = 0;
        AeadTag tag{};
        std::vector<std::uint8_t> ciphertext;
    };

    class SecureSession {
        public:
            SecureSession() = default;
            ~SecureSession();

            SecureSession(SecureSession&& other) noexcept;
            SecureSession& operator=(SecureSession&& other) noexcept;

            SecureSession(const SecureSession&) = delete;
            SecureSession& operator=(const SecureSession&) = delete;

            [[nodiscard]] bool valid() const noexcept { return valid_; }

            [[nodiscard]] EncryptedFrame seal(BytesView plaintext, BytesView aad = {});
            [[nodiscard]] bool open(const EncryptedFrame& frame, std::vector<std::uint8_t>& plaintext, BytesView aad = {});

        private:
            friend class NoiseInitiator;
            friend struct ResponderHandshake;
            friend ResponderHandshake accept_responder(
                const NodeIdentity& local_identity,
                const ClientHello& hello,
                const std::optional<PublicKey>& expected_remote
            );

            enum class Role : std::uint8_t { Initiator, Responder };

            SecureSession(SecretKey initiator_to_responder, SecretKey responder_to_initiator, Role role);

            SecretKey send_key_{};
            SecretKey recv_key_{};
            std::uint64_t send_counter_ = 0;
            std::uint64_t recv_counter_ = 0;
            bool valid_ = false;
    };

    struct ResponderHandshake {
        ServerHello hello{};
        SecureSession session;
        PublicKey remote_static_public_key{};
        Fingerprint remote_fingerprint{};
        NodeId remote_node_id{};
    };

    /**
     * @brief Initiator half of AkkaraDB's Noise-style raw-public-key handshake.
     *
     * This is intentionally socket independent.  Transport code serializes the
     * hello structs, exchanges them, then uses SecureSession for AEAD frames.
     */
    class NoiseInitiator {
        public:
            explicit NoiseInitiator(const NodeIdentity& local_identity);

            [[nodiscard]] const ClientHello& hello() const noexcept { return hello_; }
            [[nodiscard]] SecureSession finish(const ServerHello& hello, const std::optional<PublicKey>& expected_remote = std::nullopt);

        private:
            NodeIdentity local_identity_{};
            SecretKey ephemeral_secret_{};
            ClientHello hello_{};
            bool finished_ = false;
    };

    /**
     * @brief Accept an initiator hello and return the responder hello + session.
     *
     * If @p expected_remote is present, the initiator static public key must
     * match it.  Without it this is suitable for enrollment, where the caller
     * must authenticate the join request through another mechanism.
     */
    [[nodiscard]] ResponderHandshake accept_responder(
        const NodeIdentity& local_identity,
        const ClientHello& hello,
        const std::optional<PublicKey>& expected_remote = std::nullopt
    );
} // namespace akkaradb::crypto

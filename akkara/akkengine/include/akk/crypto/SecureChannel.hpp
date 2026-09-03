/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/crypto/SecureChannel.hpp
#pragma once

#include "akkaradb/Export.hpp"

#include "akk/crypto/Identity.hpp"

#include <array>
#include <cstdint>
#include <mutex>
#include <optional>
#include <span>
#include <vector>

namespace akkaradb::crypto {
    using BytesView = std::span<const std::uint8_t>;
    using AeadTag = std::array<std::uint8_t, 16>;

    struct AKDB_API ClientHello {
        PublicKey staticPublicKey{};
        PublicKey ephemeralPublicKey{};
    };

    struct AKDB_API ServerHello {
        PublicKey staticPublicKey{};
        PublicKey ephemeralPublicKey{};
        AeadTag authenticator{};
    };

    struct AKDB_API EncryptedFrame {
        std::uint64_t counter = 0;
        AeadTag tag{};
        std::vector<std::uint8_t> ciphertext;
    };

    struct ResponderHandshake;

    class AKDB_API SecureSession {
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
            friend AKDB_API ResponderHandshake acceptResponder(
                const NodeIdentity& localIdentity,
                const ClientHello& hello,
                const std::optional<PublicKey>& expectedRemote
            );

            enum class Role : std::uint8_t {
                INITIATOR, RESPONDER
            };

            SecureSession(SecretKey initiatorToResponder, SecretKey responderToInitiator, Role role);

            SecretKey sendKey_{};
            SecretKey recvKey_{};
            std::uint64_t sendCounter_ = 0;
            std::uint64_t recvCounter_ = 0;
            bool valid_ = false;
            mutable std::mutex mutex_;
    };

    struct AKDB_API ResponderHandshake {
        ServerHello hello{};
        SecureSession session;
        PublicKey remoteStaticPublicKey{};
        Fingerprint remoteFingerprint{};
        NodeId remoteNodeId{};
    };

    /**
     * @brief Initiator half of AkkaraDB's Noise-style raw-public-key handshake.
     *
     * This is intentionally socket independent.  Transport code serializes the
     * hello structs, exchanges them, then uses SecureSession for AEAD frames.
     */
    class AKDB_API NoiseInitiator {
        public:
            explicit NoiseInitiator(const NodeIdentity& localIdentity);
            ~NoiseInitiator();

            NoiseInitiator(const NoiseInitiator&) = delete;
            NoiseInitiator& operator=(const NoiseInitiator&) = delete;
            NoiseInitiator(NoiseInitiator&&) = delete;
            NoiseInitiator& operator=(NoiseInitiator&&) = delete;

            [[nodiscard]] const ClientHello& hello() const noexcept { return hello_; }
            /**
             * @brief Finish the initiator handshake after receiving the responder hello.
             *
             * When @p expectedRemote is present, the responder static public key
             * must match it. Passing std::nullopt disables static-key pinning and
             * is intended only for enrollment/TOFU flows where the caller records
             * or authenticates the responder identity through another channel.
             */
            [[nodiscard]] SecureSession finish(const ServerHello& hello, const std::optional<PublicKey>& expectedRemote = std::nullopt);

        private:
            const NodeIdentity& localIdentity_;
            SecretKey ephemeralSecret_{};
            ClientHello hello_{};
            bool finished_ = false;
    };

    /**
     * @brief Accept an initiator hello and return the responder hello + session.
     *
     * If @p expectedRemote is present, the initiator static public key must
     * match it. Passing std::nullopt disables static-key pinning and is suitable
     * only for enrollment/TOFU flows where the caller records or authenticates
     * the join request through another mechanism before trusting the peer.
     */
    [[nodiscard]] AKDB_API ResponderHandshake acceptResponder(
        const NodeIdentity& localIdentity,
        const ClientHello& hello,
        const std::optional<PublicKey>& expectedRemote = std::nullopt
    );
} // namespace akkaradb::crypto

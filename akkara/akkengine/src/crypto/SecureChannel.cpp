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

// akkengine/src/crypto/SecureChannel.cpp
#include "akk/crypto/SecureChannel.hpp"

#include "akk/crypto/Random.hpp"

#include <monocypher.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <stdexcept>
#include <string_view>

namespace akkaradb::crypto {
    namespace {
        constexpr std::string_view PROTOCOL_NAME = "AkkaraDB-NoiseStyle-X25519-XChaCha20Poly1305-BLAKE2b-v1";
        constexpr std::string_view SERVER_FINISHED = "server-finished";
        constexpr std::string_view NONCE_CONTEXT = "transport-nonce";

        struct DerivedKeys {
            SecretKey initiator_to_responder{};
            SecretKey responder_to_initiator{};
            SecretKey handshake_auth_key{};
        };

        void append(std::vector<std::uint8_t>& out, std::string_view value) {
            out.insert(out.end(), value.begin(), value.end());
        }

        void append(std::vector<std::uint8_t>& out, const std::uint8_t* data, std::size_t size) {
            out.insert(out.end(), data, data + size);
        }

        template <typename T>
        void append(std::vector<std::uint8_t>& out, const T& value) {
            append(out, value.data(), value.size());
        }

        void append_u64_le(std::vector<std::uint8_t>& out, std::uint64_t value) {
            for (int i = 0; i < 8; ++i) {
                out.push_back(static_cast<std::uint8_t>((value >> (i * 8)) & 0xffu));
            }
        }

        [[nodiscard]] SecretKey x25519_checked(const SecretKey& secret_key, const PublicKey& public_key) {
            SecretKey shared{};
            crypto_x25519(shared.data(), secret_key.data(), public_key.data());

            const std::array<std::uint8_t, 32> zeros{};
            if (crypto_verify32(shared.data(), zeros.data()) == 0) {
                secure_wipe(shared);
                throw std::runtime_error("SecureChannel: invalid low-order X25519 shared secret");
            }
            return shared;
        }

        void keyed_hash(
            std::span<std::uint8_t> out,
            const std::array<std::uint8_t, 64>& key,
            const std::vector<std::uint8_t>& message
        ) {
            auto mutable_key = key;
            crypto_blake2b_keyed(
                out.data(),
                out.size(),
                mutable_key.data(),
                mutable_key.size(),
                message.empty() ? nullptr : message.data(),
                message.size()
            );
            secure_wipe(mutable_key);
        }

        [[nodiscard]] DerivedKeys derive_keys(
            const PublicKey& initiator_static,
            const PublicKey& responder_static,
            const PublicKey& initiator_ephemeral,
            const PublicKey& responder_ephemeral,
            const SecretKey& dh_ee,
            const SecretKey& dh_es,
            const SecretKey& dh_se,
            const SecretKey& dh_ss
        ) {
            std::vector<std::uint8_t> extract_input;
            append(extract_input, PROTOCOL_NAME);
            append(extract_input, initiator_static);
            append(extract_input, responder_static);
            append(extract_input, initiator_ephemeral);
            append(extract_input, responder_ephemeral);
            append(extract_input, dh_ee);
            append(extract_input, dh_es);
            append(extract_input, dh_se);
            append(extract_input, dh_ss);

            std::array<std::uint8_t, 64> prk{};
            crypto_blake2b(prk.data(), prk.size(), extract_input.data(), extract_input.size());

            auto expand = [&prk](std::string_view label) {
                SecretKey out{};
                std::vector<std::uint8_t> info;
                append(info, PROTOCOL_NAME);
                append(info, label);
                keyed_hash(out, prk, info);
                return out;
            };

            DerivedKeys keys{
                expand("initiator-to-responder"),
                expand("responder-to-initiator"),
                expand("handshake-auth"),
            };
            secure_wipe(prk);
            return keys;
        }

        [[nodiscard]] std::vector<std::uint8_t> transcript_for_auth(
            const ClientHello& client_hello,
            const ServerHello& server_hello_without_tag
        ) {
            std::vector<std::uint8_t> transcript;
            append(transcript, PROTOCOL_NAME);
            append(transcript, client_hello.static_public_key);
            append(transcript, client_hello.ephemeral_public_key);
            append(transcript, server_hello_without_tag.static_public_key);
            append(transcript, server_hello_without_tag.ephemeral_public_key);
            append(transcript, SERVER_FINISHED);
            return transcript;
        }

        [[nodiscard]] AeadTag server_authenticator(const SecretKey& handshake_auth_key, const ClientHello& client, const ServerHello& server) {
            std::array<std::uint8_t, 64> expanded_key{};
            std::copy(handshake_auth_key.begin(), handshake_auth_key.end(), expanded_key.begin());
            const auto transcript = transcript_for_auth(client, server);

            AeadTag tag{};
            keyed_hash(tag, expanded_key, transcript);
            secure_wipe(expanded_key);
            return tag;
        }

        [[nodiscard]] std::array<std::uint8_t, 24> derive_nonce(const SecretKey& key, std::uint64_t counter) {
            std::vector<std::uint8_t> info;
            append(info, PROTOCOL_NAME);
            append(info, NONCE_CONTEXT);
            append_u64_le(info, counter);

            auto mutable_key = key;
            std::array<std::uint8_t, 24> nonce{};
            crypto_blake2b_keyed(nonce.data(), nonce.size(), mutable_key.data(), mutable_key.size(), info.data(), info.size());
            secure_wipe(mutable_key);
            return nonce;
        }

        [[nodiscard]] DerivedKeys derive_for_initiator(
            const NodeIdentity& local_identity,
            const SecretKey& local_ephemeral_secret,
            const ClientHello& client_hello,
            const ServerHello& server_hello
        ) {
            auto dh_ee = x25519_checked(local_ephemeral_secret, server_hello.ephemeral_public_key);
            auto dh_es = x25519_checked(local_ephemeral_secret, server_hello.static_public_key);
            auto dh_se = x25519_checked(local_identity.secret_key, server_hello.ephemeral_public_key);
            auto dh_ss = x25519_checked(local_identity.secret_key, server_hello.static_public_key);

            auto keys = derive_keys(
                client_hello.static_public_key,
                server_hello.static_public_key,
                client_hello.ephemeral_public_key,
                server_hello.ephemeral_public_key,
                dh_ee,
                dh_es,
                dh_se,
                dh_ss
            );

            secure_wipe(dh_ee);
            secure_wipe(dh_es);
            secure_wipe(dh_se);
            secure_wipe(dh_ss);
            return keys;
        }

        [[nodiscard]] DerivedKeys derive_for_responder(
            const NodeIdentity& local_identity,
            const SecretKey& local_ephemeral_secret,
            const ClientHello& client_hello,
            const ServerHello& server_hello
        ) {
            auto dh_ee = x25519_checked(local_ephemeral_secret, client_hello.ephemeral_public_key);
            auto dh_es = x25519_checked(local_identity.secret_key, client_hello.ephemeral_public_key);
            auto dh_se = x25519_checked(local_ephemeral_secret, client_hello.static_public_key);
            auto dh_ss = x25519_checked(local_identity.secret_key, client_hello.static_public_key);

            auto keys = derive_keys(
                client_hello.static_public_key,
                server_hello.static_public_key,
                client_hello.ephemeral_public_key,
                server_hello.ephemeral_public_key,
                dh_ee,
                dh_es,
                dh_se,
                dh_ss
            );

            secure_wipe(dh_ee);
            secure_wipe(dh_es);
            secure_wipe(dh_se);
            secure_wipe(dh_ss);
            return keys;
        }
    } // namespace

    SecureSession::~SecureSession() {
        secure_wipe(send_key_);
        secure_wipe(recv_key_);
    }

    SecureSession::SecureSession(SecureSession&& other) noexcept
        : send_key_(other.send_key_),
          recv_key_(other.recv_key_),
          send_counter_(other.send_counter_),
          recv_counter_(other.recv_counter_),
          valid_(other.valid_) {
        secure_wipe(other.send_key_);
        secure_wipe(other.recv_key_);
        other.valid_ = false;
        other.send_counter_ = 0;
        other.recv_counter_ = 0;
    }

    SecureSession& SecureSession::operator=(SecureSession&& other) noexcept {
        if (this != &other) {
            secure_wipe(send_key_);
            secure_wipe(recv_key_);
            send_key_ = other.send_key_;
            recv_key_ = other.recv_key_;
            send_counter_ = other.send_counter_;
            recv_counter_ = other.recv_counter_;
            valid_ = other.valid_;

            secure_wipe(other.send_key_);
            secure_wipe(other.recv_key_);
            other.valid_ = false;
            other.send_counter_ = 0;
            other.recv_counter_ = 0;
        }
        return *this;
    }

    SecureSession::SecureSession(SecretKey initiator_to_responder, SecretKey responder_to_initiator, Role role) {
        if (role == Role::Initiator) {
            send_key_ = initiator_to_responder;
            recv_key_ = responder_to_initiator;
        }
        else {
            send_key_ = responder_to_initiator;
            recv_key_ = initiator_to_responder;
        }
        valid_ = true;
        secure_wipe(initiator_to_responder);
        secure_wipe(responder_to_initiator);
    }

    EncryptedFrame SecureSession::seal(BytesView plaintext, BytesView aad) {
        if (!valid_) { throw std::runtime_error("SecureSession::seal on invalid session"); }

        EncryptedFrame frame;
        frame.counter = send_counter_++;
        frame.ciphertext.resize(plaintext.size());

        const auto nonce = derive_nonce(send_key_, frame.counter);
        crypto_aead_lock(
            frame.ciphertext.empty() ? nullptr : frame.ciphertext.data(),
            frame.tag.data(),
            send_key_.data(),
            nonce.data(),
            aad.empty() ? nullptr : aad.data(),
            aad.size(),
            plaintext.empty() ? nullptr : plaintext.data(),
            plaintext.size()
        );
        return frame;
    }

    bool SecureSession::open(const EncryptedFrame& frame, std::vector<std::uint8_t>& plaintext, BytesView aad) {
        if (!valid_ || frame.counter != recv_counter_) { return false; }

        std::vector<std::uint8_t> out(frame.ciphertext.size());
        const auto nonce = derive_nonce(recv_key_, frame.counter);
        const int rc = crypto_aead_unlock(
            out.empty() ? nullptr : out.data(),
            frame.tag.data(),
            recv_key_.data(),
            nonce.data(),
            aad.empty() ? nullptr : aad.data(),
            aad.size(),
            frame.ciphertext.empty() ? nullptr : frame.ciphertext.data(),
            frame.ciphertext.size()
        );
        if (rc != 0) { return false; }

        ++recv_counter_;
        plaintext = std::move(out);
        return true;
    }

    NoiseInitiator::NoiseInitiator(const NodeIdentity& local_identity) : local_identity_(local_identity) {
        secure_random(ephemeral_secret_);
        hello_.static_public_key = local_identity_.public_key;
        crypto_x25519_public_key(hello_.ephemeral_public_key.data(), ephemeral_secret_.data());
    }

    SecureSession NoiseInitiator::finish(const ServerHello& hello, const std::optional<PublicKey>& expected_remote) {
        if (finished_) { throw std::runtime_error("NoiseInitiator::finish called twice"); }
        if (expected_remote && hello.static_public_key != *expected_remote) {
            throw std::runtime_error("NoiseInitiator: unexpected responder public key");
        }

        auto keys = derive_for_initiator(local_identity_, ephemeral_secret_, hello_, hello);
        const auto expected_tag = server_authenticator(keys.handshake_auth_key, hello_, hello);
        if (crypto_verify16(expected_tag.data(), hello.authenticator.data()) != 0) {
            secure_wipe(keys.initiator_to_responder);
            secure_wipe(keys.responder_to_initiator);
            secure_wipe(keys.handshake_auth_key);
            throw std::runtime_error("NoiseInitiator: responder authenticator mismatch");
        }

        finished_ = true;
        secure_wipe(ephemeral_secret_);
        secure_wipe(keys.handshake_auth_key);
        return SecureSession(std::move(keys.initiator_to_responder), std::move(keys.responder_to_initiator), SecureSession::Role::Initiator);
    }

    ResponderHandshake accept_responder(
        const NodeIdentity& local_identity,
        const ClientHello& hello,
        const std::optional<PublicKey>& expected_remote
    ) {
        if (expected_remote && hello.static_public_key != *expected_remote) {
            throw std::runtime_error("accept_responder: unexpected initiator public key");
        }

        SecretKey ephemeral_secret{};
        secure_random(ephemeral_secret);

        ResponderHandshake result;
        result.hello.static_public_key = local_identity.public_key;
        crypto_x25519_public_key(result.hello.ephemeral_public_key.data(), ephemeral_secret.data());

        auto keys = derive_for_responder(local_identity, ephemeral_secret, hello, result.hello);
        result.hello.authenticator = server_authenticator(keys.handshake_auth_key, hello, result.hello);

        result.remote_static_public_key = hello.static_public_key;
        result.remote_fingerprint = fingerprint_public_key(hello.static_public_key);
        result.remote_node_id = node_id_from_public_key(hello.static_public_key);
        result.session = SecureSession(
            std::move(keys.initiator_to_responder),
            std::move(keys.responder_to_initiator),
            SecureSession::Role::Responder
        );

        secure_wipe(ephemeral_secret);
        secure_wipe(keys.handshake_auth_key);
        return result;
    }
} // namespace akkaradb::crypto

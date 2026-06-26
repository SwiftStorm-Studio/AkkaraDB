/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/crypto/SecureChannel.cpp
#include "akk/crypto/SecureChannel.hpp"

#include "akk/crypto/Random.hpp"

#include <monocypher.h>

#include <algorithm>
#include <array>
#include <limits>
#include <span>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace akkaradb::crypto {
    namespace {
        constexpr std::string_view PROTOCOL_NAME = "AkkaraDB-NoiseStyle-X25519-XChaCha20Poly1305-BLAKE2b-v1";
        constexpr std::string_view SERVER_FINISHED = "server-finished";
        constexpr std::string_view NONCE_CONTEXT = "transport-nonce";

        struct DerivedKeys {
            SecretKey initiatorToResponder{};
            SecretKey responderToInitiator{};
            SecretKey handshakeAuthKey{};

            DerivedKeys() = default;

            DerivedKeys(SecretKey initiator, SecretKey responder, SecretKey handshake)
                : initiatorToResponder(initiator), responderToInitiator(responder), handshakeAuthKey(handshake) {
                secureWipe(initiator);
                secureWipe(responder);
                secureWipe(handshake);
            }

            ~DerivedKeys() { wipe(); }

            DerivedKeys(const DerivedKeys&) = delete;
            DerivedKeys& operator=(const DerivedKeys&) = delete;

            DerivedKeys(DerivedKeys&& other) noexcept
                : initiatorToResponder(other.initiatorToResponder),
                  responderToInitiator(other.responderToInitiator),
                  handshakeAuthKey(other.handshakeAuthKey) { other.wipe(); }

            DerivedKeys& operator=(DerivedKeys&& other) noexcept {
                if (this != &other) {
                    wipe();
                    initiatorToResponder = other.initiatorToResponder;
                    responderToInitiator = other.responderToInitiator;
                    handshakeAuthKey = other.handshakeAuthKey;
                    other.wipe();
                }
                return *this;
            }

            void wipe() noexcept {
                secureWipe(initiatorToResponder);
                secureWipe(responderToInitiator);
                secureWipe(handshakeAuthKey);
            }
        };

        void append(std::vector<std::uint8_t>& out, std::string_view value) { out.insert(out.end(), value.begin(), value.end()); }

        void append(std::vector<std::uint8_t>& out, const std::uint8_t* data, std::size_t size) {
            out.insert(out.end(), data, data + size);
        }

        template <typename T>
        void append(std::vector<std::uint8_t>& out, const T& value) { append(out, value.data(), value.size()); }

        void wipeVector(std::vector<std::uint8_t>& secret) noexcept {
            if (!secret.empty()) { secureWipe(std::span<std::uint8_t>(secret.data(), secret.size())); }
        }

        void appendU64Le(std::vector<std::uint8_t>& out, std::uint64_t value) {
            for (int i = 0; i < 8; ++i) { out.push_back(static_cast<std::uint8_t>((value >> (i * 8)) & 0xffu)); }
        }

        [[nodiscard]] SecretKey x25519Checked(const SecretKey& secretKey, const PublicKey& publicKey) {
            SecretKey shared{};
            crypto_x25519(shared.data(), secretKey.data(), publicKey.data());

            const std::array<std::uint8_t, 32> zeros{};
            if (crypto_verify32(shared.data(), zeros.data()) == 0) {
                secureWipe(shared);
                throw std::runtime_error("SecureChannel: invalid low-order X25519 shared secret");
            }
            return shared;
        }

        void keyedHash(std::span<std::uint8_t> out, const std::array<std::uint8_t, 64>& key, const std::vector<std::uint8_t>& message) {
            auto mutableKey = key;
            crypto_blake2b_keyed(
                out.data(),
                out.size(),
                mutableKey.data(),
                mutableKey.size(),
                message.empty() ? nullptr : message.data(),
                message.size()
            );
            secureWipe(mutableKey);
        }

        [[nodiscard]] DerivedKeys deriveKeys(
            const PublicKey& initiatorStatic,
            const PublicKey& responderStatic,
            const PublicKey& initiatorEphemeral,
            const PublicKey& responderEphemeral,
            const SecretKey& dhEe,
            const SecretKey& dhEs,
            const SecretKey& dhSe,
            const SecretKey& dhSs
        ) {
            std::vector<std::uint8_t> extractInput;
            append(extractInput, PROTOCOL_NAME);
            append(extractInput, initiatorStatic);
            append(extractInput, responderStatic);
            append(extractInput, initiatorEphemeral);
            append(extractInput, responderEphemeral);
            append(extractInput, dhEe);
            append(extractInput, dhEs);
            append(extractInput, dhSe);
            append(extractInput, dhSs);

            std::array<std::uint8_t, 64> prk{};
            crypto_blake2b(prk.data(), prk.size(), extractInput.data(), extractInput.size());
            wipeVector(extractInput);

            auto expand = [&prk](std::string_view label) {
                SecretKey out{};
                std::vector<std::uint8_t> info;
                append(info, PROTOCOL_NAME);
                append(info, label);
                keyedHash(out, prk, info);
                return out;
            };

            DerivedKeys keys{expand("initiator-to-responder"), expand("responder-to-initiator"), expand("handshake-auth"),};
            secureWipe(prk);
            return keys;
        }

        [[nodiscard]] std::vector<std::uint8_t> transcriptForAuth(
            const ClientHello& clientHello,
            const ServerHello& serverHelloWithoutTag
        ) {
            std::vector<std::uint8_t> transcript;
            append(transcript, PROTOCOL_NAME);
            append(transcript, clientHello.staticPublicKey);
            append(transcript, clientHello.ephemeralPublicKey);
            append(transcript, serverHelloWithoutTag.staticPublicKey);
            append(transcript, serverHelloWithoutTag.ephemeralPublicKey);
            append(transcript, SERVER_FINISHED);
            return transcript;
        }

        [[nodiscard]] AeadTag serverAuthenticator(const SecretKey& handshakeAuthKey, const ClientHello& client, const ServerHello& server) {
            const auto transcript = transcriptForAuth(client, server);
            std::array<std::uint8_t, 64> expandedKey{};
            std::copy(handshakeAuthKey.begin(), handshakeAuthKey.end(), expandedKey.begin());

            AeadTag tag{};
            keyedHash(tag, expandedKey, transcript);
            secureWipe(expandedKey);
            return tag;
        }

        [[nodiscard]] std::array<std::uint8_t, 24> deriveNonce(const SecretKey& key, std::uint64_t counter) {
            std::vector<std::uint8_t> info;
            append(info, PROTOCOL_NAME);
            append(info, NONCE_CONTEXT);
            appendU64Le(info, counter);

            auto mutableKey = key;
            std::array<std::uint8_t, 24> nonce{};
            crypto_blake2b_keyed(nonce.data(), nonce.size(), mutableKey.data(), mutableKey.size(), info.data(), info.size());
            secureWipe(mutableKey);
            return nonce;
        }

        [[nodiscard]] DerivedKeys deriveForInitiator(
            const NodeIdentity& localIdentity,
            const SecretKey& localEphemeralSecret,
            const ClientHello& clientHello,
            const ServerHello& serverHello
        ) {
            SecretKey dhEe{};
            SecretKey dhEs{};
            SecretKey dhSe{};
            SecretKey dhSs{};
            try {
                dhEe = x25519Checked(localEphemeralSecret, serverHello.ephemeralPublicKey);
                dhEs = x25519Checked(localEphemeralSecret, serverHello.staticPublicKey);
                dhSe = x25519Checked(localIdentity.secretKey, serverHello.ephemeralPublicKey);
                dhSs = x25519Checked(localIdentity.secretKey, serverHello.staticPublicKey);

                auto keys = deriveKeys(
                    clientHello.staticPublicKey,
                    serverHello.staticPublicKey,
                    clientHello.ephemeralPublicKey,
                    serverHello.ephemeralPublicKey,
                    dhEe,
                    dhEs,
                    dhSe,
                    dhSs
                );

                secureWipe(dhEe);
                secureWipe(dhEs);
                secureWipe(dhSe);
                secureWipe(dhSs);
                return keys;
            }
            catch (...) {
                secureWipe(dhEe);
                secureWipe(dhEs);
                secureWipe(dhSe);
                secureWipe(dhSs);
                throw;
            }
        }

        [[nodiscard]] DerivedKeys deriveForResponder(
            const NodeIdentity& localIdentity,
            const SecretKey& localEphemeralSecret,
            const ClientHello& clientHello,
            const ServerHello& serverHello
        ) {
            SecretKey dhEe{};
            SecretKey dhEs{};
            SecretKey dhSe{};
            SecretKey dhSs{};
            try {
                dhEe = x25519Checked(localEphemeralSecret, clientHello.ephemeralPublicKey);
                dhEs = x25519Checked(localIdentity.secretKey, clientHello.ephemeralPublicKey);
                dhSe = x25519Checked(localEphemeralSecret, clientHello.staticPublicKey);
                dhSs = x25519Checked(localIdentity.secretKey, clientHello.staticPublicKey);

                auto keys = deriveKeys(
                    clientHello.staticPublicKey,
                    serverHello.staticPublicKey,
                    clientHello.ephemeralPublicKey,
                    serverHello.ephemeralPublicKey,
                    dhEe,
                    dhEs,
                    dhSe,
                    dhSs
                );

                secureWipe(dhEe);
                secureWipe(dhEs);
                secureWipe(dhSe);
                secureWipe(dhSs);
                return keys;
            }
            catch (...) {
                secureWipe(dhEe);
                secureWipe(dhEs);
                secureWipe(dhSe);
                secureWipe(dhSs);
                throw;
            }
        }
    } // namespace

    SecureSession::~SecureSession() {
        secureWipe(sendKey_);
        secureWipe(recvKey_);
    }

    SecureSession::SecureSession(SecureSession&& other) noexcept
        : sendKey_(other.sendKey_),
          recvKey_(other.recvKey_),
          sendCounter_(other.sendCounter_),
          recvCounter_(other.recvCounter_),
          valid_(other.valid_) {
        secureWipe(other.sendKey_);
        secureWipe(other.recvKey_);
        other.valid_ = false;
        other.sendCounter_ = 0;
        other.recvCounter_ = 0;
    }

    SecureSession& SecureSession::operator=(SecureSession&& other) noexcept {
        if (this != &other) {
            secureWipe(sendKey_);
            secureWipe(recvKey_);
            sendKey_ = other.sendKey_;
            recvKey_ = other.recvKey_;
            sendCounter_ = other.sendCounter_;
            recvCounter_ = other.recvCounter_;
            valid_ = other.valid_;

            secureWipe(other.sendKey_);
            secureWipe(other.recvKey_);
            other.valid_ = false;
            other.sendCounter_ = 0;
            other.recvCounter_ = 0;
        }
        return *this;
    }

    SecureSession::SecureSession(SecretKey initiatorToResponder, SecretKey responderToInitiator, Role role) {
        if (role == Role::INITIATOR) {
            sendKey_ = initiatorToResponder;
            recvKey_ = responderToInitiator;
        }
        else {
            sendKey_ = responderToInitiator;
            recvKey_ = initiatorToResponder;
        }
        valid_ = true;
        secureWipe(initiatorToResponder);
        secureWipe(responderToInitiator);
    }

    EncryptedFrame SecureSession::seal(BytesView plaintext, BytesView aad) {
        if (!valid_) { throw std::runtime_error("SecureSession::seal on invalid session"); }
        if (sendCounter_ == std::numeric_limits<std::uint64_t>::max()) {
            valid_ = false;
            throw std::runtime_error("SecureSession::seal counter exhausted");
        }

        EncryptedFrame frame;
        frame.counter = sendCounter_++;
        frame.ciphertext.resize(plaintext.size());

        const auto nonce = deriveNonce(sendKey_, frame.counter);
        crypto_aead_lock(
            frame.ciphertext.empty() ? nullptr : frame.ciphertext.data(),
            frame.tag.data(),
            sendKey_.data(),
            nonce.data(),
            aad.empty() ? nullptr : aad.data(),
            aad.size(),
            plaintext.empty() ? nullptr : plaintext.data(),
            plaintext.size()
        );
        return frame;
    }

    bool SecureSession::open(const EncryptedFrame& frame, std::vector<std::uint8_t>& plaintext, BytesView aad) {
        if (!valid_ || frame.counter != recvCounter_) { return false; }
        if (recvCounter_ == std::numeric_limits<std::uint64_t>::max()) {
            valid_ = false;
            return false;
        }

        std::vector<std::uint8_t> out(frame.ciphertext.size());
        const auto nonce = deriveNonce(recvKey_, frame.counter);
        const int rc = crypto_aead_unlock(
            out.empty() ? nullptr : out.data(),
            frame.tag.data(),
            recvKey_.data(),
            nonce.data(),
            aad.empty() ? nullptr : aad.data(),
            aad.size(),
            frame.ciphertext.empty() ? nullptr : frame.ciphertext.data(),
            frame.ciphertext.size()
        );
        if (rc != 0) { return false; }

        ++recvCounter_;
        plaintext = std::move(out);
        return true;
    }

    NoiseInitiator::NoiseInitiator(const NodeIdentity& localIdentity)
        : localIdentity_(localIdentity) {
        secureRandom(ephemeralSecret_);
        hello_.staticPublicKey = localIdentity_.publicKey;
        crypto_x25519_public_key(hello_.ephemeralPublicKey.data(), ephemeralSecret_.data());
    }

    NoiseInitiator::~NoiseInitiator() { secureWipe(ephemeralSecret_); }

    SecureSession NoiseInitiator::finish(const ServerHello& hello, const std::optional<PublicKey>& expectedRemote) {
        if (finished_) { throw std::runtime_error("NoiseInitiator::finish called twice"); }
        if (expectedRemote && hello.staticPublicKey != *expectedRemote) {
            throw std::runtime_error("NoiseInitiator: unexpected responder public key");
        }

        auto keys = deriveForInitiator(localIdentity_, ephemeralSecret_, hello_, hello);
        const auto expectedTag = serverAuthenticator(keys.handshakeAuthKey, hello_, hello);
        if (crypto_verify16(expectedTag.data(), hello.authenticator.data()) != 0) {
            keys.wipe();
            throw std::runtime_error("NoiseInitiator: responder authenticator mismatch");
        }

        finished_ = true;
        secureWipe(ephemeralSecret_);
        secureWipe(keys.handshakeAuthKey);
        return SecureSession(std::move(keys.initiatorToResponder), std::move(keys.responderToInitiator), SecureSession::Role::INITIATOR);
    }

    ResponderHandshake acceptResponder(
        const NodeIdentity& localIdentity,
        const ClientHello& hello,
        const std::optional<PublicKey>& expectedRemote
    ) {
        if (expectedRemote && hello.staticPublicKey != *expectedRemote) {
            throw std::runtime_error("acceptResponder: unexpected initiator public key");
        }

        SecretKey ephemeralSecret{};
        secureRandom(ephemeralSecret);

        try {
            ResponderHandshake result;
            result.hello.staticPublicKey = localIdentity.publicKey;
            crypto_x25519_public_key(result.hello.ephemeralPublicKey.data(), ephemeralSecret.data());

            auto keys = deriveForResponder(localIdentity, ephemeralSecret, hello, result.hello);
            result.hello.authenticator = serverAuthenticator(keys.handshakeAuthKey, hello, result.hello);

            result.remoteStaticPublicKey = hello.staticPublicKey;
            result.remoteFingerprint = fingerprintPublicKey(hello.staticPublicKey);
            result.remoteNodeId = nodeIdFromPublicKey(hello.staticPublicKey);
            result.session = SecureSession(
                std::move(keys.initiatorToResponder),
                std::move(keys.responderToInitiator),
                SecureSession::Role::RESPONDER
            );

            secureWipe(ephemeralSecret);
            keys.wipe();
            return result;
        }
        catch (...) {
            secureWipe(ephemeralSecret);
            throw;
        }
    }
} // namespace akkaradb::crypto

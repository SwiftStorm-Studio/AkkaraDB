/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// benchmarks/smoke/crypto_identity_smoke_test.cpp
#include "TestErrorHandlers.hpp"

#include "akk/crypto/Identity.hpp"
#include "akk/crypto/SecureChannel.hpp"

#include <cstdint>
#include <filesystem>
#include <iostream>
#include <vector>

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();

    using namespace akkaradb::crypto;

    const auto owner = generateNodeIdentity();
    const auto node = generateNodeIdentity();

    AKK_TEST_CHECK(owner.publicKey != node.publicKey);
    AKK_TEST_CHECK(owner.nodeId == nodeIdFromPublicKey(owner.publicKey));
    AKK_TEST_CHECK(node.nodeId == nodeIdFromPublicKey(node.publicKey));

    NoiseInitiator initiator(node);
    auto responder = acceptResponder(owner, initiator.hello(), node.publicKey);
    auto clientSession = initiator.finish(responder.hello, owner.publicKey);
    auto serverSession = std::move(responder.session);

    const std::vector<std::uint8_t> aad{'a', 'k', 'k', 'a', 'r', 'a'};
    const std::vector<std::uint8_t> plaintext{'h', 'e', 'l', 'l', 'o'};

    auto clientFrame = clientSession.seal(plaintext, aad);
    std::vector<std::uint8_t> opened;
    AKK_TEST_CHECK(serverSession.open(clientFrame, opened, aad));
    AKK_TEST_CHECK(opened == plaintext);

    auto serverFrame = serverSession.seal(opened, aad);
    std::vector<std::uint8_t> clientOpened;
    AKK_TEST_CHECK(clientSession.open(serverFrame, clientOpened, aad));
    AKK_TEST_CHECK(clientOpened == plaintext);

    auto tampered = clientSession.seal(plaintext, aad);
    tampered.tag[0] ^= 0x01;
    std::vector<std::uint8_t> ignored;
    AKK_TEST_CHECK(!serverSession.open(tampered, ignored, aad));

    const auto storePath = std::filesystem::temp_directory_path() / "akkaradb-crypto-identity-smoke.akid";
    std::filesystem::remove(storePath);
    IdentityStore store(storePath);
    const auto storedA = store.loadOrCreate();
    const auto storedB = store.loadOrCreate();
    AKK_TEST_CHECK(storedA.publicKey == storedB.publicKey);
    std::filesystem::remove(storePath);

    std::cout << "crypto identity smoke test passed\n";
    return 0;
}
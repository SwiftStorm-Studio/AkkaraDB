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

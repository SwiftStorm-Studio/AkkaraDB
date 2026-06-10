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

#include "akk/crypto/Identity.hpp"
#include "akk/crypto/SecureChannel.hpp"

#include <cassert>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <vector>

int main() {
    using namespace akkaradb::crypto;

    const auto owner = generate_node_identity();
    const auto node = generate_node_identity();

    assert(owner.public_key != node.public_key);
    assert(owner.node_id == node_id_from_public_key(owner.public_key));
    assert(node.node_id == node_id_from_public_key(node.public_key));

    NoiseInitiator initiator(node);
    auto responder = accept_responder(owner, initiator.hello(), node.public_key);
    auto client_session = initiator.finish(responder.hello, owner.public_key);
    auto server_session = std::move(responder.session);

    const std::vector<std::uint8_t> aad{'a', 'k', 'k', 'a', 'r', 'a'};
    const std::vector<std::uint8_t> plaintext{'h', 'e', 'l', 'l', 'o'};

    auto client_frame = client_session.seal(plaintext, aad);
    std::vector<std::uint8_t> opened;
    assert(server_session.open(client_frame, opened, aad));
    assert(opened == plaintext);

    auto server_frame = server_session.seal(opened, aad);
    std::vector<std::uint8_t> client_opened;
    assert(client_session.open(server_frame, client_opened, aad));
    assert(client_opened == plaintext);

    auto tampered = client_session.seal(plaintext, aad);
    tampered.tag[0] ^= 0x01;
    std::vector<std::uint8_t> ignored;
    assert(!server_session.open(tampered, ignored, aad));

    const auto store_path = std::filesystem::temp_directory_path() / "akkaradb-crypto-identity-smoke.akid";
    std::filesystem::remove(store_path);
    IdentityStore store(store_path);
    const auto stored_a = store.load_or_create();
    const auto stored_b = store.load_or_create();
    assert(stored_a.public_key == stored_b.public_key);
    std::filesystem::remove(store_path);

    std::cout << "crypto identity smoke test passed\n";
    return 0;
}

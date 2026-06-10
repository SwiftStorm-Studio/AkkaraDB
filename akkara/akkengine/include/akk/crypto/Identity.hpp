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

// akkengine/include/akk/crypto/Identity.hpp
#pragma once

#include <array>
#include <cstdint>
#include <filesystem>
#include <string>

namespace akkaradb::crypto {
    using SecretKey = std::array<std::uint8_t, 32>;
    using PublicKey = std::array<std::uint8_t, 32>;
    using Fingerprint = std::array<std::uint8_t, 32>;
    using NodeId = std::array<std::uint8_t, 16>;

    struct NodeIdentity {
        SecretKey secret_key{};
        PublicKey public_key{};
        Fingerprint fingerprint{};
        NodeId node_id{};
    };

    /**
     * @brief Generate a fresh raw-public-key identity for one AkkaraDB node.
     */
    [[nodiscard]] NodeIdentity generate_node_identity();

    /**
     * @brief Deterministically derive a node identity from a secret random seed.
     *
     * The seed must be secret and high entropy.  This is useful for persistent
     * load/create flows, not for deriving keys from public instance IDs.
     */
    [[nodiscard]] NodeIdentity node_identity_from_seed(const SecretKey& seed);

    [[nodiscard]] Fingerprint fingerprint_public_key(const PublicKey& public_key);
    [[nodiscard]] NodeId node_id_from_public_key(const PublicKey& public_key);
    [[nodiscard]] std::string public_key_to_hex(const PublicKey& bytes);
    [[nodiscard]] std::string fingerprint_to_hex(const Fingerprint& bytes);
    [[nodiscard]] std::string node_id_to_hex(const NodeId& bytes);

    /**
     * @brief Load or create the local node identity seed file.
     *
     * The file stores only the secret seed.  The X25519 key pair and public
     * fingerprint are derived from it at load time.
     */
    class IdentityStore {
        public:
            explicit IdentityStore(std::filesystem::path path);

            [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }
            [[nodiscard]] NodeIdentity load_or_create() const;
            void save_seed(const SecretKey& seed) const;
            [[nodiscard]] SecretKey load_seed() const;

        private:
            std::filesystem::path path_;
    };
} // namespace akkaradb::crypto

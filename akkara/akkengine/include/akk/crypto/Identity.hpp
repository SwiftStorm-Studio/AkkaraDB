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

#include "akkaradb/Export.hpp"

#include <array>
#include <cstdint>
#include <filesystem>
#include <string>

namespace akkaradb::crypto {
    using SecretKey = std::array<std::uint8_t, 32>;
    using PublicKey = std::array<std::uint8_t, 32>;
    using Fingerprint = std::array<std::uint8_t, 32>;
    using NodeId = std::array<std::uint8_t, 16>;

    struct AKDB_API NodeIdentity {
        SecretKey secretKey{};
        PublicKey publicKey{};
        Fingerprint fingerprint{};
        NodeId nodeId{};
    };

    /**
     * @brief Generate a fresh raw-public-key identity for one AkkaraDB node.
     */
    [[nodiscard]] AKDB_API NodeIdentity generateNodeIdentity();

    /**
     * @brief Deterministically derive a node identity from a secret random seed.
     *
     * The seed must be secret and high entropy.  This is useful for persistent
     * load/create flows, not for deriving keys from public instance IDs.
     */
    [[nodiscard]] AKDB_API NodeIdentity nodeIdentityFromSeed(const SecretKey& seed);

    [[nodiscard]] AKDB_API Fingerprint fingerprintPublicKey(const PublicKey& publicKey);
    [[nodiscard]] AKDB_API NodeId nodeIdFromPublicKey(const PublicKey& publicKey);
    [[nodiscard]] AKDB_API std::string publicKeyToHex(const PublicKey& bytes);
    [[nodiscard]] AKDB_API std::string fingerprintToHex(const Fingerprint& bytes);
    [[nodiscard]] AKDB_API std::string nodeIdToHex(const NodeId& bytes);

    /**
     * @brief Load or create the local node identity seed file.
     *
     * The file stores only the secret seed.  The X25519 key pair and public
     * fingerprint are derived from it at load time.
     */
    class AKDB_API IdentityStore {
        public:
            explicit IdentityStore(std::filesystem::path path);

            [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }
            [[nodiscard]] NodeIdentity loadOrCreate() const;
            void saveSeed(const SecretKey& seed) const;
            /**
             * @brief Load the raw secret seed from disk.
             *
             * The returned seed is secret material. Callers that use this
             * low-level API must wipe it after deriving the identity.
             */
            [[nodiscard]] SecretKey loadSeed() const;

        private:
            std::filesystem::path path_;
    };
} // namespace akkaradb::crypto

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

// akkengine/src/crypto/Identity.cpp
#include "akk/crypto/Identity.hpp"

#include "akk/crypto/Random.hpp"

#include <monocypher.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string_view>

#ifndef _WIN32
#include <sys/stat.h>
#endif

namespace akkaradb::crypto {
    namespace {
        constexpr std::array<std::uint8_t, 8> IDENTITY_FILE_MAGIC{
            'A', 'K', 'K', 'I', 'D', '0', '1', '\0',
        };
        constexpr std::string_view NODE_IDENTITY_CONTEXT = "AkkaraDB node identity v1";

        void derive_keyed_blake2b(
            std::span<std::uint8_t> out,
            const SecretKey& key,
            std::string_view context
        ) {
            auto mutable_key = key;
            crypto_blake2b_keyed(
                out.data(),
                out.size(),
                mutable_key.data(),
                mutable_key.size(),
                reinterpret_cast<const std::uint8_t*>(context.data()),
                context.size()
            );
            secure_wipe(mutable_key);
        }

        template <typename T>
        [[nodiscard]] std::string bytes_to_hex(const T& bytes) {
            std::ostringstream out;
            out << std::hex << std::setfill('0');
            for (const auto byte : bytes) { out << std::setw(2) << static_cast<unsigned>(byte); }
            return out.str();
        }
    } // namespace

    NodeIdentity generate_node_identity() {
        SecretKey seed{};
        secure_random(seed);
        auto identity = node_identity_from_seed(seed);
        secure_wipe(seed);
        return identity;
    }

    NodeIdentity node_identity_from_seed(const SecretKey& seed) {
        NodeIdentity identity{};
        derive_keyed_blake2b(identity.secret_key, seed, NODE_IDENTITY_CONTEXT);
        crypto_x25519_public_key(identity.public_key.data(), identity.secret_key.data());
        identity.fingerprint = fingerprint_public_key(identity.public_key);
        std::copy_n(identity.fingerprint.begin(), identity.node_id.size(), identity.node_id.begin());
        return identity;
    }

    Fingerprint fingerprint_public_key(const PublicKey& public_key) {
        Fingerprint fp{};
        crypto_blake2b(fp.data(), fp.size(), public_key.data(), public_key.size());
        return fp;
    }

    NodeId node_id_from_public_key(const PublicKey& public_key) {
        const auto fp = fingerprint_public_key(public_key);
        NodeId id{};
        std::copy_n(fp.begin(), id.size(), id.begin());
        return id;
    }

    std::string public_key_to_hex(const PublicKey& bytes) { return bytes_to_hex(bytes); }
    std::string fingerprint_to_hex(const Fingerprint& bytes) { return bytes_to_hex(bytes); }
    std::string node_id_to_hex(const NodeId& bytes) { return bytes_to_hex(bytes); }

    IdentityStore::IdentityStore(std::filesystem::path path) : path_(std::move(path)) {}

    NodeIdentity IdentityStore::load_or_create() const {
        if (std::filesystem::exists(path_)) { return node_identity_from_seed(load_seed()); }

        SecretKey seed{};
        secure_random(seed);
        save_seed(seed);
        auto identity = node_identity_from_seed(seed);
        secure_wipe(seed);
        return identity;
    }

    void IdentityStore::save_seed(const SecretKey& seed) const {
        const auto parent = path_.parent_path();
        if (!parent.empty()) { std::filesystem::create_directories(parent); }

        const auto tmp = path_.string() + ".tmp";
        {
            std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
            if (!out) { throw std::runtime_error("IdentityStore: failed to open seed file for write"); }
            out.write(reinterpret_cast<const char*>(IDENTITY_FILE_MAGIC.data()), static_cast<std::streamsize>(IDENTITY_FILE_MAGIC.size()));
            out.write(reinterpret_cast<const char*>(seed.data()), static_cast<std::streamsize>(seed.size()));
            if (!out) { throw std::runtime_error("IdentityStore: failed to write seed file"); }
        }

        #ifndef _WIN32
        (void)::chmod(tmp.c_str(), S_IRUSR | S_IWUSR);
        #endif

        std::filesystem::rename(tmp, path_);
    }

    SecretKey IdentityStore::load_seed() const {
        std::ifstream in(path_, std::ios::binary);
        if (!in) { throw std::runtime_error("IdentityStore: failed to open seed file"); }

        std::array<std::uint8_t, IDENTITY_FILE_MAGIC.size()> magic{};
        SecretKey seed{};
        in.read(reinterpret_cast<char*>(magic.data()), static_cast<std::streamsize>(magic.size()));
        in.read(reinterpret_cast<char*>(seed.data()), static_cast<std::streamsize>(seed.size()));
        if (!in) { throw std::runtime_error("IdentityStore: truncated seed file"); }
        if (magic != IDENTITY_FILE_MAGIC) { throw std::runtime_error("IdentityStore: invalid seed file magic"); }

        char extra = 0;
        if (in.read(&extra, 1)) { throw std::runtime_error("IdentityStore: seed file has trailing bytes"); }
        return seed;
    }
} // namespace akkaradb::crypto

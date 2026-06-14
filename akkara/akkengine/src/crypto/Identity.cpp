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
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <utility>

#ifndef _WIN32
#include <sys/stat.h>
#endif

namespace akkaradb::crypto {
    namespace {
        constexpr std::array<std::uint8_t, 8> IDENTITY_FILE_MAGIC{'A', 'K', 'K', 'I', 'D', '0', '1', '\0',};
        constexpr std::string_view NODE_IDENTITY_CONTEXT = "AkkaraDB node identity v1";

        void deriveKeyedBlake2b(std::span<std::uint8_t> out, const SecretKey& key, std::string_view context) {
            auto mutableKey = key;
            crypto_blake2b_keyed(
                out.data(),
                out.size(),
                mutableKey.data(),
                mutableKey.size(),
                reinterpret_cast<const std::uint8_t*>(context.data()),
                context.size()
            );
            secureWipe(mutableKey);
        }

        template <typename T>
        [[nodiscard]] std::string bytesToHex(const T& bytes) {
            std::ostringstream out;
            out << std::hex << std::setfill('0');
            for (const auto byte : bytes) { out << std::setw(2) << static_cast<unsigned>(byte); }
            return out.str();
        }
    } // namespace

    NodeIdentity generateNodeIdentity() {
        SecretKey seed{};
        secureRandom(seed);
        auto identity = nodeIdentityFromSeed(seed);
        secureWipe(seed);
        return identity;
    }

    NodeIdentity nodeIdentityFromSeed(const SecretKey& seed) {
        NodeIdentity identity{};
        deriveKeyedBlake2b(identity.secretKey, seed, NODE_IDENTITY_CONTEXT);
        crypto_x25519_public_key(identity.publicKey.data(), identity.secretKey.data());
        identity.fingerprint = fingerprintPublicKey(identity.publicKey);
        std::copy_n(identity.fingerprint.begin(), identity.nodeId.size(), identity.nodeId.begin());
        return identity;
    }

    Fingerprint fingerprintPublicKey(const PublicKey& publicKey) {
        Fingerprint fp{};
        crypto_blake2b(fp.data(), fp.size(), publicKey.data(), publicKey.size());
        return fp;
    }

    NodeId nodeIdFromPublicKey(const PublicKey& publicKey) {
        const auto fp = fingerprintPublicKey(publicKey);
        NodeId id{};
        std::copy_n(fp.begin(), id.size(), id.begin());
        return id;
    }

    std::string publicKeyToHex(const PublicKey& bytes) { return bytesToHex(bytes); }
    std::string fingerprintToHex(const Fingerprint& bytes) { return bytesToHex(bytes); }
    std::string nodeIdToHex(const NodeId& bytes) { return bytesToHex(bytes); }

    IdentityStore::IdentityStore(std::filesystem::path path) : path_(std::move(path)) {}

    NodeIdentity IdentityStore::loadOrCreate() const {
        if (std::filesystem::exists(path_)) {
            auto seed = loadSeed();
            try {
                auto identity = nodeIdentityFromSeed(seed);
                secureWipe(seed);
                return identity;
            }
            catch (...) {
                secureWipe(seed);
                throw;
            }
        }

        SecretKey seed{};
        secureRandom(seed);
        try {
            saveSeed(seed);
            auto identity = nodeIdentityFromSeed(seed);
            secureWipe(seed);
            return identity;
        }
        catch (...) {
            secureWipe(seed);
            throw;
        }
    }

    void IdentityStore::saveSeed(const SecretKey& seed) const {
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

    SecretKey IdentityStore::loadSeed() const {
        std::ifstream in(path_, std::ios::binary);
        if (!in) { throw std::runtime_error("IdentityStore: failed to open seed file"); }

        std::array<std::uint8_t, IDENTITY_FILE_MAGIC.size()> magic{};
        SecretKey seed{};
        in.read(reinterpret_cast<char*>(magic.data()), static_cast<std::streamsize>(magic.size()));
        in.read(reinterpret_cast<char*>(seed.data()), static_cast<std::streamsize>(seed.size()));
        if (!in) {
            secureWipe(seed);
            throw std::runtime_error("IdentityStore: truncated seed file");
        }
        if (magic != IDENTITY_FILE_MAGIC) {
            secureWipe(seed);
            throw std::runtime_error("IdentityStore: invalid seed file magic");
        }

        char extra = 0;
        if (in.read(&extra, 1)) {
            secureWipe(seed);
            throw std::runtime_error("IdentityStore: seed file has trailing bytes");
        }
        return seed;
    }
} // namespace akkaradb::crypto

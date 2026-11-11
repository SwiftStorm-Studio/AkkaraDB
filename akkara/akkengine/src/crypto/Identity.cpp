/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/crypto/Identity.cpp
#include "akk/crypto/Identity.hpp"

#include "akk/crypto/Random.hpp"

#include <monocypher.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#else
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
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

#ifdef _WIN32
        struct WindowsHandle {
            HANDLE value = INVALID_HANDLE_VALUE;
            ~WindowsHandle() { if (value != INVALID_HANDLE_VALUE && value != nullptr) { ::CloseHandle(value); } }
            WindowsHandle() = default;
            WindowsHandle(const WindowsHandle&) = delete;
            WindowsHandle& operator=(const WindowsHandle&) = delete;
        };

        HANDLE createPrivateSeedFile(const std::filesystem::path& path) {
            WindowsHandle token;
            if (!::OpenProcessToken(::GetCurrentProcess(), TOKEN_QUERY, &token.value)) {
                throw std::runtime_error("IdentityStore: cannot query file owner");
            }
            DWORD size = 0;
            if (::GetTokenInformation(token.value, TokenUser, nullptr, 0, &size) ||
                ::GetLastError() != ERROR_INSUFFICIENT_BUFFER || size == 0) {
                throw std::runtime_error("IdentityStore: cannot query file owner");
            }
            std::vector<uint8_t> tokenInfo(size);
            if (!::GetTokenInformation(token.value, TokenUser, tokenInfo.data(), size, &size)) {
                throw std::runtime_error("IdentityStore: cannot query file owner");
            }
            const auto user = reinterpret_cast<const TOKEN_USER*>(tokenInfo.data());
            alignas(ACL) std::array<uint8_t, sizeof(ACL) + sizeof(ACCESS_ALLOWED_ACE) + SECURITY_MAX_SID_SIZE> aclBytes{};
            const auto acl = reinterpret_cast<ACL*>(aclBytes.data());
            SECURITY_DESCRIPTOR descriptor{};
            // Protect the DACL so a shared parent cannot grant access to the seed.
            if (!::InitializeAcl(acl, static_cast<DWORD>(aclBytes.size()), ACL_REVISION) ||
                !::AddAccessAllowedAce(acl, ACL_REVISION, FILE_ALL_ACCESS, user->User.Sid) ||
                !::InitializeSecurityDescriptor(&descriptor, SECURITY_DESCRIPTOR_REVISION) ||
                !::SetSecurityDescriptorOwner(&descriptor, user->User.Sid, FALSE) ||
                !::SetSecurityDescriptorDacl(&descriptor, TRUE, acl, FALSE) ||
                !::SetSecurityDescriptorControl(&descriptor, SE_DACL_PROTECTED, SE_DACL_PROTECTED)) {
                throw std::runtime_error("IdentityStore: cannot restrict seed file access");
            }
            SECURITY_ATTRIBUTES attributes{sizeof(SECURITY_ATTRIBUTES), &descriptor, FALSE};
            return ::CreateFileW(path.c_str(), GENERIC_WRITE, 0, &attributes, CREATE_NEW, FILE_ATTRIBUTE_NORMAL, nullptr);
        }
#endif
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

        std::array<uint8_t, 16> nonce{};
        secureRandom(nonce);
        auto tmp = path_;
        tmp += ".tmp." + bytesToHex(nonce);
#ifdef _WIN32
        WindowsHandle file;
        file.value = createPrivateSeedFile(tmp);
        if (file.value == INVALID_HANDLE_VALUE) { throw std::runtime_error("IdentityStore: failed to create private seed file"); }
#else
        int flags = O_WRONLY | O_CREAT | O_EXCL;
#ifdef O_CLOEXEC
        flags |= O_CLOEXEC;
#endif
#ifdef O_NOFOLLOW
        flags |= O_NOFOLLOW;
#endif
        int file = ::open(tmp.c_str(), flags, S_IRUSR | S_IWUSR);
        if (file < 0) { throw std::runtime_error("IdentityStore: failed to create private seed file"); }
#endif
        try {
            const auto writeAll = [&](std::span<const uint8_t> bytes) {
                while (!bytes.empty()) {
#ifdef _WIN32
                    DWORD written = 0;
                    if (!::WriteFile(file.value, bytes.data(), static_cast<DWORD>(bytes.size()), &written, nullptr) || written == 0) {
                        throw std::runtime_error("IdentityStore: failed to write seed file");
                    }
#else
                    const auto written = ::write(file, bytes.data(), bytes.size());
                    if (written < 0 && errno == EINTR) { continue; }
                    if (written <= 0) { throw std::runtime_error("IdentityStore: failed to write seed file"); }
#endif
                    bytes = bytes.subspan(static_cast<size_t>(written));
                }
            };
            writeAll(IDENTITY_FILE_MAGIC);
            writeAll(seed);
#ifdef _WIN32
            if (!::FlushFileBuffers(file.value)) { throw std::runtime_error("IdentityStore: failed to sync seed file"); }
            if (!::CloseHandle(std::exchange(file.value, INVALID_HANDLE_VALUE))) {
                throw std::runtime_error("IdentityStore: failed to close seed file");
            }
#else
            if (::fsync(file) != 0) { throw std::runtime_error("IdentityStore: failed to sync seed file"); }
            if (::close(std::exchange(file, -1)) != 0) { throw std::runtime_error("IdentityStore: failed to close seed file"); }
#endif
            std::filesystem::rename(tmp, path_);
        }
        catch (...) {
#ifdef _WIN32
            if (file.value != INVALID_HANDLE_VALUE) { ::CloseHandle(std::exchange(file.value, INVALID_HANDLE_VALUE)); }
#else
            if (file >= 0) { ::close(std::exchange(file, -1)); }
#endif
            std::error_code ec;
            std::filesystem::remove(tmp, ec);
            throw;
        }
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

/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/crypto/Random.cpp
#include "akk/crypto/Random.hpp"

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <bcrypt.h>
#else
#include <cerrno>
#include <fcntl.h>
#include <unistd.h>
#if defined(__linux__)
#include <sys/random.h>
#endif
#endif

#include <algorithm>
#include <stdexcept>
#include <string>

namespace akkaradb::crypto {
    namespace {
        #ifndef _WIN32
        void fillFromUrandom(std::span<std::uint8_t> out) {
            int fd = -1;
            do { fd = ::open("/dev/urandom", O_RDONLY); }
            while (fd < 0 && errno == EINTR);

            if (fd < 0) { throw std::runtime_error("secureRandom: failed to open /dev/urandom"); }

            std::size_t offset = 0;
            while (offset < out.size()) {
                const auto remaining = out.size() - offset;
                const auto request = std::min<std::size_t>(remaining, 1u << 20);
                const ssize_t n = ::read(fd, out.data() + offset, request);
                if (n < 0) {
                    if (errno == EINTR) { continue; }
                    const int saved = errno;
                    (void)::close(fd);
                    throw std::runtime_error("secureRandom: failed to read /dev/urandom: " + std::to_string(saved));
                }
                if (n == 0) {
                    (void)::close(fd);
                    throw std::runtime_error("secureRandom: /dev/urandom returned EOF");
                }
                offset += static_cast<std::size_t>(n);
            }

            (void)::close(fd);
        }
        #endif
    } // namespace

    void secureRandom(std::span<std::uint8_t> out) {
        if (out.empty()) { return; }

        #ifdef _WIN32
        while (!out.empty()) {
            const auto chunk = std::min<std::size_t>(out.size(), static_cast<std::size_t>(ULONG_MAX));
            const NTSTATUS status = ::BCryptGenRandom(nullptr, out.data(), static_cast<ULONG>(chunk), BCRYPT_USE_SYSTEM_PREFERRED_RNG);
            if (status != 0) { throw std::runtime_error("secureRandom: BCryptGenRandom failed"); }
            out = out.subspan(chunk);
        }
        #elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
        ::arc4random_buf(out.data(), out.size());
        #elif defined(__linux__)
        std::size_t offset = 0; while (offset < out.size()) {
            const auto remaining = out.size() - offset;
            const auto request = std::min<std::size_t>(remaining, 1u << 20);
            const ssize_t n = ::getrandom(out.data() + offset, request, 0);
            if (n < 0) {
                if (errno == EINTR) { continue; }
                if (errno == ENOSYS) { break; }
                throw std::runtime_error("secureRandom: getrandom failed: " + std::to_string(errno));
            }
            offset += static_cast<std::size_t>(n);
        } if (offset < out.size()) { fillFromUrandom(out.subspan(offset)); }
        #else
        fillFromUrandom(out);
        #endif
    }

    void secureWipe(std::span<std::uint8_t> secret) noexcept {
        if (secret.empty()) { return; }

        #ifdef _WIN32
        (void)::SecureZeroMemory(secret.data(), secret.size());
        #else
        volatile std::uint8_t* p = secret.data();
        for (std::size_t i = 0; i < secret.size(); ++i) { p[i] = 0; }
        #endif
    }
} // namespace akkaradb::crypto

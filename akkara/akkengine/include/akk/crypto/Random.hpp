/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/crypto/Random.hpp
#pragma once

#include "akkaradb/Export.hpp"

#include <cstddef>
#include <cstdint>
#include <span>

namespace akkaradb::crypto {
    /**
     * @brief Fill @p out with bytes from the operating system CSPRNG.
     *
     * This is intentionally a thin OS wrapper.  AkkaraDB must not derive
     * long-term keys from unique but public machine or table identifiers.
     *
     * @throws std::runtime_error when the OS random source is unavailable.
     */
    AKDB_API void secureRandom(std::span<std::uint8_t> out);

    /**
     * @brief Best-effort constant-time wipe for temporary secret material.
     */
    AKDB_API void secureWipe(std::span<std::uint8_t> secret) noexcept;
} // namespace akkaradb::crypto

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

// akkengine/include/akk/crypto/Random.hpp
#pragma once

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
    void secureRandom(std::span<std::uint8_t> out);

    /**
     * @brief Best-effort constant-time wipe for temporary secret material.
     */
    void secureWipe(std::span<std::uint8_t> secret) noexcept;
} // namespace akkaradb::crypto

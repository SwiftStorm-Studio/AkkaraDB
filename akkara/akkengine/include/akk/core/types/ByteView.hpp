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

// akkengine/include/akk/core/types/ByteView.hpp
#pragma once

#include <cstddef>
#include <span>

namespace akkaradb::core {
    /**
     * @brief Immutable view over contiguous binary data.
     *
     * ByteView is the canonical non-owning binary slice type used
     * throughout AkkaraDB public and internal interfaces.
     *
     * Characteristics:
     * - zero-copy
     * - immutable
     * - size-aware
     * - binary-safe
     */
    using ByteView = std::span<const std::byte>;

    /**
     * @brief Mutable view over contiguous binary data.
     *
     * Intended for writable buffers such as:
     * - serialization targets
     * - temporary encoding buffers
     * - Arena-backed memory regions
     */
    using MutableByteView = std::span<std::byte>;
} // namespace akkaradb::core

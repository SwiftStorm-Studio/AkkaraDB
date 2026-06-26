/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
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

/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/core/utils/StringUtil.hpp
#pragma once

#include <cstring>
#include <string_view>
#include "akk/core/buffer/BufferArena.hpp"

namespace akkaradb::core {
    /**
     * @brief Copies a string into memory managed by a BufferArena.
     *
     * Allocates enough memory from the given arena to store the contents of the
     * provided string view plus a null terminator, then copies the string data
     * and appends `'\0'` at the end.
     *
     * The returned pointer remains valid as long as the underlying
     * BufferArena allocation remains alive.
     *
     * @param sv The source string view to copy.
     * @param arena Pointer to the target memory arena used for allocation.
     * @return Pointer to a null-terminated string stored inside the arena.
     *
     * @note The returned pointer is owned by the provided BufferArena and must
     *       not be manually deallocated.
     *
     * @warning Passing a null arena pointer results in undefined behavior.
     */
    inline const char* copyString(std::string_view sv, BufferArena* arena) {
        auto* mem = arena->allocate(sv.size() + 1);

        std::memcpy(mem, sv.data(), sv.size());

        reinterpret_cast<char*>(mem)[sv.size()] = '\0';

        return reinterpret_cast<const char*>(mem);
    }
} // namespace akkaradb::core

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

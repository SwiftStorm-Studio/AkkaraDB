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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

// akkserver/include/akk/engine/server/AkkApiServerExport.hpp
#pragma once

#if defined(_WIN32) && !defined(AKKARADB_STATIC)
#  if defined(AKKARADB_API_SERVER_BUILD_SHARED)
#    define AKKARADB_API_SERVER_API __declspec(dllexport)
#  else
#    define AKKARADB_API_SERVER_API __declspec(dllimport)
#  endif
#else
#  define AKKARADB_API_SERVER_API
#endif

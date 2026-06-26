/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkaradb/include/akkaradb/Export.hpp
#pragma once

#if defined(_MSC_VER)
#  pragma warning(disable: 4251)
#endif

#if defined(_WIN32) && !defined(AKKARADB_STATIC)
#  if defined(AKKARADB_BUILD_SHARED)
#    define AKDB_API __declspec(dllexport)
#  else
#    define AKDB_API __declspec(dllimport)
#  endif
#else
#  define AKDB_API
#endif

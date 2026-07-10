/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/cluster/AkkClusterRuntimeExport.hpp
#pragma once

#if defined(_WIN32) && !defined(AKKARADB_STATIC)
#  if defined(AKKARADB_CLUSTER_RUNTIME_BUILD_SHARED)
#    define AKKARADB_CLUSTER_RUNTIME_API __declspec(dllexport)
#  else
#    define AKKARADB_CLUSTER_RUNTIME_API __declspec(dllimport)
#  endif
#else
#  define AKKARADB_CLUSTER_RUNTIME_API
#endif

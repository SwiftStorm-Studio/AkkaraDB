/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// jni/AkkaraJni.cpp
#include "akkaradb/AkkaraDB.hpp"

#include <jni.h>

#include <cstdio>
#include <cstring>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#ifndef AKKARADB_JNI_COMPAT_LINE
#define AKKARADB_JNI_COMPAT_LINE "126.1"
#endif

#ifndef AKKARADB_REQUIRED_NATIVE_GENERATION
#define AKKARADB_REQUIRED_NATIVE_GENERATION "126"
#endif

namespace {
    using akkaradb::AkkaraDB;
    using akkaradb::Codec;
    using akkaradb::StartupMode;
    using akkaradb::core::ArenaGenerator;
    using akkaradb::core::BufferArena;
    using akkaradb::engine::AkkEngine;

    #include "detail/JniRuntime.inc"
    #include "detail/BytesReader.inc"
    #include "detail/SchemaCodec.inc"
    #include "detail/RowValueCodec.inc"
    #include "detail/QueryProgram.inc"
    #include "detail/NativeTableIndex.inc"
    #include "detail/JavaObjects.inc"
    #include "detail/StatsObjects.inc"
    #include "detail/OptionsPayload.inc"
}

extern "C" {
    #include "detail/NativeExportsTable.inc"
    #include "detail/NativeExportsEngine.inc"
    #include "detail/NativeExportsScanHistory.inc"
}

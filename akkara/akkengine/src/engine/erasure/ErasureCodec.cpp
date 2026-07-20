/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
// akkengine/src/engine/erasure/ErasureCodec.cpp
#include "akk/engine/erasure/ErasureCodec.hpp"
#include "akk/cpu/CRC32C.hpp"

namespace akkaradb::engine::erasure {
    uint16_t ErasureLayout::totalShards() const noexcept { return static_cast<uint16_t>(dataShards + parityShards); }
    bool ErasureShard::verifyCrc() const noexcept {
        return crc32c == cpu::CRC32C(reinterpret_cast<const std::byte*>(payload.data()), payload.size());
    }
} // namespace akkaradb::engine::erasure

/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/detail/ProtocolBulkWriter.hpp
#pragma once

#include "akkaradb/Export.hpp"
#include <cstdint>
#include <span>

namespace akkaradb::engine {
    class AkkEngine;
    namespace detail {
        struct BulkPutEntry {
            std::span<const uint8_t> key;
            std::span<const uint8_t> value;
        };

        // Internal adapter for existing non-transactional Cluster/API batch protocols.
        class AKDB_API ProtocolBulkWriter {
            public:
                static void put(AkkEngine& engine, std::span<const BulkPutEntry> entries);
        };
    }
}

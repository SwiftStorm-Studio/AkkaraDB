/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/wal/WalRecovery.hpp
/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/wal/WalRecovery.hpp
#pragma once

#include "akkaradb/Export.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <span>
#include <vector>

namespace akkaradb::engine::memtable {
    class MemTable;
}

namespace akkaradb::engine::wal {
    struct AKDB_API WalRecoveryOptions {
        std::filesystem::path walDir;
        uint64_t checkpointSeq = 0;
        size_t maxEntryBytes = 64ULL * 1024ULL * 1024ULL;
        bool truncateCorruptTail = false;
    };

    struct AKDB_API WalRecoveredEntry {
        std::vector<uint8_t> key;
        std::vector<uint8_t> value;
        uint64_t seq = 0;
        uint64_t keyFp64 = 0;
        uint16_t flags = 0;
        uint16_t shardId = 0;
        uint64_t segmentId = 0;
    };

    struct AKDB_API WalRecoveryResult {
        uint64_t segmentsSeen = 0;
        uint64_t segmentsReplayed = 0;
        uint64_t corruptSegments = 0;
        uint64_t entriesSeen = 0;
        uint64_t entriesReplayed = 0;
        uint64_t maxSeq = 0;
        uint64_t segmentsTruncated = 0;
        uint64_t segmentsRemoved = 0;
    };

    class AKDB_API WalRecovery {
        public:
            using Callback = std::function<void(const WalRecoveredEntry&)>;

            [[nodiscard]] static WalRecoveryResult recover(const WalRecoveryOptions& options, const Callback& callback);
            [[nodiscard]] static WalRecoveryResult recoverInto(const WalRecoveryOptions& options, memtable::MemTable& memtable);
    };
} // namespace akkaradb::engine::wal

/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/detail/AkkEngineSupport.hpp
#pragma once

#include "akk/engine/AkkEngine.hpp"
#include <akk/engine/cluster/detail/ReconfigurationDeadline.hpp>
#include "akk/cpu/CRC32C.hpp"
#include "akk/crypto/Random.hpp"
#include "akk/engine/cluster/detail/BoundedExecutor.hpp"
#include "akk/engine/cluster/detail/ClusterHealth.hpp"
#include "akk/engine/cluster/detail/ClusterPlacement.hpp"
#include "akk/engine/cluster/detail/DiskResults.hpp"
#include "akk/engine/cluster/detail/EngineSnapshot.hpp"
#include "akk/engine/cluster/detail/KeyedMutex.hpp"
#include "akk/engine/cluster/detail/NodeResourceBudget.hpp"
#include "akk/engine/cluster/detail/TransactionProtocol.hpp"
#include "akk/engine/detail/ProtocolBulkWriter.hpp"

#include "akk/core/record/KeyFingerprint.hpp"
#include "akk/core/record/MemHdr16.hpp"
#include "akk/engine/blob/BlobFraming.hpp"
#include "akk/engine/cluster/ClusterRuntimeProvider.hpp"
#include "akk/engine/erasure/ErasureCodec.hpp"
#include "akk/engine/generation/StorageGeneration.hpp"
#include "akk/engine/manifest/Manifest.hpp"
#include "akk/engine/server/AkkApiServerProvider.hpp"
#include "akk/engine/wal/WalFraming.hpp"
#include "akk/engine/wal/WalRecovery.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <ranges>
#include <set>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include "ClusterQuerySpool.hpp"

#ifdef _WIN32
#include <io.h>
#include <Windows.h>
#else
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace akkaradb::engine::detail::runtime {
    namespace fs = std::filesystem;

    #include "AkkEngineFileSupport.inc"
    #include "AkkEnginePolicy.inc"
    #include "AkkEngineSnapshotIO.inc"
    #include "AkkEngineScanSupport.inc"
}

/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/vlog/VersionLog.cpp
#include "akk/engine/vlog/VersionLog.hpp"

#include "akk/cpu/CRC32C.hpp"
#include "akk/core/record/KeyFingerprint.hpp"

#include <zstd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <deque>
#include <exception>
#include <iterator>
#include <chrono>
#include <limits>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#ifdef _WIN32
#include <io.h>
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace akkaradb::engine::vlog {
    namespace fs = std::filesystem;

    #include "detail/VersionLogFormat.hpp"

    class VersionLog::Impl {
        public:

    #include "detail/VersionLogImplState.hpp"
    #include "detail/VersionLogAppend.hpp"
    #include "detail/VersionLogFiles.hpp"
    #include "detail/VersionLogSegments.hpp"
    #include "detail/VersionLogParallel.hpp"
    #include "detail/VersionLogScan.hpp"
    #include "detail/VersionLogRetention.hpp"
    #include "detail/VersionLogReadIndex.hpp"
    #include "detail/VersionLogRecovery.hpp"
};

#include "detail/VersionLogApi.hpp"
} // namespace akkaradb::engine::vlog

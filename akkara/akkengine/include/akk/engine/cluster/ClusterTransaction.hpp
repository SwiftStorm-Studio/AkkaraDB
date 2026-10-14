/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
#pragma once
#include <array>
#include <cstdint>

namespace akkaradb::engine::cluster {
    using ClusterTransactionId = std::array<uint8_t, 16>;
    enum class ClusterTransactionStatus : uint8_t { UNKNOWN = 0, OPEN = 2, PREPARING, COMMITTED, ABORTED };
    struct ClusterTransactionResult {
        ClusterTransactionStatus status = ClusterTransactionStatus::UNKNOWN;
        bool finished = false;
    };
}

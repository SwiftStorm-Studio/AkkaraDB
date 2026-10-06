/*
 * AkkaraDB - The all-purpose KV store
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
#pragma once

#include "akk/engine/cluster/ClusterConfig.hpp"
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace akkaradb::engine::cluster {
    enum class ClusterRoutingCode : uint8_t {
        NOT_OWNER = 0, NO_TARGET = 1, CROSS_OWNER_BATCH = 2,
        FORWARD_UNAVAILABLE = 3, OUTCOME_UNKNOWN = 4, PAYLOAD_TOO_LARGE = 5, LOCAL_ONLY = 6,
    };

    struct ClusterRouteTarget {
        uint64_t nodeId = 0;
        std::string host;
        uint16_t tcpPort = 0;
        uint16_t httpPort = 0;
        uint16_t grpcPort = 0;
        uint16_t replPort = 0;
        uint64_t configurationEpoch = 0;
        uint64_t raftTerm = 0;
    };

    class ClusterRoutingError : public std::runtime_error {
    public:
        ClusterRoutingError(ClusterRoutingCode code, ClusterRouteTarget target = {},
            std::string message = "Cluster operation requires another node")
            : std::runtime_error(std::move(message)), code(code), target(std::move(target)) {}
        ClusterRoutingCode code;
        ClusterRouteTarget target;
        [[nodiscard]] bool outcomeUnknown() const noexcept { return code == ClusterRoutingCode::OUTCOME_UNKNOWN; }
    };

    inline std::string_view routingCodeName(ClusterRoutingCode code) noexcept {
        switch (code) {
            case ClusterRoutingCode::NOT_OWNER: return "NOT_OWNER";
            case ClusterRoutingCode::NO_TARGET: return "NO_TARGET";
            case ClusterRoutingCode::CROSS_OWNER_BATCH: return "CROSS_OWNER_BATCH";
            case ClusterRoutingCode::FORWARD_UNAVAILABLE: return "FORWARD_UNAVAILABLE";
            case ClusterRoutingCode::OUTCOME_UNKNOWN: return "OUTCOME_UNKNOWN";
            case ClusterRoutingCode::PAYLOAD_TOO_LARGE: return "PAYLOAD_TOO_LARGE";
            case ClusterRoutingCode::LOCAL_ONLY: return "LOCAL_ONLY";
        }
        return "UNKNOWN";
    }

    inline std::string routingErrorJson(const ClusterRoutingError& error) {
        const auto quote = [](std::string_view value) {
            std::string result = "\"";
            constexpr char hex[] = "0123456789abcdef";
            for (const unsigned char c : value) {
                if (c == '"' || c == '\\') { result += '\\'; result += static_cast<char>(c); }
                else if (c < 32) { result += "\\u00"; result += hex[c >> 4]; result += hex[c & 15]; }
                else { result += static_cast<char>(c); }
            }
            return result + '"';
        };
        const auto& t = error.target;
        return "{\"code\":" + quote(routingCodeName(error.code)) +
            ",\"outcomeUnknown\":" + (error.outcomeUnknown() ? "true" : "false") +
            ",\"nodeId\":" + std::to_string(t.nodeId) + ",\"host\":" + quote(t.host) +
            ",\"tcpPort\":" + std::to_string(t.tcpPort) + ",\"httpPort\":" + std::to_string(t.httpPort) +
            ",\"grpcPort\":" + std::to_string(t.grpcPort) + ",\"replPort\":" + std::to_string(t.replPort) +
            ",\"configurationEpoch\":" + std::to_string(t.configurationEpoch) +
            ",\"raftTerm\":" + std::to_string(t.raftTerm) + ",\"message\":" + quote(error.what()) + "}";
    }
}

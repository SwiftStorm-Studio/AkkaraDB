/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
#pragma once
#include "akkaradb/Stats.hpp"

namespace akkaradb::engine::cluster::detail {
    inline ClusterOperationAvailability combineAvailability(ClusterOperationAvailability a, ClusterOperationAvailability b) {
        using enum ClusterOperationAvailability;
        if (a == UNAVAILABLE || b == UNAVAILABLE) { return UNAVAILABLE; }
        if (a == UNKNOWN || b == UNKNOWN) { return UNKNOWN; }
        return AVAILABLE;
    }

    // Enum ordinals do not represent health severity (REBUILDING follows FAILED).
    inline ClusterHealthState combineHealth(ClusterHealthState a, ClusterHealthState b) {
        using enum ClusterHealthState;
        if (a == FAILED || b == FAILED) { return FAILED; }
        if (a == DEGRADED || b == DEGRADED) { return DEGRADED; }
        if (a == REBUILDING || b == REBUILDING) { return REBUILDING; }
        return HEALTHY;
    }

    template <typename Destination, typename Source>
    void mergeHealthObservation(Destination& out, const Source& source) {
        if (source.sampledAtUs > out.sampledAtUs) { out.sampledAtUs = source.sampledAtUs; }
        out.health = combineHealth(out.health, source.health);
        out.quorumAvailability = combineAvailability(out.quorumAvailability, source.quorumAvailability);
        out.writeAvailability = combineAvailability(out.writeAvailability, source.writeAvailability);
        out.linearizableReadAvailability = combineAvailability(out.linearizableReadAvailability, source.linearizableReadAvailability);
        if (source.lastFailureAtUs > out.lastFailureAtUs) {
            out.lastFailure = source.lastFailure; out.lastFailureAtUs = source.lastFailureAtUs;
        }
    }
}

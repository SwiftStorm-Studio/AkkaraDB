

// akkengine/include/akk/engine/cluster/ClusterRouter.hpp
/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/cluster/ClusterRouter.hpp
#pragma once

#include "akk/engine/cluster/AkkClusterRuntimeExport.hpp"
#include "akk/engine/cluster/ClusterConfig.hpp"

#include <cstdint>
#include <span>
#include <vector>

namespace akkaradb::engine::cluster {
    /**
     * ClusterRouter - Maps keys to data-bearing nodes.
     *
     * The router is a pure, in-memory view over ClusterConfig.  It does not
     * perform I/O and does not observe runtime health.  Mirror mode returns all
     * data-bearing nodes; Stripe mode returns the deterministic rendezvous-hash
     * owner for the key.
     */
    class AKKARADB_CLUSTER_RUNTIME_API ClusterRouter {
        public:
            /**
             * Builds a router from a validated cluster config.
             *
             * @throws std::invalid_argument if config is invalid.
             */
            explicit ClusterRouter(ClusterConfig config);

            /**
             * Returns nodes that should receive a write for key.
             *
             * @throws std::runtime_error if routing requires a data-bearing node
             *         but none is configured.
             */
            [[nodiscard]] std::vector<NodeInfo> writeTargets(std::span<const uint8_t> key) const;

            /**
             * Returns nodes that can satisfy a read for key.
             *
             * The current implementation mirrors write placement.  Callers may
             * choose among returned candidates according to freshness or locality.
             *
             * @throws std::runtime_error if routing requires a data-bearing node
             *         but none is configured.
             */
            [[nodiscard]] std::vector<NodeInfo> readCandidates(std::span<const uint8_t> key) const;

        private:
            /** Returns the rendezvous-hash owner for key in Stripe mode. */
            [[nodiscard]] NodeInfo stripeTarget(std::span<const uint8_t> key) const;

            ClusterConfig config_;
            std::vector<NodeInfo> dataNodes_;
    };
} // namespace akkaradb::engine::cluster

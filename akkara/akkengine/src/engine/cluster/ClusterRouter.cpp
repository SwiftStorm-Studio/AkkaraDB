/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

// akkengine/src/engine/cluster/ClusterRouter.cpp
#include "akk/engine/cluster/ClusterRouter.hpp"

#include <limits>
#include <stdexcept>

namespace akkaradb::engine::cluster {
    namespace {
        uint64_t fnv1a64(std::span<const uint8_t> bytes, uint64_t seed = 14695981039346656037ull) noexcept {
            uint64_t hash = seed;
            for (uint8_t b : bytes) {
                hash ^= b;
                hash *= 1099511628211ull;
            }
            return hash;
        }

        uint64_t rendezvousScore(std::span<const uint8_t> key, uint64_t nodeId) noexcept {
            uint8_t idBytes[8];
            for (size_t i = 0; i < 8; ++i) { idBytes[i] = static_cast<uint8_t>(nodeId >> (8 * i)); }
            return fnv1a64(std::span<const uint8_t>(idBytes, 8), fnv1a64(key));
        }
    } // namespace

    ClusterRouter::ClusterRouter(ClusterConfig config) : config_{std::move(config)}, dataNodes_{config_.dataNodes()} { config_.validate(); }

    std::vector<NodeInfo> ClusterRouter::writeTargets(std::span<const uint8_t> key) const {
        switch (config_.mode()) {
            case ReplicationMode::STANDALONE: return dataNodes_.empty()
                                                         ? std::vector<NodeInfo>{}
                                                         : std::vector<NodeInfo>{dataNodes_.front()};
            case ReplicationMode::MIRROR: return dataNodes_;
            case ReplicationMode::STRIPE: return {stripeTarget(key)};
        }
        throw std::logic_error("ClusterRouter: invalid replication mode");
    }

    std::vector<NodeInfo> ClusterRouter::readCandidates(std::span<const uint8_t> key) const {
        switch (config_.mode()) {
            case ReplicationMode::STANDALONE: return dataNodes_.empty()
                                                         ? std::vector<NodeInfo>{}
                                                         : std::vector<NodeInfo>{dataNodes_.front()};
            case ReplicationMode::MIRROR: return dataNodes_;
            case ReplicationMode::STRIPE: return {stripeTarget(key)};
        }
        throw std::logic_error("ClusterRouter: invalid replication mode");
    }

    NodeInfo ClusterRouter::stripeTarget(std::span<const uint8_t> key) const {
        if (dataNodes_.empty()) { throw std::runtime_error("ClusterRouter: no data-bearing nodes"); }

        const NodeInfo* best = nullptr;
        uint64_t bestScore = 0;
        for (const auto& node : dataNodes_) {
            const uint64_t score = rendezvousScore(key, node.nodeId);
            if (best == nullptr || score > bestScore || (score == bestScore && node.nodeId < best->nodeId)) {
                best = &node;
                bestScore = score;
            }
        }
        return *best;
    }
} // namespace akkaradb::engine::cluster

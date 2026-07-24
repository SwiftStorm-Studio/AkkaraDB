/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/cluster/ClusterRouter.cpp
#include "akk/engine/cluster/ClusterRouter.hpp"

#include <algorithm>
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
            case ReplicationMode::PARTITIONED: return {partitionTarget(key)};
            case ReplicationMode::STRIPE: {
                std::vector<NodeInfo> out;
                const auto targets = stripeShardTargets(key);
                out.reserve(targets.size());
                for (const auto& target : targets) { out.push_back(target.node); }
                return out;
            }
        }
        throw std::logic_error("ClusterRouter: invalid replication mode");
    }

    std::vector<NodeInfo> ClusterRouter::readCandidates(std::span<const uint8_t> key) const {
        switch (config_.mode()) {
            case ReplicationMode::STANDALONE: return dataNodes_.empty()
                                                         ? std::vector<NodeInfo>{}
                                                         : std::vector<NodeInfo>{dataNodes_.front()};
            case ReplicationMode::MIRROR: return dataNodes_;
            case ReplicationMode::PARTITIONED: return {partitionTarget(key)};
            case ReplicationMode::STRIPE: {
                std::vector<NodeInfo> out;
                const auto targets = stripeShardTargets(key);
                out.reserve(targets.size());
                for (const auto& target : targets) { out.push_back(target.node); }
                return out;
            }
        }
        throw std::logic_error("ClusterRouter: invalid replication mode");
    }

    std::vector<ClusterRouter::StripeShardTarget> ClusterRouter::stripeShardTargets(std::span<const uint8_t> key) const {
        const uint16_t totalShards = config_.stripe().totalShards();
        if (totalShards == 0) { throw std::runtime_error("ClusterRouter: invalid stripe shard count"); }
        if (dataNodes_.size() < totalShards) { throw std::runtime_error("ClusterRouter: not enough data-bearing nodes for stripe"); }

        std::vector<std::pair<uint64_t, NodeInfo>> scored;
        scored.reserve(dataNodes_.size());
        for (const auto& node : dataNodes_) { scored.emplace_back(rendezvousScore(key, node.nodeId), node); }
        std::sort(
            scored.begin(),
            scored.end(),
            [](const auto& left, const auto& right) {
                if (left.first != right.first) { return left.first > right.first; }
                return left.second.nodeId < right.second.nodeId;
            }
        );

        std::vector<StripeShardTarget> out;
        out.reserve(totalShards);
        for (uint16_t i = 0; i < totalShards; ++i) { out.push_back(StripeShardTarget{.shardIndex = i, .node = scored[i].second}); }
        return out;
    }

    NodeInfo ClusterRouter::partitionTarget(std::span<const uint8_t> key) const {
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

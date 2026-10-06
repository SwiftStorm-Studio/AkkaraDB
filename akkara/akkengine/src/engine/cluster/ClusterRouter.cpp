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
#include "akk/engine/cluster/detail/ClusterPlacement.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>

namespace akkaradb::engine::cluster {
    using detail::rendezvousScore;
    using detail::rankNodes;

    ClusterRouter::ClusterRouter(ClusterConfig config) { reconfigure(std::move(config)); }

    void ClusterRouter::reconfigure(ClusterConfig config) {
        config.validate();
        config_.store(std::make_shared<const ClusterConfig>(std::move(config)));
    }

    std::vector<NodeInfo> ClusterRouter::writeTargets(std::span<const uint8_t> key) const {
        const auto view = config_.load();
        const auto& config = *view;
        const auto dataNodes = config.mode() == ReplicationMode::STRIPE ? config.stripePlacementNodes() : config.dataNodes();
        switch (config.mode()) {
            case ReplicationMode::STANDALONE: return dataNodes.empty()
                                                         ? std::vector<NodeInfo>{}
                                                         : std::vector<NodeInfo>{dataNodes.front()};
            case ReplicationMode::MIRROR: return dataNodes;
            case ReplicationMode::PARTITIONED: return partitionTargets(key);
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
        const auto view = config_.load();
        const auto& config = *view;
        const auto dataNodes = config.mode() == ReplicationMode::STRIPE ? config.stripePlacementNodes() : config.dataNodes();
        switch (config.mode()) {
            case ReplicationMode::STANDALONE: return dataNodes.empty()
                                                         ? std::vector<NodeInfo>{}
                                                         : std::vector<NodeInfo>{dataNodes.front()};
            case ReplicationMode::MIRROR: return dataNodes;
            case ReplicationMode::PARTITIONED: return partitionTargets(key);
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
        const auto view = config_.load();
        const auto& config = *view;
        const auto dataNodes = config.mode() == ReplicationMode::STRIPE ? config.stripePlacementNodes() : config.dataNodes();
        const uint16_t totalShards = config.stripe().totalShards();
        const uint8_t copiesPerShard = config.stripe().copiesPerShard;
        const uint16_t totalPlacements = config.stripe().totalPlacements();
        if (totalShards == 0) { throw std::runtime_error("ClusterRouter: invalid stripe shard count"); }
        if (dataNodes.size() < totalPlacements) { throw std::runtime_error("ClusterRouter: not enough data-bearing nodes for stripe"); }

        if (copiesPerShard == 2) {
            struct MirrorPair {
                uint64_t score = 0;
                NodeInfo first;
                NodeInfo second;
            };
            std::vector<MirrorPair> pairs;
            pairs.reserve(totalShards);
            for (uint16_t i = 0; i < totalShards; ++i) {
                auto first = dataNodes[i * 2];
                auto second = dataNodes[i * 2 + 1];
                const uint64_t firstScore = rendezvousScore(key, first.nodeId);
                const uint64_t secondScore = rendezvousScore(key, second.nodeId);
                if (secondScore > firstScore || (secondScore == firstScore && second.nodeId < first.nodeId)) {
                    std::swap(first, second);
                }
                const auto pairIds = std::minmax(first.nodeId, second.nodeId);
                pairs.push_back(MirrorPair{.score = rendezvousScore(key, pairIds.first, pairIds.second), .first = std::move(first), .second = std::move(second)});
            }
            std::ranges::sort(pairs, [](const auto& left, const auto& right) {
                if (left.score != right.score) { return left.score > right.score; }
                return std::min(left.first.nodeId, left.second.nodeId) < std::min(right.first.nodeId, right.second.nodeId);
            });

            std::vector<StripeShardTarget> out;
            out.reserve(totalPlacements);
            for (uint16_t shardIndex = 0; shardIndex < pairs.size(); ++shardIndex) {
                out.push_back(StripeShardTarget{.shardIndex = shardIndex, .replicaIndex = 0, .node = pairs[shardIndex].first});
                out.push_back(StripeShardTarget{.shardIndex = shardIndex, .replicaIndex = 1, .node = pairs[shardIndex].second});
            }
            return out;
        }

        const auto scored = rankNodes(key, dataNodes);

        std::vector<StripeShardTarget> out;
        out.reserve(totalShards);
        for (uint16_t i = 0; i < totalShards; ++i) {
            out.push_back(StripeShardTarget{.shardIndex = i, .replicaIndex = 0, .node = scored[i].second});
        }
        return out;
    }

    std::vector<NodeInfo> ClusterRouter::partitionTargets(std::span<const uint8_t> key) const {
        const auto view = config_.load();
        const auto& config = *view;
        const auto dataNodes = config.mode() == ReplicationMode::STRIPE ? config.stripePlacementNodes() : config.dataNodes();
        if (dataNodes.empty()) { throw std::runtime_error("ClusterRouter: no data-bearing nodes"); }

        return detail::partitionTargets(config, key);
    }
} // namespace akkaradb::engine::cluster

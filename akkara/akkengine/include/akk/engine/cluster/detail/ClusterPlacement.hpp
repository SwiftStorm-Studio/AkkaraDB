/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
#pragma once

#include "akk/engine/cluster/ClusterConfig.hpp"
#include "akk/crypto/Random.hpp"
#include <algorithm>
#include <stdexcept>

namespace akkaradb::engine::cluster::detail {
    inline uint16_t partitionForKey(std::span<const uint8_t> key, uint16_t count) {
        if (count == 0) { throw std::invalid_argument("Cluster placement: partitionCount must be nonzero"); }
        constexpr std::array<uint8_t, 5> domain{'A', 'K', 'P', 'T', '1'};
        const std::array parts{std::span<const uint8_t>{domain}, key};
        const auto digest = crypto::hash256(parts);
        const uint32_t hash = static_cast<uint32_t>(digest[0]) | (static_cast<uint32_t>(digest[1]) << 8) |
            (static_cast<uint32_t>(digest[2]) << 16) | (static_cast<uint32_t>(digest[3]) << 24);
        return static_cast<uint16_t>(hash % count);
    }
    inline uint64_t rendezvousScore(std::span<const uint8_t> key, uint64_t nodeId, uint64_t pairMember = 0) {
        constexpr std::array<uint8_t, 6> domain{'A', 'K', 'H', 'R', 'W', '1'};
        std::array<uint8_t, 8> first{}, second{};
        for (size_t i = 0; i < 8; ++i) {
            first[i] = static_cast<uint8_t>(nodeId >> (8 * i));
            second[i] = static_cast<uint8_t>(pairMember >> (8 * i));
        }
        // hash256 length-prefixes each part, separating arbitrary binary keys
        // from node identities. Zero pairMember denotes a single node.
        const std::array parts{std::span<const uint8_t>{domain}, key,
            std::span<const uint8_t>{first}, std::span<const uint8_t>{second}};
        const auto digest = crypto::hash256(parts);
        uint64_t score = 0;
        for (size_t i = 0; i < 8; ++i) { score |= static_cast<uint64_t>(digest[i]) << (8 * i); }
        return score;
    }

    inline auto rankNodes(std::span<const uint8_t> key, const std::vector<NodeInfo>& nodes) {
        std::vector<std::pair<uint64_t, NodeInfo>> ranked;
        ranked.reserve(nodes.size());
        for (const auto& node : nodes) { ranked.emplace_back(rendezvousScore(key, node.nodeId), node); }
        std::ranges::sort(ranked, [](const auto& left, const auto& right) {
            if (left.first != right.first) { return left.first > right.first; }
            return left.second.nodeId < right.second.nodeId;
        });
        return ranked;
    }

    inline std::vector<NodeInfo> partitionTargets(const ClusterConfig& config, std::span<const uint8_t> key) {
        std::array<uint8_t, 2> group{};
        if (config.usesDataConsensus()) {
            const auto id = partitionForKey(key, config.partition().partitionCount);
            group = {static_cast<uint8_t>(id), static_cast<uint8_t>(id >> 8)};
            key = group;
        }
        auto members = config.dataNodes();
        if (config.usesDataConsensus()) { std::erase_if(members, [](const auto& node) { return node.raftLearner(); }); }
        const auto ranked = rankNodes(key, members);
        std::vector<NodeInfo> targets;
        const auto copies = config.partitionCopies();
        targets.reserve(copies);
        for (size_t i = 0; i < copies; ++i) { targets.push_back(ranked[i].second); }
        return targets;
    }
}

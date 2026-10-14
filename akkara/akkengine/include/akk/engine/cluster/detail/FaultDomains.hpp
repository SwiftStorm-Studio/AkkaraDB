/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
#pragma once
#include "akk/engine/cluster/ClusterConfig.hpp"
#include <algorithm>
#include <map>
#include <set>
#include <stdexcept>

namespace akkaradb::engine::cluster::detail {
    inline bool siteQuorumSurvives(const std::vector<NodeInfo>& nodes) {
        if (nodes.empty()) { return false; }
        std::map<std::string, size_t> counts;
        for (const auto& node : nodes) {
            if (node.siteId.empty()) { return false; }
            ++counts[node.siteId];
        }
        const size_t quorum = nodes.size() / 2 + 1;
        return std::ranges::all_of(counts, [&](const auto& entry) { return nodes.size() - entry.second >= quorum; });
    }

    inline void validateFaultDomains(const std::vector<NodeInfo>& nodes, FaultDomainOptions options, bool voters = false) {
        std::set<std::string> hosts, sites;
        std::map<std::string, std::string> physicalHosts;
        for (const auto& node : nodes) {
            if (!node.hostId.empty()) {
                const auto [it, inserted] = physicalHosts.emplace(node.hostId, node.siteId);
                if (!inserted && it->second != node.siteId) { throw std::invalid_argument("Cluster placement: one physical host belongs to multiple sites"); }
            }
            if (options.minHosts && node.hostId.empty()) { throw std::invalid_argument("Cluster placement: missing physical hostId"); }
            if ((options.minSites || options.requireSiteQuorum) && node.siteId.empty()) {
                throw std::invalid_argument("Cluster placement: missing siteId");
            }
            if (!node.hostId.empty()) { hosts.insert(node.hostId); }
            if (!node.siteId.empty()) { sites.insert(node.siteId); }
        }
        if (hosts.size() < options.minHosts || sites.size() < options.minSites) {
            throw std::invalid_argument("Cluster placement: insufficient distinct hosts or sites");
        }
        if (voters && options.requireSiteQuorum && !siteQuorumSurvives(nodes)) {
            throw std::invalid_argument("Cluster placement: one site failure would lose the Raft majority");
        }
    }

    // Input is already in deterministic rendezvous order. Select distinct sites
    // and hosts first, then fill remaining slots without exceeding the site cap.
    inline std::vector<NodeInfo> selectFaultDomainNodes(const std::vector<NodeInfo>& ranked, size_t count,
        FaultDomainOptions options, bool voters = false) {
        if (count > ranked.size() || options.minHosts > count || options.minSites > count) {
            throw std::invalid_argument("Cluster placement: fault-domain requirements exceed placement size");
        }
        if (!options.minHosts && !options.minSites && !(voters && options.requireSiteQuorum)) {
            return {ranked.begin(), ranked.begin() + static_cast<std::ptrdiff_t>(count)};
        }
        std::vector<NodeInfo> result;
        std::set<uint64_t> selected;
        std::set<std::string> hosts, sites;
        std::map<std::string, size_t> siteCounts;
        const size_t siteCap = voters && options.requireSiteQuorum ? count - (count / 2 + 1) : count;
        const auto take = [&](const NodeInfo& node) {
            result.push_back(node); selected.insert(node.nodeId);
            hosts.insert(node.hostId); sites.insert(node.siteId); ++siteCounts[node.siteId];
        };
        const auto eligible = [&](const NodeInfo& node) {
            return !selected.contains(node.nodeId) && siteCounts[node.siteId] < siteCap;
        };
        if (voters) {
            const auto leader = std::ranges::find_if(ranked, [&](const auto& node) { return node.coordinatorEligible() && eligible(node); });
            if (leader == ranked.end()) { throw std::invalid_argument("Cluster placement: no eligible coordinator satisfies the site constraint"); }
            take(*leader);
        }
        for (const auto& node : ranked) {
            if (sites.size() >= options.minSites) { break; }
            if (eligible(node) && !sites.contains(node.siteId)) { take(node); }
        }
        for (const auto& node : ranked) {
            if (hosts.size() >= options.minHosts) { break; }
            if (eligible(node) && !hosts.contains(node.hostId)) { take(node); }
        }
        for (const auto& node : ranked) {
            if (result.size() == count) { break; }
            if (eligible(node)) { take(node); }
        }
        if (result.size() != count) { throw std::invalid_argument("Cluster placement: no placement satisfies the site quorum constraint"); }
        validateFaultDomains(result, options, voters);
        // Keep ownership determined by rendezvous rank within the selected set.
        result.clear();
        for (const auto& node : ranked) { if (selected.contains(node.nodeId)) { result.push_back(node); } }
        return result;
    }

    inline std::vector<NodeInfo> raidPairNodes(const ClusterConfig& config) {
        auto nodes = config.stripePlacementNodes();
        const auto options = config.faultDomains();
        if (options.minHosts <= 1 && options.minSites <= 1) { return nodes; }
        std::map<std::string, std::vector<NodeInfo>> groups;
        for (auto& node : nodes) { groups[options.minSites > 1 ? node.siteId : node.hostId].push_back(std::move(node)); }
        for (auto& [domain, members] : groups) { (void)domain; std::ranges::sort(members, std::greater{}, &NodeInfo::nodeId); }
        std::vector<NodeInfo> result;
        while (result.size() < nodes.size()) {
            auto first = groups.end(), second = groups.end();
            for (auto it = groups.begin(); it != groups.end(); ++it) {
                if (it->second.empty()) { continue; }
                if (first == groups.end() || it->second.size() > first->second.size()) { second = first; first = it; }
                else if (second == groups.end() || it->second.size() > second->second.size()) { second = it; }
            }
            if (second == groups.end()) { throw std::invalid_argument("Cluster placement: RAID.10 cannot separate every mirror pair"); }
            result.push_back(std::move(first->second.back())); first->second.pop_back();
            result.push_back(std::move(second->second.back())); second->second.pop_back();
        }
        return result;
    }

    inline void validateStripeFaultDomains(const ClusterConfig& config, const std::vector<uint64_t>& ids) {
        if (ids.size() != config.stripe().totalPlacements()) { throw std::invalid_argument("Cluster placement: invalid stripe holder count"); }
        std::vector<NodeInfo> nodes;
        std::set<uint64_t> unique;
        for (const auto id : ids) {
            const auto* node = config.findById(id);
            if (!node || !unique.insert(id).second) { throw std::invalid_argument("Cluster placement: invalid stripe holder"); }
            nodes.push_back(*node);
        }
        validateFaultDomains(nodes, config.faultDomains());
        if (config.stripe().copiesPerShard == 2) {
            for (size_t i = 0; i + 1 < nodes.size(); i += 2) {
                if ((config.faultDomains().minHosts > 1 && nodes[i].hostId == nodes[i + 1].hostId) ||
                    (config.faultDomains().minSites > 1 && nodes[i].siteId == nodes[i + 1].siteId)) {
                    throw std::invalid_argument("Cluster placement: RAID.10 mirror pair shares a required fault domain");
                }
            }
        }
    }
}

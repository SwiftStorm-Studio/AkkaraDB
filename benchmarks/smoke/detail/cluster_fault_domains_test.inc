/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
void testClusterFaultDomains() {
    std::vector<NodeInfo> members;
    for (uint64_t id = 1; id <= 6; ++id) {
        auto member = node(id, static_cast<uint16_t>(31800 + id), static_cast<uint16_t>(31810 + id));
        member.partitionReplBasePort = static_cast<uint16_t>(31900 + 8 * id);
        member.hostId = "host-" + std::to_string(id);
        member.siteId = "site-" + std::to_string((id - 1) / 2);
        members.push_back(std::move(member));
    }
    const auto make = [&](const auto& nodes, ReplicationMode mode, FaultDomainOptions domains, StripeOptions stripe = {}) {
        return ClusterConfig{nodes, mode, {}, ConsistencyOptions{.mode = mode == ReplicationMode::STRIPE ? ConsistencyMode::PRIMARY_ACK : ConsistencyMode::RAFT_QUORUM},
            {}, stripe, 0, testClusterId(9620), PartitionOptions{.replicationFactor = 3, .partitionCount = 4}, FailoverPolicy::DEFAULT, domains};
    };
    const FaultDomainOptions policy{.minHosts = 3, .minSites = 3, .requireSiteQuorum = true};
    auto config = make(members, ReplicationMode::PARTITIONED, policy);
    auto restored = ClusterConfig::decode(config.encode());
    AKK_CLUSTER_CHECK(restored.faultDomains() == policy);
    AKK_CLUSTER_CHECK(restored.nodes()[2].hostId == members[2].hostId && restored.nodes()[2].siteId == members[2].siteId);
    AKK_CLUSTER_CHECK(restored.replicationFingerprint() == config.replicationFingerprint());
    AKK_CLUSTER_CHECK(restored.availability().failureDomainsKnown);
    AKK_CLUSTER_CHECK(restored.availability().singleSiteDataSurvival && restored.availability().singleSiteQuorumSurvival);
    ClusterRouter router{restored};
    std::ranges::reverse(members);
    ClusterRouter reordered{make(members, ReplicationMode::PARTITIONED, policy)};
    for (uint32_t i = 0; i < 256; ++i) {
        const auto key = std::to_string(i); const auto targets = router.writeTargets(bytesOf(key));
        AKK_CLUSTER_CHECK(targets.size() == 3);
        std::set<std::string> sites, hosts;
        for (const auto& target : targets) { sites.insert(target.siteId); hosts.insert(target.hostId); }
        AKK_CLUSTER_CHECK(sites.size() == 3 && hosts.size() == 3);
        const auto other = reordered.writeTargets(bytesOf(key));
        for (size_t j = 0; j < targets.size(); ++j) { AKK_CLUSTER_CHECK(targets[j].nodeId == other[j].nodeId); }
    }
    const auto rejects = [&](auto action) {
        try { action(); } catch (const std::invalid_argument&) { return true; } return false;
    };
    auto sameSite = members;
    for (auto& n : sameSite) { n.siteId = "one-site"; }
    AKK_CLUSTER_CHECK(rejects([&] { (void)make(sameSite, ReplicationMode::PARTITIONED, policy); }));
    auto missing = members; missing.front().hostId.clear();
    AKK_CLUSTER_CHECK(rejects([&] { (void)make(missing, ReplicationMode::PARTITIONED, policy); }));
    auto inconsistent = members; inconsistent[1].hostId = inconsistent[0].hostId; inconsistent[1].siteId = "other-site";
    AKK_CLUSTER_CHECK(rejects([&] { (void)make(inconsistent, ReplicationMode::PARTITIONED, policy); }));
    std::vector<NodeInfo> twoSites{members[0], members[1], members[2]};
    const auto twoSiteConfig = make(twoSites, ReplicationMode::MIRROR, {.minHosts = 3, .minSites = 2});
    AKK_CLUSTER_CHECK(twoSiteConfig.availability().singleSiteDataSurvival);
    AKK_CLUSTER_CHECK(!twoSiteConfig.availability().singleSiteQuorumSurvival);
    AKK_CLUSTER_CHECK(rejects([&] { (void)make(twoSites, ReplicationMode::MIRROR, {.minHosts = 3, .minSites = 2, .requireSiteQuorum = true}); }));
    auto unknown = members; for (auto& n : unknown) { n.hostId.clear(); n.siteId.clear(); }
    AKK_CLUSTER_CHECK(!make(unknown, ReplicationMode::MIRROR, {}).availability().failureDomainsKnown);
    auto changed = members; changed.front().hostId += "changed";
    AKK_CLUSTER_CHECK(rejects([&] { config.validatePlacementChange(make(changed, ReplicationMode::PARTITIONED, policy)); }));
    AKK_CLUSTER_CHECK(rejects([&] { config.validatePlacementChange(make(members, ReplicationMode::PARTITIONED, {.minHosts = 2, .minSites = 3, .requireSiteQuorum = true})); }));

    auto raid = members;
    for (auto& n : raid) { n.partitionReplBasePort = 0; }
    const auto mirrored = make(raid, ReplicationMode::STRIPE, {.minHosts = 2, .minSites = 2, .requireSiteQuorum = true},
        {.dataShards = 3, .parityShards = 0, .copiesPerShard = 2});
    ClusterRouter mirrorRouter{mirrored};
    auto shards = mirrorRouter.stripeShardTargets(bytesOf("pairs"));
    for (size_t i = 0; i < shards.size(); i += 2) {
        AKK_CLUSTER_CHECK(shards[i].node.siteId != shards[i + 1].node.siteId);
        AKK_CLUSTER_CHECK(shards[i].node.hostId != shards[i + 1].node.hostId);
    }
    AKK_CLUSTER_CHECK(mirrored.availability().singleSiteDataSurvival && mirrored.availability().singleSiteQuorumSurvival);
    std::vector<uint64_t> ids; for (const auto& shard : shards) { ids.push_back(shard.node.nodeId); }
    const auto original = ids;
    auto spare = node(7, 31807, 31817); spare.hostId = "spare"; spare.siteId = shards[1].node.siteId;
    spare.capabilities |= STRIPE_FAILOVER_ELIGIBLE; raid.push_back(spare);
    const auto withSpare = make(raid, ReplicationMode::STRIPE, {.minHosts = 2, .minSites = 2}, {.dataShards = 3, .parityShards = 0, .copiesPerShard = 2});
    ids[0] = spare.nodeId;
    AKK_CLUSTER_CHECK(rejects([&] { akkaradb::engine::cluster::detail::validateStripeFaultDomains(withSpare, ids); }));
    akkaradb::engine::cluster::detail::validateStripeFaultDomains(withSpare, original);
    auto encoded = config.encode(); encoded.pop_back();
    bool malformed = false; try { (void)ClusterConfig::decode(encoded); } catch (const std::exception&) { malformed = true; }
    AKK_CLUSTER_CHECK(malformed);
    auto oldLayout = config.encode(); oldLayout[7] &= 0x7f;
    std::fill(oldLayout.begin() + 24, oldLayout.begin() + 28, 0);
    const auto crc = akkaradb::cpu::CRC32C(reinterpret_cast<const std::byte*>(oldLayout.data()), oldLayout.size());
    for (size_t i = 0; i < 4; ++i) { oldLayout[24 + i] = static_cast<uint8_t>(crc >> (8 * i)); }
    bool oldRejected = false;
    try { (void)ClusterConfig::decode(oldLayout); }
    catch (const std::runtime_error& error) { oldRejected = std::string_view{error.what()}.find("fault-domain layout") != std::string_view::npos; }
    AKK_CLUSTER_CHECK(oldRejected);
    // Three physical sites retain quorum. Removing one voter must be rejected
    // before committing a topology which would lose quorum with one more site.
    const auto dir = makeTempDir("fault-domain-membership");
    auto voters = std::vector{members[0], members[2], members[4]};
    for (auto& n : voters) { n.partitionReplBasePort = 0; }
    const ClusterConfig live{voters, ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 1000},
        onlineRaftMembership(), {}, 0, testClusterId(9621), {}, FailoverPolicy::DEFAULT, policy};
    std::vector<std::unique_ptr<RuntimeHarness>> runtimes;
    const auto open = [&] {
        for (const auto& n : voters) { runtimes.push_back(makeRuntime(dir / std::to_string(n.nodeId), live, n.nodeId)); runtimes.back()->runtime->start(); }
    };
    open();
    RuntimeHarness* all[]{runtimes[0].get(), runtimes[1].get(), runtimes[2].get()};
    AKK_CLUSTER_CHECK(waitUntil([&] { return findLeader(all) != nullptr; }));
    auto* leader = findLeader(all);
    const auto removed = voters[0].nodeId == leader->nodeId ? voters[1].nodeId : voters[0].nodeId;
    AKK_CLUSTER_CHECK(rejects([&] { leader->runtime->removeRaftVotingNode(removed); }));
    leader->runtime->shipEntry(1, ReplOpType::PUT, bytesOf("domains"), bytesOf("kept"), 0, leader->nodeId);
    for (auto& runtime : runtimes) { runtime->runtime->close(); }
    runtimes.clear(); open();
    for (const auto& runtime : runtimes) {
        const auto active = runtime->runtime->activeNodes();
        AKK_CLUSTER_CHECK(active.size() == 3);
        for (const auto& member : active) {
            const auto* expected = live.findById(member.nodeId);
            AKK_CLUSTER_CHECK(expected && expected->hostId == member.hostId && expected->siteId == member.siteId);
        }
    }
    for (auto& runtime : runtimes) { runtime->runtime->close(); }

}

void testClusterWriteOutcomeContract() {
    testRetrySafeEngineWrites();
    testRetryAcrossSnapshotAndLeaderChange();
    testPrimaryAckFailWriteCommitsLocalBeforeAckFailure();
    testForwardTimeoutNeverReplays();
    ForwardResponse rejection; rejection.errorCode = ClusterRoutingCode::WRITE_REJECTED;
    const auto encoded = encodeForwardResponse(rejection); ForwardResponse decoded;
    DecodedFrame frame;
    AKK_CLUSTER_CHECK(decodeFrame(encoded, frame) && decodeForwardResponse(frame.payload, decoded));
    AKK_CLUSTER_CHECK(decoded.errorCode == ClusterRoutingCode::WRITE_REJECTED);
}

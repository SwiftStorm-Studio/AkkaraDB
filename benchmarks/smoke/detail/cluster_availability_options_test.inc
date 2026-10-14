void testClusterAvailabilityOptions() {
    auto nodes = std::vector{node(1, 31301, 31311), node(2, 31302, 31312), node(3, 31303, 31313), node(4, 31304, 31314),
        node(5, 31305, 31315, COORDINATOR_ELIGIBLE | STRIPE_METADATA_VOTER),
        node(6, 31306, 31316, COORDINATOR_ELIGIBLE | STRIPE_METADATA_VOTER),
        node(7, 31307, 31317, COORDINATOR_ELIGIBLE | STRIPE_METADATA_VOTER)};
    StripeOptions stripe{.dataShards = 2, .parityShards = 2, .metadataPlacement = StripeMetadataPlacement::EXPLICIT_VOTERS};
    const auto make = [&](const auto& members, StripeOptions options) {
        return ClusterConfig{members, ReplicationMode::STRIPE, {}, {}, {}, options, 0, testClusterId(9591)};
    };
    const auto config = make(nodes, stripe);
    const auto restored = ClusterConfig::decode(config.encode());
    AKK_CLUSTER_CHECK(restored.replicationFingerprint() == config.replicationFingerprint());
    const auto facts = restored.availability();
    AKK_CLUSTER_CHECK(facts.metadataVoters == std::vector<uint64_t>({5, 6, 7}));
    AKK_CLUSTER_CHECK(facts.metadataLearners == std::vector<uint64_t>({1, 2, 3, 4}));
    AKK_CLUSTER_CHECK(facts.metadataQuorum == 2 && facts.metadataFailureTolerance == 1 && facts.dataFailureTolerance == 2);
    AKK_CLUSTER_CHECK(std::ranges::contains(facts.warnings, ClusterAvailabilityWarning::METADATA_TOLERANCE_BELOW_DATA));
    const auto rejects = [&](const auto& members, StripeOptions options) {
        try { (void)make(members, options); } catch (const std::invalid_argument&) { return true; }
        return false;
    };
    auto bad = stripe; bad.metadataPlacement = StripeMetadataPlacement::DATA_NODES;
    AKK_CLUSTER_CHECK(rejects(nodes, bad));
    auto noVoters = nodes;
    for (auto& member : noVoters) { member.capabilities &= ~STRIPE_METADATA_VOTER; }
    AKK_CLUSTER_CHECK(rejects(noVoters, stripe));
    auto missingPort = nodes; missingPort.back().stripeMetadataPort = 0;
    AKK_CLUSTER_CHECK(rejects(missingPort, stripe));
    auto twoCandidates = nodes;
    twoCandidates[2].capabilities |= STRIPE_FAILOVER_ELIGIBLE;
    twoCandidates[3].capabilities |= STRIPE_FAILOVER_ELIGIBLE;
    stripe.parityShards = 1;
    AKK_CLUSTER_CHECK(rejects(twoCandidates, stripe));
    stripe.failoverPolicy = StripeFailoverPolicy::MULTIPLE_CANDIDATES;
    const auto multiple = ClusterConfig::decode(make(twoCandidates, stripe).encode());
    AKK_CLUSTER_CHECK(multiple.availability().failoverCandidates == std::vector<uint64_t>({3, 4}));
    AKK_CLUSTER_CHECK(multiple.stripe().failoverPolicy == StripeFailoverPolicy::MULTIPLE_CANDIDATES);
    auto mixed = twoCandidates;
    mixed[0].capabilities |= STRIPE_METADATA_VOTER;
    mixed.pop_back();
    AKK_CLUSTER_CHECK(make(mixed, stripe).availability().metadataVoters == std::vector<uint64_t>({1, 5, 6}));
    const ClusterConfig preset{twoCandidates, RaidOptions{.preset = RaidPreset::RAID5, .dataShards = 2,
        .metadataPlacement = StripeMetadataPlacement::EXPLICIT_VOTERS, .failoverPolicy = StripeFailoverPolicy::MULTIPLE_CANDIDATES}, {}};
    AKK_CLUSTER_CHECK(preset.availability().metadataVoters == std::vector<uint64_t>({5, 6, 7}));
    AKK_CLUSTER_CHECK(preset.stripeFailoverNodes().size() == 2);
}

void testIndependentStripeMetadata(bool mirrors, bool secure, bool repair = false) {
    using akkaradb::engine::AkkEngine;
    using akkaradb::engine::AkkEngineOptions;
    using namespace std::chrono_literals;
    const auto dir = makeTempDir(std::string{"independent-metadata-"} + (mirrors ? "mirrors-" : "parity-") + (secure ? "secure" : "plain"));
    const size_t dataCount = mirrors ? 6 : 4, total = dataCount + 3;
    std::vector<NodeInfo> members;
    for (size_t i = 0; i < total; ++i) {
        auto capabilities = i < dataCount ? DATA_AND_COORDINATOR : uint32_t{COORDINATOR_ELIGIBLE | STRIPE_METADATA_VOTER};
        if (i == dataCount - 2 || i == dataCount - 1) { capabilities |= STRIPE_FAILOVER_ELIGIBLE; }
        members.push_back(node(i + 1, static_cast<uint16_t>(31301 + i), static_cast<uint16_t>(31311 + i), capabilities));
    }
    if (repair) {
        for (size_t i = 0; i < members.size(); ++i) {
            members[i].hostId = "physical-" + std::to_string(i);
            members[i].siteId = "site-" + std::to_string(i < dataCount ? i % 2 : i - dataCount);
        }
    }
    const ClusterConfig config{members, ReplicationMode::STRIPE, {}, ConsistencyOptions{.ackTimeoutMs = 500}, {},
        StripeOptions{.dataShards = 2, .parityShards = static_cast<uint8_t>(mirrors ? 0 : 1),
            .copiesPerShard = static_cast<uint8_t>(mirrors ? 2 : 1),
            .metadataPlacement = StripeMetadataPlacement::EXPLICIT_VOTERS, .failoverPolicy = StripeFailoverPolicy::MULTIPLE_CANDIDATES},
        0, testClusterId(9592), {}, FailoverPolicy::DEFAULT,
        repair ? FaultDomainOptions{.minHosts = 2, .minSites = 2, .requireSiteQuorum = true} : FaultDomainOptions{}};
    ClusterRuntimeOptions runtime;
    runtime.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
    runtime.clusterGroupId = 9592;
    runtime.routingMode = ClusterRoutingMode::FORWARD;
    runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
    runtime.forwardingTimeoutMs = 3000;
    runtime.raftHeartbeatIntervalMs = 30;
    runtime.stripeAutoRebuild = repair;
    runtime.stripeRebuildIntervalMs = 100;
    if (repair) { runtime.stripeWriteCommitMode = StripeWriteCommitMode::ALL_SHARDS; }
    if (secure) {
        for (size_t i = 0; i < total; ++i) {
            const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(i + 1))}.loadOrCreate();
            runtime.secure.pinnedPeers.push_back({i + 1, identity.publicKey});
        }
    }
    std::vector<AkkEngineOptions> options(total);
    std::vector<std::unique_ptr<AkkEngine>> engines(total);
    for (size_t i = 0; i < total; ++i) {
        auto& opt = options[i];
        opt.paths.dataDir = dir / std::to_string(i + 1);
        opt.components.blobEnabled = false;
        opt.components.sstEnabled = false;
        opt.components.manifestEnabled = false;
        opt.components.versionLogEnabled = true;
        opt.components.clusterEnabled = true;
        opt.cluster.config = config;
        opt.cluster.runtime = runtime;
        opt.cluster.runtime.secure.identitySeedPath = dir / ("identity-" + std::to_string(i + 1));
        writeNodeIdFile(opt.paths.dataDir / "node.id", i + 1);
        engines[i] = AkkEngine::open(opt);
    }
    constexpr std::array<uint64_t, 4> mirrorPlacement{1, 2, 3, 4};
    const auto key = repair ? keyPlacedOn(config, mirrorPlacement, "multiple-spares") : keyOwnedBy(config, 1, "independent-metadata");
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { engines[0]->put(bytesOf(key), bytesOf("initial")); return true; } catch (...) { return false; }
    }, 20s));
    const auto facts = engines[1]->clusterAvailability();
    AKK_CLUSTER_CHECK(facts.metadataVoters.size() == 3 && facts.metadataLearners.size() == dataCount && facts.failoverCandidates.size() == 2);
    AKK_CLUSTER_CHECK(facts.metadataQuorum == 2 && facts.metadataFailureTolerance == 1);
    // Data nodes are non-voting metadata learners, and a dedicated metadata
    // node must actually lead and serve the same fenced operation protocol.
    AKK_CLUSTER_CHECK(engines[1]->stats().cluster.stripeMetadataLeaderNodeId > dataCount);
    if (repair) {
        AKK_CLUSTER_CHECK(engines[0]->clusterAvailability().singleSiteQuorumSurvival);
        const auto nodes = engines[0]->stats().cluster.configuredNodes;
        AKK_CLUSTER_CHECK(std::ranges::all_of(nodes, [](const auto& n) { return !n.hostId.empty() && !n.siteId.empty(); }));
        engines[3]->close(); engines[3].reset();
        AKK_CLUSTER_CHECK(waitUntil([&] {
            return stripeInternalCounts(*engines[4]).first + stripeInternalCounts(*engines[5]).first != 0;
        }, 20s));
        engines[0]->close(); engines[0].reset();
        const bool repaired = waitUntil([&] {
            uint64_t rebuilt = 0;
            for (const auto& engine : engines) { if (engine) { rebuilt += engine->stats().stripeRebuild.succeeded; } }
            return rebuilt >= 2 && stripeInternalCounts(*engines[4]).first != 0 && stripeInternalCounts(*engines[5]).first != 0;
        }, 20s);
        if (!repaired) {
            for (size_t i = 0; i < engines.size(); ++i) { if (engines[i]) {
                const auto stats = engines[i]->stats();
                std::fprintf(stderr, "node %zu: rebuild cycles=%llu attempts=%llu succeeded=%llu failed=%llu lastTarget=%llu shards=%zu\n",
                    i + 1, stats.stripeRebuild.cycles, stats.stripeRebuild.attempts, stats.stripeRebuild.succeeded,
                    stats.stripeRebuild.failed, stats.stripeRebuild.lastFailureNodeId, stripeInternalCounts(*engines[i]).first);
            } }
        }
        AKK_CLUSTER_CHECK(repaired);
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try { engines[1]->put(bytesOf(key), bytesOf("two-spares-preserved")); return true; } catch (...) { return false; }
        }, 15s));
        engines[2]->close(); engines[2].reset();
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try { const auto value = engines[4]->get(bytesOf(key)); return value && textOf(*value) == "two-spares-preserved"; }
            catch (...) { return false; }
        }, 15s));
        for (auto& engine : engines) { if (engine) { engine->close(); } }
        std::filesystem::remove_all(dir);
        std::fprintf(stderr, "[availability] two distinct RAID10 spare placements preserved across a new generation passed\n");
        return;
    }
    engines[0]->close(); engines[0].reset();
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { engines[1]->put(bytesOf(key), bytesOf("first-failover")); return true; } catch (...) { return false; }
    }, 15s));
    auto history = akk_test::collectHistory(engines[1]->history(bytesOf(key)));
    AKK_CLUSTER_CHECK(!history.empty());
    const auto first = history.back().sourceNodeId;
    AKK_CLUSTER_CHECK(config.isStripeFailoverNode(first));
    engines[first - 1]->close(); engines[first - 1].reset();
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { engines[1]->put(bytesOf(key), bytesOf("second-failover")); return true; } catch (...) { return false; }
    }, 15s));
    // Old generations can lose more shards than their parity permits after
    // two failures. Verify the new generation, rather than requiring history
    // for generations whose data is no longer reconstructable.
    AKK_CLUSTER_CHECK(textOf(*engines[1]->get(bytesOf(key))) == "second-failover");
    AKK_CLUSTER_CHECK(engines[dataCount]->count() == 1);
    engines.back()->close(); engines.back().reset();
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { engines[1]->put(bytesOf(key), bytesOf("metadata-minority-down")); return true; } catch (...) { return false; }
    }, 20s));
    for (auto& engine : engines) { if (engine) { engine->close(); engine.reset(); } }
    for (size_t i = 1; i < total - 1; ++i) { if (i + 1 != first) { engines[i] = AkkEngine::open(options[i]); } }
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { const auto value = engines[1]->get(bytesOf(key)); return value && textOf(*value) == "metadata-minority-down"; } catch (...) { return false; }
    }, 20s));
    for (auto& engine : engines) { if (engine) { engine->close(); } }
    std::filesystem::remove_all(dir);
    std::fprintf(stderr, "[availability] dedicated metadata, two candidate handoffs and restart (%s/%s) passed\n", mirrors ? "RAID10" : "parity", secure ? "SECURE" : "PLAIN");
}

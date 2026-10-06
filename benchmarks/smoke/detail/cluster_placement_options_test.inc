// Observable placement, history and deadline contracts; no mock storage behavior.
void testReconfigurationDeadlineBudget() {
    using Budget = akkaradb::engine::cluster::detail::ReconfigurationDeadline;
    using namespace std::chrono_literals;
    const auto started = Budget::Clock::now();
    std::timed_mutex mutex;
    std::promise<void> locked, release;
    auto released = release.get_future();
    auto holder = std::async(std::launch::async, [&] { std::lock_guard lock{mutex}; locked.set_value(); released.wait(); });
    locked.get_future().wait();
    bool expired = false;
    {
        Budget outer{150};
        const auto deadline = Budget::current().deadline;
        Budget nested{30000};
        AKK_CLUSTER_CHECK(Budget::current().deadline == deadline);
        try { auto lock = Budget::lock(mutex); } catch (const std::runtime_error&) { expired = true; }
    }
    release.set_value(); holder.get();
    AKK_CLUSTER_CHECK(expired && Budget::Clock::now() - started < 1s);
    AKK_CLUSTER_CHECK(Budget::current().deadline == Budget::Clock::time_point::max());
}

void testStripePlacementOptions(bool secure) {
    using akkaradb::engine::AkkEngine;
    using akkaradb::engine::AkkEngineOptions;
    using namespace std::chrono_literals;
    std::fprintf(stderr, "[placement options] %s live copy / cancellation / deadline\n", secure ? "SECURE" : "PLAIN");
    const auto dir = makeTempDir("stripe-placement-options");
    std::vector<NodeInfo> members;
    for (uint64_t id = 1; id <= 4; ++id) { members.push_back(node(id, 28600 + id, 28610 + id)); }
    const auto configFor = [&](std::vector<NodeInfo> selected) {
        return ClusterConfig{std::move(selected), ReplicationMode::STRIPE, {},
            {.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 2000}, {},
            {.dataShards = 2, .parityShards = 1}, 0, testClusterId(9580)};
    };
    auto initial = members; initial.resize(3);
    auto joining = members; joining.back().capabilities |= PLACEMENT_STANDBY;
    auto cancelled = std::make_shared<std::atomic<bool>>(false);
    ClusterSecureOptions security;
    if (secure) {
        for (uint64_t id = 1; id <= 4; ++id) {
            const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
            security.pinnedPeers.push_back({id, identity.publicKey});
        }
    }
    std::array<AkkEngineOptions, 4> options;
    std::array<std::unique_ptr<AkkEngine>, 4> engines;
    for (size_t i = 0; i < 4; ++i) {
        auto& opt = options[i];
        opt.paths.dataDir = dir / std::to_string(i + 1);
        opt.components.blobEnabled = opt.components.sstEnabled = opt.components.manifestEnabled = false;
        opt.components.versionLogEnabled = opt.components.clusterEnabled = true;
        opt.cluster.config = configFor(i == 3 ? joining : initial);
        opt.cluster.runtime.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
        if (secure) { opt.cluster.runtime.secure = security; opt.cluster.runtime.secure.identitySeedPath = dir / ("identity-" + std::to_string(i + 1)); }
        opt.cluster.runtime.clusterGroupId = 9580;
        opt.cluster.runtime.routingMode = ClusterRoutingMode::FORWARD;
        opt.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
        opt.cluster.runtime.raftHeartbeatIntervalMs = 30;
        opt.cluster.runtime.raftMaxForwardRequests = 1;
        opt.cluster.runtime.raftSnapshot.minLogEntries = 1;
        auto& migration = opt.cluster.runtime.reconfiguration;
        migration.stripeMigrationMode = StripeMigrationMode::LIVE_COPY;
        migration.stripeInterruptionPolicy = StripeInterruptionPolicy::CANCEL;
        migration.writePolicy = ReconfigurationWritePolicy::REJECT;
        migration.resumePendingChanges = false; // CANCEL cleanup remains enabled.
        migration.timeoutMs = 1500;
        migration.maxTransferBytesPerSecond = 256;
        migration.cancelled = [cancelled] { return cancelled->load(); };
        writeNodeIdFile(opt.paths.dataDir / "node.id", i + 1);
        if (i < 3) { engines[i] = AkkEngine::open(opt); }
    }
    const auto keyText = keyOwnedBy(configFor(initial), 3, "live-copy-");
    const auto key = bytesOf(keyText);
    const std::vector<uint8_t> original(2048, 73);
    AKK_CLUSTER_CHECK(waitUntil([&] { try { engines[0]->put(key, original); return true; } catch (...) { return false; } }, 15s));
    const auto checkpoint = engines[0]->createClusterCheckpoint();
    engines[3] = AkkEngine::open(options[3]);
    const auto authority = engines[0]->clusterReconfigurationStatus().authorityNodeId;
    // Exercise forwarding with a shorter caller budget than shard ACK timeout.
    const auto caller = authority == 1 ? 1u : 0u;
    const auto started = std::chrono::steady_clock::now();
    auto migration = std::async(std::launch::async, [&] {
        try { engines[caller]->reconfigureCluster(configFor(members)); return false; }
        catch (const std::exception&) { return true; }
    });
    const bool sawCopy = waitUntil([&] {
        try { const auto status = engines[0]->clusterReconfigurationStatus(); return status.pendingGeneration != 0 && !status.stripeWritesBlocked; }
        catch (...) { return false; }
    }, 1s);
    if (sawCopy) { engines[0]->put(key, bytesOf("during-copy")); }
    const bool interrupted = migration.get();
    const auto elapsed = std::chrono::steady_clock::now() - started;
    AKK_CLUSTER_CHECK(sawCopy && interrupted && elapsed < 2500ms);
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { const auto status = engines[0]->clusterReconfigurationStatus(); return status.pendingGeneration == 0 && status.activeGeneration == 1; }
        catch (...) { return false; }
    }, 10s));
    AKK_CLUSTER_CHECK(textOf(*engines[0]->get(key)) == "during-copy");
    AKK_CLUSTER_CHECK(!engines[0]->cancelClusterReconfiguration());
    for (auto& engine : engines) { engine->close(); engine.reset(); }
    for (size_t i = 0; i < 4; ++i) {
        options[i].cluster.config.reset();
        options[i].cluster.runtime.reconfiguration.timeoutMs = 30000;
        options[i].cluster.runtime.reconfiguration.maxTransferBytesPerSecond = 4096;
        options[i].cluster.runtime.reconfiguration.stripeInterruptionPolicy = StripeInterruptionPolicy::RETAIN;
        engines[i] = AkkEngine::open(options[i]);
    }
    AKK_CLUSTER_CHECK(waitUntil([&] { try { return engines[0]->clusterReconfigurationStatus().authorityNodeId != 0; } catch (...) { return false; } }, 15s));
    const auto secondAuthority = engines[0]->clusterReconfigurationStatus().authorityNodeId;
    migration = std::async(std::launch::async, [&] {
        try { engines[secondAuthority - 1]->reconfigureCluster(configFor(members)); return false; }
        catch (const std::exception& error) { std::fprintf(stderr, "[placement options] activation failed: %s\n", error.what()); return true; }
    });
    const bool sawSecondCopy = waitUntil([&] {
        try { const auto status = engines[0]->clusterReconfigurationStatus(); return status.pendingGeneration != 0 && !status.stripeWritesBlocked; }
        catch (...) { return false; }
    }, 5s);
    if (sawSecondCopy) {
        engines[0]->put(key, bytesOf("tail"));
        engines[0]->put(bytesOf("new-during-copy"), bytesOf("new"));
        engines[0]->put(bytesOf("deleted-during-copy"), bytesOf("old"));
        engines[0]->remove(bytesOf("deleted-during-copy"));
    }
    const auto activationInterrupted = migration.get();
    std::string activationError;
    const bool resumed = !activationInterrupted || waitUntil([&] {
        try { engines[0]->reconfigureCluster(configFor(members)); return true; }
        catch (const std::exception& error) { activationError = error.what(); return false; }
    }, 15s);
    if (!resumed) { std::fprintf(stderr, "[placement options] resume failed: %s\n", activationError.c_str()); }
    AKK_CLUSTER_CHECK(resumed && sawSecondCopy);
    const auto status = engines[0]->clusterReconfigurationStatus();
    AKK_CLUSTER_CHECK(status.activeGeneration == 3 && status.pendingGeneration == 0); // Aborted generation 2 is never reused.
    AKK_CLUSTER_CHECK(textOf(*engines[3]->get(key)) == "tail");
    AKK_CLUSTER_CHECK(textOf(*engines[3]->get(bytesOf("new-during-copy"))) == "new");
    AKK_CLUSTER_CHECK(!engines[3]->get(bytesOf("deleted-during-copy")));
    std::vector<akkaradb::engine::VersionEntry> history;
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { history = akk_test::collectHistory(engines[3]->history(key)); return history.size() == 3; } catch (...) { return false; }
    }, 10s));
    AKK_CLUSTER_CHECK(history.size() == 3 && history.front().value == original && textOf(history[1].value) == "during-copy" && textOf(history[2].value) == "tail");
    const auto rollback = engines[0]->rollbackTo(checkpoint, {.conflict = akkaradb::engine::RollbackConflictPolicy::OVERWRITE_LATEST});
    AKK_CLUSTER_CHECK(rollback.complete() && *engines[3]->get(key) == original);
    // Stage aliases after an earlier successful placement, then cancel them.
    // The previous active aliases must remain readable on every participant.
    const auto retainedHistory = akk_test::collectHistory(engines[3]->history(key));
    migration = std::async(std::launch::async, [&] {
        try { engines[0]->reconfigureCluster(configFor(initial)); return false; }
        catch (const std::exception&) { return true; }
    });
    const bool staged = waitUntil([&] {
        try {
            const auto progress = engines[0]->clusterReconfigurationStatus();
            return progress.pendingGeneration == 4 && progress.completedRecords != 0 &&
                progress.transferredBytes != 0 && progress.transferredBytes < status.transferredBytes && !progress.activationStarted;
        } catch (...) { return false; }
    }, 5s);
    cancelled->store(true);
    AKK_CLUSTER_CHECK(migration.get() && staged);
    AKK_CLUSTER_CHECK(engines[0]->clusterReconfigurationStatus().pendingGeneration == 4);
    AKK_CLUSTER_CHECK(engines[0]->cancelClusterReconfiguration());
    cancelled->store(false);
    AKK_CLUSTER_CHECK(engines[0]->clusterReconfigurationStatus().activeGeneration == 3);
    AKK_CLUSTER_CHECK(*engines[3]->get(key) == original && akk_test::collectHistory(engines[3]->history(key)).size() == retainedHistory.size());
    // A different plan can now replace the cancelled plan.
    auto different = members; different.erase(different.begin() + 1);
    AKK_CLUSTER_CHECK(waitUntil([&] {
        try { engines[0]->reconfigureCluster(configFor(different)); return true; } catch (...) { return false; }
    }, 30s));
    AKK_CLUSTER_CHECK(engines[0]->clusterReconfigurationStatus().activeGeneration == 5);
    AKK_CLUSTER_CHECK(*engines[3]->get(key) == original);
    for (auto& engine : engines) { engine->close(); engine.reset(); }
    for (const size_t i : {size_t{0}, size_t{2}, size_t{3}}) { options[i].cluster.config.reset(); engines[i] = AkkEngine::open(options[i]); }
    AKK_CLUSTER_CHECK(waitUntil([&] { try { const auto value = engines[3]->get(key); return value && *value == original; } catch (...) { return false; } }, 15s));
    for (auto& engine : engines) { if (engine) { engine->close(); } }
    std::filesystem::remove_all(dir);
}

/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
akkaradb::engine::AkkEngineOptions localRequestOptions(const std::filesystem::path& dir) {
    akkaradb::engine::AkkEngineOptions options;
    options.paths.dataDir = dir;
    options.components.clusterEnabled = true;
    options.components.blobEnabled = false;
    options.cluster.config = ClusterConfig{{node(1, 32701, 32711)}, ReplicationMode::MIRROR, {},
        ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 2000}, {}, {}, 1, testClusterId(9634)};
    options.cluster.runtime.transportMode = TransportMode::PLAIN;
    options.cluster.runtime.requests.enabled = true;
    options.cluster.runtime.requests.persistLocalApplication = true;
    options.cluster.runtime.requests.authorityNodes = {node(1, 0, 32721)};
    writeNodeIdFile(dir / "node.id", 1);
    return options;
}
int runLocalRequestCrashWriter(const char* point, const std::filesystem::path& dir) {
    auto engine = akkaradb::engine::AkkEngine::open(localRequestOptions(dir));
    const auto id = engine->newRequestId();
    AKK_CLUSTER_CHECK(waitUntil([&] { try { return engine->queryRequest(id).status == ClusterRequestStatus::NOT_FOUND; } catch (...) { return false; } }));
    {
        std::ofstream token{dir / "request-token", std::ios::binary};
        token.write(reinterpret_cast<const char*>(id.nonce.data()), id.nonce.size());
        token.write(reinterpret_cast<const char*>(&id.expiresAtUnixMs), sizeof(id.expiresAtUnixMs));
    }
    setStorageCrashPoint(point);
    (void)engine->putWithRequest(id, bytesOf("local-once"), bytesOf(std::string(4096, 'x')));
    return 1;
}
void testLocalRequestApplication(const std::filesystem::path& executable) {
    for (const char* point : {"request.local.after_wal", "request.local.after_checkpoint", "request.local.after_apply"}) {
        const auto dir = makeTempDir(std::string{"local-request-crash-"} + point);
        runStorageCrashChild(executable, "--local-request-crash-writer", point, dir);
        ClusterRequestId id;
        std::ifstream token{dir / "request-token", std::ios::binary};
        token.read(reinterpret_cast<char*>(id.nonce.data()), id.nonce.size());
        token.read(reinterpret_cast<char*>(&id.expiresAtUnixMs), sizeof(id.expiresAtUnixMs));
        AKK_CLUSTER_CHECK(token.good()); token.close();
        auto options = localRequestOptions(dir);
        auto engine = akkaradb::engine::AkkEngine::open(options);
        const auto local = engine->queryLocalRequest(id);
        AKK_CLUSTER_CHECK(local.status == ClusterRequestStatus::APPLIED && local.sequence != 0 && local.logIndex == 0);
        AKK_CLUSTER_CHECK(textOf(*engineGet(*engine, "local-once")) == std::string(4096, 'x'));
        AKK_CLUSTER_CHECK(waitUntil([&] { try { return engine->queryRequest(id).status == ClusterRequestStatus::PENDING; } catch (...) { return false; } }));
        engine->put(bytesOf("local-once"), bytesOf("later"));
        const auto before = engine->stats().currentSeq;
        AKK_CLUSTER_CHECK(engine->putWithRequest(id, bytesOf("local-once"), bytesOf(std::string(4096, 'x'))).status == ClusterRequestStatus::PENDING);
        AKK_CLUSTER_CHECK(engine->stats().currentSeq == before && textOf(*engineGet(*engine, "local-once")) == "later");
        const auto removeId = engine->newRequestId();
        AKK_CLUSTER_CHECK(engine->removeWithRequest(removeId, bytesOf("local-once")).status == ClusterRequestStatus::APPLIED);
        AKK_CLUSTER_CHECK(engine->queryLocalRequest(removeId).status == ClusterRequestStatus::APPLIED);
        auto expired = removeId; expired.expiresAtUnixMs = 1;
        AKK_CLUSTER_CHECK(engine->queryLocalRequest(expired).status == ClusterRequestStatus::EXPIRED);
        AKK_CLUSTER_CHECK(engine->queryLocalRequest(engine->newRequestId()).status == ClusterRequestStatus::NOT_FOUND);
        engine->forceFlush();
        AKK_CLUSTER_CHECK(engine->stats().manifest.lastCheckpointSeq >= engine->queryLocalRequest(removeId).sequence);
        engine->close();
        // This test owns the directory. Simulate pruning the fully checkpointed data WAL,
        // retiring its existence proofs too, and leaving the receipt authority's WAL untouched.
        for (const auto& entry : std::filesystem::directory_iterator(dir / "wal")) {
            if (entry.is_regular_file() && (entry.path().extension() == ".akwal" || entry.path().extension() == ".checkpoint")) { std::filesystem::remove(entry.path()); }
        }
        engine = akkaradb::engine::AkkEngine::open(options);
        AKK_CLUSTER_CHECK(engine->queryLocalRequest(id).sequence == local.sequence);
        AKK_CLUSTER_CHECK(engine->queryLocalRequest(removeId).status == ClusterRequestStatus::APPLIED && !engineGet(*engine, "local-once"));
        engine->close();
        auto disabled = options; disabled.cluster.runtime.requests.persistLocalApplication = false;
        bool downgradeRejected = false;
        try { (void)akkaradb::engine::AkkEngine::open(disabled); } catch (const std::runtime_error&) { downgradeRejected = true; }
        AKK_CLUSTER_CHECK(downgradeRejected);
        const auto checkpoint = dir / "request-local" / "applied.aklr";
        std::ifstream proofFile{checkpoint, std::ios::binary};
        std::vector<char> proof((std::istreambuf_iterator<char>(proofFile)), {}); proofFile.close();
        AKK_CLUSTER_CHECK(proof.size() == 5); proof.back() ^= 1;
        { std::ofstream corrupt{checkpoint, std::ios::binary | std::ios::trunc}; corrupt.write(proof.data(), static_cast<std::streamsize>(proof.size())); }
        bool corruptionRejected = false;
        try { (void)akkaradb::engine::AkkEngine::open(options); } catch (const std::runtime_error&) { corruptionRejected = true; }
        AKK_CLUSTER_CHECK(corruptionRejected);
    }
    const auto dir = makeTempDir("local-request-validation");
    auto options = localRequestOptions(dir);
    auto invalid = options; invalid.components.walEnabled = false;
    bool rejected = false;
    try { (void)akkaradb::engine::AkkEngine::open(invalid); } catch (const std::invalid_argument&) { rejected = true; }
    AKK_CLUSTER_CHECK(rejected);
    auto badRuntime = options.cluster.runtime; badRuntime.requests.enabled = false;
    bool disabledRequestsRejected = false;
    try { options.cluster.config->validateRuntime(1, badRuntime); } catch (const std::invalid_argument&) { disabledRequestsRejected = true; }
    AKK_CLUSTER_CHECK(disabledRequestsRejected);
    auto noRecovery = options; noRecovery.runtime.recoverWal = false;
    bool noRecoveryRejected = false;
    try { (void)akkaradb::engine::AkkEngine::open(noRecovery); } catch (const std::invalid_argument&) { noRecoveryRejected = true; }
    AKK_CLUSTER_CHECK(noRecoveryRejected);
    options.cluster.runtime.requests.persistLocalApplication = false;
    auto engine = akkaradb::engine::AkkEngine::open(options);
    bool disabled = false;
    try { (void)engine->queryLocalRequest(engine->newRequestId()); } catch (const std::logic_error&) { disabled = true; }
    AKK_CLUSTER_CHECK(disabled); engine->close();
    for (const auto mode : {ReplicationMode::STANDALONE, ReplicationMode::PARTITIONED}) {
        const auto modeDir = makeTempDir(mode == ReplicationMode::STANDALONE ? "local-request-standalone" : "local-request-partitioned");
        auto modeOptions = localRequestOptions(modeDir);
        modeOptions.cluster.runtime.clusterGroupId = 0xA44A'9635;
        modeOptions.cluster.config = ClusterConfig{{node(1, 32701, 32711)}, mode, {},
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 2000}, {}, {}, 0, testClusterId(9635)};
        auto modeEngine = akkaradb::engine::AkkEngine::open(modeOptions);
        const auto id = modeEngine->newRequestId();
        AKK_CLUSTER_CHECK(waitUntil([&] { try { return modeEngine->queryRequest(id, bytesOf("mode-key")).status == ClusterRequestStatus::NOT_FOUND; } catch (...) { return false; } }));
        AKK_CLUSTER_CHECK(modeEngine->putWithRequest(id, bytesOf("mode-key"), bytesOf("value")).status == ClusterRequestStatus::APPLIED);
        AKK_CLUSTER_CHECK(modeEngine->queryLocalRequest(id).status == ClusterRequestStatus::APPLIED);
        modeEngine->close();
        modeEngine = akkaradb::engine::AkkEngine::open(modeOptions);
        AKK_CLUSTER_CHECK(modeEngine->queryLocalRequest(id).status == ClusterRequestStatus::APPLIED);
        AKK_CLUSTER_CHECK(textOf(*engineGet(*modeEngine, "mode-key")) == "value");
        modeEngine->close();
    }
}

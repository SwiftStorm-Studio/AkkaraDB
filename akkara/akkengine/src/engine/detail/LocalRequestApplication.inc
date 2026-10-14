/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
// Included inside AkkEngine::Impl.
struct LocalRequestApplication {
    cluster::ClusterRequestId id{};
    std::array<uint8_t, 32> fingerprint{};
    uint64_t sequence = 0;
};
mutable std::mutex localRequestMu;
std::shared_ptr<cluster::detail::DiskResults> localRequestApplications;
bool localRequestRecoveryRequired = false; // Protected by writeMu; partial publication requires reopen.

fs::path localRequestPath() const { return opts.paths.dataDir / "request-local" / "applied.aklr"; }
static uint64_t localRequestNowMs() { return nowNs() / 1'000'000; }
static std::vector<uint8_t> localRequestMetadata(const LocalRequestApplication& request) {
    std::vector<uint8_t> bytes(request.id.nonce.begin(), request.id.nonce.end());
    cluster::detail::transactionInteger(bytes, request.id.expiresAtUnixMs);
    bytes.insert(bytes.end(), request.fingerprint.begin(), request.fingerprint.end());
    return bytes;
}
static LocalRequestApplication decodeLocalRequest(std::span<const uint8_t> bytes, uint64_t sequence) {
    if (bytes.size() != wal::WAL_REQUEST_METADATA_SIZE || !sequence) { throw std::runtime_error("Local request: invalid application record"); }
    cluster::detail::TransactionReader reader{bytes};
    LocalRequestApplication request;
    const auto nonce = reader.take(16); std::copy(nonce.begin(), nonce.end(), request.id.nonce.begin());
    request.id.expiresAtUnixMs = reader.integer();
    const auto fingerprint = reader.take(32); std::copy(fingerprint.begin(), fingerprint.end(), request.fingerprint.begin());
    request.sequence = sequence;
    if (!request.id.expiresAtUnixMs || !std::ranges::any_of(request.id.nonce, [](auto byte) { return byte != 0; })) {
        throw std::runtime_error("Local request: invalid identity");
    }
    return request;
}
static std::vector<uint8_t> localRequestKey(const cluster::ClusterRequestId& id) {
    std::vector<uint8_t> key(id.nonce.begin(), id.nonce.end());
    cluster::detail::transactionInteger(key, id.expiresAtUnixMs); return key;
}
void reserveLocalRequest(const LocalRequestApplication& request) {
    std::lock_guard lock{localRequestMu};
    if (request.id.expiresAtUnixMs <= localRequestNowMs() || localRequestApplications->get(localRequestKey(request.id))) { return; }
    if (!localRequestApplications->canReserve(cluster::detail::DiskResults::recordCharge(24, 32), localRequestNowMs())) {
        throw std::length_error("Local request: retained-result disk budget exhausted");
    }
}
void loadLocalRequests() {
    const auto path = localRequestPath();
    const bool exists = !opts.paths.dataDir.empty() && fs::exists(path);
    if (!opts.cluster.runtime.requests.persistLocalApplication) {
        if (exists) { throw std::runtime_error("Local request: persisted application tracking cannot be disabled on reopen"); }
        return;
    }
    if (!opts.components.clusterEnabled || !opts.components.walEnabled || !opts.runtime.recoverWal ||
        opts.runtime.truncateCorruptWalOnRecovery || opts.paths.dataDir.empty() ||
        (opts.components.sstEnabled && !opts.runtime.recoverSst) || opts.wal.syncPolicy == wal::WalSyncPolicy::NEVER) {
        throw std::invalid_argument("Local request: persistent WAL/SST recovery and durable sync are required; corrupt WAL must fail startup");
    }
    if (opts.runtime.sequence.allocation != AkkEngineOptions::SequenceAllocationMode::GLOBAL_ATOMIC ||
        opts.runtime.writeVisibility != AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER) {
        throw std::invalid_argument("Local request: GLOBAL_ATOMIC and COMMIT_ORDER are required");
    }
    if (exists) {
        std::ifstream file{path, std::ios::binary};
        const std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(file)), {});
        const std::vector<uint8_t> marker{'A','K','L','P','2'};
        if (!file || bytes != marker) { throw std::runtime_error("Local request: incompatible application store; recreate pre-release cluster state"); }
    }
    localRequestApplications = std::make_shared<cluster::detail::DiskResults>(path.parent_path() / "results", opts.cluster.runtime.requests.maxRetainedResultBytes);
    if (!exists) { writeFileAtomicallyDurable(path, std::vector<uint8_t>{'A','K','L','P','2'}); }

}
void recordLocalRequest(std::span<const uint8_t> metadata, uint64_t sequence) {
    const auto request = decodeLocalRequest(metadata, sequence);
    std::lock_guard lock{localRequestMu};
    const auto now = localRequestNowMs();
    localRequestApplications->advanceFloor(now);
    if (request.id.expiresAtUnixMs <= now) { return; }
    const auto key = localRequestKey(request.id);
    if (const auto old = localRequestApplications->get(key)) {
        if (old->sequence != sequence || old->value != std::vector<uint8_t>(request.fingerprint.begin(), request.fingerprint.end())) {
            throw std::runtime_error("Local request: conflicting application identity");
        }
        return;
    }
    // The mutation WAL is synced first. Publish its proof before the memtable
    // can flush the mutation and prune that WAL record.
    localRequestApplications->put(key, {request.id.expiresAtUnixMs, sequence, {request.fingerprint.begin(), request.fingerprint.end()}});
    localRequestApplications->sync();

}
cluster::ClusterRequestResult queryLocalRequest(const cluster::ClusterRequestId& id) const {
    if (!opts.cluster.runtime.requests.persistLocalApplication) { throw std::logic_error("Local request: application tracking is disabled"); }
    if (localRequestRecoveryRequired) { throw std::runtime_error("Local request: reopen is required after interrupted publication"); }
    if (!id.expiresAtUnixMs || !std::ranges::any_of(id.nonce, [](auto byte) { return byte != 0; })) {
        throw std::invalid_argument("Local request: invalid identity");
    }
    std::lock_guard lock{localRequestMu};
    if (id.expiresAtUnixMs <= localRequestNowMs()) { return {cluster::ClusterRequestStatus::EXPIRED}; }
    const auto found = localRequestApplications->get(localRequestKey(id));
    if (!found) { return {}; }
    return {cluster::ClusterRequestStatus::APPLIED, found->sequence, 0};
}

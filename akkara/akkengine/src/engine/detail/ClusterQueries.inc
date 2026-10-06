// Included inside AkkEngine::Impl: cluster read authority, snapshots and cursors.
enum class QueryAction : uint8_t { SCAN, COUNT, HISTORY, GET_AT, NEXT, CLOSE, PIN, KEEP_ALIVE };
static constexpr size_t QUERY_PAGE_BYTES = 1024 * 1024;
#include "ClusterQuerySources.inc"
struct RemoteQueryPins {
    Impl* engine;
    std::vector<std::pair<uint64_t, std::array<uint8_t, 16>>> peers;
    ~RemoteQueryPins() {
        for (const auto& [node, token] : peers) {
            try {
                cluster::ForwardRequest request; request.timeoutMs = 100;
                request.deduplicationId.nonce = token;
                request.entries.push_back({{}, {static_cast<uint8_t>(QueryAction::CLOSE)}});
                (void)engine->sendReadQuery(node, std::move(request));
            } catch (...) {} // Remote TTL reclaims unreachable pins.
        }
    }
    void renew() {
        for (const auto& [node, token] : peers) {
            cluster::ForwardRequest request; request.deduplicationId.nonce = token;
            request.entries.push_back({{}, {static_cast<uint8_t>(QueryAction::KEEP_ALIVE)}});
            (void)engine->sendReadQuery(node, std::move(request));
        }
    }
};
struct QuerySession {
    std::mutex mutex;
    std::chrono::steady_clock::time_point expires;
    std::chrono::steady_clock::time_point maxLifetime;
    uint64_t epoch = 0;
    bool streaming = false, pinOnly = false, started = false, finished = false, failed = false;
    uint64_t nextOffset = 0, replayOffset = UINT64_MAX;
    size_t recordOffset = 0;
    core::ArenaGenerator<std::vector<uint8_t>> producer;
    core::ArenaGenerator<std::vector<uint8_t>>::iterator position;
    std::vector<uint8_t> replay;
    std::shared_ptr<void> gcPin;
    std::shared_ptr<RemoteQueryPins> remotePins;
    std::unique_ptr<detail::ClusterQuerySpool> spool;
};
std::atomic<uint64_t> querySpoolBytes{0};
std::mutex querySessionsMu;
std::map<std::array<uint8_t, 16>, std::shared_ptr<QuerySession>> querySessions;
std::jthread queryReaper;

void reapQuerySessions() {
    std::vector<std::shared_ptr<QuerySession>> retired;
    {
        std::lock_guard lock{querySessionsMu};
        const auto now = std::chrono::steady_clock::now();
        for (auto it = querySessions.begin(); it != querySessions.end();) {
            std::unique_lock sessionLock{it->second->mutex, std::try_to_lock};
            if (sessionLock.owns_lock() && (it->second->expires <= now || it->second->maxLifetime <= now)) {
                retired.push_back(std::move(it->second)); it = querySessions.erase(it);
            } else { ++it; }
        }
    }
}
void clearQuerySessions() {
    decltype(querySessions) retired;
    { std::lock_guard lock{querySessionsMu}; retired.swap(querySessions); }
}
void startQueryReaper() {
    queryReaper = std::jthread([this](std::stop_token stop) {
        while (!stop.stop_requested()) {
            reapQuerySessions();
            std::unique_lock lock{lifecycleMu};
            lifecycleCv.wait_for(lock, std::chrono::seconds{1}, [&] { return stop.stop_requested(); });
        }
    });
}

static std::vector<uint8_t> materializeQueryValue(uint8_t flags, std::span<const uint8_t> value, blob::BlobManager* blobs) {
    if ((flags & MemHdr16::FLAG_BLOB) == 0) { return {value.begin(), value.end()}; }
    if (!blobs || value.size() < blob::BLOB_REF_SIZE) { throw std::runtime_error("Cluster query: historical Blob is unavailable"); }
    const auto [id, size, crc] = blob::decodeBlobRef(value.data());
    auto result = blobs->read(id, crc);
    if (result.size() != size) { throw std::runtime_error("Cluster query: Blob is unavailable"); }
    return result;
}
static core::ArenaGenerator<VersionEntry> materializedHistory(core::ArenaGenerator<VersionEntry> entries,
    std::shared_ptr<StorageState> storage, blob::BlobManager::ReadPin blobPin) {
    (void)blobPin;
    for (const auto& original : entries) {
        auto entry = original;
        if ((entry.flags & MemHdr16::FLAG_TOMBSTONE) != 0) { entry.value.clear(); }
        else { entry.value = materializeQueryValue(entry.flags, entry.value, storage->blobManager.get()); }
        entry.flags &= static_cast<uint8_t>(~MemHdr16::FLAG_BLOB);
        co_yield std::move(entry);
    }
}
core::ArenaGenerator<VersionEntry> stripeHistory(std::vector<uint8_t> key,
    std::map<uint64_t, std::pair<VersionEntry, StripeMetadata>> revisions,
    std::shared_ptr<void> gcPin, std::shared_ptr<StorageState> storage) {
    (void)gcPin; (void)storage;
    for (auto& [seq, item] : revisions) {
        auto& [entry, metadata] = item;
        entry.seq = seq;
        entry.sourceNodeId = metadata.rollback ? vlog::ROLLBACK_NODE : metadata.originNodeId;
        entry.timestampNs = metadata.timestampNs;
        entry.flags = static_cast<uint8_t>((metadata.tombstone ? MemHdr16::FLAG_TOMBSTONE : 0) |
            (metadata.rollback ? vlog::VLOG_FLAG_ROLLBACK : 0) | (entry.flags & vlog::VLOG_FLAG_RETENTION_BASE));
        if (!metadata.tombstone) {
            auto value = readStripeValueFromMetadata(key, metadata, false);
            if (!value) { throw std::runtime_error("Cluster query: historical STRIPE value is unavailable"); }
            entry.value = std::move(*value);
        }
        co_yield std::move(entry);
    }
}
core::ArenaGenerator<VersionEntry> publicHistoryLocal(std::span<const uint8_t> key) {
    if (usesPartitionRaft()) {
        auto& child = requireLocalPartition(key);
        child.impl_->clusterRuntime->queryReadBarrier();
        return child.impl_->publicHistoryLocal(key);
    }
    std::shared_lock storageLock{storageMu};
    if (!versionLog) { return {}; }
    if (clusterReplicationMode != cluster::ReplicationMode::STRIPE) {
        auto blobPin = blobManager ? blobManager->pinReads() : blob::BlobManager::ReadPin{};
        return materializedHistory(versionLog->history(key), storage, std::move(blobPin));
    }
    auto gcPin = pinStripeQuery();
    auto keyLock = stripeKeys.lock(key);
    checkStripeHealthy();
    std::map<uint64_t, std::pair<VersionEntry, StripeMetadata>> revisions;
    for (const auto& entry : versionLog->history(stripeMetaKey(key))) {
        auto metadata = decodeStripeMetadata(entry.value);
        if (metadata) { *metadata = effectiveStripeMetadata(key, *metadata); }
        if (!metadata || metadata->logicalSeq == 0) { throw std::runtime_error("Cluster query: invalid STRIPE history"); }
        auto saved = entry; saved.value.clear();
        auto [it, inserted] = revisions.try_emplace(metadata->logicalSeq, std::move(saved), *metadata);
        if (!inserted) { it->second.second = std::move(*metadata); }
    }
    return stripeHistory({key.begin(), key.end()}, std::move(revisions), std::move(gcPin), storage);
}

std::optional<VersionEntry> publicVersionAtLocal(std::span<const uint8_t> key, uint64_t seq) {
    if (usesPartitionRaft()) {
        auto& child = requireLocalPartition(key);
        child.impl_->clusterRuntime->queryReadBarrier();
        return child.impl_->publicVersionAtLocal(key, seq);
    }
    std::optional<cluster::detail::KeyedMutex::Guard> keyLock;
    if (clusterReplicationMode == cluster::ReplicationMode::STRIPE) { keyLock.emplace(stripeKeys.lock(key)); }
    std::shared_lock storageLock{storageMu};
    if (!versionLog) { return {}; }
    if (clusterReplicationMode == cluster::ReplicationMode::STRIPE) {
        checkStripeHealthy();
        const auto metadata = stripeMetadataAt(key, seq);
        if (!metadata || metadata->tombstone) { return {}; }
        auto value = readStripeValueFromMetadata(key, *metadata, false);
        if (!value) { throw std::runtime_error("Cluster query: historical STRIPE value is unavailable"); }
        return VersionEntry{.seq = metadata->logicalSeq, .value = std::move(*value)};
    }
    auto entry = versionLog->getAt(key, seq);
    if (!entry || (entry->flags & MemHdr16::FLAG_TOMBSTONE) != 0) { return {}; }
    auto value = resolveValue(entry->flags, entry->value);
    if (!value) { throw std::runtime_error("Cluster query: historical Blob is unavailable"); }
    entry->flags &= static_cast<uint8_t>(~MemHdr16::FLAG_BLOB);
    entry->value = std::move(*value);
    return entry;
}

static core::ArenaGenerator<VersionEntry> singleQueryVersion(VersionEntry entry) { co_yield std::move(entry); }
static core::ArenaGenerator<std::vector<uint8_t>> encodeQueryHistory(core::ArenaGenerator<VersionEntry> history) {
    for (const auto& entry : history) {
        std::vector<uint8_t> bytes;
        pushU64(bytes, entry.seq); pushU64(bytes, entry.sourceNodeId); pushU64(bytes, entry.timestampNs);
        bytes.push_back(entry.flags); pushU64(bytes, entry.value.size()); bytes.insert(bytes.end(), entry.value.begin(), entry.value.end());
        co_yield std::move(bytes);
    }
}
core::ArenaGenerator<std::vector<uint8_t>> encodeQueryRecords(core::ArenaGenerator<OwnedQueryRecord> records,
    bool countOnly, bool stripe, bool filterOwner, std::shared_ptr<void> gcPin, uint64_t epoch) {
    (void)gcPin;
    for (const auto& record : records) {
        if (epoch != clusterRuntime->configurationEpoch()) { throw std::runtime_error("Cluster query: configuration changed while reading fixed sources"); }
        if (filterOwner && clusterRuntime->ownerNodeId(record.key) != nodeId) { continue; }
        std::vector<uint8_t> key, value;
        if (stripe) {
            auto publicKey = publicKeyFromStripeMetaKey(record.key);
            auto metadata = decodeStripeMetadata(record.value);
            if (!publicKey || !metadata) { throw std::runtime_error("Cluster query: invalid STRIPE metadata"); }
            if (metadata->tombstone) { continue; }
            key = std::move(*publicKey);
            if (!countOnly) {
                *metadata = effectiveStripeMetadata(key, *metadata);
                auto decoded = readStripeValueFromMetadata(key, *metadata, false);
                if (!decoded) { throw std::runtime_error("Cluster query: STRIPE value unavailable"); }
                value = std::move(*decoded);
            }
        } else { key = record.key; if (!countOnly) { value = materializeQueryValue(record.flags, record.value, record.blobs); } }
        std::vector<uint8_t> bytes;
        if (!countOnly) { pushU64(bytes, key.size()); pushU64(bytes, value.size()); bytes.insert(bytes.end(), key.begin(), key.end()); bytes.insert(bytes.end(), value.begin(), value.end()); }
        co_yield std::move(bytes);
    }
}

cluster::ForwardResponse receiveReadQuery(const cluster::ForwardRequest& request) {
    const auto& input = request.entries.at(0);
    if (input.value.empty() || input.value[0] > static_cast<uint8_t>(QueryAction::KEEP_ALIVE)) {
        throw std::invalid_argument("Cluster query: malformed action");
    }
    const auto action = static_cast<QueryAction>(input.value[0]);
    cluster::ForwardResponse response;
    response.success = true;
    const auto now = std::chrono::steady_clock::now();
    cluster::detail::ReconfigurationDeadline budget{request.timeoutMs};
    if (action == QueryAction::PIN) {
        if (!input.key.empty() || input.value.size() != 1 || clusterReplicationMode != cluster::ReplicationMode::STRIPE) { throw std::invalid_argument("Cluster query: invalid retention pin request"); }
        auto session = std::make_shared<QuerySession>(); session->pinOnly = true; session->gcPin = pinStripeQuery();
        session->expires = now + std::chrono::milliseconds{opts.cluster.runtime.queryCursorIdleTimeoutMs};
        session->maxLifetime = now + std::chrono::milliseconds{opts.cluster.runtime.queryCursorMaxLifetimeMs};
        std::array<uint8_t, 16> token{};
        { std::lock_guard lock{querySessionsMu};
          if (querySessions.size() >= opts.cluster.runtime.queryMaxOpenCursors) { throw std::runtime_error("Cluster query: cursor limit exceeded"); }
          do { crypto::secureRandom(token); } while (querySessions.contains(token)); querySessions.emplace(token, std::move(session)); }
        response.value.assign(token.begin(), token.end()); return response;
    }
    if (action == QueryAction::NEXT || action == QueryAction::CLOSE || action == QueryAction::KEEP_ALIVE) {
        uint64_t offset = 0;
        size_t position = 1;
        if (!input.key.empty() || (action != QueryAction::NEXT ? input.value.size() != 1 :
            !pullU64(input.value, position, offset) || position != input.value.size())) {
            throw std::invalid_argument("Cluster query: malformed cursor request");
        }
        std::shared_ptr<QuerySession> session;
        {
            std::lock_guard lock{querySessionsMu};
            const auto it = querySessions.find(request.deduplicationId.nonce);
            if (it == querySessions.end()) {
                if (action == QueryAction::CLOSE) { return response; }
                throw std::runtime_error("Cluster query: cursor is expired or unknown");
            }
            session = it->second;
            if (action == QueryAction::CLOSE) { querySessions.erase(it); }
        }
        if (action == QueryAction::CLOSE) { return response; }
        std::lock_guard lock{session->mutex};
        if (session->expires <= std::chrono::steady_clock::now() || session->maxLifetime <= std::chrono::steady_clock::now()) { throw std::runtime_error("Cluster query: cursor expired"); }
        if (action == QueryAction::KEEP_ALIVE) {
            if (!session->pinOnly) { throw std::invalid_argument("Cluster query: cursor is not a retention pin"); }
            session->expires = now + std::chrono::milliseconds{opts.cluster.runtime.queryCursorIdleTimeoutMs}; return response;
        }
        if (session->pinOnly) { throw std::invalid_argument("Cluster query: retention pin has no result pages"); }
        if (session->streaming) {
            if (session->failed) { throw std::runtime_error("Cluster query: streaming cursor failed; start a new query"); }
            try {
                if (session->epoch != clusterRuntime->configurationEpoch()) { throw std::runtime_error("Cluster query: configuration changed during streaming"); }
                if (session->remotePins) { session->remotePins->renew(); }
                if (offset == session->replayOffset) { response.value = session->replay; return response; }
                if (offset != session->nextOffset) { throw std::invalid_argument("Cluster query: non-sequential streaming offset"); }
                response.value.push_back(0);
                while (response.value.size() <= QUERY_PAGE_BYTES && !session->finished) {
                    cluster::detail::ReconfigurationDeadline::check();
                    if (!session->started) { session->position = session->producer.begin(); session->started = true; }
                    if (session->position == std::default_sentinel) { session->finished = true; break; }
                    const auto& record = *session->position;
                    const auto count = std::min(record.size() - session->recordOffset, QUERY_PAGE_BYTES + 1 - response.value.size());
                    response.value.insert(response.value.end(), record.begin() + static_cast<std::ptrdiff_t>(session->recordOffset),
                        record.begin() + static_cast<std::ptrdiff_t>(session->recordOffset + count));
                    session->recordOffset += count;
                    if (session->recordOffset == record.size()) { session->recordOffset = 0; ++session->position; }
                    if (response.value.size() == QUERY_PAGE_BYTES + 1) { break; }
                }
                if (session->finished) { response.value[0] = 1; session->producer = {}; session->remotePins.reset(); session->gcPin.reset(); }
                session->replayOffset = offset;
                session->nextOffset += response.value.size() - 1;
                session->replay = response.value;
                session->expires = now + std::chrono::milliseconds{opts.cluster.runtime.queryCursorIdleTimeoutMs};
                return response;
            } catch (...) { session->failed = true; session->producer = {}; session->remotePins.reset(); session->gcPin.reset(); throw; }
        }
        if (offset > session->spool->size()) { throw std::invalid_argument("Cluster query: offset exceeds snapshot"); }
        response.value = session->spool->read(offset, static_cast<size_t>(std::min<uint64_t>(
            QUERY_PAGE_BYTES, session->spool->size() - offset)));
        session->expires = now + std::chrono::milliseconds{opts.cluster.runtime.queryCursorIdleTimeoutMs};
        return response;
    }
    if (input.value.size() < 31 || input.value[2] > static_cast<uint8_t>(cluster::QueryResultMode::STREAMING) || input.value[1] > static_cast<uint8_t>(cluster::ClusterReadMode::OWNER_LINEARIZABLE)) {
        throw std::invalid_argument("Cluster query: malformed open request");
    }
    const auto readMode = static_cast<cluster::ClusterReadMode>(input.value[1]);
    size_t position = 3;
    uint64_t epoch, term, seq;
    uint32_t endSize;
    std::vector<uint8_t> end;
    if (!pullU64(input.value, position, epoch) || !pullU64(input.value, position, term) ||
        !pullU64(input.value, position, seq) || !pullU32(input.value, position, endSize) ||
        !pullBytes(input.value, position, endSize, end)) {
        throw std::invalid_argument("Cluster query: malformed bounds");
    }
    std::vector<uint16_t> expectedPartitions;
    if (usesPartitionRaft() && (action == QueryAction::SCAN || action == QueryAction::COUNT)) {
        uint32_t count = 0;
        if (!pullU32(input.value, position, count) || count == 0 || count > clusterConfig.partition().partitionCount) {
            throw std::invalid_argument("Cluster query: missing partition coverage");
        }
        for (uint32_t i = 0; i < count; ++i) {
            uint32_t index = 0;
            if (!pullU32(input.value, position, index) || index >= clusterConfig.partition().partitionCount ||
                std::ranges::find(expectedPartitions, index) != expectedPartitions.end()) {
                throw std::invalid_argument("Cluster query: invalid partition coverage");
            }
            expectedPartitions.push_back(static_cast<uint16_t>(index));
        }
    }
    if (position != input.value.size()) { throw std::invalid_argument("Cluster query: trailing query payload"); }
    const bool stripe = clusterReplicationMode == cluster::ReplicationMode::STRIPE;
    const bool partition = clusterReplicationMode == cluster::ReplicationMode::PARTITIONED;
    if (epoch != clusterRuntime->configurationEpoch()) { throw std::runtime_error("Cluster query: configuration changed"); }
    if (partition && (action == QueryAction::HISTORY || action == QueryAction::GET_AT) &&
        clusterRuntime->ownerNodeId(input.key) != nodeId) { throw std::runtime_error("Cluster query: non-owner history request"); }
    if (stripe) {
        if (clusterRuntime->stripeMetadataLeaderNodeId() != nodeId) { throw std::runtime_error("Cluster query: STRIPE metadata leader changed"); }
        clusterRuntime->queryReadBarrier();
    }
    else if (readMode != cluster::ClusterReadMode::LOCAL_STALE_OK) {
        if (!partition && clusterRuntime->routeTarget(input.key).nodeId != nodeId) {
            throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::NOT_OWNER, clusterRuntime->routeTarget(input.key));
        }
        if (term != 0 && term != clusterRuntime->raftStats().currentTerm) { throw std::runtime_error("Cluster query: Raft term changed"); }
        // OWNER_ONLY skips Raft ReadIndex, but MIRROR fencing remains mandatory.
        if (readMode == cluster::ClusterReadMode::OWNER_LINEARIZABLE || clusterConfig.consistency().mode != cluster::ConsistencyMode::RAFT_QUORUM) {
            clusterRuntime->queryReadBarrier();
        }
    }
    const auto deadline = now + std::chrono::milliseconds{request.timeoutMs};
    const auto checkDeadline = [&] {
        if (std::chrono::steady_clock::now() >= deadline) { throw std::runtime_error("Cluster query: snapshot preparation timed out"); }
    };
    std::shared_ptr<QuerySession> session;
    std::array<uint8_t, 16> token{};
    if (action != QueryAction::COUNT) {
        session = std::make_shared<QuerySession>();
        session->expires = std::chrono::steady_clock::time_point::max();
        session->maxLifetime = now + std::chrono::milliseconds{opts.cluster.runtime.queryCursorMaxLifetimeMs};
        session->epoch = epoch;
        session->spool = std::make_unique<detail::ClusterQuerySpool>(
            opts.cluster.runtime.transfer.spoolDirectory.empty() ? fs::temp_directory_path() : opts.cluster.runtime.transfer.spoolDirectory,
            querySpoolBytes, opts.cluster.runtime.queryMaxSpoolBytes);
        std::lock_guard lock{querySessionsMu};
        if (querySessions.size() >= opts.cluster.runtime.queryMaxOpenCursors) { throw std::runtime_error("Cluster query: cursor limit exceeded"); }
        do { crypto::secureRandom(token); } while (querySessions.contains(token));
        querySessions.emplace(token, session);
    }
    uint64_t records = 0;
    try {
        core::ArenaGenerator<std::vector<uint8_t>> producer;
        std::shared_ptr<void> gcPin;
        if (stripe) { gcPin = pinStripeQuery(); }
        const bool streaming = action != QueryAction::COUNT && action != QueryAction::GET_AT &&
            static_cast<cluster::QueryResultMode>(input.value[2]) == cluster::QueryResultMode::STREAMING;
        std::shared_ptr<RemoteQueryPins> remotePins;
        if (streaming && stripe) {
            remotePins = std::make_shared<RemoteQueryPins>(); remotePins->engine = this;
            for (const auto& peer : currentPlacement().dataNodes()) {
                if (peer.nodeId == nodeId) { continue; }
                cluster::ForwardRequest pin; pin.entries.push_back({{}, {static_cast<uint8_t>(QueryAction::PIN)}});
                const auto ack = sendReadQuery(peer.nodeId, std::move(pin));
                if (ack.value.size() != 16) { throw std::runtime_error("Cluster query: invalid retention pin reply"); }
                std::array<uint8_t, 16> peerToken{}; std::copy_n(ack.value.begin(), 16, peerToken.begin());
                remotePins->peers.emplace_back(peer.nodeId, peerToken);
            }
        }
        if (action == QueryAction::GET_AT) {
            auto entry = publicVersionAtLocal(input.key, seq);
            if (entry) { producer = encodeQueryHistory(singleQueryVersion(std::move(*entry))); }
        } else if (action == QueryAction::HISTORY) { producer = encodeQueryHistory(publicHistoryLocal(input.key)); }
        else {
            auto begin = stripe ? stripeMetaKey(input.key) : input.key;
            auto finish = stripe ? (end.empty() ? std::vector<uint8_t>{0, 'A', 'K', 'S', 'M', '2'} : stripeMetaKey(end)) : end;
            producer = encodeQueryRecords(captureQueryRecords(begin, finish, expectedPartitions), action == QueryAction::COUNT,
                stripe, partition && !usesPartitionRaft(), std::move(gcPin), epoch);
        }
        if (streaming) {
            session->streaming = true; session->producer = std::move(producer);
            session->remotePins = std::move(remotePins); session->gcPin = std::move(gcPin); session->spool.reset();
            session->expires = std::chrono::steady_clock::now() + std::chrono::milliseconds{opts.cluster.runtime.queryCursorIdleTimeoutMs};
            response.value.assign(token.begin(), token.end()); pushU64(response.value, UINT64_MAX); pushU64(response.value, UINT64_MAX);
            return response;
        }
        for (const auto& record : producer) {
            checkDeadline();
            if (action != QueryAction::COUNT) { session->spool->append(record); }
            if (records == UINT64_MAX) { throw std::overflow_error("Cluster query: count overflow"); }
            ++records;
        }
        checkDeadline();
        if (action == QueryAction::COUNT) { pushU64(response.value, records); }
        else {
            std::lock_guard lock{session->mutex};
            session->expires = std::chrono::steady_clock::now() + std::chrono::milliseconds{opts.cluster.runtime.queryCursorIdleTimeoutMs};
            response.value.assign(token.begin(), token.end());
            pushU64(response.value, session->spool->size()); pushU64(response.value, records);
        }
        return response;
    }
    catch (...) {
        if (session) { std::lock_guard lock{querySessionsMu}; querySessions.erase(token); }
        throw;
    }
}

cluster::ForwardResponse sendReadQuery(uint64_t target, cluster::ForwardRequest request) {
    request.operation = cluster::ForwardOperation::READ_QUERY;
    request.timeoutMs = request.timeoutMs == 0 ? opts.cluster.runtime.forwardingTimeoutMs :
        std::min(request.timeoutMs, opts.cluster.runtime.forwardingTimeoutMs);
    if (request.entries.size() != 1 || request.entries[0].key.size() > cluster::MAX_FORWARD_PAYLOAD - 64 ||
        request.entries[0].value.size() > cluster::MAX_FORWARD_PAYLOAD - 64 - request.entries[0].key.size()) {
        throw std::length_error("Cluster query: request too large");
    }
    request.timeoutMs = cluster::detail::ReconfigurationDeadline::remaining(request.timeoutMs);
    cluster::ForwardResponse response;
    // The public caller already holds admission through cursor destruction.
    // Local prepared snapshots remain consumable while close waits for them.
    if (target == nodeId) { response = receiveReadQuery(request); }
    else {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{request.timeoutMs};
        while (true) {
            const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now()).count();
            if (remaining <= 0) { throw std::runtime_error("Cluster query: destination unavailable before timeout"); }
            request.timeoutMs = static_cast<uint32_t>(remaining);
            try { response = clusterRuntime->forwardTo(target, request); break; }
            catch (const cluster::ClusterRoutingError& error) {
                // Only wait/retry when the transport guarantees nothing was sent.
                // A lost OPEN response can leave one orphan cursor until its TTL.
                if (error.code != cluster::ClusterRoutingCode::FORWARD_UNAVAILABLE) { throw; }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{10});
        }
    }
    if (!response.success) { throw std::runtime_error("Cluster query failed: " + response.message); }
    return response;
}

std::vector<uint64_t> queryTargets(std::span<const uint8_t> key, bool range) {
    const auto readMode = opts.cluster.runtime.readMode;
    std::vector<uint64_t> targets;
    if (usesPartitionRaft() && !range) {
        const auto index = partitionIndex(key);
        const auto placement = livePlacement.load();
        const auto response = partitionRpcLeader(index, partitionAdminHeader(PartitionAdmin::STATUS, index, 0), *placement,
            std::chrono::steady_clock::now() + std::chrono::milliseconds{opts.cluster.runtime.forwardingTimeoutMs});
        targets.push_back(response.target.nodeId);
        partitionLeaderHints[index].store(response.target.nodeId, std::memory_order_release);
    }
    else if (clusterReplicationMode == cluster::ReplicationMode::STRIPE) {
        targets.push_back(clusterRuntime->stripeMetadataLeaderNodeId());
    }
    else if (clusterReplicationMode == cluster::ReplicationMode::PARTITIONED && range) {
        for (const auto& node : clusterConfig.dataNodes()) { targets.push_back(node.nodeId); }
    }
    // Replica physical apply sequences are not owner stream revisions.
    else if (clusterReplicationMode == cluster::ReplicationMode::PARTITIONED) {
        targets.push_back(clusterRuntime->ownerNodeId(key));
    }
    else if (readMode == cluster::ClusterReadMode::LOCAL_STALE_OK) { targets.push_back(nodeId); }
    else { targets.push_back(clusterRuntime->routeTarget(key).nodeId); }
    for (const auto target : targets) {
        if (target == 0) { throw std::runtime_error("Cluster query: read authority unavailable"); }
        if (readMode == cluster::ClusterReadMode::OWNER_ONLY && target != nodeId) {
            throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::NOT_OWNER, clusterRuntime->routeTarget(key),
                "Cluster query requires a remote authority but readMode is OWNER_ONLY");
        }
    }
    return targets;
}

cluster::ForwardRequest openQueryRequest(QueryAction action, std::span<const uint8_t> key,
    std::span<const uint8_t> end, uint64_t seq, std::span<const uint16_t> partitions = {}) {
    cluster::ForwardRequest request;
    auto& entry = request.entries.emplace_back();
    entry.key.assign(key.begin(), key.end());
    entry.value.push_back(static_cast<uint8_t>(action));
    entry.value.push_back(static_cast<uint8_t>(opts.cluster.runtime.readMode));
    entry.value.push_back(static_cast<uint8_t>(opts.cluster.runtime.queryResultMode));
    pushU64(entry.value, clusterRuntime->configurationEpoch());
    pushU64(entry.value, clusterConfig.consistency().mode == cluster::ConsistencyMode::RAFT_QUORUM ? clusterRuntime->raftStats().currentTerm : 0);
    pushU64(entry.value, seq);
    if (end.size() > UINT32_MAX) { throw std::length_error("Cluster query: oversized end bound"); }
    pushU32(entry.value, static_cast<uint32_t>(end.size()));
    entry.value.insert(entry.value.end(), end.begin(), end.end());
    if (usesPartitionRaft() && (action == QueryAction::SCAN || action == QueryAction::COUNT)) {
        pushU32(entry.value, static_cast<uint32_t>(partitions.size()));
        for (const auto index : partitions) { pushU32(entry.value, index); }
    }
    return request;
}

struct QueryReader {
    std::shared_ptr<OperationGuard> operationLifetime;
    Impl* engine;
    uint64_t target;
    std::array<uint8_t, 16> token{};
    uint64_t total = 0, records = 0, offset = 0;
    std::vector<uint8_t> page;
    size_t position = 0;
    bool opened = false, streaming = false, eof = false;
    QueryReader(Impl& impl, uint64_t node, cluster::ForwardRequest request) : engine{&impl}, target{node} {
        const auto response = engine->sendReadQuery(target, std::move(request));
        if (response.value.size() != 32) { throw std::runtime_error("Cluster query: invalid cursor response"); }
        std::copy_n(response.value.begin(), token.size(), token.begin());
        size_t cursor = token.size();
        (void)pullU64(response.value, cursor, total); (void)pullU64(response.value, cursor, records);
        opened = true;
        streaming = total == UINT64_MAX && records == UINT64_MAX;
        if ((records == 0) != (total == 0)) { throw std::runtime_error("Cluster query: invalid snapshot length/count"); }
    }
    ~QueryReader() {
        if (!opened) { return; }
        try {
            cluster::ForwardRequest request;
            request.timeoutMs = 100;
            request.deduplicationId.nonce = token;
            request.entries.push_back({{}, {static_cast<uint8_t>(QueryAction::CLOSE)}});
            (void)engine->sendReadQuery(target, std::move(request));
        }
        catch (...) {} // Server TTL also reclaims cursors after disconnect/abandonment.
    }
    QueryReader(const QueryReader&) = delete;
    bool available() {
        if (position < page.size()) { return true; }
        if (streaming && eof) { return false; }
        if (!streaming && offset == total) { return false; }
        cluster::ForwardRequest request; request.deduplicationId.nonce = token;
        auto& entry = request.entries.emplace_back();
        entry.value.push_back(static_cast<uint8_t>(QueryAction::NEXT)); pushU64(entry.value, offset);
        auto response = engine->sendReadQuery(target, std::move(request));
        page = std::move(response.value); position = 0;
        if (streaming) {
            if (page.empty() || page.front() > 1 || page.size() > QUERY_PAGE_BYTES + 1) { throw std::runtime_error("Cluster query: malformed streaming page"); }
            eof = page.front() != 0; page.erase(page.begin());
            if (page.empty() && !eof) { throw std::runtime_error("Cluster query: empty unfinished page"); }
        } else if (page.empty() || page.size() > QUERY_PAGE_BYTES || page.size() > total - offset) { throw std::runtime_error("Cluster query: malformed page"); }
        return !page.empty();
    }
    void read(std::span<uint8_t> output) {
        if (!streaming && (offset > total || output.size() > total - offset)) { throw std::runtime_error("Cluster query: truncated snapshot"); }
        size_t done = 0;
        while (done < output.size()) {
            if (!available()) { throw std::runtime_error("Cluster query: truncated streaming record"); }
            const size_t count = std::min(output.size() - done, page.size() - position);
            std::copy_n(page.begin() + static_cast<std::ptrdiff_t>(position), count, output.begin() + static_cast<std::ptrdiff_t>(done));
            position += count; done += count; offset += count;
        }
    }
    uint64_t integer() {
        std::array<uint8_t, 8> bytes{}; read(bytes);
        size_t cursor = 0; uint64_t value = 0; (void)pullU64(bytes, cursor, value); return value;
    }
    std::vector<uint8_t> bytes(uint64_t count) {
        if (count > SIZE_MAX || (!streaming && (offset > total || count > total - offset))) { throw std::runtime_error("Cluster query: invalid record length"); }
        std::vector<uint8_t> value(static_cast<size_t>(count)); read(value); return value;
    }
    std::optional<std::pair<std::vector<uint8_t>, std::vector<uint8_t>>> nextRecord() {
        if ((streaming && !available()) || (!streaming && records == 0)) {
            if (!streaming && offset != total) { throw std::runtime_error("Cluster query: trailing snapshot data"); }
            return {};
        }
        const auto keySize = integer(), valueSize = integer();
        auto key = bytes(keySize), value = bytes(valueSize); if (!streaming) { --records; }
        return std::pair{std::move(key), std::move(value)};
    }
    std::optional<VersionEntry> nextVersion() {
        if ((streaming && !available()) || (!streaming && records == 0)) {
            if (!streaming && offset != total) { throw std::runtime_error("Cluster query: trailing history data"); }
            return {};
        }
        VersionEntry entry;
        entry.seq = integer(); entry.sourceNodeId = integer(); entry.timestampNs = integer();
        read(std::span<uint8_t>{&entry.flags, 1}); entry.value = bytes(integer()); if (!streaming) { --records; }
        if (!streaming && records == 0 && offset != total) { throw std::runtime_error("Cluster query: trailing history data"); }
        return entry;
    }
};

std::optional<std::vector<uint8_t>> getAtValue(std::span<const uint8_t> key, uint64_t atSeq) {
    throwIfBackgroundFailed();
    waitForVersionLogRecovery();
    if (clusterRuntime) {
        const auto target = queryTargets(key, false).front();
        QueryReader reader{*this, target, openQueryRequest(QueryAction::GET_AT, key, {}, atSeq)};
        if (reader.records == 0) { return {}; }
        if (reader.records != 1) { throw std::runtime_error("AkkEngine: invalid historical result count"); }
        return reader.nextVersion()->value;
    }
    auto entry = publicVersionAtLocal(key, atSeq);
    return entry ? std::optional{std::move(entry->value)} : std::nullopt;
}

core::ArenaGenerator<AkkEngine::ScanRecordView> clusterScanGenerator(core::BufferArena& arena,
    std::vector<std::unique_ptr<QueryReader>> readers, std::shared_ptr<OperationGuard> operation) {
    (void)operation;
    using Record = std::pair<std::vector<uint8_t>, std::vector<uint8_t>>;
    std::vector<std::optional<Record>> heads;
    for (auto& reader : readers) { heads.push_back(reader->nextRecord()); }
    std::optional<std::vector<uint8_t>> previous;
    while (true) {
        size_t selected = readers.size();
        for (size_t i = 0; i < heads.size(); ++i) {
            if (heads[i] && (selected == readers.size() || compareKey(heads[i]->first, heads[selected]->first) < 0)) { selected = i; }
        }
        if (selected == readers.size()) { co_return; }
        auto& record = *heads[selected];
        if (previous && compareKey(*previous, record.first) >= 0) { throw std::runtime_error("Cluster query: duplicate or unordered public key"); }
        previous = record.first;
        co_yield AkkEngine::ScanRecordView{copySpanToArena(record.first, arena), copySpanToArena(record.second, arena)};
        heads[selected] = readers[selected]->nextRecord();
    }
}

static core::ArenaGenerator<VersionEntry> publicHistoryGenerator(core::ArenaGenerator<VersionEntry> local,
    std::unique_ptr<QueryReader> reader, std::shared_ptr<OperationGuard> operation) {
    (void)operation;
    if (reader) { while (auto entry = reader->nextVersion()) { co_yield std::move(*entry); } }
    else { for (const auto& entry : local) { co_yield entry; } }
    local = {}; reader.reset(); operation.reset();
}

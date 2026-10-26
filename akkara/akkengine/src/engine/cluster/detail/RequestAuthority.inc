/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
// Included in ClusterRuntime.cpp's anonymous namespace.
class RequestAuthority {
    struct Receipt {
        std::array<uint8_t, 32> fingerprint{};
        std::array<uint8_t, 16> attempt{};
        bool applied = false;
        uint64_t sequence = 0, index = 0;
    };
    struct State {
        uint64_t applied = 0, floor = 0, generation = 0, count = 0, bytes = 0;

    };
    std::mutex mutex_;
    std::timed_mutex proposalMutex_;
    State state_;
    std::shared_ptr<detail::DiskResults> receipts_;
    std::set<ClusterRequestId> pending_;
    std::filesystem::path path_;
    detail::AuthoritySnapshotFile snapshotFile_;
    ClusterRequestOptions options_;
    std::unique_ptr<RaftConsensusRuntime> raft_;
    std::vector<NodeInfo> nodes_;
    uint64_t self_;
    uint32_t timeoutMs_;
    ClusterConfig dataConfig_;
    ClusterRuntimeOptions runtimeOptions_;
    mutable std::mutex topologyMu_;
    bool hosted_;
    std::atomic<uint64_t> preferredLeader_{0};
    static constexpr size_t headerSize = 45, recordSize = 65;
    static constexpr size_t maxImageSize = headerSize;
    static uint64_t nowMs() {
        return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
    }
    static std::vector<uint8_t> encode(const State& state) {
        auto bytes = std::vector<uint8_t>{'A','K','R','A','1'};
        for (auto field : {state.applied, state.floor, state.generation, state.count, state.bytes}) { writeLe64(bytes, field); }
        return bytes;
    }
    static State decode(std::span<const uint8_t> bytes) {
        if (bytes.size() != headerSize || !std::ranges::starts_with(bytes, std::string_view{"AKRA1"})) { throw std::runtime_error("Request authority: invalid state image"); }
        State state{readLe64(bytes, 5), readLe64(bytes, 13), readLe64(bytes, 21), readLe64(bytes, 29), readLe64(bytes, 37)};
        if (state.count > UINT64_MAX / 155 || state.bytes != state.count * 155 || (!state.generation && (state.count || state.bytes))) {
            throw std::runtime_error("Request authority: invalid result-store reference");
        }
        return state;
    }
    void observeResults(State& state) {
        state.generation = receipts_->generation(); state.count = receipts_->count(); state.bytes = receipts_->bytes();
    }
    static std::vector<uint8_t> token(const ClusterRequestId& id) {
        std::vector key(id.nonce.begin(), id.nonce.end()); writeLe64(key, id.expiresAtUnixMs); return key;
    }
    static std::vector<uint8_t> encodeReceipt(const Receipt& receipt) {
        std::vector bytes(receipt.fingerprint.begin(), receipt.fingerprint.end());
        bytes.insert(bytes.end(), receipt.attempt.begin(), receipt.attempt.end()); bytes.push_back(receipt.applied ? 1 : 0);
        writeLe64(bytes, receipt.sequence); writeLe64(bytes, receipt.index); return bytes;
    }
    static Receipt decodeReceipt(std::span<const uint8_t> bytes) {
        if (bytes.size() != recordSize || bytes[48] > 1) { throw std::runtime_error("Request authority: malformed retained receipt"); }
        Receipt receipt; std::copy_n(bytes.begin(), 32, receipt.fingerprint.begin()); std::copy_n(bytes.begin() + 32, 16, receipt.attempt.begin());
        receipt.applied = bytes[48] != 0; receipt.sequence = readLe64(bytes, 49); receipt.index = readLe64(bytes, 57);
        if (!receipt.index || (!receipt.applied && receipt.sequence) || !std::ranges::any_of(receipt.attempt, [](auto byte) { return byte != 0; })) {
            throw std::runtime_error("Request authority: invalid retained receipt");
        }
        return receipt;
    }
    std::optional<Receipt> retained(const ClusterRequestId& id) const {
        const auto record = receipts_->get(token(id)); if (!record) { return {}; }
        const auto receipt = decodeReceipt(record->value);
        if (receipt.index != record->sequence || record->expires != id.expiresAtUnixMs) { throw std::runtime_error("Request authority: invalid retained identity"); }
        return receipt;
    }
    void rebuildPending() {
        pending_.clear(); const auto snapshot = receipts_->capture();
        snapshot.forEach([&](auto key, const detail::DiskResults::Record& record) {
            if (key.size() != 24) { throw std::runtime_error("Request authority: invalid retained key"); }
            const auto receipt = decodeReceipt(record.value);
            if (!receipt.applied) {
                ClusterRequestId id; std::copy_n(key.begin(), 16, id.nonce.begin()); id.expiresAtUnixMs = readLe64(key, 16); pending_.insert(id);
            }
            return true;
        });
    }
    void installSnapshot(uint64_t sequence) {
        std::optional<State> state; std::optional<detail::DiskResults::Import> target;
        std::vector<uint8_t> key, value;
        SnapshotEntryVisitor visitor;
        visitor.beginEntry = [&](auto bytes, uint64_t size, uint32_t) {
            if (size > recordSize) { throw std::runtime_error("Request authority: oversized snapshot entry"); }
            key.assign(bytes.begin(), bytes.end()); value.clear(); return true;
        };
        visitor.appendValueChunk = [&](uint64_t offset, auto bytes) {
            if (offset != value.size() || bytes.size() > recordSize - value.size()) { throw std::runtime_error("Request authority: invalid snapshot chunk"); }
            value.insert(value.end(), bytes.begin(), bytes.end()); return true;
        };
        visitor.finishEntry = [&] {
            if (key == std::vector<uint8_t>{'S'}) {
                if (state) { throw std::runtime_error("Request authority: duplicate snapshot header"); }
                state = decode(value); if (state->applied != sequence) { throw std::runtime_error("Request authority: invalid snapshot cut"); }
                target = receipts_->beginImport(sequence, state->floor);
            } else {
                if (!state || key.size() != 25 || key[0] != 'R') { throw std::runtime_error("Request authority: malformed snapshot receipt"); }
                const auto id = std::span{key}.subspan(1); const auto receipt = decodeReceipt(value);
                if (!std::ranges::any_of(id.first(16), [](auto byte) { return byte != 0; })) { throw std::runtime_error("Request authority: invalid snapshot identity"); }
                receipts_->import(*target, id, {readLe64(id, 16), receipt.index, value});
            }
            return true;
        };
        if (!snapshotFile_.replay(sequence, visitor) || !state || target->count != state->count || target->bytes != state->bytes) { throw std::runtime_error("Request authority: incomplete snapshot"); }
        receipts_->prepare(*target);
        state->generation = target->generation;
        // The state file names a fully synced generation before activation.
        // Recovery can activate that reference after any interrupted install.
        std::lock_guard lock{mutex_};
        saveFencingImage(path_, encode(*state)); receipts_->activate(target->generation); state_ = *state;
        rebuildPending(); receipts_->collectGarbage();
    }
    void publish(State next) {
        saveFencingImage(path_, encode(next)); state_ = std::move(next);
    }
    uint64_t validate(const ClusterRequestId& id) {
        if (!id.expiresAtUnixMs || !std::ranges::any_of(id.nonce, [](auto b) { return b != 0; })) {
            throw std::invalid_argument("Request authority: invalid request identity");
        }
        const auto now = std::max(nowMs(), state_.floor);
        if (id.expiresAtUnixMs > now && id.expiresAtUnixMs - now > options_.maxRetentionMs) {
            throw std::invalid_argument("Request authority: request exceeds maximum retention");
        }
        return now;
    }
    static ClusterRequestResult view(const Receipt& receipt) {
        return {receipt.applied ? ClusterRequestStatus::APPLIED : ClusterRequestStatus::PENDING,
            receipt.sequence, 0};
    }
    static RaftOptions membershipOptions() {
        return {.membership = {.mode = RaftMembershipMode::JOINT_CONSENSUS, .allowOnlineVoterChanges = true, .allowLearners = true}};
    }
    ClusterConfig controlConfig(std::vector<NodeInfo> nodes) const {
        std::lock_guard lock{topologyMu_};
        for (auto& node : nodes) {
            if (const auto* physical = dataConfig_.findById(node.nodeId)) { node.hostId = physical->hostId; node.siteId = physical->siteId; }
            node.dataPort = node.stripeMetadataPort = node.partitionReplBasePort = 0;
        }
        return ClusterConfig{std::move(nodes), ReplicationMode::STANDALONE, {},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = dataConfig_.consistency().ackTimeoutMs},
            membershipOptions(), {}, 0, dataConfig_.clusterId(), {}, FailoverPolicy::DEFAULT, dataConfig_.faultDomains()};
    }
    ForwardResponse receive(const ForwardRequest& request) {
        auto response = receiveCore(request);
        const auto topology = controlConfig(raft_->activeNodes()).encode();
        response.value.insert(response.value.end(), topology.begin(), topology.end());
        return response;
    }
    ForwardResponse receiveCore(const ForwardRequest& request) {
        if (request.operation == ForwardOperation::CLUSTER_ADMIN && request.entries.size() == 1 && request.entries.front().key == std::vector<uint8_t>{'A','K','R','C','1'}) {
            if (raft_->role() != NodeRole::PRIMARY) {
                ClusterRouteTarget target; target.nodeId = raft_->leaderNodeId();
                throw ClusterRoutingError(ClusterRoutingCode::NOT_OWNER, target, "Request authority: reconfiguration requires leader");
            }
            const auto target = ClusterConfig::decode(request.entries.front().value);
            validateTarget(target.nodes());
            if (!PlacementAuthority::matches(target.encode(), controlConfig(target.nodes()).encode())) { throw std::invalid_argument("Request authority: invalid reconfiguration policy"); }
            auto serial = detail::ReconfigurationDeadline::lock(proposalMutex_);
            auto active = raft_->activeNodes();
            for (auto node : target.nodes()) {
                if (!std::ranges::contains(active, node.nodeId, &NodeInfo::nodeId)) {
                    node.capabilities |= RAFT_LEARNER; raft_->addLearner(node);
                }
            }
            raft_->reconfigure(target);
            ForwardResponse response; response.success = true; response.value = {0}; return response;
        }
        if (request.operation != ForwardOperation::CLUSTER_ADMIN || request.entries.size() != 1 || !request.entries.front().key.empty() || request.entries.front().value.size() != 90 || request.entries.front().value[0] > 2) {
            throw std::invalid_argument("Request authority: malformed control request");
        }
        if (raft_->role() != NodeRole::PRIMARY) {
            ClusterRouteTarget target; target.nodeId = raft_->stats().leaderNodeId;
            throw ClusterRoutingError(ClusterRoutingCode::NOT_OWNER, target, "Request authority: leader required");
        }
        const auto action = request.entries.front().value[0];
        ClusterRequestId id; std::copy_n(request.entries.front().value.begin() + 1, 16, id.nonce.begin());
        id.expiresAtUnixMs = readLe64(request.entries.front().value, 17);
        Receipt incoming;
        std::copy_n(request.entries.front().value.begin() + 25, 32, incoming.fingerprint.begin());
        std::copy_n(request.entries.front().value.begin() + 57, 16, incoming.attempt.begin());
        incoming.sequence = readLe64(request.entries.front().value, 81);
        if (request.entries.front().value[89] != 0) { throw std::invalid_argument("Request authority: reserved byte is nonzero"); }
        auto serial = detail::ReconfigurationDeadline::lock(proposalMutex_);
        raft_->linearizableReadBarrier();
        ForwardResponse response; response.success = true; response.value = {0};
        uint64_t now;
        {
            std::lock_guard lock{mutex_}; now = validate(id);
            if (id.expiresAtUnixMs <= now) { response.requestResult.status = ClusterRequestStatus::EXPIRED; return response; }
            const auto found = retained(id);
            if (found) {
                if (action != 2 && found->fingerprint != incoming.fingerprint) { throw ClusterRoutingError(ClusterRoutingCode::WRITE_REJECTED, {}, "Request authority: ID reused for a different mutation"); }
                if (action != 1 || found->applied) { response.requestResult = view(*found); return response; }
                if (found->attempt != incoming.attempt) { throw std::invalid_argument("Request authority: foreign completion token"); }
            } else {
                if (action == 2) { return response; }
                if (action == 1) { throw std::invalid_argument("Request authority: completion without claim"); }
                const auto pending = std::ranges::count_if(pending_, [now](const auto& token) { return token.expiresAtUnixMs > now; });
                if (pending >= options_.maxPendingRequests || !receipts_->canReserve(detail::DiskResults::recordCharge(24, recordSize), now)) {
                    throw ClusterRoutingError(ClusterRoutingCode::WRITE_REJECTED, {}, "Request authority: pending-claim or retained-result disk budget exhausted");
                }
            }
        }
        auto command = request.entries.front().value;
        for (auto i = size_t{0}; i < 8; ++i) { command[73 + i] = static_cast<uint8_t>(now >> (8 * i)); }
        const auto prepare = [command](uint64_t) {
            ClusterMutation mutation; mutation.key = {'R'}; mutation.value = command; return mutation;
        };
        if (action == 0) {
            auto completion = raft_->submitRequest(id, incoming.fingerprint, prepare);
            (void)detail::ReconfigurationDeadline::wait(completion);
        } else {
            auto proposal = raft_->submitMutation(prepare);
            detail::ReconfigurationDeadline::wait(proposal.completion);
        }
        std::lock_guard lock{mutex_};
        const auto found = retained(id);
        if (!found) { response.requestResult.status = ClusterRequestStatus::EXPIRED; return response; }
        response.requestResult = view(*found);
        // A claim is granted only in the invocation that durably inserted it.
        response.value[0] = action == 0 && found->attempt == incoming.attempt ? 1 : 0;
        return response;
    }
    ForwardResponse rpc(uint8_t action, const ClusterRequestId& id, const Receipt& receipt) {
        detail::ReconfigurationDeadline budget{timeoutMs_};
        ForwardRequest request; request.operation = ForwardOperation::CLUSTER_ADMIN; request.timeoutMs = timeoutMs_; request.entries.emplace_back();
        request.entries.front().value = {action}; request.entries.front().value.insert(request.entries.front().value.end(), id.nonce.begin(), id.nonce.end());
        writeLe64(request.entries.front().value, id.expiresAtUnixMs);
        request.entries.front().value.insert(request.entries.front().value.end(), receipt.fingerprint.begin(), receipt.fingerprint.end());
        request.entries.front().value.insert(request.entries.front().value.end(), receipt.attempt.begin(), receipt.attempt.end());
        writeLe64(request.entries.front().value, 0); writeLe64(request.entries.front().value, receipt.sequence); request.entries.front().value.push_back(0);
        if (raft_->role() == NodeRole::PRIMARY) { return receive(request); }
        // Only explicit NOT_OWNER permits another hop. A timeout never replays
        // a claim, because it might already have committed at the old leader.
        uint64_t target = hosted_ ? raft_->leaderNodeId() : preferredLeader_.load();
        if (!target) { const auto nodes = raft_->activeNodes(); if (!nodes.empty()) { target = nodes.front().nodeId; } }
        if (!target) { throw ClusterRoutingError(ClusterRoutingCode::FORWARD_UNAVAILABLE, {}, "Request authority: leader is not known"); }
        for (size_t hop = 0; hop < 512; ++hop) {
            detail::ReconfigurationDeadline::check();
            try {
                auto response = raft_->forwardTo(target, request);
                if (response.success) { learnTopology(response); preferredLeader_.store(target); return response; }
                if (response.errorCode != ClusterRoutingCode::NOT_OWNER || !response.target.nodeId) {
                    throw ClusterRoutingError(response.errorCode, response.target, "Request authority: control request failed");
                }
                target = response.target.nodeId;
            } catch (const ClusterRoutingError& error) {
                if (error.code == ClusterRoutingCode::NOT_OWNER && error.target.nodeId) { target = error.target.nodeId; }
                else if (error.code == ClusterRoutingCode::FORWARD_UNAVAILABLE || (action == 2 && error.code == ClusterRoutingCode::OUTCOME_UNKNOWN)) {
                    const auto candidates = raft_->activeNodes();
                    const auto current = std::ranges::find(candidates, target, &NodeInfo::nodeId);
                    if (candidates.size() < 2) { throw; }
                    const auto position = current == candidates.end() ? 0 : (static_cast<size_t>(current - candidates.begin()) + 1) % candidates.size();
                    target = candidates[position].nodeId; preferredLeader_.store(0);
                } else { throw; }
            }
            if (target == self_ && hosted_) { return receive(request); }
        }
        throw ClusterRoutingError(ClusterRoutingCode::FORWARD_UNAVAILABLE, {}, "Request authority: leadership did not stabilize");
    }
public:
    RequestAuthority(const std::filesystem::path& dir, const ClusterConfig& config, uint64_t self, ClusterRuntimeOptions options)
        : path_{dir / "request-authority" / "state.akrq"}, snapshotFile_{path_.parent_path() / "state.snapshot"}, options_{options.requests}, nodes_{options.requests.authorityNodes}, self_{self}, timeoutMs_{options.forwardingTimeoutMs},
          dataConfig_{config}, runtimeOptions_{options}, hosted_{std::ranges::contains(nodes_, self, &NodeInfo::nodeId)} {
        if (dir.empty()) { throw std::invalid_argument("Request authority: persistent directory required"); }
        if (std::filesystem::exists(path_)) {
            if (std::filesystem::file_size(path_) > maxImageSize + 4) { throw std::runtime_error("Request authority: oversized durable state"); }
            state_ = decode(loadFencingImage(path_));
        }
        if (hosted_) {
            receipts_ = std::make_shared<detail::DiskResults>(path_.parent_path() / "receipts", options_.maxRetainedResultBytes);
            if (state_.generation && receipts_->generation() != state_.generation) { receipts_->activate(state_.generation); }
            state_.applied = std::max(state_.applied, receipts_->sequence()); state_.floor = std::max(state_.floor, receipts_->floor());
            observeResults(state_); publish(state_); rebuildPending();
        }
        for (auto& node : nodes_) {
            const auto* physical = config.findById(node.nodeId);
            node.hostId = physical->hostId; node.siteId = physical->siteId;
            node.dataPort = node.stripeMetadataPort = node.partitionReplBasePort = 0;
        }
        auto callbacks = ClusterEngineCallbacks{};
        callbacks.getLastSeq = callbacks.getCurrentSeq = [this] { std::lock_guard lock{mutex_}; return state_.applied; };
        callbacks.forceDurable = [] {};
        callbacks.forward = [this](const ForwardRequest& request) { return receive(request); };
        callbacks.apply = [this](uint64_t seq, ReplOpType op, auto key, auto bytes, uint8_t flags, uint64_t, uint64_t) {
            if (op != ReplOpType::PUT || key.size() != 1 || key[0] != 'R' || flags || bytes.size() != 90 || bytes[0] > 1 || bytes[89]) {
                throw std::runtime_error("Request authority: malformed command");
            }
            ClusterRequestId id; std::copy_n(bytes.begin() + 1, 16, id.nonce.begin()); id.expiresAtUnixMs = readLe64(bytes, 17);
            Receipt incoming; std::copy_n(bytes.begin() + 25, 32, incoming.fingerprint.begin());
            std::copy_n(bytes.begin() + 57, 16, incoming.attempt.begin()); incoming.index = seq;
            std::lock_guard lock{mutex_}; if (seq <= state_.applied) { return; }
            State next = state_; next.floor = std::max(next.floor, readLe64(bytes, 73));
            receipts_->advanceFloor(next.floor); std::erase_if(pending_, [&](const auto& token) { return token.expiresAtUnixMs <= next.floor; });
            if (id.expiresAtUnixMs > next.floor) {
                if (bytes[0] == 0) {
                    if (!retained(id)) { receipts_->put(token(id), {id.expiresAtUnixMs, seq, encodeReceipt(incoming)}); pending_.insert(id); }
                } else {
                    const auto found = retained(id);
                    if (!found || found->attempt != incoming.attempt || found->fingerprint != incoming.fingerprint) { throw std::runtime_error("Request authority: invalid completion"); }
                    incoming.applied = true; incoming.sequence = readLe64(bytes, 81);
                    receipts_->put(token(id), {id.expiresAtUnixMs, seq, encodeReceipt(incoming)}); pending_.erase(id);
                }
            }
            receipts_->sync(); next.applied = seq; observeResults(next); publish(next);
        };
        callbacks.exportSnapshot = [this]() -> std::optional<ClusterSnapshot> {
            std::lock_guard lock{mutex_}; if (!state_.applied) { return std::nullopt; }
            const auto state = state_; const auto results = receipts_->capture();
            return ClusterSnapshot{.seq = state.applied, .forEachEntry = [state, results](const SnapshotEntryVisitor& visitor) {
                const auto image = encode(state); const std::array<uint8_t, 1> header{'S'};
                const auto send = [&](auto key, const auto& value) {
                    return visitor.beginEntry(key, value.size(), cpu::CRC32C(reinterpret_cast<const std::byte*>(value.data()), value.size())) &&
                        visitor.appendValueChunk(0, value) && visitor.finishEntry();
                };
                if (!send(std::span{header}, image)) { return false; }
                return results.forEach([&](auto id, const detail::DiskResults::Record& record) {
                    auto key = std::vector<uint8_t>{'R'}; key.insert(key.end(), id.begin(), id.end()); return send(std::span{key}, record.value);
                });
            }};
        };
        callbacks.beginSnapshot = [this](uint64_t seq, uint64_t) { snapshotFile_.begin(seq); };
        callbacks.beginSnapshotEntry = [this](auto key, uint64_t size, uint32_t crc) { snapshotFile_.beginEntry(key, size, crc); };
        callbacks.appendSnapshotEntryChunk = [this](uint64_t offset, auto bytes) { snapshotFile_.append(offset, bytes); };
        callbacks.finishSnapshotEntry = [this] { snapshotFile_.finishEntry(); };
        callbacks.prepareSnapshot = [this](uint64_t seq, uint64_t count) {
            snapshotFile_.prepare(seq, count); syncFile(snapshotFile_.path(), "Request authority snapshot");
#ifndef _WIN32
            syncParentDirectory(snapshotFile_.path(), "Request authority snapshot");
#endif
        };
        callbacks.finishSnapshot = [this](uint64_t seq, uint64_t) { installSnapshot(seq); };
        callbacks.recoverSnapshot = [this](uint64_t seq) { installSnapshot(seq); };
        callbacks.isSnapshotDurable = [this](uint64_t seq) { std::lock_guard lock{mutex_}; return state_.applied >= seq; };
        options.mirrorFencing = {}; options.mirrorPromotion = {}; options.mirrorRecovery = {};
        options.clusterMembershipPath.clear(); options.resetClusterMembership = false;
        options.requests.enabled = true; options.requests.persistLocalApplication = false;
        options.requests.authorityNodes.clear(); options.transactions = {};
        options.primaryNodeId = 0; options.primaryHost.clear(); options.primaryReplPort = 0;
        options.secure.expectedPrimaryNodeId = 0;
        options.transfer.spoolDirectory = path_.parent_path() / "transfer-spool";
        options.raftGroupKind = RaftGroupKind::REQUEST_AUTHORITY; options.raftPartitionIndex = 0;
        auto control = controlConfig(nodes_);
        if (hosted_) {
            std::vector<uint64_t> clients; for (const auto& node : config.nodes()) { clients.push_back(node.nodeId); }
            raft_ = RaftConsensusRuntime::create(path_.parent_path(), std::move(control), self, std::move(callbacks), std::move(options), std::move(clients));
        } else { raft_ = RaftConsensusRuntime::createRequestClient(std::move(control), *config.findById(self), std::move(options)); }
    }
    void learnTopology(const ForwardResponse& response) {
        if (!hosted_ && response.value.size() > 1) {
            const auto control = ClusterConfig::decode(std::span<const uint8_t>{response.value}.subspan(1));
            if (!PlacementAuthority::matches(control.encode(), controlConfig(control.nodes()).encode())) { throw std::runtime_error("Request authority: foreign topology response"); }
            raft_->setRequestForwardTargets(control.nodes());
        }
    }
    void reconfigure(const std::vector<NodeInfo>& nodes) {
        validateTarget(nodes);
        ForwardRequest request; request.operation = ForwardOperation::CLUSTER_ADMIN;
        request.timeoutMs = detail::ReconfigurationDeadline::remaining(runtimeOptions_.reconfiguration.timeoutMs);
        request.entries.push_back({{'A','K','R','C','1'}, controlConfig(nodes).encode()});
        uint64_t target = hosted_ ? raft_->leaderNodeId() : preferredLeader_.load();
        if (!target) { target = raft_->activeNodes().front().nodeId; }
        for (size_t hop = 0; hop < 512; ++hop) {
            detail::ReconfigurationDeadline::check();
            request.timeoutMs = detail::ReconfigurationDeadline::remaining(runtimeOptions_.reconfiguration.timeoutMs);
            try {
                auto response = hosted_ && target == self_ ? receive(request) : raft_->forwardTo(target, request);
                if (response.success) { learnTopology(response); preferredLeader_.store(target); return; }
                throw ClusterRoutingError(response.errorCode, response.target, response.message);
            } catch (const ClusterRoutingError& error) {
                if (error.code != ClusterRoutingCode::NOT_OWNER || !error.target.nodeId) { throw; }
                target = error.target.nodeId;
            }
        }
        throw std::runtime_error("Request authority: reconfiguration leadership did not stabilize");
    }
    void validateTarget(const std::vector<NodeInfo>& nodes) const {
        std::lock_guard lock{topologyMu_};
        auto validation = runtimeOptions_; validation.requests.authorityNodes = nodes; dataConfig_.validateRuntime(self_, validation);
    }
    void prepareDataNodes(const ClusterConfig& target) {
        auto clients = std::vector<uint64_t>{};
        {
            std::lock_guard lock{topologyMu_};
            for (const auto& node : dataConfig_.nodes()) { clients.push_back(node.nodeId); }
            for (const auto& node : target.nodes()) { clients.push_back(node.nodeId); }
            dataConfig_ = target;
        }
        raft_->setRequestForwardClients(std::move(clients));
    }
    void activateDataNodes(const ClusterConfig& target) {
        auto clients = std::vector<uint64_t>{};
        for (const auto& node : target.nodes()) { clients.push_back(node.nodeId); }
        { std::lock_guard lock{topologyMu_}; dataConfig_ = target; }
        raft_->setRequestForwardClients(std::move(clients));
    }
    void validateDataPlacement(const ClusterConfig& target) {
        ClusterRequestId probe; crypto::secureRandom(probe.nonce); probe.expiresAtUnixMs = nowMs() + options_.defaultRetentionMs;
        (void)rpc(2, probe, {});
        auto validation = runtimeOptions_; validation.requests.authorityNodes = raft_->activeNodes();
        // Validate the future runtime from a retained authority host when the
        // current placement administrator is itself being retired.
        const auto validator = target.findById(self_) || validation.requests.authorityNodes.empty()
            ? self_ : validation.requests.authorityNodes.front().nodeId;
        target.validateRuntime(validator, validation);
    }
    void start() {
        if (receipts_ && receipts_->isClosed()) {
            receipts_ = std::make_shared<detail::DiskResults>(path_.parent_path() / "receipts", options_.maxRetainedResultBytes); rebuildPending();
        }
        raft_->start();
    }
    void close() { raft_->close(); if (receipts_) { receipts_->close(); } }
    ClusterRequestResult query(const ClusterRequestId& id) { return rpc(2, id, {}).requestResult; }
    ClusterRequestResult execute(const ClusterRequestId& id, const std::array<uint8_t, 32>& fingerprint, const std::function<uint64_t()>& write) {
        Receipt receipt; receipt.fingerprint = fingerprint; crypto::secureRandom(receipt.attempt);
        auto granted = false;
        try {
            const auto claimed = rpc(0, id, receipt);
            if (claimed.value.empty() || claimed.value.front() != 1) { return claimed.requestResult; }
            granted = true;
            receipt.sequence = write();
            return rpc(1, id, receipt).requestResult;
        } catch (const std::invalid_argument& error) {
            if (!granted) { throw; }
            throw ClusterRoutingError(ClusterRoutingCode::OUTCOME_UNKNOWN, {}, error.what(), id);
        }
        catch (ClusterRoutingError& error) { error.requestId = id; throw; }
        catch (const std::exception& error) {
            throw ClusterRoutingError(ClusterRoutingCode::OUTCOME_UNKNOWN, {}, error.what(), id);
        }
    }
    void observe(RaftRuntimeStats& out) {
        out.requestCapacity = options_.maxPendingRequests; out.requestResultByteLimit = options_.maxRetainedResultBytes;
        const auto members = raft_->activeNodes();
        if (!hosted_ || !std::ranges::contains(members, self_, &NodeInfo::nodeId)) {
            out.quorumAvailability = detail::combineAvailability(out.quorumAvailability, ClusterOperationAvailability::UNKNOWN);
            out.writeAvailability = detail::combineAvailability(out.writeAvailability, ClusterOperationAvailability::UNKNOWN);
            return;
        }
        detail::mergeHealthObservation(out, raft_->stats());
        std::lock_guard lock{mutex_};
        if (receipts_) { out.pendingRequests += pending_.size(); out.retainedRequestResults += receipts_->count() - pending_.size(); out.retainedRequestResultBytes += receipts_->bytes(); }
    }
};

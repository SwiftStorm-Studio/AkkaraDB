// Included in ClusterRuntime.cpp's anonymous namespace.
class PlacementAuthority {
    struct State {
        uint64_t applied = 0;
        uint64_t generation = 1;
        uint64_t pending = 0;
        uint64_t lastGeneration = 1;
        bool frozen = false, activating = false, cancelOnInterruption = false;
        std::vector<uint8_t> activeImage;
        std::vector<uint8_t> pendingImage;
    };
    std::mutex stateMu_;
    std::timed_mutex proposalMu_;
    State state_;
    std::filesystem::path path_;
    uint64_t self_;
    ClusterEngineCallbacks callbacks_;
    std::unique_ptr<RaftConsensusRuntime> raft_;
    detail::AuthoritySnapshotFile snapshotFile_;
    bool stripe_;
    ClusterReconfigurationOptions options_;

    void installSnapshot(uint64_t sequence) {
        uint64_t after;
        { std::lock_guard lock{stateMu_}; after = state_.applied; }
        if (after >= sequence) {
            std::error_code ignored;
            std::filesystem::remove(snapshotFile_.path(), ignored);
            return;
        }
        std::optional<State> restored;
        std::vector<uint8_t> stateBytes;
        bool stateEntry = false;
        uint64_t ordinal = 0;
        // Validate the entire durable spool before changing the state machine.
        SnapshotEntryVisitor validate;
        validate.beginEntry = [&](auto key, uint64_t size, uint32_t) {
            const auto record = detail::decodeSnapshotKey(key);
            stateEntry = record.kind == detail::SnapshotRecordKind::STATE;
            if (stateEntry != (ordinal++ == 0) || (stateEntry && (!record.key.empty() || size > 4 * 1024 * 1024))) {
                throw std::runtime_error("Cluster placement: invalid snapshot state entry");
            }
            if (stateEntry) { stateBytes.clear(); }
            return true;
        };
        validate.appendValueChunk = [&](uint64_t, auto chunk) {
            if (stateEntry) { stateBytes.insert(stateBytes.end(), chunk.begin(), chunk.end()); }
            return true;
        };
        validate.finishEntry = [&] { if (stateEntry) { restored = decodeState(stateBytes); } return true; };
        if (!snapshotFile_.replay(sequence, validate) || !restored || restored->applied != sequence) {
            throw std::runtime_error("Cluster placement: incomplete authority snapshot");
        }
        ClusterConfig::decode(state_.activeImage).validatePlacementChange(ClusterConfig::decode(restored->activeImage));
        if (stripe_) {
            if (!callbacks_.installStripeMetadataSnapshot) { throw std::logic_error("Cluster placement: metadata snapshot callback unavailable"); }
            ClusterSnapshot metadata;
            metadata.seq = sequence;
            metadata.forEachEntry = [&](const SnapshotEntryVisitor& visitor) {
                bool skip = false;
                SnapshotEntryVisitor filter;
                filter.beginEntry = [&](auto key, uint64_t size, uint32_t crc) {
                    skip = detail::decodeSnapshotKey(key).kind == detail::SnapshotRecordKind::STATE;
                    return skip || visitor.beginEntry(key, size, crc);
                };
                filter.appendValueChunk = [&](uint64_t offset, auto bytes) { return skip || visitor.appendValueChunk(offset, bytes); };
                filter.finishEntry = [&] { return skip || visitor.finishEntry(); };
                return snapshotFile_.replay(sequence, filter);
            };
            callbacks_.installStripeMetadataSnapshot(metadata, after);
        }
        persist(*restored);
        { std::lock_guard lock{stateMu_}; state_ = *restored; }
        std::error_code ignored;
        std::filesystem::remove(snapshotFile_.path(), ignored);
        if (callbacks_.placementChanged) { callbacks_.placementChanged(ClusterConfig::decode(restored->activeImage), restored->generation); }
    }

    static bool sameConfig(std::vector<uint8_t> left, std::vector<uint8_t> right) {
        // Wall-clock save time and file CRC do not identify a placement plan.
        if (left.size() < 28 || right.size() < 28) { return false; }
        std::fill(left.begin() + 16, left.begin() + 28, 0);
        std::fill(right.begin() + 16, right.begin() + 28, 0);
        return left == right;
    }
    static std::vector<uint8_t> encodeState(const State& state) {
        std::vector<uint8_t> bytes{'A', 'K', 'P', 'C', '2'};
        writeLe64(bytes, state.applied); writeLe64(bytes, state.generation); writeLe64(bytes, state.pending);
        writeLe64(bytes, state.lastGeneration);
        bytes.push_back(static_cast<uint8_t>((state.frozen ? 1 : 0) | (state.activating ? 2 : 0) | (state.cancelOnInterruption ? 4 : 0)));
        writeLe32(bytes, static_cast<uint32_t>(state.activeImage.size()));
        bytes.insert(bytes.end(), state.activeImage.begin(), state.activeImage.end());
        writeLe32(bytes, static_cast<uint32_t>(state.pendingImage.size()));
        bytes.insert(bytes.end(), state.pendingImage.begin(), state.pendingImage.end());
        writeLe32(bytes, cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size()));
        return bytes;
    }
    static State decodeState(std::span<const uint8_t> bytes) {
        if (bytes.size() < 50 || !std::equal(bytes.begin(), bytes.begin() + 5, "AKPC2") ||
            readLe32(bytes, bytes.size() - 4) != cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size() - 4)) {
            throw std::runtime_error("Cluster placement: invalid authority state");
        }
        State state;
        state.applied = readLe64(bytes, 5); state.generation = readLe64(bytes, 13); state.pending = readLe64(bytes, 21);
        state.lastGeneration = readLe64(bytes, 29);
        if (bytes[37] > 7) { throw std::runtime_error("Cluster placement: malformed phase"); }
        state.frozen = (bytes[37] & 1) != 0; state.activating = (bytes[37] & 2) != 0;
        state.cancelOnInterruption = (bytes[37] & 4) != 0;
        const auto activeSize = readLe32(bytes, 38);
        size_t cursor = 42;
        if (activeSize > bytes.size() - cursor - 8) { throw std::runtime_error("Cluster placement: truncated active config"); }
        state.activeImage.assign(bytes.begin() + cursor, bytes.begin() + cursor + activeSize); cursor += activeSize;
        const auto pendingSize = readLe32(bytes, cursor); cursor += 4;
        if (pendingSize != bytes.size() - cursor - 4 || state.generation == 0 ||
            (state.pending == 0) != (pendingSize == 0) || state.lastGeneration < state.generation ||
            (state.pending != 0 && (state.pending <= state.generation || state.pending != state.lastGeneration)) ||
            (state.pending == 0 && (state.frozen || state.activating || state.cancelOnInterruption)) ||
            (state.activating && !state.frozen)) {
            throw std::runtime_error("Cluster placement: malformed pending config");
        }
        state.pendingImage.assign(bytes.begin() + cursor, bytes.begin() + cursor + pendingSize);
        (void)ClusterConfig::decode(state.activeImage);
        if (state.pending != 0) { (void)ClusterConfig::decode(state.pendingImage); }
        return state;
    }
    void persist(const State& state) {
        const auto bytes = encodeState(state);
        std::filesystem::create_directories(path_.parent_path());
        const auto temporary = makeTempPath(path_, "Cluster placement");
        {
            std::ofstream out(temporary, std::ios::binary | std::ios::trunc);
            out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
            out.flush();
            if (!out) { throw std::runtime_error("Cluster placement: state write failed"); }
        }
        syncFile(temporary, "Cluster placement");
        replaceFileAtomically(temporary, path_, "Cluster placement");
    }
    void apply(uint64_t sequence, std::span<const uint8_t> command) {
        if (command.size() < 9 || command[0] > 5) { throw std::runtime_error("Cluster placement: malformed command"); }
        const auto expected = readLe64(command, 1);
        std::optional<ClusterConfig> changed;
        uint64_t generation = 0;
        {
            std::lock_guard lock{stateMu_};
            if (sequence <= state_.applied) { return; }
            auto next = state_;
            if (command[0] == 2) {
                if (next.applied != 0 || next.generation != 1 || next.pending != 0 || expected != 1) {
                    throw std::runtime_error("Cluster placement: invalid genesis placement");
                }
                const auto genesis = ClusterConfig::decode(command.subspan(9));
                ClusterConfig::decode(next.activeImage).validatePlacementChange(genesis);
                next.activeImage = genesis.encode();
                changed = ClusterConfig::decode(next.activeImage);
            }
            else if (command[0] == 0) {
                if (command.size() < 10 || command[9] > 3) { throw std::runtime_error("Cluster placement: malformed intent"); }
                auto target = ClusterConfig::decode(command.subspan(10));
                ClusterConfig::decode(next.activeImage).validatePlacementChange(target);
                if (next.generation == expected && next.pending == 0) {
                    if (next.lastGeneration == UINT64_MAX) { throw std::overflow_error("Cluster placement: generation exhausted"); }
                    next.pending = ++next.lastGeneration;
                    next.frozen = (command[9] & 1) != 0;
                    next.cancelOnInterruption = (command[9] & 2) != 0;
                    next.pendingImage = target.encode();
                }
            }
            else {
                if (command.size() != 9) { throw std::runtime_error("Cluster placement: malformed completion"); }
                if (next.pending == expected) {
                    if (command[0] == 3) { next.frozen = true; }
                    else if (command[0] == 4) {
                        if (!next.frozen) { throw std::runtime_error("Cluster placement: activation requires frozen writes"); }
                        next.activating = true;
                    }
                    else if (command[0] == 1 || (command[0] == 5 && !next.activating)) {
                        if (command[0] == 1) {
                            if (!next.activating) { throw std::runtime_error("Cluster placement: activation was not prepared"); }
                            next.activeImage = std::move(next.pendingImage);
                            next.generation = next.pending;
                            changed = ClusterConfig::decode(next.activeImage);
                        }
                        next.pendingImage.clear(); next.pending = 0;
                        next.frozen = next.activating = next.cancelOnInterruption = false;
                    }
                }
            }
            next.applied = sequence;
            persist(next);
            state_ = std::move(next);
            generation = state_.generation;
        }
        if (changed && callbacks_.placementChanged) { callbacks_.placementChanged(*changed, generation); }
    }
    std::vector<uint8_t> wireState() {
        std::lock_guard lock{stateMu_};
        return encodeState(state_);
    }
    static ClusterPlacementState view(const State& state, uint64_t leader) {
        ClusterPlacementState result;
        result.leaderNodeId = leader;
        result.generation = state.generation;
        result.pendingGeneration = state.pending;
        result.lastIssuedGeneration = state.lastGeneration;
        result.stripeWritesBlocked = state.frozen;
        result.activationStarted = state.activating;
        result.cancelOnInterruption = state.cancelOnInterruption;
        result.activeConfig = state.activeImage;
        result.pendingConfig = state.pendingImage;
        return result;
    }
    static ClusterConfig authorityConfig(const ClusterConfig& config) {
        if (config.mode() == ReplicationMode::STRIPE) { return stripeMetadataRaftConfig(config); }
        auto nodes = config.dataNodes();
        for (auto& node : nodes) {
            node.replPort = static_cast<uint16_t>(node.partitionReplBasePort + config.partition().partitionCount);
            node.stripeMetadataPort = 0; node.partitionReplBasePort = 0;
        }
        return ClusterConfig{std::move(nodes), ReplicationMode::MIRROR, {},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = config.consistency().ackTimeoutMs},
            config.raft(), {}, 0, config.clusterId()};
    }
    void appliedMetadata(uint64_t sequence, const std::function<void()>& apply) {
        {
            std::lock_guard lock{stateMu_};
            if (sequence <= state_.applied) { return; }
        }
        // Raft serializes application. Storage I/O must not hold the placement
        // view lock, which independent keys need while publication is pending.
        apply();
        std::lock_guard lock{stateMu_};
        auto next = state_; next.applied = sequence; persist(next); state_ = std::move(next);
    }
    void commitGenesis(const std::vector<uint8_t>& active) {
        bool needsGenesis;
        { std::lock_guard lock{stateMu_}; needsGenesis = state_.applied == 0; }
        if (!needsGenesis) { return; }
        std::vector<uint8_t> command{2}; writeLe64(command, 1);
        command.insert(command.end(), active.begin(), active.end());
        auto proposal = raft_->submitMutation([&](uint64_t) {
            ClusterMutation mutation; mutation.key = {0}; mutation.value = command; return mutation;
        });
        detail::ReconfigurationDeadline::wait(proposal.completion);
    }
    void requireLeader() {
        if (raft_->role() == NodeRole::PRIMARY) { return; }
        ClusterRouteTarget target;
        target.nodeId = raft_->stats().leaderNodeId;
        throw ClusterRoutingError(ClusterRoutingCode::NOT_OWNER, target, "Cluster placement: call the authority leader");
    }
public:
    PlacementAuthority(const std::filesystem::path& dir, const ClusterConfig& config, uint64_t self,
        ClusterRuntimeOptions options, ClusterEngineCallbacks callbacks)
        : path_{dir / "placement-authority" / "state.akpc"}, self_{self}, callbacks_{std::move(callbacks)},
          snapshotFile_{path_.parent_path() / "state.snapshot"}, stripe_{config.mode() == ReplicationMode::STRIPE} {
        state_.activeImage = config.encode();
        if (std::filesystem::exists(path_)) {
            std::ifstream in(path_, std::ios::binary);
            const std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
            state_ = decodeState(bytes);
            config.validatePlacementChange(ClusterConfig::decode(state_.activeImage));
            if (ClusterConfig::decode(state_.activeImage).clusterId() != config.clusterId()) {
                throw std::runtime_error("Cluster placement: foreign persisted cluster");
            }
        }
        ClusterEngineCallbacks control;
        control.getCurrentSeq = control.getLastSeq = [this] { std::lock_guard lock{stateMu_}; return state_.applied; };
        control.apply = [this](uint64_t seq, ReplOpType op, auto key, auto value, uint8_t flags, uint64_t, uint64_t) {
            if (op == ReplOpType::PUT && !key.empty() && (key[0] == 'M' || key[0] == 'H') && callbacks_.commitStripeMetadata) {
                if ((key[0] == 'M' && flags > 1) || (key[0] == 'H' && flags != 1)) {
                    throw std::runtime_error("Cluster placement: malformed metadata flags");
                }
                appliedMetadata(seq, [&] {
                    callbacks_.commitStripeMetadata(key[0] == 'H' ? UINT64_MAX - 1 : flags == 1 ? 0 : seq, key.subspan(1), value);
                });
                return;
            }
            if (op != ReplOpType::PUT || key.size() != 1 || key[0] != 0 || flags != 0) {
                throw std::runtime_error("Cluster placement: invalid authority mutation");
            }
            apply(seq, value);
        };
        control.forceDurable = [] {};
        control.exportSnapshot = [this]() -> std::optional<ClusterSnapshot> {
            State state;
            { std::lock_guard lock{stateMu_}; state = state_; }
            auto metadata = stripe_ && callbacks_.exportStripeMetadataSnapshot
                ? callbacks_.exportStripeMetadataSnapshot(state.applied) : std::optional<ClusterSnapshot>{};
            if (stripe_ && !metadata) { return std::nullopt; }
            const auto image = encodeState(state);
            ClusterSnapshot snapshot;
            snapshot.seq = state.applied;
            snapshot.forEachEntry = [image, metadata](const SnapshotEntryVisitor& visitor) {
                const auto key = detail::encodeSnapshotKey({detail::SnapshotRecordKind::STATE, 0, 0, 0, 0, {}});
                const auto crc = cpu::CRC32C(reinterpret_cast<const std::byte*>(image.data()), image.size());
                if (!visitor.beginEntry(key, image.size(), crc) || !visitor.appendValueChunk(0, image) || !visitor.finishEntry()) { return false; }
                return !metadata || metadata->forEachEntry(visitor);
            };
            return snapshot;
        };
        control.beginSnapshot = [this](uint64_t seq, uint64_t) { snapshotFile_.begin(seq); };
        control.beginSnapshotEntry = [this](auto key, uint64_t size, uint32_t crc) { snapshotFile_.beginEntry(key, size, crc); };
        control.appendSnapshotEntryChunk = [this](uint64_t offset, auto bytes) { snapshotFile_.append(offset, bytes); };
        control.finishSnapshotEntry = [this] { snapshotFile_.finishEntry(); };
        control.prepareSnapshot = [this](uint64_t seq, uint64_t count) {
            snapshotFile_.prepare(seq, count); syncFile(snapshotFile_.path(), "Cluster placement snapshot");
#ifndef _WIN32
            syncParentDirectory(snapshotFile_.path(), "Cluster placement snapshot");
#endif
        };
        control.finishSnapshot = [this](uint64_t seq, uint64_t) { installSnapshot(seq); };
        control.recoverSnapshot = [this](uint64_t seq) { installSnapshot(seq); };
        control.isSnapshotDurable = [this](uint64_t seq) { std::lock_guard lock{stateMu_}; return state_.applied >= seq; };
        control.forward = [this](const ForwardRequest& request) {
            if (request.operation == ForwardOperation::CLUSTER_ADMIN && request.entries.size() == 1 &&
                request.entries[0].key.empty() && request.entries[0].value.empty()) {
                detail::ReconfigurationDeadline budget{request.timeoutMs};
                requireLeader(); raft_->linearizableReadBarrier();
                ForwardResponse result;
                result.success = true; result.value = wireState(); return result;
            }
            if (!callbacks_.forward) { throw std::logic_error("Cluster placement: forwarding callback unavailable"); }
            return callbacks_.forward(request);
        };
        options_ = options.reconfiguration;
        options.reconfiguration.stripeMigrationMode = StripeMigrationMode::FREEZE;
        options.reconfiguration.stripeInterruptionPolicy = StripeInterruptionPolicy::RETAIN;
        options.requests.enabled = false;
        options.raftBlobPolicy = RaftBlobPolicy::REJECT;
        options.clusterMembershipPath.clear();
        options.resetClusterMembership = false;
        options.primaryNodeId = 0; options.primaryHost.clear(); options.primaryReplPort = 0;
        options.mirrorFencing = {}; options.mirrorPromotion = {};
        options.mirrorRecovery = {};
        options.secure.expectedPrimaryNodeId = 0;
        options.transfer.spoolDirectory = path_.parent_path() / "transfer-spool";
        const auto controlConfig = authorityConfig(config);
        raft_ = RaftConsensusRuntime::create(path_.parent_path(), controlConfig, self_, std::move(control), options);
        if (callbacks_.placementChanged) { callbacks_.placementChanged(ClusterConfig::decode(state_.activeImage), state_.generation); }
    }
    void ensureGenesis() {
        auto proposal = detail::ReconfigurationDeadline::lock(proposalMu_); requireLeader();
        commitGenesis(state(true).activeConfig);
    }
    static bool matches(std::vector<uint8_t> left, std::vector<uint8_t> right) { return sameConfig(std::move(left), std::move(right)); }
    RaftConsensusRuntime& consensus() { return *raft_; }
    ForwardResponse forwardTo(uint64_t node, ForwardRequest request) { return raft_->forwardTo(node, std::move(request)); }
    std::vector<NodeInfo> activeNodes() const { return raft_->activeNodes(); }
    void start() { raft_->start(); }
    void close() { raft_->close(); }
    uint64_t generation() { std::lock_guard lock{stateMu_}; return state_.generation; }
    ClusterPlacementState state(bool fresh) {
        const auto leader = raft_->stats().leaderNodeId;
        if (fresh) {
            if (leader == self_) { raft_->linearizableReadBarrier(); }
            else {
                if (leader == 0) { throw std::runtime_error("Cluster placement: authority unavailable"); }
                ForwardRequest request;
                request.operation = ForwardOperation::CLUSTER_ADMIN;
                request.timeoutMs = detail::ReconfigurationDeadline::remaining(5000);
                request.entries.emplace_back();
                const auto response = raft_->forwardTo(leader, std::move(request));
                if (!response.success) { throw ClusterRoutingError(response.errorCode, response.target, response.message); }
                return view(decodeState(response.value), leader);
            }
        }
        std::lock_guard lock{stateMu_};
        return view(state_, leader);
    }
    uint64_t begin(const ClusterConfig& target) {
        auto proposal = detail::ReconfigurationDeadline::lock(proposalMu_);
        requireLeader();
        auto current = state(true);
        const auto image = target.encode();
        ClusterConfig::decode(current.activeConfig).validatePlacementChange(target);
        if (current.pendingGeneration != 0) {
            if (!sameConfig(current.pendingConfig, image)) { throw std::runtime_error("Cluster placement: another migration is pending"); }
            return current.pendingGeneration;
        }
        if (sameConfig(current.activeConfig, image)) { return current.generation; }
        commitGenesis(current.activeConfig);
        const auto members = raft_->activeNodes();
        for (auto node : authorityConfig(target).dataNodes()) {
            if (std::ranges::any_of(members, [&](const auto& member) { return member.nodeId == node.nodeId; })) { continue; }
            node.capabilities |= RAFT_LEARNER;
            raft_->addLearner(node);
        }
        std::vector<uint8_t> command{0}; writeLe64(command, current.generation);
        command.push_back(static_cast<uint8_t>((!stripe_ || options_.stripeMigrationMode == StripeMigrationMode::FREEZE ? 1 : 0) |
            (stripe_ && options_.stripeInterruptionPolicy == StripeInterruptionPolicy::CANCEL ? 2 : 0)));
        command.insert(command.end(), image.begin(), image.end());
        auto submission = raft_->submitMutation([&](uint64_t) {
            ClusterMutation mutation; mutation.key = {0}; mutation.value = command; return mutation;
        });
        detail::ReconfigurationDeadline::wait(submission.completion);
        current = state(true);
        if (!sameConfig(current.pendingConfig, image)) { throw std::runtime_error("Cluster placement: intent was superseded"); }
        return current.pendingGeneration;
    }
    void commitPhase(uint8_t action, uint64_t generation) {
        std::vector<uint8_t> command{action}; writeLe64(command, generation);
        auto submission = raft_->submitMutation([command = std::move(command)](uint64_t) {
            ClusterMutation mutation; mutation.key = {0}; mutation.value = command; return mutation;
        });
        detail::ReconfigurationDeadline::wait(submission.completion);
    }
    void freeze(uint64_t generation) {
        auto proposal = detail::ReconfigurationDeadline::lock(proposalMu_);
        requireLeader(); const auto current = state(true);
        if (current.pendingGeneration != generation) { throw std::runtime_error("Cluster placement: stale freeze"); }
        if (!current.stripeWritesBlocked) { commitPhase(3, generation); }
    }
    void cancel(uint64_t generation) {
        auto proposal = detail::ReconfigurationDeadline::lock(proposalMu_);
        requireLeader(); const auto current = state(true);
        if (current.pendingGeneration == 0) { return; }
        if (!stripe_ || current.pendingGeneration != generation || current.activationStarted) {
            throw std::runtime_error("Cluster placement: cancellation unavailable after authority activation starts; resume this plan");
        }
        // Keep preparatory learners reusable. Removing a learner also revokes
        // its bootstrap reception; a later plan must still be able to admit it.
        // The next successful authority membership transition drops unused ones.
        commitPhase(5, generation);
        if (state(true).pendingGeneration != 0) { throw std::runtime_error("Cluster placement: cancellation was superseded"); }
    }
    void finish(uint64_t generation) {
        auto proposal = detail::ReconfigurationDeadline::lock(proposalMu_);
        requireLeader();
        auto current = state(true);
        if (current.pendingGeneration == 0 && current.generation == generation) { return; }
        if (current.pendingGeneration != generation) { throw std::runtime_error("Cluster placement: stale migration completion"); }
        if (!current.activationStarted) { commitPhase(4, generation); }
        // Keep the intent pending across authority-leader retirement. Its
        // successor resumes the migration and commits the activation.
        raft_->reconfigure(authorityConfig(ClusterConfig::decode(current.pendingConfig)));
        requireLeader();
        std::vector<uint8_t> command{1}; writeLe64(command, generation);
        auto submission = raft_->submitMutation([&](uint64_t) {
            ClusterMutation mutation; mutation.key = {0}; mutation.value = command; return mutation;
        });
        detail::ReconfigurationDeadline::wait(submission.completion);
        current = state(true);
        if (current.pendingGeneration != 0 || current.generation != generation) {
            throw std::runtime_error("Cluster placement: completion was superseded");
        }
    }
};

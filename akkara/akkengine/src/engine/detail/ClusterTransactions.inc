/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
// Included inside AkkEngine::Impl. Decisions use the placement Raft group;
// reservations and prepared values use each participant's data Raft group.
static bool distributedCoordinatorAction(DistributedAction action) {
    return action <= DistributedAction::FINISH;
}
DistributedRecord localDistributedRecord(const DistributedId& id) {
    std::lock_guard lock{distributedStateMu};
    const auto it = distributedState.decisions.find(id);
    if (it == distributedState.decisions.end()) {
        if (const auto archived = distributedResults(true)->get(id)) { return decodeDistributedRecord(archived->value); }
        DistributedRecord missing; missing.status = DistributedStatus::UNKNOWN; return missing;
    }
    return it->second;
}
static std::vector<uint8_t> encodeDistributedRecord(const DistributedRecord& record) {
    std::vector<uint8_t> out{static_cast<uint8_t>(record.status), static_cast<uint8_t>((record.finished ? 1 : 0) | (record.ready ? 2 : 0))};
    cluster::detail::transactionInteger(out, record.expires);
    cluster::detail::transactionInteger(out, record.participants.size(), 4);
    for (auto index : record.participants) { cluster::detail::transactionInteger(out, index, 2); }
    return out;
}
static DistributedRecord decodeDistributedRecord(std::span<const uint8_t> bytes) {
    cluster::detail::TransactionReader reader{bytes}; DistributedRecord record;
    const auto status = reader.integer(1), flags = reader.integer(1);
    if ((status != 0 && status < static_cast<uint8_t>(DistributedStatus::OPEN)) || status > static_cast<uint8_t>(DistributedStatus::ABORTED) || flags > 3) { throw std::runtime_error("Cluster transaction: invalid status"); }
    record.status = static_cast<DistributedStatus>(status); record.finished = (flags & 1) != 0; record.ready = (flags & 2) != 0;
    record.expires = reader.integer(); const auto count = reader.count(2);
    for (uint32_t i = 0; i < count; ++i) { record.participants.push_back(static_cast<uint16_t>(reader.integer(2))); }
    reader.finish();
    if (count > 256 || !std::ranges::is_sorted(record.participants) || std::ranges::adjacent_find(record.participants) != record.participants.end() ||
        (record.status == DistributedStatus::UNKNOWN ? record.finished || record.ready || record.expires || count : !record.expires) ||
        (record.finished && record.status != DistributedStatus::COMMITTED && record.status != DistributedStatus::ABORTED)) {
        throw std::runtime_error("Cluster transaction: malformed decision result");
    }
    return record;
}
cluster::ForwardResponse distributedRpc(const DistributedMessage& message) {
    const bool coordinator = distributedCoordinatorAction(message.action);
    const auto placement = livePlacement.load();
    auto candidates = usesPartitionRaft() && !coordinator
        ? partitionConfig(*placement, message.participant).nodes() : clusterRuntime->activeNodes();
    uint64_t preferred = coordinator ? clusterRuntime->transactionLeaderNodeId() : 0;
    std::exception_ptr last;
    const auto deadline = cluster::detail::ReconfigurationDeadline::current().deadline;
    do {
        if (preferred) {
            const auto it = std::ranges::find(candidates, preferred, &cluster::NodeInfo::nodeId);
            if (it != candidates.end()) { std::rotate(candidates.begin(), it, it + 1); }
        }
        for (const auto& node : candidates) {
            cluster::detail::ReconfigurationDeadline::check();
            cluster::ForwardRequest request; request.operation = cluster::ForwardOperation::TRANSACTION;
            request.timeoutMs = cluster::detail::ReconfigurationDeadline::remaining(opts.cluster.runtime.transactions.timeoutMs);
            request.entries.push_back({{}, cluster::detail::encodeTransactionMessage(message)});
            try {
                auto response = node.nodeId == nodeId ? receiveClusterTransaction(request) : clusterRuntime->forwardTo(node.nodeId, std::move(request));
                if (!response.success) { throw cluster::ClusterRoutingError(response.errorCode, response.target, response.message); }
                return response;
            } catch (const TransactionConflict&) { throw; }
            catch (const cluster::ClusterRoutingError& error) {
                if (error.code == cluster::ClusterRoutingCode::TRANSACTION_CONFLICT) { throw TransactionConflict(error.what()); }
                preferred = error.target.nodeId; last = std::current_exception();
            } catch (...) { last = std::current_exception(); }
        }
        if (placementClosing()) { throw std::runtime_error("Cluster transaction: engine is closing"); }
        std::this_thread::sleep_for(std::chrono::milliseconds{20});
    } while (std::chrono::steady_clock::now() < deadline);
    if (last) { std::rethrow_exception(last); }
    throw std::runtime_error("Cluster transaction: leader unavailable");
}
DistributedRecord distributedRecord(const DistributedId& id) {
    return decodeDistributedRecord(distributedRpc(DistributedMessage{.action = DistributedAction::STATUS, .id = id}).value);
}
void proposeDistributed(const DistributedMessage& message, bool coordinator, uint64_t expectedTerm) {
    if (expectedTerm == 0) { throw std::logic_error("Cluster transaction: missing validation term"); }
    const auto bytes = cluster::detail::encodeTransactionMessage(message);
    auto factory = [bytes](uint64_t) {
        cluster::ClusterMutation mutation; mutation.op = cluster::ReplOpType::TRANSACTION; mutation.recordFlags = 0; mutation.value = bytes; return mutation;
    };
    auto proposal = coordinator ? clusterRuntime->submitTransactionControl(factory, expectedTerm) : clusterRuntime->submitMutation(factory, expectedTerm);
    cluster::detail::ReconfigurationDeadline::wait(proposal.completion);
}
uint64_t requireDistributedLeader() {
    const auto leader = clusterRuntime->transactionLeaderNodeId();
    if (!leader || leader != nodeId) {
        cluster::ClusterRouteTarget target; target.nodeId = leader;
        throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::NOT_OWNER, target, "Cluster transaction: call the leader");
    }
    return clusterRuntime->transactionReadBarrier();
}
cluster::ForwardResponse receiveDistributedCoordinator(DistributedMessage message) {
    requireDistributedLeader();
    cluster::ForwardResponse response; response.success = true;
    if (message.action == DistributedAction::STATUS) { response.value = encodeDistributedRecord(localDistributedRecord(message.id)); return response; }
    auto actor = cluster::detail::ReconfigurationDeadline::lock(distributedCoordinatorMu);
    // MIRROR stores coordinator and participant records in the same state image.
    // Admission must include the participant's outstanding prepare reservation.
    std::unique_lock<std::timed_mutex> storageAdmission{distributedProposalMu, std::defer_lock};
    if (!usesPartitionRaft()) { storageAdmission = cluster::detail::ReconfigurationDeadline::lock(distributedProposalMu); }
    auto validationTerm = requireDistributedLeader();
    auto record = localDistributedRecord(message.id);
    const auto reject = [] { throw TransactionConflict("Cluster transaction: incompatible decision or capacity exceeded; retry transaction"); };
    if (message.action == DistributedAction::CREATE) {
        if (record.status != DistributedStatus::UNKNOWN) {
            response.value = encodeDistributedRecord(record); return response;
        }
        const auto now = distributedNowMs();
        if (!distributedIdPresent(message.id) || !message.argument || message.argument > opts.transactions.maxLifetimeMs) { reject(); }
        message.expires = now + message.argument;
        if (usesPartitionRaft() && clusterRuntime->placementState(true).pendingGeneration) { reject(); }
        {
            std::lock_guard lock{distributedStateMu};
            if (std::ranges::count_if(distributedState.decisions, [](const auto& item) { return !item.second.finished; }) >= opts.transactions.maxOpen) { reject(); }
            const auto reservation = (distributedState.decisions.size() + 1) * cluster::detail::DiskResults::recordCharge(16, 14 + 2 * 256);
            if (!distributedResults(true)->canReserve(reservation, now)) { reject(); }
        }
        if (!message.payload.empty()) { reject(); }
        message.argument = now;
        {
            std::lock_guard lock{distributedStateMu}; auto retained = distributedState;
            const auto size = distributedReservedBytes(retained);
            const uint64_t growth = 30;
            if (size > opts.transactions.maxJournalBytes || growth > opts.transactions.maxJournalBytes - size) { reject(); }
        }
    } else {
        if (record.status == DistributedStatus::UNKNOWN) { reject(); }
        const auto phase = record.status;
        bool done = false;
        switch (message.action) {
            case DistributedAction::ENLIST: {
                const auto count = usesPartitionRaft() ? clusterConfig.partition().partitionCount : 1;
                if (phase != DistributedStatus::OPEN || record.finished || record.expires <= distributedNowMs() || message.participant >= count) { reject(); }
                if (!std::ranges::contains(record.participants, message.participant)) {
                    {
                        std::lock_guard lock{distributedStateMu};
                        const auto size = distributedReservedBytes(distributedState);
                        if (size > opts.transactions.maxJournalBytes || 2 > opts.transactions.maxJournalBytes - size) { reject(); }
                    }
                    proposeDistributed(message, true, validationTerm);
                }
                // Record the claim before FREEZE. Abort/recovery now knows every
                // possibly reserved participant, including a lost freeze reply.
                if (storageAdmission.owns_lock()) { storageAdmission.unlock(); }
                actor.unlock();
                (void)distributedRpc({.action = DistributedAction::FREEZE, .id = message.id, .participant = message.participant});
                record = distributedRecord(message.id);
                if (record.status != DistributedStatus::OPEN || record.finished) { reject(); }
                response.value = encodeDistributedRecord(record); return response;
            }
            case DistributedAction::START_PREPARE: done = phase == DistributedStatus::PREPARING; if (!done && phase != DistributedStatus::OPEN) { reject(); } break;
            case DistributedAction::DECIDE_COMMIT:
                done = phase == DistributedStatus::COMMITTED;
                if (!done && phase != DistributedStatus::PREPARING) { reject(); }
                break;
            case DistributedAction::DECIDE_ABORT: done = phase == DistributedStatus::ABORTED; if (phase == DistributedStatus::COMMITTED) { reject(); } break;
            case DistributedAction::READY:
                if (phase != DistributedStatus::COMMITTED) { reject(); } done = record.ready;
                break;
            case DistributedAction::FINISH:
                done = record.finished;
                if (phase != DistributedStatus::ABORTED && !(phase == DistributedStatus::COMMITTED && record.ready)) { reject(); }
                break;
            default: reject();
        }
        if (done) { response.value = encodeDistributedRecord(record); return response; }
        if (message.action != DistributedAction::DECIDE_ABORT) {
            if (storageAdmission.owns_lock()) { storageAdmission.unlock(); }
            actor.unlock();
            // After a terminal decision, an ordered leader status showing no reservation
            // proves release even if its TTL receipt has expired. Admission prevents
            // a stale FREEZE/prepare proposal from crossing this status barrier.
            for (auto index : record.participants) {
                const auto status = distributedRpc({.action = DistributedAction::PARTICIPANT_STATUS, .id = message.id, .participant = index});
                if (status.value.size() != 1) { reject(); }
                const auto flags = status.value[0];
                if ((message.action == DistributedAction::START_PREPARE && (flags & 1) == 0) ||
                    (message.action == DistributedAction::DECIDE_COMMIT && (flags & 2) == 0) ||
                    (message.action == DistributedAction::READY && (flags & 12) == 0) ||
                    (message.action == DistributedAction::FINISH && (flags & 1) != 0)) { reject(); }
            }
            actor = cluster::detail::ReconfigurationDeadline::lock(distributedCoordinatorMu);
            if (!usesPartitionRaft()) { storageAdmission = cluster::detail::ReconfigurationDeadline::lock(distributedProposalMu); }
            validationTerm = requireDistributedLeader();
            const auto current = localDistributedRecord(message.id);
            if ((message.action == DistributedAction::START_PREPARE && current.status == DistributedStatus::PREPARING) ||
                (message.action == DistributedAction::DECIDE_COMMIT && current.status == DistributedStatus::COMMITTED)) {
                response.value = encodeDistributedRecord(current); return response;
            }
            if (current.status != record.status || current.participants != record.participants) { reject(); }
            if ((message.action == DistributedAction::READY && current.ready) ||
                (message.action == DistributedAction::FINISH && current.finished)) {
                response.value = encodeDistributedRecord(current); return response;
            }
        }
        if (message.action != DistributedAction::DECIDE_ABORT && message.action != DistributedAction::READY && message.action != DistributedAction::FINISH && record.expires <= distributedNowMs()) { reject(); }
    }
    proposeDistributed(message, true, validationTerm);
    response.value = encodeDistributedRecord(localDistributedRecord(message.id)); return response;
}
cluster::ForwardResponse receiveDistributedParticipant(const DistributedMessage& message, Impl& authority) {
    auto actor = cluster::detail::ReconfigurationDeadline::lock(distributedProposalMu);
    const auto validationTerm = requireDistributedLeader();
    cluster::ForwardResponse response; response.success = true;
    DistributedParticipant participant; bool completed;
    {
        std::lock_guard lock{distributedStateMu};
        if (const auto it = distributedState.participants.find(message.id); it != distributedState.participants.end()) {
            if (message.action == DistributedAction::APPEND) {
                const auto& p = it->second;
                participant.id = p.id; participant.frozen = p.frozen; participant.sealed = p.sealed;
                participant.expectedBytes = p.expectedBytes; participant.receivedBytes = p.receivedBytes; participant.chunks = p.chunks;
            } else { participant = it->second; }
        }
        completed = distributedCompleted(message.id);
    }
    if (message.action == DistributedAction::PARTICIPANT_STATUS) {
        response.value = {static_cast<uint8_t>((participant.id == message.id ? 1 : 0) | (participant.id == message.id && participant.sealed ? 2 : 0) |
            (participant.id == message.id && participant.applied ? 4 : 0) | (completed ? 8 : 0))}; return response;
    }
    const auto decision = authority.distributedRecord(message.id);
    const auto reject = [] { throw TransactionConflict("Cluster transaction: reservation or decision changed; retry transaction"); };
    if (decision.status == DistributedStatus::UNKNOWN || !std::ranges::contains(decision.participants, message.participant)) { reject(); }
    if (message.action == DistributedAction::RELEASE) {
        if (decision.status != DistributedStatus::ABORTED && !(decision.status == DistributedStatus::COMMITTED && decision.ready)) { reject(); }
        if (completed) { return response; }
        auto command = message; command.expires = decision.expires; command.argument = distributedNowMs();
        {
            std::lock_guard lock{distributedStateMu}; auto released = distributedState;
            released.participants.erase(message.id);
            if (distributedReservedBytes(released) > opts.transactions.maxJournalBytes) { reject(); }
        }
        proposeDistributed(command, false, validationTerm); return response;
    }
    if (message.action == DistributedAction::APPLY) {
        if (decision.status != DistributedStatus::COMMITTED) { reject(); }
        if (completed || (participant.id == message.id && participant.applied)) { return response; }
        if (participant.id != message.id || !participant.sealed) { reject(); }
        auto command = message; command.expires = decision.expires;
        proposeDistributed(command, false, validationTerm); return response;
    }
    if (decision.finished || decision.expires <= distributedNowMs()) { reject(); }
    if (message.action == DistributedAction::FREEZE) {
        if (decision.status != DistributedStatus::OPEN || completed) { reject(); }
        if (participant.id == message.id) { return response; }
        auto command = message; command.expires = decision.expires; command.argument = distributedNowMs();
        {
            std::lock_guard lock{distributedStateMu}; auto retained = distributedState;
            if (distributedState.participants.size() >= opts.transactions.maxOpen ||
                !distributedResults(false)->canReserve((distributedState.participants.size() + 1) * cluster::detail::DiskResults::recordCharge(16, 0), command.argument)) { reject(); }
            DistributedParticipant p; p.id = message.id; p.expires = decision.expires; p.frozen = true;
            retained.participants.emplace(p.id, std::move(p));
            const auto size = distributedReservedBytes(retained);
            if (size > opts.transactions.maxJournalBytes) { reject(); }
        }
        proposeDistributed(command, false, validationTerm); return response;
    }
    if (participant.id != message.id || !participant.frozen) { reject(); }
    if (message.action == DistributedAction::LOCK_KEY || message.action == DistributedAction::LOCK_RANGE) {
        if (decision.status != DistributedStatus::OPEN || participant.expectedBytes || participant.sealed) { reject(); }
        auto retained = participant;
        if (message.action == DistributedAction::LOCK_KEY) {
            if (message.payload.size() > UINT16_MAX) { throw std::length_error("Cluster transaction: oversized reservation key"); }
            if (authority.usesPartitionRaft() && authority.partitionIndex(message.payload) != message.participant) { reject(); }
            if (distributedKeyLocked(participant, message.payload)) { return response; }
            retained.keys.insert(message.payload);
        } else {
            if (message.argument > 1) { throw std::invalid_argument("Cluster transaction: invalid query barrier flag"); }
            cluster::detail::TransactionReader reader{message.payload}; auto start = reader.field(), end = reader.field(); reader.finish();
            if (start.size() > UINT16_MAX || end.size() > UINT16_MAX || (!end.empty() && start >= end)) { throw std::invalid_argument("Cluster transaction: invalid reservation range"); }
            if (std::ranges::find(participant.ranges, std::pair{start, end}) != participant.ranges.end() &&
                (message.argument == 0 || participant.queryBarrier)) { return response; }
            retained.queryBarrier = retained.queryBarrier || message.argument == 1;
            retained.ranges.emplace_back(std::move(start), std::move(end));
        }
        uint64_t charge = 0;
        for (const auto& key : retained.keys) { charge += key.size() + 64; }
        for (const auto& range : retained.ranges) { charge += range.first.size() + range.second.size() + 64; }
        if (charge > opts.transactions.maxReadSetBytes) { throw std::length_error("Cluster transaction: reservation set capacity exceeded"); }
        {
            std::lock_guard lock{distributedStateMu};
            if (distributedReservationConflict(distributedState, retained)) { reject(); }
            auto next = distributedState; next.participants[message.id] = std::move(retained);
            if (distributedReservedBytes(next) > opts.transactions.maxJournalBytes) { reject(); }
        }
        auto command = message; command.expires = decision.expires;
        proposeDistributed(command, false, validationTerm); return response;
    }
    if (message.action == DistributedAction::GET || message.action == DistributedAction::SCAN) {
        if (decision.status != DistributedStatus::OPEN || participant.sealed) { reject(); }
        if (message.action == DistributedAction::GET) {
            if (message.payload.size() > UINT16_MAX) { throw std::length_error("Cluster transaction: oversized read key"); }
            if (!distributedKeyLocked(participant, message.payload)) { reject(); }
            auto value = getValueInternal(message.payload, false);
            response.found = value.has_value();
            if (value) {
                if (message.argument > value->size()) { throw std::invalid_argument("Cluster transaction: invalid read offset"); }
                cluster::detail::transactionInteger(response.value, value->size());
                const auto chunk = std::span<const uint8_t>{*value}.subspan(static_cast<size_t>(message.argument), std::min<size_t>(1024 * 1024, value->size() - static_cast<size_t>(message.argument)));
                response.value.insert(response.value.end(), chunk.begin(), chunk.end());
            }
            return response;
        }
        cluster::detail::TransactionReader reader{message.payload}; auto start = reader.field(), end = reader.field(); reader.finish();
        if (start.size() > UINT16_MAX || end.size() > UINT16_MAX || (!end.empty() && start > end)) { throw std::invalid_argument("Cluster transaction: invalid scan bounds"); }
        if (std::ranges::none_of(participant.ranges, [&](const auto& range) {
            return range.first <= start && (range.second.empty() || (!end.empty() && end <= range.second));
        })) { reject(); }
        auto source = captureTransactionReadSnapshot({});
        auto rows = storedScanGenerator(source->source.memtable.fork({start, end}), source->source.sst.fork(start, end));
        uint32_t count = 0; bool more = false; response.value.resize(5);
        for (const auto& row : rows) {
            if (message.argument && compareKey(row.key, start) == 0) { continue; }
            const auto valueSize = (row.flags & MemHdr16::FLAG_BLOB) != 0 ? blob::decodeBlobRef(row.value.data()).totalSize : row.value.size();
            const bool inlineValue = valueSize <= 64 * 1024;
            auto value = inlineValue ? materializeQueryValue(row.flags, row.value, source->source.storage->blobManager.get()) : std::vector<uint8_t>{};
            if (count && response.value.size() + row.key.size() + value.size() + 9 > 1024 * 1024) { more = true; break; }
            cluster::detail::transactionBytes(response.value, row.key); response.value.push_back(static_cast<uint8_t>(inlineValue));
            cluster::detail::transactionBytes(response.value, value); ++count;
        }
        response.value[0] = static_cast<uint8_t>(more);
        for (unsigned i = 0; i < 4; ++i) { response.value[1 + i] = static_cast<uint8_t>(count >> (8 * i)); }
        return response;
    }
    if (decision.status != DistributedStatus::PREPARING || participant.queryBarrier) { reject(); }
    if (message.action == DistributedAction::PREPARE) {
        if (message.argument < 9 || message.argument > opts.transactions.maxWriteBytes + 1024 * 1024 || message.argument > opts.transactions.maxJournalBytes / 2 || message.payload.size() != 8) { throw std::length_error("Cluster transaction: prepared values exceed capacity"); }
        if (participant.expectedBytes) {
            cluster::detail::TransactionReader timestamp{message.payload};
            if (participant.expectedBytes != message.argument || timestamp.integer() != participant.timestamp) { reject(); } return response;
        }
        {
            std::lock_guard lock{distributedStateMu}; auto retained = distributedState;
            retained.participants.at(message.id).expectedBytes = message.argument;
            if (distributedReservedBytes(retained) > opts.transactions.maxJournalBytes) { reject(); }
        }
    } else if (message.action == DistributedAction::APPEND) {
        if (!participant.expectedBytes || participant.sealed || message.payload.empty() || message.payload.size() > 1024 * 1024) { reject(); }
        if (message.argument < participant.receivedBytes) {
            if (!distributedPrepareMatches(participant, message.argument, message.payload)) { reject(); }
            return response;
        }
        if (message.argument != participant.receivedBytes || message.payload.size() > participant.expectedBytes - participant.receivedBytes) { reject(); }
        {
            std::lock_guard lock{distributedStateMu};
            const auto planned = (participant.expectedBytes + 1024 * 1024 - 1) / (1024 * 1024);
            if (participant.chunks.size() >= planned) {
                const auto size = distributedReservedBytes(distributedState);
                if (size > opts.transactions.maxJournalBytes || 44 > opts.transactions.maxJournalBytes - size) { reject(); }
            }
        }
    } else if (message.action == DistributedAction::SEAL) {
        if (participant.sealed) { return response; }
        if (participant.receivedBytes != participant.expectedBytes) { reject(); }
        const auto prepare = cluster::detail::decodeTransactionPrepare(distributedPrepareBytes(participant));
        if (prepare.writes.size() > std::min<uint32_t>(65536, opts.transactions.maxWrites)) { throw std::length_error("Cluster transaction: too many writes"); }
        validateDistributedWrites(prepare);
        for (const auto& write : prepare.writes) {
            if (!distributedKeyLocked(participant, write.key)) { reject(); }
            if (authority.usesPartitionRaft() && authority.partitionIndex(write.key) != message.participant) { throw std::invalid_argument("Cluster transaction: incorrect write participant"); }
        }
    } else { throw std::invalid_argument("Cluster transaction: unsupported participant action"); }
    auto command = message;
    command.expires = decision.expires;
    proposeDistributed(command, false, validationTerm); return response;
}
cluster::ForwardResponse receiveClusterTransaction(const cluster::ForwardRequest& request) {
    if (!distributedTransactionsEnabled() || opts.cluster.runtime.transactions.participantOnly || request.entries.size() != 1 || !request.entries[0].key.empty()) { throw std::invalid_argument("Cluster transaction: disabled or invalid request"); }
    cluster::detail::ReconfigurationDeadline budget{request.timeoutMs};
    const auto message = cluster::detail::decodeTransactionMessage(request.entries[0].value);
    try {
        if (distributedCoordinatorAction(message.action)) { return receiveDistributedCoordinator(message); }
        if (!usesPartitionRaft()) {
            if (message.participant) { throw std::invalid_argument("Cluster transaction: invalid participant"); }
            return receiveDistributedParticipant(message, *this);
        }
        auto children = partitionSnapshot();
        if (message.participant >= children.size() || !children[message.participant]) { throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::NOT_OWNER); }
        auto& child = *children[message.participant]->impl_;
        OperationGuard operation{child}; return child.receiveDistributedParticipant(message, *this);
    } catch (const TransactionConflict& error) {
        throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::TRANSACTION_CONFLICT, {}, error.what());
    }
}
void completeDistributed(const DistributedId& id) {
    auto record = distributedRecord(id);
    if (record.finished) { return; }
    if (record.status != DistributedStatus::COMMITTED && record.status != DistributedStatus::ABORTED) { return; }
    if (record.status == DistributedStatus::COMMITTED && !record.ready) {
        for (auto index : record.participants) { (void)distributedRpc({.action = DistributedAction::APPLY, .id = id, .participant = index}); }
        (void)distributedRpc({.action = DistributedAction::READY, .id = id});
    }
    for (auto index : record.participants) { (void)distributedRpc({.action = DistributedAction::RELEASE, .id = id, .participant = index}); }
    (void)distributedRpc({.action = DistributedAction::FINISH, .id = id});
}
void abortDistributed(const DistributedId& id) {
    (void)distributedRpc({.action = DistributedAction::DECIDE_ABORT, .id = id}); completeDistributed(id);
}
void startDistributedRecovery() {
    if (!distributedTransactionsEnabled() || opts.cluster.runtime.transactions.participantOnly) { return; }
    const auto workers = opts.cluster.runtime.transactions.recoveryConcurrency;
    auto executor = std::make_shared<cluster::detail::BoundedExecutor>(workers, workers);
    distributedRecoveryThread = std::jthread([this, executor, workers](std::stop_token stop) {
        using Clock = std::chrono::steady_clock;
        struct Retry { Clock::time_point due{}; uint32_t failures = 0; bool running = false; };
        struct Schedule { std::mutex mutex; std::map<DistributedId, Retry> retries; uint64_t completed = 0; };
        auto schedule = std::make_shared<Schedule>();
        uint64_t observed = 0; DistributedId cursor{};
        while (!stop.stop_requested()) {
            try {
                OperationGuard operation{*this, false};
                if (!operation) { break; }
                if (clusterRuntime->transactionLeaderNodeId() == nodeId) {
                    std::map<DistributedId, DistributedRecord> pending;
                    {
                        std::lock_guard lock{distributedStateMu};
                        for (const auto& [id, record] : distributedState.decisions) {
                            if (record.status == DistributedStatus::COMMITTED || record.status == DistributedStatus::ABORTED ||
                                record.expires <= distributedNowMs()) { pending.emplace(id, record); }
                        }
                    }
                    bool failed = false, report = false;
                    {
                        std::lock_guard lock{schedule->mutex};
                        std::erase_if(schedule->retries, [&](const auto& item) { return !item.second.running && !pending.contains(item.first); });
                        failed = std::ranges::any_of(schedule->retries, [](const auto& item) { return item.second.failures != 0; });
                        report = observed != schedule->completed || (pending.empty() && schedule->retries.empty());
                        observed = schedule->completed;
                    }
                    if (report) { finishDistributedRecoveryPass(failed); }
                    // Rotate admission so slow or failing IDs cannot monopolize the next worker slot.
                    auto first = pending.upper_bound(cursor);
                    for (size_t n = 0; n < pending.size(); ++n) {
                        if (first == pending.end()) { first = pending.begin(); }
                        const auto [id, record] = *first++;
                        {
                            std::lock_guard lock{schedule->mutex};
                            const auto active = std::ranges::count_if(schedule->retries, [](const auto& item) { return item.second.running; });
                            if (active >= workers) { break; }
                            auto& retry = schedule->retries[id];
                            if (retry.running || retry.due > Clock::now()) { continue; }
                            retry.running = true;
                        }
                        const bool accepted = executor->trySubmit([this, stop, schedule, id, record] {
                            bool failed = false;
                            try {
                                OperationGuard operation{*this, false};
                                if (operation && clusterRuntime->transactionLeaderNodeId() == nodeId) {
                                    const auto& policy = opts.cluster.runtime.transactions;
                                    cluster::detail::ReconfigurationDeadline budget{std::min(policy.timeoutMs, policy.recoveryAttemptTimeoutMs),
                                        [this, stop] { return stop.stop_requested() || placementClosing() || clusterRuntime->transactionLeaderNodeId() != nodeId; }};
                                    if (record.status == DistributedStatus::COMMITTED || record.status == DistributedStatus::ABORTED) { completeDistributed(id); }
                                    else { abortDistributed(id); }
                                }
                            } catch (const std::exception& error) { failed = true; recordDistributedRecoveryFailure(id, error.what()); }
                            catch (...) { failed = true; recordDistributedRecoveryFailure(id, "Unknown non-standard exception during transaction recovery"); }
                            std::lock_guard lock{schedule->mutex};
                            auto& retry = schedule->retries[id]; retry.running = false;
                            retry.failures = failed ? std::min<uint32_t>(retry.failures + 1, 16) : 0;
                            const uint64_t delay = uint64_t{opts.cluster.runtime.transactions.recoveryIntervalMs} << std::min<uint32_t>(retry.failures, 7);
                            retry.due = Clock::now() + std::chrono::milliseconds{std::min<uint64_t>(delay, 30000)};
                            ++schedule->completed;
                        });
                        if (!accepted) { std::lock_guard lock{schedule->mutex}; schedule->retries[id].running = false; break; }
                        cursor = id;
                    }
                }
            } catch (const std::exception& error) { recordDistributedRecoveryFailure({}, error.what()); finishDistributedRecoveryPass(true); }
            catch (...) { recordDistributedRecoveryFailure({}, "Unknown non-standard exception during recovery pass"); finishDistributedRecoveryPass(true); }
            std::unique_lock lock{lifecycleMu};
            lifecycleCv.wait_for(lock, std::chrono::milliseconds{opts.cluster.runtime.transactions.recoveryIntervalMs}, [&] { return stop.stop_requested(); });
        }
        // The executor joins admitted jobs when this thread's capture is destroyed.
    });
}
std::unique_lock<std::timed_mutex> distributedConfigurationAdmission() {
    std::unique_lock<std::timed_mutex> guard{distributedCoordinatorMu, std::defer_lock};
    if (!distributedTransactionsEnabled()) { return guard; }
    cluster::detail::ReconfigurationDeadline budget{opts.cluster.runtime.transactions.timeoutMs};
    guard = cluster::detail::ReconfigurationDeadline::lock(distributedCoordinatorMu);
    clusterRuntime->transactionReadBarrier();
    std::lock_guard lock{distributedStateMu};
    if (!distributedState.decisions.empty() || !distributedState.participants.empty()) {
        throw TransactionConflict("Cluster transaction: placement/membership change requires transactions to finish");
    }
    return guard;
}

std::pair<DistributedId, std::shared_ptr<void>> reserveGlobalQueryRange(std::span<const uint8_t> start, std::span<const uint8_t> end) {
    DistributedId id{}; crypto::secureRandom(id);
    auto release = std::shared_ptr<void>{this, [this, id](void*) {
        try {
            cluster::detail::ReconfigurationDeadline budget{5000, [this] { return placementClosing(); }};
            abortDistributed(id);
        } catch (...) { /* Replicated expiry and recovery release an abandoned reservation. */ }
    }};
    (void)distributedRpc({.action = DistributedAction::CREATE, .id = id,
        .argument = std::min(opts.cluster.runtime.forwardingTimeoutMs, opts.transactions.maxLifetimeMs)});
    const auto count = usesPartitionRaft() ? clusterConfig.partition().partitionCount : 1;
    for (uint16_t index = 0; index < count; ++index) {
        (void)distributedRpc({.action = DistributedAction::ENLIST, .id = id, .participant = index});
        DistributedMessage message{.action = DistributedAction::LOCK_RANGE, .id = id, .participant = index, .argument = 1};
        cluster::detail::transactionBytes(message.payload, start); cluster::detail::transactionBytes(message.payload, end);
        (void)distributedRpc(message);
    }
    return {id, std::move(release)};
}

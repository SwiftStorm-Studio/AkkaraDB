// Fixed query sources own storage and immutable iterators, never thread-owned
// locks. A cursor can therefore continue on another RPC worker.
struct QueryPinBudget {
    std::mutex mutex;
    uint64_t bytes = 0;
    uint32_t snapshots = 0;
};
std::shared_ptr<QueryPinBudget> queryPinBudget = std::make_shared<QueryPinBudget>();

std::shared_ptr<void> reserveQuerySnapshot(uint64_t bytes, std::shared_ptr<QueryPinBudget> budget = {}) {
    if (!budget) { budget = queryPinBudget; }
    const auto& policy = opts.cluster.runtime;
    std::lock_guard lock{budget->mutex};
    if (bytes > policy.queryMaxPinnedBytes) { throw std::runtime_error("Cluster query: fixed snapshot exceeds pinned-byte limit"); }
    if (budget->bytes > policy.queryMaxPinnedBytes - bytes || budget->snapshots >= policy.queryMaxPinnedSnapshots) { return {}; }
    budget->bytes += bytes; ++budget->snapshots;
    return std::shared_ptr<void>{budget.get(), [budget, bytes](void*) {
        std::lock_guard release{budget->mutex}; budget->bytes -= bytes; --budget->snapshots;
    }};
}

std::shared_ptr<void> pinStripeQuery() {
    std::unique_lock lock{stripeGcEpochMu};
    stripeQueryPins->fetch_add(1, std::memory_order_acq_rel);
    return std::shared_ptr<void>{stripeQueryPins.get(), [pins = stripeQueryPins](void*) { pins->fetch_sub(1, std::memory_order_acq_rel); }};
}

struct FixedQuerySource {
    std::shared_ptr<StorageState> storage;
    std::shared_ptr<void> reservation;
    blob::BlobManager::ReadPin blobPin;
    memtable::MemTable::RangeIterator memtable;
    sst::SSTManager::Iterator sst;
};

struct QuerySnapshotStamp {
    std::shared_ptr<AkkEngine> child;
    std::shared_ptr<StorageState> storage;
    uint64_t sequence, allocated, term, epoch, snapshotGeneration;
    std::vector<uint8_t> start, end;
};
// Snapshot installation can reuse an already visible sequence.
// This process-local generation is protected by writeMu and never persisted.
uint64_t querySnapshotGeneration = 0;

bool validateQuerySnapshot(const std::vector<QuerySnapshotStamp>& stamps, bool singleSource,
    const std::optional<cluster::ClusterTransactionId>& barrier) {
    for (const auto& stamp : stamps) {
        auto& engine = stamp.child ? *stamp.child->impl_ : *this;
        std::optional<OperationGuard> operation;
        if (stamp.child) { operation.emplace(engine); }
        engine.clusterRuntime->queryReadBarrier();
        std::unique_lock writeLock{engine.writeMu};
        auto transactionRead = engine.distributedReadAdmission(stamp.start, stamp.end);
        std::shared_lock storageLock{engine.storageMu};
        const bool immutableRange = barrier.has_value() || (singleSource && stamps.size() == 1);
        if (barrier) {
            const auto found = engine.distributedState.participants.find(*barrier);
            if (found == engine.distributedState.participants.end()) { return false; }
            const auto& participant = found->second;
            if (!participant.queryBarrier ||
                !std::ranges::any_of(participant.ranges, [&](const auto& range) {
                    return compareKey(range.first, stamp.start) <= 0 &&
                        (range.second.empty() || (!stamp.end.empty() && compareKey(stamp.end, range.second) <= 0));
                })) { return false; }
        }
        if (engine.storage != stamp.storage || (!immutableRange && engine.snapshotSeq() != stamp.sequence) ||
            (!immutableRange && engine.memtable->lastSeq() != stamp.allocated) ||
            engine.clusterRuntime->raftStats().currentTerm != stamp.term ||
            engine.clusterRuntime->configurationEpoch() != stamp.epoch ||
            engine.querySnapshotGeneration != stamp.snapshotGeneration ||
            (engine.clusterConfig.usesDataConsensus() && engine.clusterRuntime->role() != cluster::NodeRole::PRIMARY)) {
            return false;
        }
    }
    return !stamps.empty();
}

FixedQuerySource captureQuerySource(std::span<const uint8_t> start, std::span<const uint8_t> end,
    std::shared_ptr<QueryPinBudget> budget = {}, std::vector<QuerySnapshotStamp>* stamps = nullptr,
    std::shared_ptr<AkkEngine> child = {}) {
    if (stamps && (opts.runtime.writeVisibility != AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER ||
        opts.runtime.sequence.allocation != AkkEngineOptions::SequenceAllocationMode::GLOBAL_ATOMIC)) {
        throw std::invalid_argument("Cluster query: GLOBAL_SNAPSHOT requires COMMIT_ORDER and GLOBAL_ATOMIC on every source");
    }
    waitForKeySequenceReadVisibility();
    for (;;) {
        cluster::detail::ReconfigurationDeadline::check();
        std::unique_lock writeLock{writeMu};
        auto transactionRead = distributedReadAdmission(start, end);
        std::shared_lock storageLock{storageMu};
        auto reservation = reserveQuerySnapshot(memtable->approxSize(), budget);
        if (reservation) {
            const auto seq = snapshotSeq();
            memtable::MemTable::KeyRange range{{start.begin(), start.end()}, {end.begin(), end.end()}};
            std::optional<memtable::MemTable::RangeIterator> mt;
            if (sstManager) { mt.emplace(memtable->sealAndPinIterator(range, seq)); }
            else { mt = memtable->sealAndPinMemoryIterator(range, seq, false,
                opts.cluster.runtime.queryMaxPinnedBytes, opts.cluster.runtime.queryMaxPinnedSnapshots); }
            if (mt) {
                sst::SSTManager::Iterator st;
                if (sstManager) { st = sstManager->scanIter(start, end, seq, true); }
                auto blobPin = blobManager ? blobManager->pinReads() : blob::BlobManager::ReadPin{};
                if (stamps) {
                    stamps->push_back({std::move(child), storage, seq, memtable->lastSeq(),
                        clusterRuntime->raftStats().currentTerm, clusterRuntime->configurationEpoch(), querySnapshotGeneration,
                        {start.begin(), start.end()}, {end.begin(), end.end()}});
                }
                return {storage, std::move(reservation), std::move(blobPin), std::move(*mt), std::move(st)};
            }
        }
        // Capacity waits must not hold write admission: an existing cursor may
        // need metadata apply/read visibility to finish and release its pin.
        reservation.reset(); storageLock.unlock(); writeLock.unlock();
        if (opts.cluster.runtime.querySnapshotAdmission == cluster::QuerySnapshotAdmission::REJECT) {
            throw std::runtime_error("Cluster query: immutable snapshot capacity exceeded");
        }
        if (placementClosing()) { throw std::runtime_error("Cluster query: engine is closing"); }
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
    }
}

struct OwnedQueryRecord { std::vector<uint8_t> key, value; uint8_t flags; blob::BlobManager* blobs; };
static core::ArenaGenerator<OwnedQueryRecord> fixedQueryRecords(FixedQuerySource source) {
    for (const auto& record : storedScanGenerator(std::move(source.memtable), std::move(source.sst))) {
        co_yield OwnedQueryRecord{{record.key.begin(), record.key.end()}, {record.value.begin(), record.value.end()}, record.flags, source.storage->blobManager.get()};
    }
}

core::ArenaGenerator<OwnedQueryRecord> captureQueryRecords(std::span<const uint8_t> start,
    std::span<const uint8_t> end, const std::vector<uint16_t>& expected = {},
    std::vector<QuerySnapshotStamp>* stamps = nullptr) {
    if (!usesPartitionRaft()) { return fixedQueryRecords(captureQuerySource(start, end, {}, stamps)); }
    std::vector<core::ArenaGenerator<OwnedQueryRecord>> sources;
    const auto children = partitionSnapshot();
    for (const auto index : expected) {
        if (index >= children.size() || !children[index] || children[index]->impl_->clusterRuntime->role() != cluster::NodeRole::PRIMARY) {
            throw std::runtime_error("Cluster query: partition leadership/coverage changed");
        }
        auto& child = *children[index]->impl_;
        child.clusterRuntime->queryReadBarrier();
        sources.push_back(fixedQueryRecords(child.captureQuerySource(start, end, queryPinBudget, stamps, children[index])));
    }
    return mergeFixedQueryRecords(std::move(sources));
}
static core::ArenaGenerator<OwnedQueryRecord> mergeFixedQueryRecords(std::vector<core::ArenaGenerator<OwnedQueryRecord>> sources) {
    std::vector<core::ArenaGenerator<OwnedQueryRecord>::iterator> positions;
    for (auto& source : sources) { positions.push_back(source.begin()); }
    for (;;) {
        size_t selected = positions.size();
        for (size_t i = 0; i < positions.size(); ++i) {
            if (positions[i] != std::default_sentinel &&
                (selected == positions.size() || compareKey(positions[i]->key, positions[selected]->key) < 0)) { selected = i; }
        }
        if (selected == positions.size()) { co_return; }
        co_yield *positions[selected];
        ++positions[selected];
    }
}

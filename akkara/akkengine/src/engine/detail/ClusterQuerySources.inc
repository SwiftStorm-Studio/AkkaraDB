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

FixedQuerySource captureQuerySource(std::span<const uint8_t> start, std::span<const uint8_t> end,
    std::shared_ptr<QueryPinBudget> budget = {}) {
    waitForKeySequenceReadVisibility();
    for (;;) {
        cluster::detail::ReconfigurationDeadline::check();
        std::unique_lock writeLock{writeMu};
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
                if (sstManager) { st = sstManager->scanIter(start, end, seq); }
                auto blobPin = blobManager ? blobManager->pinReads() : blob::BlobManager::ReadPin{};
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
    std::span<const uint8_t> end, const std::vector<uint16_t>& expected = {}) {
    if (!usesPartitionRaft()) { return fixedQueryRecords(captureQuerySource(start, end)); }
    std::vector<core::ArenaGenerator<OwnedQueryRecord>> sources;
    const auto children = partitionSnapshot();
    for (const auto index : expected) {
        if (index >= children.size() || !children[index] || children[index]->impl_->clusterRuntime->role() != cluster::NodeRole::PRIMARY) {
            throw std::runtime_error("Cluster query: partition leadership/coverage changed");
        }
        auto& child = *children[index]->impl_;
        child.clusterRuntime->queryReadBarrier();
        sources.push_back(fixedQueryRecords(child.captureQuerySource(start, end, queryPinBudget)));
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

/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/detail/TransactionApi.inc
// Included at namespace scope after AkkEngine::Impl is complete.
class AkkEngine::Transaction::Impl {
    public:
        using Engine = AkkEngine::Impl;
        using Writes = std::map<std::vector<uint8_t>, std::optional<std::vector<uint8_t>>>;
        struct Range { std::vector<uint8_t> start, end; };
        Engine* engine;
        TransactionIsolation isolation;
        std::shared_ptr<Engine::TransactionReadSnapshot> snapshot;
        std::shared_ptr<Engine::TransactionRegistry> registry;
        uint64_t id = 0, cut = 0, generation = 0, writeBytes = 0, readBytes = 0;
        std::chrono::steady_clock::time_point expires;
        bool active = true;
        Writes writes;
        std::set<std::vector<uint8_t>> reads;
        std::vector<Range> ranges;

        Impl(Engine& owner, TransactionIsolation mode, std::shared_ptr<Engine::TransactionReadSnapshot> source)
            : engine{&owner}, isolation{mode}, snapshot{std::move(source)}, registry{owner.transactions},
              expires{std::chrono::steady_clock::now() + std::chrono::milliseconds{owner.opts.transactions.maxLifetimeMs}} {
            std::lock_guard lock{registry->mutex};
            if (registry->cuts.size() >= owner.opts.transactions.maxOpen) { throw std::runtime_error("AkkEngine: transaction limit exceeded"); }
            id = ++registry->nextId; cut = registry->revision; generation = registry->generation;
            registry->cuts.emplace(id, cut); registry->active.store(static_cast<uint32_t>(registry->cuts.size()), std::memory_order_release);
        }
        ~Impl() { finish(); }
        void finish() noexcept {
            if (!active) { return; }
            active = false;
            {
                std::lock_guard lock{registry->mutex}; registry->cuts.erase(id);
                registry->active.store(static_cast<uint32_t>(registry->cuts.size()), std::memory_order_release);
                if (registry->cuts.empty()) { registry->changes.clear(); registry->trackedBytes = 0; }
                else {
                    uint64_t oldest = UINT64_MAX;
                    for (const auto& [unused, begin] : registry->cuts) { oldest = std::min(oldest, begin); }
                    for (auto it = registry->changes.begin(); it != registry->changes.end();) {
                        if (it->second <= oldest) { registry->trackedBytes -= it->first.size() + 64; it = registry->changes.erase(it); }
                        else { ++it; }
                    }
                }
            }
            writes.clear(); reads.clear(); ranges.clear(); snapshot.reset();
        }
        void check() {
            if (!active) { throw std::logic_error("AkkEngine: transaction is no longer active"); }
            if (std::chrono::steady_clock::now() >= expires) {
                finish(); throw TransactionConflict("AkkEngine: transaction lifetime exceeded; transaction aborted");
            }
            engine->throwIfBackgroundFailed();
        }
        void trackRead(std::span<const uint8_t> key) {
            if (isolation != TransactionIsolation::SERIALIZABLE) { return; }
            std::vector<uint8_t> owned{key.begin(), key.end()};
            if (reads.contains(owned)) { return; }
            const auto charge = owned.size() + 64;
            if (charge > engine->opts.transactions.maxReadSetBytes - readBytes) { throw std::length_error("AkkEngine: transaction read-set limit exceeded"); }
            reads.emplace(std::move(owned)); readBytes += charge;
        }
        void trackRange(std::span<const uint8_t> start, std::span<const uint8_t> end) {
            if (!end.empty() && compareKey(start, end) > 0) { throw std::invalid_argument("AkkEngine: inverted transaction range"); }
            if (isolation != TransactionIsolation::SERIALIZABLE || (!end.empty() && compareKey(start, end) == 0)) { return; }
            const auto charge = start.size() + end.size() + 64;
            if (charge > engine->opts.transactions.maxReadSetBytes - readBytes) { throw std::length_error("AkkEngine: transaction read-set limit exceeded"); }
            ranges.push_back({{start.begin(), start.end()}, {end.begin(), end.end()}}); readBytes += charge;
        }
        void stage(std::span<const uint8_t> key, std::optional<std::span<const uint8_t>> value) {
            check();
            if (key.size() > UINT16_MAX) { throw std::length_error("AkkEngine: transaction key is too large"); }
            const auto& limits = engine->opts.transactions;
            std::vector<uint8_t> owned{key.begin(), key.end()};
            auto found = writes.find(owned);
            const uint64_t oldCharge = found == writes.end() ? 0 : found->first.size() + (found->second ? found->second->size() : 0) + 64;
            const uint64_t newCharge = key.size() + (value ? value->size() : 0) + 64;
            if ((found == writes.end() && writes.size() >= limits.maxWrites) || newCharge > limits.maxWriteBytes ||
                writeBytes - oldCharge > limits.maxWriteBytes - newCharge) { throw std::length_error("AkkEngine: transaction write-set limit exceeded"); }
            // Validate storage limits before any commit decision can be published.
            const auto storedSize = value && engine->blobManager && value->size() >= engine->blobManager->threshold()
                ? blob::BLOB_REF_SIZE : (value ? value->size() : 0);
            if (value && value->size() > UINT32_MAX) { throw std::length_error("AkkEngine: transaction value is too large"); }
            if (key.size() + storedSize + 64 >= 32ull * 1024 * 1024) { throw std::length_error("AkkEngine: transaction record is too large"); }
            std::optional<std::vector<uint8_t>> staged;
            if (value) { staged.emplace(value->begin(), value->end()); }
            writes.insert_or_assign(std::move(owned), std::move(staged)); writeBytes = writeBytes - oldCharge + newCharge;
        }
        void validate() {
            std::lock_guard lock{registry->mutex};
            if (generation != registry->generation) { throw TransactionConflict("AkkEngine: transaction conflict tracking capacity exceeded"); }
            const auto changed = [&](const auto& key) {
                const auto found = registry->changes.find(key); return found != registry->changes.end() && found->second > cut;
            };
            for (const auto& [key, unused] : writes) { if (changed(key)) { throw TransactionConflict("AkkEngine: transaction write conflict"); } }
            if (isolation == TransactionIsolation::SERIALIZABLE) {
                for (const auto& key : reads) { if (changed(key)) { throw TransactionConflict("AkkEngine: transaction read conflict"); } }
                for (const auto& range : ranges) {
                    for (auto it = registry->changes.lower_bound(range.start); it != registry->changes.end() &&
                         (range.end.empty() || compareKey(it->first, range.end) < 0); ++it) {
                        if (it->second > cut) { throw TransactionConflict("AkkEngine: transaction range conflict"); }
                    }
                }
            }
        }
        static core::ArenaGenerator<ScanRecordView> scanRows(std::shared_ptr<Engine::TransactionReadSnapshot> snapshot,
            Writes overlay, std::vector<uint8_t> start, std::vector<uint8_t> end, core::BufferArena& arena) {
            auto records = storedScanGenerator(snapshot->source.memtable.fork({start, end}), snapshot->source.sst.fork(start, end));
            auto it = records.begin(); auto staged = overlay.lower_bound(start);
            const auto inRange = [&] { return staged != overlay.end() && (end.empty() || compareKey(staged->first, end) < 0); };
            while (it != std::default_sentinel || inRange()) {
                if (inRange() && (it == std::default_sentinel || compareKey(staged->first, it->key) <= 0)) {
                    const bool equal = it != std::default_sentinel && compareKey(staged->first, it->key) == 0;
                    if (staged->second) { co_yield ScanRecordView{copySpanToArena(staged->first, arena), copySpanToArena(*staged->second, arena)}; }
                    ++staged; if (equal) { ++it; }
                } else {
                    auto value = Engine::materializeQueryValue(it->flags, it->value, snapshot->source.storage->blobManager.get());
                    co_yield ScanRecordView{copySpanToArena(it->key, arena), copySpanToArena(value, arena)}; ++it;
                }
            }
            records = {}; overlay.clear(); snapshot.reset();
        }
};

AkkEngine::Transaction::Transaction(std::shared_ptr<Impl> impl) : impl_{std::move(impl)} {}
AkkEngine::Transaction::~Transaction() = default;
AkkEngine::Transaction::Transaction(Transaction&&) noexcept = default;
AkkEngine::Transaction& AkkEngine::Transaction::operator=(Transaction&&) noexcept = default;
bool AkkEngine::Transaction::active() const noexcept { return impl_ && impl_->active; }
void AkkEngine::Transaction::rollback() noexcept { if (impl_) { impl_->finish(); } }
std::optional<std::vector<uint8_t>> AkkEngine::Transaction::get(std::span<const uint8_t> key) {
    if (!impl_) { throw std::logic_error("AkkEngine: moved transaction"); }
    impl_->check(); impl_->trackRead(key);
    const auto staged = impl_->writes.find(std::vector<uint8_t>{key.begin(), key.end()});
    if (staged != impl_->writes.end()) { return staged->second; }
    const auto& source = impl_->snapshot->source;
    auto mt = source.memtable.get(key);
    auto st = source.sst.get(key);
    if (mt && (!st || mt->seq() >= st->seq)) {
        if (mt->isTombstone()) { return {}; }
        return AkkEngine::Impl::materializeQueryValue(mt->flags(), mt->value(), source.storage->blobManager.get());
    }
    if (st) {
        if (st->isTombstone()) { return {}; }
        return AkkEngine::Impl::materializeQueryValue(st->flags, st->value, source.storage->blobManager.get());
    }
    return {};
}
bool AkkEngine::Transaction::exists(std::span<const uint8_t> key) { return get(key).has_value(); }
std::vector<AkkEngine::BatchGetResult> AkkEngine::Transaction::getBatch(std::span<const std::span<const uint8_t>> keys) {
    if (!impl_) { throw std::logic_error("AkkEngine: moved transaction"); }
    impl_->check();
    std::vector<BatchGetResult> result; result.reserve(keys.size());
    for (const auto key : keys) { auto value = get(key); result.push_back({value.has_value(), value ? std::move(*value) : std::vector<uint8_t>{}}); }
    return result;
}
void AkkEngine::Transaction::put(std::span<const uint8_t> key, std::span<const uint8_t> value) {
    if (!impl_) { throw std::logic_error("AkkEngine: moved transaction"); } impl_->stage(key, value);
}
void AkkEngine::Transaction::remove(std::span<const uint8_t> key) {
    if (!impl_) { throw std::logic_error("AkkEngine: moved transaction"); } impl_->stage(key, {});
}
core::ArenaGenerator<AkkEngine::ScanRecordView> AkkEngine::Transaction::scan(core::BufferArena& arena,
    std::span<const uint8_t> start, std::span<const uint8_t> end) {
    if (!impl_) { throw std::logic_error("AkkEngine: moved transaction"); }
    impl_->check(); impl_->trackRange(start, end);
    return Impl::scanRows(impl_->snapshot, impl_->writes, {start.begin(), start.end()}, {end.begin(), end.end()}, arena);
}
size_t AkkEngine::Transaction::count(std::span<const uint8_t> start, std::span<const uint8_t> end) {
    core::BufferArena arena{4096}; size_t count = 0;
    for (const auto& unused : scan(arena, start, end)) { (void)unused; ++count; arena.reset(); }
    return count;
}
AkkEngine::Transaction AkkEngine::begin(TransactionOptions options) {
    if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
    auto operation = std::make_shared<Impl::OperationGuard>(*impl_);
    if (impl_->opts.components.clusterEnabled) { throw std::logic_error("AkkEngine: transactions currently require standalone mode"); }
    if (impl_->opts.runtime.sequence.allocation != AkkEngineOptions::SequenceAllocationMode::GLOBAL_ATOMIC) {
        throw std::invalid_argument("AkkEngine: transactions require GLOBAL_ATOMIC sequence allocation");
    }
    if (options.isolation != TransactionIsolation::SERIALIZABLE && options.isolation != TransactionIsolation::SNAPSHOT_ISOLATION) {
        throw std::invalid_argument("AkkEngine: invalid transaction isolation");
    }
    impl_->waitForVersionLogRecovery();
    std::unique_lock admission{impl_->mutationEpochMu};
    impl_->throwIfBackgroundFailed();
    if (impl_->transactionHasPersistence() && impl_->opts.paths.transactionDir.empty()) { throw std::invalid_argument("AkkEngine: transaction directory is required for persistent transactions"); }
    if ((impl_->walWriter && !impl_->opts.runtime.recoverWal) || (impl_->sstManager && !impl_->opts.runtime.recoverSst)) {
        throw std::invalid_argument("AkkEngine: persistent transactions require storage recovery to remain enabled");
    }
    auto snapshot = impl_->captureTransactionReadSnapshot(std::move(operation));
    return Transaction{std::make_shared<Transaction::Impl>(*impl_, options.isolation, std::move(snapshot))};
}
void AkkEngine::Transaction::end() {
    if (!impl_) { throw std::logic_error("AkkEngine: moved transaction"); }
    auto& state = *impl_; state.check();
    // Keep Engine mutexes alive until admission/publication locks are released,
    // even when finish() drops the session's final snapshot lease.
    auto operation = state.snapshot->operation;
    auto& engine = *state.engine;
    std::unique_lock admission{engine.mutationEpochMu};
    std::unique_lock visibility{engine.transactionVisibilityMu};
    bool reserved = false, decided = false;
    uint64_t base = 0, seq = 0;
    try {
        state.check(); state.validate();
        if (state.writes.empty()) { state.finish(); return; }
        engine.applyWriteBackpressure();
        const auto count = state.writes.size();
        if (engine.memtable->lastSeq() > UINT64_MAX - count) { throw std::overflow_error("AkkEngine: transaction sequence exhausted"); }
        base = engine.reserveWriteSeq(count); seq = base + count - 1; reserved = true;
        AkkEngine::Impl::TransactionDecision decision{base, seq, engine.nodeId, nowNs(), {}};
        const bool persistent = engine.transactionHasPersistence();
        if (persistent) { engine.writeTransactionWatermark("allocated.aktm", seq); }
        uint64_t blobSequence = base;
        std::unique_lock gcPlanning{engine.blobGcPlanMu};
        // Keep newly prepared Blob files safe from flush-triggered orphan GC.
        engine.transactionJournalPending.store(true, std::memory_order_release);
        for (const auto& [key, value] : state.writes) {
            uint8_t flags = value ? MemHdr16::FLAG_NORMAL : MemHdr16::FLAG_TOMBSTONE;
            auto stored = value ? engine.maybeExternalize(blobSequence, *value, flags) : std::vector<uint8_t>{};
            ++blobSequence; decision.records.push_back({key, std::move(stored), flags});
        }
        crashAtTestPoint("transaction.before_decision");
        if (persistent) {
            const auto bytes = AkkEngine::Impl::encodeTransactionDecision(decision);
            if (bytes.size() > engine.opts.transactions.maxJournalBytes) { throw std::length_error("AkkEngine: transaction journal limit exceeded"); }
            if (engine.transactionJournalBytes > engine.opts.transactions.maxJournalBytes - bytes.size() ||
                engine.transactionJournals.size() >= 1024) { engine.checkpointTransactions(); }
            engine.transactionJournalPending.store(true, std::memory_order_release);
            const auto path = engine.opts.paths.transactionDir / (std::to_string(seq) + ".aktxn");
            engine.transactionJournals.reserve(engine.transactionJournals.size() + 1);
            try { writeFileAtomicallyDurable(path, bytes); }
            catch (...) { if (fs::exists(path)) { decided = true; } throw; }
            decided = true; engine.transactionJournals.push_back(path); engine.transactionJournalBytes += bytes.size();
            crashAtTestPoint("transaction.after_decision");
        }
        // Once memory application starts, failures also require fail-stop even
        // for memory-only engines: never expose a partially applied commit.
        decided = true;
        engine.applyTransactionDecision(decision, false);
        for (const auto& record : decision.records) {
            if ((record.flags & MemHdr16::FLAG_TOMBSTONE) != 0) { engine.removesTotal.fetch_add(1, std::memory_order_relaxed); }
            else { engine.putsTotal.fetch_add(1, std::memory_order_relaxed); }
            if ((record.flags & MemHdr16::FLAG_BLOB) != 0) { engine.blobPutsTotal.fetch_add(1, std::memory_order_relaxed); }
        }
        if (!persistent) { engine.transactionJournalPending.store(false, std::memory_order_release); }
        crashAtTestPoint("transaction.after_publish"); state.finish();
    } catch (...) {
        const auto failure = std::current_exception();
        if (decided) {
            { std::lock_guard lock{engine.transactionFailureMu}; engine.transactionFailure = failure; }
            state.finish(); throw TransactionOutcomeUnknown("AkkEngine: transaction commit outcome requires closing and reopening the engine");
        }
        if (reserved) { for (auto next = base; next <= seq; ++next) { engine.markWriteCommitted(next); } }
        engine.transactionJournalPending.store(!engine.transactionJournals.empty(), std::memory_order_release);
        state.finish(); std::rethrow_exception(failure);
    }
}

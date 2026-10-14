/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/detail/AkkTransaction.inc
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
        std::optional<cluster::ClusterTransactionId> distributedId;
        std::shared_ptr<Engine::OperationGuard> operation;
        std::vector<uint16_t> participants;
        bool distributedDecided = false;

        Impl(Engine& owner, TransactionIsolation mode, std::shared_ptr<Engine::OperationGuard> guard,
             cluster::ClusterTransactionId token)
            : engine{&owner}, isolation{mode}, registry{owner.transactions},
              expires{std::chrono::steady_clock::now() + std::chrono::milliseconds{owner.opts.transactions.maxLifetimeMs}},
              distributedId{token}, operation{std::move(guard)} {}

        Impl(Engine& owner, TransactionIsolation mode, std::shared_ptr<Engine::TransactionReadSnapshot> source)
            : engine{&owner}, isolation{mode}, snapshot{std::move(source)}, registry{owner.transactions},
              expires{std::chrono::steady_clock::now() + std::chrono::milliseconds{owner.opts.transactions.maxLifetimeMs}} {
            std::lock_guard lock{registry->mutex};
            if (registry->cuts.size() >= owner.opts.transactions.maxOpen) { throw TransactionSnapshotUnavailable("AkkEngine: transaction limit exceeded"); }
            id = ++registry->nextId; cut = registry->revision; generation = registry->generation;
            registry->cuts.emplace(id, cut); registry->active.store(static_cast<uint32_t>(registry->cuts.size()), std::memory_order_release);
        }
        ~Impl() { finish(); }
        void finish() noexcept {
            if (!active) { return; }
            active = false;
            if (distributedId) {
                if (!distributedDecided) {
                    try {
                        cluster::detail::ReconfigurationDeadline budget{engine->opts.cluster.runtime.transactions.timeoutMs, [&] { return engine->placementClosing(); }};
                        engine->abortDistributed(*distributedId);
                    } catch (...) { /* The replicated expiry causes eventual abort; never assume commit failed. */ }
                }
                writes.clear(); reads.clear(); ranges.clear(); operation.reset(); return;
            }
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
            if (distributedId) { return; }
            if (isolation != TransactionIsolation::SERIALIZABLE) { return; }
            std::vector<uint8_t> owned{key.begin(), key.end()};
            if (reads.contains(owned)) { return; }
            const auto charge = owned.size() + 64;
            if (charge > engine->opts.transactions.maxReadSetBytes - readBytes) { throw std::length_error("AkkEngine: transaction read-set limit exceeded"); }
            reads.emplace(std::move(owned)); readBytes += charge;
        }
        void trackRange(std::span<const uint8_t> start, std::span<const uint8_t> end) {
            if (!end.empty() && compareKey(start, end) > 0) { throw std::invalid_argument("AkkEngine: inverted transaction range"); }
            if ((!distributedId && isolation != TransactionIsolation::SERIALIZABLE) || (!end.empty() && compareKey(start, end) == 0)) { return; }
            const auto charge = start.size() + end.size() + 64;
            if (charge > engine->opts.transactions.maxReadSetBytes - readBytes) { throw std::length_error("AkkEngine: transaction read-set limit exceeded"); }
            ranges.push_back({{start.begin(), start.end()}, {end.begin(), end.end()}}); readBytes += charge;
        }
        void enlist(uint16_t participant) {
            check();
            if (std::ranges::binary_search(participants, participant)) { return; }
            try {
                cluster::detail::ReconfigurationDeadline budget{engine->opts.cluster.runtime.transactions.timeoutMs};
                (void)engine->distributedRpc({.action = Engine::DistributedAction::ENLIST, .id = *distributedId, .participant = participant});
                participants.insert(std::ranges::lower_bound(participants, participant), participant);
            } catch (...) {
                // A competing partition reservation is never waited on: abort
                // the whole transaction and release its existing reservations.
                const auto failure = std::current_exception(); finish(); std::rethrow_exception(failure);
            }
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
            Engine::validateStoredRecord(key.size(), storedSize);
            std::optional<std::vector<uint8_t>> staged;
            if (value) { staged.emplace(value->begin(), value->end()); }
            if (distributedId) { reserveKey(key, engine->usesPartitionRaft() ? engine->partitionIndex(key) : uint16_t{0}); }
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
        void reserveKey(std::span<const uint8_t> key, uint16_t participant) {
            if (key.size() > UINT16_MAX) { throw std::length_error("Cluster transaction: oversized reservation key"); }
            const std::vector<uint8_t> owned{key.begin(), key.end()};
            if (reads.contains(owned)) { return; }
            const auto charge = owned.size() + 64;
            if (charge > engine->opts.transactions.maxReadSetBytes - readBytes) { throw std::length_error("Cluster transaction: reservation set capacity exceeded"); }
            enlist(participant);
            try {
                cluster::detail::ReconfigurationDeadline budget{engine->opts.cluster.runtime.transactions.timeoutMs};
                (void)engine->distributedRpc({.action = Engine::DistributedAction::LOCK_KEY, .id = *distributedId,
                    .participant = participant, .payload = {key.begin(), key.end()}});
                reads.insert(owned);
                readBytes += charge;
            } catch (...) { const auto failure = std::current_exception(); finish(); std::rethrow_exception(failure); }
        }
        void reserveRange(std::span<const uint8_t> start, std::span<const uint8_t> end, uint16_t participant) {
            enlist(participant);
            try {
                cluster::detail::ReconfigurationDeadline budget{engine->opts.cluster.runtime.transactions.timeoutMs};
                Engine::DistributedMessage message{.action = Engine::DistributedAction::LOCK_RANGE, .id = *distributedId, .participant = participant};
                cluster::detail::transactionBytes(message.payload, start); cluster::detail::transactionBytes(message.payload, end);
                (void)engine->distributedRpc(message);
            } catch (...) { const auto failure = std::current_exception(); finish(); std::rethrow_exception(failure); }
        }
        std::optional<std::vector<uint8_t>> distributedGet(std::span<const uint8_t> key, uint16_t participant) {
            reserveKey(key, participant);
            if (key.size() > UINT16_MAX) { throw std::length_error("Cluster transaction: oversized read key"); }
            cluster::detail::ReconfigurationDeadline budget{engine->opts.cluster.runtime.transactions.timeoutMs};
            std::vector<uint8_t> value; uint64_t expected = UINT64_MAX;
            do {
                check();
                auto response = engine->distributedRpc({.action = Engine::DistributedAction::GET, .id = *distributedId,
                    .participant = participant, .argument = value.size(), .payload = {key.begin(), key.end()}});
                if (!response.found) { if (expected != UINT64_MAX) { throw std::runtime_error("Cluster transaction: value vanished during read"); } return {}; }
                cluster::detail::TransactionReader reader{response.value}; const auto size = reader.integer();
                const auto chunk = std::span<const uint8_t>{response.value}.subspan(8);
                if (size > UINT32_MAX || size < value.size() || (expected != UINT64_MAX && expected != size) || chunk.size() > size - value.size() || (chunk.empty() && value.size() != size)) { throw std::runtime_error("Cluster transaction: invalid read chunk"); }
                expected = size; value.insert(value.end(), chunk.begin(), chunk.end());
            } while (value.size() != expected);
            return value;
        }
        static core::ArenaGenerator<ScanRecordView> distributedRows(std::shared_ptr<Impl> state, Writes overlay,
            std::vector<uint8_t> start, std::vector<uint8_t> end, core::BufferArena& arena) {
            if (!end.empty() && start == end) { co_return; }
            struct Cursor {
                uint16_t participant;
                std::vector<std::pair<std::vector<uint8_t>, std::optional<std::vector<uint8_t>>>> rows{};
                std::vector<uint8_t> after;
                size_t position = 0;
                bool exclusive = false, more = true;
            };
            std::vector<Cursor> cursors;
            for (auto index : state->participants) { cursors.push_back(Cursor{.participant = index, .after = start}); }
            auto staged = overlay.lower_bound(start);
            while (true) {
                state->check();
                for (auto& cursor : cursors) {
                    if (cursor.position < cursor.rows.size() || !cursor.more) { continue; }
                    cluster::detail::TransactionMessage request{.action = Engine::DistributedAction::SCAN, .id = *state->distributedId,
                        .participant = cursor.participant, .argument = static_cast<uint64_t>(cursor.exclusive)};
                    cluster::detail::transactionBytes(request.payload, cursor.after); cluster::detail::transactionBytes(request.payload, end);
                    cluster::detail::ReconfigurationDeadline budget{state->engine->opts.cluster.runtime.transactions.timeoutMs};
                    auto response = state->engine->distributedRpc(request);
                    cluster::detail::TransactionReader reader{response.value}; const auto more = reader.integer(1); const auto count = reader.count(9);
                    if (more > 1 || (more && !count)) { throw std::runtime_error("Cluster transaction: invalid scan page"); }
                    cursor.rows.clear(); cursor.position = 0; cursor.more = more != 0;
                    for (uint32_t i = 0; i < count; ++i) {
                        auto key = reader.field(); const auto inlineValue = reader.integer(1); auto value = reader.field();
                        if (inlineValue > 1 || (!inlineValue && !value.empty())) { throw std::runtime_error("Cluster transaction: invalid scan value"); }
                        if (key < cursor.after || (cursor.exclusive && key == cursor.after) || (!end.empty() && key >= end) ||
                            (!cursor.rows.empty() && cursor.rows.back().first >= key)) { throw std::runtime_error("Cluster transaction: invalid scan ordering"); }
                        std::optional<std::vector<uint8_t>> stored;
                        if (inlineValue) { stored = std::move(value); }
                        cursor.rows.emplace_back(std::move(key), std::move(stored));
                    }
                    reader.finish();
                    if (!cursor.rows.empty()) { cursor.after = cursor.rows.back().first; cursor.exclusive = true; }
                }
                Cursor* next = nullptr;
                for (auto& cursor : cursors) {
                    if (cursor.position < cursor.rows.size() && (!next || cursor.rows[cursor.position].first < next->rows[next->position].first)) { next = &cursor; }
                }
                const bool hasStaged = staged != overlay.end() && (end.empty() || staged->first < end);
                if (!next && !hasStaged) { break; }
                if (hasStaged && (!next || staged->first <= next->rows[next->position].first)) {
                    if (next && staged->first == next->rows[next->position].first) { ++next->position; }
                    if (staged->second) { co_yield ScanRecordView{copySpanToArena(staged->first, arena), copySpanToArena(*staged->second, arena)}; }
                    ++staged;
                } else {
                    const auto& row = next->rows[next->position++];
                    // Large values are fetched only when yielded, so a page of
                    // Blob keys cannot materialize all Blob payloads at once.
                    std::optional<std::vector<uint8_t>> fetched;
                    if (!row.second) {
                        fetched = state->distributedGet(row.first, next->participant);
                        if (!fetched) { throw std::runtime_error("Cluster transaction: scan value disappeared"); }
                    }
                    const auto& value = row.second ? *row.second : *fetched;
                    co_yield ScanRecordView{copySpanToArena(row.first, arena), copySpanToArena(value, arena)};
                }
            }
        }
};

AkkEngine::Transaction::Transaction(std::shared_ptr<Impl> impl) : impl_{std::move(impl)} {}
AkkEngine::Transaction::~Transaction() = default;
AkkEngine::Transaction::Transaction(Transaction&&) noexcept = default;
AkkEngine::Transaction& AkkEngine::Transaction::operator=(Transaction&&) noexcept = default;
bool AkkEngine::Transaction::active() const noexcept { return impl_ && impl_->active; }
std::optional<cluster::ClusterTransactionId> AkkEngine::Transaction::clusterId() const noexcept { return impl_ ? impl_->distributedId : std::nullopt; }
void AkkEngine::Transaction::rollback() noexcept { if (impl_) { impl_->finish(); } }
std::optional<std::vector<uint8_t>> AkkEngine::Transaction::get(std::span<const uint8_t> key) {
    if (!impl_) { throw std::logic_error("AkkEngine: moved transaction"); }
    impl_->check(); impl_->trackRead(key);
    const auto staged = impl_->writes.find(std::vector<uint8_t>{key.begin(), key.end()});
    if (staged != impl_->writes.end()) { return staged->second; }
    if (impl_->distributedId) {
        return impl_->distributedGet(key, impl_->engine->usesPartitionRaft() ? impl_->engine->partitionIndex(key) : uint16_t{0});
    }
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
    if (impl_->distributedId) {
        if (start.size() > UINT16_MAX || end.size() > UINT16_MAX) { throw std::length_error("Cluster transaction: oversized range boundary"); }
        if (!end.empty() && compareKey(start, end) == 0) { return Impl::distributedRows(impl_, impl_->writes, {start.begin(), start.end()}, {end.begin(), end.end()}, arena); }
        // Hash partitioning cannot exclude a partition from a key range.
        const auto count = impl_->engine->usesPartitionRaft() ? impl_->engine->clusterConfig.partition().partitionCount : 1;
        for (uint16_t index = 0; index < count; ++index) { impl_->reserveRange(start, end, index); }
        return Impl::distributedRows(impl_, impl_->writes, {start.begin(), start.end()}, {end.begin(), end.end()}, arena);
    }
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
    if (impl_->opts.components.clusterEnabled) {
        if (!impl_->distributedTransactionsEnabled() || impl_->opts.cluster.runtime.transactions.participantOnly) { throw std::logic_error("AkkEngine: enable TWO_PHASE_COMMIT on the Cluster root to begin transactions"); }
        if (options.isolation != TransactionIsolation::SERIALIZABLE && options.isolation != TransactionIsolation::SNAPSHOT_ISOLATION) { throw std::invalid_argument("AkkEngine: invalid transaction isolation"); }
        cluster::detail::ReconfigurationDeadline budget{impl_->opts.cluster.runtime.transactions.timeoutMs};
        Impl::DistributedId token{}; crypto::secureRandom(token);
        for (unsigned i = 0; i < 8; ++i) { token[i] = static_cast<uint8_t>(impl_->nodeId >> (8 * i)); }
        Impl::DistributedMessage create{.action = Impl::DistributedAction::CREATE, .id = token,
            .argument = impl_->opts.transactions.maxLifetimeMs};
        try {
            (void)impl_->distributedRpc(create);
            return Transaction{std::make_shared<Transaction::Impl>(*impl_, options.isolation, std::move(operation), token)};
        } catch (...) {
            const auto failure = std::current_exception();
            try { impl_->abortDistributed(token); } catch (...) {}
            std::rethrow_exception(failure);
        }
    }
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
    if (state.distributedId) {
        auto operation = state.operation;
        auto& engine = *state.engine;
        cluster::detail::ReconfigurationDeadline budget{engine.opts.cluster.runtime.transactions.timeoutMs};
        bool deciding = false;
        const char* phase = "prepare";
        try {
            (void)engine.distributedRpc({.action = Impl::Engine::DistributedAction::START_PREPARE, .id = *state.distributedId});
            std::map<uint16_t, Impl::Engine::DistributedPrepare> prepared;
            for (const auto& [key, value] : state.writes) {
                const auto index = engine.usesPartitionRaft() ? engine.partitionIndex(key) : uint16_t{0};
                prepared[index].writes.push_back({key, value});
            }
            for (auto index : state.participants) {
                const auto bytes = cluster::detail::encodeTransactionPrepare(prepared[index]);
                Impl::Engine::DistributedMessage header{.action = Impl::Engine::DistributedAction::PREPARE, .id = *state.distributedId,
                    .participant = index, .argument = bytes.size()};
                cluster::detail::transactionInteger(header.payload, nowNs());
                (void)engine.distributedRpc(header);
                for (size_t offset = 0; offset < bytes.size();) {
                    const auto size = std::min<size_t>(1024 * 1024, bytes.size() - offset);
                    (void)engine.distributedRpc({.action = Impl::Engine::DistributedAction::APPEND, .id = *state.distributedId, .participant = index,
                        .argument = offset, .payload = {bytes.begin() + static_cast<ptrdiff_t>(offset), bytes.begin() + static_cast<ptrdiff_t>(offset + size)}});
                    offset += size;
                }
                (void)engine.distributedRpc({.action = Impl::Engine::DistributedAction::SEAL, .id = *state.distributedId, .participant = index});
            }
            deciding = true;
            phase = "decision";
            crashAtTestPoint("cluster_transaction.before_decision");
            (void)engine.distributedRpc({.action = Impl::Engine::DistributedAction::DECIDE_COMMIT, .id = *state.distributedId});
            state.distributedDecided = true;
            phase = "completion";
            crashAtTestPoint("cluster_transaction.after_decision");
            engine.completeDistributed(*state.distributedId); state.finish(); return;
        } catch (...) {
            const auto failure = std::current_exception();
            if (deciding) {
                state.distributedDecided = true; state.finish();
                std::string reason;
                try { std::rethrow_exception(failure); } catch (const std::exception& error) { reason = error.what(); } catch (...) { reason = "unknown failure"; }
                throw TransactionOutcomeUnknown("AkkEngine: Cluster transaction requires transactionOutcome(id); " + std::string{phase} + ": " + reason, *state.distributedId);
            }
            state.finish(); std::rethrow_exception(failure);
        }
    }
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

cluster::ClusterTransactionResult AkkEngine::transactionOutcome(const cluster::ClusterTransactionId& id) {
    if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
    Impl::OperationGuard operation{*impl_};
    if (!impl_->distributedTransactionsEnabled() || impl_->opts.cluster.runtime.transactions.participantOnly) { throw std::logic_error("AkkEngine: Cluster transactions are disabled"); }
    cluster::detail::ReconfigurationDeadline budget{impl_->opts.cluster.runtime.transactions.timeoutMs};
    const auto record = impl_->distributedRecord(id); return {record.status, record.finished};
}

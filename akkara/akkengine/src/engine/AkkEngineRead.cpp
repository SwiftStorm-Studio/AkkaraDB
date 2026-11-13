/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/AkkEngineRead.cpp
#include "detail/AkkEngineImpl.hpp"

namespace akkaradb::engine {
    std::optional<std::vector<uint8_t>> AkkEngine::Impl::get(std::span<const uint8_t> key) {
        OperationGuard operation{*this};
        throwIfBackgroundFailed();
        waitForVersionLogRecovery();
        // Routed reads may wait for Raft application, which needs exclusive
        // visibility. The destination admits its local read after ReadIndex.
        std::shared_lock transactionRead{transactionVisibilityMu, std::defer_lock};
        if (!clusterReadMustRoute()) { transactionRead.lock(); }
        throwIfBackgroundFailed();
        return getValueClusterAware(key);
    }

    std::vector<AkkEngine::BatchGetResult> AkkEngine::Impl::getBatch(std::span<const std::span<const uint8_t>> keys) {
        OperationGuard operation{*this};
        throwIfBackgroundFailed();
        waitForVersionLogRecovery();
        std::shared_lock transactionRead{transactionVisibilityMu, std::defer_lock};
        if (!clusterReadMustRoute()) { transactionRead.lock(); }
        throwIfBackgroundFailed();
        if (!clusterReadMustRoute()) { waitForKeySequenceReadVisibility(); }
        if (usesPartitionRaft() && !clusterReadMustRoute()) {
            std::vector<BatchGetResult> results(keys.size());
            std::map<uint16_t, std::vector<size_t>> grouped;
            for (const auto& [i, key] : std::views::enumerate(keys)) { grouped[partitionIndex(key)].push_back(i); }
            for (const auto& [partition, indexes] : grouped) {
                const auto children = partitionSnapshot();
                if (!children[partition]) { continue; }
                std::vector<std::span<const uint8_t>> batch;
                for (const auto index : indexes) { batch.push_back(keys[index]); }
                auto values = children[partition]->getBatch(batch);
                for (const auto& [index, value] : std::views::zip(indexes, values)) { results[index] = std::move(value); }
            }
            return results;
        }
        auto blobReadPin = blobManager ? blobManager->pinReads() : blob::BlobManager::ReadPin{};
        std::optional<StoredReadSnapshot> snapshot;
        if (!clusterReadMustRoute()) { snapshot = captureStoredReadSnapshot(); }

        if (clusterReadMustRoute() && keys.size() > 1) {
            cluster::detail::BoundedExecutor* executor;
            {
                std::lock_guard lock{batchReadExecutorMu};
                if (!batchReadExecutor) {
                    batchReadExecutor = std::make_unique<cluster::detail::BoundedExecutor>(8, 64);
                }
                executor = batchReadExecutor.get();
            }
            std::vector<BatchGetResult> results(keys.size());
            const auto context = cluster::detail::ReconfigurationDeadline::current();
            executor->forEach(keys.size(), [&](size_t index) {
                cluster::detail::ReconfigurationDeadline scope{context};
                results[index].found = getIntoClusterAware(keys[index], results[index].value);
            }, context.deadline);
            return results;
        }

        std::vector<BatchGetResult> out;
        out.reserve(keys.size());

        for (const auto& key : keys) {
            BatchGetResult result;
            result.found = clusterReadMustRoute()
                               ? getIntoClusterAware(key, result.value)
                               : getIntoInternal(key, result.value, true, snapshot->memory.snapshotSeq(), &*snapshot);
            out.push_back(std::move(result));
        }

        return out;
    }

    bool AkkEngine::Impl::exists(std::span<const uint8_t> key) {
        OperationGuard operation{*this};
        throwIfBackgroundFailed();
        waitForVersionLogRecovery();
        std::shared_lock transactionRead{transactionVisibilityMu, std::defer_lock};
        if (!clusterReadMustRoute()) { transactionRead.lock(); }
        throwIfBackgroundFailed();
        if (clusterReadMustRoute()) {
            existsTotal.fetch_add(1, std::memory_order_relaxed);
            return getValueClusterAware(key, false).has_value();
        }
        waitForKeySequenceReadVisibility();
        existsTotal.fetch_add(1, std::memory_order_relaxed);
        if (usesPartitionRaft()) {
            auto* child = localPartition(key);
            return child && child->exists(key);
        }
        const auto record = readStoredPoint(key);
        return record && !record.view().isTombstone();
    }

    bool AkkEngine::Impl::getInto(std::span<const uint8_t> key, std::vector<uint8_t>& out) {
        OperationGuard operation{*this};
        throwIfBackgroundFailed();
        waitForVersionLogRecovery();
        std::shared_lock transactionRead{transactionVisibilityMu, std::defer_lock};
        if (!clusterReadMustRoute()) { transactionRead.lock(); }
        throwIfBackgroundFailed();
        return getIntoClusterAware(key, out);
    }

    bool AkkEngine::Impl::getIntoArena(std::span<const uint8_t> key, BufferArena& arena, std::span<const uint8_t>& out) {
        OperationGuard operation{*this};
        throwIfBackgroundFailed();
        waitForVersionLogRecovery();
        std::shared_lock transactionRead{transactionVisibilityMu, std::defer_lock};
        if (!clusterReadMustRoute()) { transactionRead.lock(); }
        throwIfBackgroundFailed();
        if (clusterReadMustRoute()) {
            auto value = getValueClusterAware(key);
            if (!value) {
                out = {};
                return false;
            }
            out = copySpanToArena(*value, arena);
            return true;
        }
        return getIntoArenaInternal(key, arena, out, true);
    }

    size_t AkkEngine::Impl::count(std::span<const uint8_t> startKey, std::span<const uint8_t> endKey) {
        OperationGuard operation{*this};
        throwIfBackgroundFailed();
        waitForVersionLogRecovery();
        if (clusterRuntime) {
            size_t total = 0;
            if (opts.cluster.runtime.queryConsistencyMode == cluster::QueryConsistencyMode::GLOBAL_SNAPSHOT) {
                cluster::detail::ReconfigurationDeadline budget{opts.cluster.runtime.forwardingTimeoutMs, [this] { return placementClosing(); }};
                auto readers = prepareGlobalQuery(startKey, endKey, true);
                const auto context = cluster::detail::ReconfigurationDeadline::current();
                std::vector<uint64_t> counts(readers.size());
                opts.cluster.runtime.nodeResourceBudget->queryExecutor()->forEach(readers.size(), [&](size_t slot) {
                    cluster::detail::ReconfigurationDeadline scope{context};
                    counts[slot] = readers[slot]->activate(true);
                }, context.deadline);
                for (const auto count : counts) {
                    if (count > SIZE_MAX - total) { throw std::overflow_error("AkkEngine: overflowing cluster count"); }
                    total += count;
                }
                cluster::detail::ReconfigurationDeadline::check();
                return total;
            }
            const auto targets = rangeQueryPlan(startKey);
            const auto context = cluster::detail::ReconfigurationDeadline::current();
            std::vector<uint64_t> counts(targets.size());
            opts.cluster.runtime.nodeResourceBudget->queryExecutor()->forEach(targets.size(), [&](size_t slot) {
                cluster::detail::ReconfigurationDeadline scope{context};
                const auto& target = targets[slot];
                const auto response = sendReadQuery(
                    target.node,
                    openQueryRequest(QueryAction::COUNT, startKey, endKey, 0, target.partitions)
                );
                size_t cursor = 0;
                uint64_t count = 0;
                if (!pullU64(response.value, cursor, count) || cursor != response.value.size()) {
                    throw std::overflow_error("AkkEngine: invalid or overflowing cluster count");
                }
                counts[slot] = count;
            }, context.deadline);
            for (const auto count : counts) {
                if (count > SIZE_MAX - total) { throw std::overflow_error("AkkEngine: overflowing cluster count"); }
                total += count;
            }
            return total;
        }
        std::shared_lock transactionRead{transactionVisibilityMu};
        throwIfBackgroundFailed();
        waitForKeySequenceReadVisibility();

        memtable::MemTable::KeyRange range;
        range.start.assign_range(startKey);
        range.end.assign_range(endKey);
        auto sources = captureStoredRangeRead(
            range, opts.runtime.scanConsistency == AkkEngineOptions::ScanConsistencyMode::PINNED_SNAPSHOT);

        size_t n = 0;
        for (const auto& record : storedScanGenerator(std::move(sources.memory), std::move(sources.disk))) {
            (void)record;
            ++n;
        }

        return n;
    }

    ArenaGenerator<AkkEngine::ScanRecordView> AkkEngine::Impl::scan(
        BufferArena& arena,
        std::span<const uint8_t> startKey,
        std::span<const uint8_t> endKey
    ) {
        auto operation = std::make_shared<OperationGuard>(*this);
        throwIfBackgroundFailed();
        if (clusterRuntime) {
            waitForVersionLogRecovery();
            std::vector<std::unique_ptr<QueryReader>> readers;
            if (opts.cluster.runtime.queryConsistencyMode == cluster::QueryConsistencyMode::GLOBAL_SNAPSHOT) {
                cluster::detail::ReconfigurationDeadline budget{opts.cluster.runtime.forwardingTimeoutMs, [this] { return placementClosing(); }};
                readers = prepareGlobalQuery(startKey, endKey, false);
                const auto context = cluster::detail::ReconfigurationDeadline::current();
                opts.cluster.runtime.nodeResourceBudget->queryExecutor()->forEach(readers.size(), [&](size_t slot) {
                    cluster::detail::ReconfigurationDeadline scope{context};
                    (void)readers[slot]->activate(false);
                }, context.deadline);
                cluster::detail::ReconfigurationDeadline::check();
            }
            else {
                const auto targets = rangeQueryPlan(startKey);
                const auto context = cluster::detail::ReconfigurationDeadline::current();
                readers.resize(targets.size());
                opts.cluster.runtime.nodeResourceBudget->queryExecutor()->forEach(targets.size(), [&](size_t slot) {
                    cluster::detail::ReconfigurationDeadline scope{context};
                    const auto& target = targets[slot];
                    readers[slot] = std::make_unique<QueryReader>(
                            *this,
                            target.node,
                            openQueryRequest(QueryAction::SCAN, startKey, endKey, 0, target.partitions)
                        );
                }, context.deadline);
            }
            for (auto& reader : readers) { reader->operationLifetime = operation; }
            scansTotal.fetch_add(1, std::memory_order_relaxed);
            return ArenaGenerator<ScanRecordView>::withArena(
                arena,
                [&] { return clusterScanGenerator(arena, std::move(readers), std::move(operation)); }
            );
        }
        return scanLocalStorage(arena, startKey, endKey);
    }

    ArenaGenerator<AkkEngine::ScanRecordView> AkkEngine::Impl::scanLocalStorage(
        BufferArena& arena,
        std::span<const uint8_t> startKey,
        std::span<const uint8_t> endKey
    ) {
        auto operation = std::make_shared<OperationGuard>(*this);
        throwIfBackgroundFailed();
        waitForVersionLogRecovery();
        std::shared_lock transactionRead{transactionVisibilityMu};
        throwIfBackgroundFailed();
        waitForKeySequenceReadVisibility();
        scansTotal.fetch_add(1, std::memory_order_relaxed);
        if (usesPartitionRaft()) {
            return ArenaGenerator<ScanRecordView>::withArena(
                arena,
                [&] { return partitionPublicLocalScan({startKey.begin(), startKey.end()}, {endKey.begin(), endKey.end()}, std::move(operation)); }
            );
        }

        auto blobReadPin = blobManager ? blobManager->pinReads() : blob::BlobManager::ReadPin{};
        memtable::MemTable::KeyRange range;
        range.start.assign_range(startKey);
        range.end.assign_range(endKey);
        auto sources = captureStoredRangeRead(
            range, opts.runtime.scanConsistency == AkkEngineOptions::ScanConsistencyMode::PINNED_SNAPSHOT);
        return ArenaGenerator<ScanRecordView>::withArena(
            arena,
            [&arena, mt = std::move(sources.memory), st = std::move(sources.disk), blobManager = blobManager.get(),
                readPin = std::move(blobReadPin), operationLifetime = std::move(operation)]() mutable {
                return scanGenerator(arena, std::move(mt), std::move(st), blobManager, std::move(readPin), std::move(operationLifetime));
            }
        );
    }

    std::optional<std::vector<uint8_t>> AkkEngine::Impl::getAt(std::span<const uint8_t> key, uint64_t atSeq) {
        OperationGuard operation{*this};
        std::shared_lock transactionRead{transactionVisibilityMu, std::defer_lock};
        if (!clusterRuntime) { transactionRead.lock(); }
        throwIfBackgroundFailed();
        return getAtValue(key, atSeq);
    }

    std::optional<std::vector<uint8_t>> AkkEngine::Impl::getAt(std::span<const uint8_t> key, Revision revision) {
        OperationGuard operation{*this};
        if (!clusterRuntime) { throw std::logic_error("AkkEngine: Revision historical read requires cluster mode"); }
        const uint64_t stream = logicalStreamId(key);
        if (revision.streamId != stream) { throw std::invalid_argument("AkkEngine: revision stream does not own key"); }
        return getAtValue(key, revision.seq);
    }

    ArenaGenerator<VersionEntry> AkkEngine::Impl::history(std::span<const uint8_t> key) {
        auto operation = std::make_shared<OperationGuard>(*this);
        throwIfBackgroundFailed();
        waitForVersionLogRecovery();
        std::shared_lock transactionRead{transactionVisibilityMu, std::defer_lock};
        if (!clusterRuntime) { transactionRead.lock(); }
        throwIfBackgroundFailed();
        ArenaGenerator<VersionEntry> local;
        std::unique_ptr<QueryReader> reader;
        if (!clusterRuntime) { local = publicHistoryLocal(key); }
        else {
            const auto target = queryTargets(key, false).front();
            reader = std::make_unique<QueryReader>(*this, target, openQueryRequest(QueryAction::HISTORY, key, {}, 0));
        }
        return ArenaGenerator<VersionEntry>::withOwnedArena(
            4096,
            65536,
            [&] { return publicHistoryGenerator(std::move(local), std::move(reader), std::move(operation)); }
        );
    }
}

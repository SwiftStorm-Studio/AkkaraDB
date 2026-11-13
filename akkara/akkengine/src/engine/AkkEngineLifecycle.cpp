/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/AkkEngineLifecycle.cpp
#include "detail/AkkEngineImpl.hpp"

namespace akkaradb::engine {
    std::unique_ptr<AkkEngine> AkkEngine::Impl::create(AkkEngineOptions options) {
        prepareEngineOptions(options);
        auto engine = std::unique_ptr<AkkEngine>{new AkkEngine()};
        engine->impl_ = std::make_unique<Impl>(std::move(options));
        auto& impl = *engine->impl_;
        impl.initializeStorage();

        if (impl.opts.components.clusterEnabled) { impl.initializeCluster(); }

        if (!impl.writeCoordinator) { impl.writeCoordinator = impl.createWriteCoordinator(); }
        impl.forwardReady.store(true, std::memory_order_release);
        impl.startDistributedRecovery();
        impl.startPlacementRecovery();
        impl.startRollbackRecovery();

        if (impl.opts.components.apiEnabled) {
            if (!server::akkApiServerFactoryAvailable() && !server::loadAkkApiServerBackend(impl.opts.api.serverBackendPath)) {
                const auto detail = server::lastAkkApiServerBackendLoadError();
                throw std::runtime_error(
                    detail.empty()
                        ? "AkkEngine: API server component is enabled, but the API server backend library is not available"
                        : "AkkEngine: API server component is enabled, but the API server backend library is not available: " + detail
                );
            }
            impl.apiServer = server::createAkkApiServer(*engine, impl.opts.api);
            impl.apiServer->start();
        }

        return engine;
    }

    void AkkEngine::Impl::forceSync() {
        OperationGuard operation{*this, false};
        if (!operation) { return; }
        if (usesPartitionRaft()) {
            for (const auto& child : partitionSnapshot()) { if (child) { child->forceSync(); } }
            return;
        }
        if (walWriter) { walWriter->forceSync(); }
        if (versionLog) { versionLog->forceSync(); }
    }

    void AkkEngine::Impl::forceFlush() {
        if (OperationGuard operation{*this, false}) {
            std::shared_lock admission{mutationEpochMu};
            if (usesPartitionRaft()) {
                for (const auto& child : partitionSnapshot()) { if (child) { child->forceFlush(); } }
                return;
            }
            throwIfBackgroundFailed();
            if (memtable) { memtable->forceFlush(); }
        }
    }

    void AkkEngine::Impl::runBlobGc() {
        if (usesPartitionRaft()) {
            OperationGuard operation{*this};
            for (const auto& child : partitionSnapshot()) { if (child) { child->runBlobGc(); } }
            return;
        }
        OperationGuard operation{*this};
        throwIfBackgroundFailed();
        if (!blobManager) { return; }
        if (versionLog) { throw std::runtime_error("AkkEngine: blob GC is disabled while version log is enabled"); }
        runBlobGcIfSafe();
    }

    void AkkEngine::Impl::shutdown() {
        rollbackRecoveryThread.request_stop();
        stripeGcThread.request_stop();
        queryReaper.request_stop();
        placementRecoveryThread.request_stop();
        distributedRecoveryThread.request_stop();
        lifecycleCv.notify_all();
        if (!beginClose()) { return; }

        std::exception_ptr closeFailure;
        if (rollbackRecoveryThread.joinable()) { rollbackRecoveryThread.join(); }
        if (stripeGcThread.joinable()) { stripeGcThread.join(); }
        if (queryReaper.joinable()) { queryReaper.join(); }
        if (placementRecoveryThread.joinable()) { placementRecoveryThread.join(); }
        if (distributedRecoveryThread.joinable()) { distributedRecoveryThread.join(); }
        partitionQueryExecutor.reset();
        batchReadExecutor.reset();
        clearQuerySessions();
        const auto closeStep = [&closeFailure](const auto& operation) {
            try { operation(); }
            catch (...) { if (!closeFailure) { closeFailure = std::current_exception(); } }
        };

        closeStep(
            [&] {
                if (apiServer) {
                    apiServer->close();
                    apiServer.reset();
                }
            }
        );
        closeStep(
            [&] {
                if (clusterRuntime) {
                    clusterRuntime->close();
                    clusterRuntime.reset();
                }
                for (const auto& child : partitionSnapshot()) { if (child) { child->close(); } }
            }
        );
        closeStep(
            [&] {
                std::lock_guard lock{writeMu};
                bool failed = false;
                {
                    std::lock_guard failureLock{transactionFailureMu};
                    failed = static_cast<bool>(transactionFailure);
                }
                // Durable Raft install intent may still name this staging file.
                resetReplicaSnapshotStagingLocked(failed && clusterConfig.usesDataConsensus());
            }
        );
        closeStep([&] { if (localRequestApplications) { localRequestApplications->close(); } });
        closeStep([&] { if (distributedDecisions) { distributedDecisions->close(); } });
        closeStep([&] { if (distributedCompletions) { distributedCompletions->close(); } });
        closeStep([&] { checkpointTransactions(); });
        closeStep([&] { if (memtable && opts.runtime.forceFlushOnClose) { memtable->forceFlush(); } });
        // Flush callbacks borrow SST/Manifest/WAL/Blob state. Even when active
        // tables are not flushed, or forceFlush failed, drain their queued work
        // before destroying any of those dependencies.
        closeStep([&] { if (memtable) { memtable->drainFlushes(); } });
        closeStep(
            [&] {
                if (blobManager) {
                    if (opts.blob.gcOnClose) { runBlobGcIfSafe(); }
                    blobManager->close();
                    blobManager.reset();
                }
            }
        );
        closeStep(
            [&] {
                if (sstManager) {
                    sstManager->shutdown();
                    sstManager.reset();
                }
            }
        );
        closeStep([&] { if (walWriter && opts.runtime.forceSyncOnClose) { walWriter->forceSync(); } });
        closeStep([&] { if (versionLog && opts.runtime.forceSyncOnClose) { versionLog->forceSync(); } });
        closeStep(
            [&] {
                if (walWriter) {
                    walWriter->close();
                    walWriter.reset();
                }
            }
        );
        closeStep(
            [&] {
                if (manifest) {
                    manifest->close();
                    manifest.reset();
                }
            }
        );
        closeStep(
            [&] {
                if (versionLog) {
                    versionLog->close();
                    versionLog.reset();
                }
            }
        );
        closeStep([&] { memtable.reset(); });
        finishClose();
        if (closeFailure) { std::rethrow_exception(closeFailure); }
    }
}

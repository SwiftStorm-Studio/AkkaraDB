/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/AkkEngineTransactions.cpp
#include "detail/AkkEngineImpl.hpp"

namespace akkaradb::engine {
    #include "detail/AkkTransaction.inc"

    AkkEngine::Transaction AkkEngine::Impl::begin(TransactionOptions options) {
    auto operation = std::make_shared<OperationGuard>(*this);
    if (opts.components.clusterEnabled) {
        if (!distributedTransactionsEnabled() || opts.cluster.runtime.transactions.participantOnly) { throw std::logic_error("AkkEngine: enable TWO_PHASE_COMMIT on the Cluster root to begin transactions"); }
        if (options.isolation != TransactionIsolation::SERIALIZABLE && options.isolation != TransactionIsolation::SNAPSHOT_ISOLATION) { throw std::invalid_argument("AkkEngine: invalid transaction isolation"); }
        cluster::detail::ReconfigurationDeadline budget{opts.cluster.runtime.transactions.timeoutMs};
        DistributedId token{}; crypto::secureRandom(token);
        for (unsigned i = 0; i < 8; ++i) { token[i] = static_cast<uint8_t>(nodeId >> (8 * i)); }
        DistributedMessage create{.action = DistributedAction::CREATE, .id = token,
            .argument = opts.transactions.maxLifetimeMs};
        try {
            (void)distributedRpc(create);
            return Transaction{std::make_shared<Transaction::Impl>(*this, options.isolation, std::move(operation), token)};
        } catch (...) {
            const auto failure = std::current_exception();
            try { abortDistributed(token); } catch (...) {}
            std::rethrow_exception(failure);
        }
    }
    if (opts.runtime.sequence.allocation != AkkEngineOptions::SequenceAllocationMode::GLOBAL_ATOMIC) {
        throw std::invalid_argument("AkkEngine: transactions require GLOBAL_ATOMIC sequence allocation");
    }
    if (options.isolation != TransactionIsolation::SERIALIZABLE && options.isolation != TransactionIsolation::SNAPSHOT_ISOLATION) {
        throw std::invalid_argument("AkkEngine: invalid transaction isolation");
    }
    waitForVersionLogRecovery();
    std::unique_lock admission{mutationEpochMu};
    throwIfBackgroundFailed();
    if (transactionHasPersistence() && opts.paths.transactionDir.empty()) { throw std::invalid_argument("AkkEngine: transaction directory is required for persistent transactions"); }
    if ((walWriter && !opts.runtime.recoverWal) || (sstManager && !opts.runtime.recoverSst)) {
        throw std::invalid_argument("AkkEngine: persistent transactions require storage recovery to remain enabled");
    }
    auto snapshot = captureTransactionReadSnapshot(std::move(operation));
    return Transaction{std::make_shared<Transaction::Impl>(*this, options.isolation, std::move(snapshot))};
}

    cluster::ClusterTransactionResult AkkEngine::Impl::transactionOutcome(const cluster::ClusterTransactionId& id) {
    OperationGuard operation{*this};
    if (!distributedTransactionsEnabled() || opts.cluster.runtime.transactions.participantOnly) { throw std::logic_error("AkkEngine: Cluster transactions are disabled"); }
    cluster::detail::ReconfigurationDeadline budget{opts.cluster.runtime.transactions.timeoutMs};
    const auto record = distributedRecord(id); return {record.status, record.finished};
}
}

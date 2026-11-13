/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/detail/AkkEngineWriteCoordinators.inc
// Included inside AkkEngine::Impl by AkkEngineImpl.hpp.
class WriteCoordinator {
    public:
        explicit WriteCoordinator(Impl& engine) : engine_{engine} {}
        virtual ~WriteCoordinator() = default;
        WriteCoordinator(const WriteCoordinator&) = delete;
        WriteCoordinator& operator=(const WriteCoordinator&) = delete;

        virtual void put(std::span<const uint8_t> key, std::span<const uint8_t> value) = 0;
        virtual void putHinted(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t fp64, uint64_t miniKey) = 0;
        virtual void putBatch(std::span<const BatchPutEntry> entries) = 0;
        virtual void remove(std::span<const uint8_t> key) = 0;
        virtual void rollback(std::span<const uint8_t> key, std::span<const uint8_t> value, bool tombstone) = 0;

        virtual cluster::ClusterRequestResult writeWithRequest(
            const cluster::ClusterRequestId& id,
            cluster::ReplOpType op,
            std::span<const uint8_t> key,
            std::span<const uint8_t> value
        ) {
            if (!engine_.clusterRuntime || !engine_.opts.cluster.runtime.requests.enabled) {
                throw std::logic_error("AkkEngine: retry-safe requests are disabled");
            }
            engine_.requireOwnsWriteKey(key);
            return engine_.clusterRuntime->executeRequest(
                id,
                requestFingerprint(op, key, value),
                [&] {
                    if (op == cluster::ReplOpType::PUT) { put(key, value); }
                    else { remove(key); }
                    engine_.forceClusterLocalDurable();
                    return engine_.snapshotSeq();
                }
            );
        }

        virtual void removeHinted(std::span<const uint8_t> key, uint64_t fp64, uint64_t miniKey) = 0;

    protected:
        static std::array<uint8_t, 32> requestFingerprint(cluster::ReplOpType op, std::span<const uint8_t> key, std::span<const uint8_t> value) {
            const auto operation = std::to_underlying(op);
            const std::array<std::span<const uint8_t>, 3> parts{std::span<const uint8_t>{&operation, 1}, key, value};
            return crypto::hash256(parts);
        }

        Impl& engine_;
};

class LocalUnreplicatedWriteCoordinator final : public WriteCoordinator {
    public:
        using WriteCoordinator::WriteCoordinator;

        void put(std::span<const uint8_t> key, std::span<const uint8_t> value) override { engine_.applyLocalPutUnreplicated(key, value); }

        void putHinted(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t fp64, uint64_t miniKey) override {
            engine_.applyLocalPutUnreplicated(key, value, fp64, miniKey);
        }

        void putBatch(std::span<const BatchPutEntry> entries) override { engine_.applyLocalPutBatchUnreplicated(entries); }

        void remove(std::span<const uint8_t> key) override { engine_.applyLocalRemoveUnreplicated(key); }

        void rollback(std::span<const uint8_t> key, std::span<const uint8_t> value, bool tombstone) override {
            if (tombstone) { engine_.applyLocalRemoveUnreplicated(key, 0, 0, vlog::ROLLBACK_NODE); }
            else { engine_.applyLocalPutUnreplicated(key, value, 0, 0, vlog::ROLLBACK_NODE); }
        }

        void removeHinted(std::span<const uint8_t> key, uint64_t fp64, uint64_t miniKey) override {
            engine_.applyLocalRemoveUnreplicated(key, fp64, miniKey);
        }
};

class LocalReplicationWriteCoordinator final : public WriteCoordinator {
    public:
        using WriteCoordinator::WriteCoordinator;

        cluster::ClusterRequestResult writeWithRequest(
            const cluster::ClusterRequestId& id,
            cluster::ReplOpType op,
            std::span<const uint8_t> key,
            std::span<const uint8_t> value
        ) override {
            if (!engine_.opts.cluster.runtime.requests.persistLocalApplication) { return WriteCoordinator::writeWithRequest(id, op, key, value); }
            engine_.requireOwnsWriteKey(key);
            const LocalRequestApplication request{.id = id, .fingerprint = requestFingerprint(op, key, value)};
            return engine_.clusterRuntime->executeRequest(
                id,
                request.fingerprint,
                [&] {
                    AppliedWrite write;
                    engine_.clusterRuntime->executePrimaryWrite(
                        [&] {
                            engine_.requireOwnsWriteKey(key);
                            write = op == cluster::ReplOpType::PUT
                                        ? engine_.applyLocalPut(key, value, 0, 0, 0, &request)
                                        : engine_.applyLocalRemove(key, 0, 0, 0, &request);
                            engine_.forceClusterLocalDurable();
                            crashAtTestPoint("request.local.after_apply");
                            engine_.replicateCommitted(write);
                        }
                    );
                    return write.seq;
                }
            );
        }

        void put(std::span<const uint8_t> key, std::span<const uint8_t> value) override {
            engine_.clusterRuntime->executePrimaryWrite(
                [&] {
                    engine_.requireOwnsWriteKey(key);
                    if (engine_.strictPrimaryAckFailWrite()) {
                        strictPut(key, value);
                        return;
                    }
                    const auto write = engine_.applyLocalPut(key, value);
                    engine_.replicateCommitted(write);
                }
            );
        }

        void putHinted(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t fp64, uint64_t miniKey) override {
            engine_.clusterRuntime->executePrimaryWrite(
                [&] {
                    engine_.requireOwnsWriteKey(key);
                    if (engine_.strictPrimaryAckFailWrite()) {
                        strictPut(key, value, fp64, miniKey);
                        return;
                    }
                    const auto write = engine_.applyLocalPut(key, value, fp64, miniKey);
                    engine_.replicateCommitted(write);
                }
            );
        }

        void putBatch(std::span<const BatchPutEntry> entries) override {
            engine_.clusterRuntime->executePrimaryWrite(
                [&] {
                    engine_.requireOwnsWriteBatch(entries);
                    if (engine_.strictPrimaryAckFailWrite()) {
                        strictPutBatch(entries);
                        return;
                    }
                    const auto writes = engine_.applyLocalPutBatch(entries);
                    for (const auto& write : writes) { engine_.replicateCommitted(write); }
                }
            );
        }

        void remove(std::span<const uint8_t> key) override {
            engine_.clusterRuntime->executePrimaryWrite(
                [&] {
                    engine_.requireOwnsWriteKey(key);
                    if (engine_.strictPrimaryAckFailWrite()) {
                        strictRemove(key);
                        return;
                    }
                    const auto write = engine_.applyLocalRemove(key);
                    engine_.replicateCommitted(write);
                }
            );
        }

        void rollback(std::span<const uint8_t> key, std::span<const uint8_t> value, bool tombstone) override {
            engine_.clusterRuntime->executePrimaryWrite(
                [&] {
                    engine_.requireOwnsWriteKey(key);
                    const auto write = tombstone
                                           ? engine_.applyLocalRemove(key, 0, 0, vlog::ROLLBACK_NODE)
                                           : engine_.applyLocalPut(key, value, 0, 0, vlog::ROLLBACK_NODE);
                    if (engine_.strictPrimaryAckFailWrite()) { engine_.forceClusterLocalDurable(); }
                    engine_.replicateCommitted(write);
                }
            );
        }

        void removeHinted(std::span<const uint8_t> key, uint64_t fp64, uint64_t miniKey) override {
            engine_.clusterRuntime->executePrimaryWrite(
                [&] {
                    engine_.requireOwnsWriteKey(key);
                    if (engine_.strictPrimaryAckFailWrite()) {
                        strictRemove(key, fp64, miniKey);
                        return;
                    }
                    const auto write = engine_.applyLocalRemove(key, fp64, miniKey);
                    engine_.replicateCommitted(write);
                }
            );
        }

    private:
        void strictPut(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t fp64 = 0, uint64_t miniKey = 0) {
            const auto write = engine_.applyLocalPut(key, value, fp64, miniKey);
            engine_.forceClusterLocalDurable();
            engine_.replicateCommitted(write);
        }

        void strictPutBatch(std::span<const BatchPutEntry> entries) {
            if (entries.empty()) { return; }
            const auto writes = engine_.applyLocalPutBatch(entries);
            engine_.forceClusterLocalDurable();
            for (const auto& write : writes) { engine_.replicateCommitted(write); }
        }

        void strictRemove(std::span<const uint8_t> key, uint64_t fp64 = 0, uint64_t miniKey = 0) {
            const auto write = engine_.applyLocalRemove(key, fp64, miniKey);
            engine_.forceClusterLocalDurable();
            engine_.replicateCommitted(write);
        }
};

class StripeWriteCoordinator final : public WriteCoordinator {
    public:
        using WriteCoordinator::WriteCoordinator;

        void put(std::span<const uint8_t> key, std::span<const uint8_t> value) override { engine_.writeStripeValue(key, value); }

        void putHinted(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t, uint64_t) override {
            engine_.writeStripeValue(key, value);
        }

        void putBatch(std::span<const BatchPutEntry> entries) override {
            engine_.requireOwnsWriteBatch(entries);
            for (const auto& entry : entries) { engine_.writeStripeValue(entry.key, entry.value); }
        }

        void remove(std::span<const uint8_t> key) override { engine_.removeStripeValue(key); }

        void rollback(std::span<const uint8_t> key, std::span<const uint8_t> value, bool tombstone) override {
            engine_.writeStripeValue(key, value, tombstone, true);
        }

        void removeHinted(std::span<const uint8_t> key, uint64_t, uint64_t) override { engine_.removeStripeValue(key); }
};

class RaftQuorumWriteCoordinator final : public WriteCoordinator {
    public:
        using WriteCoordinator::WriteCoordinator;

        cluster::ClusterRequestResult writeWithRequest(
            const cluster::ClusterRequestId& id,
            cluster::ReplOpType op,
            std::span<const uint8_t> key,
            std::span<const uint8_t> value
        ) override {
            engine_.requireOwnsWriteKey(key);
            auto admission = engine_.distributedWriteAdmission(key);
            engine_.applyWriteBackpressure();
            const auto fingerprint = requestFingerprint(op, key, value);
            std::optional<uint8_t> preparedFlags;
            const auto submit = [&] {
                return engine_.clusterRuntime->submitRequest(
                    id,
                    fingerprint,
                    [&](uint64_t sequence) {
                        auto mutation = prepareMutation(
                            sequence,
                            op,
                            key,
                            value,
                            op == cluster::ReplOpType::PUT ? MemHdr16::FLAG_NORMAL : MemHdr16::FLAG_TOMBSTONE
                        );
                        preparedFlags = mutation.recordFlags;
                        return mutation;
                    },
                    admission.term
                );
            };
            std::shared_future<cluster::ClusterRequestResult> completion;
            try { completion = submit(); }
            catch (const cluster::ClusterRoutingError&) { throw; }
            catch (const std::invalid_argument&) { throw; }
            catch (const std::exception& error) {
                throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::WRITE_REJECTED, {}, error.what(), id);
            }
            const auto result = completion.get();
            if (preparedFlags && result.status == cluster::ClusterRequestStatus::APPLIED) {
                engine_.recordCommittedRaftMutationStats(op, *preparedFlags);
            }
            return result;
        }

        void put(std::span<const uint8_t> key, std::span<const uint8_t> value) override {
            finish(submit(cluster::ReplOpType::PUT, key, value, MemHdr16::FLAG_NORMAL));
        }

        void putHinted(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t fp64, uint64_t miniKey) override {
            (void)fp64;
            (void)miniKey;
            finish(submit(cluster::ReplOpType::PUT, key, value, MemHdr16::FLAG_NORMAL));
        }

        void putBatch(std::span<const BatchPutEntry> entries) override {
            for (size_t offset = 0; offset < entries.size();) {
                std::vector<Pending> pending;
                const size_t end = std::min(entries.size(), offset + (engine_.distributedTransactionsEnabled() ? 1 : 256));
                std::exception_ptr failure;
                try {
                    for (; offset < end; ++offset) {
                        const auto& [key, value] = entries[offset];
                        pending.push_back(submit(cluster::ReplOpType::PUT, key, value, MemHdr16::FLAG_NORMAL));
                    }
                }
                catch (...) { failure = std::current_exception(); }
                for (auto& item : pending) {
                    try { finish(std::move(item)); }
                    catch (...) { if (!failure) { failure = std::current_exception(); } }
                }
                if (failure) { std::rethrow_exception(failure); }
            }
        }

        void remove(std::span<const uint8_t> key) override { finish(submit(cluster::ReplOpType::REMOVE, key, {}, MemHdr16::FLAG_TOMBSTONE)); }

        void rollback(std::span<const uint8_t> key, std::span<const uint8_t> value, bool tombstone) override {
            finish(
                submit(
                    tombstone ? cluster::ReplOpType::REMOVE : cluster::ReplOpType::PUT,
                    key,
                    value,
                    tombstone ? MemHdr16::FLAG_TOMBSTONE : MemHdr16::FLAG_NORMAL,
                    vlog::ROLLBACK_NODE
                )
            );
        }

        void removeHinted(std::span<const uint8_t> key, uint64_t fp64, uint64_t miniKey) override {
            (void)fp64;
            (void)miniKey;
            finish(submit(cluster::ReplOpType::REMOVE, key, {}, MemHdr16::FLAG_TOMBSTONE));
        }

    private:
        struct Pending {
            cluster::ReplOpType op = cluster::ReplOpType::PUT;
            uint8_t recordFlags = MemHdr16::FLAG_NORMAL;
            std::future<void> completion;
            DistributedWriteAdmission admission;
        };

        Pending submit(
            cluster::ReplOpType op,
            std::span<const uint8_t> key,
            std::span<const uint8_t> value,
            uint8_t flags,
            uint64_t sourceNodeId = 0
        ) {
            engine_.requireOwnsWriteKey(key);
            auto admission = engine_.distributedWriteAdmission(key);
            engine_.applyWriteBackpressure();
            uint8_t preparedFlags = flags;
            auto submission = engine_.clusterRuntime->submitMutation(
                [&](uint64_t sequence) {
                    auto mutation = prepareMutation(sequence, op, key, value, flags, sourceNodeId);
                    preparedFlags = mutation.recordFlags;
                    return mutation;
                },
                admission.term
            );
            return Pending{
                .op = op,
                .recordFlags = preparedFlags,
                .completion = std::move(submission.completion),
                .admission = std::move(admission)
            };
        }

        void finish(Pending pending) {
            pending.completion.get();
            engine_.recordCommittedRaftMutationStats(pending.op, pending.recordFlags);
        }

        [[nodiscard]] cluster::ClusterMutation prepareMutation(
            uint64_t sequence,
            cluster::ReplOpType op,
            std::span<const uint8_t> key,
            std::span<const uint8_t> value,
            uint8_t flags,
            uint64_t sourceNodeId = 0
        ) {
            if (key.size() > UINT16_MAX) { throw std::invalid_argument("AkkEngine: Raft key exceeds storage limit of 65535 bytes"); }
            const size_t storedSize = op == cluster::ReplOpType::PUT && engine_.blobManager && value.size() >= engine_.blobManager->threshold()
                ? blob::BLOB_REF_SIZE : value.size();
            if (storedSize > UINT16_MAX - key.size()) { throw std::invalid_argument("AkkEngine: Raft key and stored value exceed combined storage limit of 65535 bytes"); }
            cluster::ClusterMutation mutation{
                .sourceNodeId = sourceNodeId == 0 ? engine_.nodeId : sourceNodeId,
                .op = op,
                .recordFlags = flags,
                .key = {key.begin(), key.end()},
                .value = {value.begin(), value.end()},
                .blob = std::nullopt,
            };
            if (op == cluster::ReplOpType::PUT) {
                auto prepared = engine_.prepareRaftValue(sequence, value, mutation.recordFlags);
                mutation.value = std::move(prepared.stored);
                mutation.blob = std::move(prepared.blob);
            }
            return mutation;
        }
};

#include "PartitionedRaft.inc"
#include "StripePlacement.inc"

[[nodiscard]] std::unique_ptr<WriteCoordinator> createWriteCoordinator() {
    if (usesPartitionRaft()) { return std::make_unique<PartitionedRaftWriteCoordinator>(*this); }
    if (clusterConfig.usesDataConsensus()) { return std::make_unique<RaftQuorumWriteCoordinator>(*this); }
    if (!clusterRuntime) { return std::make_unique<LocalUnreplicatedWriteCoordinator>(*this); }
    if (clusterReplicationMode == cluster::ReplicationMode::STRIPE) { return std::make_unique<StripeWriteCoordinator>(*this); }
    return std::make_unique<LocalReplicationWriteCoordinator>(*this);
}

std::unique_ptr<WriteCoordinator> writeCoordinator;
std::atomic<bool> forwardReady{false};
std::mutex batchReadExecutorMu;
std::unique_ptr<cluster::detail::BoundedExecutor> batchReadExecutor;

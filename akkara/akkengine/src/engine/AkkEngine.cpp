/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/AkkEngine.cpp
#include "detail/AkkEngineImpl.hpp"

namespace akkaradb::engine {
    AkkEngine::AkkEngine() = default;

    AkkEngine::~AkkEngine() {
        try { close(); }
        catch (...) {}
    }

    std::unique_ptr<AkkEngine> AkkEngine::open(AkkEngineOptions options) {
        return Impl::create(std::move(options));
    }

    cluster::ClusterRequestId AkkEngine::newRequestId(uint64_t retentionMs) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->newRequestId(retentionMs);
    }

    cluster::ClusterRequestResult AkkEngine::putWithRequest(const cluster::ClusterRequestId& id, std::span<const uint8_t> key, std::span<const uint8_t> value) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->putWithRequest(id, key, value);
    }

    cluster::ClusterRequestResult AkkEngine::removeWithRequest(const cluster::ClusterRequestId& id, std::span<const uint8_t> key) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->removeWithRequest(id, key);
    }

    cluster::ClusterRequestResult AkkEngine::queryLocalRequest(const cluster::ClusterRequestId& id) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->queryLocalRequestResult(id);
    }

    cluster::ClusterRequestResult AkkEngine::queryRequest(const cluster::ClusterRequestId& id) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->queryRequest(id);
    }

    cluster::ClusterRequestResult AkkEngine::queryRequest(const cluster::ClusterRequestId& id, std::span<const uint8_t> key) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->queryRequest(id, key);
    }

    void AkkEngine::put(std::span<const uint8_t> key, std::span<const uint8_t> value) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        impl_->put(key, value);
    }

    void AkkEngine::putHinted(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t fp64, uint64_t miniKey) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        impl_->putHinted(key, value, fp64, miniKey);
    }

    void AkkEngine::applyProtocolBulk(std::span<const detail::BulkPutEntry> entries) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        impl_->applyProtocolBulk(entries);
    }

    void AkkEngine::remove(std::span<const uint8_t> key) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        impl_->remove(key);
    }

    void AkkEngine::removeHinted(std::span<const uint8_t> key, uint64_t fp64, uint64_t miniKey) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        impl_->removeHinted(key, fp64, miniKey);
    }

    std::optional<std::vector<uint8_t>> AkkEngine::get(std::span<const uint8_t> key) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->get(key);
    }

    std::vector<AkkEngine::BatchGetResult> AkkEngine::getBatch(std::span<const std::span<const uint8_t>> keys) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->getBatch(keys);
    }

    bool AkkEngine::exists(std::span<const uint8_t> key) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->exists(key);
    }

    bool AkkEngine::getInto(std::span<const uint8_t> key, std::vector<uint8_t>& out) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->getInto(key, out);
    }

    bool AkkEngine::getIntoArena(std::span<const uint8_t> key, BufferArena& arena, std::span<const uint8_t>& out) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->getIntoArena(key, arena, out);
    }

    size_t AkkEngine::count(std::span<const uint8_t> startKey, std::span<const uint8_t> endKey) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->count(startKey, endKey);
    }

    ArenaGenerator<AkkEngine::ScanRecordView> AkkEngine::scan(
        BufferArena& arena,
        std::span<const uint8_t> startKey,
        std::span<const uint8_t> endKey
    ) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->scan(arena, startKey, endKey);
    }

    ArenaGenerator<AkkEngine::ScanRecordView> AkkEngine::scanLocalStorage(
        BufferArena& arena,
        std::span<const uint8_t> startKey,
        std::span<const uint8_t> endKey
    ) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->scanLocalStorage(arena, startKey, endKey);
    }

    std::optional<std::vector<uint8_t>> AkkEngine::getAt(std::span<const uint8_t> key, uint64_t atSeq) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->getAt(key, atSeq);
    }

    std::optional<std::vector<uint8_t>> AkkEngine::getAt(std::span<const uint8_t> key, Revision revision) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->getAt(key, revision);
    }

    ArenaGenerator<VersionEntry> AkkEngine::history(std::span<const uint8_t> key) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->history(key);
    }

    RollbackResult AkkEngine::rollbackTo(uint64_t targetSeq, RollbackOptions options) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->rollbackTo(targetSeq, options);
    }

    RollbackResult AkkEngine::rollbackKey(std::span<const uint8_t> key, uint64_t targetSeq, RollbackOptions options) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->rollbackKey(key, targetSeq, options);
    }

    uint64_t AkkEngine::revisionStreamId(std::span<const uint8_t> key) const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->revisionStreamId(key);
    }

    ClusterCheckpoint AkkEngine::createClusterCheckpoint() const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->createClusterCheckpoint();
    }

    RollbackResult AkkEngine::rollbackKey(std::span<const uint8_t> key, Revision target, RollbackOptions options) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->rollbackKey(key, target, options);
    }

    RollbackResult AkkEngine::rollbackTo(const ClusterCheckpoint& target, RollbackOptions options) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->rollbackTo(target, options);
    }

    void AkkEngine::addClusterVotingNode(const cluster::NodeInfo& node) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        impl_->addClusterVotingNode(node);
    }

    void AkkEngine::addClusterLearner(const cluster::NodeInfo& node) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        impl_->addClusterLearner(node);
    }

    void AkkEngine::transferMirrorAuthorityLeadership(uint64_t targetNodeId) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        impl_->transferMirrorAuthorityLeadership(targetNodeId);
    }

    void AkkEngine::reconfigureRequestAuthority(const std::vector<cluster::NodeInfo>& nodes) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        impl_->reconfigureRequestAuthority(nodes);
    }

    bool AkkEngine::recoverMirrorWrite() {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->recoverMirrorWrite();
    }

    void AkkEngine::promoteClusterLearner(uint64_t nodeId) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        impl_->promoteClusterLearner(nodeId);
    }

    void AkkEngine::removeClusterLearner(uint64_t nodeId) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        impl_->removeClusterLearner(nodeId);
    }

    void AkkEngine::removeClusterVotingNode(uint64_t nodeId) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        impl_->removeClusterVotingNode(nodeId);
    }

    void AkkEngine::transferClusterLeadership(uint64_t targetNodeId) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        impl_->transferClusterLeadership(targetNodeId);
    }

    void AkkEngine::campaignClusterLeadership(uint64_t streamId) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        impl_->campaignClusterLeadership(streamId);
    }

    bool AkkEngine::cancelClusterReconfiguration() {
        if (!impl_) { throw std::logic_error("AkkEngine: engine is closed"); }
        return impl_->cancelClusterReconfiguration();
    }

    void AkkEngine::reconfigureCluster(cluster::ClusterConfig config) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        Impl::OperationGuard operation{*impl_};
        impl_->reconfigureCluster(std::move(config));
    }

    ClusterReconfigurationStatus AkkEngine::clusterReconfigurationStatus() const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->clusterReconfigurationStatus();
    }

    std::vector<ClusterPartitionStats> AkkEngine::clusterPartitionStats() const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->clusterPartitionStats();
    }

    cluster::ClusterAvailability AkkEngine::clusterAvailability() const {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->clusterAvailability();
    }

    void AkkEngine::forceSync() {
        if (!impl_) { return; }
        impl_->forceSync();
    }

    void AkkEngine::forceFlush() {
        if (!impl_) { return; }
        impl_->forceFlush();
    }

    void AkkEngine::runBlobGc() {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        impl_->runBlobGc();
    }

    void AkkEngine::close() {
        if (!impl_) { return; }
        impl_->shutdown();
    }

    AkkEngine::Transaction AkkEngine::begin(TransactionOptions options) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->begin(options);
    }

    cluster::ClusterTransactionResult AkkEngine::transactionOutcome(const cluster::ClusterTransactionId& id) {
        if (!impl_) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->transactionOutcome(id);
    }

    EngineStats AkkEngine::stats() const noexcept {
        if (!impl_) { return {}; }
        return impl_->stats();
    }
}

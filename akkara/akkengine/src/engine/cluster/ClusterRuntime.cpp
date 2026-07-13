/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/cluster/ClusterRuntime.cpp
#include "akk/engine/cluster/ClusterRuntime.hpp"
#include "RaftConsensusRuntime.hpp"

#include <array>
#include <charconv>
#include <cctype>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

namespace akkaradb::engine::cluster {
    namespace {
        std::string lowerAscii(std::string_view value) {
            std::string out;
            out.reserve(value.size());
            for (const char ch : value) { out.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch)))); }
            return out;
        }

        bool parseIpv4(std::string_view host, std::array<uint8_t, 4>& out) noexcept {
            size_t start = 0;
            for (size_t part = 0; part < out.size(); ++part) {
                const size_t dot = host.find('.', start);
                const size_t end = dot == std::string_view::npos ? host.size() : dot;
                if (start == end) { return false; }

                unsigned value = 0;
                const auto* first = host.data() + start;
                const auto* last = host.data() + end;
                const auto result = std::from_chars(first, last, value);
                if (result.ec != std::errc{} || result.ptr != last || value > 255) { return false; }
                out[part] = static_cast<uint8_t>(value);

                if (part + 1 == out.size()) { return dot == std::string_view::npos; }
                if (dot == std::string_view::npos) { return false; }
                start = dot + 1;
            }
            return true;
        }

        bool isLanOrLoopbackHost(std::string_view host) {
            const std::string normalized = lowerAscii(host);
            if (normalized == "localhost") { return true; }

            std::array<uint8_t, 4> ipv4{};
            if (parseIpv4(normalized, ipv4)) {
                if (ipv4[0] == 10) { return true; }
                if (ipv4[0] == 127) { return true; }
                if (ipv4[0] == 169 && ipv4[1] == 254) { return true; }
                if (ipv4[0] == 172 && ipv4[1] >= 16 && ipv4[1] <= 31) { return true; }
                if (ipv4[0] == 192 && ipv4[1] == 168) { return true; }
                return false;
            }

            if (normalized.find(':') != std::string::npos) {
                if (normalized == "::1") { return true; }
                if (normalized.rfind("fc", 0) == 0 || normalized.rfind("fd", 0) == 0) { return true; }
                if (normalized.rfind("fe80:", 0) == 0) { return true; }
            }

            return false;
        }

        void validateTransportScope(const ClusterConfig& config, const ClusterRuntimeOptions& options) {
            if (options.transportMode != TransportMode::PLAIN) { return; }
            for (const auto& node : config.nodes()) {
                if (!isLanOrLoopbackHost(node.host)) {
                    throw std::invalid_argument(
                        "ClusterRuntime: Plain replication transport is only allowed for LAN or loopback node hosts; use Secure for WAN"
                    );
                }
            }
        }

        uint16_t configuredReplicaCount(const ClusterConfig& config, uint64_t selfNodeId) {
            size_t count = 0;
            for (const auto& node : config.nodes()) {
                if (node.nodeId != selfNodeId && node.dataBearing()) { ++count; }
            }
            if (count > UINT16_MAX) { throw std::invalid_argument("ClusterRuntime: too many configured replicas"); }
            return static_cast<uint16_t>(count);
        }

        uint16_t raftReplicaQuorum(const ClusterConfig& config, uint64_t selfNodeId) {
            const auto* self = config.findById(selfNodeId);
            if (self == nullptr || !self->dataBearing()) {
                throw std::invalid_argument("ClusterRuntime: RAFT_QUORUM requires the primary node to be data-bearing");
            }

            size_t dataNodes = 0;
            for (const auto& node : config.nodes()) {
                if (node.dataBearing()) { ++dataNodes; }
            }
            if (dataNodes == 0) { throw std::invalid_argument("ClusterRuntime: RAFT_QUORUM requires data-bearing nodes"); }

            const size_t majority = (dataNodes / 2) + 1;
            const size_t replicaAcks = majority > 0 ? majority - 1 : 0;
            if (replicaAcks > UINT16_MAX) { throw std::invalid_argument("ClusterRuntime: RAFT_QUORUM replica quorum is too large"); }
            return static_cast<uint16_t>(replicaAcks);
        }

        AckPolicy effectiveAckPolicy(const ClusterConfig& config, uint64_t selfNodeId) {
            const auto consistency = config.consistency();
            const auto legacy = config.ackPolicy();
            if (consistency.mode == ConsistencyMode::ASYNC) {
                return AckPolicy{.mode = AckPolicyMode::NONE, .stage = legacy.stage};
            }
            if (consistency.mode == ConsistencyMode::RAFT_QUORUM) {
                const uint16_t quorum = raftReplicaQuorum(config, selfNodeId);
                if (quorum == 0) { return AckPolicy{.mode = AckPolicyMode::NONE, .stage = AckStage::DURABLE}; }
                return AckPolicy{.mode = AckPolicyMode::QUORUM, .stage = AckStage::DURABLE, .quorum = quorum};
            }
            switch (consistency.writeConsistency) {
                case WriteConsistency::LEGACY_ACK_POLICY: return legacy;
                case WriteConsistency::LOCAL: return AckPolicy{.mode = AckPolicyMode::NONE, .stage = legacy.stage};
                case WriteConsistency::ONE_REPLICA:
                    return AckPolicy{.mode = AckPolicyMode::QUORUM, .stage = legacy.stage, .quorum = 1};
                case WriteConsistency::QUORUM:
                    return AckPolicy{.mode = AckPolicyMode::QUORUM, .stage = legacy.stage, .quorum = legacy.quorum};
                case WriteConsistency::ALL_CONFIGURED:
                    return AckPolicy{.mode = AckPolicyMode::ALL_TARGETS, .stage = legacy.stage};
            }
            throw std::invalid_argument("ClusterRuntime: invalid write consistency");
        }

        ConsistencyOptions effectiveConsistency(const ClusterConfig& config) {
            auto consistency = config.consistency();
            if (consistency.mode == ConsistencyMode::RAFT_QUORUM) {
                consistency.ackTimeoutAction = AckTimeoutAction::FAIL_WRITE;
                consistency.readConsistency = ReadConsistency::QUORUM;
            }
            return consistency;
        }
    } // namespace

    class ClusterRuntime::Impl {
        public:
            Impl(
                std::filesystem::path dbDir,
                ClusterConfig config,
                uint64_t selfNodeId,
                ClusterEngineCallbacks callbacks,
                ClusterRuntimeOptions runtimeOptions
            )
                : config_{std::move(config)},
                  router_{config_},
                  selfNodeId_{selfNodeId},
                  callbacks_{std::move(callbacks)},
                  runtimeOptions_{std::move(runtimeOptions)} {
                if (runtimeOptions_.transportMode == TransportMode::SECURE && runtimeOptions_.secure.identitySeedPath.empty() && !dbDir.
                    empty()) { runtimeOptions_.secure.identitySeedPath = dbDir / "cluster.identity"; }
                validateTransportScope(config_, runtimeOptions_);
                if (config_.consistency().mode == ConsistencyMode::RAFT_QUORUM) {
                    raftRuntime_ = RaftConsensusRuntime::create(dbDir, config_, selfNodeId_, callbacks_, runtimeOptions_);
                    return;
                }
                manager_ = ClusterManager::create(dbDir, config_, selfNodeId, runtimeOptions);
                effectiveAckPolicy_ = effectiveAckPolicy(config_, selfNodeId_);
                effectiveConsistency_ = effectiveConsistency(config_);
                configuredReplicaCount_ = configuredReplicaCount(config_, selfNodeId_);
                if (effectiveAckPolicy_.mode == AckPolicyMode::QUORUM && effectiveAckPolicy_.quorum > configuredReplicaCount_) {
                    throw std::invalid_argument("ClusterRuntime: write quorum exceeds configured replica count");
                }
                manager_->setRoleChangeCallback(
                    [this](NodeRole role) {
                        installRole(role);
                        if (callbacks_.roleChange) { callbacks_.roleChange(role); }
                    }
                );
            }

            ~Impl() { close(); }

            void start() {
                if (raftRuntime_) {
                    raftRuntime_->start();
                    return;
                }
                {
                    std::lock_guard lock{mutex_};
                    if (started_) { return; }
                    started_ = true;
                }
                manager_->start();
                installRole(manager_->role());
            }

            void close() {
                if (raftRuntime_) {
                    raftRuntime_->close();
                    return;
                }
                std::lock_guard lock{mutex_};
                stopReplication();
                manager_->close();
                started_ = false;
            }

            NodeRole role() const noexcept { return raftRuntime_ ? raftRuntime_->role() : manager_->role(); }

            const ClusterRouter& router() const noexcept { return raftRuntime_ ? raftRuntime_->router() : router_; }

            void shipEntry(
                uint64_t seq,
                ReplOpType op,
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint8_t recordFlags,
                uint64_t sourceNodeId
            ) {
                if (raftRuntime_) {
                    raftRuntime_->shipEntry(seq, op, key, value, recordFlags, sourceNodeId);
                    return;
                }
                std::lock_guard lock{mutex_};
                if (server_) { server_->shipEntry(seq, op, key, value, recordFlags, sourceNodeId); }
            }

            void shipBlob(uint64_t seq, uint64_t blobId, std::span<const uint8_t> content) {
                if (raftRuntime_) {
                    raftRuntime_->shipBlob(seq, blobId, content);
                    return;
                }
                std::lock_guard lock{mutex_};
                if (server_) { server_->shipBlob(seq, blobId, content); }
            }

        private:
            void installRole(NodeRole role) {
                std::lock_guard lock{mutex_};
                stopReplication();

                if (role == NodeRole::PRIMARY) {
                    const auto* self = config_.findById(selfNodeId_);
                    if (!self && !config_.isStandalone()) { throw std::runtime_error("ClusterRuntime: self node is missing from config"); }
                    const uint16_t replPort = self ? self->replPort : 0;
                    ReplicationServer::HistoryProvider historyProvider;
                    if (callbacks_.getEntries) {
                        historyProvider = [getEntries = callbacks_.getEntries](uint64_t afterSeq, uint64_t throughSeq)
                            -> std::optional<std::vector<ReplEntry>> {
                                const auto history = getEntries(afterSeq, throughSeq);
                                if (!history) { return std::nullopt; }
                                std::vector<ReplEntry> entries;
                                entries.reserve(history->size());
                                for (const auto& entry : *history) {
                                    entries.push_back(ReplEntry{
                                        .seq = entry.seq,
                                        .sourceNodeId = entry.sourceNodeId,
                                        .op = static_cast<ReplOpType>(entry.op),
                                        .recordFlags = entry.recordFlags,
                                        .key = entry.key,
                                        .value = entry.value,
                                    });
                                }
                                return entries;
                            };
                    }
                    ReplicationServer::SnapshotProvider snapshotProvider;
                    if (callbacks_.exportSnapshot) {
                        snapshotProvider = [exportSnapshot = callbacks_.exportSnapshot]() -> std::optional<ReplicationServer::Snapshot> {
                            const auto source = exportSnapshot();
                            if (!source) { return std::nullopt; }
                            ReplicationServer::Snapshot snapshot;
                            snapshot.seq = source->seq;
                            snapshot.entries.reserve(source->entries.size());
                            for (const auto& entry : source->entries) {
                                snapshot.entries.push_back(ReplSnapshotEntry{.key = entry.key, .value = entry.value});
                            }
                            return snapshot;
                        };
                    }
                    server_ = ReplicationServer::create(
                        replPort,
                        selfNodeId_,
                        callbacks_.getCurrentSeq,
                        effectiveAckPolicy_,
                        effectiveConsistency_,
                        configuredReplicaCount_,
                        runtimeOptions_,
                        std::move(historyProvider),
                        std::move(snapshotProvider)
                    );
                    server_->start();
                }
                else if (role == NodeRole::REPLICA) {
                    auto clientOptions = runtimeOptions_;
                    clientOptions.secure.expectedPrimaryNodeId = manager_->primaryNodeId();
                    client_ = ReplicationClient::create(
                        manager_->primaryHost(),
                        manager_->primaryReplPort(),
                        selfNodeId_,
                        callbacks_.getLastSeq,
                        effectiveAckPolicy_,
                        std::move(clientOptions)
                    );
                    client_->setApplyCallback(callbacks_.apply);
                    client_->setSnapshotCallbacks(callbacks_.beginSnapshot, callbacks_.applySnapshotEntry, callbacks_.finishSnapshot);
                    client_->setForceDurableCallback(callbacks_.forceDurable);
                    client_->setBlobCallback(callbacks_.applyBlob);
                    client_->start();
                }
            }

            void stopReplication() {
                if (client_) {
                    client_->close();
                    client_.reset();
                }
                if (server_) {
                    server_->close();
                    server_.reset();
                }
            }

            ClusterConfig config_;
            ClusterRouter router_;
            std::unique_ptr<ClusterManager> manager_;
            std::unique_ptr<RaftConsensusRuntime> raftRuntime_;
            uint64_t selfNodeId_;
            ClusterEngineCallbacks callbacks_;
            ClusterRuntimeOptions runtimeOptions_;
            AckPolicy effectiveAckPolicy_;
            ConsistencyOptions effectiveConsistency_;
            uint16_t configuredReplicaCount_ = 0;

            mutable std::mutex mutex_;
            bool started_ = false;
            std::unique_ptr<ReplicationServer> server_;
            std::unique_ptr<ReplicationClient> client_;
    };

    std::unique_ptr<ClusterRuntime> ClusterRuntime::create(
        std::filesystem::path dbDir,
        ClusterConfig config,
        uint64_t selfNodeId,
        ClusterEngineCallbacks callbacks,
        ClusterRuntimeOptions runtimeOptions
    ) {
        return std::unique_ptr<ClusterRuntime>(
            new ClusterRuntime(
                std::make_unique<Impl>(std::move(dbDir), std::move(config), selfNodeId, std::move(callbacks), std::move(runtimeOptions))
            )
        );
    }

    ClusterRuntime::ClusterRuntime(std::unique_ptr<Impl> impl) : impl_{std::move(impl)} {}

    ClusterRuntime::~ClusterRuntime() = default;

    void ClusterRuntime::start() { impl_->start(); }

    void ClusterRuntime::close() { impl_->close(); }

    NodeRole ClusterRuntime::role() const noexcept { return impl_->role(); }

    const ClusterRouter& ClusterRuntime::router() const noexcept { return impl_->router(); }

    void ClusterRuntime::shipEntry(
        uint64_t seq,
        ReplOpType op,
        std::span<const uint8_t> key,
        std::span<const uint8_t> value,
        uint8_t recordFlags,
        uint64_t sourceNodeId
    ) { impl_->shipEntry(seq, op, key, value, recordFlags, sourceNodeId); }

    void ClusterRuntime::shipBlob(uint64_t seq, uint64_t blobId, std::span<const uint8_t> content) {
        impl_->shipBlob(seq, blobId, content);
    }
} // namespace akkaradb::engine::cluster

extern "C" AKKARADB_CLUSTER_RUNTIME_API bool akkaradb_cluster_register() noexcept {
    return akkaradb::engine::cluster::registerClusterRuntimeFactory(
        [](
        std::filesystem::path dbDir,
        akkaradb::engine::cluster::ClusterConfig config,
        uint64_t selfNodeId,
        akkaradb::engine::cluster::ClusterEngineCallbacks callbacks,
        akkaradb::engine::cluster::ClusterRuntimeOptions runtimeOptions
    ) -> std::unique_ptr<akkaradb::engine::cluster::IClusterRuntime> {
            return akkaradb::engine::cluster::ClusterRuntime::create(
                std::move(dbDir),
                std::move(config),
                selfNodeId,
                std::move(callbacks),
                std::move(runtimeOptions)
            );
        }
    );
}

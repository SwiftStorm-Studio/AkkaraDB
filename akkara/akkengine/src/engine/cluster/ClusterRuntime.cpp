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
#include "akk/engine/cluster/detail/RaftConsensusRuntime.hpp"
#include "akk/cpu/CRC32C.hpp"

#include <array>
#include <atomic>
#include <charconv>
#include <cctype>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

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
            for (const auto& node : config.nodes()) { if (node.nodeId != selfNodeId && node.dataBearing()) { ++count; } }
            if (count > UINT16_MAX) { throw std::invalid_argument("ClusterRuntime: too many configured replicas"); }
            return static_cast<uint16_t>(count);
        }

        uint16_t raftReplicaQuorum(const ClusterConfig& config, uint64_t selfNodeId) {
            const auto* self = config.findById(selfNodeId);
            if (self == nullptr || !self->dataBearing()) {
                throw std::invalid_argument("ClusterRuntime: RAFT_QUORUM requires the primary node to be data-bearing");
            }

            size_t dataNodes = 0;
            for (const auto& node : config.nodes()) { if (node.dataBearing()) { ++dataNodes; } }
            if (dataNodes == 0) { throw std::invalid_argument("ClusterRuntime: RAFT_QUORUM requires data-bearing nodes"); }

            const size_t majority = (dataNodes / 2) + 1;
            const size_t replicaAcks = majority > 0 ? majority - 1 : 0;
            if (replicaAcks > UINT16_MAX) { throw std::invalid_argument("ClusterRuntime: RAFT_QUORUM replica quorum is too large"); }
            return static_cast<uint16_t>(replicaAcks);
        }

        AckPolicy effectiveAckPolicy(const ClusterConfig& config, uint64_t selfNodeId) {
            const auto consistency = config.consistency();
            const auto legacy = config.ackPolicy();
            if (consistency.mode == ConsistencyMode::ASYNC) { return AckPolicy{.mode = AckPolicyMode::NONE, .stage = legacy.stage}; }
            if (consistency.mode == ConsistencyMode::RAFT_QUORUM) {
                const uint16_t quorum = raftReplicaQuorum(config, selfNodeId);
                if (quorum == 0) { return AckPolicy{.mode = AckPolicyMode::NONE, .stage = AckStage::DURABLE}; }
                return AckPolicy{.mode = AckPolicyMode::QUORUM, .stage = AckStage::DURABLE, .quorum = quorum};
            }
            switch (consistency.writeConsistency) {
                case WriteConsistency::LEGACY_ACK_POLICY: return legacy;
                case WriteConsistency::LOCAL: return AckPolicy{.mode = AckPolicyMode::NONE, .stage = legacy.stage};
                case WriteConsistency::ONE_REPLICA: return AckPolicy{.mode = AckPolicyMode::QUORUM, .stage = legacy.stage, .quorum = 1};
                case WriteConsistency::QUORUM: return AckPolicy{
                        .mode = AckPolicyMode::QUORUM,
                        .stage = legacy.stage,
                        .quorum = legacy.quorum
                    };
                case WriteConsistency::ALL_CONFIGURED: return AckPolicy{.mode = AckPolicyMode::ALL_TARGETS, .stage = legacy.stage};
            }
            throw std::invalid_argument("ClusterRuntime: invalid write consistency");
        }

        ConsistencyOptions effectiveConsistency(const ClusterConfig& config) {
            auto consistency = config.consistency();
            if (consistency.mode == ConsistencyMode::RAFT_QUORUM) { consistency.ackTimeoutAction = AckTimeoutAction::FAIL_WRITE; }
            return consistency;
        }

        std::vector<uint64_t> configuredReplicaNodeIds(const ClusterConfig& config, uint64_t selfNodeId) {
            std::vector<uint64_t> out;
            for (const auto& node : config.nodes()) { if (node.nodeId != selfNodeId && node.dataBearing()) { out.push_back(node.nodeId); } }
            return out;
        }

        uint64_t randomNonZeroU64() {
            std::random_device rd;
            std::mt19937_64 rng{
                (static_cast<uint64_t>(rd()) << 32) ^ static_cast<uint64_t>(rd()) ^ static_cast<uint64_t>(std::chrono::steady_clock::now().
                    time_since_epoch().count())
            };
            uint64_t value = 0;
            while (value == 0) { value = rng(); }
            return value;
        }

        struct GroupState {
            uint64_t groupId = 0;
            uint64_t primaryNodeId = 0;
            uint64_t groupEpoch = 1;
        };

        void writeLe32(std::vector<uint8_t>& out, uint32_t value) {
            for (size_t i = 0; i < 4; ++i) { out.push_back(static_cast<uint8_t>(value >> (i * 8))); }
        }

        void writeLe64(std::vector<uint8_t>& out, uint64_t value) {
            for (size_t i = 0; i < 8; ++i) { out.push_back(static_cast<uint8_t>(value >> (i * 8))); }
        }

        uint32_t readLe32(std::span<const uint8_t> in, size_t off) {
            return static_cast<uint32_t>(in[off]) | (static_cast<uint32_t>(in[off + 1]) << 8) | (static_cast<uint32_t>(in[off + 2]) << 16) |
                (static_cast<uint32_t>(in[off + 3]) << 24);
        }

        uint64_t readLe64(std::span<const uint8_t> in, size_t off) {
            uint64_t out = 0;
            for (size_t i = 0; i < 8; ++i) { out |= static_cast<uint64_t>(in[off + i]) << (i * 8); }
            return out;
        }

        void writeLe32At(std::vector<uint8_t>& out, size_t off, uint32_t value) {
            for (size_t i = 0; i < 4; ++i) { out[off + i] = static_cast<uint8_t>(value >> (i * 8)); }
        }

        uint32_t crcWithZeroedField(std::vector<uint8_t> bytes, size_t crcOffset) {
            if (crcOffset + 4 > bytes.size()) { throw std::runtime_error("ClusterRuntime: invalid CRC field"); }
            writeLe32At(bytes, crcOffset, 0);
            return cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size());
        }

        std::filesystem::path corruptBackupPath(const std::filesystem::path& path) {
            for (uint32_t i = 0; i < 10000; ++i) {
                auto candidate = path;
                candidate += i == 0 ? ".corrupt" : ".corrupt." + std::to_string(i);
                if (!std::filesystem::exists(candidate)) { return candidate; }
            }
            throw std::runtime_error("ClusterRuntime: cannot allocate corrupt state backup path");
        }

        bool handleCorruptStateFile(
            const std::filesystem::path& path,
            CorruptClusterStateAction action,
            const char* context,
            const std::exception& cause
        ) {
            if (action == CorruptClusterStateAction::FAIL_STARTUP) { throw std::runtime_error(std::string{context} + ": " + cause.what()); }
            std::error_code ec;
            if (action == CorruptClusterStateAction::BACKUP_AND_RECREATE) {
                std::filesystem::rename(path, corruptBackupPath(path), ec);
                if (ec) { throw std::runtime_error(std::string{context} + ": cannot back up corrupt state: " + ec.message()); }
                return true;
            }
            if (action == CorruptClusterStateAction::DELETE_AND_RECREATE) {
                std::filesystem::remove(path, ec);
                if (ec) { throw std::runtime_error(std::string{context} + ": cannot delete corrupt state: " + ec.message()); }
                return true;
            }
            throw std::runtime_error(std::string{context} + ": invalid corrupt state action");
        }

        void syncFile(const std::filesystem::path& path, const char* context) {
            #ifdef _WIN32
            const int fd = _wopen(path.c_str(), _O_RDWR | _O_BINARY);
            if (fd < 0) { throw std::runtime_error(std::string{context} + ": cannot reopen temp state for sync"); }
            const int rc = _commit(fd);
            const int closeRc = _close(fd);
            if (rc != 0 || closeRc != 0) { throw std::runtime_error(std::string{context} + ": temp state sync failed"); }
            #else
            const int fd = ::open(path.c_str(), O_RDONLY); if (fd < 0) {
                throw std::runtime_error(std::string{context} + ": cannot reopen temp state for sync");
            } const int rc = ::fsync(fd); const int closeRc = ::close(fd); if (rc != 0 || closeRc != 0) {
                throw std::runtime_error(std::string{context} + ": temp state sync failed");
            }
            #endif
        }

        std::filesystem::path makeTempPath(const std::filesystem::path& path, const char* context) {
            static std::atomic<uint64_t> sequence{0};
            const auto parent = path.parent_path();
            const auto stem = path.filename().string();
            #ifdef _WIN32
            const auto pid = static_cast<uint64_t>(::GetCurrentProcessId());
            #else
            const auto pid = static_cast<uint64_t>(::getpid());
            #endif
            for (uint32_t attempt = 0; attempt < 1024; ++attempt) {
                const auto suffix = ".tmp." + std::to_string(pid) + "." + std::to_string(sequence.fetch_add(1)) + "." + std::to_string(
                    attempt
                );
                auto candidate = parent / (stem + suffix);
                if (!std::filesystem::exists(candidate)) { return candidate; }
            }
            throw std::runtime_error(std::string{context} + ": cannot allocate temp state file name");
        }

        #ifndef _WIN32
        void syncParentDirectory(const std::filesystem::path& path, const char* context) {
            const auto parent = path.parent_path().empty() ? std::filesystem::path{"."} : path.parent_path();
            int flags = O_RDONLY;
        #ifdef O_DIRECTORY
        flags|= O_DIRECTORY;
        #endif
        const int fd = ::open(parent.c_str(), flags);if (fd<0) {
            throw std::runtime_error(std::string{context} + ": cannot open parent directory for sync");
        } const int rc = ::fsync(fd); const int closeRc = ::close(fd);if (rc!= 0 || closeRc
!= 0) { throw std::runtime_error(std::string{context} + ": parent directory sync failed"); }
        }
        #endif

        void replaceFileAtomically(const std::filesystem::path& tmp, const std::filesystem::path& path, const char* context) {
            #ifdef _WIN32
            if (!::MoveFileExW(tmp.wstring().c_str(), path.wstring().c_str(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
                throw std::runtime_error(std::string{context} + ": atomic cluster group state replace failed");
            }
            #else
            std::filesystem::rename(tmp, path); syncParentDirectory(path, context);
            #endif
        }

        std::optional<GroupState> loadGroupState(
            const std::filesystem::path& path,
            CorruptClusterStateAction corruptAction,
            const char* context
        ) {
            if (path.empty() || !std::filesystem::exists(path)) { return std::nullopt; }
            try {
                std::ifstream in(path, std::ios::binary);
                if (!in) { throw std::runtime_error("cannot open state file"); }
                std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
                constexpr size_t expectedSize = 5 + 8 + 8 + 8 + 4;
                constexpr size_t crcOffset = expectedSize - 4;
                if (bytes.size() != expectedSize) { throw std::runtime_error("invalid cluster group state size"); }
                if (std::string_view{reinterpret_cast<const char*>(bytes.data()), 5} != "AKCG2") {
                    throw std::runtime_error("bad cluster group state magic");
                }
                if (readLe32(bytes, crcOffset) != crcWithZeroedField(bytes, crcOffset)) {
                    throw std::runtime_error("cluster group state CRC mismatch");
                }
                return GroupState{.groupId = readLe64(bytes, 5), .primaryNodeId = readLe64(bytes, 13), .groupEpoch = readLe64(bytes, 21),};
            }
            catch (const std::exception& ex) {
                (void)handleCorruptStateFile(path, corruptAction, context, ex);
                return std::nullopt;
            }
        }

        void saveGroupState(const std::filesystem::path& path, GroupState state, const char* context) {
            if (path.empty()) { return; }
            if (path.has_parent_path()) { std::filesystem::create_directories(path.parent_path()); }
            std::vector<uint8_t> bytes;
            bytes.insert(bytes.end(), {'A', 'K', 'C', 'G', '2'});
            writeLe64(bytes, state.groupId);
            writeLe64(bytes, state.primaryNodeId);
            writeLe64(bytes, state.groupEpoch);
            const size_t crcOffset = bytes.size();
            writeLe32(bytes, 0);
            writeLe32At(bytes, crcOffset, crcWithZeroedField(bytes, crcOffset));

            const auto tmpPath = makeTempPath(path, context);
            {
                std::ofstream out(tmpPath, std::ios::binary | std::ios::trunc);
                if (!out) { throw std::runtime_error(std::string{context} + ": cannot create cluster group state"); }
                out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
                out.flush();
                if (!out) { throw std::runtime_error(std::string{context} + ": cluster group state write failed"); }
            }
            syncFile(tmpPath, context);
            replaceFileAtomically(tmpPath, path, context);
        }

        GroupState loadOrCreatePrimaryGroup(
            const std::filesystem::path& path,
            uint64_t selfNodeId,
            uint64_t requestedGroupId,
            uint64_t requestedEpoch,
            CorruptClusterStateAction corruptAction
        ) {
            if (!path.empty() && std::filesystem::exists(path)) {
                const auto loaded = loadGroupState(path, corruptAction, "ClusterRuntime");
                if (!loaded) { return loadOrCreatePrimaryGroup(path, selfNodeId, requestedGroupId, requestedEpoch, corruptAction); }
                const auto state = *loaded;
                if (state.primaryNodeId != selfNodeId) {
                    throw std::runtime_error("ClusterRuntime: cluster group state belongs to a different primary");
                }
                if (requestedGroupId != 0 && requestedGroupId != state.groupId) {
                    throw std::runtime_error("ClusterRuntime: requested cluster group id conflicts with persisted group state");
                }
                if (requestedEpoch != 0 && requestedEpoch != state.groupEpoch) {
                    throw std::runtime_error("ClusterRuntime: requested cluster group epoch conflicts with persisted group state");
                }
                return state;
            }

            GroupState state{
                .groupId = requestedGroupId != 0 ? requestedGroupId : randomNonZeroU64(),
                .primaryNodeId = selfNodeId,
                .groupEpoch = requestedEpoch != 0 ? requestedEpoch : 1,
            };
            saveGroupState(path, state, "ClusterRuntime");
            return state;
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
                if (runtimeOptions_.clusterMembershipPath.empty() && !dbDir.empty() && config_.consistency().mode !=
                    ConsistencyMode::RAFT_QUORUM) { runtimeOptions_.clusterMembershipPath = dbDir / "cluster.membership"; }
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

            std::vector<NodeInfo> activeNodes() const { return raftRuntime_ ? raftRuntime_->activeNodes() : manager_->activeNodes(); }

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

            void addRaftVotingNode(const NodeInfo& node) {
                if (!raftRuntime_) { throw std::runtime_error("ClusterRuntime: online Raft membership change requires RAFT_QUORUM"); }
                raftRuntime_->addVotingNode(node);
            }

            void removeRaftVotingNode(uint64_t nodeId) {
                if (!raftRuntime_) { throw std::runtime_error("ClusterRuntime: online Raft membership change requires RAFT_QUORUM"); }
                raftRuntime_->removeVotingNode(nodeId);
            }

            void transferRaftLeadership(uint64_t targetNodeId) {
                if (!raftRuntime_) { throw std::runtime_error("ClusterRuntime: Raft leader transfer requires RAFT_QUORUM"); }
                raftRuntime_->transferLeadership(targetNodeId);
            }

        private:
            void installRole(NodeRole role) {
                std::lock_guard lock{mutex_};
                stopReplication();

                if (role == NodeRole::PRIMARY) {
                    const auto* self = config_.findById(selfNodeId_);
                    if (!self && !config_.isStandalone()) { throw std::runtime_error("ClusterRuntime: self node is missing from config"); }
                    const auto groupState = loadOrCreatePrimaryGroup(
                        runtimeOptions_.clusterMembershipPath,
                        selfNodeId_,
                        runtimeOptions_.clusterGroupId,
                        runtimeOptions_.clusterGroupEpoch,
                        runtimeOptions_.corruptStateAction
                    );
                    runtimeOptions_.clusterGroupId = groupState.groupId;
                    runtimeOptions_.clusterGroupEpoch = groupState.groupEpoch;
                    const uint16_t replPort = self ? self->replPort : 0;
                    ReplicationServer::HistoryProvider historyProvider;
                    if (callbacks_.getEntries) {
                        historyProvider = [getEntries = callbacks_.getEntries](
                            uint64_t afterSeq,
                            uint64_t throughSeq
                        ) -> std::optional<std::vector<ReplEntry>> {
                                const auto history = getEntries(afterSeq, throughSeq);
                                if (!history) { return std::nullopt; }
                                std::vector<ReplEntry> entries;
                                entries.reserve(history->size());
                                for (const auto& entry : *history) {
                                    entries.push_back(
                                        ReplEntry{
                                            .seq = entry.seq,
                                            .sourceNodeId = entry.sourceNodeId,
                                            .op = static_cast<ReplOpType>(entry.op),
                                            .recordFlags = entry.recordFlags,
                                            .key = entry.key,
                                            .value = entry.value,
                                        }
                                    );
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
                        configuredReplicaNodeIds(config_, selfNodeId_),
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
                    client_->setSnapshotCallbacks(
                        callbacks_.beginSnapshot,
                        callbacks_.beginSnapshotEntry,
                        callbacks_.appendSnapshotEntryChunk,
                        callbacks_.finishSnapshotEntry,
                        callbacks_.finishSnapshot
                    );
                    client_->setForceDurableCallback(callbacks_.forceDurable);
                    client_->setBlobCallbacks(callbacks_.beginBlob, callbacks_.appendBlobChunk, callbacks_.finishBlob);
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

    std::vector<NodeInfo> ClusterRuntime::activeNodes() const { return impl_->activeNodes(); }

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

    void ClusterRuntime::addRaftVotingNode(const NodeInfo& node) { impl_->addRaftVotingNode(node); }

    void ClusterRuntime::removeRaftVotingNode(uint64_t nodeId) { impl_->removeRaftVotingNode(nodeId); }

    void ClusterRuntime::transferRaftLeadership(uint64_t targetNodeId) { impl_->transferRaftLeadership(targetNodeId); }
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

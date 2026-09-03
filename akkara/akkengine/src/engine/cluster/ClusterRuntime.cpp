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
#include <algorithm>
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
#include <unordered_map>
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

        void validateRuntimePlacement(const ClusterConfig& config) {
            if (config.isStandalone() || config.mode() == ReplicationMode::MIRROR || config.mode() == ReplicationMode::PARTITIONED ||
                config.mode() == ReplicationMode::STRIPE) {
                return;
            }
            throw std::invalid_argument("ClusterRuntime: invalid native placement mode");
        }

        void validateRuntimeOptions(const ClusterConfig& config, const ClusterRuntimeOptions& options) {
            (void)config;
            if (options.readMode != ClusterReadMode::LOCAL_STALE_OK && options.readMode != ClusterReadMode::OWNER_ONLY &&
                options.readMode != ClusterReadMode::OWNER_LINEARIZABLE) {
                throw std::invalid_argument("ClusterRuntime: invalid read mode");
            }
            if (options.stripeWriteCommitMode != StripeWriteCommitMode::ALL_SHARDS) {
                throw std::invalid_argument("ClusterRuntime: invalid or unsafe STRIPE write commit mode");
            }
            if (options.stripeReadCoordinatorMode != StripeReadCoordinatorMode::OWNER && options.stripeReadCoordinatorMode !=
                StripeReadCoordinatorMode::LOCAL_COORDINATOR) {
                throw std::invalid_argument("ClusterRuntime: invalid STRIPE read coordinator mode");
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

        struct PeerProgressState {
            uint64_t groupId = 0;
            uint64_t groupEpoch = 0;
            uint64_t peerNodeId = 0;
            uint64_t lastSeq = 0;
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

        std::optional<PeerProgressState> loadPeerProgressState(
            const std::filesystem::path& path,
            CorruptClusterStateAction corruptAction
        ) {
            if (path.empty() || !std::filesystem::exists(path)) { return std::nullopt; }
            try {
                std::ifstream in(path, std::ios::binary);
                if (!in) { throw std::runtime_error("cannot open peer progress state"); }
                std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
                constexpr size_t expectedSize = 5 + 8 + 8 + 8 + 8 + 4;
                constexpr size_t crcOffset = expectedSize - 4;
                if (bytes.size() != expectedSize) { throw std::runtime_error("invalid peer progress state size"); }
                if (std::string_view{reinterpret_cast<const char*>(bytes.data()), 5} != "AKCP1") {
                    throw std::runtime_error("bad peer progress state magic");
                }
                if (readLe32(bytes, crcOffset) != crcWithZeroedField(bytes, crcOffset)) {
                    throw std::runtime_error("peer progress state CRC mismatch");
                }
                return PeerProgressState{
                    .groupId = readLe64(bytes, 5),
                    .groupEpoch = readLe64(bytes, 13),
                    .peerNodeId = readLe64(bytes, 21),
                    .lastSeq = readLe64(bytes, 29),
                };
            }
            catch (const std::exception& ex) {
                (void)handleCorruptStateFile(path, corruptAction, "ClusterRuntime peer progress", ex);
                return std::nullopt;
            }
        }

        void savePeerProgressState(const std::filesystem::path& path, PeerProgressState state) {
            if (path.empty()) { return; }
            if (path.has_parent_path()) { std::filesystem::create_directories(path.parent_path()); }
            std::vector<uint8_t> bytes;
            bytes.insert(bytes.end(), {'A', 'K', 'C', 'P', '1'});
            writeLe64(bytes, state.groupId);
            writeLe64(bytes, state.groupEpoch);
            writeLe64(bytes, state.peerNodeId);
            writeLe64(bytes, state.lastSeq);
            const size_t crcOffset = bytes.size();
            writeLe32(bytes, 0);
            writeLe32At(bytes, crcOffset, crcWithZeroedField(bytes, crcOffset));

            const auto tmpPath = makeTempPath(path, "ClusterRuntime peer progress");
            {
                std::ofstream out(tmpPath, std::ios::binary | std::ios::trunc);
                if (!out) { throw std::runtime_error("ClusterRuntime peer progress: cannot create state"); }
                out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
                out.flush();
                if (!out) { throw std::runtime_error("ClusterRuntime peer progress: write failed"); }
            }
            syncFile(tmpPath, "ClusterRuntime peer progress");
            replaceFileAtomically(tmpPath, path, "ClusterRuntime peer progress");
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
                validateRuntimePlacement(config_);
                validateRuntimeOptions(config_, runtimeOptions_);
                if (config_.consistency().mode == ConsistencyMode::RAFT_QUORUM) {
                    raftRuntime_ = RaftConsensusRuntime::create(dbDir, config_, selfNodeId_, callbacks_, runtimeOptions_);
                    return;
                }
                effectiveAckPolicy_ = effectiveAckPolicy(config_, selfNodeId_);
                effectiveConsistency_ = effectiveConsistency(config_);
                configuredReplicaCount_ = configuredReplicaCount(config_, selfNodeId_);
                if (effectiveAckPolicy_.mode == AckPolicyMode::QUORUM && effectiveAckPolicy_.quorum > configuredReplicaCount_) {
                    throw std::invalid_argument("ClusterRuntime: write quorum exceeds configured replica count");
                }
                if (config_.mode() != ReplicationMode::PARTITIONED) {
                    manager_ = ClusterManager::create(dbDir, config_, selfNodeId, runtimeOptions);
                    manager_->setRoleChangeCallback(
                        [this](NodeRole role) {
                            installRole(role);
                            if (callbacks_.roleChange) { callbacks_.roleChange(role); }
                        }
                    );
                }
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
                if (config_.mode() == ReplicationMode::PARTITIONED || config_.mode() == ReplicationMode::STRIPE) {
                    installPartitioned();
                    if (callbacks_.roleChange) { callbacks_.roleChange(role()); }
                    return;
                }
                manager_->start();
            }

            void close() {
                if (raftRuntime_) {
                    raftRuntime_->close();
                    return;
                }
                std::lock_guard lock{mutex_};
                stopReplication();
                if (manager_) { manager_->close(); }
                started_ = false;
            }

            NodeRole role() const noexcept {
                if (raftRuntime_) { return raftRuntime_->role(); }
                if (config_.mode() == ReplicationMode::PARTITIONED || config_.mode() == ReplicationMode::STRIPE) {
                    const auto* self = config_.findById(selfNodeId_);
                    return self != nullptr && self->dataBearing() ? NodeRole::PRIMARY : NodeRole::REPLICA;
                }
                return manager_ ? manager_->role() : NodeRole::STANDALONE;
            }

            std::vector<NodeInfo> activeNodes() const {
                if (raftRuntime_) { return raftRuntime_->activeNodes(); }
                if (config_.mode() == ReplicationMode::PARTITIONED || config_.mode() == ReplicationMode::STRIPE) { return config_.dataNodes(); }
                return manager_ ? manager_->activeNodes() : std::vector<NodeInfo>{};
            }

            const ClusterRouter& router() const noexcept { return raftRuntime_ ? raftRuntime_->router() : router_; }

            bool ownsWriteKey(std::span<const uint8_t> key) const {
                if (raftRuntime_) { return raftRuntime_->role() == NodeRole::PRIMARY; }
                if (config_.isStandalone()) { return true; }
                if (config_.mode() == ReplicationMode::MIRROR) { return manager_ && manager_->role() == NodeRole::PRIMARY; }
                if (config_.mode() == ReplicationMode::PARTITIONED || config_.mode() == ReplicationMode::STRIPE) {
                    return ownerForKey(key).nodeId == selfNodeId_;
                }
                return false;
            }

            ReadResponse readKey(std::span<const uint8_t> key, uint64_t snapshotSeq) {
                if (runtimeOptions_.readMode == ClusterReadMode::LOCAL_STALE_OK) { return readLocal(key, snapshotSeq); }
                if (raftRuntime_) {
                    if (raftRuntime_->role() != NodeRole::PRIMARY) {
                        ReadResponse response;
                        response.status = ReadStatus::ERROR_STATUS;
                        return response;
                    }
                    if (runtimeOptions_.readMode == ClusterReadMode::OWNER_LINEARIZABLE) {
                        try { raftRuntime_->linearizableReadBarrier(); }
                        catch (...) {
                            ReadResponse response;
                            response.status = ReadStatus::ERROR_STATUS;
                            return response;
                        }
                    }
                    return readLocal(key, snapshotSeq);
                }
                if (ownsWriteKey(key)) { return readLocal(key, snapshotSeq); }
                if (runtimeOptions_.readMode == ClusterReadMode::OWNER_ONLY) {
                    ReadResponse response;
                    response.status = ReadStatus::ERROR_STATUS;
                    return response;
                }
                if (runtimeOptions_.readMode != ClusterReadMode::OWNER_LINEARIZABLE) {
                    ReadResponse response;
                    response.status = ReadStatus::ERROR_STATUS;
                    return response;
                }
                const NodeInfo owner = ownerForKey(key);
                return readPeer(owner.nodeId, key, snapshotSeq);
            }

            ReadResponse readKeyFromNode(uint64_t nodeId, std::span<const uint8_t> key, uint64_t snapshotSeq) {
                if (nodeId == selfNodeId_) { return readLocal(key, snapshotSeq); }
                return readPeer(nodeId, key, snapshotSeq);
            }

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
                if ((config_.mode() == ReplicationMode::PARTITIONED || config_.mode() == ReplicationMode::STRIPE) &&
                    ownerForKey(key).nodeId != selfNodeId_) {
                    throw std::runtime_error("ClusterRuntime: write attempted on non-owner node");
                }
                if (server_) { server_->shipEntry(seq, op, key, value, recordFlags, sourceNodeId); }
            }

            void shipEntryTo(
                uint64_t targetNodeId,
                uint64_t seq,
                ReplOpType op,
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint8_t recordFlags,
                uint64_t sourceNodeId,
                bool waitForAck
            ) {
                if (raftRuntime_) {
                    raftRuntime_->shipEntry(seq, op, key, value, recordFlags, sourceNodeId);
                    return;
                }
                if (targetNodeId == selfNodeId_) { return; }
                std::lock_guard lock{mutex_};
                if (!server_) { throw std::runtime_error("ClusterRuntime: targeted write requires local replication server"); }
                if (config_.mode() == ReplicationMode::STRIPE) {
                    auto& targetSeq = nextSeqByTarget_[targetNodeId];
                    const uint64_t wireSeq = targetSeq + 1;
                    server_->shipEntryTo(targetNodeId, wireSeq, op, key, value, recordFlags, sourceNodeId, waitForAck);
                    targetSeq = wireSeq;
                    return;
                }
                server_->shipEntryTo(targetNodeId, seq, op, key, value, recordFlags, sourceNodeId, waitForAck);
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

            void reconfigure(ClusterConfig config) {
                (void)config;
                throw std::runtime_error(
                    "ClusterRuntime: online placement reconfiguration is unsupported; stop every node, persist one new ClusterConfig, and reopen. "
                    "Use the Raft voting-member APIs for online RAFT_QUORUM membership changes"
                );
            }

        private:
            NodeInfo ownerForKey(std::span<const uint8_t> key) const {
                const auto targets = router_.writeTargets(key);
                if (targets.empty()) { throw std::runtime_error("ClusterRuntime: key has no owner"); }
                return targets.front();
            }

            ReadResponse readLocal(std::span<const uint8_t> key, uint64_t snapshotSeq) const {
                if (!callbacks_.read) {
                    ReadResponse response;
                    response.status = ReadStatus::ERROR_STATUS;
                    return response;
                }
                return callbacks_.read(key, snapshotSeq);
            }

            ReadResponse readPeer(uint64_t nodeId, std::span<const uint8_t> key, uint64_t snapshotSeq) {
                const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(effectiveConsistency_.ackTimeoutMs);
                while (std::chrono::steady_clock::now() < deadline) {
                    try {
                        std::lock_guard lock{mutex_};
                        const auto it = peerClients_.find(nodeId);
                        if (it != peerClients_.end() && it->second) {
                            const auto now = std::chrono::steady_clock::now();
                            const auto remaining = deadline > now
                                                       ? std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count()
                                                       : int64_t{0};
                            return it->second->readKey(key, snapshotSeq, static_cast<uint32_t>(std::max<int64_t>(1, remaining)));
                        }
                    }
                    catch (const std::runtime_error&) {
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds{25});
                }
                ReadResponse response;
                response.status = ReadStatus::ERROR_STATUS;
                return response;
            }

            uint64_t lastSeqFromPeer(uint64_t nodeId) const {
                std::lock_guard lock{peerSeqMutex_};
                const auto it = lastSeqByPeer_.find(nodeId);
                return it == lastSeqByPeer_.end() ? 0 : it->second;
            }

            [[nodiscard]] std::filesystem::path peerProgressPath(uint64_t nodeId) const {
                if (runtimeOptions_.clusterMembershipPath.empty()) { return {}; }
                auto path = runtimeOptions_.clusterMembershipPath;
                path += ".peer-" + std::to_string(nodeId) + ".progress";
                return path;
            }

            void loadPeerProgress(uint64_t nodeId) {
                const auto path = peerProgressPath(nodeId);
                if (path.empty()) { return; }
                if (runtimeOptions_.resetClusterMembership) {
                    std::error_code ec;
                    std::filesystem::remove(path, ec);
                    if (ec) { throw std::runtime_error("ClusterRuntime: cannot reset peer progress: " + ec.message()); }
                    return;
                }
                const auto persisted = loadPeerProgressState(path, runtimeOptions_.corruptStateAction);
                if (!persisted) { return; }
                if (persisted->groupId != runtimeOptions_.clusterGroupId || persisted->groupEpoch != runtimeOptions_.clusterGroupEpoch ||
                    persisted->peerNodeId != nodeId) {
                    throw std::runtime_error("ClusterRuntime: peer progress belongs to a different cluster group or peer");
                }
                std::lock_guard lock{peerSeqMutex_};
                lastSeqByPeer_[nodeId] = std::max(lastSeqByPeer_[nodeId], persisted->lastSeq);
            }

            void recordSeqFromPeer(uint64_t nodeId, uint64_t seq) {
                std::lock_guard lock{peerSeqMutex_};
                auto& current = lastSeqByPeer_[nodeId];
                if (seq <= current) { return; }
                savePeerProgressState(
                    peerProgressPath(nodeId),
                    PeerProgressState{
                        .groupId = runtimeOptions_.clusterGroupId,
                        .groupEpoch = runtimeOptions_.clusterGroupEpoch,
                        .peerNodeId = nodeId,
                        .lastSeq = seq,
                    }
                );
                current = seq;
            }

            std::unique_ptr<ReplicationClient> createPeerClient(const NodeInfo& peer) {
                loadPeerProgress(peer.nodeId);
                auto clientOptions = runtimeOptions_;
                clientOptions.secure.expectedPrimaryNodeId = peer.nodeId;
                if (!clientOptions.clusterMembershipPath.empty()) {
                    clientOptions.clusterMembershipPath += ".peer-" + std::to_string(peer.nodeId);
                }
                auto client = ReplicationClient::create(
                    peer.host,
                    peer.replPort,
                    selfNodeId_,
                    [this, peerNodeId = peer.nodeId] { return lastSeqFromPeer(peerNodeId); },
                    effectiveAckPolicy_,
                    std::move(clientOptions)
                );
                client->setApplyCallback(
                    [this, peerNodeId = peer.nodeId](
                    uint64_t seq,
                    ReplOpType op,
                    std::span<const uint8_t> key,
                    std::span<const uint8_t> value,
                    uint8_t recordFlags,
                    uint64_t sourceNodeId
                ) {
                        if (sourceNodeId != peerNodeId) { return; }
                        if (config_.mode() == ReplicationMode::PARTITIONED && ownerForKey(key).nodeId != sourceNodeId) { return; }
                        if (callbacks_.apply) { callbacks_.apply(seq, op, key, value, recordFlags, sourceNodeId); }
                        recordSeqFromPeer(sourceNodeId, seq);
                    }
                );
                client->setForceDurableCallback(callbacks_.forceDurable);
                return client;
            }

            ReplicationServer::HistoryProvider makeHistoryProvider() const {
                if (!callbacks_.getEntries) { return {}; }
                return [getEntries = callbacks_.getEntries](
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

            ReplicationServer::SnapshotProvider makeSnapshotProvider() const {
                if (!callbacks_.exportSnapshot) { return {}; }
                return [exportSnapshot = callbacks_.exportSnapshot]() -> std::optional<ReplicationServer::Snapshot> {
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

            void installPartitionedLocked() {
                stopReplication();
                const auto* self = config_.findById(selfNodeId_);
                if (self == nullptr || !self->dataBearing()) {
                    throw std::runtime_error("ClusterRuntime: partitioned/stripe runtime requires self to be data-bearing");
                }

                const auto groupState = loadOrCreatePrimaryGroup(
                    runtimeOptions_.clusterMembershipPath,
                    selfNodeId_,
                    runtimeOptions_.clusterGroupId,
                    runtimeOptions_.clusterGroupEpoch,
                    runtimeOptions_.corruptStateAction
                );
                runtimeOptions_.clusterGroupId = groupState.groupId;
                runtimeOptions_.clusterGroupEpoch = groupState.groupEpoch;

                server_ = ReplicationServer::create(
                    self->replPort,
                    selfNodeId_,
                    callbacks_.getCurrentSeq,
                    effectiveAckPolicy_,
                    effectiveConsistency_,
                    configuredReplicaCount_,
                    configuredReplicaNodeIds(config_, selfNodeId_),
                    runtimeOptions_,
                    config_.mode() == ReplicationMode::PARTITIONED ? makeHistoryProvider() : ReplicationServer::HistoryProvider{},
                    config_.mode() == ReplicationMode::PARTITIONED ? makeSnapshotProvider() : ReplicationServer::SnapshotProvider{}
                );
                server_->setReadCallback(
                    [this](const ReadRequest& request) {
                        return readLocal(
                            std::span<const uint8_t>{request.key.data(), request.key.size()},
                            request.snapshotSeq
                        );
                    }
                );
                server_->start();

                for (const auto& peer : config_.dataNodes()) {
                    if (peer.nodeId == selfNodeId_) { continue; }
                    auto client = createPeerClient(peer);
                    client->start();
                    peerClients_.emplace(peer.nodeId, std::move(client));
                }
            }

            void installPartitioned() {
                std::lock_guard lock{mutex_};
                installPartitionedLocked();
            }

            void installRoleLocked(NodeRole role) {
                stopReplication();

                if (role == NodeRole::PRIMARY) {
                    if (config_.mode() == ReplicationMode::MIRROR && config_.primaryNodeId() != 0 &&
                        config_.primaryNodeId() != selfNodeId_) {
                        throw std::runtime_error("ClusterRuntime: only the configured MIRROR Primary may install the PRIMARY role");
                    }
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
                    server_ = ReplicationServer::create(
                        replPort,
                        selfNodeId_,
                        callbacks_.getCurrentSeq,
                        effectiveAckPolicy_,
                        effectiveConsistency_,
                        configuredReplicaCount_,
                        configuredReplicaNodeIds(config_, selfNodeId_),
                        runtimeOptions_,
                        makeHistoryProvider(),
                        makeSnapshotProvider()
                    );
                    server_->setReadCallback(
                        [this](const ReadRequest& request) {
                            return readLocal(
                                std::span<const uint8_t>{request.key.data(), request.key.size()},
                                request.snapshotSeq
                            );
                        }
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

            void installRole(NodeRole role) {
                std::lock_guard lock{mutex_};
                installRoleLocked(role);
            }

            void stopReplication() {
                if (client_) {
                    client_->close();
                    client_.reset();
                }
                for (auto& [_, client] : peerClients_) {
                    if (client) { client->close(); }
                }
                peerClients_.clear();
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
            mutable std::mutex peerSeqMutex_;
            std::unordered_map<uint64_t, uint64_t> lastSeqByPeer_;
            std::unordered_map<uint64_t, uint64_t> nextSeqByTarget_;
            bool started_ = false;
            std::unique_ptr<ReplicationServer> server_;
            std::unique_ptr<ReplicationClient> client_;
            std::unordered_map<uint64_t, std::unique_ptr<ReplicationClient>> peerClients_;
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

    bool ClusterRuntime::ownsWriteKey(std::span<const uint8_t> key) const { return impl_->ownsWriteKey(key); }

    ReadResponse ClusterRuntime::readKey(std::span<const uint8_t> key, uint64_t snapshotSeq) { return impl_->readKey(key, snapshotSeq); }

    ReadResponse ClusterRuntime::readKeyFromNode(uint64_t nodeId, std::span<const uint8_t> key, uint64_t snapshotSeq) {
        return impl_->readKeyFromNode(nodeId, key, snapshotSeq);
    }

    const ClusterRouter& ClusterRuntime::router() const noexcept { return impl_->router(); }

    void ClusterRuntime::shipEntry(
        uint64_t seq,
        ReplOpType op,
        std::span<const uint8_t> key,
        std::span<const uint8_t> value,
        uint8_t recordFlags,
        uint64_t sourceNodeId
    ) { impl_->shipEntry(seq, op, key, value, recordFlags, sourceNodeId); }

    void ClusterRuntime::shipEntryTo(
        uint64_t targetNodeId,
        uint64_t seq,
        ReplOpType op,
        std::span<const uint8_t> key,
        std::span<const uint8_t> value,
        uint8_t recordFlags,
        uint64_t sourceNodeId,
        bool waitForAck
    ) {
        impl_->shipEntryTo(targetNodeId, seq, op, key, value, recordFlags, sourceNodeId, waitForAck);
    }

    void ClusterRuntime::shipBlob(uint64_t seq, uint64_t blobId, std::span<const uint8_t> content) {
        impl_->shipBlob(seq, blobId, content);
    }

    void ClusterRuntime::addRaftVotingNode(const NodeInfo& node) { impl_->addRaftVotingNode(node); }

    void ClusterRuntime::removeRaftVotingNode(uint64_t nodeId) { impl_->removeRaftVotingNode(nodeId); }

    void ClusterRuntime::transferRaftLeadership(uint64_t targetNodeId) { impl_->transferRaftLeadership(targetNodeId); }

    void ClusterRuntime::reconfigure(ClusterConfig config) { impl_->reconfigure(std::move(config)); }
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

/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/cluster/ClusterRuntime.cpp
#include <akk/engine/cluster/detail/ReconfigurationDeadline.hpp>
#include "akk/engine/cluster/ClusterRuntime.hpp"
#include "akk/engine/cluster/detail/RaftConsensusRuntime.hpp"
#include "akk/engine/cluster/detail/ClusterPlacement.hpp"
#include "akk/engine/cluster/detail/ReplicationTransfer.hpp"
#include "akk/cpu/CRC32C.hpp"
#include "akk/crypto/Random.hpp"
#include "PartitionSnapshotSpool.hpp"
#include "AuthoritySnapshotFile.hpp"
#include "akk/engine/cluster/detail/EngineSnapshot.hpp"

#include <array>
#include <algorithm>
#include <atomic>
#include <charconv>
#include <cctype>
#include <chrono>
#include <exception>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
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
        uint64_t observationNowUs() noexcept {
            return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count());
        }

        void observeLatency(ClusterLatencyHistogram& histogram, uint64_t latencyUs) noexcept {
            ++histogram.sampleCount;
            histogram.totalUs += latencyUs;
            histogram.maxUs = std::max(histogram.maxUs, latencyUs);
            for (size_t i = 0; i < histogram.bucketUpperBoundsUs.size(); ++i) {
                if (latencyUs <= histogram.bucketUpperBoundsUs[i]) { ++histogram.bucketCounts[i]; }
            }
        }

        void mergeLatency(ClusterLatencyHistogram& target, const ClusterLatencyHistogram& source) noexcept {
            target.sampleCount += source.sampleCount;
            target.totalUs += source.totalUs;
            target.maxUs = std::max(target.maxUs, source.maxUs);
            for (size_t i = 0; i < target.bucketCounts.size(); ++i) {
                target.bucketCounts[i] += source.bucketCounts[i];
            }
        }

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
            if (options.stripeWriteCommitMode != StripeWriteCommitMode::ALL_SHARDS &&
                options.stripeWriteCommitMode != StripeWriteCommitMode::DATA_SHARDS) {
                throw std::invalid_argument("ClusterRuntime: invalid or unsafe STRIPE write commit mode");
            }
            if (options.stripeReadCoordinatorMode != StripeReadCoordinatorMode::OWNER && options.stripeReadCoordinatorMode !=
                StripeReadCoordinatorMode::LOCAL_COORDINATOR) {
                throw std::invalid_argument("ClusterRuntime: invalid STRIPE read coordinator mode");
            }
            if (options.stripeRebuildIntervalMs < 100 || options.stripeRebuildIntervalMs > 3'600'000 ||
                options.stripeRebuildBatchKeys == 0 || options.stripeRebuildBatchKeys > 65'536) {
                throw std::invalid_argument("ClusterRuntime: invalid STRIPE rebuild limits");
            }
        }

        uint16_t configuredReplicaCount(const ClusterConfig& config, uint64_t selfNodeId) {
            if (config.mode() == ReplicationMode::PARTITIONED) { return config.partitionCopies() - 1; }
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
            for (const auto& node : config.nodes()) { if (node.dataBearing() && !node.raftLearner()) { ++dataNodes; } }
            if (dataNodes == 0) { throw std::invalid_argument("ClusterRuntime: RAFT_QUORUM requires data-bearing nodes"); }

            const size_t majority = (dataNodes / 2) + 1;
            const size_t replicaAcks = majority > 0 ? majority - 1 : 0;
            if (replicaAcks > UINT16_MAX) { throw std::invalid_argument("ClusterRuntime: RAFT_QUORUM replica quorum is too large"); }
            return static_cast<uint16_t>(replicaAcks);
        }

        AckPolicy effectiveAckPolicy(const ClusterConfig& config, uint64_t selfNodeId) {
            if (config.mode() == ReplicationMode::PARTITIONED && config.usesDataConsensus()) {
                // Parent endpoints carry discovery and administration. Each
                // partition's Raft group commits its own data quorum.
                return {};
            }
            if (config.mode() == ReplicationMode::STRIPE) {
                return AckPolicy{.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::DURABLE};
            }
            const auto consistency = config.consistency();
            const auto legacy = config.ackPolicy();
            if (consistency.mode == ConsistencyMode::ASYNC) { return AckPolicy{.mode = AckPolicyMode::NONE, .stage = legacy.stage}; }
            if (config.usesDataConsensus()) {
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
                case WriteConsistency::AVAILABLE_REPLICAS:
                    return AckPolicy{.mode = AckPolicyMode::ALL_TARGETS, .stage = legacy.stage};
            }
            throw std::invalid_argument("ClusterRuntime: invalid write consistency");
        }

        ConsistencyOptions effectiveConsistency(const ClusterConfig& config) {
            auto consistency = config.consistency();
            if (config.usesDataConsensus() || config.mode() == ReplicationMode::STRIPE) {
                consistency.ackTimeoutAction = AckTimeoutAction::FAIL_WRITE;
            }
            return consistency;
        }

        ClusterConfig stripeMetadataRaftConfig(const ClusterConfig& config) {
            std::vector<NodeInfo> voters;
            for (auto node : config.dataNodes()) {
                node.dataPort = 0;
                node.replPort = node.stripeMetadataPort;
                node.stripeMetadataPort = 0;
                node.capabilities = DATA_BEARING | COORDINATOR_ELIGIBLE | (node.placementStandby() ? RAFT_LEARNER : 0u);
                node.partitionReplBasePort = 0;
                voters.push_back(std::move(node));
            }
            return ClusterConfig{
                std::move(voters),
                ReplicationMode::MIRROR,
                AckPolicy{.mode = AckPolicyMode::QUORUM, .stage = AckStage::DURABLE, .quorum = 1},
                ConsistencyOptions{
                    .mode = ConsistencyMode::RAFT_QUORUM,
                    .writeConsistency = WriteConsistency::QUORUM,
                    .ackTimeoutAction = AckTimeoutAction::FAIL_WRITE,
                    .replicaLagAction = ReplicaLagAction::ASYNC_RESYNC,
                    .ackTimeoutMs = config.consistency().ackTimeoutMs,
                },
                RaftOptions{.membership = {.mode = RaftMembershipMode::JOINT_CONSENSUS, .allowOnlineVoterChanges = true, .allowLearners = true}},
                StripeOptions{},
                0,
                config.clusterId()
            };
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
                if (std::string_view{reinterpret_cast<const char*>(bytes.data()), 5} != "AKCG1") {
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
            bytes.insert(bytes.end(), {'A', 'K', 'C', 'G', '1'});
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
            CorruptClusterStateAction corruptAction,
            const MirrorPromotionOptions& promotion,
            const std::function<uint64_t()>& getDurableSeq,
            const std::function<void()>& forceDurable
        ) {
            if (!path.empty() && std::filesystem::exists(path)) {
                const auto loaded = loadGroupState(path, corruptAction, "ClusterRuntime");
                if (!loaded) {
                    return loadOrCreatePrimaryGroup(
                        path, selfNodeId, requestedGroupId, requestedEpoch, corruptAction,
                        promotion, getDurableSeq, forceDurable
                    );
                }
                auto state = *loaded;
                if (requestedGroupId != 0 && requestedGroupId != state.groupId) {
                    throw std::runtime_error("ClusterRuntime: requested cluster group id conflicts with persisted group state");
                }
                bool publishPromotion = false;
                if (state.primaryNodeId != selfNodeId) {
                    if (!promotion.enabled) {
                        throw std::runtime_error(
                            "ClusterRuntime: MIRROR Primary change requires explicit offline promotion authorization"
                        );
                    }
                    if (state.primaryNodeId != promotion.previousPrimaryNodeId ||
                        state.groupEpoch != promotion.previousGroupEpoch) {
                        throw std::runtime_error("ClusterRuntime: MIRROR promotion source does not match persisted membership");
                    }
                    if (!getDurableSeq || !forceDurable) {
                        throw std::runtime_error("ClusterRuntime: MIRROR promotion requires durable engine callbacks");
                    }
                    forceDurable();
                    if (getDurableSeq() != promotion.expectedDurableSeq) {
                        throw std::runtime_error("ClusterRuntime: MIRROR promotion candidate durable sequence does not match authorization");
                    }
                    state.primaryNodeId = selfNodeId;
                    state.groupEpoch = promotion.previousGroupEpoch + 1;
                    publishPromotion = true;
                }
                else if (promotion.enabled && state.groupEpoch != promotion.previousGroupEpoch + 1) {
                    throw std::runtime_error("ClusterRuntime: persisted MIRROR promotion epoch does not match authorization");
                }
                if (requestedEpoch != 0 && requestedEpoch != state.groupEpoch) {
                    throw std::runtime_error("ClusterRuntime: requested cluster group epoch conflicts with persisted group state");
                }
                if (publishPromotion) {
                    saveGroupState(path, state, "ClusterRuntime MIRROR promotion");
                }
                return state;
            }

            if (promotion.enabled) {
                throw std::runtime_error("ClusterRuntime: MIRROR promotion requires existing persisted membership");
            }

            GroupState state{
                .groupId = requestedGroupId != 0 ? requestedGroupId : randomNonZeroU64(),
                .primaryNodeId = selfNodeId,
                .groupEpoch = requestedEpoch != 0 ? requestedEpoch : 1,
            };
            saveGroupState(path, state, "ClusterRuntime");
            return state;
        }
        void saveFencingImage(const std::filesystem::path& path, std::vector<uint8_t> bytes) {
            std::filesystem::create_directories(path.parent_path());
            writeLe32(bytes, cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size()));
            const auto temporary = makeTempPath(path, "MIRROR fencing");
            {
                std::ofstream out(temporary, std::ios::binary | std::ios::trunc);
                out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
                out.flush();
                if (!out) { throw std::runtime_error("MIRROR fencing: state write failed"); }
            }
            syncFile(temporary, "MIRROR fencing");
            replaceFileAtomically(temporary, path, "MIRROR fencing");
        }

        std::vector<uint8_t> loadFencingImage(const std::filesystem::path& path) {
            std::ifstream in(path, std::ios::binary);
            if (!in) { throw std::runtime_error("MIRROR fencing: cannot open state"); }
            std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(in)), {});
            if (bytes.size() < 4 || readLe32(bytes, bytes.size() - 4) !=
                cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size() - 4)) {
                throw std::runtime_error("MIRROR fencing: corrupt state");
            }
            bytes.resize(bytes.size() - 4);
            return bytes;
        }

        // A clock-free distributed write mutex. A pending operation is never
        // expired: promotion must wait for its durable release.
        class MirrorAuthority {
            struct State { uint64_t seq = 0, primary = 0, epoch = 0, token = 0; };
            ClusterId id_{};
            std::filesystem::path path_;
            uint64_t self_;
            mutable std::mutex stateMutex_;
            std::mutex operationMutex_;
            State state_;
            std::unique_ptr<RaftConsensusRuntime> raft_;
            std::vector<uint8_t> snapshotBuffer_;
            uint64_t configuredPrimary_ = 0;
            bool promotionRequested_ = false;
            uint64_t initialEpoch_ = 1;
            std::atomic<bool> aligning_{false};
            std::thread alignmentThread_;
            ClusterId dataClusterId_{};
            uint64_t groupId_ = 0;
            MirrorRecoveryOptions recovery_;
            std::stop_source recoveryStop_;
            std::function<void()> forceDurable_;
            std::function<uint64_t()> getLastSeq_;

            std::vector<uint8_t> encode(State state) const {
                std::vector<uint8_t> bytes{'A', 'K', 'M', 'A', '1'};
                bytes.insert(bytes.end(), id_.begin(), id_.end());
                for (auto field : {state.seq, state.primary, state.epoch, state.token}) { writeLe64(bytes, field); }
                return bytes;
            }
            State decode(std::span<const uint8_t> bytes) const {
                if (bytes.size() != 53 || std::string_view{reinterpret_cast<const char*>(bytes.data()), 5} != "AKMA1" ||
                    !std::equal(id_.begin(), id_.end(), bytes.begin() + 5)) {
                    throw std::runtime_error("MIRROR authority: foreign or invalid state");
                }
                State out{readLe64(bytes, 21), readLe64(bytes, 29), readLe64(bytes, 37), readLe64(bytes, 45)};
                if ((out.primary == 0) != (out.epoch == 0) || (out.token != 0 && out.primary == 0)) {
                    throw std::runtime_error("MIRROR authority: invalid authority state");
                }
                return out;
            }
            void publish(State state) {
                saveFencingImage(path_, encode(state));
                state_ = state;
            }
            State state() const { std::lock_guard lock{stateMutex_}; return state_; }
            void command(uint8_t action, uint64_t primary, uint64_t epoch, uint64_t argument = 0) {
                std::vector<uint8_t> value{action};
                for (auto field : {primary, epoch, argument}) { writeLe64(value, field); }
                const std::array<uint8_t, 5> key{'A', 'K', 'M', 'C', '1'};
                auto pending = raft_->submitMutation([&](uint64_t) {
                    return ClusterMutation{.sourceNodeId = self_, .op = ReplOpType::PUT,
                        .key = {key.begin(), key.end()}, .value = value, .blob = std::nullopt};
                });
                pending.completion.get();
                raft_->linearizableReadBarrier();
            }
            void endGrant(State grant) {
                command(2, grant.primary, grant.epoch, grant.token);
                const auto released = state();
                if (released.primary != grant.primary || released.epoch != grant.epoch || released.token != 0) {
                    throw std::runtime_error("MIRROR authority: release was rejected");
                }
            }
            // The caller holds operationMutex_. A receipt proves completion
            // only for this exact grant, confirmed by an authority barrier.
            void recoverCompletedWrite() {
                const auto current = state();
                const auto receiptPath = path_.parent_path() / "completed";
                if (current.primary != self_ || current.token == 0 || !std::filesystem::exists(receiptPath)) { return; }
                const auto receipt = decode(loadFencingImage(receiptPath));
                if (receipt.primary != current.primary || receipt.epoch != current.epoch || receipt.token != current.token) { return; }
                raft_->linearizableReadBarrier();
                const auto authorized = state();
                if (authorized.primary != current.primary || authorized.epoch != current.epoch || authorized.token != current.token) { return; }
                endGrant(current);
            }
            bool recoverUnresolvedWrite(State expected, MirrorRecoveryAction action, uint64_t expectedDurableSeq) {
                if (recovery_.mode == MirrorRecoveryMode::BLOCK) { return false; }
                raft_->linearizableReadBarrier();
                const auto current = state();
                if (current.primary != expected.primary || current.epoch != expected.epoch || current.token != expected.token) {
                    throw std::runtime_error("MIRROR recovery: requested authority grant has changed");
                }
                if (current.token == 0) { return false; }
                if ((action == MirrorRecoveryAction::RESUME_PRIMARY) != (current.primary == self_)) {
                    throw std::runtime_error("MIRROR recovery: invalid recovery target");
                }
                const MirrorRecoveryRequest request{.clusterId = dataClusterId_, .groupId = groupId_,
                    .previousPrimaryNodeId = current.primary, .previousGroupEpoch = current.epoch,
                    .operationId = current.token, .candidateNodeId = self_, .expectedDurableSeq = expectedDurableSeq, .action = action};
                const auto cancelled = recoveryStop_.get_token();
                if (cancelled.stop_requested()) { throw std::runtime_error("MIRROR recovery: cancelled"); }
                const auto proof = recovery_.provider->recover(request, cancelled);
                if (cancelled.stop_requested()) { throw std::runtime_error("MIRROR recovery: cancelled"); }
                if (!proof) { return false; }
                if (proof->request != request) { throw std::runtime_error("MIRROR recovery: proof does not match the requested grant"); }
                raft_->linearizableReadBarrier();
                const auto authorized = state();
                if (authorized.primary != current.primary || authorized.epoch != current.epoch || authorized.token != current.token) {
                    throw std::runtime_error("MIRROR recovery: authority changed while obtaining proof");
                }
                forceDurable_();
                if (getLastSeq_() != expectedDurableSeq) { throw std::runtime_error("MIRROR recovery: durable sequence changed while obtaining proof"); }
                if (cancelled.stop_requested()) { throw std::runtime_error("MIRROR recovery: cancelled before release"); }
                if (action == MirrorRecoveryAction::RESUME_PRIMARY) {
                    saveFencingImage(path_.parent_path() / "completed", encode(current));
                }
                // END compares the exact operation token in the state machine.
                // Promotion then uses the normal transition, which rejects any
                // intervening grant instead of forcibly discarding it.
                endGrant(current);
                return true;
            }
        public:
            MirrorAuthority(const std::filesystem::path& directory, const ClusterConfig& config,
                uint64_t self, ClusterRuntimeOptions options, const ClusterEngineCallbacks& engineCallbacks)
                : path_{directory / "mirror-authority" / "state"}, self_{self}, dataClusterId_{config.clusterId()},
                  groupId_{options.clusterGroupId}, recovery_{options.mirrorRecovery},
                  forceDurable_{engineCallbacks.forceDurable}, getLastSeq_{engineCallbacks.getLastSeq} {
                configuredPrimary_ = config.primaryNodeId();
                promotionRequested_ = options.mirrorPromotion.enabled;
                initialEpoch_ = options.clusterGroupEpoch != 0 ? options.clusterGroupEpoch : 1;
                auto nodes = options.mirrorFencing.authorityNodes;
                for (auto& node : nodes) { node.dataPort = 0; node.stripeMetadataPort = 0; }
                std::ranges::sort(nodes, {}, &NodeInfo::nodeId);
                std::vector<uint8_t> fingerprint{'A', 'K', 'M', 'Q', '1'};
                fingerprint.insert(fingerprint.end(), config.clusterId().begin(), config.clusterId().end());
                writeLe64(fingerprint, options.clusterGroupId);
                for (const auto& node : nodes) {
                    writeLe64(fingerprint, node.nodeId);
                    writeLe32(fingerprint, node.capabilities);
                    writeLe64(fingerprint, node.replPort);
                    writeLe64(fingerprint, node.host.size());
                    fingerprint.insert(fingerprint.end(), node.host.begin(), node.host.end());
                }
                const std::array parts{std::span<const uint8_t>{fingerprint}};
                const auto hash = crypto::hash256(parts);
                std::copy_n(hash.begin(), id_.size(), id_.begin());
                if (std::filesystem::exists(path_)) { state_ = decode(loadFencingImage(path_)); }
                ClusterEngineCallbacks callbacks;
                callbacks.getLastSeq = callbacks.getCurrentSeq = [this] { return state().seq; };
                callbacks.forceDurable = [] {};
                callbacks.apply = [this](uint64_t seq, ReplOpType op, std::span<const uint8_t> key,
                    std::span<const uint8_t> value, uint8_t flags, uint64_t, uint64_t) {
                    if (op != ReplOpType::PUT || flags != 0 || key.size() != 5 ||
                        std::string_view{reinterpret_cast<const char*>(key.data()), key.size()} != "AKMC1" || value.size() != 25 || value[0] > 4) {
                        throw std::runtime_error("MIRROR authority: invalid command");
                    }
                    std::lock_guard lock{stateMutex_};
                    auto next = state_;
                    if (seq <= next.seq) { return; }
                    const auto primary = readLe64(value, 1), epoch = readLe64(value, 9), argument = readLe64(value, 17);
                    if (value[0] == 0 && next.primary == 0 && primary != 0 && epoch != 0) { next.primary = primary; next.epoch = epoch; }
                    else if (next.primary == primary && next.epoch == epoch) {
                        if (value[0] == 1 && next.token == 0 && argument != 0) { next.token = argument; }
                        if (value[0] == 2 && next.token == argument && argument != 0) { next.token = 0; }
                        // Action 4 is retained solely for replay of already
                        // persisted forced transitions; new recovery uses END
                        // followed by the guarded normal transition (action 3).
                        if ((value[0] == 3 || value[0] == 4) && (next.token == 0 || value[0] == 4) && argument != 0 && argument != primary && epoch != UINT64_MAX) {
                            next.primary = argument; ++next.epoch; next.token = 0;
                        }
                    }
                    next.seq = seq;
                    publish(next);
                };
                callbacks.exportSnapshot = [this]() -> std::optional<ClusterSnapshot> {
                    const auto captured = state();
                    if (captured.seq == 0) { return std::nullopt; }
                    auto bytes = encode(captured);
                    return ClusterSnapshot{.seq = captured.seq, .forEachEntry = [bytes](const SnapshotEntryVisitor& visitor) {
                        const std::array<uint8_t, 5> key{'A', 'K', 'M', 'A', '1'};
                        return visitor && visitor.beginEntry(key, bytes.size(),
                            cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size())) &&
                            visitor.appendValueChunk(0, bytes) && visitor.finishEntry();
                    }};
                };
                callbacks.beginSnapshot = [this](uint64_t, uint64_t) { snapshotBuffer_.clear(); };
                callbacks.beginSnapshotEntry = [](std::span<const uint8_t> key, uint64_t size, uint32_t) {
                    if (size != 53 || key.size() != 5 || std::string_view{reinterpret_cast<const char*>(key.data()), 5} != "AKMA1") {
                        throw std::runtime_error("MIRROR authority: invalid snapshot entry");
                    }
                };
                callbacks.appendSnapshotEntryChunk = [this](uint64_t offset, std::span<const uint8_t> chunk) {
                    if (offset != snapshotBuffer_.size() || chunk.size() > 53 - snapshotBuffer_.size()) {
                        throw std::runtime_error("MIRROR authority: invalid snapshot chunk");
                    }
                    snapshotBuffer_.insert(snapshotBuffer_.end(), chunk.begin(), chunk.end());
                };
                callbacks.finishSnapshotEntry = [this] {
                    std::lock_guard lock{stateMutex_}; publish(decode(snapshotBuffer_));
                };
                callbacks.finishSnapshot = [this](uint64_t seq, uint64_t count) {
                    if (count != 1 || state().seq != seq) { throw std::runtime_error("MIRROR authority: incomplete snapshot"); }
                };
                callbacks.recoverSnapshot = [this](uint64_t seq) {
                    if (state().seq != seq) { throw std::runtime_error("MIRROR authority: snapshot state missing"); }
                };
                callbacks.isSnapshotDurable = [this](uint64_t seq) { return state().seq >= seq; };
                options.mirrorFencing = {};
                options.mirrorPromotion = {};
                options.mirrorRecovery = {};
                options.clusterMembershipPath.clear();
                options.resetClusterMembership = false;
                options.requests.enabled = false;
                options.primaryNodeId = 0; options.primaryHost.clear(); options.primaryReplPort = 0;
                options.secure.expectedPrimaryNodeId = 0;
                options.transfer.spoolDirectory = path_.parent_path() / "transfer-spool";
                ClusterConfig control{nodes, ReplicationMode::STANDALONE, {},
                    ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM,
                        .ackTimeoutMs = config.consistency().ackTimeoutMs}, {}, {}, 0, id_};
                raft_ = RaftConsensusRuntime::create(path_.parent_path(), std::move(control), self, std::move(callbacks), std::move(options));
            }
            ~MirrorAuthority() { close(); }
            void start() {
                if (aligning_.load()) { return; }
                recoveryStop_ = std::stop_source{};
                raft_->start();
                if (aligning_.exchange(true)) { return; }
                alignmentThread_ = std::thread([this] {
                    auto nextRecovery = std::chrono::steady_clock::now();
                    while (aligning_.load()) {
                        const auto current = state();
                        const auto target = promotionRequested_ || current.primary == 0 ? configuredPrimary_ : current.primary;
                        if (raft_->role() == NodeRole::PRIMARY && target != self_) {
                            try { raft_->transferLeadership(target); }
                            catch (const std::runtime_error&) { /* elections/catch-up are retried */ }
                        }
                        else if (raft_->role() == NodeRole::PRIMARY && configuredPrimary_ == self_ && current.primary == 0 && !promotionRequested_) {
                            try {
                                std::unique_lock serial{operationMutex_, std::try_to_lock};
                                if (serial.owns_lock() && state().primary == 0) { command(0, self_, initialEpoch_); }
                            }
                            catch (const std::runtime_error&) { /* quorum establishment is retried */ }
                        }
                        if (current.primary == self_ && current.token != 0) {
                            try {
                                std::unique_lock serial{operationMutex_, std::try_to_lock};
                                if (serial.owns_lock()) {
                                    recoverCompletedWrite();
                                    const auto now = std::chrono::steady_clock::now();
                                    if (recovery_.mode == MirrorRecoveryMode::AUTOMATIC && state().token != 0 && now >= nextRecovery) {
                                        nextRecovery = now + std::chrono::milliseconds{recovery_.retryIntervalMs};
                                        recoverUnresolvedWrite(state(), MirrorRecoveryAction::RESUME_PRIMARY, getLastSeq_());
                                    }
                                }
                            }
                            catch (...) { /* provider failure/uncertainty leaves the grant pending; retry under the configured policy */ }
                        }
                        std::this_thread::sleep_for(std::chrono::milliseconds{50});
                    }
                });
            }
            void close() {
                aligning_.store(false);
                recoveryStop_.request_stop();
                if (raft_) { raft_->close(); }
                if (alignmentThread_.joinable()) { alignmentThread_.join(); }
            }
            void cancelRecovery() noexcept { recoveryStop_.request_stop(); }
            RaftRuntimeStats stats() const { return raft_->stats(); }
            void observe(RaftRuntimeStats& out) const {
                const auto raft = stats(); const auto current = state();
                out.mirrorAuthorityLeaderNodeId = raft.leaderNodeId;
                out.mirrorAuthorityCommitIndex = raft.commitIndex;
                out.mirrorAuthorityPrimaryNodeId = current.primary;
                out.mirrorAuthorityEpoch = current.epoch;
                out.mirrorWritePending = current.token != 0;
            }
            void transfer(uint64_t node) { raft_->transferLeadership(node); }
            bool accepts(uint64_t primary, uint64_t epoch) const {
                const auto current = state();
                return current.primary == primary && current.epoch == epoch;
            }
            void validateWrite(uint64_t epoch) const {
                const auto current = state();
                if (current.primary != self_ || current.epoch != epoch || current.token == 0) {
                    throw std::runtime_error("MIRROR authority: data shipping requires an active write grant");
                }
            }
            void validateRead(uint64_t epoch) {
                if (state().primary == 0) {
                    std::lock_guard serial{operationMutex_};
                    if (state().primary == 0) { command(0, self_, epoch); }
                }
                raft_->linearizableReadBarrier();
                if (!accepts(self_, epoch)) { throw std::runtime_error("MIRROR authority: obsolete Primary read"); }
            }
            void execute(uint64_t epoch, const std::function<void()>& write, const std::function<void()>& durable) {
                std::lock_guard serial{operationMutex_};
                raft_->linearizableReadBarrier();
                if (state().primary == 0) { command(0, self_, epoch); }
                recoverCompletedWrite();
                const auto current = state();
                if (current.primary != self_ || current.epoch != epoch || current.token != 0) {
                    throw std::runtime_error("MIRROR authority: not the authorized Primary or a write remains unresolved");
                }
                if (current.seq == UINT64_MAX) { throw std::runtime_error("MIRROR authority: operation ids exhausted"); }
                const auto token = current.seq + 1;
                command(1, self_, epoch, token);
                const auto grant = state();
                if (grant.primary != self_ || grant.epoch != epoch || grant.token != token) {
                    throw std::runtime_error("MIRROR authority: grant was rejected");
                }
                // Do not release on exceptions: partial local application cannot
                // be assumed completed or revoked by another process.
                write();
                durable();
                // Authority may have advanced while the callback ran (external
                // fencing). Never certify a different generation or operation.
                saveFencingImage(path_.parent_path() / "completed", encode(grant));
                endGrant(grant);
            }
            bool recoverWrite(uint64_t epoch) {
                std::lock_guard serial{operationMutex_};
                raft_->linearizableReadBarrier();
                const auto current = state();
                if (current.primary != self_ || current.epoch != epoch) { throw std::runtime_error("MIRROR recovery: only the current Primary may resume"); }
                if (current.token == 0) { return false; }
                recoverCompletedWrite();
                const auto remaining = state();
                if (remaining.primary != self_ || remaining.epoch != epoch) {
                    throw std::runtime_error("MIRROR recovery: authority changed during completion recovery");
                }
                if (remaining.token == 0) { return true; }
                return recoverUnresolvedWrite(remaining, MirrorRecoveryAction::RESUME_PRIMARY, getLastSeq_());
            }
            void promote(uint64_t previousPrimary, uint64_t previousEpoch, uint64_t expectedDurableSeq) {
                std::lock_guard serial{operationMutex_};
                const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{20};
                while (raft_->role() != NodeRole::PRIMARY && std::chrono::steady_clock::now() < deadline) {
                    std::this_thread::sleep_for(std::chrono::milliseconds{25});
                }
                raft_->linearizableReadBarrier();
                auto current = state();
                if (current.primary == self_ && current.epoch == previousEpoch + 1 && current.token == 0) { return; }
                if (current.primary != previousPrimary || current.epoch != previousEpoch) {
                    throw std::runtime_error("MIRROR authority: promotion denied; prior authority mismatch");
                }
                if (current.token != 0) {
                    if (!recoverUnresolvedWrite(current, MirrorRecoveryAction::PROMOTE_PRIMARY, expectedDurableSeq)) {
                        throw std::runtime_error("MIRROR authority: unresolved write requires a recovery proof");
                    }
                }
                command(3, previousPrimary, previousEpoch, self_);
                if (!accepts(self_, previousEpoch + 1)) { throw std::runtime_error("MIRROR authority: promotion rejected"); }
            }
        };
        #include "PlacementAuthority.inc"
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
                : dbDir_{dbDir},
                  config_{std::move(config)},
                  router_{config_},
                  selfNodeId_{selfNodeId},
                  callbacks_{std::move(callbacks)},
                  runtimeOptions_{std::move(runtimeOptions)} {
                config_.validateRuntime(selfNodeId_, runtimeOptions_);
                if (runtimeOptions_.mirrorPromotion.enabled && runtimeOptions_.startupRole == NodeStartupRole::REPLICA &&
                    callbacks_.getLastSeq && callbacks_.getLastSeq() > runtimeOptions_.mirrorPromotion.expectedDurableSeq) {
                    throw std::runtime_error("MIRROR promotion: replica has a divergent/ahead prefix; restore an authoritative copy before rejoining");
                }
                if (runtimeOptions_.transportMode == TransportMode::SECURE && runtimeOptions_.secure.identitySeedPath.empty() && !dbDir.
                    empty()) { runtimeOptions_.secure.identitySeedPath = dbDir / "cluster.identity"; }
                if (runtimeOptions_.clusterMembershipPath.empty() && !dbDir.empty() && !config_.usesDataConsensus()) { runtimeOptions_.clusterMembershipPath = dbDir / "cluster.membership"; }
                validateTransportScope(config_, runtimeOptions_);
                    validateRuntimePlacement(config_);
                validateRuntimeOptions(config_, runtimeOptions_);
                runtimeOptions_.replicationClusterId = config_.clusterId();
                runtimeOptions_.replicationConfigFingerprint = config_.replicationFingerprint();
                if (config_.mode() == ReplicationMode::MIRROR && !config_.usesDataConsensus()) {
                    if (runtimeOptions_.mirrorFencing.mode == MirrorFencingMode::QUORUM_FENCED &&
                        (dbDir_.empty() || !callbacks_.apply || !callbacks_.forceDurable || !callbacks_.getLastSeq)) {
                        throw std::invalid_argument("MIRROR quorum fencing requires a persistent directory and engine apply, durability and sequence callbacks");
                    }
                    std::vector<uint8_t> policy{'A', 'K', 'M', 'F', '1', static_cast<uint8_t>(runtimeOptions_.mirrorFencing.mode)};
                    policy.insert(policy.end(), config_.clusterId().begin(), config_.clusterId().end());
                    auto authorities = runtimeOptions_.mirrorFencing.authorityNodes;
                    std::ranges::sort(authorities, {}, &NodeInfo::nodeId);
                    for (const auto& node : authorities) {
                        writeLe64(policy, node.nodeId); writeLe64(policy, node.replPort); writeLe64(policy, node.host.size());
                        policy.insert(policy.end(), node.host.begin(), node.host.end());
                    }
                    const auto path = dbDir_ / "mirror.fencing";
                    if (std::filesystem::exists(path)) {
                        if (loadFencingImage(path) != policy) { throw std::runtime_error("MIRROR fencing: persisted policy/topology cannot be changed or downgraded"); }
                    }
                    else { saveFencingImage(path, policy); }
                    if (runtimeOptions_.mirrorFencing.mode == MirrorFencingMode::QUORUM_FENCED) {
                        mirrorAuthority_ = std::make_unique<MirrorAuthority>(dbDir_, config_, selfNodeId_, runtimeOptions_, callbacks_);
                    }
                }
                if (config_.mode() == ReplicationMode::PARTITIONED && (!callbacks_.apply || !callbacks_.forceDurable)) {
                    throw std::invalid_argument("ClusterRuntime: PARTITIONED requires apply and forceDurable callbacks");
                }
                if (config_.usesDataConsensus() && config_.mode() != ReplicationMode::PARTITIONED) {
                    raftRuntime_ = RaftConsensusRuntime::create(dbDir, config_, selfNodeId_, callbacks_, runtimeOptions_);
                    return;
                }
                if (config_.mode() == ReplicationMode::PARTITIONED && config_.usesDataConsensus()) {
                    if (!callbacks_.partitionLeader) { throw std::invalid_argument("ClusterRuntime: PARTITIONED Raft requires a partition leader callback"); }
                    placementAuthority_ = std::make_unique<PlacementAuthority>(dbDir_, config_, selfNodeId_, runtimeOptions_, callbacks_);
                }
                effectiveAckPolicy_ = effectiveAckPolicy(config_, selfNodeId_);
                transferBudget_ = std::make_shared<detail::TransferBudget>(runtimeOptions_.transfer);
                effectiveConsistency_ = effectiveConsistency(config_);
                configuredReplicaCount_ = configuredReplicaCount(config_, selfNodeId_);
                if (effectiveAckPolicy_.mode == AckPolicyMode::QUORUM && effectiveAckPolicy_.quorum > configuredReplicaCount_) {
                    throw std::invalid_argument("ClusterRuntime: write quorum exceeds configured replica count");
                }
                if (config_.mode() == ReplicationMode::STRIPE) {
                    if (!callbacks_.commitStripeMetadata || !callbacks_.readStripeMetadata || !callbacks_.forceDurable) {
                        throw std::invalid_argument("ClusterRuntime: STRIPE metadata Raft requires metadata and durability callbacks");
                    }
                    auto control = callbacks_;
                    control.placementChanged = [this](const ClusterConfig& target, uint64_t generation) {
                        router_.reconfigure(target);
                        if (callbacks_.placementChanged) { callbacks_.placementChanged(target, generation); }
                    };
                    placementAuthority_ = std::make_unique<PlacementAuthority>(dbDir_, config_, selfNodeId_, runtimeOptions_, std::move(control));
                    stripeMetadataRaft_ = &placementAuthority_->consensus();
                }
                if (config_.mode() != ReplicationMode::PARTITIONED) {
                    manager_ = ClusterManager::create(dbDir, config_, selfNodeId, runtimeOptions);
                    manager_->setRoleChangeCallback(
                        [this](NodeRole role) {
                            try { manager_->checkHealth(); }
                            catch (...) {
                                ++leaseRenewFailures_;
                                recordFailure(ClusterFailureCode::LEASE_RENEWAL, ClusterHealthState::FAILED);
                                stopReplicationAfterManagerFailure();
                                if (callbacks_.roleChange) { callbacks_.roleChange(role); }
                                return;
                            }
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
                bool alreadyStarted = false;
                {
                    std::lock_guard lock{mutex_};
                    alreadyStarted = started_;
                    if (!started_) { started_ = true; }
                }
                if (alreadyStarted) {
                    if (manager_) { manager_->checkHealth(); }
                    return;
                }
                runtimeStartedAtUs_.store(observationNowUs(), std::memory_order_relaxed);
                try {
                    health_.store(ClusterHealthState::HEALTHY, std::memory_order_relaxed);
                    if (mirrorAuthority_) { mirrorAuthority_->start(); }
                    if (placementAuthority_) {
                        placementAuthority_->start();
                        if (config_.mode() == ReplicationMode::PARTITIONED) {
                            if (callbacks_.roleChange) { callbacks_.roleChange(role()); }
                            return;
                        }
                    }

                    if (config_.mode() == ReplicationMode::PARTITIONED || config_.mode() == ReplicationMode::STRIPE) {
                        installPartitioned();
                        if (callbacks_.roleChange) { callbacks_.roleChange(role()); }
                        return;
                    }
                    manager_->start();
                }
                catch (...) {
                    const auto startupError = std::current_exception();
                    try { close(); } catch (...) {}
                    std::rethrow_exception(startupError);
                }
            }

            void close() {
                if (raftRuntime_) {
                    raftRuntime_->close();
                    return;
                }
                {
                    std::lock_guard lock{mutex_};
                    started_ = false;
                }
                // ClusterManager::close() may join a role-change callback. Do
                // not hold the endpoint transition mutex while waiting for it.
                if (manager_) { manager_->close(); }
                stopReplication();

                if (mirrorAuthority_) { mirrorAuthority_->close(); }
                if (placementAuthority_) { placementAuthority_->close(); }
            }

            RaftRuntimeStats raftStats() const {
                if (raftRuntime_) { return raftRuntime_->stats(); }
                RaftRuntimeStats out;
                if (mirrorAuthority_) { mirrorAuthority_->observe(out); }
                out.sampledAtUs = observationNowUs();
                out.runtimeStartedAtUs = runtimeStartedAtUs_.load(std::memory_order_relaxed);
                out.health = health_.load(std::memory_order_relaxed);
                out.lastFailure = lastFailure_.load(std::memory_order_relaxed);
                out.lastFailureAtUs = lastFailureAtUs_.load(std::memory_order_relaxed);
                out.leaseRenewFailures = leaseRenewFailures_.load(std::memory_order_relaxed);
                out.endpointStartFailures = endpointStartFailures_.load(std::memory_order_relaxed);
                out.peerReadTimeouts = peerReadTimeouts_.load(std::memory_order_relaxed);
                if (transferBudget_) {
                    out.transferMemoryBytes = transferBudget_->used(detail::TransferBudget::Resource::MEMORY);
                    out.transferSpoolBytes = transferBudget_->used(detail::TransferBudget::Resource::SPOOL);
                    out.activeTransfers = transferBudget_->used(detail::TransferBudget::Resource::ACTIVE);
                    const auto transfer = transferBudget_->stats();
                    out.transferResumeAttempts = transfer.resumeAttempts;
                    out.transferResumed = transfer.resumedTransfers;
                    out.transferResumedBytes = transfer.resumedBytes;
                    out.transferDiscardedPartials = transfer.discardedPartials;
                    out.transferRetainedPartials = transfer.retainedPartials;
                }
                std::shared_ptr<ReplicationServer> server;
                std::shared_ptr<ReplicationClient> client;
                decltype(peerClients_) peerClients;
                bool started = false;
                {
                    std::lock_guard lock{mutex_};
                    out.clusterGroupId = runtimeOptions_.clusterGroupId;
                    out.clusterGroupEpoch = runtimeOptions_.clusterGroupEpoch;
                    server = server_;
                    client = client_;
                    peerClients = peerClients_;
                    started = started_;
                }
                ReplicationServer::Stats serverStats;
                if (server) {
                    serverStats = server->stats();
                    out.replicationQueueFrames = serverStats.queuedFrames;
                    out.replicationQueueBytes = serverStats.queuedBytes;
                }

                struct PeerObservation {
                    bool inbound = false;
                    bool outbound = false;
                    uint64_t lastSuccessfulContactAtUs = 0;
                    uint64_t roundTripsSucceeded = 0;
                    uint64_t roundTripsFailed = 0;
                    uint64_t consecutiveRoundTripFailures = 0;
                    uint64_t lastRoundTripAtUs = 0;
                    uint64_t lastRoundTripFailureAtUs = 0;
                    ClusterLatencyHistogram roundTripLatencyUs;
                };
                std::unordered_map<uint64_t, PeerObservation> observations;
                for (const auto& peer : serverStats.peers) {
                    auto& observed = observations[peer.nodeId];
                    observed.inbound = peer.connected;
                    observed.lastSuccessfulContactAtUs = std::max(observed.lastSuccessfulContactAtUs, peer.lastSuccessfulContactAtUs);
                }
                if (client && manager_) {
                    const uint64_t primaryNodeId = manager_->primaryNodeId();
                    if (primaryNodeId != 0) {
                        auto& observed = observations[primaryNodeId];
                        observed.outbound = client->connected();
                        observed.lastSuccessfulContactAtUs = std::max(
                            observed.lastSuccessfulContactAtUs,
                            client->lastSuccessfulContactAtUs()
                        );
                    }
                }
                for (const auto& [nodeId, peer] : peerClients) {
                    auto& observed = observations[nodeId];
                    observed.outbound = peer && peer->connected();
                    if (peer) {
                        observed.lastSuccessfulContactAtUs = std::max(
                            observed.lastSuccessfulContactAtUs,
                            peer->lastSuccessfulContactAtUs()
                        );
                    }
                }
                {
                    std::lock_guard lock{peerRoundTripMutex_};
                    for (const auto& [nodeId, metrics] : peerRoundTrips_) {
                        auto& observed = observations[nodeId];
                        observed.roundTripsSucceeded = metrics.succeeded;
                        observed.roundTripsFailed = metrics.failed;
                        observed.consecutiveRoundTripFailures = metrics.consecutiveFailures;
                        observed.lastRoundTripAtUs = metrics.lastRoundTripAtUs;
                        observed.lastRoundTripFailureAtUs = metrics.lastFailureAtUs;
                        mergeLatency(observed.roundTripLatencyUs, metrics.latencyUs);
                    }
                }
                const bool bidirectional = config_.mode() == ReplicationMode::PARTITIONED || config_.mode() == ReplicationMode::STRIPE;
                for (const auto& peer : config_.dataNodes()) {
                    if (peer.nodeId == selfNodeId_) { continue; }
                    const auto observed = observations.find(peer.nodeId);
                    bool connected = false;
                    if (observed != observations.end()) {
                        connected = bidirectional ? observed->second.inbound && observed->second.outbound
                                                  : server ? observed->second.inbound : observed->second.outbound;
                    }
                    out.peers.push_back(RaftPeerStats{
                        .nodeId = peer.nodeId,
                        .connected = connected,
                        .lastSuccessfulContactAtUs = observed == observations.end() ? 0 : observed->second.lastSuccessfulContactAtUs,
                        .roundTripsSucceeded = observed == observations.end() ? 0 : observed->second.roundTripsSucceeded,
                        .roundTripsFailed = observed == observations.end() ? 0 : observed->second.roundTripsFailed,
                        .consecutiveRoundTripFailures = observed == observations.end() ? 0 : observed->second.consecutiveRoundTripFailures,
                        .lastRoundTripAtUs = observed == observations.end() ? 0 : observed->second.lastRoundTripAtUs,
                        .lastRoundTripFailureAtUs = observed == observations.end() ? 0 : observed->second.lastRoundTripFailureAtUs,
                        .roundTripLatencyUs = observed == observations.end()
                                                  ? ClusterLatencyHistogram{}
                                                  : observed->second.roundTripLatencyUs,
                    });
                }
                if (started && out.health != ClusterHealthState::FAILED) {
                    if (config_.raidPreset() == RaidPreset::RAID1 && serverStats.rebuildingReplicas != 0) {
                        out.health = ClusterHealthState::REBUILDING;
                    }
                    else if (std::ranges::any_of(out.peers, [](const RaftPeerStats& peer) { return !peer.connected; })) {
                        out.health = ClusterHealthState::DEGRADED;
                    }
                }
                out.sampledAtUs = observationNowUs();
                return out;
            }

            RaftRuntimeStats stripeMetadataRaftStats() const {
                return stripeMetadataRaft_ ? stripeMetadataRaft_->stats() : RaftRuntimeStats{};
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
                if (placementAuthority_) { return placementAuthority_->activeNodes(); }
                if (config_.mode() == ReplicationMode::PARTITIONED || config_.mode() == ReplicationMode::STRIPE) { return config_.dataNodes(); }
                return manager_ ? manager_->activeNodes() : std::vector<NodeInfo>{};
            }

            const ClusterRouter& router() const noexcept { return raftRuntime_ ? raftRuntime_->router() : router_; }

            bool ownsWriteKey(std::span<const uint8_t> key) const {
                if (raftRuntime_) { return raftRuntime_->role() == NodeRole::PRIMARY; }
                if (callbacks_.partitionLeader) { return callbacks_.partitionLeader(key) == selfNodeId_; }
                if (manager_) { manager_->checkHealth(); }
                if (config_.isStandalone()) { return true; }
                if (config_.mode() == ReplicationMode::MIRROR) {
                    if (!manager_ || manager_->role() != NodeRole::PRIMARY) { return false; }
                    if (mirrorAuthority_ && mirrorAuthority_->stats().leaderNodeId != selfNodeId_) { return false; }
                    return true;
                }
                if (config_.mode() == ReplicationMode::PARTITIONED || config_.mode() == ReplicationMode::STRIPE) {
                    const uint64_t owner = ownerForKey(key).nodeId;
                    if (owner == selfNodeId_) { return true; }
                    const auto view = placementAuthority_ ? ClusterConfig::decode(placementAuthority_->state(false).activeConfig) : config_;
                    const auto* failover = view.stripeFailoverNode();
                    return config_.mode() == ReplicationMode::STRIPE && failover && failover->nodeId == selfNodeId_ &&
                           !stripeNodeReachable(owner);
                }
                return false;
            }

            ClusterRouteTarget routeTarget(std::span<const uint8_t> key) const {
                ClusterRouteTarget target;
                target.configurationEpoch = configurationEpoch();
                if (raftRuntime_) {
                    const auto stats = raftRuntime_->stats();
                    target.configurationEpoch = 0; // Data Raft authority is identified by its term, not the non-Raft group epoch.
                    target.nodeId = stats.leaderNodeId;
                    target.raftTerm = stats.currentTerm;
                }
                else if (config_.mode() == ReplicationMode::MIRROR) {
                    target.nodeId = manager_ ? manager_->primaryNodeId() : config_.primaryNodeId();
                    if (mirrorAuthority_) {
                        RaftRuntimeStats stats;
                        mirrorAuthority_->observe(stats);
                        if (stats.mirrorAuthorityPrimaryNodeId != 0) {
                            target.nodeId = stats.mirrorAuthorityPrimaryNodeId;
                            target.configurationEpoch = stats.mirrorAuthorityEpoch;
                        }
                    }
                }
                else if (!config_.isStandalone()) {
                    target.nodeId = ownerForKey(key).nodeId;
                    if (callbacks_.partitionLeader) {
                        const auto leader = callbacks_.partitionLeader(key);
                        if (leader != 0) { target.nodeId = leader; }
                        else {
                            const auto holders = callbacks_.partitionCandidates ? callbacks_.partitionCandidates(key) : detail::partitionTargets(config_, key);
                            for (const auto& holder : holders) {
                                if (holder.nodeId == selfNodeId_ || stripeNodeReachable(holder.nodeId)) {
                                    target.nodeId = holder.nodeId;
                                    break;
                                }
                            }
                        }
                    }
                    if (config_.mode() == ReplicationMode::STRIPE && !stripeNodeReachable(target.nodeId)) {
                        const auto view = ClusterConfig::decode(placementAuthority_->state(false).activeConfig);
                        const auto* failover = view.stripeFailoverNode();
                        if (failover && stripeNodeReachable(failover->nodeId)) { target.nodeId = failover->nodeId; }
                    }
                }
                else { target.nodeId = selfNodeId_; }
                auto nodes = raftRuntime_ ? raftRuntime_->activeNodes() : placementAuthority_ ? ClusterConfig::decode(placementAuthority_->state(false).activeConfig).nodes() : config_.nodes();
                if (placementAuthority_ && callbacks_.partitionLeader &&
                    std::ranges::none_of(nodes, [&](const auto& node) { return node.nodeId == target.nodeId; })) {
                    // A partition may elect its new holder before the placement
                    // authority activates all groups. Redirects still need its endpoint.
                    const auto pending = placementAuthority_->state(false).pendingConfig;
                    if (!pending.empty()) {
                        const auto proposed = ClusterConfig::decode(pending);
                        if (const auto* node = proposed.findById(target.nodeId)) { nodes.push_back(*node); }
                    }
                }
                for (const auto& node : nodes) {
                    if (node.nodeId != target.nodeId) { continue; }
                    target.host = node.host; target.tcpPort = node.dataPort; target.replPort = node.replPort;
                    break;
                }
                for (const auto& endpoint : runtimeOptions_.apiEndpoints) {
                    if (endpoint.nodeId != target.nodeId) { continue; }
                    target.tcpPort = endpoint.tcpPort; target.httpPort = endpoint.httpPort; target.grpcPort = endpoint.grpcPort;
                }
                return target;
            }

            ForwardResponse forwardTo(uint64_t nodeId, ForwardRequest request) {
                if (raftRuntime_) { return raftRuntime_->forwardTo(nodeId, std::move(request)); }
                if (placementAuthority_) { return placementAuthority_->forwardTo(nodeId, std::move(request)); }
                std::shared_ptr<ReplicationClient> client;
                {
                    std::lock_guard lock{mutex_};
                    if (manager_ && manager_->primaryNodeId() == nodeId) { client = client_; }
                    if (!client) {
                        const auto it = peerClients_.find(nodeId);
                        if (it != peerClients_.end()) { client = it->second; }
                    }
                }
                if (!client) { throw ClusterRoutingError(ClusterRoutingCode::FORWARD_UNAVAILABLE, {}, "No forwarding connection to target"); }
                return client->forward(std::move(request));
            }

            ReadResponse linearizableReadKey(std::span<const uint8_t> key) {
                if (!ownsWriteKey(key)) { throw ClusterRoutingError(ClusterRoutingCode::NOT_OWNER, routeTarget(key)); }
                if (raftRuntime_) { raftRuntime_->linearizableReadBarrier(); return readLocal(key, 0); }
                return readPrimaryEndpoint(key, 0);
            }

            void queryReadBarrier() {
                if (raftRuntime_) {
                    if (raftRuntime_->role() != NodeRole::PRIMARY) {
                        throw ClusterRoutingError(ClusterRoutingCode::NOT_OWNER, routeTarget({}));
                    }
                    raftRuntime_->linearizableReadBarrier();
                }
                else if (config_.mode() == ReplicationMode::STRIPE) {
                    (void)stripeMetadataLinearizableWatermark();
                }
                else if (config_.mode() == ReplicationMode::MIRROR) {
                    if (routeTarget({}).nodeId != selfNodeId_) {
                        throw ClusterRoutingError(ClusterRoutingCode::NOT_OWNER, routeTarget({}));
                    }
                    if (mirrorAuthority_) { mirrorAuthority_->validateRead(runtimeOptions_.clusterGroupEpoch); }
                    if (runtimeOptions_.mirrorFencing.mode == MirrorFencingMode::EXTERNAL_FENCED) {
                        runtimeOptions_.mirrorFencing.external->validatePrimary(config_.clusterId(), runtimeOptions_.clusterGroupId,
                            selfNodeId_, runtimeOptions_.clusterGroupEpoch);
                    }
                }
                if (manager_) { manager_->checkHealth(); }
            }

            ReadResponse readKey(std::span<const uint8_t> key, uint64_t snapshotSeq) {
                if (manager_) { manager_->checkHealth(); }
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
                if (ownsWriteKey(key)) {
                    return readPrimaryEndpoint(key, snapshotSeq);
                }
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
                if (manager_) { manager_->checkHealth(); }
                if (nodeId == selfNodeId_) { return readLocal(key, snapshotSeq); }
                return readPeer(nodeId, key, snapshotSeq);
            }

            uint64_t stripeFailoverNodeId() const noexcept {
                const auto view = placementAuthority_ ? ClusterConfig::decode(placementAuthority_->state(false).activeConfig) : config_;
                const auto* node = view.stripeFailoverNode();
                return node ? node->nodeId : 0;
            }

            uint64_t stripeMetadataLeaderNodeId() const noexcept {
                if (!stripeMetadataRaft_) { return 0; }
                try { return stripeMetadataRaft_->stats().leaderNodeId; }
                catch (...) { return 0; }
            }

            bool stripeNodeReachable(uint64_t nodeId) const noexcept {
                if (nodeId == selfNodeId_) { return true; }
                std::lock_guard lock{mutex_};
                const auto it = peerClients_.find(nodeId);
                return it != peerClients_.end() && it->second && it->second->connected();
            }

            StripeOperationLease acquireStripeOperation(std::span<const uint8_t> key, uint64_t ownerNodeId) {
                StripeControlRequest request;
                request.action = StripeControlAction::ACQUIRE;
                request.ownerNodeId = ownerNodeId;
                request.key.assign(key.begin(), key.end());
                auto deadline = stripeMetadataDeadline();
                const auto migrationDeadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{runtimeOptions_.reconfiguration.timeoutMs};
                while (std::chrono::steady_clock::now() < deadline) {
                    const auto response = routeStripeControl(request, deadline);
                    if (response.status == StripeControlStatus::GRANTED) {
                        return {
                            .ownerNodeId = ownerNodeId,
                            .authorityNodeId = response.authorityNodeId,
                            .fenceToken = response.fenceToken,
                            .coordinated = true,
                            .metadata = response.metadata,
                        };
                    }
                    if (response.status == StripeControlStatus::RECONFIGURING) {
                        if (runtimeOptions_.reconfiguration.writePolicy == ReconfigurationWritePolicy::REJECT) {
                            throw std::runtime_error("ClusterRuntime: STRIPE placement migration is pending");
                        }
                        deadline = migrationDeadline;
                        std::this_thread::sleep_for(std::chrono::milliseconds{25}); continue;
                    }
                    if (response.status != StripeControlStatus::BUSY) {
                        if (ownerForKey(key).nodeId != ownerNodeId) { throw ClusterRoutingError(ClusterRoutingCode::NOT_OWNER, routeTarget(key)); }
                        throw std::runtime_error("ClusterRuntime: STRIPE authority request rejected");
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds{10});
                }
                throw std::runtime_error("ClusterRuntime: timed out waiting for STRIPE authority handoff");
            }

            void commitStripeMetadata(const StripeOperationLease& lease, std::span<const uint8_t> key,
                std::span<const uint8_t> metadata) {
                if (!lease.coordinated) { throw std::runtime_error("ClusterRuntime: unfenced STRIPE metadata commit is forbidden"); }
                StripeControlRequest request;
                request.action = StripeControlAction::COMMIT;
                request.ownerNodeId = lease.ownerNodeId;
                request.fenceToken = lease.fenceToken;
                request.key.assign(key.begin(), key.end());
                request.metadata.assign(metadata.begin(), metadata.end());
                const auto deadline = stripeMetadataDeadline();
                const auto response = routeStripeControl(request, deadline);
                if (response.status != StripeControlStatus::COMMITTED) {
                    throw std::runtime_error("ClusterRuntime: stale or rejected STRIPE metadata commit");
                }
            }

            void releaseStripeOperation(const StripeOperationLease& lease, std::span<const uint8_t> key) noexcept {
                if (!lease.coordinated) { return; }
                try {
                    StripeControlRequest request;
                    request.action = StripeControlAction::RELEASE;
                    request.ownerNodeId = lease.ownerNodeId;
                    request.fenceToken = lease.fenceToken;
                    request.key.assign(key.begin(), key.end());
                    const auto deadline = stripeMetadataDeadline();
                    (void)routeStripeControl(request, deadline);
                }
                catch (...) {}
            }

            std::optional<std::vector<uint8_t>> readStripeMetadata(std::span<const uint8_t> key, uint64_t ownerNodeId) {
                StripeControlRequest request;
                request.action = StripeControlAction::READ_METADATA;
                request.ownerNodeId = ownerNodeId;
                request.key.assign(key.begin(), key.end());
                const auto deadline = stripeMetadataDeadline();
                const auto response = routeStripeControl(request, deadline);
                if (response.status == StripeControlStatus::NOT_FOUND) { return std::nullopt; }
                if (response.status != StripeControlStatus::FOUND) {
                    throw std::runtime_error("ClusterRuntime: STRIPE metadata read rejected");
                }
                return response.metadata;
            }

            bool repairStripeMetadata(std::span<const uint8_t> key, uint64_t expectedVersion,
                std::span<const uint8_t> metadata) {
                StripeControlRequest request;
                request.action = StripeControlAction::REPAIR_METADATA;
                request.ownerNodeId = ownerForKey(key).nodeId;
                request.fenceToken = expectedVersion;
                request.key.assign(key.begin(), key.end());
                request.metadata.assign(metadata.begin(), metadata.end());
                const auto deadline = stripeMetadataDeadline();
                const auto response = routeStripeControl(request, deadline);
                if (response.status == StripeControlStatus::COMMITTED) { return true; }
                if (response.status == StripeControlStatus::BUSY || response.status == StripeControlStatus::REJECTED ||
                    response.status == StripeControlStatus::RECONFIGURING) { return false; }
                throw std::runtime_error("ClusterRuntime: STRIPE repair metadata commit failed");
            }

            uint64_t ownerNodeId(std::span<const uint8_t> key) const {
                return callbacks_.partitionLeader ? routeTarget(key).nodeId : ownerForKey(key).nodeId;
            }

            uint64_t configurationEpoch() const noexcept {
                if (placementAuthority_) { return placementAuthority_->generation(); }
                std::lock_guard lock{mutex_};
                return runtimeOptions_.clusterGroupEpoch == 0 ? 1 : runtimeOptions_.clusterGroupEpoch;
            }

            uint64_t stripeMetadataLinearizableWatermark() {
                if (!stripeMetadataRaft_ || stripeMetadataRaft_->role() != NodeRole::PRIMARY) {
                    throw std::runtime_error("ClusterRuntime: local node is not the STRIPE metadata leader");
                }
                stripeMetadataRaft_->linearizableReadBarrier();
                return stripeMetadataRaft_->stats().appliedStateMachineSeq;
            }

            StripeControlResponse rollbackControl(uint64_t targetNodeId, StripeControlRequest request) {
                if (targetNodeId == 0) { throw std::invalid_argument("ClusterRuntime: rollback target node is zero"); }
                if (config_.mode() == ReplicationMode::STRIPE &&
                    (request.action == StripeControlAction::ROLLBACK_WATERMARK ||
                     request.action == StripeControlAction::ROLLBACK_KEY ||
                     request.action == StripeControlAction::ROLLBACK_STREAM)) {
                    return routeStripeControl(request, stripeMetadataDeadline());
                }
                if (targetNodeId == selfNodeId_) {
                    if (!callbacks_.rollbackControl) {
                        throw std::runtime_error("ClusterRuntime: local rollback control is unavailable");
                    }
                    return callbacks_.rollbackControl(selfNodeId_, request);
                }
                return sendStripeControl(targetNodeId, request);
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
                if (manager_) { manager_->checkHealth(); }
                if ((config_.mode() == ReplicationMode::PARTITIONED || config_.mode() == ReplicationMode::STRIPE) &&
                    ownerForKey(key).nodeId != selfNodeId_) {
                    throw std::runtime_error("ClusterRuntime: write attempted on non-owner node");
                }
                std::shared_ptr<ReplicationServer> server;
                {
                    std::lock_guard lock{mutex_};
                    server = server_;
                }
                if (server) {
                    if (mirrorAuthority_) {
                        mirrorAuthority_->validateWrite(runtimeOptions_.clusterGroupEpoch);
                        const auto wrapped = mirrorWireKey(key, runtimeOptions_.clusterGroupEpoch);
                        server->shipEntry(seq, op, wrapped, value, recordFlags, sourceNodeId);
                    }
                    else {
                        if (runtimeOptions_.mirrorFencing.mode == MirrorFencingMode::EXTERNAL_FENCED) {
                            runtimeOptions_.mirrorFencing.external->validatePrimary(config_.clusterId(),
                                runtimeOptions_.clusterGroupId, selfNodeId_, runtimeOptions_.clusterGroupEpoch);
                        }
                        server->shipEntry(seq, op, key, value, recordFlags, sourceNodeId);
                    }
                }
            }

            std::future<void> submitEntry(uint64_t seq, ReplOpType op, std::span<const uint8_t> key,
                std::span<const uint8_t> value, uint8_t flags, uint64_t source) {
                if (!raftRuntime_) { throw std::runtime_error("ClusterRuntime: async proposals require RAFT_QUORUM"); }
                return raftRuntime_->submitEntry(seq, op, key, value, flags, source);
            }

            ClusterMutationSubmission submitMutation(ClusterMutationFactory prepare) {
                if (!raftRuntime_) { throw std::runtime_error("ClusterRuntime: runtime-assigned mutations require RAFT_QUORUM"); }
                return raftRuntime_->submitMutation(std::move(prepare));
            }

            std::shared_future<ClusterRequestResult> submitRequest(const ClusterRequestId& id, const std::array<uint8_t, 32>& fingerprint,
                ClusterMutationFactory prepare) {
                if (!raftRuntime_) { throw std::logic_error("ClusterRuntime: retry-safe requests require RAFT_QUORUM"); }
                return raftRuntime_->submitRequest(id, fingerprint, std::move(prepare));
            }
            ClusterRequestResult queryRequest(const ClusterRequestId& id) {
                if (!raftRuntime_) { throw std::logic_error("ClusterRuntime: retry-safe requests require RAFT_QUORUM"); }
                return raftRuntime_->queryRequest(id);
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
                if (runtimeOptions_.mirrorFencing.mode != MirrorFencingMode::STATIC) {
                    throw std::runtime_error("ClusterRuntime: fenced MIRROR writes must use the group-wide write path");
                }
                if (raftRuntime_) {
                    raftRuntime_->shipEntry(seq, op, key, value, recordFlags, sourceNodeId);
                    return;
                }
                if (manager_) { manager_->checkHealth(); }
                if (targetNodeId == selfNodeId_) { return; }
                if (config_.mode() == ReplicationMode::PARTITIONED && !partitionReceivesFrom(key, selfNodeId_, targetNodeId)) {
                    throw std::runtime_error("ClusterRuntime: targeted partition write violates placement");
                }
                std::shared_ptr<ReplicationServer> server;
                uint64_t wireSeq = seq;
                {
                    std::lock_guard lock{mutex_};
                    server = server_;
                    if (!server) { throw std::runtime_error("ClusterRuntime: targeted write requires local replication server"); }
                    if (config_.mode() == ReplicationMode::STRIPE) {
                        auto& targetSeq = nextSeqByTarget_[targetNodeId];
                        if (targetSeq == UINT64_MAX) { throw std::runtime_error("ClusterRuntime: targeted request ids exhausted"); }
                        wireSeq = ++targetSeq;
                    }
                }
                server->shipEntryTo(targetNodeId, wireSeq, op, key, value, recordFlags, sourceNodeId, waitForAck);
            }

            void shipBlob(uint64_t seq, uint64_t blobId, std::span<const uint8_t> content) {
                if (runtimeOptions_.mirrorFencing.mode != MirrorFencingMode::STATIC) {
                    throw std::runtime_error("ClusterRuntime: fenced MIRROR does not support Blob replication");
                }
                if (raftRuntime_) {
                    raftRuntime_->shipBlob(seq, blobId, content);
                    return;
                }
                if (manager_) { manager_->checkHealth(); }
                std::shared_ptr<ReplicationServer> server;
                {
                    std::lock_guard lock{mutex_};
                    server = server_;
                }
                if (server) { server->shipBlob(seq, blobId, content); }
            }

            void addRaftVotingNode(const NodeInfo& node) {
                if (!raftRuntime_) { throw std::runtime_error("ClusterRuntime: online Raft membership change requires RAFT_QUORUM"); }
                raftRuntime_->addVotingNode(node);
            }

            void addRaftLearner(const NodeInfo& node) {
                if (!raftRuntime_) { throw std::runtime_error("ClusterRuntime: learners require RAFT_QUORUM"); }
                raftRuntime_->addLearner(node);
            }

            void executePrimaryWrite(const std::function<void()>& write) {
                if (!write) { throw std::invalid_argument("ClusterRuntime: write callback is required"); }
                if (config_.mode() == ReplicationMode::MIRROR && (!manager_ || manager_->role() != NodeRole::PRIMARY ||
                    config_.primaryNodeId() != selfNodeId_)) { throw std::runtime_error("ClusterRuntime: write requires configured MIRROR Primary"); }
                if (manager_) { manager_->checkHealth(); }
                if (mirrorAuthority_) {
                    mirrorAuthority_->execute(runtimeOptions_.clusterGroupEpoch, write, callbacks_.forceDurable);
                }
                else {
                    if (runtimeOptions_.mirrorFencing.mode == MirrorFencingMode::EXTERNAL_FENCED) {
                        runtimeOptions_.mirrorFencing.external->validatePrimary(config_.clusterId(),
                            runtimeOptions_.clusterGroupId, selfNodeId_, runtimeOptions_.clusterGroupEpoch);
                    }
                    write();
                }
            }
            MirrorFencingMode mirrorFencingMode() const noexcept { return runtimeOptions_.mirrorFencing.mode; }
            MirrorRecoveryMode mirrorRecoveryMode() const noexcept { return runtimeOptions_.mirrorRecovery.mode; }
            void cancelMirrorRecovery() noexcept { if (mirrorAuthority_) { mirrorAuthority_->cancelRecovery(); } }
            bool recoverMirrorWrite() {
                if (!mirrorAuthority_) { throw std::runtime_error("ClusterRuntime: MIRROR authority recovery is unavailable"); }
                if (!manager_ || manager_->role() != NodeRole::PRIMARY || config_.primaryNodeId() != selfNodeId_) {
                    throw std::runtime_error("ClusterRuntime: MIRROR recovery requires the configured Primary");
                }
                manager_->checkHealth();
                return mirrorAuthority_->recoverWrite(runtimeOptions_.clusterGroupEpoch);
            }

            void transferMirrorAuthorityLeadership(uint64_t nodeId) {
                if (!mirrorAuthority_) { throw std::runtime_error("ClusterRuntime: MIRROR authority quorum is not enabled"); }
                mirrorAuthority_->transfer(nodeId);
            }
            void promoteRaftLearner(uint64_t nodeId) {
                if (!raftRuntime_) { throw std::runtime_error("ClusterRuntime: learners require RAFT_QUORUM"); }
                raftRuntime_->promoteLearner(nodeId);
            }
            void removeRaftLearner(uint64_t nodeId) {
                if (!raftRuntime_) { throw std::runtime_error("ClusterRuntime: learners require RAFT_QUORUM"); }
                raftRuntime_->removeLearner(nodeId);
            }

            void removeRaftVotingNode(uint64_t nodeId) {
                if (!raftRuntime_) { throw std::runtime_error("ClusterRuntime: online Raft membership change requires RAFT_QUORUM"); }
                raftRuntime_->removeVotingNode(nodeId);
            }

            void transferRaftLeadership(uint64_t targetNodeId) {
                if (!raftRuntime_) { throw std::runtime_error("ClusterRuntime: Raft leader transfer requires RAFT_QUORUM"); }
                raftRuntime_->transferLeadership(targetNodeId);
            }

            void campaignLeadership() {
                if (!raftRuntime_) { throw std::logic_error("ClusterRuntime: campaign requires a data consensus group"); }
                raftRuntime_->campaignLeadership();
            }

            void reconfigure(ClusterConfig config, uint64_t generation) {
                if (raftRuntime_) { raftRuntime_->reconfigure(config, generation); return; }
                throw std::runtime_error(
                    "ClusterRuntime: online placement reconfiguration is unsupported; stop every node, persist one new ClusterConfig, and reopen. "
                    "Use the Raft voting-member APIs for online RAFT_QUORUM membership changes"
                );
            }
            void preparePlacementNodes(const ClusterConfig& target) {
                if (config_.mode() != ReplicationMode::STRIPE) { return; }
                const auto state = placementAuthority_->state(true);
                if (state.pendingGeneration == 0 || !PlacementAuthority::matches(state.pendingConfig, target.encode())) {
                    throw std::runtime_error("Cluster placement: transport preparation requires the committed intent");
                }
                auto transition = detail::ReconfigurationDeadline::lock(endpointTransitionMutex_);
                std::shared_ptr<ReplicationServer> server;
                { std::lock_guard lock{mutex_}; server = server_; }
                if (!server) { throw std::runtime_error("Cluster placement: shard server is not running"); }
                for (const auto& peer : target.dataNodes()) { if (peer.nodeId != selfNodeId_) { server->allowReplica(peer.nodeId); } }
                for (const auto& peer : target.dataNodes()) {
                    if (peer.nodeId == selfNodeId_) { continue; }
                    { std::lock_guard lock{mutex_}; if (peerClients_.contains(peer.nodeId)) { continue; } }
                    auto client = createPeerClient(peer, runtimeOptions_);
                    startEndpoint(*client);
                    std::lock_guard lock{mutex_};
                    peerClients_.emplace(peer.nodeId, std::move(client));
                }
            }
            void waitForStripeOperations() {
                const auto deadline = detail::ReconfigurationDeadline::cap(std::chrono::steady_clock::now() + std::chrono::milliseconds{runtimeOptions_.reconfiguration.timeoutMs});
                do {
                    if (!placementAuthority_ || stripeMetadataRaft_->role() != NodeRole::PRIMARY ||
                        placementAuthority_->state(true).pendingGeneration == 0) {
                        throw std::runtime_error("Cluster placement: stripe authority changed");
                    }
                    { auto lock = detail::ReconfigurationDeadline::lock(stripeAuthorityMutex_);
                        const auto term = stripeMetadataRaft_->stats().currentTerm;
                        if (stripeAuthorityTerm_ != term) { stripeLeases_.clear(); stripeAuthorityTerm_ = term; }
                        if (stripeLeases_.empty()) { return; }
                    }
                    if (runtimeOptions_.reconfiguration.cancelled && runtimeOptions_.reconfiguration.cancelled()) {
                        throw std::runtime_error("Cluster placement: migration cancelled while draining operations");
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds{25});
                } while (std::chrono::steady_clock::now() < deadline);
                throw std::runtime_error("Cluster placement: outstanding stripe operations did not drain");
            }
            void publishStripePlacementAlias(std::span<const uint8_t> key, std::span<const uint8_t> metadata, uint64_t generation) {
                if (!placementAuthority_ || stripeMetadataRaft_->role() != NodeRole::PRIMARY ||
                    placementAuthority_->state(true).pendingGeneration != generation) {
                    throw std::runtime_error("Cluster placement: stale stripe migration");
                }
                ClusterMutation mutation; mutation.recordFlags = 1; mutation.key = {'H'};
                mutation.key.insert(mutation.key.end(), key.begin(), key.end());
                writeLe64(mutation.value, generation);
                mutation.value.insert(mutation.value.end(), metadata.begin(), metadata.end());
                auto submission = stripeMetadataRaft_->submitMutation([&](uint64_t) { return mutation; });
                detail::ReconfigurationDeadline::wait(submission.completion);
            }
            ClusterPlacementState placementState(bool fresh) {
                return placementAuthority_ ? placementAuthority_->state(fresh) : ClusterPlacementState{};
            }
            uint64_t beginPlacementChange(const ClusterConfig& config) {
                if (!placementAuthority_) { throw std::runtime_error("Cluster placement: authority is unavailable"); }
                return placementAuthority_->begin(config);
            }
            void freezeStripePlacement(uint64_t generation) {
                if (!placementAuthority_) { throw std::logic_error("Cluster placement: authority is unavailable"); }
                placementAuthority_->freeze(generation);
            }
            void cancelPlacementChange(uint64_t generation) {
                if (!placementAuthority_) { throw std::logic_error("Cluster placement: authority is unavailable"); }
                placementAuthority_->cancel(generation);
            }
            void finishPlacementChange(uint64_t generation) {
                if (!placementAuthority_) { throw std::runtime_error("Cluster placement: authority is unavailable"); }
                placementAuthority_->finish(generation);
            }

        private:
            struct ReplicationEndpoints {
                std::shared_ptr<ReplicationServer> server;
                std::shared_ptr<ReplicationClient> client;
                std::unordered_map<uint64_t, std::shared_ptr<ReplicationClient>> peers;
            };

            struct StripeLeaseState {
                uint64_t ownerNodeId = 0;
                uint64_t authorityNodeId = 0;
                uint64_t fenceToken = 0;
                std::shared_ptr<const ReplicationServer::PeerSession> session;
                bool publishing = false;
            };

            [[nodiscard]] uint32_t stripeMetadataTimeoutMs() const noexcept {
                // A metadata request may arrive while Raft is electing a new
                // leader. Shard acknowledgement timeouts can intentionally be
                // very short, but must not prevent automatic metadata failover.
                return std::max<uint32_t>({
                    effectiveConsistency_.ackTimeoutMs,
                    runtimeOptions_.raftHeartbeatIntervalMs * 10U,
                    3000U,
                });
            }

            [[nodiscard]] std::chrono::steady_clock::time_point stripeMetadataDeadline() const noexcept {
                return std::chrono::steady_clock::now() +
                    std::chrono::milliseconds{stripeMetadataTimeoutMs()};
            }

            StripeControlResponse sendStripeControl(uint64_t targetNodeId, const StripeControlRequest& request) {
                std::shared_ptr<ReplicationClient> client;
                {
                    std::lock_guard lock{mutex_};
                    const auto it = peerClients_.find(targetNodeId);
                    if (it != peerClients_.end()) { client = it->second; }
                }
                if (!client) { throw std::runtime_error("ClusterRuntime: STRIPE metadata leader is unavailable"); }
                return client->stripeControl(request, stripeMetadataTimeoutMs());
            }

            StripeControlResponse routeStripeControl(
                const StripeControlRequest& request,
                std::chrono::steady_clock::time_point deadline
            ) {
                const bool leaderOwnedRollback = request.action == StripeControlAction::ROLLBACK_WATERMARK ||
                    request.action == StripeControlAction::ROLLBACK_KEY ||
                    request.action == StripeControlAction::ROLLBACK_STREAM;
                while (std::chrono::steady_clock::now() < deadline) {
                    const uint64_t leader = stripeMetadataLeaderNodeId();
                    if (leader == 0) {
                        std::this_thread::sleep_for(std::chrono::milliseconds{10});
                        continue;
                    }
                    try {
                        auto routedRequest = request;
                        if (leaderOwnedRollback) { routedRequest.ownerNodeId = leader; }
                        auto response = leader == selfNodeId_
                                            ? handleStripeControl(selfNodeId_, routedRequest)
                                            : sendStripeControl(leader, routedRequest);
                        if (response.status != StripeControlStatus::ERROR_STATUS) { return response; }
                    }
                    catch (...) {}
                    std::this_thread::sleep_for(std::chrono::milliseconds{10});
                }
                throw StripeMetadataUnavailable("ClusterRuntime: STRIPE metadata quorum is unavailable");
            }

            void replicateStripeMetadata(
                std::span<const uint8_t> key,
                std::span<const uint8_t> metadata,
                bool preserveLogicalSequence = false
            ) {
                if (!stripeMetadataRaft_ || stripeMetadataRaft_->role() != NodeRole::PRIMARY) {
                    throw std::runtime_error("ClusterRuntime: local node is not the STRIPE metadata leader");
                }
                placementAuthority_->ensureGenesis();
                std::vector<uint8_t> keyCopy{'M'}; keyCopy.insert(keyCopy.end(), key.begin(), key.end());
                std::vector<uint8_t> metadataCopy{metadata.begin(), metadata.end()};
                auto submission = stripeMetadataRaft_->submitMutation(
                    [key = std::move(keyCopy), value = std::move(metadataCopy), source = selfNodeId_, preserveLogicalSequence](uint64_t) mutable {
                        return ClusterMutation{
                            .sourceNodeId = source,
                            .op = ReplOpType::PUT,
                            .recordFlags = static_cast<uint8_t>(preserveLogicalSequence ? 1 : 0),
                            .key = std::move(key),
                            .value = std::move(value),
                            .blob = std::nullopt,
                        };
                    }
                );
                detail::ReconfigurationDeadline::wait(submission.completion);
            }

            StripeControlResponse handleStripeControl(uint64_t requesterNodeId, const StripeControlRequest& request,
                const std::shared_ptr<const ReplicationServer::PeerSession>& session = {}) {
                StripeControlResponse response;
                response.requestId = request.requestId;
                if (session && !session->connected.load(std::memory_order_acquire)) {
                    response.status = StripeControlStatus::REJECTED;
                    return response;
                }
                if (placementAuthority_ && placementAuthority_->state(false).stripeWritesBlocked &&
                    (request.action == StripeControlAction::ACQUIRE || request.action == StripeControlAction::REPAIR_METADATA ||
                     request.action == StripeControlAction::ROLLBACK_KEY || request.action == StripeControlAction::ROLLBACK_STREAM ||
                     request.action == StripeControlAction::ROLLBACK_WATERMARK)) {
                    response.status = StripeControlStatus::RECONFIGURING; return response;
                }
                if (request.action == StripeControlAction::ROLLBACK_WATERMARK ||
                    request.action == StripeControlAction::ROLLBACK_KEY ||
                    request.action == StripeControlAction::ROLLBACK_STREAM ||
                    request.action == StripeControlAction::ROLLBACK_APPLY) {
                    if (!callbacks_.rollbackControl) {
                        response.status = StripeControlStatus::ERROR_STATUS;
                        return response;
                    }
                    return callbacks_.rollbackControl(requesterNodeId, request);
                }
                const auto placement = ClusterConfig::decode(placementAuthority_->state(false).activeConfig);
                const auto* failover = placement.stripeFailoverNode();
                if (!stripeMetadataRaft_ || stripeMetadataRaft_->role() != NodeRole::PRIMARY) {
                    response.status = StripeControlStatus::ERROR_STATUS;
                    return response;
                }
                if (config_.mode() != ReplicationMode::STRIPE || request.key.empty() || request.ownerNodeId == 0 ||
                    ownerForKey(request.key).nodeId != request.ownerNodeId) {
                    response.status = StripeControlStatus::REJECTED;
                    return response;
                }
                if (request.action == StripeControlAction::READ_METADATA) {
                    if (!callbacks_.readStripeMetadata) {
                        response.status = StripeControlStatus::ERROR_STATUS;
                        return response;
                    }
                    try { stripeMetadataRaft_->linearizableReadBarrier(); }
                    catch (...) {
                        response.status = StripeControlStatus::ERROR_STATUS;
                        return response;
                    }
                    if (auto metadata = callbacks_.readStripeMetadata(request.key)) {
                        response.status = StripeControlStatus::FOUND;
                        response.metadata = std::move(*metadata);
                    }
                    else { response.status = StripeControlStatus::NOT_FOUND; }
                    return response;
                }
                const std::string keyId{reinterpret_cast<const char*>(request.key.data()), request.key.size()};
                auto lock = detail::ReconfigurationDeadline::lock(stripeAuthorityMutex_);
                // Close the admission race with the committed final freeze.
                if (placementAuthority_->state(false).stripeWritesBlocked &&
                    (request.action == StripeControlAction::ACQUIRE || request.action == StripeControlAction::REPAIR_METADATA)) {
                    response.status = StripeControlStatus::RECONFIGURING; return response;
                }
                const auto metadataStats = stripeMetadataRaft_->stats();
                if (metadataStats.leaderNodeId != selfNodeId_) {
                    response.status = StripeControlStatus::ERROR_STATUS;
                    return response;
                }
                if (stripeAuthorityTerm_ != metadataStats.currentTerm) {
                    stripeAuthorityTerm_ = metadataStats.currentTerm;
                    std::erase_if(stripeLeases_, [](const auto& item) { return !item.second.publishing; });
                    stripeOwnerObservedOnline_.clear();
                }
                // A restarted/reconnected authority cannot release its old token.
                // Fence every lease from a dead connection before owner handoff,
                // new grants, repair, or commit; node reachability alone is not
                // enough because a new connection may already be live.
                std::erase_if(stripeLeases_, [](const auto& item) {
                    return !item.second.publishing && item.second.session && !item.second.session->connected.load(std::memory_order_acquire);
                });
                const auto now = std::chrono::steady_clock::now();
                if (requesterNodeId == request.ownerNodeId) { stripeOwnerObservedOnline_[request.ownerNodeId] = now; }
                const auto observed = stripeOwnerObservedOnline_.find(request.ownerNodeId);
                const bool recentlyObserved = observed != stripeOwnerObservedOnline_.end() &&
                    now - observed->second < std::chrono::milliseconds{effectiveConsistency_.ackTimeoutMs};
                const bool ownerOnline = requesterNodeId == request.ownerNodeId || stripeNodeReachable(request.ownerNodeId) || recentlyObserved;
                if (!ownerOnline) {
                    // Losing an owner fences every in-flight operation from
                    // that owner before failover starts, not just this key.
                    std::erase_if(stripeLeases_, [&](const auto& item) {
                        return !item.second.publishing && item.second.ownerNodeId == request.ownerNodeId &&
                               item.second.authorityNodeId == request.ownerNodeId;
                    });
                }
                auto existing = stripeLeases_.find(keyId);
                if (request.action == StripeControlAction::ACQUIRE &&
                    existing != stripeLeases_.end() && !existing->second.publishing && existing->second.authorityNodeId == requesterNodeId) {
                    // Owners and failover engines serialize operations for this key.
                    // A fresh acquire fences an abandoned grant, including one
                    // whose response was lost while the connection remained live.
                    stripeLeases_.erase(existing);
                    existing = stripeLeases_.end();
                }
                if (request.action == StripeControlAction::ACQUIRE) {
                    if (requesterNodeId != request.ownerNodeId && std::ranges::any_of(stripeLeases_, [&](const auto& item) {
                            return item.second.ownerNodeId == request.ownerNodeId && item.second.publishing &&
                                   item.second.authorityNodeId == request.ownerNodeId;
                        })) {
                        response.status = StripeControlStatus::BUSY;
                        return response;
                    }
                    if (requesterNodeId == request.ownerNodeId && std::ranges::any_of(stripeLeases_, [&](const auto& item) {
                            return item.second.ownerNodeId == request.ownerNodeId && failover &&
                                   item.second.authorityNodeId == failover->nodeId;
                        })) {
                        // Owner handoff is group-wide for that owner: wait for
                        // every failover read/write, even on other keys.
                        response.status = StripeControlStatus::BUSY;
                        return response;
                    }
                    if (existing != stripeLeases_.end()) {
                        response.status = StripeControlStatus::BUSY;
                        return response;
                    }
                    const uint64_t authority = ownerOnline ? request.ownerNodeId : (failover ? failover->nodeId : 0);
                    if (authority == 0) {
                        response.status = StripeControlStatus::REJECTED;
                        return response;
                    }
                    if (requesterNodeId != authority) {
                        response.status = StripeControlStatus::REJECTED;
                        return response;
                    }
                    StripeLeaseState state{
                        .ownerNodeId = request.ownerNodeId,
                        .authorityNodeId = authority,
                        .fenceToken = randomNonZeroU64(),
                        .session = session,
                    };
                    stripeLeases_.emplace(keyId, state);
                    response.status = StripeControlStatus::GRANTED;
                    response.authorityNodeId = authority;
                    response.fenceToken = state.fenceToken;
                    if (callbacks_.readStripeMetadata) {
                        if (auto metadata = callbacks_.readStripeMetadata(request.key)) { response.metadata = std::move(*metadata); }
                    }
                    return response;
                }
                // Keep the per-key reservation, including across term/session
                // changes and owner handoff, while releasing the shared map lock.
                const auto publish = [&](bool repair) {
                    if (repair) {
                        stripeLeases_.emplace(keyId, StripeLeaseState{.ownerNodeId = request.ownerNodeId,
                            .authorityNodeId = readLe64(request.metadata, 22), .fenceToken = request.fenceToken,
                            .session = session, .publishing = true});
                    }
                    else { stripeLeases_.at(keyId).publishing = true; }
                    lock.unlock();
                    bool committed = false;
                    try { replicateStripeMetadata(request.key, request.metadata, repair); committed = true; }
                    catch (...) { /* Report the existing control error contract below. */ }
                    lock.lock();
                    const auto pending = stripeLeases_.find(keyId);
                    if (pending != stripeLeases_.end() && pending->second.fenceToken == request.fenceToken) {
                        if (committed || repair) { stripeLeases_.erase(pending); }
                        else { pending->second.publishing = false; }
                    }
                    response.status = committed ? StripeControlStatus::COMMITTED : StripeControlStatus::ERROR_STATUS;
                };
                if (request.action == StripeControlAction::REPAIR_METADATA) {
                    if (existing != stripeLeases_.end() || !callbacks_.readStripeMetadata || request.metadata.size() < 62 ||
                        request.metadata[0] != 'A' || request.metadata[1] != 'K' || request.metadata[2] != 'S' ||
                        request.metadata[3] != 'M' || request.metadata[4] != '1' ||
                        readLe64(request.metadata, 6) != request.fenceToken ||
                        readLe64(request.metadata, 14) != request.ownerNodeId) {
                        response.status = existing != stripeLeases_.end() ? StripeControlStatus::BUSY : StripeControlStatus::REJECTED;
                        return response;
                    }
                    const auto current = callbacks_.readStripeMetadata(request.key);
                    if (!current || current->size() < 62 || readLe64(*current, 6) != request.fenceToken ||
                        readLe64(*current, 14) != request.ownerNodeId || readLe64(*current, 22) != readLe64(request.metadata, 22) ||
                        readLe64(*current, 30) != readLe64(request.metadata, 30)) {
                        response.status = StripeControlStatus::REJECTED;
                        return response;
                    }
                    publish(true);
                    return response;
                }
                if (existing == stripeLeases_.end() || existing->second.ownerNodeId != request.ownerNodeId ||
                    existing->second.authorityNodeId != requesterNodeId || existing->second.fenceToken != request.fenceToken) {
                    response.status = StripeControlStatus::REJECTED;
                    return response;
                }
                if (existing->second.publishing) { response.status = StripeControlStatus::BUSY; return response; }
                response.authorityNodeId = existing->second.authorityNodeId;
                response.fenceToken = existing->second.fenceToken;
                if (request.action == StripeControlAction::COMMIT) {
                    const bool validMetadata = request.metadata.size() >= 62 &&
                        request.metadata[0] == 'A' && request.metadata[1] == 'K' && request.metadata[2] == 'S' &&
                        request.metadata[3] == 'M' && request.metadata[4] == '1' &&
                        readLe64(request.metadata, 6) == request.fenceToken &&
                        readLe64(request.metadata, 14) == request.ownerNodeId &&
                        readLe64(request.metadata, 22) == requesterNodeId &&
                        readLe64(request.metadata, 30) == request.fenceToken;
                    if (!validMetadata) {
                        response.status = StripeControlStatus::ERROR_STATUS;
                        return response;
                    }
                    publish(false);
                    return response;
                }
                stripeLeases_.erase(existing);
                response.status = StripeControlStatus::RELEASED;
                return response;
            }

            void recordFailure(ClusterFailureCode failure, ClusterHealthState health) const noexcept {
                lastFailureAtUs_.store(observationNowUs(), std::memory_order_relaxed);
                lastFailure_.store(failure, std::memory_order_relaxed);
                if (health == ClusterHealthState::FAILED || health_.load(std::memory_order_relaxed) != ClusterHealthState::FAILED) {
                    health_.store(health, std::memory_order_relaxed);
                }
            }

            template <typename Endpoint>
            void startEndpoint(Endpoint& endpoint) {
                try { endpoint.start(); }
                catch (...) {
                    ++endpointStartFailures_;
                    recordFailure(ClusterFailureCode::ENDPOINT_START, ClusterHealthState::FAILED);
                    throw;
                }
            }

            void stopReplicationAfterManagerFailure() noexcept {
                try { stopReplication(); } catch (...) {}
            }

            NodeInfo ownerForKey(std::span<const uint8_t> key) const {
                if (config_.mode() == ReplicationMode::MIRROR && config_.primaryNodeId() != 0) {
                    const auto* primary = config_.findById(config_.primaryNodeId());
                    if (primary == nullptr) { throw std::runtime_error("ClusterRuntime: configured MIRROR Primary is missing"); }
                    return *primary;
                }
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

            ReadResponse readPrimaryEndpoint(std::span<const uint8_t> key, uint64_t snapshotSeq) {
                try {
                    if (mirrorAuthority_) { mirrorAuthority_->validateRead(runtimeOptions_.clusterGroupEpoch); }
                    if (runtimeOptions_.mirrorFencing.mode == MirrorFencingMode::EXTERNAL_FENCED) {
                        runtimeOptions_.mirrorFencing.external->validatePrimary(config_.clusterId(), runtimeOptions_.clusterGroupId,
                            selfNodeId_, runtimeOptions_.clusterGroupEpoch);
                    }
                    return readLocal(key, snapshotSeq);
                }
                catch (const std::runtime_error&) { ReadResponse response; response.status = ReadStatus::ERROR_STATUS; return response; }
            }

            ReadResponse readPeer(uint64_t nodeId, std::span<const uint8_t> key, uint64_t snapshotSeq) {
                const auto roundTripStarted = std::chrono::steady_clock::now();
                const auto deadline = detail::ReconfigurationDeadline::cap(std::chrono::steady_clock::now() + std::chrono::milliseconds(effectiveConsistency_.ackTimeoutMs));
                while (std::chrono::steady_clock::now() < deadline) {
                    try {
                        std::shared_ptr<ReplicationClient> client;
                        {
                            std::lock_guard lock{mutex_};
                            if (manager_ && manager_->primaryNodeId() == nodeId) { client = client_; }
                            if (!client) {
                                const auto it = peerClients_.find(nodeId);
                                if (it != peerClients_.end()) { client = it->second; }
                            }
                        }
                        if (client) {
                            const auto now = std::chrono::steady_clock::now();
                            const auto remaining = deadline > now
                                                       ? std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count()
                                                       : int64_t{0};
                            auto response = client->readKey(key, snapshotSeq, static_cast<uint32_t>(std::max<int64_t>(1, remaining)));
                            if (lastFailure_.load(std::memory_order_relaxed) == ClusterFailureCode::PEER_READ_TIMEOUT) {
                                health_.store(ClusterHealthState::HEALTHY, std::memory_order_relaxed);
                            }
                            recordPeerRoundTrip(nodeId, true, roundTripStarted);
                            return response;
                        }
                    }
                    catch (const std::runtime_error&) {}
                    std::this_thread::sleep_for(std::chrono::milliseconds{25});
                }
                ++peerReadTimeouts_;
                recordPeerRoundTrip(nodeId, false, roundTripStarted);
                recordFailure(ClusterFailureCode::PEER_READ_TIMEOUT, ClusterHealthState::DEGRADED);
                ReadResponse response;
                response.status = ReadStatus::ERROR_STATUS;
                return response;
            }

            void recordPeerRoundTrip(
                uint64_t nodeId,
                bool succeeded,
                std::chrono::steady_clock::time_point started
            ) {
                const auto finishedAtUs = observationNowUs();
                const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - started
                ).count();
                std::lock_guard lock{peerRoundTripMutex_};
                auto& metrics = peerRoundTrips_[nodeId];
                metrics.lastRoundTripAtUs = finishedAtUs;
                if (succeeded) {
                    ++metrics.succeeded;
                    metrics.consecutiveFailures = 0;
                    observeLatency(metrics.latencyUs, static_cast<uint64_t>(std::max<int64_t>(0, elapsed)));
                }
                else {
                    ++metrics.failed;
                    ++metrics.consecutiveFailures;
                    metrics.lastFailureAtUs = finishedAtUs;
                }
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

            std::unique_ptr<ReplicationClient> createPeerClient(
                const NodeInfo& peer,
                const ClusterRuntimeOptions& baseOptions
            ) {
                if (config_.mode() != ReplicationMode::STRIPE) { loadPeerProgress(peer.nodeId); }
                auto clientOptions = baseOptions;
                clientOptions.secure.expectedPrimaryNodeId = peer.nodeId;
                if (!clientOptions.clusterMembershipPath.empty()) {
                    clientOptions.clusterMembershipPath += ".peer-" + std::to_string(peer.nodeId);
                }
                auto client = ReplicationClient::create(
                    peer.host,
                    peer.replPort,
                    selfNodeId_,
                    [this, peerNodeId = peer.nodeId] {
                        return config_.mode() == ReplicationMode::STRIPE ? uint64_t{0} : lastSeqFromPeer(peerNodeId);
                    },
                    effectiveAckPolicy_,
                    std::move(clientOptions),
                    config_.mode() == ReplicationMode::STRIPE,
                    transferBudget_,
                    config_.mode() == ReplicationMode::PARTITIONED
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
                        if (callbacks_.partitionLeader) {
                            throw std::runtime_error("ClusterRuntime: partition discovery links do not accept data mutations");
                        }
                        const bool migrationSource = config_.mode() == ReplicationMode::STRIPE && placementAuthority_ &&
                            placementAuthority_->state(false).pendingGeneration != 0 && stripeMetadataLeaderNodeId() == peerNodeId;
                        const bool validSource = sourceNodeId == peerNodeId || sourceNodeId == ROLLBACK_SOURCE_NODE_ID || migrationSource;
                        if (config_.mode() == ReplicationMode::PARTITIONED && (!validSource || !partitionReceivesFrom(key, peerNodeId, selfNodeId_))) {
                            throw std::runtime_error("ClusterRuntime: partition entry violates placement");
                        }
                        if (!validSource) { return; }
                        if (callbacks_.apply) { callbacks_.apply(seq, op, key, value, recordFlags, sourceNodeId, 0); }
                        if (config_.mode() == ReplicationMode::PARTITIONED) {
                            // Peer progress is itself durable. Publish it only after
                            // the corresponding engine mutation is durable as well.
                            if (callbacks_.forceDurable) { callbacks_.forceDurable(); }
                            recordSeqFromPeer(peerNodeId, seq);
                        }
                    }
                );
                client->setForceDurableCallback(callbacks_.forceDurable);
                if (config_.mode() == ReplicationMode::PARTITIONED && callbacks_.installPartitionSnapshot) {
                    struct Incoming {
                        uint64_t seq = 0;
                        std::shared_ptr<detail::PartitionSnapshotSpool> spool;
                    };
                    auto incoming = std::make_shared<Incoming>();
                    client->setSnapshotCallbacks(
                        [this, incoming](uint64_t seq, uint64_t) {
                            incoming->spool.reset();
                            incoming->spool = std::make_shared<detail::PartitionSnapshotSpool>(transferBudget_);
                            incoming->seq = seq;
                        },
                        [this, incoming, peerNodeId = peer.nodeId](auto key, uint64_t size, uint32_t crc) {
                            if (!incoming->spool || !partitionReceivesFrom(key, peerNodeId, selfNodeId_)) {
                                throw std::runtime_error("ClusterRuntime: partition snapshot violates placement");
                            }
                            incoming->spool->begin(key, size, crc);
                        },
                        [incoming](uint64_t offset, auto chunk) { incoming->spool->append(offset, chunk); },
                        [incoming] { incoming->spool->finish(); },
                        [this, incoming, peerNodeId = peer.nodeId](uint64_t seq, uint64_t count) {
                            if (!incoming->spool || incoming->seq != seq) { throw std::runtime_error("ClusterRuntime: invalid partition snapshot"); }
                            ClusterSnapshot snapshot{.seq = seq,
                                .forEachEntry = [spool = incoming->spool, count](const auto& visitor) { return spool->replay(visitor, count); }};
                            callbacks_.installPartitionSnapshot(peerNodeId, snapshot);
                            callbacks_.forceDurable();
                            recordSeqFromPeer(peerNodeId, seq);
                            incoming->spool.reset();
                        }
                    );
                }
                return client;
            }

            bool partitionReceivesFrom(std::span<const uint8_t> key, uint64_t owner, uint64_t receiver) const {
                const auto targets = router_.writeTargets(key);
                return !targets.empty() && targets.front().nodeId == owner &&
                    std::ranges::any_of(targets, [&](const auto& node) { return node.nodeId == receiver; });
            }

            ReplicationServer::EntryTargets partitionEntryTargets() const {
                if (config_.mode() != ReplicationMode::PARTITIONED) { return {}; }
                return [this](std::span<const uint8_t> key) {
                    std::vector<uint64_t> replicas;
                    const auto targets = router_.writeTargets(key);
                    if (targets.empty() || targets.front().nodeId != selfNodeId_) { return replicas; }
                    for (const auto& node : targets) { if (node.nodeId != selfNodeId_) { replicas.push_back(node.nodeId); } }
                    return replicas;
                };
            }

            static std::vector<uint8_t> mirrorWireKey(std::span<const uint8_t> key, uint64_t epoch) {
                std::vector<uint8_t> bytes{'A', 'K', 'M', 'D', '1'};
                writeLe64(bytes, epoch); bytes.insert(bytes.end(), key.begin(), key.end());
                return bytes;
            }

            std::span<const uint8_t> mirrorPublicKey(std::span<const uint8_t> key, uint64_t primary) const {
                if (!mirrorAuthority_) { return key; }
                if (key.size() < 13 || std::string_view{reinterpret_cast<const char*>(key.data()), 5} != "AKMD1" ||
                    !mirrorAuthority_->accepts(primary, readLe64(key, 5))) {
                    throw std::runtime_error("MIRROR fencing: obsolete or unapproved data generation");
                }
                return key.subspan(13);
            }

            ReplicationServer::HistoryProvider makeHistoryProvider() const {
                if (!callbacks_.getEntries) {
                    if (mirrorAuthority_) {
                        return [](uint64_t, uint64_t) -> std::optional<std::vector<ReplEntry>> { return std::nullopt; };
                    }
                    return {};
                }
                return [this, getEntries = callbacks_.getEntries](
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
                        if (mirrorAuthority_) {
                            for (auto& entry : entries) { entry.key = mirrorWireKey(entry.key, runtimeOptions_.clusterGroupEpoch); }
                        }
                        return entries;
                    };
            }

            ReplicationServer::SnapshotProvider makeSnapshotProvider() const {
                if (!callbacks_.exportSnapshot) { return {}; }
                return [this, exportSnapshot = callbacks_.exportSnapshot]() -> std::optional<ReplicationServer::Snapshot> {
                    const auto source = exportSnapshot();
                    if (!source) { return std::nullopt; }
                    ReplicationServer::Snapshot snapshot;
                    snapshot.seq = source->seq;
                    const auto epoch = runtimeOptions_.clusterGroupEpoch;
                    const bool fenced = mirrorAuthority_ != nullptr;
                    snapshot.forEachEntry = [source = *source, epoch, fenced](const ReplicationServer::Snapshot::EntryVisitor& visitor) {
                        if (!source.forEachEntry) { return false; }
                        if (!fenced) { return source.forEachEntry(visitor); }
                        auto wrapped = visitor;
                        wrapped.fileEntry = {}; // fencing needs a generation on every public key
                        wrapped.beginEntry = [&](std::span<const uint8_t> key, uint64_t size, uint32_t crc) {
                            const auto wire = mirrorWireKey(key, epoch);
                            return visitor.beginEntry(wire, size, crc);
                        };
                        return source.forEachEntry(wrapped);
                    };
                    return snapshot;
                };
            }

            ClusterRuntimeOptions primaryEndpointOptions() {
                ClusterRuntimeOptions endpointOptions;
                {
                    std::lock_guard lock{mutex_};
                    endpointOptions = runtimeOptions_;
                }
                if (config_.mode() == ReplicationMode::MIRROR && endpointOptions.mirrorPromotion.enabled) {
                    auto pendingResync = endpointOptions.clusterMembershipPath;
                    pendingResync += ".mirror-resync";
                    if (std::filesystem::exists(pendingResync)) {
                        throw std::runtime_error("MIRROR promotion: candidate resynchronization is incomplete");
                    }
                    const auto& promotion = endpointOptions.mirrorPromotion;
                    const auto existing = loadGroupState(endpointOptions.clusterMembershipPath,
                        CorruptClusterStateAction::FAIL_STARTUP, "MIRROR promotion");
                    if (!existing || (endpointOptions.clusterGroupId != 0 && existing->groupId != endpointOptions.clusterGroupId)) {
                        throw std::runtime_error("MIRROR promotion: existing group identity is required");
                    }
                    std::function<void()> externalFence;
                    if (endpointOptions.mirrorFencing.external) {
                        externalFence = [&, existing] {
                            endpointOptions.mirrorFencing.external->fence(MirrorFenceRequest{
                                .clusterId = config_.clusterId(), .groupId = existing->groupId,
                                .previousPrimaryNodeId = promotion.previousPrimaryNodeId,
                                .previousGroupEpoch = promotion.previousGroupEpoch,
                                .candidateNodeId = selfNodeId_, .expectedDurableSeq = promotion.expectedDurableSeq});
                        };
                    }
                    if (existing->primaryNodeId != selfNodeId_) {
                        if (existing->primaryNodeId != promotion.previousPrimaryNodeId || existing->groupEpoch != promotion.previousGroupEpoch ||
                            !callbacks_.forceDurable || !callbacks_.getLastSeq) { throw std::runtime_error("MIRROR promotion: invalid source membership"); }
                        callbacks_.forceDurable();
                        if (callbacks_.getLastSeq() != promotion.expectedDurableSeq) { throw std::runtime_error("MIRROR promotion: stale durable candidate"); }
                        if (mirrorAuthority_) { mirrorAuthority_->promote(promotion.previousPrimaryNodeId, promotion.previousGroupEpoch, promotion.expectedDurableSeq); }
                        else if (endpointOptions.mirrorFencing.mode == MirrorFencingMode::EXTERNAL_FENCED) {
                            externalFence();
                        }
                        else { throw std::runtime_error("MIRROR promotion: STATIC policy forbids promotion"); }
                    }
                    else if (mirrorAuthority_) { mirrorAuthority_->promote(promotion.previousPrimaryNodeId, promotion.previousGroupEpoch, promotion.expectedDurableSeq); }
                }
                const auto groupState = loadOrCreatePrimaryGroup(
                    endpointOptions.clusterMembershipPath,
                    selfNodeId_,
                    endpointOptions.clusterGroupId,
                    endpointOptions.clusterGroupEpoch,
                    endpointOptions.corruptStateAction,
                    endpointOptions.mirrorPromotion,
                    callbacks_.getLastSeq,
                    callbacks_.forceDurable
                );
                endpointOptions.clusterGroupId = groupState.groupId;
                endpointOptions.clusterGroupEpoch = groupState.groupEpoch;
                if (config_.mode() == ReplicationMode::MIRROR && endpointOptions.mirrorFencing.mode == MirrorFencingMode::EXTERNAL_FENCED) {
                    endpointOptions.mirrorFencing.external->validatePrimary(config_.clusterId(), groupState.groupId,
                        selfNodeId_, groupState.groupEpoch);
                }
                {
                    std::lock_guard lock{mutex_};
                    runtimeOptions_.clusterGroupId = groupState.groupId;
                    runtimeOptions_.clusterGroupEpoch = groupState.groupEpoch;
                }
                return endpointOptions;
            }

            ReplicationEndpoints makePartitionedEndpoints() {
                const auto* self = config_.findById(selfNodeId_);
                if (self == nullptr || !self->dataBearing()) {
                    throw std::runtime_error("ClusterRuntime: partitioned/stripe runtime requires self to be data-bearing");
                }

                auto endpointOptions = primaryEndpointOptions();
                ReplicationEndpoints endpoints;
                endpoints.server = ReplicationServer::create(
                    self->replPort,
                    selfNodeId_,
                    callbacks_.getCurrentSeq,
                    effectiveAckPolicy_,
                    effectiveConsistency_,
                    configuredReplicaCount_,
                    configuredReplicaNodeIds(config_, selfNodeId_),
                    endpointOptions,
                    config_.mode() == ReplicationMode::PARTITIONED ? makeHistoryProvider() : ReplicationServer::HistoryProvider{},
                    config_.mode() == ReplicationMode::PARTITIONED ? makeSnapshotProvider() : ReplicationServer::SnapshotProvider{},
                    transferBudget_,
                    partitionEntryTargets()
                );
                endpoints.server->setForwardCallback(callbacks_.forward);
                endpoints.server->setReadCallback(
                    [this](const ReadRequest& request) {
                        return readLocal(
                            std::span<const uint8_t>{request.key.data(), request.key.size()},
                            request.snapshotSeq
                        );
                    }
                );
                endpoints.server->setStripeControlCallback(
                    [this](uint64_t peerNodeId, const StripeControlRequest& request,
                        const std::shared_ptr<const ReplicationServer::PeerSession>& session) {
                        return handleStripeControl(peerNodeId, request, session);
                    }
                );
                startEndpoint(*endpoints.server);

                for (const auto& peer : config_.dataNodes()) {
                    if (peer.nodeId == selfNodeId_) { continue; }
                    auto client = createPeerClient(peer, endpointOptions);
                    startEndpoint(*client);
                    endpoints.peers.emplace(peer.nodeId, std::move(client));
                }
                return endpoints;
            }

            void installPartitioned() {
                std::lock_guard transitionLock{endpointTransitionMutex_};
                closeReplicationEndpoints(detachReplication());
                {
                    std::lock_guard lock{mutex_};
                    if (!started_) { return; }
                }
                auto endpoints = makePartitionedEndpoints();
                {
                    std::lock_guard lock{mutex_};
                    if (started_) {
                        server_ = std::move(endpoints.server);
                        client_ = std::move(endpoints.client);
                        peerClients_ = std::move(endpoints.peers);
                    }
                }
                closeReplicationEndpoints(std::move(endpoints));
            }

            ReplicationEndpoints makeRoleEndpoints(NodeRole role) {
                ReplicationEndpoints endpoints;
                if (role == NodeRole::PRIMARY) {
                    if (config_.mode() == ReplicationMode::MIRROR && config_.primaryNodeId() != 0 &&
                        config_.primaryNodeId() != selfNodeId_) {
                        throw std::runtime_error("ClusterRuntime: only the configured MIRROR Primary may install the PRIMARY role");
                    }
                    const auto* self = config_.findById(selfNodeId_);
                    if (!self && !config_.isStandalone()) { throw std::runtime_error("ClusterRuntime: self node is missing from config"); }
                    auto endpointOptions = primaryEndpointOptions();
                    const uint16_t replPort = self ? self->replPort : 0;
                    endpoints.server = ReplicationServer::create(
                        replPort,
                        selfNodeId_,
                        callbacks_.getCurrentSeq,
                        effectiveAckPolicy_,
                        effectiveConsistency_,
                        configuredReplicaCount_,
                        configuredReplicaNodeIds(config_, selfNodeId_),
                        endpointOptions,
                        makeHistoryProvider(),
                        makeSnapshotProvider(),
                        transferBudget_
                    );
                    endpoints.server->setForwardCallback(callbacks_.forward);
                    endpoints.server->setReadCallback(
                        [this](const ReadRequest& request) {
                            return readPrimaryEndpoint(
                                std::span<const uint8_t>{request.key.data(), request.key.size()},
                                request.snapshotSeq
                            );
                        }
                    );
                    startEndpoint(*endpoints.server);
                }
                else if (role == NodeRole::REPLICA) {
                    if (mirrorAuthority_ && !config_.findById(selfNodeId_)->dataBearing()) { return endpoints; }
                    ClusterRuntimeOptions clientOptions;
                    {
                        std::lock_guard lock{mutex_};
                        clientOptions = runtimeOptions_;
                    }
                    clientOptions.secure.expectedPrimaryNodeId = manager_->primaryNodeId();
                    endpoints.client = ReplicationClient::create(
                        manager_->primaryHost(),
                        manager_->primaryReplPort(),
                        selfNodeId_,
                        callbacks_.getLastSeq,
                        effectiveAckPolicy_,
                        std::move(clientOptions),
                        false,
                        transferBudget_
                    );
                    endpoints.client->setApplyCallback([this](uint64_t seq, ReplOpType op, std::span<const uint8_t> key,
                        std::span<const uint8_t> value, uint8_t flags, uint64_t source) {
                        const auto publicKey = mirrorPublicKey(key, manager_->primaryNodeId());
                        callbacks_.apply(seq, op, publicKey, value, flags, source, 0);
                    });
                    endpoints.client->setSnapshotCallbacks(
                        callbacks_.beginSnapshot,
                        [this](std::span<const uint8_t> key, uint64_t size, uint32_t crc) {
                            const auto publicKey = mirrorPublicKey(key, manager_->primaryNodeId());
                            callbacks_.beginSnapshotEntry(publicKey, size, crc);
                        },
                        callbacks_.appendSnapshotEntryChunk,
                        callbacks_.finishSnapshotEntry,
                        callbacks_.finishSnapshot
                    );
                    endpoints.client->setForceDurableCallback(callbacks_.forceDurable);
                    endpoints.client->setBlobCallbacks(callbacks_.beginBlob, callbacks_.appendBlobChunk, callbacks_.finishBlob);
                    startEndpoint(*endpoints.client);
                }
                return endpoints;
            }

            void installRole(NodeRole role) {
                std::lock_guard transitionLock{endpointTransitionMutex_};
                closeReplicationEndpoints(detachReplication());
                {
                    std::lock_guard lock{mutex_};
                    if (!started_) { return; }
                }
                auto endpoints = makeRoleEndpoints(role);
                {
                    std::lock_guard lock{mutex_};
                    if (started_) {
                        server_ = std::move(endpoints.server);
                        client_ = std::move(endpoints.client);
                        peerClients_ = std::move(endpoints.peers);
                    }
                }
                closeReplicationEndpoints(std::move(endpoints));
            }

            ReplicationEndpoints detachReplicationLocked() {
                return ReplicationEndpoints{
                    .server = std::move(server_),
                    .client = std::move(client_),
                    .peers = std::move(peerClients_),
                };
            }

            ReplicationEndpoints detachReplication() {
                std::lock_guard lock{mutex_};
                return detachReplicationLocked();
            }

            static void closeReplicationEndpoints(ReplicationEndpoints endpoints) {
                // Endpoint shutdown joins workers whose callbacks can re-enter
                // ClusterRuntime. The runtime state mutex must never be held
                // while these joins are in progress.
                if (endpoints.client) { endpoints.client->close(); }
                for (auto& [_, peer] : endpoints.peers) {
                    if (peer) { peer->close(); }
                }
                if (endpoints.server) { endpoints.server->close(); }
            }

            void stopReplication() {
                std::lock_guard transitionLock{endpointTransitionMutex_};
                closeReplicationEndpoints(detachReplication());
            }

            std::filesystem::path dbDir_;
            ClusterConfig config_;
            ClusterRouter router_;
            std::unique_ptr<ClusterManager> manager_;
            std::unique_ptr<RaftConsensusRuntime> raftRuntime_;
            std::unique_ptr<MirrorAuthority> mirrorAuthority_;
            RaftConsensusRuntime* stripeMetadataRaft_ = nullptr;
            uint64_t selfNodeId_;
            ClusterEngineCallbacks callbacks_;
            ClusterRuntimeOptions runtimeOptions_;
            std::shared_ptr<detail::TransferBudget> transferBudget_;
            AckPolicy effectiveAckPolicy_;
            ConsistencyOptions effectiveConsistency_;
            uint16_t configuredReplicaCount_ = 0;

            std::unique_ptr<PlacementAuthority> placementAuthority_;
            std::timed_mutex endpointTransitionMutex_;
            mutable std::mutex mutex_;
            mutable std::mutex peerSeqMutex_;
            std::unordered_map<uint64_t, uint64_t> lastSeqByPeer_;
            std::unordered_map<uint64_t, uint64_t> nextSeqByTarget_;
            bool started_ = false;
            mutable std::atomic<ClusterHealthState> health_{ClusterHealthState::HEALTHY};
            mutable std::atomic<ClusterFailureCode> lastFailure_{ClusterFailureCode::NONE};
            mutable std::atomic<uint64_t> lastFailureAtUs_{0};
            std::atomic<uint64_t> leaseRenewFailures_{0};
            std::atomic<uint64_t> endpointStartFailures_{0};
            mutable std::atomic<uint64_t> peerReadTimeouts_{0};
            std::atomic<uint64_t> runtimeStartedAtUs_{0};
            struct PeerRoundTripMetrics {
                uint64_t succeeded = 0;
                uint64_t failed = 0;
                uint64_t consecutiveFailures = 0;
                uint64_t lastRoundTripAtUs = 0;
                uint64_t lastFailureAtUs = 0;
                ClusterLatencyHistogram latencyUs;
            };
            mutable std::mutex peerRoundTripMutex_;
            std::unordered_map<uint64_t, PeerRoundTripMetrics> peerRoundTrips_;
            std::shared_ptr<ReplicationServer> server_;
            std::shared_ptr<ReplicationClient> client_;
            std::unordered_map<uint64_t, std::shared_ptr<ReplicationClient>> peerClients_;
            std::timed_mutex stripeAuthorityMutex_;
            uint64_t stripeAuthorityTerm_ = 0;
            std::unordered_map<std::string, StripeLeaseState> stripeLeases_;
            std::unordered_map<uint64_t, std::chrono::steady_clock::time_point> stripeOwnerObservedOnline_;
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
    RaftRuntimeStats ClusterRuntime::raftStats() const { return impl_->raftStats(); }
    RaftRuntimeStats ClusterRuntime::stripeMetadataRaftStats() const { return impl_->stripeMetadataRaftStats(); }

    NodeRole ClusterRuntime::role() const noexcept { return impl_->role(); }

    std::vector<NodeInfo> ClusterRuntime::activeNodes() const { return impl_->activeNodes(); }

    bool ClusterRuntime::ownsWriteKey(std::span<const uint8_t> key) const { return impl_->ownsWriteKey(key); }
    ClusterRouteTarget ClusterRuntime::routeTarget(std::span<const uint8_t> key) const { return impl_->routeTarget(key); }
    ForwardResponse ClusterRuntime::forwardTo(uint64_t nodeId, ForwardRequest request) { return impl_->forwardTo(nodeId, std::move(request)); }
    void ClusterRuntime::queryReadBarrier() { impl_->queryReadBarrier(); }
    ReadResponse ClusterRuntime::linearizableReadKey(std::span<const uint8_t> key) { return impl_->linearizableReadKey(key); }

    uint64_t ClusterRuntime::ownerNodeId(std::span<const uint8_t> key) const { return impl_->ownerNodeId(key); }

    uint64_t ClusterRuntime::configurationEpoch() const noexcept { return impl_->configurationEpoch(); }

    uint64_t ClusterRuntime::stripeMetadataLinearizableWatermark() {
        return impl_->stripeMetadataLinearizableWatermark();
    }

    StripeControlResponse ClusterRuntime::rollbackControl(uint64_t targetNodeId, StripeControlRequest request) {
        return impl_->rollbackControl(targetNodeId, std::move(request));
    }

    ReadResponse ClusterRuntime::readKey(std::span<const uint8_t> key, uint64_t snapshotSeq) { return impl_->readKey(key, snapshotSeq); }

    ReadResponse ClusterRuntime::readKeyFromNode(uint64_t nodeId, std::span<const uint8_t> key, uint64_t snapshotSeq) {
        return impl_->readKeyFromNode(nodeId, key, snapshotSeq);
    }
    StripeOperationLease ClusterRuntime::acquireStripeOperation(std::span<const uint8_t> key, uint64_t ownerNodeId) {
        return impl_->acquireStripeOperation(key, ownerNodeId);
    }
    void ClusterRuntime::commitStripeMetadata(const StripeOperationLease& lease, std::span<const uint8_t> key,
        std::span<const uint8_t> metadata) { impl_->commitStripeMetadata(lease, key, metadata); }
    void ClusterRuntime::releaseStripeOperation(const StripeOperationLease& lease, std::span<const uint8_t> key) noexcept {
        impl_->releaseStripeOperation(lease, key);
    }
    uint64_t ClusterRuntime::stripeFailoverNodeId() const noexcept { return impl_->stripeFailoverNodeId(); }
    uint64_t ClusterRuntime::stripeMetadataLeaderNodeId() const noexcept { return impl_->stripeMetadataLeaderNodeId(); }
    bool ClusterRuntime::stripeNodeReachable(uint64_t nodeId) const noexcept { return impl_->stripeNodeReachable(nodeId); }
    std::optional<std::vector<uint8_t>> ClusterRuntime::readStripeMetadata(std::span<const uint8_t> key, uint64_t ownerNodeId) {
        return impl_->readStripeMetadata(key, ownerNodeId);
    }
    bool ClusterRuntime::repairStripeMetadata(std::span<const uint8_t> key, uint64_t expectedVersion,
        std::span<const uint8_t> metadata) {
        return impl_->repairStripeMetadata(key, expectedVersion, metadata);
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

    std::future<void> ClusterRuntime::submitEntry(uint64_t seq, ReplOpType op, std::span<const uint8_t> key,
        std::span<const uint8_t> value, uint8_t flags, uint64_t source) {
        return impl_->submitEntry(seq, op, key, value, flags, source);
    }

    ClusterMutationSubmission ClusterRuntime::submitMutation(ClusterMutationFactory prepare) {
        return impl_->submitMutation(std::move(prepare));
    }

    std::shared_future<ClusterRequestResult> ClusterRuntime::submitRequest(const ClusterRequestId& id,
        const std::array<uint8_t, 32>& fingerprint, ClusterMutationFactory prepare) {
        return impl_->submitRequest(id, fingerprint, std::move(prepare));
    }
    ClusterRequestResult ClusterRuntime::queryRequest(const ClusterRequestId& id) { return impl_->queryRequest(id); }

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
    void ClusterRuntime::addRaftLearner(const NodeInfo& node) { impl_->addRaftLearner(node); }
    void ClusterRuntime::executePrimaryWrite(const std::function<void()>& write) { impl_->executePrimaryWrite(write); }
    bool ClusterRuntime::recoverMirrorWrite() { return impl_->recoverMirrorWrite(); }
    void ClusterRuntime::cancelMirrorRecovery() noexcept { impl_->cancelMirrorRecovery(); }
    MirrorRecoveryMode ClusterRuntime::mirrorRecoveryMode() const noexcept { return impl_->mirrorRecoveryMode(); }
    MirrorFencingMode ClusterRuntime::mirrorFencingMode() const noexcept { return impl_->mirrorFencingMode(); }
    void ClusterRuntime::transferMirrorAuthorityLeadership(uint64_t nodeId) { impl_->transferMirrorAuthorityLeadership(nodeId); }
    void ClusterRuntime::promoteRaftLearner(uint64_t nodeId) { impl_->promoteRaftLearner(nodeId); }
    void ClusterRuntime::removeRaftLearner(uint64_t nodeId) { impl_->removeRaftLearner(nodeId); }

    void ClusterRuntime::removeRaftVotingNode(uint64_t nodeId) { impl_->removeRaftVotingNode(nodeId); }

    void ClusterRuntime::transferRaftLeadership(uint64_t targetNodeId) { impl_->transferRaftLeadership(targetNodeId); }

    void ClusterRuntime::campaignLeadership() { impl_->campaignLeadership(); }

    void ClusterRuntime::reconfigure(ClusterConfig config, uint64_t generation) { impl_->reconfigure(std::move(config), generation); }
    void ClusterRuntime::preparePlacementNodes(const ClusterConfig& config) { impl_->preparePlacementNodes(config); }
    void ClusterRuntime::waitForStripeOperations() { impl_->waitForStripeOperations(); }
    void ClusterRuntime::publishStripePlacementAlias(std::span<const uint8_t> key, std::span<const uint8_t> metadata, uint64_t generation) {
        impl_->publishStripePlacementAlias(key, metadata, generation);
    }
    ClusterPlacementState ClusterRuntime::placementState(bool fresh) { return impl_->placementState(fresh); }
    uint64_t ClusterRuntime::beginPlacementChange(const ClusterConfig& config) { return impl_->beginPlacementChange(config); }
    void ClusterRuntime::freezeStripePlacement(uint64_t generation) { impl_->freezeStripePlacement(generation); }
    void ClusterRuntime::cancelPlacementChange(uint64_t generation) { impl_->cancelPlacementChange(generation); }
    void ClusterRuntime::finishPlacementChange(uint64_t generation) { impl_->finishPlacementChange(generation); }
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

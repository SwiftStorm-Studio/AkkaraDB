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
#include "akk/engine/cluster/detail/ReplicationTransfer.hpp"
#include "akk/cpu/CRC32C.hpp"

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
            if (config.mode() == ReplicationMode::STRIPE) {
                return AckPolicy{.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::DURABLE};
            }
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
                case WriteConsistency::AVAILABLE_REPLICAS:
                    return AckPolicy{.mode = AckPolicyMode::ALL_TARGETS, .stage = legacy.stage};
            }
            throw std::invalid_argument("ClusterRuntime: invalid write consistency");
        }

        ConsistencyOptions effectiveConsistency(const ClusterConfig& config) {
            auto consistency = config.consistency();
            if (consistency.mode == ConsistencyMode::RAFT_QUORUM || config.mode() == ReplicationMode::STRIPE) {
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
                node.capabilities = DATA_BEARING | COORDINATOR_ELIGIBLE;
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
                RaftOptions{},
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
                if (runtimeOptions_.transportMode == TransportMode::SECURE && runtimeOptions_.secure.identitySeedPath.empty() && !dbDir.
                    empty()) { runtimeOptions_.secure.identitySeedPath = dbDir / "cluster.identity"; }
                if (runtimeOptions_.clusterMembershipPath.empty() && !dbDir.empty() && config_.consistency().mode !=
                    ConsistencyMode::RAFT_QUORUM) { runtimeOptions_.clusterMembershipPath = dbDir / "cluster.membership"; }
                validateTransportScope(config_, runtimeOptions_);
                validateRuntimePlacement(config_);
                validateRuntimeOptions(config_, runtimeOptions_);
                if (config_.mode() == ReplicationMode::PARTITIONED && (!callbacks_.apply || !callbacks_.forceDurable)) {
                    throw std::invalid_argument("ClusterRuntime: PARTITIONED requires apply and forceDurable callbacks");
                }
                if (config_.consistency().mode == ConsistencyMode::RAFT_QUORUM) {
                    raftRuntime_ = RaftConsensusRuntime::create(dbDir, config_, selfNodeId_, callbacks_, runtimeOptions_);
                    return;
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
                    ClusterEngineCallbacks metadataCallbacks;
                    metadataCallbacks.apply = [this](
                        uint64_t, ReplOpType op, std::span<const uint8_t> key, std::span<const uint8_t> value,
                        uint8_t, uint64_t
                    ) {
                        if (op != ReplOpType::PUT || key.empty() || value.empty()) {
                            throw std::runtime_error("ClusterRuntime: invalid STRIPE metadata Raft mutation");
                        }
                        callbacks_.commitStripeMetadata(key, value);
                    };
                    metadataCallbacks.forceDurable = callbacks_.forceDurable;
                    if (callbacks_.exportStripeMetadataSnapshot) {
                        metadataCallbacks.exportSnapshot = [this]() -> std::optional<ClusterSnapshot> {
                            if (!stripeMetadataRaft_) { return std::nullopt; }
                            const uint64_t sequence = stripeMetadataRaft_->stats().appliedStateMachineSeq;
                            if (sequence == 0) { return std::nullopt; }
                            return callbacks_.exportStripeMetadataSnapshot(sequence);
                        };
                        metadataCallbacks.beginSnapshot = [this](uint64_t, uint64_t) {
                            stripeMetadataSnapshotKey_.clear();
                            stripeMetadataSnapshotValue_.clear();
                            stripeMetadataSnapshotValueSize_ = 0;
                            stripeMetadataSnapshotValueCrc32c_ = 0;
                        };
                        metadataCallbacks.beginSnapshotEntry = [this](
                            std::span<const uint8_t> key, uint64_t valueSize, uint32_t valueCrc32c
                        ) {
                            if (key.empty() || valueSize > ReplFrameHeader::MAX_RAFT_PAYLOAD_SIZE) {
                                throw std::runtime_error("ClusterRuntime: invalid STRIPE metadata snapshot entry");
                            }
                            stripeMetadataSnapshotKey_.assign(key.begin(), key.end());
                            stripeMetadataSnapshotValue_.clear();
                            stripeMetadataSnapshotValue_.reserve(static_cast<size_t>(valueSize));
                            stripeMetadataSnapshotValueSize_ = valueSize;
                            stripeMetadataSnapshotValueCrc32c_ = valueCrc32c;
                        };
                        metadataCallbacks.appendSnapshotEntryChunk = [this](uint64_t offset, std::span<const uint8_t> chunk) {
                            if (offset != stripeMetadataSnapshotValue_.size() ||
                                offset > stripeMetadataSnapshotValueSize_ ||
                                chunk.size() > stripeMetadataSnapshotValueSize_ - offset) {
                                throw std::runtime_error("ClusterRuntime: invalid STRIPE metadata snapshot chunk");
                            }
                            stripeMetadataSnapshotValue_.insert(
                                stripeMetadataSnapshotValue_.end(), chunk.begin(), chunk.end()
                            );
                        };
                        metadataCallbacks.finishSnapshotEntry = [this] {
                            if (stripeMetadataSnapshotValue_.size() != stripeMetadataSnapshotValueSize_ ||
                                cpu::CRC32C(
                                    reinterpret_cast<const std::byte*>(stripeMetadataSnapshotValue_.data()),
                                    stripeMetadataSnapshotValue_.size()
                                ) != stripeMetadataSnapshotValueCrc32c_) {
                                throw std::runtime_error("ClusterRuntime: corrupt STRIPE metadata snapshot entry");
                            }
                            callbacks_.commitStripeMetadata(stripeMetadataSnapshotKey_, stripeMetadataSnapshotValue_);
                            stripeMetadataSnapshotKey_.clear();
                            stripeMetadataSnapshotValue_.clear();
                        };
                        metadataCallbacks.finishSnapshot = [this](uint64_t, uint64_t) {
                            if (callbacks_.forceDurable) { callbacks_.forceDurable(); }
                        };
                        // Each entry is durably committed before Raft persists
                        // the install intent, so intent recovery has no staging
                        // work left to perform.
                        metadataCallbacks.recoverSnapshot = [](uint64_t) {};
                        metadataCallbacks.isSnapshotDurable = [](uint64_t) { return true; };
                    }

                    auto metadataOptions = runtimeOptions_;
                    metadataOptions.requests.enabled = false;
                    metadataOptions.raftBlobPolicy = RaftBlobPolicy::REJECT;
                    metadataOptions.primaryHost.clear();
                    metadataOptions.primaryReplPort = 0;
                    metadataOptions.primaryNodeId = 0;
                    metadataOptions.clusterMembershipPath.clear();
                    metadataOptions.resetClusterMembership = false;
                    metadataOptions.mirrorPromotion = {};
                    metadataOptions.secure.expectedPrimaryNodeId = 0;
                    const auto metadataDir = dbDir_ / "stripe-metadata-raft";
                    metadataOptions.transfer.spoolDirectory = metadataDir / "transfer-spool";
                    stripeMetadataRaft_ = RaftConsensusRuntime::create(
                        metadataDir,
                        stripeMetadataRaftConfig(config_),
                        selfNodeId_,
                        std::move(metadataCallbacks),
                        std::move(metadataOptions)
                    );
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
                    if (stripeMetadataRaft_) { stripeMetadataRaft_->start(); }
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
                if (stripeMetadataRaft_) { stripeMetadataRaft_->close(); }
            }

            RaftRuntimeStats raftStats() const {
                if (raftRuntime_) { return raftRuntime_->stats(); }
                RaftRuntimeStats out;
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
                if (config_.mode() == ReplicationMode::PARTITIONED || config_.mode() == ReplicationMode::STRIPE) { return config_.dataNodes(); }
                return manager_ ? manager_->activeNodes() : std::vector<NodeInfo>{};
            }

            const ClusterRouter& router() const noexcept { return raftRuntime_ ? raftRuntime_->router() : router_; }

            bool ownsWriteKey(std::span<const uint8_t> key) const {
                if (raftRuntime_) { return raftRuntime_->role() == NodeRole::PRIMARY; }
                if (manager_) { manager_->checkHealth(); }
                if (config_.isStandalone()) { return true; }
                if (config_.mode() == ReplicationMode::MIRROR) { return manager_ && manager_->role() == NodeRole::PRIMARY; }
                if (config_.mode() == ReplicationMode::PARTITIONED || config_.mode() == ReplicationMode::STRIPE) {
                    const uint64_t owner = ownerForKey(key).nodeId;
                    if (owner == selfNodeId_) { return true; }
                    const auto* failover = config_.stripeFailoverNode();
                    return config_.mode() == ReplicationMode::STRIPE && failover && failover->nodeId == selfNodeId_ &&
                           !stripeNodeReachable(owner);
                }
                return false;
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
                if (manager_) { manager_->checkHealth(); }
                if (nodeId == selfNodeId_) { return readLocal(key, snapshotSeq); }
                return readPeer(nodeId, key, snapshotSeq);
            }

            uint64_t stripeFailoverNodeId() const noexcept {
                const auto* node = config_.stripeFailoverNode();
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
                const auto deadline = stripeMetadataDeadline();
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
                    if (response.status != StripeControlStatus::BUSY) {
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
                if (response.status == StripeControlStatus::BUSY || response.status == StripeControlStatus::REJECTED) { return false; }
                throw std::runtime_error("ClusterRuntime: STRIPE repair metadata commit failed");
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
                if (server) { server->shipEntry(seq, op, key, value, recordFlags, sourceNodeId); }
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
                if (raftRuntime_) {
                    raftRuntime_->shipEntry(seq, op, key, value, recordFlags, sourceNodeId);
                    return;
                }
                if (manager_) { manager_->checkHealth(); }
                if (targetNodeId == selfNodeId_) { return; }
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
            struct ReplicationEndpoints {
                std::shared_ptr<ReplicationServer> server;
                std::shared_ptr<ReplicationClient> client;
                std::unordered_map<uint64_t, std::shared_ptr<ReplicationClient>> peers;
            };

            struct StripeLeaseState {
                uint64_t ownerNodeId = 0;
                uint64_t authorityNodeId = 0;
                uint64_t fenceToken = 0;
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
                while (std::chrono::steady_clock::now() < deadline) {
                    const uint64_t leader = stripeMetadataLeaderNodeId();
                    if (leader == 0) {
                        std::this_thread::sleep_for(std::chrono::milliseconds{10});
                        continue;
                    }
                    try {
                        auto response = leader == selfNodeId_
                                            ? handleStripeControl(selfNodeId_, request)
                                            : sendStripeControl(leader, request);
                        if (response.status != StripeControlStatus::ERROR_STATUS) { return response; }
                    }
                    catch (...) {}
                    std::this_thread::sleep_for(std::chrono::milliseconds{10});
                }
                throw StripeMetadataUnavailable("ClusterRuntime: STRIPE metadata quorum is unavailable");
            }

            void replicateStripeMetadata(std::span<const uint8_t> key, std::span<const uint8_t> metadata) {
                if (!stripeMetadataRaft_ || stripeMetadataRaft_->role() != NodeRole::PRIMARY) {
                    throw std::runtime_error("ClusterRuntime: local node is not the STRIPE metadata leader");
                }
                std::vector<uint8_t> keyCopy{key.begin(), key.end()};
                std::vector<uint8_t> metadataCopy{metadata.begin(), metadata.end()};
                auto submission = stripeMetadataRaft_->submitMutation(
                    [key = std::move(keyCopy), value = std::move(metadataCopy), source = selfNodeId_](uint64_t) mutable {
                        return ClusterMutation{
                            .sourceNodeId = source,
                            .op = ReplOpType::PUT,
                            .recordFlags = 0,
                            .key = std::move(key),
                            .value = std::move(value),
                            .blob = std::nullopt,
                        };
                    }
                );
                submission.completion.get();
            }

            StripeControlResponse handleStripeControl(uint64_t requesterNodeId, const StripeControlRequest& request) {
                StripeControlResponse response;
                response.requestId = request.requestId;
                const auto* failover = config_.stripeFailoverNode();
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
                std::lock_guard lock{stripeAuthorityMutex_};
                const auto metadataStats = stripeMetadataRaft_->stats();
                if (metadataStats.leaderNodeId != selfNodeId_) {
                    response.status = StripeControlStatus::ERROR_STATUS;
                    return response;
                }
                if (stripeAuthorityTerm_ != metadataStats.currentTerm) {
                    stripeAuthorityTerm_ = metadataStats.currentTerm;
                    stripeLeases_.clear();
                    stripeOwnerObservedOnline_.clear();
                }
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
                        return item.second.ownerNodeId == request.ownerNodeId &&
                               item.second.authorityNodeId == request.ownerNodeId;
                    });
                }
                auto existing = stripeLeases_.find(keyId);
                if (request.action == StripeControlAction::ACQUIRE && requesterNodeId == request.ownerNodeId &&
                    existing != stripeLeases_.end() && existing->second.authorityNodeId == requesterNodeId) {
                    // The engine serializes STRIPE operations locally, so a
                    // new acquire proves this is an abandoned prior request.
                    stripeLeases_.erase(existing);
                    existing = stripeLeases_.end();
                }
                if (request.action == StripeControlAction::ACQUIRE) {
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
                if (request.action == StripeControlAction::REPAIR_METADATA) {
                    if (existing != stripeLeases_.end() || !callbacks_.readStripeMetadata || request.metadata.size() < 54 ||
                        request.metadata[0] != 'A' || request.metadata[1] != 'K' || request.metadata[2] != 'S' ||
                        request.metadata[3] != 'M' || request.metadata[4] != '1' ||
                        readLe64(request.metadata, 6) != request.fenceToken ||
                        readLe64(request.metadata, 14) != request.ownerNodeId) {
                        response.status = existing != stripeLeases_.end() ? StripeControlStatus::BUSY : StripeControlStatus::REJECTED;
                        return response;
                    }
                    const auto current = callbacks_.readStripeMetadata(request.key);
                    if (!current || current->size() < 54 || readLe64(*current, 6) != request.fenceToken ||
                        readLe64(*current, 14) != request.ownerNodeId || readLe64(*current, 22) != readLe64(request.metadata, 22) ||
                        readLe64(*current, 30) != readLe64(request.metadata, 30)) {
                        response.status = StripeControlStatus::REJECTED;
                        return response;
                    }
                    try { replicateStripeMetadata(request.key, request.metadata); }
                    catch (...) {
                        response.status = StripeControlStatus::ERROR_STATUS;
                        return response;
                    }
                    response.status = StripeControlStatus::COMMITTED;
                    return response;
                }
                if (existing == stripeLeases_.end() || existing->second.ownerNodeId != request.ownerNodeId ||
                    existing->second.authorityNodeId != requesterNodeId || existing->second.fenceToken != request.fenceToken) {
                    response.status = StripeControlStatus::REJECTED;
                    return response;
                }
                response.authorityNodeId = existing->second.authorityNodeId;
                response.fenceToken = existing->second.fenceToken;
                if (request.action == StripeControlAction::COMMIT) {
                    const bool validMetadata = request.metadata.size() >= 54 &&
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
                    try { replicateStripeMetadata(request.key, request.metadata); }
                    catch (...) {
                        response.status = StripeControlStatus::ERROR_STATUS;
                        return response;
                    }
                    stripeLeases_.erase(existing);
                    response.status = StripeControlStatus::COMMITTED;
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

            ReadResponse readPeer(uint64_t nodeId, std::span<const uint8_t> key, uint64_t snapshotSeq) {
                const auto roundTripStarted = std::chrono::steady_clock::now();
                const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(effectiveConsistency_.ackTimeoutMs);
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
                        if (sourceNodeId != peerNodeId) { return; }
                        if (config_.mode() == ReplicationMode::PARTITIONED && ownerForKey(key).nodeId != sourceNodeId) { return; }
                        if (callbacks_.apply) { callbacks_.apply(seq, op, key, value, recordFlags, sourceNodeId); }
                        if (config_.mode() == ReplicationMode::PARTITIONED) {
                            // Peer progress is itself durable. Publish it only after
                            // the corresponding engine mutation is durable as well.
                            if (callbacks_.forceDurable) { callbacks_.forceDurable(); }
                            recordSeqFromPeer(sourceNodeId, seq);
                        }
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
                    snapshot.forEachEntry = [source = *source](const ReplicationServer::Snapshot::EntryVisitor& visitor) {
                        return source.forEachEntry && source.forEachEntry(visitor);
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
                    transferBudget_
                );
                endpoints.server->setReadCallback(
                    [this](const ReadRequest& request) {
                        return readLocal(
                            std::span<const uint8_t>{request.key.data(), request.key.size()},
                            request.snapshotSeq
                        );
                    }
                );
                if (config_.mode() == ReplicationMode::STRIPE) {
                    endpoints.server->setStripeControlCallback(
                        [this](uint64_t peerNodeId, const StripeControlRequest& request) {
                            return handleStripeControl(peerNodeId, request);
                        }
                    );
                }
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
                    endpoints.server->setReadCallback(
                        [this](const ReadRequest& request) {
                            return readLocal(
                                std::span<const uint8_t>{request.key.data(), request.key.size()},
                                request.snapshotSeq
                            );
                        }
                    );
                    startEndpoint(*endpoints.server);
                }
                else if (role == NodeRole::REPLICA) {
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
                    endpoints.client->setApplyCallback(callbacks_.apply);
                    endpoints.client->setSnapshotCallbacks(
                        callbacks_.beginSnapshot,
                        callbacks_.beginSnapshotEntry,
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
            std::unique_ptr<RaftConsensusRuntime> stripeMetadataRaft_;
            std::vector<uint8_t> stripeMetadataSnapshotKey_;
            std::vector<uint8_t> stripeMetadataSnapshotValue_;
            uint64_t stripeMetadataSnapshotValueSize_ = 0;
            uint32_t stripeMetadataSnapshotValueCrc32c_ = 0;
            uint64_t selfNodeId_;
            ClusterEngineCallbacks callbacks_;
            ClusterRuntimeOptions runtimeOptions_;
            std::shared_ptr<detail::TransferBudget> transferBudget_;
            AckPolicy effectiveAckPolicy_;
            ConsistencyOptions effectiveConsistency_;
            uint16_t configuredReplicaCount_ = 0;

            std::mutex endpointTransitionMutex_;
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
            std::mutex stripeAuthorityMutex_;
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

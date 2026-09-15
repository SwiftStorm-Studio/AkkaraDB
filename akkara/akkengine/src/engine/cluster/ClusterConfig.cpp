/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/cluster/ClusterConfig.cpp
#include "akk/engine/cluster/ClusterConfig.hpp"
#include "akk/crypto/Random.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <unordered_set>

#include "akk/cpu/CRC32C.hpp"

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
        constexpr size_t HEADER_SIZE = 72;
        constexpr size_t MAX_HOST_BYTES = 1024;

        uint64_t nowUs() noexcept {
            return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch()
            ).count());
        }

        bool isZeroClusterId(const ClusterId& id) noexcept {
            return std::all_of(id.begin(), id.end(), [](uint8_t value) { return value == 0; });
        }

        void writeU16(uint8_t* b, size_t off, uint16_t v) noexcept {
            b[off] = static_cast<uint8_t>(v);
            b[off + 1] = static_cast<uint8_t>(v >> 8);
        }

        void writeU32(uint8_t* b, size_t off, uint32_t v) noexcept {
            b[off] = static_cast<uint8_t>(v);
            b[off + 1] = static_cast<uint8_t>(v >> 8);
            b[off + 2] = static_cast<uint8_t>(v >> 16);
            b[off + 3] = static_cast<uint8_t>(v >> 24);
        }

        void writeU64(uint8_t* b, size_t off, uint64_t v) noexcept {
            for (size_t i = 0; i < 8; ++i) { b[off + i] = static_cast<uint8_t>(v >> (8 * i)); }
        }

        uint16_t readU16(const uint8_t* b, size_t off) noexcept {
            return static_cast<uint16_t>(b[off]) | static_cast<uint16_t>(static_cast<uint16_t>(b[off + 1]) << 8);
        }

        uint32_t readU32(const uint8_t* b, size_t off) noexcept {
            return static_cast<uint32_t>(b[off]) | (static_cast<uint32_t>(b[off + 1]) << 8) | (static_cast<uint32_t>(b[off + 2]) << 16) | (
                static_cast<uint32_t>(b[off + 3]) << 24);
        }

        uint64_t readU64(const uint8_t* b, size_t off) noexcept {
            uint64_t v = 0;
            for (size_t i = 0; i < 8; ++i) { v |= static_cast<uint64_t>(b[off + i]) << (8 * i); }
            return v;
        }

        uint32_t crcFileImage(std::vector<uint8_t> bytes) {
            if (bytes.size() >= 28) { writeU32(bytes.data(), 24, 0); }
            return cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size());
        }

        void syncFile(const std::filesystem::path& path, const char* context) {
            #ifdef _WIN32
            const int fd = _wopen(path.c_str(), _O_RDWR | _O_BINARY);
            if (fd < 0) { throw std::runtime_error(std::string{context} + ": cannot reopen temp file for sync"); }
            const int rc = _commit(fd);
            const int closeRc = _close(fd);
            if (rc != 0 || closeRc != 0) { throw std::runtime_error(std::string{context} + ": temp file sync failed"); }
            #else
            const int fd = ::open(path.c_str(), O_RDONLY); if (fd < 0) {
                throw std::runtime_error(std::string{context} + ": cannot reopen temp file for sync");
            } const int rc = ::fsync(fd); const int closeRc = ::close(fd); if (rc != 0 || closeRc != 0) {
                throw std::runtime_error(std::string{context} + ": temp file sync failed");
            }
            #endif
        }

        std::filesystem::path makeTempPath(const std::filesystem::path& path) {
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
            throw std::runtime_error("ClusterConfig: cannot allocate temp file name");
        }

        #ifndef _WIN32
        void syncParentDirectory(const std::filesystem::path& path) {
            const auto parent = path.parent_path().empty() ? std::filesystem::path{"."} : path.parent_path();
            int flags = O_RDONLY;
        #ifdef O_DIRECTORY
        flags|= O_DIRECTORY;
        #endif
        const int fd = ::open(parent.c_str(), flags);if (fd<0) {
            throw std::runtime_error("ClusterConfig: cannot open parent directory for sync");
        } const int rc = ::fsync(fd); const int closeRc = ::close(fd);if (rc!= 0 || closeRc
!= 0) { throw std::runtime_error("ClusterConfig: parent directory sync failed"); }
        }
        #endif

        void replaceFileAtomically(const std::filesystem::path& tmp, const std::filesystem::path& path) {
            #ifdef _WIN32
            if (!::MoveFileExW(tmp.wstring().c_str(), path.wstring().c_str(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
                throw std::runtime_error("ClusterConfig: atomic file replace failed");
            }
            #else
            std::filesystem::rename(tmp, path); syncParentDirectory(path);
            #endif
        }
    } // namespace

    ClusterConfig::ClusterConfig() { crypto::secureRandom(clusterId_); }

    ClusterConfig::ClusterConfig(
        std::vector<NodeInfo> nodes,
        ReplicationMode mode,
        AckPolicy ackPolicy,
        ConsistencyOptions consistency,
        RaftOptions raft,
        StripeOptions stripe,
        uint64_t primaryNodeId,
        ClusterId clusterId
    )
        : nodes_{std::move(nodes)},
          mode_{mode},
          ackPolicy_{ackPolicy},
          consistency_{consistency},
          raft_{raft},
          stripe_{stripe},
          primaryNodeId_{primaryNodeId},
          clusterId_{clusterId} {
        if (isZeroClusterId(clusterId_)) { crypto::secureRandom(clusterId_); }
        if (mode_ == ReplicationMode::MIRROR && consistency_.mode != ConsistencyMode::RAFT_QUORUM && primaryNodeId_ == 0) {
            for (const auto& node : nodes_) {
                if (node.coordinatorEligible() && node.dataBearing()) {
                    primaryNodeId_ = node.nodeId;
                    break;
                }
            }
        }
        validate();
    }

    ClusterConfig ClusterConfig::load(const std::filesystem::path& path) {
        std::ifstream file(path, std::ios::binary);
        if (!file) { throw std::runtime_error("ClusterConfig: cannot open " + path.string()); }

        std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
        if (bytes.size() < HEADER_SIZE) { throw std::runtime_error("ClusterConfig: file too short"); }

        const uint32_t magic = readU32(bytes.data(), 0);
        const uint16_t version = readU16(bytes.data(), 4);
        if (magic != MAGIC) { throw std::runtime_error("ClusterConfig: bad magic"); }
        if (version != VERSION) { throw std::runtime_error("ClusterConfig: unsupported version"); }

        const uint32_t storedCrc = readU32(bytes.data(), 24);
        if (storedCrc != crcFileImage(bytes)) { throw std::runtime_error("ClusterConfig: CRC mismatch"); }

        const uint16_t flags = readU16(bytes.data(), 6);
        const uint16_t nodeCount = readU16(bytes.data(), 8);
        auto mode = static_cast<ReplicationMode>(bytes[10]);
        AckPolicy ack{};
        ack.mode = static_cast<AckPolicyMode>(bytes[11]);
        ack.stage = static_cast<AckStage>(bytes[12]);
        ack.quorum = readU16(bytes.data(), 14);

        ConsistencyOptions consistency{};
        consistency.writeConsistency = static_cast<WriteConsistency>(bytes[28]);
        consistency.ackTimeoutAction = static_cast<AckTimeoutAction>(bytes[29]);
        consistency.replicaLagAction = static_cast<ReplicaLagAction>(bytes[30]);
        consistency.ackTimeoutMs = readU32(bytes.data(), 32);
        consistency.mode = static_cast<ConsistencyMode>(bytes[36]);
        RaftOptions raft{};
        raft.membership.mode = static_cast<RaftMembershipMode>(bytes[37]);
        raft.membership.allowOnlineVoterChanges = bytes[38] != 0;
        raft.membership.allowLearners = bytes[39] != 0;
        StripeOptions stripe{};
        stripe.dataShards = bytes[40];
        stripe.parityShards = bytes[41];
        const uint64_t primaryNodeId = readU64(bytes.data(), 48);
        ClusterId clusterId{};
        std::copy_n(bytes.data() + 56, clusterId.size(), clusterId.begin());

        size_t cursor = HEADER_SIZE;
        std::vector<NodeInfo> nodes;
        nodes.reserve(nodeCount);
        for (uint16_t i = 0; i < nodeCount; ++i) {
            if (cursor + 18 > bytes.size()) { throw std::runtime_error("ClusterConfig: truncated node entry"); }
            NodeInfo node{};
            node.nodeId = readU64(bytes.data(), cursor);
            node.capabilities = readU32(bytes.data(), cursor + 8);
            node.dataPort = readU16(bytes.data(), cursor + 12);
            node.replPort = readU16(bytes.data(), cursor + 14);
            const uint16_t hostLen = readU16(bytes.data(), cursor + 16);
            cursor += 18;
            if (hostLen > MAX_HOST_BYTES) { throw std::runtime_error("ClusterConfig: host name too long"); }
            if (cursor + hostLen > bytes.size()) { throw std::runtime_error("ClusterConfig: truncated host"); }
            node.host.assign(reinterpret_cast<const char*>(bytes.data() + cursor), hostLen);
            cursor += hostLen;
            nodes.push_back(std::move(node));
        }
        if (cursor != bytes.size()) { throw std::runtime_error("ClusterConfig: trailing bytes"); }

        ClusterConfig cfg{std::move(nodes), mode, ack, consistency, raft, stripe, primaryNodeId, clusterId};
        cfg.flags_ = flags;
        cfg.validate();
        return cfg;
    }

    void ClusterConfig::save(const std::filesystem::path& path, const ClusterConfig& config) {
        config.validate();
        if (path.has_parent_path()) { std::filesystem::create_directories(path.parent_path()); }

        std::vector<uint8_t> bytes(HEADER_SIZE);
        writeU32(bytes.data(), 0, MAGIC);
        writeU16(bytes.data(), 4, VERSION);
        writeU16(bytes.data(), 6, config.flags_);
        writeU16(bytes.data(), 8, static_cast<uint16_t>(config.nodes_.size()));
        bytes[10] = static_cast<uint8_t>(config.mode_);
        bytes[11] = static_cast<uint8_t>(config.ackPolicy_.mode);
        bytes[12] = static_cast<uint8_t>(config.ackPolicy_.stage);
        bytes[13] = 0;
        writeU16(bytes.data(), 14, config.ackPolicy_.quorum);
        writeU64(bytes.data(), 16, nowUs());
        writeU32(bytes.data(), 24, 0);
        bytes[28] = static_cast<uint8_t>(config.consistency_.writeConsistency);
        bytes[29] = static_cast<uint8_t>(config.consistency_.ackTimeoutAction);
        bytes[30] = static_cast<uint8_t>(config.consistency_.replicaLagAction);
        bytes[31] = 0;
        writeU32(bytes.data(), 32, config.consistency_.ackTimeoutMs);
        bytes[36] = static_cast<uint8_t>(config.consistency_.mode);
        bytes[37] = static_cast<uint8_t>(config.raft_.membership.mode);
        bytes[38] = config.raft_.membership.allowOnlineVoterChanges ? 1 : 0;
        bytes[39] = config.raft_.membership.allowLearners ? 1 : 0;
        bytes[40] = config.stripe_.dataShards;
        bytes[41] = config.stripe_.parityShards;
        writeU64(bytes.data(), 48, config.primaryNodeId_);
        std::copy(config.clusterId_.begin(), config.clusterId_.end(), bytes.begin() + 56);

        for (const auto& node : config.nodes_) {
            if (node.host.size() > MAX_HOST_BYTES) { throw std::invalid_argument("ClusterConfig: host name too long"); }
            const auto hostLen = static_cast<uint16_t>(node.host.size());
            const size_t off = bytes.size();
            bytes.resize(off + 18 + hostLen);
            writeU64(bytes.data(), off, node.nodeId);
            writeU32(bytes.data(), off + 8, node.capabilities);
            writeU16(bytes.data(), off + 12, node.dataPort);
            writeU16(bytes.data(), off + 14, node.replPort);
            writeU16(bytes.data(), off + 16, hostLen);
            std::memcpy(bytes.data() + off + 18, node.host.data(), hostLen);
        }

        writeU32(bytes.data(), 24, crcFileImage(bytes));

        const auto tmpPath = makeTempPath(path);
        {
            std::ofstream out(tmpPath, std::ios::binary | std::ios::trunc);
            if (!out) { throw std::runtime_error("ClusterConfig: cannot create " + tmpPath.string()); }
            out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
            out.flush();
            if (!out) { throw std::runtime_error("ClusterConfig: write failed"); }
        }
        syncFile(tmpPath, "ClusterConfig");
        replaceFileAtomically(tmpPath, path);
    }

    const NodeInfo* ClusterConfig::findById(uint64_t nodeId) const noexcept {
        for (const auto& node : nodes_) { if (node.nodeId == nodeId) { return &node; } }
        return nullptr;
    }

    std::vector<NodeInfo> ClusterConfig::dataNodes() const {
        std::vector<NodeInfo> result;
        for (const auto& node : nodes_) { if (node.dataBearing()) { result.push_back(node); } }
        return result;
    }

    std::vector<NodeInfo> ClusterConfig::coordinatorNodes() const {
        std::vector<NodeInfo> result;
        for (const auto& node : nodes_) { if (node.coordinatorEligible()) { result.push_back(node); } }
        return result;
    }

    const NodeInfo* ClusterConfig::stripeFailoverNode() const noexcept {
        for (const auto& node : nodes_) { if (node.stripeFailoverEligible()) { return &node; } }
        return nullptr;
    }

    bool ClusterConfig::isStandalone() const noexcept { return mode_ == ReplicationMode::STANDALONE || nodes_.size() <= 1; }

    void ClusterConfig::validate() const {
        if (isZeroClusterId(clusterId_)) { throw std::invalid_argument("ClusterConfig: cluster id must be nonzero"); }
        if (nodes_.size() > UINT16_MAX) { throw std::invalid_argument("ClusterConfig: too many nodes"); }
        if (mode_ != ReplicationMode::STANDALONE && mode_ != ReplicationMode::MIRROR && mode_ != ReplicationMode::PARTITIONED && mode_ !=
            ReplicationMode::STRIPE) { throw std::invalid_argument("ClusterConfig: invalid replication mode"); }
        if (ackPolicy_.mode != AckPolicyMode::NONE && ackPolicy_.mode != AckPolicyMode::ALL_TARGETS && ackPolicy_.mode !=
            AckPolicyMode::QUORUM) { throw std::invalid_argument("ClusterConfig: invalid ack policy"); }
        if (ackPolicy_.stage != AckStage::RECEIVED && ackPolicy_.stage != AckStage::APPLIED && ackPolicy_.stage != AckStage::DURABLE) {
            throw std::invalid_argument("ClusterConfig: invalid ack stage");
        }
        if (ackPolicy_.mode == AckPolicyMode::QUORUM && ackPolicy_.quorum == 0) {
            throw std::invalid_argument("ClusterConfig: quorum policy requires quorum > 0");
        }
        if (consistency_.mode != ConsistencyMode::PRIMARY_ACK && consistency_.mode != ConsistencyMode::ASYNC && consistency_.mode !=
            ConsistencyMode::RAFT_QUORUM) { throw std::invalid_argument("ClusterConfig: invalid consistency mode"); }
        if (consistency_.writeConsistency != WriteConsistency::LEGACY_ACK_POLICY && consistency_.writeConsistency != WriteConsistency::LOCAL
            && consistency_.writeConsistency != WriteConsistency::ONE_REPLICA && consistency_.writeConsistency != WriteConsistency::QUORUM
            && consistency_.writeConsistency != WriteConsistency::ALL_CONFIGURED) {
            throw std::invalid_argument("ClusterConfig: invalid write consistency");
        }
        if (consistency_.writeConsistency == WriteConsistency::QUORUM && ackPolicy_.quorum == 0) {
            throw std::invalid_argument("ClusterConfig: QUORUM write consistency requires ackPolicy.quorum > 0");
        }
        if (consistency_.ackTimeoutAction != AckTimeoutAction::ACCEPT_LOCAL && consistency_.ackTimeoutAction != AckTimeoutAction::FAIL_ACK
            && consistency_.ackTimeoutAction != AckTimeoutAction::FAIL_WRITE) {
            throw std::invalid_argument("ClusterConfig: invalid acknowledgement timeout action");
        }
        if (consistency_.replicaLagAction != ReplicaLagAction::ASYNC_RESYNC && consistency_.replicaLagAction !=
            ReplicaLagAction::REJECT_REPLICA && consistency_.replicaLagAction != ReplicaLagAction::BLOCK_WRITES) {
            throw std::invalid_argument("ClusterConfig: invalid replica lag action");
        }
        if (consistency_.ackTimeoutMs == 0) { throw std::invalid_argument("ClusterConfig: acknowledgement timeout must be > 0"); }
        if (raft_.membership.mode != RaftMembershipMode::STATIC && raft_.membership.mode != RaftMembershipMode::JOINT_CONSENSUS) {
            throw std::invalid_argument("ClusterConfig: invalid Raft membership mode");
        }
        if (raft_.membership.allowOnlineVoterChanges && raft_.membership.mode != RaftMembershipMode::JOINT_CONSENSUS) {
            throw std::invalid_argument("ClusterConfig: online Raft voter changes require joint consensus membership mode");
        }
        if ((raft_.membership.allowOnlineVoterChanges || raft_.membership.allowLearners || raft_.membership.mode ==
            RaftMembershipMode::JOINT_CONSENSUS) && consistency_.mode != ConsistencyMode::RAFT_QUORUM) {
            throw std::invalid_argument("ClusterConfig: Raft membership options require RAFT_QUORUM consistency");
        }
        if (consistency_.mode == ConsistencyMode::RAFT_QUORUM &&
            (mode_ == ReplicationMode::PARTITIONED || mode_ == ReplicationMode::STRIPE)) {
            throw std::invalid_argument("ClusterConfig: RAFT_QUORUM currently requires STANDALONE or MIRROR placement");
        }
        const bool fixedMirrorPrimary = mode_ == ReplicationMode::MIRROR && consistency_.mode != ConsistencyMode::RAFT_QUORUM;
        if (!fixedMirrorPrimary && primaryNodeId_ != 0) {
            throw std::invalid_argument("ClusterConfig: primaryNodeId is only valid for non-Raft MIRROR placement");
        }
        std::unordered_set<uint64_t> ids;
        std::unordered_set<std::string> replicationEndpoints;
        bool hasDataNode = false;
        bool hasCoordinator = false;
        size_t dataNodeCount = 0;
        size_t stripeFailoverCount = 0;
        for (const auto& node : nodes_) {
            if (node.nodeId == 0) { throw std::invalid_argument("ClusterConfig: nodeId 0 is reserved"); }
            if (!ids.insert(node.nodeId).second) { throw std::invalid_argument("ClusterConfig: duplicate nodeId"); }
            if (node.host.empty()) { throw std::invalid_argument("ClusterConfig: empty host"); }
            if (node.host.size() > MAX_HOST_BYTES) { throw std::invalid_argument("ClusterConfig: host name too long"); }
            std::string endpointHost = node.host;
            for (auto& ch : endpointHost) {
                if (static_cast<unsigned char>(ch) <= 32 || static_cast<unsigned char>(ch) == 127) {
                    throw std::invalid_argument("ClusterConfig: host contains whitespace or control characters");
                }
                if (ch >= 'A' && ch <= 'Z') { ch = static_cast<char>(ch + ('a' - 'A')); }
            }
            if (endpointHost.size() > 1 && endpointHost.back() == '.') { endpointHost.pop_back(); }
            if ((mode_ != ReplicationMode::STANDALONE || consistency_.mode == ConsistencyMode::RAFT_QUORUM) && node.replPort == 0) {
                throw std::invalid_argument("ClusterConfig: cluster replication port must be nonzero");
            }
            if (node.replPort != 0 && !replicationEndpoints.insert(endpointHost + ":" + std::to_string(node.replPort)).second) {
                throw std::invalid_argument("ClusterConfig: duplicate replication endpoint");
            }
            if ((node.capabilities & ~(COORDINATOR_ELIGIBLE | DATA_BEARING | STRIPE_FAILOVER_ELIGIBLE)) != 0) {
                throw std::invalid_argument("ClusterConfig: unknown node capability");
            }
            if (node.stripeFailoverEligible()) {
                ++stripeFailoverCount;
                if (!node.dataBearing() || !node.coordinatorEligible()) {
                    throw std::invalid_argument("ClusterConfig: STRIPE failover candidate must be coordinator-eligible and data-bearing");
                }
            }
            if (node.dataBearing()) { ++dataNodeCount; }
            hasDataNode = hasDataNode || node.dataBearing();
            hasCoordinator = hasCoordinator || node.coordinatorEligible();
        }
        if (mode_ != ReplicationMode::STANDALONE && !hasDataNode) {
            throw std::invalid_argument("ClusterConfig: cluster mode requires a data-bearing node");
        }
        if (mode_ != ReplicationMode::STANDALONE && !hasCoordinator) {
            throw std::invalid_argument("ClusterConfig: cluster mode requires a coordinator-eligible node");
        }
        if (stripeFailoverCount > 1) {
            throw std::invalid_argument("ClusterConfig: at most one STRIPE failover candidate may be configured");
        }
        if (stripeFailoverCount != 0 && mode_ != ReplicationMode::STRIPE) {
            throw std::invalid_argument("ClusterConfig: STRIPE failover capability requires STRIPE placement");
        }
        if (fixedMirrorPrimary) {
            if (primaryNodeId_ == 0) { throw std::invalid_argument("ClusterConfig: non-Raft MIRROR requires one Primary"); }
            const auto* primary = findById(primaryNodeId_);
            if (primary == nullptr) { throw std::invalid_argument("ClusterConfig: MIRROR Primary is not a configured node"); }
            if (!primary->coordinatorEligible() || !primary->dataBearing()) {
                throw std::invalid_argument("ClusterConfig: MIRROR Primary must be coordinator-eligible and data-bearing");
            }
        }
        if (mode_ == ReplicationMode::STRIPE) {
            if (stripe_.dataShards == 0) { throw std::invalid_argument("ClusterConfig: stripe dataShards must be > 0"); }
            if (stripe_.parityShards == 0) { throw std::invalid_argument("ClusterConfig: stripe parityShards must be > 0"); }
            if (stripe_.totalShards() > 255) { throw std::invalid_argument("ClusterConfig: stripe total shard count must be <= 255"); }
            if (dataNodeCount < stripe_.totalShards()) {
                throw std::invalid_argument("ClusterConfig: stripe mode requires at least dataShards + parityShards data-bearing nodes");
            }
            if (stripeFailoverCount == 1 && dataNodeCount <= stripe_.totalShards()) {
                throw std::invalid_argument("ClusterConfig: STRIPE owner failover requires one spare data-bearing node");
            }
        }
    }
    void ClusterSecureOptions::validatePins(std::span<const uint64_t> requiredPeerIds) const {
        std::unordered_set<uint64_t> pinnedIds;
        for (const auto& pin : pinnedPeers) {
            if (pin.nodeId == 0 || !pinnedIds.insert(pin.nodeId).second) {
                throw std::invalid_argument("ClusterConfig: peer pins require unique nonzero node ids");
            }
        }
        for (const auto id : requiredPeerIds) {
            if (!pinnedIds.contains(id)) {
                throw std::invalid_argument("ClusterConfig: missing SECURE peer pin for node " + std::to_string(id));
            }
        }
    }

    void ReplicationTransferOptions::validate() const {
        if (thresholdBytes == 0 || thresholdBytes > 64u * 1024 || chunkBytes == 0 || chunkBytes > 64u * 1024 - 8 ||
            maxConcurrentTransfers == 0 || maxConcurrentTransfers > 1024 || maxMemoryBytes < 8u * 64u * 1024 || maxSpoolBytes == 0 ||
            resumeRetentionMs == 0 || resumeRetentionMs > 7ull * 24 * 60 * 60 * 1000 || maxResumeTransfers == 0 ||
            maxResumeTransfers > 65'536 || resumeHandshakeTimeoutMs == 0 || resumeHandshakeTimeoutMs > 300'000) {
            throw std::invalid_argument("ClusterConfig: invalid replication transfer limits");
        }
    }

    void ClusterConfig::validateRuntime(uint64_t selfNodeId, const ClusterRuntimeOptions& options) const {
        validate();
        options.transfer.validate();
        if (options.transportMode != TransportMode::PLAIN && options.transportMode != TransportMode::SECURE) {
            throw std::invalid_argument("ClusterConfig: invalid transport mode");
        }
        const bool raft = consistency_.mode == ConsistencyMode::RAFT_QUORUM;
        if (options.requests.maxRetentionMs == 0 || options.requests.maxRetentionMs > 365ull * 24 * 60 * 60 * 1000 ||
            options.requests.maxTrackedRequests == 0 || options.requests.maxTrackedRequests > 65'536) {
            throw std::invalid_argument("ClusterConfig: invalid request retention or result capacity");
        }
        if (raft && (options.raftSnapshot.minLogEntries == 0 || options.raftSnapshot.minLogEntries > 1'000'000'000ull ||
            options.raftSnapshot.minLogBytes == 0 || options.raftSnapshot.minLogBytes > 1024ull * 1024 * 1024 * 1024 ||
            options.raftSnapshot.maxIntervalMs == 0 || options.raftSnapshot.maxIntervalMs > 7ull * 24 * 60 * 60 * 1000)) {
            throw std::invalid_argument("ClusterConfig: invalid Raft snapshot policy");
        }
        if (options.requests.enabled && !raft) { throw std::invalid_argument("ClusterConfig: retry-safe requests require RAFT_QUORUM"); }
        if ((mode_ == ReplicationMode::PARTITIONED || mode_ == ReplicationMode::STRIPE) && options.clusterGroupId == 0) {
            throw std::invalid_argument("ClusterConfig: PARTITIONED and STRIPE require an explicit nonzero shared cluster group id");
        }
        if (!isStandalone() || raft) {
            if (findById(selfNodeId) == nullptr) { throw std::invalid_argument("ClusterConfig: self node is not configured"); }
        }
        std::vector<uint64_t> required;
        if (options.transportMode == TransportMode::SECURE && (!isStandalone() || raft)) {
            for (const auto& peer : nodes_) {
                if (peer.nodeId == selfNodeId || !peer.dataBearing()) { continue; }
                if (!raft && mode_ == ReplicationMode::MIRROR && selfNodeId != primaryNodeId_ && peer.nodeId != primaryNodeId_) { continue; }
                required.push_back(peer.nodeId);
            }
        }
        // Extra pins are allowed so future Raft voters can be provisioned before joining.
        options.secure.validatePins(required);
    }
} // namespace akkaradb::engine::cluster

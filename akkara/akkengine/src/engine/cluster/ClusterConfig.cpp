/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/cluster/ClusterConfig.cpp
#include "akk/engine/cluster/detail/NodeResourceBudget.hpp"
#include "akk/engine/cluster/ClusterConfig.hpp"
#include "akk/engine/cluster/detail/ClusterPlacement.hpp"
#include "akk/crypto/Random.hpp"
#include "akk/engine/cluster/ReplFraming.hpp"

#include <algorithm>
#include <cctype>
#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <unordered_set>

#include "akk/cpu/CRC32C.hpp"

#include <ranges>
#include <utility>

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
        constexpr uint16_t FAULT_DOMAIN_LAYOUT_FLAG = 0x8000;

        uint64_t nowUs() noexcept {
            return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch()
            ).count());
        }

        bool isZeroClusterId(const ClusterId& id) noexcept {
            return std::ranges::all_of(id, [](uint8_t value) { return value == 0; });
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
            return static_cast<uint16_t>(b[off] | (b[off + 1] << 8));
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

        uint32_t crcFileImage(std::span<const uint8_t> image) {
            std::vector bytes(image.begin(), image.end());
            if (bytes.size() >= 28) { writeU32(bytes.data(), 24, 0); }
            return cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size());
        }

        void syncFile(const std::filesystem::path& path, const char* context) {
            #ifdef _WIN32
            const auto fd = _wopen(path.c_str(), _O_RDWR | _O_BINARY);
            if (fd < 0) { throw std::runtime_error(std::string{context} + ": cannot reopen temp file for sync"); }
            const auto rc = _commit(fd);
            const auto closeRc = _close(fd);
            if (rc != 0 || closeRc != 0) { throw std::runtime_error(std::string{context} + ": temp file sync failed"); }
            #else
            const auto fd = ::open(path.c_str(), O_RDONLY); if (fd < 0) {
                throw std::runtime_error(std::string{context} + ": cannot reopen temp file for sync");
            } const auto rc = ::fsync(fd); const auto closeRc = ::close(fd); if (rc != 0 || closeRc != 0) {
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
        const auto fd = ::open(parent.c_str(), flags);if (fd<0) {
            throw std::runtime_error("ClusterConfig: cannot open parent directory for sync");
        } const auto rc = ::fsync(fd); const auto closeRc = ::close(fd);if (rc!= 0 || closeRc
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

        ReplicationMode raidPlacementMode(RaidPreset preset) {
            switch (preset) {
                case RaidPreset::RAID0: return ReplicationMode::STRIPE;
                case RaidPreset::RAID1: return ReplicationMode::MIRROR;
                case RaidPreset::RAID5:
                case RaidPreset::RAID6:
                case RaidPreset::RAID10: return ReplicationMode::STRIPE;
            }
            throw std::invalid_argument("ClusterConfig: unknown RAID preset");
        }

        StripeOptions raidStripeOptions(const RaidOptions& raid) {
            StripeOptions options{.dataShards = raid.dataShards,
                .metadataPlacement = raid.metadataPlacement, .failoverPolicy = raid.failoverPolicy};
            switch (raid.preset) {
                case RaidPreset::RAID0: options.parityShards = 0; break;
                case RaidPreset::RAID1: return {};
                case RaidPreset::RAID5:
                    if (raid.dataShards < 2) {
                        throw std::invalid_argument("ClusterConfig: RAID.5 requires at least two data shards");
                    }
                    options.parityShards = 1; break;
                case RaidPreset::RAID6:
                    if (raid.dataShards < 2) {
                        throw std::invalid_argument("ClusterConfig: RAID.6 requires at least two data shards");
                    }
                    options.parityShards = 2; break;
                case RaidPreset::RAID10:
                    if (raid.dataShards < 2) {
                        throw std::invalid_argument("ClusterConfig: RAID.10 requires at least two mirrored data shards");
                    }
                    options.parityShards = 0; options.copiesPerShard = 2; break;
                default: throw std::invalid_argument("ClusterConfig: unknown RAID preset");
            }
            return options;
        }

        std::vector<NodeInfo> raidNodes(std::vector<NodeInfo> nodes, const RaidOptions& raid) {
            if (raid.hotSpareNodeId == 0) { return nodes; }
            if (raid.preset != RaidPreset::RAID10) {
                throw std::invalid_argument("ClusterConfig: hotSpareNodeId is only valid for RAID.10");
            }
            const auto spare = std::ranges::find(nodes, raid.hotSpareNodeId, &NodeInfo::nodeId);
            if (spare == nodes.end()) {
                throw std::invalid_argument("ClusterConfig: RAID.10 hot spare is not a configured node");
            }
            spare->capabilities |= STRIPE_FAILOVER_ELIGIBLE;
            return nodes;
        }

        AckPolicy raidAckPolicy(const RaidOptions& raid, AckPolicy requested) {
            switch (raid.preset) {
                case RaidPreset::RAID0:
                case RaidPreset::RAID5:
                case RaidPreset::RAID6:
                case RaidPreset::RAID10: return requested;
                case RaidPreset::RAID1: return AckPolicy{.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::DURABLE};
            }
            throw std::invalid_argument("ClusterConfig: unknown RAID preset");
        }

        ConsistencyOptions raidConsistency(const RaidOptions& raid, ConsistencyOptions requested) {
            switch (raid.preset) {
                case RaidPreset::RAID0:
                case RaidPreset::RAID5:
                case RaidPreset::RAID6:
                case RaidPreset::RAID10: return requested;
                case RaidPreset::RAID1:
                    return ConsistencyOptions{
                        .mode = ConsistencyMode::PRIMARY_ACK,
                        .writeConsistency = WriteConsistency::AVAILABLE_REPLICAS,
                        .ackTimeoutAction = AckTimeoutAction::FAIL_WRITE,
                        .replicaLagAction = ReplicaLagAction::ASYNC_RESYNC,
                        .ackTimeoutMs = requested.ackTimeoutMs,
                    };
            }
            throw std::invalid_argument("ClusterConfig: unknown RAID preset");
        }

        uint64_t raidPrimaryNodeId(const RaidOptions& raid) {
            switch (raid.preset) {
                case RaidPreset::RAID0:
                case RaidPreset::RAID5:
                case RaidPreset::RAID6:
                case RaidPreset::RAID10: return 0;
                case RaidPreset::RAID1: return raid.primaryNodeId;
            }
            throw std::invalid_argument("ClusterConfig: unknown RAID preset");
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
        ClusterId clusterId,
        PartitionOptions partition,
        FailoverPolicy failover,
        FaultDomainOptions faultDomains
    )
        : nodes_{std::move(nodes)},
          mode_{mode},
          ackPolicy_{ackPolicy},
          consistency_{consistency},
          failover_{failover == FailoverPolicy::DEFAULT ? (consistency.mode == ConsistencyMode::RAFT_QUORUM ? FailoverPolicy::PRESERVE_ACKNOWLEDGED : FailoverPolicy::NONE) : failover},
          raft_{raft},
          stripe_{stripe},
          partition_{partition},
          faultDomains_{faultDomains},
          primaryNodeId_{primaryNodeId},
          clusterId_{clusterId} {
        if (isZeroClusterId(clusterId_)) { crypto::secureRandom(clusterId_); }
        if (mode_ == ReplicationMode::MIRROR && !usesDataConsensus() && primaryNodeId_ == 0) {
            for (const auto& node : nodes_) {
                if (node.coordinatorEligible() && node.dataBearing()) {
                    primaryNodeId_ = node.nodeId;
                    break;
                }
            }
        }
        validate();
    }

    ClusterConfig::ClusterConfig(
        std::vector<NodeInfo> nodes,
        RaidOptions raid,
        AckPolicy ackPolicy,
        ConsistencyOptions consistency,
        ClusterId clusterId,
        FaultDomainOptions faultDomains
    )
        : ClusterConfig(
              raidNodes(std::move(nodes), raid),
              raidPlacementMode(raid.preset),
              raidAckPolicy(raid, ackPolicy),
              raidConsistency(raid, consistency),
              {},
              raidStripeOptions(raid),
              raidPrimaryNodeId(raid),
              clusterId, {}, FailoverPolicy::DEFAULT, faultDomains
          ) {}

    ClusterConfig ClusterConfig::load(const std::filesystem::path& path) {
        std::ifstream file(path, std::ios::binary);
        if (!file) { throw std::runtime_error("ClusterConfig: cannot open " + path.string()); }

        std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
        return decode(bytes);
    }

    ClusterConfig ClusterConfig::decode(std::span<const uint8_t> bytes) {
        if (bytes.size() < HEADER_SIZE) { throw std::runtime_error("ClusterConfig: file too short"); }

        const auto magic = readU32(bytes.data(), 0);
        const auto version = readU16(bytes.data(), 4);
        if (magic != MAGIC) { throw std::runtime_error("ClusterConfig: bad magic"); }
        if (version != VERSION) { throw std::runtime_error("ClusterConfig: unsupported version"); }

        const auto storedCrc = readU32(bytes.data(), 24);
        if (storedCrc != crcFileImage(bytes)) { throw std::runtime_error("ClusterConfig: CRC mismatch"); }
        if (bytes[38] > 1 || bytes[39] > 1 || bytes[43] > std::to_underlying(FailoverPolicy::ALLOW_ACKNOWLEDGED_LOSS)) {
            throw std::runtime_error("ClusterConfig: invalid membership flags or failover policy");
        }

        const auto flags = readU16(bytes.data(), 6);
        if (!(flags & FAULT_DOMAIN_LAYOUT_FLAG)) { throw std::runtime_error("ClusterConfig: missing physical fault-domain layout; recreate the pre-release configuration"); }
        const auto nodeCount = readU16(bytes.data(), 8);
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
        stripe.copiesPerShard = bytes[42];
        stripe.metadataPlacement = static_cast<StripeMetadataPlacement>(bytes[13]);
        stripe.failoverPolicy = static_cast<StripeFailoverPolicy>(bytes[31]);
        PartitionOptions partition{.replicationFactor = readU16(bytes.data(), 44)};
        partition.partitionCount = readU16(bytes.data(), 46);
        const auto primaryNodeId = readU64(bytes.data(), 48);
        ClusterId clusterId{};
        std::copy_n(bytes.data() + 56, clusterId.size(), clusterId.begin());

        if (bytes.size() < HEADER_SIZE + 5 || bytes[HEADER_SIZE + 4] > 1) { throw std::runtime_error("ClusterConfig: truncated or invalid fault-domain policy"); }
        const FaultDomainOptions domains{readU16(bytes.data(), HEADER_SIZE), readU16(bytes.data(), HEADER_SIZE + 2), bytes[HEADER_SIZE + 4] != 0};
        size_t cursor = HEADER_SIZE + 5;
        std::vector<NodeInfo> nodes;
        nodes.reserve(nodeCount);
        for (uint16_t i = 0; i < nodeCount; ++i) {
            if (cursor + 22 > bytes.size()) { throw std::runtime_error("ClusterConfig: truncated node entry"); }
            NodeInfo node{};
            node.nodeId = readU64(bytes.data(), cursor);
            node.capabilities = readU32(bytes.data(), cursor + 8);
            node.dataPort = readU16(bytes.data(), cursor + 12);
            node.replPort = readU16(bytes.data(), cursor + 14);
            node.stripeMetadataPort = readU16(bytes.data(), cursor + 16);
            node.partitionReplBasePort = readU16(bytes.data(), cursor + 18);
            const auto hostLen = readU16(bytes.data(), cursor + 20);
            cursor += 22;
            if (hostLen > MAX_HOST_BYTES) { throw std::runtime_error("ClusterConfig: host name too long"); }
            if (cursor + hostLen > bytes.size()) { throw std::runtime_error("ClusterConfig: truncated host"); }
            node.host.assign(reinterpret_cast<const char*>(bytes.data() + cursor), hostLen);
            cursor += hostLen;
            const auto label = [&]() {
                if (cursor + 2 > bytes.size()) { throw std::runtime_error("ClusterConfig: truncated domain length"); }
                const auto length = readU16(bytes.data(), cursor); cursor += 2;
                if (length > MAX_HOST_BYTES || cursor + length > bytes.size()) { throw std::runtime_error("ClusterConfig: invalid domain label"); }
                std::string value(reinterpret_cast<const char*>(bytes.data() + cursor), length); cursor += length;
                return value;
            };
            node.hostId = label(); node.siteId = label();
            nodes.push_back(std::move(node));
        }
        if (cursor != bytes.size()) { throw std::runtime_error("ClusterConfig: trailing bytes"); }

        ClusterConfig cfg{std::move(nodes), mode, ack, consistency, raft, stripe, primaryNodeId, clusterId, partition, static_cast<FailoverPolicy>(bytes[43]), domains};
        cfg.flags_ = flags & ~FAULT_DOMAIN_LAYOUT_FLAG;
        cfg.validate();
        return cfg;
    }

    std::vector<uint8_t> ClusterConfig::encode() const {
        const auto& config = *this;
        config.validate();

        std::vector<uint8_t> bytes(HEADER_SIZE);
        writeU32(bytes.data(), 0, MAGIC);
        writeU16(bytes.data(), 4, VERSION);
        writeU16(bytes.data(), 6, config.flags_ | FAULT_DOMAIN_LAYOUT_FLAG);
        writeU16(bytes.data(), 8, static_cast<uint16_t>(config.nodes_.size()));
        bytes[10] = std::to_underlying(config.mode_);
        bytes[11] = std::to_underlying(config.ackPolicy_.mode);
        bytes[12] = std::to_underlying(config.ackPolicy_.stage);
        bytes[13] = std::to_underlying(config.stripe_.metadataPlacement);
        writeU16(bytes.data(), 14, config.ackPolicy_.quorum);
        writeU64(bytes.data(), 16, nowUs());
        writeU32(bytes.data(), 24, 0);
        bytes[28] = std::to_underlying(config.consistency_.writeConsistency);
        bytes[29] = std::to_underlying(config.consistency_.ackTimeoutAction);
        bytes[30] = std::to_underlying(config.consistency_.replicaLagAction);
        bytes[31] = std::to_underlying(config.stripe_.failoverPolicy);
        writeU32(bytes.data(), 32, config.consistency_.ackTimeoutMs);
        bytes[36] = std::to_underlying(config.consistency_.mode);
        bytes[37] = std::to_underlying(config.raft_.membership.mode);
        bytes[38] = config.raft_.membership.allowOnlineVoterChanges ? 1 : 0;
        bytes[39] = config.raft_.membership.allowLearners ? 1 : 0;
        bytes[40] = config.stripe_.dataShards;
        bytes[41] = config.stripe_.parityShards;
        bytes[42] = config.stripe_.copiesPerShard;
        bytes[43] = std::to_underlying(config.failover_);
        writeU16(bytes.data(), 44, config.partition_.replicationFactor);
        writeU16(bytes.data(), 46, config.partition_.partitionCount);
        writeU64(bytes.data(), 48, config.primaryNodeId_);
        std::ranges::copy(config.clusterId_, bytes.begin() + 56);

        bytes.resize(HEADER_SIZE + 5);
        writeU16(bytes.data(), HEADER_SIZE, config.faultDomains_.minHosts);
        writeU16(bytes.data(), HEADER_SIZE + 2, config.faultDomains_.minSites);
        bytes[HEADER_SIZE + 4] = config.faultDomains_.requireSiteQuorum;
        for (const auto& node : config.nodes_) {
            if (node.host.size() > MAX_HOST_BYTES) { throw std::invalid_argument("ClusterConfig: host name too long"); }
            const auto hostLen = static_cast<uint16_t>(node.host.size());
            const auto off = bytes.size();
            bytes.resize(off + 22 + hostLen);
            writeU64(bytes.data(), off, node.nodeId);
            writeU32(bytes.data(), off + 8, node.capabilities);
            writeU16(bytes.data(), off + 12, node.dataPort);
            writeU16(bytes.data(), off + 14, node.replPort);
            writeU16(bytes.data(), off + 16, node.stripeMetadataPort);
            writeU16(bytes.data(), off + 18, node.partitionReplBasePort);
            writeU16(bytes.data(), off + 20, hostLen);
            std::memcpy(bytes.data() + off + 22, node.host.data(), hostLen);
            for (const auto label : {std::string_view{node.hostId}, std::string_view{node.siteId}}) {
                const auto position = bytes.size(); bytes.resize(position + 2 + label.size());
                writeU16(bytes.data(), position, static_cast<uint16_t>(label.size()));
                std::memcpy(bytes.data() + position + 2, label.data(), label.size());
            }
        }

        writeU32(bytes.data(), 24, crcFileImage(bytes));
        return bytes;
    }

    void ClusterConfig::validatePlacementChange(const ClusterConfig& target) const {
        auto before = encode(), after = target.encode();
        before.resize(HEADER_SIZE + 5); after.resize(HEADER_SIZE + 5);
        std::fill(before.begin() + 16, before.begin() + 28, 0);
        std::fill(after.begin() + 16, after.begin() + 28, 0);
        after[8] = before[8]; after[9] = before[9];
        if (mode_ == ReplicationMode::PARTITIONED) { after[44] = before[44]; after[45] = before[45]; }
        if (before != after) { throw std::invalid_argument("ClusterConfig: placement change must preserve identity, geometry and consensus policy"); }
        for (const auto& node : nodes_) {
            if (const auto* replacement = target.findById(node.nodeId)) {
                if (node.host != replacement->host || node.dataPort != replacement->dataPort || node.replPort != replacement->replPort ||
                    node.stripeMetadataPort != replacement->stripeMetadataPort || node.partitionReplBasePort != replacement->partitionReplBasePort ||
                    node.hostId != replacement->hostId || node.siteId != replacement->siteId) {
                    throw std::invalid_argument("ClusterConfig: placement change cannot alter an existing node endpoint");
                }
            }
        }
    }

    void ClusterConfig::save(const std::filesystem::path& path, const ClusterConfig& config) {
        const auto bytes = config.encode();
        if (path.has_parent_path()) { std::filesystem::create_directories(path.parent_path()); }

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
        const auto node = std::ranges::find(nodes_, nodeId, &NodeInfo::nodeId);
        return node == nodes_.end() ? nullptr : &*node;
    }

    std::array<uint8_t, 32> ClusterConfig::replicationFingerprint() const {
        validate();
        std::vector<uint8_t> bytes{'A', 'K', 'N', 'P', '5'};
        const auto integer = [&](uint64_t value, size_t width) {
            for (size_t i = 0; i < width; ++i) { bytes.push_back(static_cast<uint8_t>(value >> (8 * i))); }
        };
        bytes.insert(bytes.end(), clusterId_.begin(), clusterId_.end());
        integer(flags_, 2);
        integer(std::to_underlying(mode_), 1);
        integer(primaryNodeId_, 8);
        integer(std::to_underlying(ackPolicy_.mode), 1);
        integer(std::to_underlying(ackPolicy_.stage), 1);
        integer(ackPolicy_.quorum, 2);
        integer(std::to_underlying(consistency_.mode), 1);
        integer(std::to_underlying(failover_), 1);
        integer(std::to_underlying(consistency_.writeConsistency), 1);
        integer(std::to_underlying(consistency_.ackTimeoutAction), 1);
        integer(std::to_underlying(consistency_.replicaLagAction), 1);
        integer(consistency_.ackTimeoutMs, 4);
        integer(std::to_underlying(raft_.membership.mode), 1);
        integer(raft_.membership.allowOnlineVoterChanges, 1);
        integer(raft_.membership.allowLearners, 1);
        integer(stripe_.dataShards, 1);
        integer(stripe_.parityShards, 1);
        integer(stripe_.copiesPerShard, 1);
        integer(std::to_underlying(stripe_.metadataPlacement), 1);
        integer(std::to_underlying(stripe_.failoverPolicy), 1);
        if (mode_ != ReplicationMode::PARTITIONED || !usesDataConsensus()) {
            integer(partition_.replicationFactor, 2);
        }
        integer(partition_.partitionCount, 2);
        integer(faultDomains_.minHosts, 2); integer(faultDomains_.minSites, 2); integer(faultDomains_.requireSiteQuorum, 1);
        // Placement is part of the admission contract, including the score algorithm.
        const std::string_view placement = "BLAKE2b-256-HRW1";
        bytes.insert(bytes.end(), placement.begin(), placement.end());
        if (mode_ == ReplicationMode::STRIPE) {
            const std::string_view reclamation = "GC-AUTHORITY1";
            bytes.insert(bytes.end(), reclamation.begin(), reclamation.end());
            const std::string_view historyMigration = "HISTORY-UNION1";
            bytes.insert(bytes.end(), historyMigration.begin(), historyMigration.end());
            const std::array parts{std::span<const uint8_t>{bytes}};
            return crypto::hash256(parts);
        }
        auto members = nodes_;
        std::ranges::sort(members, {}, &NodeInfo::nodeId);
        integer(members.size(), 2);
        for (const auto& node : members) {
            integer(node.nodeId, 8);
            integer(node.capabilities, 4);
            integer(node.host.size(), 2);
            bytes.insert(bytes.end(), node.host.begin(), node.host.end());
            integer(node.dataPort, 2);
            integer(node.replPort, 2);
            integer(node.stripeMetadataPort, 2);
            integer(node.partitionReplBasePort, 2);
            for (const auto label : {std::string_view{node.hostId}, std::string_view{node.siteId}}) { integer(label.size(), 2); bytes.insert(bytes.end(), label.begin(), label.end()); }
        }
        const std::array parts{std::span<const uint8_t>{bytes}};
        return crypto::hash256(parts);
    }

    std::vector<NodeInfo> ClusterConfig::dataNodes() const {
        return nodes_ | std::views::filter(&NodeInfo::dataBearing) | std::ranges::to<std::vector>();
    }

    uint16_t ClusterConfig::partitionCopies() const noexcept {
        const auto count = std::ranges::count_if(nodes_, [this](const NodeInfo& n) { return n.dataBearing() && (!usesDataConsensus() || !n.raftLearner()); });
        return static_cast<uint16_t>(std::min<size_t>(partition_.replicationFactor, count));
    }

    std::vector<NodeInfo> ClusterConfig::stripePlacementNodes() const {
        return nodes_ | std::views::filter([this](const NodeInfo& node) {
            return node.dataBearing() && !node.placementStandby() &&
                !(mode_ == ReplicationMode::STRIPE && stripe_.copiesPerShard == 2 && node.stripeFailoverEligible());
        }) | std::ranges::to<std::vector>();
    }

    std::vector<NodeInfo> ClusterConfig::coordinatorNodes() const {
        return nodes_ | std::views::filter(&NodeInfo::coordinatorEligible) | std::ranges::to<std::vector>();
    }

    std::optional<RaidPreset> ClusterConfig::raidPreset() const noexcept {
        if (mode_ == ReplicationMode::STRIPE && stripe_.copiesPerShard == 1 &&
            stripe_.parityShards == 0 && stripe_.dataShards >= 2) {
            return RaidPreset::RAID0;
        }
        if (mode_ == ReplicationMode::MIRROR && consistency_.mode == ConsistencyMode::PRIMARY_ACK &&
            consistency_.writeConsistency == WriteConsistency::AVAILABLE_REPLICAS &&
            consistency_.ackTimeoutAction == AckTimeoutAction::FAIL_WRITE &&
            consistency_.replicaLagAction == ReplicaLagAction::ASYNC_RESYNC &&
            ackPolicy_.mode == AckPolicyMode::ALL_TARGETS && ackPolicy_.stage == AckStage::DURABLE) {
            return RaidPreset::RAID1;
        }
        if (mode_ == ReplicationMode::STRIPE && stripe_.copiesPerShard == 1 &&
            stripe_.dataShards >= 2 && stripe_.parityShards == 1) {
            return RaidPreset::RAID5;
        }
        if (mode_ == ReplicationMode::STRIPE && stripe_.copiesPerShard == 1 &&
            stripe_.dataShards >= 2 && stripe_.parityShards == 2) {
            return RaidPreset::RAID6;
        }
        if (mode_ == ReplicationMode::STRIPE && stripe_.copiesPerShard == 2 &&
            stripe_.dataShards >= 2 && stripe_.parityShards == 0) {
            return RaidPreset::RAID10;
        }
        return std::nullopt;
    }

    std::vector<NodeInfo> ClusterConfig::stripeFailoverNodes() const {
        return nodes_ | std::views::filter(&NodeInfo::stripeFailoverEligible) | std::ranges::to<std::vector>();
    }

    bool ClusterConfig::isStripeFailoverNode(uint64_t id) const noexcept {
        const auto* node = findById(id);
        return mode_ == ReplicationMode::STRIPE && node && node->stripeFailoverEligible();
    }

    bool ClusterConfig::isStripeMetadataVoter(const NodeInfo& node) const noexcept {
        if (mode_ != ReplicationMode::STRIPE || node.placementStandby()) { return false; }
        return stripe_.metadataPlacement == StripeMetadataPlacement::DATA_NODES
            ? node.dataBearing() : (node.capabilities & STRIPE_METADATA_VOTER) != 0;
    }

    std::vector<NodeInfo> ClusterConfig::stripeMetadataNodes() const {
        if (mode_ != ReplicationMode::STRIPE) { return {}; }
        return nodes_ | std::views::filter([this](const NodeInfo& node) {
            return node.dataBearing() || isStripeMetadataVoter(node);
        }) | std::ranges::to<std::vector>();
    }

    ClusterAvailability ClusterConfig::availability() const {
        ClusterAvailability result; result.mode = mode_;
        for (const auto& node : nodes_) {
            if (node.dataBearing()) { result.dataNodes.push_back(node.nodeId); }
            if (node.stripeFailoverEligible()) { result.failoverCandidates.push_back(node.nodeId); }
        }
        if (mode_ == ReplicationMode::STRIPE) {
            for (const auto& node : stripeMetadataNodes()) {
                (isStripeMetadataVoter(node) ? result.metadataVoters : result.metadataLearners).push_back(node.nodeId);
            }
            const auto voters = result.metadataVoters.size();
            result.metadataQuorum = static_cast<uint16_t>(voters / 2 + 1);
            result.metadataFailureTolerance = static_cast<uint16_t>((voters - 1) / 2);
            result.dataFailureTolerance = stripe_.copiesPerShard == 2 ? 1 : stripe_.parityShards;
            if (voters % 2 == 0) { result.warnings.push_back(ClusterAvailabilityWarning::EVEN_METADATA_VOTER_COUNT); }
            if (result.metadataFailureTolerance < result.dataFailureTolerance) {
                result.warnings.push_back(ClusterAvailabilityWarning::METADATA_TOLERANCE_BELOW_DATA);
            }
            if (result.failoverCandidates.empty()) { result.warnings.push_back(ClusterAvailabilityWarning::OWNER_FAILOVER_DISABLED); }
            else if (result.failoverCandidates.size() == 1) { result.warnings.push_back(ClusterAvailabilityWarning::SINGLE_OWNER_FAILOVER_CANDIDATE); }
            if (stripe_.copiesPerShard == 2) { result.warnings.push_back(ClusterAvailabilityWarning::MIRROR_PAIR_FAILURE_DEPENDENT); }
        } else if (mode_ == ReplicationMode::PARTITIONED) {
            result.dataFailureTolerance = static_cast<uint16_t>(partitionCopies() - 1);
        } else if (mode_ == ReplicationMode::MIRROR && !result.dataNodes.empty()) {
            result.dataFailureTolerance = static_cast<uint16_t>(result.dataNodes.size() - 1);
        }
        result.failureDomainsKnown = !nodes_.empty() && std::ranges::all_of(nodes_, [](const auto& n) { return !n.hostId.empty() && !n.siteId.empty(); });
        if (result.failureDomainsKnown) {
            auto data = dataNodes();
            std::map<std::string, size_t> counts;
            for (const auto& n : data) { ++counts[n.siteId]; }
            if (mode_ == ReplicationMode::MIRROR) {
                result.singleSiteDataSurvival = counts.size() > 1;
                std::erase_if(data, [](const auto& n) { return n.raftLearner(); });
                result.singleSiteQuorumSurvival = usesDataConsensus() && detail::siteQuorumSurvives(data);
            } else if (mode_ == ReplicationMode::PARTITIONED) {
                if (usesDataConsensus()) {
                    std::erase_if(data, [](const auto& n) { return n.raftLearner(); });
                    result.singleSiteQuorumSurvival = detail::siteQuorumSurvives(data);
                    result.singleSiteDataSurvival = true;
                    for (uint16_t i = 0; i < partition_.partitionCount; ++i) {
                        const std::array<uint8_t, 2> key{static_cast<uint8_t>(i), static_cast<uint8_t>(i >> 8)};
                        const auto holders = detail::placementNodes(key, data, partitionCopies(), faultDomains_, true);
                        std::set<std::string> sites; for (const auto& n : holders) { sites.insert(n.siteId); }
                        result.singleSiteDataSurvival &= sites.size() > 1;
                        result.singleSiteQuorumSurvival &= detail::siteQuorumSurvives(holders);
                    }
                } else {
                    result.singleSiteDataSurvival = faultDomains_.minSites > 1 || std::ranges::all_of(counts, [&](const auto& e) { return e.second < partitionCopies(); });
                }
            } else if (mode_ == ReplicationMode::STRIPE) {
                auto metadata = stripeMetadataNodes(); std::erase_if(metadata, [this](const auto& n) { return !isStripeMetadataVoter(n); });
                result.singleSiteQuorumSurvival = detail::siteQuorumSurvives(metadata);
                counts.clear(); for (const auto& n : stripePlacementNodes()) { ++counts[n.siteId]; }
                if (stripe_.copiesPerShard == 2) {
                    const auto pairs = detail::raidPairNodes(*this); result.singleSiteDataSurvival = true;
                    for (size_t i = 0; i + 1 < pairs.size(); i += 2) { result.singleSiteDataSurvival &= pairs[i].siteId != pairs[i + 1].siteId; }
                } else {
                    const size_t cap = stripe_.totalShards() - (std::max<uint16_t>(1, faultDomains_.minSites) - 1);
                    result.singleSiteDataSurvival = std::ranges::all_of(counts, [&](const auto& e) { return std::min(e.second, cap) <= stripe_.parityShards; });
                }
            }
        }
        std::ranges::sort(result.dataNodes); std::ranges::sort(result.metadataVoters);
        std::ranges::sort(result.metadataLearners); std::ranges::sort(result.failoverCandidates);
        return result;
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
        if (failover_ != FailoverPolicy::NONE && failover_ != FailoverPolicy::PRESERVE_ACKNOWLEDGED &&
            failover_ != FailoverPolicy::ALLOW_ACKNOWLEDGED_LOSS) {
            throw std::invalid_argument("ClusterConfig: invalid failover policy");
        }
        if (failover_ == FailoverPolicy::PRESERVE_ACKNOWLEDGED && consistency_.mode != ConsistencyMode::RAFT_QUORUM) {
            throw std::invalid_argument("ClusterConfig: preserving acknowledged writes requires RAFT_QUORUM");
        }
        if (failover_ == FailoverPolicy::ALLOW_ACKNOWLEDGED_LOSS && mode_ != ReplicationMode::MIRROR && mode_ != ReplicationMode::PARTITIONED) {
            throw std::invalid_argument("ClusterConfig: lossy automatic failover requires MIRROR or PARTITIONED placement");
        }
        if (usesDataConsensus() && consistency_.mode != ConsistencyMode::RAFT_QUORUM &&
            consistency_.mode != ConsistencyMode::ASYNC &&
            !(consistency_.writeConsistency == WriteConsistency::LOCAL ||
                (consistency_.writeConsistency == WriteConsistency::LEGACY_ACK_POLICY && ackPolicy_.mode == AckPolicyMode::NONE))) {
            throw std::invalid_argument("ClusterConfig: lossy primary-ACK failover requires LOCAL completion");
        }
        if (consistency_.writeConsistency != WriteConsistency::LEGACY_ACK_POLICY && consistency_.writeConsistency != WriteConsistency::LOCAL
            && consistency_.writeConsistency != WriteConsistency::ONE_REPLICA && consistency_.writeConsistency != WriteConsistency::QUORUM
            && consistency_.writeConsistency != WriteConsistency::ALL_CONFIGURED && consistency_.writeConsistency !=
            WriteConsistency::AVAILABLE_REPLICAS) {
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
            RaftMembershipMode::JOINT_CONSENSUS) && !usesDataConsensus()) {
            throw std::invalid_argument("ClusterConfig: Raft membership options require RAFT_QUORUM consistency");
        }
        if (usesDataConsensus() && mode_ == ReplicationMode::STRIPE) {
            throw std::invalid_argument("ClusterConfig: STRIPE uses its separate metadata quorum");
        }
        const auto fixedMirrorPrimary = mode_ == ReplicationMode::MIRROR && !usesDataConsensus();
        if (!fixedMirrorPrimary && primaryNodeId_ != 0) {
            throw std::invalid_argument("ClusterConfig: primaryNodeId is only valid for non-Raft MIRROR placement");
        }
        std::unordered_set<uint64_t> ids;
        std::unordered_set<std::string> replicationEndpoints;
        std::unordered_set<std::string> stripeMetadataEndpoints;
        bool hasDataNode = false;
        bool hasCoordinator = false;
        size_t dataNodeCount = 0;
        size_t stripeFailoverCount = 0;
        std::map<std::string, std::string> physicalHosts;
        for (const auto& node : nodes_) {
            if (node.nodeId == 0) { throw std::invalid_argument("ClusterConfig: nodeId 0 is reserved"); }
            if (!ids.insert(node.nodeId).second) { throw std::invalid_argument("ClusterConfig: duplicate nodeId"); }
            if (node.host.empty()) { throw std::invalid_argument("ClusterConfig: empty host"); }
            if (node.host.size() > MAX_HOST_BYTES) { throw std::invalid_argument("ClusterConfig: host name too long"); }
            for (const auto label : {std::string_view{node.hostId}, std::string_view{node.siteId}}) {
                if (label.size() > MAX_HOST_BYTES || std::ranges::any_of(label, [](unsigned char b) { return b <= 32 || b == 127; })) {
                    throw std::invalid_argument("ClusterConfig: invalid physical fault-domain label");
                }
            }
            if (!node.hostId.empty()) {
                const auto [it, inserted] = physicalHosts.emplace(node.hostId, node.siteId);
                if (!inserted && it->second != node.siteId) { throw std::invalid_argument("ClusterConfig: one physical host cannot belong to multiple sites"); }
            }
            if ((faultDomains_.minHosts && node.hostId.empty()) || ((faultDomains_.minSites || faultDomains_.requireSiteQuorum) && node.siteId.empty())) {
                throw std::invalid_argument("ClusterConfig: fault-domain policy requires explicit node labels");
            }
            std::string endpointHost = node.host;
            for (auto& ch : endpointHost) {
                if (static_cast<unsigned char>(ch) <= 32 || static_cast<unsigned char>(ch) == 127) {
                    throw std::invalid_argument("ClusterConfig: host contains whitespace or control characters");
                }
                if (ch >= 'A' && ch <= 'Z') { ch = static_cast<char>(ch + ('a' - 'A')); }
            }
            if (endpointHost.size() > 1 && endpointHost.back() == '.') { endpointHost.pop_back(); }
            if ((mode_ != ReplicationMode::STANDALONE || usesDataConsensus()) && node.replPort == 0) {
                throw std::invalid_argument("ClusterConfig: cluster replication port must be nonzero");
            }
            if (node.replPort != 0) {
                const auto endpoint = endpointHost + ":" + std::to_string(node.replPort);
                if (!replicationEndpoints.insert(endpoint).second || stripeMetadataEndpoints.contains(endpoint)) {
                    throw std::invalid_argument("ClusterConfig: duplicate replication endpoint");
                }
            }
            if (mode_ == ReplicationMode::STRIPE && (node.dataBearing() || isStripeMetadataVoter(node)) && node.stripeMetadataPort == 0) {
                throw std::invalid_argument("ClusterConfig: STRIPE metadata Raft port must be nonzero on every metadata participant");
            }
            if (node.stripeMetadataPort != 0) {
                const auto endpoint = endpointHost + ":" + std::to_string(node.stripeMetadataPort);
                if (!stripeMetadataEndpoints.insert(endpoint).second || replicationEndpoints.contains(endpoint)) {
                    throw std::invalid_argument("ClusterConfig: duplicate STRIPE metadata endpoint");
                }
            }
            if ((node.capabilities & ~(COORDINATOR_ELIGIBLE | DATA_BEARING | STRIPE_FAILOVER_ELIGIBLE | RAFT_LEARNER | PLACEMENT_STANDBY | STRIPE_METADATA_VOTER)) != 0) {
                throw std::invalid_argument("ClusterConfig: unknown node capability");
            }
            if (node.placementStandby() && (mode_ != ReplicationMode::STRIPE || !node.dataBearing())) {
                throw std::invalid_argument("ClusterConfig: placement standby requires a STRIPE data node");
            }
            if (node.raftLearner() && (!node.dataBearing() || !raft_.membership.allowLearners ||
                !usesDataConsensus())) {
                throw std::invalid_argument("ClusterConfig: learner requires a data-bearing RAFT_QUORUM node and allowLearners");
            }
            if (node.stripeFailoverEligible()) {
                ++stripeFailoverCount;
                if (!node.dataBearing() || !node.coordinatorEligible()) {
                    throw std::invalid_argument("ClusterConfig: STRIPE failover candidate must be coordinator-eligible and data-bearing");
                }
            }
            if ((node.capabilities & STRIPE_METADATA_VOTER) != 0 &&
                (mode_ != ReplicationMode::STRIPE || stripe_.metadataPlacement != StripeMetadataPlacement::EXPLICIT_VOTERS ||
                 !node.coordinatorEligible() || node.placementStandby())) {
                throw std::invalid_argument("ClusterConfig: explicit metadata voter requires EXPLICIT_VOTERS, coordinator eligibility and no standby role");
            }
            if (mode_ == ReplicationMode::STRIPE && !node.dataBearing() && !isStripeMetadataVoter(node)) {
                throw std::invalid_argument("ClusterConfig: STRIPE participant must store data or vote on metadata");
            }
            if (node.dataBearing()) { ++dataNodeCount; }
            hasDataNode = hasDataNode || node.dataBearing();
            hasCoordinator = hasCoordinator || (node.coordinatorEligible() && !node.raftLearner());
        }
        if (usesDataConsensus() && std::ranges::none_of(nodes_, [](const NodeInfo& node) {
            return node.dataBearing() && !node.raftLearner();
        })) { throw std::invalid_argument("ClusterConfig: Raft requires at least one voter"); }
        if (mode_ != ReplicationMode::STANDALONE && !hasDataNode) {
            throw std::invalid_argument("ClusterConfig: cluster mode requires a data-bearing node");
        }
        if (mode_ != ReplicationMode::STANDALONE && !hasCoordinator) {
            throw std::invalid_argument("ClusterConfig: cluster mode requires a coordinator-eligible node");
        }
        if (usesDataConsensus() && dataNodeCount > 512) {
            throw std::invalid_argument("ClusterConfig: RAFT_QUORUM supports at most 512 members (voters plus learners)");
        }
        if (stripeFailoverCount > 1 && stripe_.failoverPolicy != StripeFailoverPolicy::MULTIPLE_CANDIDATES) {
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
        if (partition_.replicationFactor == 0) {
            throw std::invalid_argument("ClusterConfig: partition replicationFactor must be > 0");
        }
        if (partition_.partitionCount == 0 || partition_.partitionCount > 256) {
            throw std::invalid_argument("ClusterConfig: partitionCount must be in [1, 256]");
        }
        if (mode_ == ReplicationMode::PARTITIONED && usesDataConsensus()) {
            std::unordered_set<std::string> reserved = replicationEndpoints;
            reserved.insert(stripeMetadataEndpoints.begin(), stripeMetadataEndpoints.end());
            for (const auto& node : dataNodes()) {
                if (node.partitionReplBasePort == 0 ||
                    static_cast<uint32_t>(node.partitionReplBasePort) + partition_.partitionCount >= 65536) {
                    throw std::invalid_argument("ClusterConfig: PARTITIONED Raft requires a valid partition replication port range");
                }
                for (uint32_t group = 0; group <= partition_.partitionCount; ++group) {
                    std::string host = node.host;
                    for (auto& ch : host) { ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch))); }
                    if (host.size() > 1 && host.back() == '.') { host.pop_back(); }
                    const auto endpoint = host + ":" + std::to_string(node.partitionReplBasePort + group);
                    if (!reserved.insert(endpoint).second) {
                        throw std::invalid_argument("ClusterConfig: overlapping partition replication endpoints");
                    }
                }
            }
        }
        if (mode_ == ReplicationMode::PARTITIONED) {
            const auto replicaCount = partitionCopies() - 1;
            if (((ackPolicy_.mode == AckPolicyMode::QUORUM || consistency_.writeConsistency == WriteConsistency::QUORUM) && ackPolicy_.quorum > replicaCount) ||
                (consistency_.writeConsistency == WriteConsistency::ONE_REPLICA && replicaCount == 0)) {
                throw std::invalid_argument("ClusterConfig: partition acknowledgement count exceeds key replicas");
            }
        }
        if (consistency_.writeConsistency == WriteConsistency::AVAILABLE_REPLICAS && raidPreset() != RaidPreset::RAID1) {
            throw std::invalid_argument("ClusterConfig: AVAILABLE_REPLICAS is reserved for the RAID.1 contract");
        }
        if (raidPreset() == RaidPreset::RAID1 && (dataNodeCount < 2 || dataNodeCount != nodes_.size())) {
            throw std::invalid_argument("ClusterConfig: RAID.1 requires at least two nodes and every node must be data-bearing");
        }
        if (mode_ == ReplicationMode::STRIPE) {
            if (stripe_.metadataPlacement > StripeMetadataPlacement::EXPLICIT_VOTERS ||
                stripe_.failoverPolicy > StripeFailoverPolicy::MULTIPLE_CANDIDATES) {
                throw std::invalid_argument("ClusterConfig: invalid STRIPE metadata/failover policy");
            }
            if (std::ranges::none_of(nodes_, [this](const auto& node) { return isStripeMetadataVoter(node); })) {
                throw std::invalid_argument("ClusterConfig: STRIPE metadata requires at least one voter");
            }
            if (stripeMetadataNodes().size() > 512) { throw std::invalid_argument("ClusterConfig: metadata supports at most 512 voters and learners"); }
            if (stripe_.dataShards == 0) { throw std::invalid_argument("ClusterConfig: stripe dataShards must be > 0"); }
            if (stripe_.copiesPerShard != 1 && stripe_.copiesPerShard != 2) {
                throw std::invalid_argument("ClusterConfig: stripe copiesPerShard must be 1 or 2");
            }
            if (stripe_.copiesPerShard == 2 && stripe_.parityShards != 0) {
                throw std::invalid_argument("ClusterConfig: mirrored STRIPE shards cannot also use parity shards");
            }
            if (stripe_.parityShards == 0 && stripe_.dataShards < 2) {
                throw std::invalid_argument("ClusterConfig: RAID.0 requires at least two data shards");
            }
            if (stripe_.totalShards() > 255) { throw std::invalid_argument("ClusterConfig: stripe total shard count must be <= 255"); }
            if (stripePlacementNodes().size() < stripe_.totalPlacements()) {
                throw std::invalid_argument("ClusterConfig: stripe mode requires enough data-bearing nodes for every shard placement");
            }
            const auto stripePlacementNodeCount = stripePlacementNodes().size();
            if (stripe_.copiesPerShard == 2 && stripePlacementNodeCount != stripe_.totalPlacements()) {
                throw std::invalid_argument(
                    "ClusterConfig: RAID.10 requires exactly two normal placement nodes per mirrored shard"
                );
            }
            if (stripe_.parityShards >= 2 && stripe_.metadataPlacement == StripeMetadataPlacement::DATA_NODES) {
                const auto metadataVotersRequired = static_cast<size_t>(stripe_.parityShards) * 2 + 1;
                const auto metadataVoters = std::ranges::count_if(nodes_, [](const auto& node) { return node.dataBearing() && !node.placementStandby(); });
                if (static_cast<size_t>(metadataVoters) < metadataVotersRequired) {
                    throw std::invalid_argument(
                        "ClusterConfig: multi-failure STRIPE requires at least 2 * parityShards + 1 "
                        "data-bearing nodes for metadata quorum"
                    );
                }
            }
            if (stripeFailoverCount != 0 && dataNodeCount <= stripe_.totalPlacements()) {
                throw std::invalid_argument("ClusterConfig: STRIPE owner failover requires one spare data-bearing node");
            }
        }
        if (faultDomains_.minHosts || faultDomains_.minSites || faultDomains_.requireSiteQuorum) {
            if (mode_ == ReplicationMode::PARTITIONED) {
                auto candidates = dataNodes();
                if (usesDataConsensus()) { std::erase_if(candidates, [](const auto& n) { return n.raftLearner(); }); }
                if (usesDataConsensus()) { detail::validateFaultDomains(candidates, faultDomains_, true); }
                const uint16_t groups = usesDataConsensus() ? partition_.partitionCount : 1;
                for (uint16_t i = 0; i < groups; ++i) {
                    const std::array<uint8_t, 2> key{static_cast<uint8_t>(i), static_cast<uint8_t>(i >> 8)};
                    (void)detail::placementNodes(key, candidates, partitionCopies(), faultDomains_, usesDataConsensus());
                }
            } else if (mode_ == ReplicationMode::MIRROR || (mode_ == ReplicationMode::STANDALONE && usesDataConsensus())) {
                auto voters = dataNodes(); std::erase_if(voters, [](const auto& n) { return n.raftLearner(); });
                detail::validateFaultDomains(voters, faultDomains_, usesDataConsensus());
            } else if (mode_ == ReplicationMode::STRIPE) {
                auto metadata = stripeMetadataNodes();
                std::erase_if(metadata, [this](const auto& n) { return !isStripeMetadataVoter(n); });
                detail::validateFaultDomains(metadata, faultDomains_, true);
                if (stripe_.copiesPerShard == 2) {
                    std::vector<uint64_t> ids; for (const auto& n : detail::raidPairNodes(*this)) { ids.push_back(n.nodeId); }
                    detail::validateStripeFaultDomains(*this, ids);
                } else { (void)detail::placementNodes({}, stripePlacementNodes(), stripe_.totalShards(), faultDomains_); }
            } else {
                throw std::invalid_argument("ClusterConfig: fault-domain placement requires a cluster mode");
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
        options.replicationLiveness.validate();
        validate();
        options.transfer.validate();
        detail::validateNodeResourceLimits(options.nodeResources);
        if (options.transactions.mode != ClusterTransactionMode::DISABLED && options.transactions.mode != ClusterTransactionMode::TWO_PHASE_COMMIT) {
            throw std::invalid_argument("ClusterConfig: invalid transaction mode");
        }
        if (options.transactions.mode == ClusterTransactionMode::TWO_PHASE_COMMIT &&
            (!usesDataConsensus() || (mode() != ReplicationMode::MIRROR && mode() != ReplicationMode::PARTITIONED) ||
             options.readMode != ClusterReadMode::OWNER_LINEARIZABLE || options.transactions.timeoutMs == 0 || options.transactions.timeoutMs > 300'000 ||
             options.transactions.recoveryIntervalMs == 0 || options.transactions.recoveryIntervalMs > 60'000 ||
             options.transactions.recoveryConcurrency == 0 || options.transactions.recoveryConcurrency > 32 ||
             options.transactions.recoveryAttemptTimeoutMs == 0 || options.transactions.recoveryAttemptTimeoutMs > 300'000)) {
            throw std::invalid_argument("ClusterConfig: TWO_PHASE_COMMIT requires MIRROR/PARTITIONED Raft, OWNER_LINEARIZABLE reads and valid transaction timeouts");
        }
        if (options.raftMaxForwardRequests == 0 || options.raftMaxForwardRequests > 256) {
            throw std::invalid_argument("ClusterConfig: Raft forward concurrency must be 1-256");
        }
        constexpr uint64_t minReceiveBytes = 3ull * (ReplFrameHeader::SIZE + ReplFrameHeader::MAX_RAFT_PAYLOAD_SIZE);
        if (options.raftGroupKind > RaftGroupKind::REQUEST_AUTHORITY ||
            (options.raftGroupKind != RaftGroupKind::PARTITION_DATA && options.raftPartitionIndex != 0) ||
            options.raftMaxIncomingForwardRequests == 0 || options.raftMaxIncomingForwardRequests > 256 ||
            options.raftMaxForwardReceiveMemoryBytes < minReceiveBytes ||
            options.raftMaxForwardReceiveMemoryBytes > 64ull * 1024 * 1024 * 1024) {
            throw std::invalid_argument("ClusterConfig: invalid Raft namespace or inbound forwarding limits");
        }
        if ((options.reconfiguration.writePolicy != ReconfigurationWritePolicy::WAIT &&
            options.reconfiguration.writePolicy != ReconfigurationWritePolicy::REJECT) ||
            (options.reconfiguration.stripeMigrationMode != StripeMigrationMode::FREEZE &&
             options.reconfiguration.stripeMigrationMode != StripeMigrationMode::LIVE_COPY) ||
            (options.reconfiguration.stripeInterruptionPolicy != StripeInterruptionPolicy::RETAIN &&
             options.reconfiguration.stripeInterruptionPolicy != StripeInterruptionPolicy::CANCEL) ||
            options.reconfiguration.timeoutMs == 0 || options.reconfiguration.timeoutMs > 300'000 ||
            options.reconfiguration.maxConcurrentPartitions == 0 || options.reconfiguration.maxConcurrentPartitions > 16) {
            throw std::invalid_argument("ClusterConfig: invalid reconfiguration admission policy or timeout");
        }
        if (mode() != ReplicationMode::STRIPE &&
            (options.reconfiguration.stripeMigrationMode != StripeMigrationMode::FREEZE ||
             options.reconfiguration.stripeInterruptionPolicy != StripeInterruptionPolicy::RETAIN)) {
            throw std::invalid_argument("ClusterConfig: stripe migration options require STRIPE");
        }
        if (options.routingMode != ClusterRoutingMode::LOCAL_ONLY && options.routingMode != ClusterRoutingMode::REDIRECT &&
            options.routingMode != ClusterRoutingMode::FORWARD) {
            throw std::invalid_argument("ClusterConfig: invalid routing mode");
        }
        if (options.forwardingTimeoutMs == 0 || options.forwardingTimeoutMs > 300000) {
            throw std::invalid_argument("ClusterConfig: forwarding timeout must be 1-300000 ms");
        }
        if ((options.queryResultMode != QueryResultMode::PREPARED && options.queryResultMode != QueryResultMode::STREAMING) ||
            (options.queryConsistencyMode != QueryConsistencyMode::PER_PARTITION && options.queryConsistencyMode != QueryConsistencyMode::GLOBAL_SNAPSHOT) ||
            (options.globalSnapshotConflictPolicy != GlobalSnapshotConflictPolicy::RETRY && options.globalSnapshotConflictPolicy != GlobalSnapshotConflictPolicy::BLOCK_WRITES) ||
            (options.querySnapshotAdmission != QuerySnapshotAdmission::WAIT && options.querySnapshotAdmission != QuerySnapshotAdmission::REJECT) ||
            options.queryMaxPinnedBytes < 1024ull * 1024 || options.queryMaxPinnedBytes > 1024ull * 1024 * 1024 * 1024 ||
            options.queryMaxPinnedSnapshots == 0 || options.queryMaxPinnedSnapshots > 1024 ||
            options.queryCursorMaxLifetimeMs == 0 || options.queryCursorMaxLifetimeMs > 3'600'000) {
            throw std::invalid_argument("ClusterConfig: invalid query snapshot/delivery policy");
        }
        if (options.queryMaxSpoolBytes == 0 || options.queryMaxSpoolBytes > static_cast<uint64_t>(INT64_MAX) ||
            options.queryMaxOpenCursors == 0 || options.queryMaxOpenCursors > 1024 ||
            options.queryCursorIdleTimeoutMs == 0 || options.queryCursorIdleTimeoutMs > 300'000) {
            throw std::invalid_argument("ClusterConfig: invalid cluster query limits");
        }
        if (options.globalSnapshotConflictPolicy == GlobalSnapshotConflictPolicy::BLOCK_WRITES &&
            (options.queryConsistencyMode != QueryConsistencyMode::GLOBAL_SNAPSHOT || !usesDataConsensus() ||
             (mode() != ReplicationMode::MIRROR && mode() != ReplicationMode::PARTITIONED) ||
             options.transactions.mode != ClusterTransactionMode::TWO_PHASE_COMMIT)) {
            throw std::invalid_argument("ClusterConfig: BLOCK_WRITES requires GLOBAL_SNAPSHOT and MIRROR/PARTITIONED Raft TWO_PHASE_COMMIT");
        }
        std::unordered_set<uint64_t> endpointIds;
        for (const auto& endpoint : options.apiEndpoints) {
            if (!findById(endpoint.nodeId) || !endpointIds.insert(endpoint.nodeId).second) {
                throw std::invalid_argument("ClusterConfig: unknown or duplicate API endpoint node");
            }
        }
        if (options.transportMode != TransportMode::PLAIN && options.transportMode != TransportMode::SECURE) {
            throw std::invalid_argument("ClusterConfig: invalid transport mode");
        }
        const auto raft = usesDataConsensus();
        const auto fencing = options.mirrorFencing.mode;
        if (fencing != MirrorFencingMode::STATIC && fencing != MirrorFencingMode::QUORUM_FENCED && fencing != MirrorFencingMode::EXTERNAL_FENCED) {
            throw std::invalid_argument("ClusterConfig: invalid MIRROR fencing mode");
        }
        if (fencing != MirrorFencingMode::STATIC && (mode_ != ReplicationMode::MIRROR || raft)) {
            throw std::invalid_argument("ClusterConfig: MIRROR fencing policy requires non-Raft MIRROR");
        }
        if (fencing == MirrorFencingMode::STATIC && options.mirrorPromotion.enabled) {
            throw std::invalid_argument("ClusterConfig: STATIC MIRROR forbids promotion");
        }
        if (fencing == MirrorFencingMode::EXTERNAL_FENCED && !options.mirrorFencing.external) {
            throw std::invalid_argument("ClusterConfig: EXTERNAL_FENCED requires a fencing provider");
        }
        if (fencing != MirrorFencingMode::EXTERNAL_FENCED && options.mirrorFencing.external) {
            throw std::invalid_argument("ClusterConfig: external authority provider requires EXTERNAL_FENCED; use mirrorRecovery for quorum grant recovery");
        }
        const auto& recovery = options.mirrorRecovery;
        if (recovery.mode != MirrorRecoveryMode::BLOCK && recovery.mode != MirrorRecoveryMode::MANUAL && recovery.mode != MirrorRecoveryMode::AUTOMATIC) {
            throw std::invalid_argument("ClusterConfig: invalid MIRROR recovery mode");
        }
        if ((recovery.mode == MirrorRecoveryMode::BLOCK) != !recovery.provider) {
            throw std::invalid_argument("ClusterConfig: MIRROR recovery provider is required only for MANUAL/AUTOMATIC recovery");
        }
        if (recovery.mode != MirrorRecoveryMode::BLOCK && fencing != MirrorFencingMode::QUORUM_FENCED) {
            throw std::invalid_argument("ClusterConfig: unresolved MIRROR grant recovery requires QUORUM_FENCED authority");
        }
        if (recovery.retryIntervalMs < 50 || recovery.retryIntervalMs > 3'600'000) {
            throw std::invalid_argument("ClusterConfig: MIRROR recovery retry interval must be in [50, 3600000] ms");
        }
        if (fencing != MirrorFencingMode::QUORUM_FENCED && !options.mirrorFencing.authorityNodes.empty()) {
            throw std::invalid_argument("ClusterConfig: authority nodes require QUORUM_FENCED");
        }
        if (fencing == MirrorFencingMode::QUORUM_FENCED) {
            if (nodes_.size() < 3 || nodes_.size() > 512 || options.mirrorFencing.authorityNodes.size() != nodes_.size() ||
                options.clusterGroupId == 0 || options.resetClusterMembership) {
                throw std::invalid_argument("ClusterConfig: QUORUM_FENCED requires 3-512 matching authority nodes, a shared group id, and no membership reset");
            }
            std::unordered_set<uint64_t> authorityIds;
            for (const auto& authority : options.mirrorFencing.authorityNodes) {
                const auto* node = findById(authority.nodeId);
                if (!authorityIds.insert(authority.nodeId).second || !node || authority.host != node->host ||
                    authority.capabilities != (COORDINATOR_ELIGIBLE | DATA_BEARING) || authority.replPort == 0) {
                    throw std::invalid_argument("ClusterConfig: invalid MIRROR authority participant");
                }
                for (const auto& peer : nodes_) {
                    if (peer.host == authority.host && (peer.replPort == authority.replPort || peer.stripeMetadataPort == authority.replPort || peer.dataPort == authority.replPort)) {
                        throw std::invalid_argument("ClusterConfig: MIRROR authority port conflicts with data replication");
                    }
                }
            }
            if (options.corruptStateAction != CorruptClusterStateAction::FAIL_STARTUP) {
                throw std::invalid_argument("ClusterConfig: MIRROR authority corruption must fail startup");
            }
        }
        if (options.requests.defaultRetentionMs == 0 || options.requests.defaultRetentionMs > options.requests.maxRetentionMs ||
            options.requests.maxRetentionMs == 0 || options.requests.maxRetentionMs > 365ull * 24 * 60 * 60 * 1000 ||
            options.requests.maxPendingRequests == 0 || options.requests.maxPendingRequests > 32'768 ||
            options.requests.maxRetainedResultBytes == 0 || options.requests.maxRetainedResultBytes > (1ull << 40) ||
            options.transactions.maxRetainedResultBytes == 0 || options.transactions.maxRetainedResultBytes > (1ull << 40)) {
            throw std::invalid_argument("ClusterConfig: invalid request retention or result capacity");
        }
        if (options.requests.persistLocalApplication && (!options.requests.enabled || usesDataConsensus() || mode() == ReplicationMode::STRIPE)) {
            throw std::invalid_argument("ClusterConfig: local request application requires non-Raft MIRROR, PARTITIONED or STANDALONE data");
        }
        if (raft && (options.raftSnapshot.minLogEntries == 0 || options.raftSnapshot.minLogEntries > 1'000'000'000ull ||
            options.raftSnapshot.minLogBytes == 0 || options.raftSnapshot.minLogBytes > 1024ull * 1024 * 1024 * 1024 ||
            options.raftSnapshot.maxIntervalMs == 0 || options.raftSnapshot.maxIntervalMs > 7ull * 24 * 60 * 60 * 1000)) {
            throw std::invalid_argument("ClusterConfig: invalid Raft snapshot policy");
        }
        if (raft && (options.raftHeartbeatIntervalMs < 10 || options.raftHeartbeatIntervalMs > 1'000)) {
            throw std::invalid_argument("ClusterConfig: Raft heartbeat interval must be in [10, 1000] ms");
        }
        constexpr uint64_t minRaftReceiveMemoryBytes =
            3ull * (ReplFrameHeader::SIZE + ReplFrameHeader::MAX_RAFT_PAYLOAD_SIZE);
        if (raft && (options.raftMaxReceiveMemoryBytes < minRaftReceiveMemoryBytes ||
            options.raftMaxReceiveMemoryBytes > 64ull * 1024 * 1024 * 1024)) {
            throw std::invalid_argument("ClusterConfig: invalid Raft receive-memory limit");
        }
        if ((options.memoryOnlySnapshot.mode != MemoryOnlySnapshotMode::THROUGHPUT_FIRST &&
                options.memoryOnlySnapshot.mode != MemoryOnlySnapshotMode::COMPLETION_FIRST) ||
            options.memoryOnlySnapshot.maxPinnedBytes < 1024ull * 1024 ||
            options.memoryOnlySnapshot.maxPinnedBytes > 1024ull * 1024 * 1024 * 1024 ||
            options.memoryOnlySnapshot.maxPinnedGenerations == 0 || options.memoryOnlySnapshot.maxPinnedGenerations > 64) {
            throw std::invalid_argument("ClusterConfig: invalid memory-only snapshot policy");
        }
        if (!options.requests.authorityNodes.empty() && (!options.requests.enabled || usesDataConsensus())) {
            throw std::invalid_argument("ClusterConfig: request authority nodes are only used by enabled non-Raft requests");
        }
        if (options.requests.enabled && !usesDataConsensus()) {
            if (options.requests.authorityNodes.empty() || options.requests.authorityNodes.size() > 512 || options.resetClusterMembership ||
                options.corruptStateAction != CorruptClusterStateAction::FAIL_STARTUP) {
                throw std::invalid_argument("ClusterConfig: non-Raft requests require an authority group and fail-closed persistent state");
            }
            std::unordered_set<uint64_t> ids;
            std::unordered_set<std::string> endpoints;
            for (const auto& authority : options.requests.authorityNodes) {
                const auto* node = findById(authority.nodeId);
                if (!node || !ids.insert(authority.nodeId).second || authority.host != node->host || !authority.replPort ||
                    (authority.capabilities != (COORDINATOR_ELIGIBLE | DATA_BEARING) && authority.capabilities != (COORDINATOR_ELIGIBLE | DATA_BEARING | RAFT_LEARNER)) ||
                    !endpoints.insert(authority.host + ":" + std::to_string(authority.replPort)).second) {
                    throw std::invalid_argument("ClusterConfig: invalid request authority participant");
                }
                for (const auto& peer : nodes_) {
                    if (peer.host == authority.host && (peer.replPort == authority.replPort || peer.dataPort == authority.replPort ||
                        peer.stripeMetadataPort == authority.replPort ||
                        (peer.partitionReplBasePort && authority.replPort >= peer.partitionReplBasePort &&
                         uint32_t{authority.replPort} <= uint32_t{peer.partitionReplBasePort} + partition_.partitionCount))) {
                        throw std::invalid_argument("ClusterConfig: request authority port conflicts with data or placement");
                    }
                }
                for (const auto& peer : options.mirrorFencing.authorityNodes) {
                    if (peer.host == authority.host && peer.replPort == authority.replPort) {
                        throw std::invalid_argument("ClusterConfig: request authority port conflicts with MIRROR authority");
                    }
                }
            }
            if (std::ranges::none_of(options.requests.authorityNodes, [](const auto& node) { return !node.raftLearner(); })) {
                throw std::invalid_argument("ClusterConfig: request authority requires a voter");
            }
        }
        if (options.mirrorPromotion.enabled) {
            if (raft || mode_ != ReplicationMode::MIRROR) {
                throw std::invalid_argument("ClusterConfig: offline Primary promotion is only valid for non-Raft MIRROR");
            }
            if (primaryNodeId_ == options.mirrorPromotion.previousPrimaryNodeId ||
                primaryNodeId_ == 0 || options.startupRole == NodeStartupRole::AUTO) {
                throw std::invalid_argument("ClusterConfig: MIRROR promotion requires a new configured Primary and explicit local role");
            }
            if (options.mirrorPromotion.previousPrimaryNodeId == 0 ||
                findById(options.mirrorPromotion.previousPrimaryNodeId) == nullptr) {
                throw std::invalid_argument("ClusterConfig: MIRROR promotion source is not a configured node");
            }
            if (options.mirrorPromotion.previousPrimaryNodeId == selfNodeId &&
                options.startupRole == NodeStartupRole::PRIMARY) {
                throw std::invalid_argument("ClusterConfig: MIRROR promotion candidate must differ from the previous Primary");
            }
            if (options.mirrorPromotion.previousGroupEpoch == 0 ||
                options.mirrorPromotion.previousGroupEpoch == UINT64_MAX) {
                throw std::invalid_argument("ClusterConfig: invalid MIRROR promotion epoch");
            }
            if (options.clusterGroupEpoch != 0 &&
                options.clusterGroupEpoch != options.mirrorPromotion.previousGroupEpoch + 1) {
                throw std::invalid_argument("ClusterConfig: MIRROR promotion group epoch must be the next epoch");
            }
            if ((selfNodeId == primaryNodeId_) != (options.startupRole == NodeStartupRole::PRIMARY)) {
                throw std::invalid_argument("ClusterConfig: only the promoted MIRROR candidate may start as PRIMARY");
            }
            if (options.resetClusterMembership) {
                throw std::invalid_argument("ClusterConfig: MIRROR promotion cannot reset membership");
            }
        }
        if ((mode_ == ReplicationMode::PARTITIONED || mode_ == ReplicationMode::STRIPE) && options.clusterGroupId == 0) {
            throw std::invalid_argument("ClusterConfig: PARTITIONED and STRIPE require an explicit nonzero shared cluster group id");
        }
        if (!isStandalone() || raft) {
            if (findById(selfNodeId) == nullptr) { throw std::invalid_argument("ClusterConfig: self node is not configured"); }
        }
        std::vector<uint64_t> required;
        if (options.transportMode == TransportMode::SECURE && (!isStandalone() || raft)) {
            for (const auto& peer : nodes_) {
                if (peer.nodeId == selfNodeId || (!peer.dataBearing() && !isStripeMetadataVoter(peer) && fencing != MirrorFencingMode::QUORUM_FENCED)) { continue; }
                if (!raft && fencing != MirrorFencingMode::QUORUM_FENCED && mode_ == ReplicationMode::MIRROR && selfNodeId != primaryNodeId_ && peer.nodeId != primaryNodeId_) { continue; }
                required.push_back(peer.nodeId);
            }
        }
        // Extra pins are allowed so future Raft voters can be provisioned before joining.
        options.secure.validatePins(required);
    }
    MirrorFencingRecoveryProvider::MirrorFencingRecoveryProvider(std::shared_ptr<IMirrorFencingProvider> fencing)
        : fencing_{std::move(fencing)} {
        if (!fencing_) { throw std::invalid_argument("MIRROR recovery: fencing provider is required"); }
    }

    std::optional<MirrorRecoveryProof> MirrorFencingRecoveryProvider::recover(
        const MirrorRecoveryRequest& request, std::stop_token cancelled) {
        if (cancelled.stop_requested()) { throw std::runtime_error("MIRROR recovery: cancelled"); }
        if (request.action != MirrorRecoveryAction::PROMOTE_PRIMARY) { return std::nullopt; }
        fencing_->fence(MirrorFenceRequest{.clusterId = request.clusterId, .groupId = request.groupId,
            .previousPrimaryNodeId = request.previousPrimaryNodeId, .previousGroupEpoch = request.previousGroupEpoch,
            .candidateNodeId = request.candidateNodeId, .expectedDurableSeq = request.expectedDurableSeq});
        if (cancelled.stop_requested()) { throw std::runtime_error("MIRROR recovery: cancelled after fencing"); }
        return MirrorRecoveryProof{request};
    }
} // namespace akkaradb::engine::cluster

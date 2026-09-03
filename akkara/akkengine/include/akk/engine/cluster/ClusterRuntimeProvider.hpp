/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/cluster/ClusterRuntimeProvider.hpp
#pragma once

#include "akkaradb/Export.hpp"

#include "akk/engine/cluster/ClusterConfig.hpp"
#include "akk/engine/cluster/ReplFraming.hpp"

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

namespace akkaradb::engine::cluster {
    /** Core-to-runtime representation of a retained mutation. */
    struct ClusterHistoryEntry {
        uint64_t seq = 0;
        uint64_t sourceNodeId = 0;
        uint8_t op = 0;
        uint8_t recordFlags = 0;
        std::vector<uint8_t> key;
        std::vector<uint8_t> value;
    };

    struct ClusterSnapshot {
        uint64_t seq = 0;
        std::vector<ClusterHistoryEntry> entries;
    };

    struct AKDB_API ClusterEngineCallbacks {
        std::function<uint64_t()> getCurrentSeq;
        std::function<uint64_t()> getLastSeq;
        // Returns a complete, contiguous (afterSeq, throughSeq] mutation range,
        // or nullopt when the retained WAL cannot satisfy the request.
        std::function<std::optional<std::vector<ClusterHistoryEntry>>(uint64_t afterSeq, uint64_t throughSeq)> getEntries;
        std::function<std::optional<ClusterSnapshot>()> exportSnapshot;
        std::function<void(uint64_t snapshotSeq, uint64_t entryCount)> beginSnapshot;
        std::function<void(std::span<const uint8_t> key, uint64_t valueSize, uint32_t valueCrc32c)> beginSnapshotEntry;
        std::function<void(uint64_t offset, std::span<const uint8_t> chunk)> appendSnapshotEntryChunk;
        std::function<void()> finishSnapshotEntry;
        std::function<void(uint64_t snapshotSeq)> finishSnapshot;
        std::function<void(uint64_t snapshotSeq)> recoverSnapshot;
        std::function<bool(uint64_t snapshotSeq)> isSnapshotDurable;
        std::function<void(
uint64_t seq,
 ReplOpType op,
 std::span<const uint8_t> key,
 std::span<const uint8_t> value,
 uint8_t recordFlags,
 uint64_t sourceNodeId
        )> apply;
        std::function<ReadResponse(std::span<const uint8_t> key, uint64_t snapshotSeq)> read;
        std::function<void()> forceDurable;
        std::function<void(uint64_t seq, uint64_t blobId, uint64_t totalSize, uint32_t contentCrc32c)> beginBlob;
        std::function<void(uint64_t seq, uint64_t blobId, uint64_t offset, std::span<const uint8_t> chunk)> appendBlobChunk;
        std::function<void(uint64_t seq, uint64_t blobId)> finishBlob;
        std::function<void(uint64_t seq, uint64_t blobId)> abortBlob;
        std::function<void(NodeRole role)> roleChange;
    };

    class AKDB_API IClusterRuntime {
        public:
            virtual ~IClusterRuntime() = default;

            IClusterRuntime(const IClusterRuntime&) = delete;
            IClusterRuntime& operator=(const IClusterRuntime&) = delete;

            virtual void start() = 0;
            virtual void close() = 0;
            [[nodiscard]] virtual NodeRole role() const noexcept { return NodeRole::STANDALONE; }
            [[nodiscard]] virtual std::vector<NodeInfo> activeNodes() const { return {}; }
            [[nodiscard]] virtual bool ownsWriteKey(std::span<const uint8_t>) const { return true; }
            [[nodiscard]] virtual ReadResponse readKey(std::span<const uint8_t>, uint64_t) {
                throw std::runtime_error("IClusterRuntime: cluster reads are not supported");
            }
            [[nodiscard]] virtual ReadResponse readKeyFromNode(uint64_t, std::span<const uint8_t>, uint64_t) {
                throw std::runtime_error("IClusterRuntime: targeted cluster reads are not supported");
            }
            virtual void shipEntry(
                uint64_t seq,
                ReplOpType op,
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint8_t recordFlags,
                uint64_t sourceNodeId
            ) = 0;
            virtual void shipEntryTo(
                uint64_t targetNodeId,
                uint64_t seq,
                ReplOpType op,
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint8_t recordFlags,
                uint64_t sourceNodeId,
                bool waitForAck = true
            ) {
                (void)targetNodeId;
                (void)waitForAck;
                shipEntry(seq, op, key, value, recordFlags, sourceNodeId);
            }
            virtual void shipBlob(uint64_t seq, uint64_t blobId, std::span<const uint8_t> content) = 0;
            virtual void reconfigure(ClusterConfig) {
                throw std::runtime_error("IClusterRuntime: cluster reconfiguration is not supported");
            }

            virtual void addRaftVotingNode(const NodeInfo&) {
                throw std::runtime_error("IClusterRuntime: online Raft membership change is not supported");
            }

            virtual void removeRaftVotingNode(uint64_t) {
                throw std::runtime_error("IClusterRuntime: online Raft membership change is not supported");
            }

            virtual void transferRaftLeadership(uint64_t) {
                throw std::runtime_error("IClusterRuntime: Raft leader transfer is not supported");
            }

        protected:
            IClusterRuntime() = default;
    };

    using ClusterRuntimeFactory = std::unique_ptr<IClusterRuntime> (*)(
        std::filesystem::path,
        ClusterConfig,
        uint64_t,
        ClusterEngineCallbacks,
        ClusterRuntimeOptions
    );

    AKDB_API bool registerClusterRuntimeFactory(ClusterRuntimeFactory factory) noexcept;
    [[nodiscard]] AKDB_API bool clusterRuntimeFactoryAvailable() noexcept;
    [[nodiscard]] AKDB_API bool loadClusterRuntimeBackend(const std::filesystem::path& libraryPath = {});
    [[nodiscard]] AKDB_API std::string lastClusterRuntimeBackendLoadError();
    [[nodiscard]] AKDB_API std::unique_ptr<IClusterRuntime> createClusterRuntime(
        std::filesystem::path dbDir,
        ClusterConfig config,
        uint64_t selfNodeId,
        ClusterEngineCallbacks callbacks,
        ClusterRuntimeOptions runtimeOptions = {}
    );
}

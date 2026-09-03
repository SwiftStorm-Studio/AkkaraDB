/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/cluster/ClusterRuntime.hpp
#pragma once

#include "akk/engine/cluster/AkkClusterRuntimeExport.hpp"
#include "akk/engine/cluster/ClusterConfig.hpp"
#include "akk/engine/cluster/ClusterManager.hpp"
#include "akk/engine/cluster/ClusterRuntimeProvider.hpp"
#include "akk/engine/cluster/ClusterRouter.hpp"
#include "akk/engine/cluster/ReplicationClient.hpp"
#include "akk/engine/cluster/ReplicationServer.hpp"

#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <vector>

namespace akkaradb::engine::cluster {
    /**
     * ClusterRuntime - Orchestrates manager, router, replication client/server.
     *
     * The runtime is the high-level cluster facade used by the storage engine.
     * It starts ClusterManager, installs the appropriate ReplicationServer when
     * the node is Primary, and installs ReplicationClient when the node is
     * Replica.  Role changes tear down the old replication side before starting
     * the new one.
     *
     * Thread-safety: start(), close(), shipEntry(), and shipBlob() serialize
     * access to the active replication endpoint.
     */
    class AKKARADB_CLUSTER_RUNTIME_API ClusterRuntime final : public IClusterRuntime {
        public:
            /**
             * Creates a cluster runtime.
             *
             * @param dbDir          Directory used by ClusterManager.
             * @param config          Cluster membership and replication policy.
             * @param selfNodeId    Stable id of the local node.
             * @param callbacks       Engine callbacks used by replication.
             * @param runtimeOptions Transport options for replication links.
             * @throws std::invalid_argument if Plain transport is requested for
             *         a config containing non-LAN advertised node hosts.
             */
            [[nodiscard]] static std::unique_ptr<ClusterRuntime> create(
                std::filesystem::path dbDir,
                ClusterConfig config,
                uint64_t selfNodeId,
                ClusterEngineCallbacks callbacks,
                ClusterRuntimeOptions runtimeOptions = {}
            );

            ~ClusterRuntime() override;

            ClusterRuntime(const ClusterRuntime&) = delete;
            ClusterRuntime& operator=(const ClusterRuntime&) = delete;

            /** Starts role election and the role-appropriate replication endpoint. */
            void start() override;

            /** Stops the active replication endpoint and cluster manager. */
            void close() override;

            /** Returns the current local cluster role. */
            [[nodiscard]] NodeRole role() const noexcept override;

            /** Returns Manifest-derived active nodes for non-Raft runtimes. */
            [[nodiscard]] std::vector<NodeInfo> activeNodes() const override;

            /** Returns true when the local node owns writes for key. */
            [[nodiscard]] bool ownsWriteKey(std::span<const uint8_t> key) const override;

            /** Performs a cluster-routed key read according to runtime options. */
            [[nodiscard]] ReadResponse readKey(std::span<const uint8_t> key, uint64_t snapshotSeq) override;
            [[nodiscard]] ReadResponse readKeyFromNode(uint64_t nodeId, std::span<const uint8_t> key, uint64_t snapshotSeq) override;

            /** Returns the immutable key router for this config. */
            [[nodiscard]] const ClusterRouter& router() const noexcept;

            /**
             * Ships a key/value mutation to connected replicas when primary.
             *
             * No-op when the local node is not currently serving as primary.
             */
            void shipEntry(
                uint64_t seq,
                ReplOpType op,
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint8_t recordFlags,
                uint64_t sourceNodeId
            ) override;
            void shipEntryTo(
                uint64_t targetNodeId,
                uint64_t seq,
                ReplOpType op,
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint8_t recordFlags,
                uint64_t sourceNodeId,
                bool waitForAck = true
            ) override;

            /**
             * Ships a blob payload to connected replicas when primary.
             *
             * In non-Raft modes Blob frames are live-only and are not
             * sequence-ack gated by ReplicationServer.
             */
            void shipBlob(uint64_t seq, uint64_t blobId, std::span<const uint8_t> content) override;

            void addRaftVotingNode(const NodeInfo& node) override;

            void removeRaftVotingNode(uint64_t nodeId) override;

            void transferRaftLeadership(uint64_t targetNodeId) override;
            void reconfigure(ClusterConfig config) override;

        private:
            class Impl;
            explicit ClusterRuntime(std::unique_ptr<Impl> impl);
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::cluster

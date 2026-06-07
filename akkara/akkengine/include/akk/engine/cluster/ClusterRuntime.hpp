/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

// akkengine/include/akk/engine/cluster/ClusterRuntime.hpp
#pragma once

#include "akk/engine/cluster/ClusterConfig.hpp"
#include "akk/engine/cluster/ClusterManager.hpp"
#include "akk/engine/cluster/ClusterRouter.hpp"
#include "akk/engine/cluster/ReplicationClient.hpp"
#include "akk/engine/cluster/ReplicationServer.hpp"

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <span>

namespace akkaradb::engine::cluster {
    /**
     * ClusterEngineCallbacks - Hooks from cluster runtime back into the engine.
     *
     * The runtime uses these callbacks to query sequence state, apply incoming
     * replica records, apply replicated blob payloads, and report role changes.
     * Empty callbacks are allowed for optional paths, but replication work will
     * be skipped when the corresponding callback is absent.
     */
    struct ClusterEngineCallbacks {
        std::function<uint64_t()> get_current_seq; ///< Primary hello: current sequence advertised to replicas.
        std::function<uint64_t()> get_last_seq; ///< Replica hello: last applied sequence sent to primary.
        ReplicationClient::ApplyCallback apply; ///< Applies replicated put/remove entries on replicas.
        ReplicationClient::BlobCallback apply_blob; ///< Applies replicated blob payloads on replicas.
        ClusterManager::RoleChangeCallback role_change; ///< Notifies the engine after local role changes.
    };

    /**
     * ClusterRuntime - Orchestrates manager, router, replication client/server.
     *
     * The runtime is the high-level cluster facade used by the storage engine.
     * It starts ClusterManager, installs the appropriate ReplicationServer when
     * the node is Primary, and installs ReplicationClient when the node is
     * Replica.  Role changes tear down the old replication side before starting
     * the new one.
     *
     * Thread-safety: start(), close(), ship_entry(), and ship_blob() serialize
     * access to the active replication endpoint.
     */
    class ClusterRuntime {
        public:
            /**
             * Creates a cluster runtime.
             *
             * @param db_dir          Directory used by ClusterManager.
             * @param config          Cluster membership and replication policy.
             * @param self_node_id    Stable id of the local node.
             * @param callbacks       Engine callbacks used by replication.
             * @param runtime_options Transport/TLS options for replication links.
             * @throws std::invalid_argument if Stripe mode is requested before
             *         distributed write routing and ownership migration exist.
             * @throws std::invalid_argument if Plain transport is requested for
             *         a config containing non-LAN advertised node hosts.
             */
            [[nodiscard]] static std::unique_ptr<ClusterRuntime> create(
                std::filesystem::path db_dir,
                ClusterConfig config,
                uint64_t self_node_id,
                ClusterEngineCallbacks callbacks,
                ClusterRuntimeOptions runtime_options = {}
            );

            ~ClusterRuntime();

            ClusterRuntime(const ClusterRuntime&) = delete;
            ClusterRuntime& operator=(const ClusterRuntime&) = delete;

            /** Starts role election and the role-appropriate replication endpoint. */
            void start();

            /** Stops the active replication endpoint and cluster manager. */
            void close();

            /** Returns the current local cluster role. */
            [[nodiscard]] NodeRole role() const noexcept;

            /** Returns the immutable key router for this config. */
            [[nodiscard]] const ClusterRouter& router() const noexcept;

            /**
             * Ships a key/value mutation to connected replicas when primary.
             *
             * No-op when the local node is not currently serving as primary.
             */
            void ship_entry(
                uint64_t seq,
                ReplOpType op,
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint8_t record_flags,
                uint64_t source_node_id
            );

            /**
             * Ships a blob payload to connected replicas when primary.
             *
             * Blob frames are not sequence-ack gated by ReplicationServer.
             */
            void ship_blob(uint64_t seq, uint64_t blob_id, std::span<const uint8_t> content);

        private:
            class Impl;
            explicit ClusterRuntime(std::unique_ptr<Impl> impl);
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::cluster

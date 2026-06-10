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

// akkengine/include/akk/engine/cluster/ClusterManager.hpp
#pragma once

#include "akk/engine/cluster/ClusterConfig.hpp"

#include <filesystem>
#include <functional>
#include <memory>
#include <string>

namespace akkaradb::engine::cluster {
    /**
     * ClusterManager - Selects the local cluster role.
     *
     * ClusterManager owns the lightweight primary-election loop. In cluster
     * mode, every node selects the same primary from the durable config: the
     * coordinator-eligible node with the lowest nodeId. Other nodes become
     * Replica and connect to that configured primary endpoint through the
     * replication client.
     *
     * This is intentionally deterministic and LAN/WAN-safe, but it is not a
     * consensus protocol. Automatic failover across network partitions requires
     * a quorum-based coordinator above this layer.
     *
     * Thread-safety: public methods are safe to call from different threads
     * unless otherwise noted.  start() and close() are idempotent.
     */
    class ClusterManager {
        public:
            /**
             * Called whenever the local node's role changes.
             *
             * The callback is invoked after the internal role value has been
             * updated.  It may be empty.
             */
            using RoleChangeCallback = std::function<void(NodeRole)>;

            /**
             * Creates a manager for the given cluster config.
             *
             * @param dbDir       Database directory retained for API compatibility.
             * @param config       Valid cluster membership and policy.
             * @param selfNodeId Stable id of the local node.
             * @throws std::runtime_error if selfNodeId is not present in a
             *         non-standalone config.
             * @throws std::invalid_argument if config is invalid.
             */
            [[nodiscard]] static std::unique_ptr<ClusterManager> create(
                std::filesystem::path dbDir,
                ClusterConfig config,
                uint64_t selfNodeId
            );

            ~ClusterManager();

            ClusterManager(const ClusterManager&) = delete;
            ClusterManager& operator=(const ClusterManager&) = delete;

            /** Installs or replaces the role-change callback. */
            void setRoleChangeCallback(RoleChangeCallback callback);

            /**
             * Starts role election.
             *
             * Standalone configs immediately move to NodeRole::STANDALONE.
             * Cluster configs select the primary deterministically from config.
             */
            void start();

            /** Stops the manager. */
            void close();

            /** Returns the current local role. */
            [[nodiscard]] NodeRole role() const noexcept;

            /** Returns the configured local node id. */
            [[nodiscard]] uint64_t selfNodeId() const noexcept;

            /** Returns the currently known primary host, or empty if unknown. */
            [[nodiscard]] std::string primaryHost() const;

            /** Returns the currently known primary replication port, or 0 if unknown. */
            [[nodiscard]] uint16_t primaryReplPort() const;

            /** Returns true when the cluster config resolves to standalone mode. */
            [[nodiscard]] bool isStandalone() const noexcept;

        private:
            class Impl;
            explicit ClusterManager(std::unique_ptr<Impl> impl);
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::cluster

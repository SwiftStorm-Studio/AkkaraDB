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
     * ClusterManager - Elects and monitors the local cluster role.
     *
     * ClusterManager owns the lightweight primary-election loop.  In cluster
     * mode, coordinator-eligible nodes try to acquire PRIMARY.lock under the
     * database directory.  The holder becomes Primary; other nodes become
     * Replica and monitor the primary replication endpoint for failover.
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
             * @param db_dir       Directory containing PRIMARY.lock.
             * @param config       Valid cluster membership and policy.
             * @param self_node_id Stable id of the local node.
             * @throws std::runtime_error if self_node_id is not present in a
             *         non-standalone config.
             * @throws std::invalid_argument if config is invalid.
             */
            [[nodiscard]] static std::unique_ptr<ClusterManager> create(std::filesystem::path db_dir, ClusterConfig config, uint64_t self_node_id);

            ~ClusterManager();

            ClusterManager(const ClusterManager&) = delete;
            ClusterManager& operator=(const ClusterManager&) = delete;

            /** Installs or replaces the role-change callback. */
            void set_role_change_callback(RoleChangeCallback callback);

            /**
             * Starts role election and primary monitoring.
             *
             * Standalone configs immediately move to NodeRole::Standalone.
             * Cluster configs create db_dir if needed and start the monitor
             * thread after the first election.
             */
            void start();

            /** Stops monitoring, releases PRIMARY.lock if held, and joins workers. */
            void close();

            /** Returns the current local role. */
            [[nodiscard]] NodeRole role() const noexcept;

            /** Returns the configured local node id. */
            [[nodiscard]] uint64_t self_node_id() const noexcept;

            /** Returns the currently known primary host, or empty if unknown. */
            [[nodiscard]] std::string primary_host() const;

            /** Returns the currently known primary replication port, or 0 if unknown. */
            [[nodiscard]] uint16_t primary_repl_port() const;

            /** Returns true when the cluster config resolves to standalone mode. */
            [[nodiscard]] bool is_standalone() const noexcept;

        private:
            class Impl;
            explicit ClusterManager(std::unique_ptr<Impl> impl);
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::cluster

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

// akkengine/include/akk/engine/cluster/ReplicationServer.hpp
#pragma once

#include "akk/engine/cluster/ClusterConfig.hpp"
#include "akk/engine/cluster/ReplFraming.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <span>

namespace akkaradb::engine::cluster {
    /**
     * ReplicationServer - Primary-side replication fan-out server.
     *
     * The server listens on the primary replication port, accepts replica
     * handshakes, sends buffered entries newer than each replica's last_seq,
     * streams new entries/blobs to all live replicas, and waits for entry
     * acknowledgements according to AckPolicy.
     *
     * Thread-safety: start(), close(), ship_entry(), ship_blob(), and
     * replica_count() may be called concurrently.  close() is idempotent.
     */
    class ReplicationServer {
        public:
            /**
             * Maximum number of recent entry frames kept for reconnect catch-up.
             *
             * Blob frames are shipped immediately and are not retained in this
             * entry buffer.
             */
            static constexpr size_t ENTRY_BUFFER_SIZE = 4096;

            /**
             * Creates a primary replication server.
             *
             * @param repl_port       Local replication listener port.
             * @param self_node_id    Primary node id advertised in ServerHello.
             * @param get_current_seq Returns current primary seq for ServerHello.
             * @param ack_policy      Entry acknowledgement policy.
             * @param runtime_options Transport/TLS options.
             */
            [[nodiscard]] static std::unique_ptr<ReplicationServer> create(
                uint16_t repl_port,
                uint64_t self_node_id,
                std::function<uint64_t()> get_current_seq,
                AckPolicy ack_policy,
                ClusterRuntimeOptions runtime_options = {}
            );

            ~ReplicationServer();

            ReplicationServer(const ReplicationServer&) = delete;
            ReplicationServer& operator=(const ReplicationServer&) = delete;

            /**
             * Binds the replication listener and starts the accept worker.
             *
             * @throws std::runtime_error if socket creation, bind, or listen fails.
             */
            void start();

            /** Stops accepting, disconnects replicas, and joins worker threads. */
            void close();

            /**
             * Ships a replicated key/value entry to all live replicas.
             *
             * Depending on AckPolicy, this call may wait for replica acknowledgements
             * before returning.
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
             * Ships a blob payload to all live replicas.
             *
             * Blob frames are sent with an internal buffer seq of zero and are not
             * waited on by the acknowledgement policy.
             */
            void ship_blob(uint64_t seq, uint64_t blob_id, std::span<const uint8_t> content);

            /** Returns the number of currently live replica connections. */
            [[nodiscard]] size_t replica_count() const noexcept;

        private:
            class Impl;
            explicit ReplicationServer(std::unique_ptr<Impl> impl);
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::cluster

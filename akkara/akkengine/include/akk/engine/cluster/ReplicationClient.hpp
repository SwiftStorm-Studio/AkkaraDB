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

// akkengine/include/akk/engine/cluster/ReplicationClient.hpp
#pragma once

#include "akk/engine/cluster/AkkClusterRuntimeExport.hpp"
#include "akk/engine/cluster/ClusterConfig.hpp"
#include "akk/engine/cluster/ReplFraming.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <string>

namespace akkaradb::engine::cluster {
    /**
     * ReplicationClient - Replica-side connection to the current primary.
     *
     * The client reconnects in the background, performs the replication
     * handshake, receives Entry and BlobPut frames, invokes engine callbacks,
     * and acknowledges applied entries back to the primary.
     *
     * Thread-safety: callback setters, start(), close(), and connected() are
     * synchronized internally.  Callbacks are invoked from the client's worker
     * thread.
     */
    class AKKARADB_CLUSTER_RUNTIME_API ReplicationClient {
        public:
            /**
             * Applies a replicated put/remove entry to the local engine.
             *
             * The key and value spans are valid only for the duration of the
             * callback invocation.
             */
            using ApplyCallback = std::function<void(
                uint64_t seq,
                ReplOpType op,
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint8_t recordFlags,
                uint64_t sourceNodeId
            )>;

            /**
             * Applies a replicated blob payload to the local blob store.
             *
             * The content span is valid only for the duration of the callback.
             */
            using BlobCallback = std::function<void(uint64_t seq, uint64_t blobId, std::span<const uint8_t> content)>;

            /**
             * Creates a replica client.
             *
             * @param primaryHost       Hostname or address of the primary.
             * @param primaryReplPort  Primary replication listener port.
             * @param selfNodeId       Local replica node id.
             * @param getLastSeq       Returns the highest local applied seq
             *                           for ClientHello.
             * @param runtimeOptions    Transport options.
             */
            [[nodiscard]] static std::unique_ptr<ReplicationClient> create(
                std::string primaryHost,
                uint16_t primaryReplPort,
                uint64_t selfNodeId,
                std::function<uint64_t()> getLastSeq,
                ClusterRuntimeOptions runtimeOptions = {}
            );

            ~ReplicationClient();

            ReplicationClient(const ReplicationClient&) = delete;
            ReplicationClient& operator=(const ReplicationClient&) = delete;

            /** Sets the callback used for replicated key/value entries. */
            void setApplyCallback(ApplyCallback callback);

            /** Sets the callback used for replicated blob payloads. */
            void setBlobCallback(BlobCallback callback);

            /** Starts the reconnecting receive worker. */
            void start();

            /** Stops the worker, closes the active connection, and joins it. */
            void close();

            /** Returns true while the current primary connection is handshaken. */
            [[nodiscard]] bool connected() const noexcept;

        private:
            class Impl;
            explicit ReplicationClient(std::unique_ptr<Impl> impl);
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::cluster

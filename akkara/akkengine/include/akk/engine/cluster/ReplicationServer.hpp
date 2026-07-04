

// akkengine/include/akk/engine/cluster/ReplicationServer.hpp
#pragma once

#include "akk/engine/cluster/AkkClusterRuntimeExport.hpp"
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
     * handshakes, sends buffered entries newer than each replica's lastSeq,
     * streams new entries/blobs to current replication targets, and waits for entry
     * acknowledgements according to AckPolicy.
     *
     * Thread-safety: start(), close(), shipEntry(), shipBlob(), and
     * replicaCount() may be called concurrently.  close() is idempotent.
     */
    class AKKARADB_CLUSTER_RUNTIME_API ReplicationServer {
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
             * @param replPort       Local replication listener port.
             * @param selfNodeId    Primary node id advertised in ServerHello.
             * @param getCurrentSeq Returns current primary seq for ServerHello.
             * @param ackPolicy      Entry acknowledgement policy.
             * @param runtimeOptions Transport options.
             */
            [[nodiscard]] static std::unique_ptr<ReplicationServer> create(
                uint16_t replPort,
                uint64_t selfNodeId,
                std::function<uint64_t()> getCurrentSeq,
                AckPolicy ackPolicy,
                ClusterRuntimeOptions runtimeOptions = {}
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
             * Ships a replicated key/value entry to all current replication targets.
             *
             * Depending on AckPolicy, this call may wait for replica acknowledgements
             * before returning.
             */
            void shipEntry(
                uint64_t seq,
                ReplOpType op,
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint8_t recordFlags,
                uint64_t sourceNodeId
            );

            /**
             * Ships a blob payload to all current replication targets.
             *
             * Blob frames are sent with an internal buffer seq of zero and are not
             * waited on by the acknowledgement policy.
             */
            void shipBlob(uint64_t seq, uint64_t blobId, std::span<const uint8_t> content);

            /** Returns the number of currently live replica connections. */
            [[nodiscard]] size_t replicaCount() const noexcept;

        private:
            class Impl;
            explicit ReplicationServer(std::unique_ptr<Impl> impl);
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::cluster

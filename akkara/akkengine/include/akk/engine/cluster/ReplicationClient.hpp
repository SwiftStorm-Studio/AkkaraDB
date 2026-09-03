/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/cluster/ReplicationClient.hpp
#pragma once
#include "akk/engine/cluster/AkkClusterRuntimeExport.hpp"
#include "akk/engine/cluster/ClusterConfig.hpp"
#include "akk/engine/cluster/ReplFraming.hpp"
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>

namespace akkaradb::engine::cluster {
    /**
     * ReplicationClient - Replica-side connection to the current primary.
     *
     * The client reconnects in the background, performs the replication
     * handshake, receives Entry and BlobPut frames, invokes engine callbacks,
     * and acknowledges entries back to the primary according to AckPolicy.
     *
     * Thread-safety: callback setters, start(), close(), and connected() are
     * synchronized internally.  Callbacks are invoked from the client's worker
     * thread.
     */
    class AKKARADB_CLUSTER_RUNTIME_API ReplicationClient {
        public:
            using ApplyCallback = std::function<void(
uint64_t seq,
 ReplOpType op,
 std::span<const uint8_t> key,
 std::span<const uint8_t> value,
 uint8_t recordFlags,
 uint64_t sourceNodeId
            )>;
            using BlobBeginCallback = std::function<void(uint64_t seq, uint64_t blobId, uint64_t totalSize, uint32_t contentCrc32c)>;
            using BlobChunkCallback = std::function<void(uint64_t seq, uint64_t blobId, uint64_t offset, std::span<const uint8_t> chunk)>;
            using BlobEndCallback = std::function<void(uint64_t seq, uint64_t blobId)>;
            using SnapshotBeginCallback = std::function<void(uint64_t snapshotSeq, uint64_t entryCount)>;
            using SnapshotEntryBeginCallback = std::function<void(std::span<const uint8_t> key, uint64_t valueSize, uint32_t valueCrc32c)>;
            using SnapshotEntryChunkCallback = std::function<void(uint64_t offset, std::span<const uint8_t> chunk)>;
            using SnapshotEntryEndCallback = std::function<void()>;
            using SnapshotEndCallback = std::function<void(uint64_t snapshotSeq)>;
            [[nodiscard]] static std::unique_ptr<ReplicationClient> create(
                std::string primaryHost,
                uint16_t primaryReplPort,
                uint64_t selfNodeId,
                std::function<uint64_t()> getLastSeq,
                AckPolicy ackPolicy = {},
                ClusterRuntimeOptions runtimeOptions = {}
            );
            ~ReplicationClient();
            ReplicationClient(const ReplicationClient&) = delete;
            ReplicationClient& operator=(const ReplicationClient&) = delete;
            void setApplyCallback(ApplyCallback callback);
            void setBlobCallbacks(BlobBeginCallback begin, BlobChunkCallback chunk, BlobEndCallback end);
            void setForceDurableCallback(std::function<void()> callback);
            void setSnapshotCallbacks(
                SnapshotBeginCallback begin,
                SnapshotEntryBeginCallback beginEntry,
                SnapshotEntryChunkCallback chunk,
                SnapshotEntryEndCallback endEntry,
                SnapshotEndCallback end
            );
            void start();
            void close();
            [[nodiscard]] bool connected() const noexcept;
            [[nodiscard]] ReadResponse readKey(std::span<const uint8_t> key, uint64_t snapshotSeq, uint32_t timeoutMs);

        private:
            class Impl;
            explicit ReplicationClient(std::unique_ptr<Impl> impl);
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::cluster

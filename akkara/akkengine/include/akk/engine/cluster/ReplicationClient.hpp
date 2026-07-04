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
            using BlobCallback = std::function<void(uint64_t seq, uint64_t blobId, std::span<const uint8_t> content)>;
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
            void setBlobCallback(BlobCallback callback);
            void setForceDurableCallback(std::function<void()> callback);
            void start();
            void close();
            [[nodiscard]] bool connected() const noexcept;
        private:
            class Impl;
            explicit ReplicationClient(std::unique_ptr<Impl> impl);
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine::cluster
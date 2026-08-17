/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/cluster/detail/RaftConsensusRuntime.hpp
#pragma once

#include "akk/engine/cluster/ClusterConfig.hpp"
#include "akk/engine/cluster/ClusterRuntimeProvider.hpp"
#include "akk/engine/cluster/ClusterRouter.hpp"
#include "akk/engine/cluster/ReplFraming.hpp"

#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <vector>

namespace akkaradb::engine::cluster {
    class RaftConsensusRuntime {
        public:
            [[nodiscard]] static std::unique_ptr<RaftConsensusRuntime> create(
                std::filesystem::path dbDir,
                ClusterConfig config,
                uint64_t selfNodeId,
                ClusterEngineCallbacks callbacks,
                ClusterRuntimeOptions runtimeOptions
            );

            ~RaftConsensusRuntime();

            RaftConsensusRuntime(const RaftConsensusRuntime&) = delete;
            RaftConsensusRuntime& operator=(const RaftConsensusRuntime&) = delete;

            void start();
            void close();
            [[nodiscard]] NodeRole role() const noexcept;
            [[nodiscard]] std::vector<NodeInfo> activeNodes() const;
            [[nodiscard]] const ClusterRouter& router() const noexcept;

            void shipEntry(
                uint64_t seq,
                ReplOpType op,
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint8_t recordFlags,
                uint64_t sourceNodeId
            );

            void shipBlob(uint64_t seq, uint64_t blobId, std::span<const uint8_t> content);

            void addVotingNode(const NodeInfo& node);

            void removeVotingNode(uint64_t nodeId);

            void transferLeadership(uint64_t targetNodeId);

        private:
            class Impl;
            explicit RaftConsensusRuntime(std::unique_ptr<Impl> impl);
            std::unique_ptr<Impl> impl_;
    };
}

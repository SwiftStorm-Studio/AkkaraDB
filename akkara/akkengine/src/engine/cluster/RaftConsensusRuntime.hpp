#pragma once

#include "akk/engine/cluster/ClusterConfig.hpp"
#include "akk/engine/cluster/ClusterRuntimeProvider.hpp"
#include "akk/engine/cluster/ClusterRouter.hpp"
#include "akk/engine/cluster/ReplFraming.hpp"

#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>

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

        private:
            class Impl;
            explicit RaftConsensusRuntime(std::unique_ptr<Impl> impl);
            std::unique_ptr<Impl> impl_;
    };
}

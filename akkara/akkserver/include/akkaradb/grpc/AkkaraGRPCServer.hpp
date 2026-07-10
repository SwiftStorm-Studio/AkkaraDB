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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

// akkserver/include/akkaradb/grpc/AkkaraGRPCServer.hpp
#pragma once

#include "akkaradb/grpc/Export.hpp"

#include "akk/engine/AkkEngine.hpp"

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>

namespace akkaradb::grpcapi {
    struct AKKARADB_GRPC_API AkkaraGRPCServerOptions {
        std::string bindHost = "127.0.0.1";
        uint16_t port = 7072;
        std::filesystem::path certPath;
        std::filesystem::path keyPath;
        std::filesystem::path rootCertPath;
        bool requireClientCert = false;
        int maxReceiveMessageBytes = 64 * 1024 * 1024;
        int maxSendMessageBytes = 64 * 1024 * 1024;
        uint32_t workerThreads = 0;
        uint32_t completionQueues = 0;
        uint32_t minPollers = 0;
        uint32_t maxPollers = 0;
        uint32_t maxConcurrentStreams = 0;
        uint64_t resourceQuotaBytes = 0;
        uint32_t maxBatchItems = 4096;
        uint32_t maxScanItems = 4096;
        uint32_t maxHistoryEntries = 4096;
    };

    struct AKKARADB_GRPC_API AkkaraGRPCServerStats {
        bool running = false;
        bool tlsEnabled = false;
        uint16_t port = 0;
        uint64_t requestsTotal = 0;
        uint64_t responsesTotal = 0;
        uint64_t activeRequests = 0;
        uint64_t errorsTotal = 0;
        uint64_t batchPutItemsTotal = 0;
        uint64_t batchGetItemsTotal = 0;
        uint32_t workerThreads = 0;
        uint32_t completionQueues = 0;
        uint32_t minPollers = 0;
        uint32_t maxPollers = 0;
        uint32_t maxConcurrentStreams = 0;
        uint64_t resourceQuotaBytes = 0;
        uint32_t maxBatchItems = 0;
        uint32_t maxScanItems = 0;
        uint32_t maxHistoryEntries = 0;
    };

    class AKKARADB_GRPC_API AkkaraGRPCServer {
        public:
            [[nodiscard]] static std::unique_ptr<AkkaraGRPCServer> create(engine::AkkEngine& engine, AkkaraGRPCServerOptions options = {});

            ~AkkaraGRPCServer();

            AkkaraGRPCServer(const AkkaraGRPCServer&) = delete;
            AkkaraGRPCServer& operator=(const AkkaraGRPCServer&) = delete;

            void start();
            void close();
            [[nodiscard]] AkkaraGRPCServerStats stats() const noexcept;

        private:
            class Impl;

            explicit AkkaraGRPCServer(std::unique_ptr<Impl> impl);

            std::unique_ptr<Impl> impl_;
    };
}

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

// akkengine/include/akk/engine/server/TcpApiServer.hpp
#pragma once

#include "akk/engine/AkkEngine.hpp"
#include "akk/engine/server/AkkApiServerExport.hpp"
#include "akk/engine/server/AkkApiTransportProvider.hpp"
#include "akk/engine/server/ApiTransport.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

namespace akkaradb::engine::server {
    class AKKARADB_API_SERVER_API TcpApiServer final : public IAkkApiTransport {
        public:
            [[nodiscard]] static std::unique_ptr<TcpApiServer> create(AkkEngine& engine, AkkEngineOptions::ApiOptions options);

            ~TcpApiServer() override;

            TcpApiServer(const TcpApiServer&) = delete;
            TcpApiServer& operator=(const TcpApiServer&) = delete;

            void start() override;
            void close() override;
            [[nodiscard]] EngineStats::ApiStats stats() const noexcept override;

        private:
            TcpApiServer(AkkEngine& engine, AkkEngineOptions::ApiOptions options);

            void acceptLoop();
            void workerLoop();
            void enqueueClient(detail::SocketHandle client);
            void handleConnection(detail::Connection& connection);
            [[nodiscard]] uint32_t workerCount() const;
            [[nodiscard]] uint32_t acceptQueueLimit() const noexcept;
            [[nodiscard]] uint32_t acceptQueueTimeoutMs() const noexcept;
            [[nodiscard]] uint32_t maxBatchItems() const noexcept;
            [[nodiscard]] AkkEngineOptions::ApiIoBackend resolvedIoBackend() const noexcept;

            using Clock = std::chrono::steady_clock;

            struct PendingClient {
                detail::SocketHandle socket = detail::BAD_SOCKET_VALUE;
                Clock::time_point enqueuedAt{};
            };

            void pruneExpiredPendingLocked(Clock::time_point now, std::vector<detail::SocketHandle>& expired);
            void recordAcceptQueueDepth(size_t depth) noexcept;

            AkkEngine& engine_;
            AkkEngineOptions::ApiOptions options_;
            std::atomic<bool> running_{false};
            detail::SocketHandle listenSocket_{detail::BAD_SOCKET_VALUE};
            std::thread acceptThread_;
            std::vector<std::thread> workerThreads_;
            mutable std::mutex queueMu_;
            std::condition_variable queueCv_;
            std::deque<PendingClient> pendingClients_;
            std::mutex activeMu_;
            std::unordered_set<detail::SocketHandle> activeClients_;
            std::atomic<uint64_t> connectionsAcceptedTotal_{0};
            std::atomic<uint64_t> connectionsClosedTotal_{0};
            std::atomic<uint64_t> connectionsActive_{0};
            std::atomic<uint64_t> acceptQueuePeakDepth_{0};
            std::atomic<uint64_t> acceptQueueRejectedTotal_{0};
            std::atomic<uint64_t> acceptQueueExpiredTotal_{0};
            std::atomic<uint64_t> requestsTotal_{0};
            std::atomic<uint64_t> responsesTotal_{0};
            std::atomic<uint64_t> bytesReceivedTotal_{0};
            std::atomic<uint64_t> bytesSentTotal_{0};
            std::atomic<uint64_t> protocolErrorsTotal_{0};
            std::atomic<uint64_t> crcErrorsTotal_{0};
            std::atomic<uint64_t> pipelineBatchesTotal_{0};
            std::atomic<uint64_t> backpressureFlushesTotal_{0};
            std::atomic<uint64_t> backpressureDisconnectsTotal_{0};
            std::atomic<uint64_t> batchPutItemsTotal_{0};
            std::atomic<uint64_t> batchGetItemsTotal_{0};
    };
}

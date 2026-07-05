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

// akkengine/include/akk/engine/server/HttpApiServer.hpp
#pragma once

#include "akk/engine/AkkEngine.hpp"
#include "akk/engine/server/AkkApiServerExport.hpp"
#include "akk/engine/server/AkkApiTransportProvider.hpp"
#include "akk/engine/server/ApiTransport.hpp"

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <vector>

namespace akkaradb::engine::server {
    class AKKARADB_API_SERVER_API HttpApiServer final : public IAkkApiTransport {
        public:
            [[nodiscard]] static std::unique_ptr<HttpApiServer> create(AkkEngine& engine, AkkEngineOptions::ApiOptions options);

            ~HttpApiServer() override;

            HttpApiServer(const HttpApiServer&) = delete;
            HttpApiServer& operator=(const HttpApiServer&) = delete;

            void start() override;
            void close() override;
            [[nodiscard]] EngineStats::ApiStats stats() const noexcept override;

        private:
            struct ParsedRequest {
                std::string method;
                std::string path;
                std::string query;
                std::vector<uint8_t> body;
                bool keepAlive = true;
            };

            HttpApiServer(AkkEngine& engine, AkkEngineOptions::ApiOptions options);

            void acceptLoop();
            void handleConnection(detail::Connection& connection);
            bool readRequest(detail::Connection& connection, ParsedRequest& request, bool& protocolError);
            bool route(detail::Connection& connection, const ParsedRequest& request, std::vector<uint8_t>& valueBuffer);
            bool sendResponse(detail::Connection& connection, int statusCode, std::span<const uint8_t> body);
            bool sendChunkedResponseHeader(
                detail::Connection& connection,
                int statusCode,
                std::string_view contentType,
                uint64_t& bytesSent
            );
            bool sendChunk(detail::Connection& connection, std::span<const uint8_t> body, uint64_t& bytesSent);
            bool finishChunkedResponse(detail::Connection& connection, int statusCode, uint64_t bytesSent);
            bool sendText(detail::Connection& connection, int statusCode, std::string_view body);
            bool sendEmpty(detail::Connection& connection, int statusCode);

            [[nodiscard]] static std::string queryParam(std::string_view query, std::string_view name);
            [[nodiscard]] static std::vector<uint8_t> urlDecode(std::string_view encoded);
            [[nodiscard]] static bool wantsStreaming(std::string_view value) noexcept;
            [[nodiscard]] uint32_t maxBatchItems() const noexcept;
            [[nodiscard]] uint32_t maxScanItems() const noexcept;
            [[nodiscard]] uint32_t maxHistoryEntries() const noexcept;
            [[nodiscard]] uint64_t maxContentLength() const noexcept;

            AkkEngine& engine_;
            AkkEngineOptions::ApiOptions options_;
            std::atomic<bool> running_{false};
            detail::SocketHandle listenSocket_{detail::BAD_SOCKET_VALUE};
            std::thread acceptThread_;
            mutable std::mutex clientsMu_;
            std::unordered_set<detail::SocketHandle> activeClients_;
            std::vector<std::thread> connectionThreads_;
            std::atomic<uint64_t> connectionsAcceptedTotal_{0};
            std::atomic<uint64_t> connectionsClosedTotal_{0};
            std::atomic<uint64_t> requestsTotal_{0};
            std::atomic<uint64_t> responsesTotal_{0};
            std::atomic<uint64_t> bytesReceivedTotal_{0};
            std::atomic<uint64_t> bytesSentTotal_{0};
            std::atomic<uint64_t> protocolErrorsTotal_{0};
            std::atomic<uint64_t> errorsTotal_{0};
            std::atomic<uint64_t> batchPutItemsTotal_{0};
            std::atomic<uint64_t> batchGetItemsTotal_{0};
    };
}

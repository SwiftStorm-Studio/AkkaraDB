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

#pragma once

#include "akk/engine/AkkEngine.hpp"
#include "akk/engine/server/ApiTransport.hpp"

#include <atomic>
#include <cstdint>

namespace akkaradb::engine::server::tcp {
    struct ConnectionCounters {
        std::atomic<uint64_t>& requestsTotal;
        std::atomic<uint64_t>& responsesTotal;
        std::atomic<uint64_t>& bytesReceivedTotal;
        std::atomic<uint64_t>& bytesSentTotal;
        std::atomic<uint64_t>& protocolErrorsTotal;
        std::atomic<uint64_t>& crcErrorsTotal;
        std::atomic<uint64_t>& pipelineBatchesTotal;
        std::atomic<uint64_t>& backpressureFlushesTotal;
        std::atomic<uint64_t>& backpressureDisconnectsTotal;
        std::atomic<uint64_t>& batchPutItemsTotal;
        std::atomic<uint64_t>& batchGetItemsTotal;
    };

    class ConnectionHandler {
        public:
            ConnectionHandler(AkkEngine& engine, const AkkEngineOptions::ApiOptions& options, ConnectionCounters counters);

            void handle(detail::Connection& connection, const std::atomic<bool>& running);

        private:
            [[nodiscard]] uint32_t maxBatchItems() const noexcept;

            AkkEngine& engine_;
            const AkkEngineOptions::ApiOptions& options_;
            ConnectionCounters counters_;
    };
}

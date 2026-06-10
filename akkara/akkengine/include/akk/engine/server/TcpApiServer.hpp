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
    class TcpApiServer {
        public:
            [[nodiscard]] static std::unique_ptr<TcpApiServer> create(AkkEngine& engine, AkkEngineOptions::ApiOptions options);

            ~TcpApiServer();

            TcpApiServer(const TcpApiServer&) = delete;
            TcpApiServer& operator=(const TcpApiServer&) = delete;

            void start();
            void close();
            [[nodiscard]] EngineStats::ApiStats stats() const noexcept;

        private:
            TcpApiServer(AkkEngine& engine, AkkEngineOptions::ApiOptions options);

            void accept_loop();
            void worker_loop();
            void enqueue_client(detail::socket_t client);
            void handle_connection(detail::Connection& connection);
            [[nodiscard]] uint32_t worker_count() const;
            [[nodiscard]] uint32_t accept_queue_limit() const noexcept;
            [[nodiscard]] uint32_t accept_queue_timeout_ms() const noexcept;
            [[nodiscard]] uint32_t max_batch_items() const noexcept;
            [[nodiscard]] AkkEngineOptions::ApiIoBackend resolved_io_backend() const noexcept;

            using Clock = std::chrono::steady_clock;
            struct PendingClient {
                detail::socket_t socket = detail::BAD_SOCKET_VALUE;
                Clock::time_point enqueued_at{};
            };

            void prune_expired_pending_locked(Clock::time_point now, std::vector<detail::socket_t>& expired);
            void record_accept_queue_depth(size_t depth) noexcept;

            AkkEngine& engine_;
            AkkEngineOptions::ApiOptions options_;
            std::atomic<bool> running_{false};
            detail::socket_t listen_socket_{detail::BAD_SOCKET_VALUE};
            std::thread accept_thread_;
            std::vector<std::thread> worker_threads_;
            mutable std::mutex queue_mu_;
            std::condition_variable queue_cv_;
            std::deque<PendingClient> pending_clients_;
            std::mutex active_mu_;
            std::unordered_set<detail::socket_t> active_clients_;
            std::atomic<uint64_t> connections_accepted_total_{0};
            std::atomic<uint64_t> connections_closed_total_{0};
            std::atomic<uint64_t> connections_active_{0};
            std::atomic<uint64_t> accept_queue_peak_depth_{0};
            std::atomic<uint64_t> accept_queue_rejected_total_{0};
            std::atomic<uint64_t> accept_queue_expired_total_{0};
            std::atomic<uint64_t> requests_total_{0};
            std::atomic<uint64_t> responses_total_{0};
            std::atomic<uint64_t> bytes_received_total_{0};
            std::atomic<uint64_t> bytes_sent_total_{0};
            std::atomic<uint64_t> protocol_errors_total_{0};
            std::atomic<uint64_t> crc_errors_total_{0};
            std::atomic<uint64_t> pipeline_batches_total_{0};
            std::atomic<uint64_t> backpressure_flushes_total_{0};
            std::atomic<uint64_t> backpressure_disconnects_total_{0};
            std::atomic<uint64_t> batch_put_items_total_{0};
            std::atomic<uint64_t> batch_get_items_total_{0};
    };
}

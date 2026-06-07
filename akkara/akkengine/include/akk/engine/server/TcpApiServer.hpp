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

        private:
            TcpApiServer(AkkEngine& engine, AkkEngineOptions::ApiOptions options);

            void accept_loop();
            void worker_loop();
            void enqueue_client(detail::socket_t client);
            void handle_connection(detail::Connection& connection);
            [[nodiscard]] uint32_t worker_count() const;

            AkkEngine& engine_;
            AkkEngineOptions::ApiOptions options_;
            std::atomic<bool> running_{false};
            detail::socket_t listen_socket_{detail::BAD_SOCKET_VALUE};
            std::thread accept_thread_;
            std::vector<std::thread> worker_threads_;
            std::mutex queue_mu_;
            std::condition_variable queue_cv_;
            std::deque<detail::socket_t> pending_clients_;
            std::mutex active_mu_;
            std::unordered_set<detail::socket_t> active_clients_;
    };
}

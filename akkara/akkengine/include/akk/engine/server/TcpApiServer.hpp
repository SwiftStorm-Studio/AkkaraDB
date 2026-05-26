/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 */

// akkengine/include/akk/engine/server/TcpApiServer.hpp
#pragma once

#include "akk/engine/AkkEngine.hpp"
#include "akk/engine/server/ApiTransport.hpp"

#include <atomic>
#include <memory>
#include <string>
#include <thread>

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
            void handle_connection(detail::Connection& connection);

            AkkEngine& engine_;
            AkkEngineOptions::ApiOptions options_;
            std::atomic<bool> running_{false};
            detail::socket_t listen_socket_{detail::BAD_SOCKET_VALUE};
            std::thread accept_thread_;
    };
}

/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 */

// akkengine/include/akk/engine/server/HttpApiServer.hpp
#pragma once

#include "akk/engine/AkkEngine.hpp"
#include "akk/engine/server/ApiTransport.hpp"

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace akkaradb::engine::server {
    class HttpApiServer {
        public:
            [[nodiscard]] static std::unique_ptr<HttpApiServer> create(AkkEngine& engine, AkkEngineOptions::ApiOptions options);

            ~HttpApiServer();

            HttpApiServer(const HttpApiServer&) = delete;
            HttpApiServer& operator=(const HttpApiServer&) = delete;

            void start();
            void close();

        private:
            struct ParsedRequest {
                std::string method;
                std::string path;
                std::string query;
                std::vector<uint8_t> body;
                bool keep_alive = true;
            };

            HttpApiServer(AkkEngine& engine, AkkEngineOptions::ApiOptions options);

            void accept_loop();
            void handle_connection(detail::Connection& connection);
            bool read_request(detail::Connection& connection, ParsedRequest& request);
            bool route(detail::Connection& connection, const ParsedRequest& request, std::vector<uint8_t>& value_buffer);
            bool send_response(detail::Connection& connection, int status_code, std::span<const uint8_t> body);
            bool send_empty(detail::Connection& connection, int status_code);

            [[nodiscard]] static std::string query_param(std::string_view query, std::string_view name);
            [[nodiscard]] static std::vector<uint8_t> url_decode(std::string_view encoded);

            AkkEngine& engine_;
            AkkEngineOptions::ApiOptions options_;
            std::atomic<bool> running_{false};
            detail::socket_t listen_socket_{detail::BAD_SOCKET_VALUE};
            std::thread accept_thread_;
    };
}

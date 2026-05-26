/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 */

// akkengine/include/akk/engine/server/AkkApiServer.hpp
#pragma once

#include "akk/engine/AkkEngine.hpp"

#include <memory>

namespace akkaradb::engine::server {
    class HttpApiServer;
    class TcpApiServer;

    class AkkApiServer {
        public:
            [[nodiscard]] static std::unique_ptr<AkkApiServer> create(AkkEngine& engine, const AkkEngineOptions::ApiOptions& options);

            ~AkkApiServer();

            AkkApiServer(const AkkApiServer&) = delete;
            AkkApiServer& operator=(const AkkApiServer&) = delete;

            void start();
            void close();

        private:
            AkkApiServer() = default;

            std::unique_ptr<HttpApiServer> http_;
            std::unique_ptr<TcpApiServer> tcp_;
    };
}

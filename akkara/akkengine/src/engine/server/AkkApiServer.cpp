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

// akkengine/src/engine/server/AkkApiServer.cpp
#include "akk/engine/server/AkkApiServer.hpp"

#include "akk/engine/server/HttpApiServer.hpp"
#include "akk/engine/server/TcpApiServer.hpp"

namespace akkaradb::engine::server {
    std::unique_ptr<AkkApiServer> AkkApiServer::create(AkkEngine& engine, const AkkEngineOptions::ApiOptions& options) {
        auto server = std::unique_ptr<AkkApiServer>{new AkkApiServer()};
        auto backends = options.backends;
        if (backends.empty()) {
            backends.push_back(AkkEngineOptions::ApiBackend::Http);
            backends.push_back(AkkEngineOptions::ApiBackend::Tcp);
        }

        for (const auto backend : backends) {
            switch (backend) {
                case AkkEngineOptions::ApiBackend::Http: server->http_ = HttpApiServer::create(engine, options);
                    break;
                case AkkEngineOptions::ApiBackend::Tcp: server->tcp_ = TcpApiServer::create(engine, options);
                    break;
            }
        }
        return server;
    }

    AkkApiServer::~AkkApiServer() { close(); }

    void AkkApiServer::start() {
        try {
            if (http_) { http_->start(); }
            if (tcp_) { tcp_->start(); }
        }
        catch (...) {
            close();
            throw;
        }
    }

    void AkkApiServer::close() {
        if (http_) {
            http_->close();
            http_.reset();
        }
        if (tcp_) {
            tcp_->close();
            tcp_.reset();
        }
    }

    EngineStats::ApiStats AkkApiServer::stats() const noexcept {
        EngineStats::ApiStats out;
        out.enabled = true;
        if (tcp_) { out = tcp_->stats(); }
        out.enabled = true;
        return out;
    }
}

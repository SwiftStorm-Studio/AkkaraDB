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

// akkengine/include/akk/engine/server/AkkApiServer.hpp
#pragma once

#include "akk/engine/AkkEngine.hpp"
#include "akk/engine/server/AkkApiServerExport.hpp"
#include "akk/engine/server/AkkApiServerProvider.hpp"
#include "akk/engine/server/AkkApiTransportProvider.hpp"

#include <memory>
#include <vector>

namespace akkaradb::engine::server {
    class AKKARADB_API_SERVER_API AkkApiServer final : public IAkkApiServer {
        public:
            [[nodiscard]] static std::unique_ptr<AkkApiServer> create(AkkEngine& engine, const AkkEngineOptions::ApiOptions& options);

            ~AkkApiServer() override;

            AkkApiServer(const AkkApiServer&) = delete;
            AkkApiServer& operator=(const AkkApiServer&) = delete;

            void start() override;
            void close() override;
            [[nodiscard]] EngineStats::ApiStats stats() const noexcept override;

        private:
            AkkApiServer();

            std::vector<std::unique_ptr<IAkkApiTransport>> transports_;
    };
}

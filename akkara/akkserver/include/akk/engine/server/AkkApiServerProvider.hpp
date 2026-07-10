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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

// akkserver/include/akk/engine/server/AkkApiServerProvider.hpp
#pragma once

#include "akkaradb/Export.hpp"

#include "akk/engine/AkkEngine.hpp"

#include <filesystem>
#include <memory>
#include <string>

namespace akkaradb::engine::server {
    class AKDB_API IAkkApiServer {
        public:
            virtual ~IAkkApiServer() = default;

            IAkkApiServer(const IAkkApiServer&) = delete;
            IAkkApiServer& operator=(const IAkkApiServer&) = delete;

            virtual void start() = 0;
            virtual void close() = 0;
            [[nodiscard]] virtual EngineStats::ApiStats stats() const noexcept = 0;

        protected:
            IAkkApiServer() = default;
    };

    using AkkApiServerFactory = std::unique_ptr<IAkkApiServer> (*)(AkkEngine&, const AkkEngineOptions::ApiOptions&);

    AKDB_API bool registerAkkApiServerFactory(AkkApiServerFactory factory) noexcept;
    [[nodiscard]] AKDB_API bool akkApiServerFactoryAvailable() noexcept;
    [[nodiscard]] AKDB_API bool loadAkkApiServerBackend(const std::filesystem::path& libraryPath = {});
    [[nodiscard]] AKDB_API std::string lastAkkApiServerBackendLoadError();
    [[nodiscard]] AKDB_API std::unique_ptr<IAkkApiServer> createAkkApiServer(
        AkkEngine& engine,
        const AkkEngineOptions::ApiOptions& options
    );
}

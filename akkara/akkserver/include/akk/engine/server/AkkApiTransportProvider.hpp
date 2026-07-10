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

// akkserver/include/akk/engine/server/AkkApiTransportProvider.hpp
#pragma once

#include "akkaradb/Export.hpp"

#include "akk/engine/AkkEngine.hpp"

#include <filesystem>
#include <memory>
#include <string>

namespace akkaradb::engine::server {
    class AKDB_API IAkkApiTransport {
        public:
            virtual ~IAkkApiTransport() = default;

            IAkkApiTransport(const IAkkApiTransport&) = delete;
            IAkkApiTransport& operator=(const IAkkApiTransport&) = delete;

            virtual void start() = 0;
            virtual void close() = 0;
            [[nodiscard]] virtual EngineStats::ApiStats stats() const noexcept = 0;

        protected:
            IAkkApiTransport() = default;
    };

    using AkkApiTransportFactory = std::unique_ptr<IAkkApiTransport> (*)(AkkEngine&, const AkkEngineOptions::ApiOptions&);

    AKDB_API bool registerAkkApiTransportFactory(AkkEngineOptions::ApiBackend backend, AkkApiTransportFactory factory) noexcept;
    [[nodiscard]] AKDB_API bool akkApiTransportFactoryAvailable(AkkEngineOptions::ApiBackend backend) noexcept;
    [[nodiscard]] AKDB_API bool loadAkkApiTransportBackend(
        AkkEngineOptions::ApiBackend backend,
        const std::filesystem::path& libraryPath = {}
    );
    [[nodiscard]] AKDB_API std::string lastAkkApiTransportBackendLoadError(AkkEngineOptions::ApiBackend backend);
    [[nodiscard]] AKDB_API std::unique_ptr<IAkkApiTransport> createAkkApiTransport(
        AkkEngineOptions::ApiBackend backend,
        AkkEngine& engine,
        const AkkEngineOptions::ApiOptions& options
    );
}

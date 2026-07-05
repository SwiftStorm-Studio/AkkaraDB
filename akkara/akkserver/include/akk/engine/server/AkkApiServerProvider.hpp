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

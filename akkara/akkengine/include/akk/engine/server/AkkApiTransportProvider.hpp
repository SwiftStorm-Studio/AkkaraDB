#pragma once

#include "akkaradb/Export.hpp"

#include "akk/engine/AkkEngine.hpp"

#include <filesystem>
#include <memory>

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
    [[nodiscard]] AKDB_API std::unique_ptr<IAkkApiTransport> createAkkApiTransport(
        AkkEngineOptions::ApiBackend backend,
        AkkEngine& engine,
        const AkkEngineOptions::ApiOptions& options
    );
}

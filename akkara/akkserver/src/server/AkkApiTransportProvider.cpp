#include "akk/engine/server/AkkApiTransportProvider.hpp"

#include "akk/core/utils/DynamicLibrary.hpp"

#include <array>
#include <atomic>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

namespace akkaradb::engine::server {
    namespace {
        using RegisterPluginFn = bool (*)() noexcept;

        constexpr size_t kBackendCount = 3;
        std::array<std::atomic<AkkApiTransportFactory>, kBackendCount> g_factories{};
        std::mutex g_loadMutex;
        std::array<std::string, kBackendCount> g_lastLoadErrors{};
        std::vector<core::DynamicLibrary> g_loadedModules;

        [[nodiscard]] size_t backendIndex(AkkEngineOptions::ApiBackend backend) {
            switch (backend) {
                case AkkEngineOptions::ApiBackend::HTTP: return 0;
                case AkkEngineOptions::ApiBackend::TCP: return 1;
                case AkkEngineOptions::ApiBackend::GRPC: return 2;
            }
            throw std::invalid_argument("AkkApiTransportProvider: unknown API backend");
        }

        [[nodiscard]] const char* backendRegisterSymbol(AkkEngineOptions::ApiBackend backend) {
            switch (backend) {
                case AkkEngineOptions::ApiBackend::HTTP: return "akkaradb_api_http_register";
                case AkkEngineOptions::ApiBackend::TCP: return "akkaradb_api_tcp_register";
                case AkkEngineOptions::ApiBackend::GRPC: return "akkaradb_api_grpc_register";
            }
            return "";
        }

        [[nodiscard]] std::filesystem::path backendFilename(AkkEngineOptions::ApiBackend backend) {
            switch (backend) {
                case AkkEngineOptions::ApiBackend::HTTP:
                    #ifdef AKKARADB_API_HTTP_LIBRARY_FILENAME
                    return std::filesystem::path{AKKARADB_API_HTTP_LIBRARY_FILENAME};
                    #elif defined(_WIN32)
                    return "akkaradb-api-http.dll";
                    #elif defined(__APPLE__)
                    return "libakkaradb-api-http.dylib";
                    #else
                    return "libakkaradb-api-http.so";
                    #endif
                case AkkEngineOptions::ApiBackend::TCP:
                    #ifdef AKKARADB_API_TCP_LIBRARY_FILENAME
                    return std::filesystem::path{AKKARADB_API_TCP_LIBRARY_FILENAME};
                    #elif defined(_WIN32)
                    return "akkaradb-api-tcp.dll";
                    #elif defined(__APPLE__)
                    return "libakkaradb-api-tcp.dylib";
                    #else
                    return "libakkaradb-api-tcp.so";
                    #endif
                case AkkEngineOptions::ApiBackend::GRPC:
                    #ifdef AKKARADB_API_GRPC_LIBRARY_FILENAME
                    return std::filesystem::path{AKKARADB_API_GRPC_LIBRARY_FILENAME};
                    #elif defined(_WIN32)
                    return "akkaradb-api-grpc.dll";
                    #elif defined(__APPLE__)
                    return "libakkaradb-api-grpc.dylib";
                    #else
                    return "libakkaradb-api-grpc.so";
                    #endif
            }
            return {};
        }

        [[nodiscard]] std::filesystem::path currentModuleDirectory() {
            #ifdef _WIN32
            HMODULE module = nullptr; const auto flags = GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS |
                GET_MODULE_HANDLE_EX_FLAG_UNCHANGED_REFCOUNT; if (!::GetModuleHandleExW(
                flags,
                reinterpret_cast<LPCWSTR>(&currentModuleDirectory),
                &module
            )) { return {}; } std::wstring buffer(MAX_PATH, L'\0'); for (;;) {
                const DWORD len = ::GetModuleFileNameW(module, buffer.data(), static_cast<DWORD>(buffer.size()));
                if (len == 0) { return {}; }
                if (len < buffer.size() - 1) {
                    buffer.resize(len);
                    return std::filesystem::path{buffer}.parent_path();
                }
                buffer.resize(buffer.size() * 2);
            }
            #else
            Dl_info info{};
            if (::dladdr(reinterpret_cast<void*>(&currentModuleDirectory), &info) == 0 || info.dli_fname == nullptr) { return {}; }
            return std::filesystem::path{info.dli_fname}.parent_path();
            #endif
        }

        [[nodiscard]] std::filesystem::path defaultBackendPath(AkkEngineOptions::ApiBackend backend) {
            const auto filename = backendFilename(backend);
            const auto dir = currentModuleDirectory();
            return dir.empty() ? filename : dir / filename;
        }

        [[nodiscard]] std::filesystem::path resolveBackendPath(AkkEngineOptions::ApiBackend backend, const std::filesystem::path& path) {
            if (path.empty()) { return defaultBackendPath(backend); }
            if (std::filesystem::is_directory(path)) { return path / backendFilename(backend); }
            return path;
        }
    }

    bool registerAkkApiTransportFactory(AkkEngineOptions::ApiBackend backend, AkkApiTransportFactory factory) noexcept {
        if (factory == nullptr) { return false; }
        try {
            g_factories[backendIndex(backend)].store(factory, std::memory_order_release);
            return true;
        }
        catch (...) { return false; }
    }

    bool akkApiTransportFactoryAvailable(AkkEngineOptions::ApiBackend backend) noexcept {
        try { return g_factories[backendIndex(backend)].load(std::memory_order_acquire) != nullptr; }
        catch (...) { return false; }
    }

    bool loadAkkApiTransportBackend(AkkEngineOptions::ApiBackend backend, const std::filesystem::path& libraryPath) {
        std::lock_guard loadLock{g_loadMutex};
        if (akkApiTransportFactoryAvailable(backend)) { return true; }

        const auto idx = backendIndex(backend);
        const auto path = resolveBackendPath(backend, libraryPath);
        std::string error;
        auto module = core::DynamicLibrary::open(path, error);
        if (!module.valid()) {
            g_lastLoadErrors[idx] = std::move(error);
            return false;
        }

        const char* registerSymbol = backendRegisterSymbol(backend);
        auto* symbol = module.symbol(registerSymbol, error);
        if (symbol == nullptr) {
            g_lastLoadErrors[idx] = std::move(error);
            return false;
        }

        const auto registerPlugin = reinterpret_cast<RegisterPluginFn>(symbol);
        if (!registerPlugin()) {
            g_lastLoadErrors[idx] = std::string{"API transport backend "} + path.string() + " returned false from " + registerSymbol;
            return false;
        }
        if (!akkApiTransportFactoryAvailable(backend)) {
            g_lastLoadErrors[idx] = std::string{"API transport backend "} + path.string() + " did not register a factory";
            return false;
        }

        g_loadedModules.push_back(std::move(module));
        g_lastLoadErrors[idx].clear();
        return true;
    }

    std::string lastAkkApiTransportBackendLoadError(AkkEngineOptions::ApiBackend backend) {
        std::lock_guard lock{g_loadMutex};
        return g_lastLoadErrors[backendIndex(backend)];
    }

    std::unique_ptr<IAkkApiTransport> createAkkApiTransport(
        AkkEngineOptions::ApiBackend backend,
        AkkEngine& engine,
        const AkkEngineOptions::ApiOptions& options
    ) {
        const auto factory = g_factories[backendIndex(backend)].load(std::memory_order_acquire);
        if (factory == nullptr) { throw std::runtime_error("AkkApiServer: API transport backend library is not loaded"); }
        return factory(engine, options);
    }
}

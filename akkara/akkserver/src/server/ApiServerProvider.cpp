#include "akk/engine/server/AkkApiServerProvider.hpp"

#include "akk/core/utils/DynamicLibrary.hpp"

#include <atomic>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

namespace akkaradb::engine::server {
    namespace {
        using RegisterPluginFn = bool (*)() noexcept;

        std::atomic<AkkApiServerFactory> g_factory{nullptr};
        std::mutex g_loadMutex;
        std::string g_lastLoadError;
        std::vector<core::DynamicLibrary> g_loadedModules;

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

        [[nodiscard]] std::filesystem::path defaultBackendPath() {
            #ifdef AKKARADB_API_SERVER_LIBRARY_FILENAME
            const std::filesystem::path filename{AKKARADB_API_SERVER_LIBRARY_FILENAME};
            #else
            #ifdef _WIN32
            const std::filesystem::path filename{"akkaradb-api.dll"};
            #elif defined(__APPLE__)
            const std::filesystem::path filename{"libakkaradb-api.dylib"};
            #else
            const std::filesystem::path filename{"libakkaradb-api.so"};
            #endif
            #endif

            const auto dir = currentModuleDirectory();
            return dir.empty() ? filename : dir / filename;
        }

        [[nodiscard]] std::filesystem::path resolveBackendPath(const std::filesystem::path& path) {
            if (path.empty()) { return defaultBackendPath(); }
            if (std::filesystem::is_directory(path)) {
                #ifdef AKKARADB_API_SERVER_LIBRARY_FILENAME
                return path / std::filesystem::path{AKKARADB_API_SERVER_LIBRARY_FILENAME};
                #elif defined(_WIN32)
                return path / "akkaradb-api.dll";
                #elif defined(__APPLE__)
                return path / "libakkaradb-api.dylib";
                #else
                return path / "libakkaradb-api.so";
                #endif
            }
            return path;
        }
    }

    bool registerAkkApiServerFactory(AkkApiServerFactory factory) noexcept {
        if (factory == nullptr) { return false; }
        g_factory.store(factory, std::memory_order_release);
        return true;
    }

    bool akkApiServerFactoryAvailable() noexcept { return g_factory.load(std::memory_order_acquire) != nullptr; }

    bool loadAkkApiServerBackend(const std::filesystem::path& libraryPath) {
        std::lock_guard loadLock{g_loadMutex};
        if (akkApiServerFactoryAvailable()) { return true; }

        const auto path = resolveBackendPath(libraryPath);
        std::string error;
        auto module = core::DynamicLibrary::open(path, error);
        if (!module.valid()) {
            g_lastLoadError = std::move(error);
            return false;
        }

        constexpr const char* kRegisterSymbol = "akkaradb_api_server_register";
        auto* symbol = module.symbol(kRegisterSymbol, error);
        if (symbol == nullptr) {
            g_lastLoadError = std::move(error);
            return false;
        }

        const auto registerPlugin = reinterpret_cast<RegisterPluginFn>(symbol);
        if (!registerPlugin()) {
            g_lastLoadError = "API server backend " + path.string() + " returned false from " + kRegisterSymbol;
            return false;
        }
        if (!akkApiServerFactoryAvailable()) {
            g_lastLoadError = "API server backend " + path.string() + " did not register a factory";
            return false;
        }

        g_loadedModules.push_back(std::move(module));
        g_lastLoadError.clear();
        return true;
    }

    std::string lastAkkApiServerBackendLoadError() {
        std::lock_guard lock{g_loadMutex};
        return g_lastLoadError;
    }

    std::unique_ptr<IAkkApiServer> createAkkApiServer(AkkEngine& engine, const AkkEngineOptions::ApiOptions& options) {
        const auto factory = g_factory.load(std::memory_order_acquire);
        if (factory == nullptr) { throw std::runtime_error("AkkEngine: API server backend library is not loaded"); }
        return factory(engine, options);
    }
}

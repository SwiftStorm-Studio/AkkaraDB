/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#pragma once

#include <filesystem>
#include <string>
#include <utility>

#ifdef _WIN32
#include <windows.h>
#else
#include <dlfcn.h>
#endif

namespace akkaradb::core {
    class DynamicLibrary {
        public:
            DynamicLibrary() noexcept = default;

            DynamicLibrary(const DynamicLibrary&) = delete;
            DynamicLibrary& operator=(const DynamicLibrary&) = delete;

            DynamicLibrary(DynamicLibrary&& other) noexcept : handle_{std::exchange(other.handle_, nullptr)} {}

            DynamicLibrary& operator=(DynamicLibrary&& other) noexcept {
                if (this != &other) {
                    close();
                    handle_ = std::exchange(other.handle_, nullptr);
                }
                return *this;
            }

            ~DynamicLibrary() { close(); }

            [[nodiscard]] static DynamicLibrary open(const std::filesystem::path& path, std::string& error) {
                error.clear();
                #ifdef _WIN32
                HMODULE handle = ::LoadLibraryW(path.wstring().c_str()); if (handle == nullptr) {
                    error = "LoadLibraryW failed for " + path.string() + ": " + lastNativeError();
                    return {};
                } return DynamicLibrary{handle};
                #else
                ::dlerror();
                void* handle = ::dlopen(path.string().c_str(), RTLD_NOW | RTLD_LOCAL);
                if (handle == nullptr) {
                    const char* msg = ::dlerror();
                    error = "dlopen failed for " + path.string() + ": " + (msg != nullptr ? msg : "unknown error");
                    return {};
                }
                return DynamicLibrary{handle};
                #endif
            }

            [[nodiscard]] bool valid() const noexcept { return handle_ != nullptr; }

            [[nodiscard]] void* symbol(const char* name, std::string& error) const {
                error.clear();
                if (handle_ == nullptr) {
                    error = "dynamic library is not loaded";
                    return nullptr;
                }

                #ifdef _WIN32
                FARPROC proc = ::GetProcAddress(handle_, name); if (proc == nullptr) {
                    error = "GetProcAddress failed for " + std::string{name} + ": " + lastNativeError();
                    return nullptr;
                } return reinterpret_cast<void*>(proc);
                #else
                ::dlerror();
                void* proc = ::dlsym(handle_, name);
                const char* msg = ::dlerror();
                if (msg != nullptr) {
                    error = "dlsym failed for " + std::string{name} + ": " + msg;
                    return nullptr;
                }
                return proc;
                #endif
            }

        private:
            #ifdef _WIN32
            using Handle = HMODULE;
            #else
            using Handle = void*;
            #endif

            explicit DynamicLibrary(Handle handle) noexcept : handle_{handle} {}

            static std::string lastNativeError() {
                #ifdef _WIN32
                const DWORD code = ::GetLastError(); if (code == 0) { return "unknown error"; } char* buffer = nullptr; const DWORD len =
                    ::FormatMessageA(
                        FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
                        nullptr,
                        code,
                        MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
                        reinterpret_cast<LPSTR>(&buffer),
                        0,
                        nullptr
                    ); if (len == 0 || buffer == nullptr) { return "Windows error " + std::to_string(code); } std::string message
                    {buffer, len}; ::LocalFree(buffer); while (!message.empty() && (message.back() == '\n' || message.back() == '\r' ||
                    message.back() == ' ')) { message.pop_back(); } return message + " (" + std::to_string(code) + ")";
                #else
                return "unknown error";
                #endif
            }

            void close() noexcept {
                if (handle_ == nullptr) { return; }
                #ifdef _WIN32
                ::FreeLibrary(handle_);
                #else
                ::dlclose(handle_);
                #endif
                handle_ = nullptr;
            }

            Handle handle_ = nullptr;
    };
} // namespace akkaradb::core

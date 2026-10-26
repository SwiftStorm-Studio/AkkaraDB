/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
#include "akk/engine/cluster/detail/ClusterConnection.hpp"
#include <algorithm>
#include <cerrno>
#include <condition_variable>
#include <cstring>
#include <map>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>

#include <ranges>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <dlfcn.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace akkaradb::engine::cluster::detail {
    namespace {
        constexpr auto CANCEL_POLL = std::chrono::milliseconds{20};
        constexpr size_t MAX_RESOLVED_ADDRESSES = 64;

        bool keepResolverModuleResident() {
            // Lookup jobs may outlive all runtimes. Retain their code and its
            // dependencies as well as their owned state until process exit.
            static const auto retained = [] {
                #ifdef _WIN32
                HMODULE module = nullptr;
                return GetModuleHandleExW(
                    GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS | GET_MODULE_HANDLE_EX_FLAG_PIN,
                    reinterpret_cast<LPCWSTR>(&keepResolverModuleResident),
                    &module
                ) != 0;
                #else
                Dl_info module{}; if (::dladdr(reinterpret_cast<void*>(&keepResolverModuleResident), &module) == 0 || !module.dli_fname) { return false; }
                return ::dlopen(module.dli_fname, RTLD_NOW | RTLD_LOCAL) != nullptr;
                #endif
            }();
            return retained;
        }

        void socketRuntime() {
            #ifdef _WIN32
            static std::once_flag once;
            std::call_once(
                once,
                [] {
                    WSADATA data{};
                    if (WSAStartup(MAKEWORD(2, 2), &data) != 0) { throw std::runtime_error("Cluster connection: WSAStartup failed"); }
                }
            );
            #endif
        }

        DeadlineResolver::Addresses lookupAddresses(const std::string& host, uint16_t port, bool numeric) {
            socketRuntime();
            addrinfo hints{};
            hints.ai_family = AF_UNSPEC;
            hints.ai_socktype = SOCK_STREAM;
            hints.ai_flags = AI_NUMERICSERV | (numeric ? AI_NUMERICHOST : 0);
            addrinfo* result = nullptr;
            if (getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &result) != 0) { return {}; }
            const auto freeResult = [](addrinfo* value) { freeaddrinfo(value); };
            std::unique_ptr<addrinfo, decltype(freeResult)> owned{result, freeResult};
            DeadlineResolver::Addresses addresses;
            for (auto* it = result; it && addresses.size() < MAX_RESOLVED_ADDRESSES; it = it->ai_next) {
                ConnectionAddress address;
                static_assert(sizeof(sockaddr_storage) <= sizeof(address.bytes));
                if (!it->ai_addr || it->ai_addrlen > address.bytes.size() || it->ai_addrlen == 0) { continue; }
                std::memcpy(address.bytes.data(), it->ai_addr, it->ai_addrlen);
                address.length = static_cast<int>(it->ai_addrlen);
                address.family = it->ai_family;
                address.socketType = it->ai_socktype;
                address.protocol = it->ai_protocol;
                addresses.push_back(address);
            }
            return addresses;
        }

        void closeConnection(ConnectionSocket socket) noexcept {
            #ifdef _WIN32
            closesocket(socket);
            #else
            ::close(socket);
            #endif
        }

        bool blocking(ConnectionSocket socket, bool enabled) {
            #ifdef _WIN32
            u_long mode = enabled ? 0u : 1u;
            return ioctlsocket(socket, FIONBIO, &mode) == 0;
            #else
            const auto flags = ::fcntl(socket, F_GETFL, 0); return flags >= 0 && ::fcntl(
                socket,
                F_SETFL,
                enabled ? flags & ~O_NONBLOCK : flags | O_NONBLOCK
            ) == 0;
            #endif
        }

        struct PendingConnection {
            ConnectionSocket socket = INVALID_CONNECTION_SOCKET;
            bool ready = false;
            ~PendingConnection() { reset(); }

            void reset() {
                if (socket != INVALID_CONNECTION_SOCKET) { closeConnection(socket); }
                socket = INVALID_CONNECTION_SOCKET;
                ready = false;
            }
        };

        void startConnection(PendingConnection& pending, const ConnectionAddress& address) {
            if (address.length <= 0 || static_cast<size_t>(address.length) > address.bytes.size()) { return; }
            pending.socket = socket(address.family, address.socketType, address.protocol);
            if (pending.socket == INVALID_CONNECTION_SOCKET) { return; }
            #if !defined(_WIN32) && defined(SO_NOSIGPIPE)
            int enabled = 1; (void)::setsockopt(pending.socket, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled));
            #endif
            if (!blocking(pending.socket, false)) {
                pending.reset();
                return;
            }
            const auto rc = connect(pending.socket, reinterpret_cast<const sockaddr*>(address.bytes.data()), address.length);
            if (rc == 0) {
                pending.ready = true;
                return;
            }
            #ifdef _WIN32
            const auto error = ::WSAGetLastError();
            const auto inProgress = error == WSAEWOULDBLOCK || error == WSAEINPROGRESS || error == WSAEINVAL;
            #else
            const auto inProgress = errno == EINPROGRESS || errno == EINTR;
            #endif
            if (!inProgress) { pending.reset(); }
        }

        ConnectionSocket connectAddresses(std::span<const ConnectionAddress> addresses, ConnectionDeadline deadline, const ConnectionCancelled& cancelled) {
            socketRuntime();
            // Race two candidates so an unreachable IPv6/first address cannot
            // consume the whole budget before a reachable alternative is tried.
            std::array<PendingConnection, 2> pending;
            size_t next = 0;
            while (!connectionExpired(deadline, cancelled)) {
                auto active = false;
                for (auto& candidate : pending) {
                    while (candidate.socket == INVALID_CONNECTION_SOCKET && next < addresses.size()) {
                        if (connectionExpired(deadline, cancelled)) { return INVALID_CONNECTION_SOCKET; }
                        startConnection(candidate, addresses[next++]);
                    }
                    if (candidate.ready && blocking(candidate.socket, true) && !connectionExpired(deadline, cancelled)) {
                        return std::exchange(candidate.socket, INVALID_CONNECTION_SOCKET);
                    }
                    if (candidate.ready) { candidate.reset(); }
                    active = active || candidate.socket != INVALID_CONNECTION_SOCKET;
                }
                if (!active) {
                    if (next == addresses.size()) { break; }
                    continue;
                }
                const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(deadline - ConnectionClock::now());
                const auto slice = std::min(CANCEL_POLL, remaining);
                if (slice.count() <= 0) { break; }
                #ifdef _WIN32
                fd_set writable, errors;
                FD_ZERO(&writable);
                FD_ZERO(&errors);
                for (const auto& [socket, ready] : pending) {
                    if (socket != INVALID_CONNECTION_SOCKET) {
                        FD_SET(socket, &writable);
                        FD_SET(socket, &errors);
                    }
                }
                timeval timeout{};
                timeout.tv_usec = static_cast<long>(slice.count() * 1000);
                const auto rc = select(0, nullptr, &writable, &errors, &timeout);
                if (rc < 0 && WSAGetLastError() == WSAEINTR) { continue; }
                #else
                std::array<pollfd, 2> writable{};
                for (size_t i = 0; i < pending.size(); ++i) { writable[i] = {pending[i].socket, POLLOUT, 0}; }
                const auto rc = ::poll(writable.data(), writable.size(), static_cast<int>(slice.count())); if (rc < 0 && errno == EINTR) { continue; }
                #endif
                if (rc < 0) { break; }
                if (rc == 0) { continue; }
                for (auto&& [i, candidate] : std::views::enumerate(pending)) {
                    if (candidate.socket == INVALID_CONNECTION_SOCKET) { continue; }

                    auto error = 0;

                    #ifdef _WIN32
                    if (!FD_ISSET(candidate.socket, &writable) && !FD_ISSET(candidate.socket, &errors)) { continue; }

                    int length = sizeof(error);
                    const auto ok = getsockopt(candidate.socket, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&error), &length) == 0;
                    #else
                    if (writable[i].revents == 0) { continue; } socklen_t length = sizeof(error);
                    const auto ok = (writable[i].revents & POLLNVAL) == 0 && getsockopt(candidate.socket, SOL_SOCKET, SO_ERROR, &error, &length) == 0;
                    #endif

                    if (ok && error == 0) { candidate.ready = true; }
                    else { candidate.reset(); }
                }
            }
            return INVALID_CONNECTION_SOCKET;
        }
    }

    struct DeadlineResolver::State {
        struct Job {
            Addresses addresses;
            bool complete = false;
        };

        Lookup lookup;
        size_t capacity;
        std::mutex mutex;
        std::condition_variable changed;
        std::map<std::pair<std::string, uint16_t>, std::shared_ptr<Job>> jobs;

        State(Lookup value, size_t limit)
            : lookup{std::move(value)}, capacity{limit} {
            if (!lookup || capacity == 0) { throw std::invalid_argument("Cluster resolver: empty bounds or lookup"); }
        }
    };

    DeadlineResolver::DeadlineResolver(size_t capacity)
        : DeadlineResolver([](const std::string& host, uint16_t port) { return lookupAddresses(host, port, false); }, capacity) { systemLookup_ = true; }

    DeadlineResolver::DeadlineResolver(Lookup lookup, size_t capacity) : state_{std::make_shared<State>(std::move(lookup), capacity)} {}
    DeadlineResolver::~DeadlineResolver() = default;

    DeadlineResolver::Addresses DeadlineResolver::resolve(
        const std::string& host,
        uint16_t port,
        ConnectionDeadline deadline,
        const ConnectionCancelled& cancelled
    ) {
        if (connectionExpired(deadline, cancelled)) { return {}; }
        if (systemLookup_) {
            auto numeric = lookupAddresses(host, port, true);
            if (!numeric.empty()) { return connectionExpired(deadline, cancelled) ? Addresses{} : numeric; }
        }
        const auto state = state_;
        const auto key = std::make_pair(host, port);
        std::shared_ptr<State::Job> job;
        std::unique_lock lock{state->mutex};
        while (!connectionExpired(deadline, cancelled)) {
            if (!job) {
                const auto existing = state->jobs.find(key);
                if (existing != state->jobs.end()) { job = existing->second; }
                else if (state->jobs.size() < state->capacity) {
                    if (!keepResolverModuleResident()) { return {}; }
                    job = std::make_shared<State::Job>();
                    state->jobs.emplace(key, job);
                    try {
                        // OS name lookup cannot portably be interrupted. Only
                        // owned job data survives the caller; no runtime is held.
                        // Outstanding jobs, including timed-out ones, stay bounded.
                        std::thread(
                            [state, key, job] {
                                Addresses addresses;
                                try {
                                    addresses = state->lookup(key.first, key.second);
                                    if (addresses.size() > MAX_RESOLVED_ADDRESSES) { addresses.resize(MAX_RESOLVED_ADDRESSES); }
                                }
                                catch (...) {}
                                std::lock_guard doneLock{state->mutex};
                                job->addresses = std::move(addresses);
                                job->complete = true;
                                state->jobs.erase(key);
                                state->changed.notify_all();
                            }
                        ).detach();
                    }
                    catch (...) {
                        state->jobs.erase(key);
                        state->changed.notify_all();
                        return {};
                    }
                }
            }
            if (job && job->complete) { return job->addresses; }
            state->changed.wait_until(lock, std::min(deadline, ConnectionClock::now() + CANCEL_POLL));
        }
        return {};
    }

    ConnectionSocket connectWithDeadline(
        DeadlineResolver& resolver,
        const std::string& host,
        uint16_t port,
        ConnectionDeadline deadline,
        const ConnectionCancelled& cancelled
    ) {
        auto addresses = resolver.resolve(host, port, deadline, cancelled);
        if (addresses.size() > 2) {
            // DNS often groups IPv6 before IPv4. Race the first address
            // with an address from the other family when one is present.
            const auto alternate = std::ranges::find_if(
                addresses | std::views::drop(1),
                [&](const auto& address) { return address.family != addresses.front().family; }
            );
            if (alternate != addresses.end()) { std::ranges::rotate(addresses.begin() + 1, alternate, alternate + 1); }
        }
        return connectAddresses(addresses, deadline, cancelled);
    }

    ConnectionSocket connectWithDeadline(const std::string& host, uint16_t port, ConnectionDeadline deadline, const ConnectionCancelled& cancelled) {
        static DeadlineResolver resolver;
        return connectWithDeadline(resolver, host, port, deadline, cancelled);
    }
}

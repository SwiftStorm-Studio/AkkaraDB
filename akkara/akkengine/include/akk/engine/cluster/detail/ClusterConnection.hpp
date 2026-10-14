/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
#pragma once

#include "akk/engine/cluster/AkkClusterRuntimeExport.hpp"
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace akkaradb::engine::cluster::detail {
    using ConnectionClock = std::chrono::steady_clock;
    using ConnectionDeadline = ConnectionClock::time_point;
    using ConnectionCancelled = std::function<bool()>;
    #ifdef _WIN32
    using ConnectionSocket = uintptr_t;
    constexpr ConnectionSocket INVALID_CONNECTION_SOCKET = ~ConnectionSocket{0};
    #else
    using ConnectionSocket = int;
    constexpr ConnectionSocket INVALID_CONNECTION_SOCKET = -1;
    #endif

    struct ConnectionAddress {
        // Value ownership lets resolver work outlive a timed-out caller.
        alignas(std::max_align_t) std::array<unsigned char, 128> bytes{};
        int length = 0;
        int family = 0;
        int socketType = 0;
        int protocol = 0;
    };

    class AKKARADB_CLUSTER_RUNTIME_API DeadlineResolver {
    public:
        using Addresses = std::vector<ConnectionAddress>;
        using Lookup = std::function<Addresses(const std::string&, uint16_t)>;
        explicit DeadlineResolver(size_t capacity = 4);
        DeadlineResolver(Lookup lookup, size_t capacity);
        ~DeadlineResolver();
        DeadlineResolver(const DeadlineResolver&) = delete;
        DeadlineResolver& operator=(const DeadlineResolver&) = delete;
        Addresses resolve(const std::string& host, uint16_t port, ConnectionDeadline deadline,
                          const ConnectionCancelled& cancelled = {});
    private:
        struct State;
        std::shared_ptr<State> state_;
        bool systemLookup_ = false;
    };

    inline bool connectionExpired(ConnectionDeadline deadline, const ConnectionCancelled& cancelled) {
        return ConnectionClock::now() >= deadline || (cancelled && cancelled());
    }

    AKKARADB_CLUSTER_RUNTIME_API ConnectionSocket connectWithDeadline(const std::string& host, uint16_t port,
        ConnectionDeadline deadline, const ConnectionCancelled& cancelled = {});
    AKKARADB_CLUSTER_RUNTIME_API ConnectionSocket connectWithDeadline(DeadlineResolver& resolver,
        const std::string& host, uint16_t port, ConnectionDeadline deadline, const ConnectionCancelled& cancelled = {});
}

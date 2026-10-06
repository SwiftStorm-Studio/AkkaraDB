/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
#pragma once

#include <akk/engine/cluster/ClusterConfig.hpp>
#include <chrono>
#include <future>
#include <mutex>

namespace akkaradb::engine::cluster::detail {
    // Kept in the core library so a call crossing the runtime DLL keeps its budget.
    class AKDB_API ReconfigurationDeadline {
    public:
        using Clock = std::chrono::steady_clock;
        struct Context {
            Clock::time_point deadline = Clock::time_point::max();
            std::function<bool()> cancelled;
        };
        explicit ReconfigurationDeadline(uint32_t timeoutMs, std::function<bool()> cancelled = {});
        explicit ReconfigurationDeadline(Context context);
        ~ReconfigurationDeadline();
        ReconfigurationDeadline(const ReconfigurationDeadline&) = delete;
        ReconfigurationDeadline& operator=(const ReconfigurationDeadline&) = delete;
        static Context current();
        static void check();
        static Clock::time_point cap(Clock::time_point deadline);
        static uint32_t remaining(uint32_t timeoutMs);
        static std::unique_lock<std::timed_mutex> lock(std::timed_mutex& mutex);
        template<class Future> static decltype(auto) wait(Future& future) {
            while (future.wait_for(std::chrono::milliseconds{10}) != std::future_status::ready) { check(); }
            check();
            return future.get();
        }
    private:
        Context previous_;
    };
}

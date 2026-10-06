/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
#include <akk/engine/cluster/detail/ReconfigurationDeadline.hpp>
#include <algorithm>
#include <stdexcept>

namespace akkaradb::engine::cluster::detail {
    namespace { thread_local ReconfigurationDeadline::Context context; }
    ReconfigurationDeadline::ReconfigurationDeadline(uint32_t timeoutMs, std::function<bool()> cancelled)
        : previous_{context} {
        context.deadline = std::min(context.deadline, Clock::now() + std::chrono::milliseconds{timeoutMs});
        if (cancelled) {
            auto parent = context.cancelled;
            context.cancelled = [parent = std::move(parent), hook = std::move(cancelled)] { return (parent && parent()) || hook(); };
        }
    }
    ReconfigurationDeadline::ReconfigurationDeadline(Context next) : previous_{std::move(context)} { context = std::move(next); }
    ReconfigurationDeadline::~ReconfigurationDeadline() { context = std::move(previous_); }
    ReconfigurationDeadline::Context ReconfigurationDeadline::current() { return context; }
    void ReconfigurationDeadline::check() {
        if (Clock::now() >= context.deadline) { throw std::runtime_error("Cluster reconfiguration: overall deadline exceeded; inspect the committed plan before retrying"); }
        if (context.cancelled && context.cancelled()) { throw std::runtime_error("Cluster reconfiguration: cancelled"); }
    }
    ReconfigurationDeadline::Clock::time_point ReconfigurationDeadline::cap(Clock::time_point deadline) { return std::min(deadline, context.deadline); }
    uint32_t ReconfigurationDeadline::remaining(uint32_t timeoutMs) {
        check();
        if (context.deadline == Clock::time_point::max()) { return timeoutMs; }
        const auto left = std::chrono::ceil<std::chrono::milliseconds>(context.deadline - Clock::now()).count();
        return static_cast<uint32_t>(std::clamp<int64_t>(left, 1, timeoutMs));
    }
    std::unique_lock<std::timed_mutex> ReconfigurationDeadline::lock(std::timed_mutex& mutex) {
        std::unique_lock result{mutex, std::defer_lock};
        do { check(); } while (!result.try_lock_for(std::chrono::milliseconds{10}));
        check();
        return result;
    }
}

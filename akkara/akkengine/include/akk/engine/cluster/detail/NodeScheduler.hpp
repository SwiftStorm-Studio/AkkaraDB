/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
#pragma once
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>
#include <vector>

namespace akkaradb::engine::cluster::detail {
    // One queued/running invocation per task, regardless of notification volume.
    // Nonblocking timer callbacks are isolated from workers doing group I/O.
    class NodeScheduler : public std::enable_shared_from_this<NodeScheduler> {
        using Clock = std::chrono::steady_clock;
        struct State {
            uint64_t id;
            std::function<void()> callback;
            std::optional<Clock::time_point> next;
            bool queued = false, running = false, cancelled = false;
            bool timer = false;
        };
        using Key = std::pair<Clock::time_point, uint64_t>;
        std::mutex mutex_;
        std::condition_variable timers_, work_, finished_;
        std::map<Key, std::shared_ptr<State>> pending_;
        std::deque<std::shared_ptr<State>> ready_;
        std::vector<std::thread> workers_;
        std::thread timer_;
        uint64_t nextId_ = 0;
        size_t workerLimit_, idleWorkers_ = 0;
        bool stopping_ = false;

        void notify(const std::shared_ptr<State>& state, Clock::time_point when) {
            std::lock_guard lock{mutex_};
            if (stopping_ || state->cancelled || (state->next && *state->next <= when)) { return; }
            if (state->next && !state->running && !state->queued) { pending_.erase({*state->next, state->id}); }
            state->next = when;
            if (!state->running && !state->queued) { pending_.emplace(Key{when, state->id}, state); }
            timers_.notify_one();
        }
        void cancel(const std::shared_ptr<State>& state) {
            std::unique_lock lock{mutex_};
            state->cancelled = true;
            if (state->next) { pending_.erase({*state->next, state->id}); state->next.reset(); }
            std::erase(ready_, state);
            state->queued = false;
            timers_.notify_one();
            finished_.wait(lock, [&] { return !state->running; });
        }
        void runTimers() {
            std::unique_lock lock{mutex_};
            while (!stopping_) {
                if (pending_.empty()) { timers_.wait(lock); continue; }
                const auto next = pending_.begin()->first.first;
                if (Clock::now() < next) { timers_.wait_until(lock, next); continue; }
                auto state = std::move(pending_.begin()->second);
                pending_.erase(pending_.begin());
                state->next.reset();
                if (state->timer) { invoke(lock, state); continue; }
                state->queued = true;
                ready_.push_back(std::move(state));
                if (ready_.size() > idleWorkers_ && workers_.size() < workerLimit_) {
                    try { workers_.emplace_back([this] { runWork(); }); }
                    catch (...) { /* Existing workers can still service the queue. */ }
                }
                work_.notify_one();
            }
        }
        void runWork() {
            std::unique_lock lock{mutex_};
            for (;;) {
                ++idleWorkers_;
                work_.wait(lock, [&] { return stopping_ || !ready_.empty(); });
                --idleWorkers_;
                if (stopping_) { return; }
                auto state = std::move(ready_.front()); ready_.pop_front();
                invoke(lock, state);
            }
        }
        void invoke(std::unique_lock<std::mutex>& lock, const std::shared_ptr<State>& state) {
            state->queued = false; state->running = true;
            lock.unlock();
            // Callbacks own failure reporting and must not throw across a worker.
            try { state->callback(); } catch (...) { std::terminate(); }
            lock.lock();
            state->running = false;
            if (!state->cancelled && state->next) {
                pending_.emplace(Key{*state->next, state->id}, state); timers_.notify_one();
            }
            finished_.notify_all();
        }
    public:
        class Task {
            friend class NodeScheduler;
            std::shared_ptr<NodeScheduler> owner_;
            std::shared_ptr<State> state_;
            Task(std::shared_ptr<NodeScheduler> owner, std::shared_ptr<State> state)
                : owner_{std::move(owner)}, state_{std::move(state)} {}
        public:
            ~Task() { cancel(); }
            void notify(Clock::time_point when = Clock::now()) { owner_->notify(state_, when); }
            void cancel() { owner_->cancel(state_); }
            Task(const Task&) = delete;
            Task& operator=(const Task&) = delete;
        };
        explicit NodeScheduler(size_t workers = 8) : workerLimit_{workers} {
            if (!workers) { throw std::invalid_argument("Node scheduler: no workers"); }
            try {
                workers_.emplace_back([this] { runWork(); });
                timer_ = std::thread([this] { runTimers(); });
            } catch (...) { stop(); throw; }
        }
        ~NodeScheduler() { stop(); }
        NodeScheduler(const NodeScheduler&) = delete;
        std::shared_ptr<Task> task(std::function<void()> callback, bool nonblockingTimer = false) {
            std::lock_guard lock{mutex_};
            auto state = std::make_shared<State>();
            state->id = ++nextId_; state->callback = std::move(callback); state->timer = nonblockingTimer;
            return std::shared_ptr<Task>{new Task{shared_from_this(), std::move(state)}};
        }
    private:
        void stop() noexcept {
            { std::lock_guard lock{mutex_}; stopping_ = true; }
            timers_.notify_all(); work_.notify_all();
            if (timer_.joinable()) { timer_.join(); }
            for (auto& worker : workers_) { if (worker.joinable()) { worker.join(); } }
        }
    };
}

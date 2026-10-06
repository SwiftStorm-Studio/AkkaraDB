/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
#pragma once

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

namespace akkaradb::engine::cluster::detail {
    class BoundedExecutor {
    public:
        BoundedExecutor(size_t workers, size_t capacity) : capacity_{capacity} {
            if (workers == 0 || capacity == 0) { throw std::invalid_argument("Executor: empty bounds"); }
            try {
                for (size_t i = 0; i < workers; ++i) { workers_.emplace_back([this] { run(); }); }
            }
            catch (...) { close(); throw; }
        }
        ~BoundedExecutor() { close(); }
        BoundedExecutor(const BoundedExecutor&) = delete;
        bool trySubmit(std::function<void()> task) {
            std::lock_guard lock{mutex_};
            if (stopping_ || queue_.size() >= capacity_) { return false; }
            queue_.push_back({std::move(task), nullptr}); available_.notify_one(); return true;
        }
        void forEach(size_t count, const std::function<void(size_t)>& task,
            std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::time_point::max()) {
            const char group = 0;
            const bool timed = deadline != std::chrono::steady_clock::time_point::max();
            std::vector<std::future<void>> completions;
            completions.reserve(count);
            std::exception_ptr failure;
            try {
                for (size_t i = 0; i < count; ++i) {
                    auto work = std::make_shared<std::packaged_task<void()>>([&task, i] { task(i); });
                    completions.push_back(work->get_future());
                    std::unique_lock lock{mutex_};
                    const auto admitted = [&] { return stopping_ || queue_.size() < capacity_; };
                    if (timed) {
                        if (!space_.wait_until(lock, deadline, admitted) || std::chrono::steady_clock::now() >= deadline) {
                            throw std::runtime_error("Executor: overall deadline exceeded while queued");
                        }
                    } else { space_.wait(lock, admitted); }
                    if (stopping_) { throw std::runtime_error("Executor: closed"); }
                    queue_.push_back({[work] { (*work)(); }, &group}); available_.notify_one();
                }
            }
            catch (...) {
                failure = std::current_exception();
                if (timed) { cancelQueued(&group); }
            }
            // Submitted work may reference the caller's stack; always join it,
            // including after submission or one shard's execution has failed.
            for (auto& completion : completions) {
                if (timed) {
                    try {
                        if (completion.wait_until(deadline) == std::future_status::timeout) {
                            throw std::runtime_error("Executor: overall deadline exceeded while queued");
                        }
                    }
                    catch (...) {
                        if (!failure) { failure = std::current_exception(); }
                        cancelQueued(&group);
                    }
                }
                // Running tasks must enforce the same deadline themselves.
                try { completion.get(); }
                catch (...) { if (!failure) { failure = std::current_exception(); } }
            }
            if (failure) { std::rethrow_exception(failure); }
        }
    private:
        struct WorkItem { std::function<void()> task; const void* group; };
        void cancelQueued(const void* group) {
            std::lock_guard lock{mutex_};
            std::erase_if(queue_, [group](const WorkItem& work) { return work.group == group; });
            space_.notify_all();
        }
        void close() noexcept {
            { std::lock_guard lock{mutex_}; stopping_ = true; }
            available_.notify_all(); space_.notify_all();
            for (auto& worker : workers_) { if (worker.joinable()) { worker.join(); } }
        }
        void run() {
            for (;;) {
                std::function<void()> task;
                {
                    std::unique_lock lock{mutex_};
                    available_.wait(lock, [&] { return stopping_ || !queue_.empty(); });
                    if (queue_.empty()) { return; }
                    task = std::move(queue_.front().task); queue_.pop_front(); space_.notify_one();
                }
                task();
            }
        }
        size_t capacity_;
        bool stopping_ = false;
        std::mutex mutex_;
        std::condition_variable available_, space_;
        std::deque<WorkItem> queue_;
        std::vector<std::thread> workers_;
    };
}

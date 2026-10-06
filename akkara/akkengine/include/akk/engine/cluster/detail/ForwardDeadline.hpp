/*
 * AkkaraDB - The all-purpose KV store
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
#pragma once
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>
#include <utility>

namespace akkaradb::engine::cluster::detail {
    // Socket timeouts apply to individual I/O calls. A slow stream can keep
    // making partial progress indefinitely; forwarding needs one total budget.
    class ForwardDeadline {
    public:
        ForwardDeadline(std::chrono::steady_clock::time_point deadline, std::function<void()> interrupt)
            : thread_([this, deadline, interrupt = std::move(interrupt)] {
                std::unique_lock lock{mutex_};
                if (!cv_.wait_until(lock, deadline, [this] { return finished_; })) {
                    lock.unlock(); interrupt();
                }
            }) {}
        ~ForwardDeadline() {
            { std::lock_guard lock{mutex_}; finished_ = true; }
            cv_.notify_one(); thread_.join();
        }
        ForwardDeadline(const ForwardDeadline&) = delete;
        ForwardDeadline& operator=(const ForwardDeadline&) = delete;
    private:
        std::mutex mutex_;
        std::condition_variable cv_;
        bool finished_ = false;
        std::thread thread_;
    };
}

/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>

namespace akkaradb::engine::cluster::detail {
    // Entries exist only while callers hold or wait for this exact binary key.
    class KeyedMutex {
        struct Entry { std::mutex mutex; };
    public:
        class Guard {
            friend class KeyedMutex;
            Guard(KeyedMutex& owner, std::string key, std::shared_ptr<Entry> entry)
                : owner_{owner}, key_{std::move(key)}, entry_{std::move(entry)}, lock_{entry_->mutex} {}
        public:
            Guard(const Guard&) = delete;
            Guard(Guard&& other) noexcept : owner_{other.owner_}, key_{std::move(other.key_)},
                entry_{std::move(other.entry_)}, lock_{std::move(other.lock_)} {}
            ~Guard() {
                if (!entry_) { return; }
                lock_.unlock();
                std::lock_guard registry{owner_.mutex_};
                if (entry_.use_count() == 2) { owner_.entries_.erase(key_); }
            }
        private:
            KeyedMutex& owner_;
            std::string key_;
            std::shared_ptr<Entry> entry_;
            std::unique_lock<std::mutex> lock_;
        };
        [[nodiscard]] Guard lock(std::span<const uint8_t> key) {
            std::string id{reinterpret_cast<const char*>(key.data()), key.size()};
            std::shared_ptr<Entry> entry;
            {
                std::lock_guard registry{mutex_};
                auto& slot = entries_[id];
                if (!slot) { slot = std::make_shared<Entry>(); }
                entry = slot;
            }
            return Guard{*this, std::move(id), std::move(entry)};
        }
    private:
        std::mutex mutex_;
        std::unordered_map<std::string, std::shared_ptr<Entry>> entries_;
    };
}

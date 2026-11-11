/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
#pragma once
#include "akk/engine/cluster/ClusterConfig.hpp"
#include "akk/engine/cluster/ReplFraming.hpp"
#include "akk/engine/cluster/detail/NodeScheduler.hpp"
#include "akk/engine/cluster/detail/BoundedExecutor.hpp"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <stdexcept>
#include <vector>

namespace akkaradb::engine::cluster::detail {
    inline void validateNodeResourceLimits(const ClusterNodeResourceLimits& value) {
        constexpr uint64_t minimumFrameMemory = 3ull * (ReplFrameHeader::SIZE + ReplFrameHeader::MAX_RAFT_PAYLOAD_SIZE);
        if (value.raftReceiveMemoryBytes < minimumFrameMemory || value.forwardReceiveMemoryBytes < minimumFrameMemory ||
            value.raftReceiveMemoryBytes > 64ull * 1024 * 1024 * 1024 || value.forwardReceiveMemoryBytes > 64ull * 1024 * 1024 * 1024 ||
            !value.outgoingForwardRequests || !value.incomingForwardRequests ||
            !value.transferMemoryBytes || value.transferMemoryBytes > 64ull * 1024 * 1024 * 1024 ||
            !value.concurrentTransfers || value.concurrentTransfers > 65'536) {
            throw std::invalid_argument("Cluster: invalid node resource limits");
        }
    }
    // Header implementation keeps the same lease domain across Native/runtime DLLs.
    class NodeResourceQuota : public std::enable_shared_from_this<NodeResourceQuota> {
        struct Lease {
            std::shared_ptr<NodeResourceQuota> owner;
            uint64_t amount;
            bool released = false;
            std::function<void()> evict;
            Lease(std::shared_ptr<NodeResourceQuota> value, uint64_t charge) : owner{std::move(value)}, amount{charge} {}
            ~Lease() {
                if (!released) { owner->used_ -= amount; }
                owner->cv_.notify_all();
            }
        };
        const uint64_t limit_;
        mutable std::mutex mutex_;
        std::condition_variable cv_;
        std::atomic<uint64_t> used_{0};
        std::vector<std::weak_ptr<Lease>> idle_;
        void reclaim(uint64_t amount, std::unique_lock<std::mutex>& lock) {
            std::vector<std::function<void()>> callbacks;
            for (auto& weak : idle_) {
                if (used_ <= limit_ - amount) { break; }
                if (auto entry = weak.lock(); entry && entry->evict && !entry->released) {
                    entry->released = true; used_ -= entry->amount;
                    callbacks.push_back(std::move(entry->evict));
                }
            }
            if (callbacks.empty()) { return; }
            lock.unlock();
            for (auto& callback : callbacks) { callback(); }
            lock.lock();
            cv_.notify_all();
        }
        std::shared_ptr<void> lease(uint64_t amount) {
            auto result = std::make_shared<Lease>(shared_from_this(), amount);
            used_ += amount; return result;
        }
    public:
        explicit NodeResourceQuota(uint64_t limit) : limit_{limit} {}
        std::shared_ptr<void> tryReserve(uint64_t amount) {
            std::unique_lock lock{mutex_};
            if (amount > limit_) { return {}; }
            if (used_ > limit_ - amount) { reclaim(amount, lock); }
            if (used_ > limit_ - amount) { return {}; }
            return lease(amount);
        }
        // Cached connections retain their budget but surrender it before new
        // active work is rejected. Activation and eviction are mutually exclusive.
        void park(const std::shared_ptr<void>& value, std::function<void()> evict) {
            const auto entry = std::static_pointer_cast<Lease>(value);
            std::lock_guard lock{mutex_};
            if (entry->released) { return; }
            std::erase_if(idle_, [](const auto& weak) {
                const auto item = weak.lock(); return !item || !item->evict || item->released;
            });
            if (!entry->evict) { idle_.push_back(entry); }
            entry->evict = std::move(evict);
        }
        bool activate(const std::shared_ptr<void>& value) {
            const auto entry = std::static_pointer_cast<Lease>(value);
            std::lock_guard lock{mutex_};
            entry->evict = {};
            return !entry->released;
        }
        std::shared_ptr<void> reserve(uint64_t amount) {
            auto result = tryReserve(amount);
            if (!result) { throw std::runtime_error("Cluster node resource budget exhausted"); }
            return result;
        }
        template<class Cancelled>
        std::shared_ptr<void> waitReserve(uint64_t amount, std::chrono::steady_clock::time_point deadline, Cancelled cancelled) {
            std::unique_lock lock{mutex_};
            if (amount > limit_) { return {}; }
            for (;;) {
                if (cancelled() || std::chrono::steady_clock::now() >= deadline) { return {}; }
                if (used_ > limit_ - amount) { reclaim(amount, lock); }
                if (used_ <= limit_ - amount) { return lease(amount); }
                cv_.wait_until(lock, std::min(deadline, std::chrono::steady_clock::now() + std::chrono::milliseconds{20}));
            }
        }
        uint64_t used() const { return used_.load(); }
    };
    class NodeResourceBudget {
        std::mutex schedulerMutex_;
        std::shared_ptr<NodeScheduler> scheduler_;
        std::shared_ptr<BoundedExecutor> queryExecutor_;
    public:
        std::shared_ptr<NodeScheduler> scheduler() {
            std::lock_guard lock{schedulerMutex_};
            if (!scheduler_) { scheduler_ = std::make_shared<NodeScheduler>(); }
            return scheduler_;
        }
        std::shared_ptr<BoundedExecutor> queryExecutor() {
            std::lock_guard lock{schedulerMutex_};
            if (!queryExecutor_) { queryExecutor_ = std::make_shared<BoundedExecutor>(8, 16); }
            return queryExecutor_;
        }
        std::shared_ptr<NodeResourceQuota> queryPrefetch = std::make_shared<NodeResourceQuota>(16);
        const ClusterNodeResourceLimits limits;
        std::shared_ptr<NodeResourceQuota> raftReceive, forwardReceive, outgoingForward, incomingForward, transferMemory, transfers;
        std::shared_ptr<NodeResourceQuota> controlReceive, handshakeReceive, incomingControl, outgoingControl, handshakes;
        // Extra optimistic send windows share a node cap, independent of inbound frames.
        std::shared_ptr<NodeResourceQuota> raftPipeline;
        explicit NodeResourceBudget(ClusterNodeResourceLimits value) : limits{value} {
            validateNodeResourceLimits(value);
            raftReceive = std::make_shared<NodeResourceQuota>(value.raftReceiveMemoryBytes);
            raftPipeline = std::make_shared<NodeResourceQuota>(value.raftReceiveMemoryBytes);
            forwardReceive = std::make_shared<NodeResourceQuota>(value.forwardReceiveMemoryBytes);
            outgoingForward = std::make_shared<NodeResourceQuota>(value.outgoingForwardRequests);
            incomingForward = std::make_shared<NodeResourceQuota>(value.incomingForwardRequests);
            transferMemory = std::make_shared<NodeResourceQuota>(value.transferMemoryBytes);
            transfers = std::make_shared<NodeResourceQuota>(value.concurrentTransfers);
            controlReceive = std::make_shared<NodeResourceQuota>(3ull * (ReplFrameHeader::SIZE + ReplFrameHeader::MAX_RAFT_PAYLOAD_SIZE));
            handshakeReceive = std::make_shared<NodeResourceQuota>(64 * 1024);
            incomingControl = std::make_shared<NodeResourceQuota>(16);
            outgoingControl = std::make_shared<NodeResourceQuota>(16);
            handshakes = std::make_shared<NodeResourceQuota>(64);
        }
        template<class Stats> void observe(Stats& out) const {
            out.nodeRaftReceiveMemoryBytes = raftReceive->used();
            out.nodeForwardReceiveMemoryBytes = forwardReceive->used();
            out.nodeOutgoingForwardRequests = outgoingForward->used();
            out.nodeIncomingForwardRequests = incomingForward->used();
            out.nodeTransferMemoryBytes = transferMemory->used();
            out.nodeActiveTransfers = transfers->used();
        }
    };
    inline void ensureNodeResourceBudget(ClusterRuntimeOptions& options) {
        validateNodeResourceLimits(options.nodeResources);
        if (options.nodeResourceBudget && options.nodeResourceBudget->limits != options.nodeResources) {
            throw std::invalid_argument("Cluster: shared node resource limits differ");
        }
        if (!options.nodeResourceBudget) { options.nodeResourceBudget = std::make_shared<NodeResourceBudget>(options.nodeResources); }
    }
}

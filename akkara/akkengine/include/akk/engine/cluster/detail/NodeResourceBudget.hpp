/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
#pragma once
#include "akk/engine/cluster/ClusterConfig.hpp"
#include "akk/engine/cluster/ReplFraming.hpp"
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <stdexcept>

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
            Lease(std::shared_ptr<NodeResourceQuota> value, uint64_t charge) : owner{std::move(value)}, amount{charge} {}
            ~Lease() {
                std::lock_guard lock{owner->mutex_};
                owner->used_ -= amount; owner->cv_.notify_all();
            }
        };
        const uint64_t limit_;
        mutable std::mutex mutex_;
        std::condition_variable cv_;
        uint64_t used_ = 0;
        std::shared_ptr<void> lease(uint64_t amount) {
            auto result = std::make_shared<Lease>(shared_from_this(), amount);
            used_ += amount; return result;
        }
    public:
        explicit NodeResourceQuota(uint64_t limit) : limit_{limit} {}
        std::shared_ptr<void> tryReserve(uint64_t amount) {
            std::lock_guard lock{mutex_};
            if (amount > limit_ || used_ > limit_ - amount) { return {}; }
            return lease(amount);
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
                if (used_ <= limit_ - amount) { return lease(amount); }
                cv_.wait_until(lock, std::min(deadline, std::chrono::steady_clock::now() + std::chrono::milliseconds{20}));
            }
        }
        uint64_t used() const { std::lock_guard lock{mutex_}; return used_; }
    };
    class NodeResourceBudget {
    public:
        const ClusterNodeResourceLimits limits;
        std::shared_ptr<NodeResourceQuota> raftReceive, forwardReceive, outgoingForward, incomingForward, transferMemory, transfers;
        std::shared_ptr<NodeResourceQuota> controlReceive, handshakeReceive, incomingControl, outgoingControl, handshakes;
        explicit NodeResourceBudget(ClusterNodeResourceLimits value) : limits{value} {
            validateNodeResourceLimits(value);
            raftReceive = std::make_shared<NodeResourceQuota>(value.raftReceiveMemoryBytes);
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

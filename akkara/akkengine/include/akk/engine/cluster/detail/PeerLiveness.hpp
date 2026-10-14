/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
#pragma once
#include "akk/engine/cluster/ClusterConfig.hpp"
#include <chrono>
#include <mutex>

namespace akkaradb::engine::cluster::detail {
    class PeerLiveness {
    public:
        using Clock = std::chrono::steady_clock;
        explicit PeerLiveness(ReplicationLivenessOptions options) : options_{options} { options_.validate(); reset(); }
        void reset() {
            std::lock_guard lock{mutex_};
            lastInbound_ = Clock::now(); nextProbe_ = lastInbound_ + interval();
            nonce_ = acknowledged_ = peerNonce_ = pendingPong_ = 0;
        }
        void observeInbound() {
            std::lock_guard lock{mutex_}; lastInbound_ = Clock::now();
        }
        Clock::time_point readDeadline() const {
            std::lock_guard lock{mutex_};
            return lastInbound_ + timeout();
        }
        Clock::time_point nextProbeDeadline() const {
            std::lock_guard lock{mutex_}; return nextProbe_;
        }
        bool expired() const { return Clock::now() >= readDeadline(); }
        uint64_t beginProbe() {
            std::lock_guard lock{mutex_};
            const auto now = Clock::now();
            if (now < nextProbe_) { return 0; }
            if (++nonce_ == 0) { throw std::overflow_error("Replication heartbeat nonce exhausted"); }
            nextProbe_ = now + interval();
            return nonce_;
        }
        bool acknowledge(uint64_t nonce) {
            std::lock_guard lock{mutex_};
            const auto now = Clock::now();
            if (nonce == 0 || nonce <= acknowledged_ || nonce > nonce_ || now >= lastInbound_ + timeout()) { return false; }
            acknowledged_ = nonce; lastInbound_ = now; return true;
        }
        bool receiveProbe(uint64_t nonce) {
            std::lock_guard lock{mutex_};
            if (nonce == 0 || nonce <= peerNonce_) { return false; }
            peerNonce_ = pendingPong_ = nonce; lastInbound_ = Clock::now(); return true;
        }
        uint64_t takePong() {
            std::lock_guard lock{mutex_};
            const auto nonce = pendingPong_; pendingPong_ = 0; return nonce;
        }
    private:
        std::chrono::milliseconds interval() const { return std::chrono::milliseconds{options_.heartbeatIntervalMs}; }
        std::chrono::milliseconds timeout() const { return std::chrono::milliseconds{options_.peerTimeoutMs}; }
        ReplicationLivenessOptions options_;
        mutable std::mutex mutex_;
        Clock::time_point lastInbound_, nextProbe_;
        // High-water marks bound tracking even while Pongs queue behind data.
        uint64_t nonce_ = 0, acknowledged_ = 0, peerNonce_ = 0, pendingPong_ = 0;
    };
}

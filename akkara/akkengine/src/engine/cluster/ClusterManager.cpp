/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/cluster/ClusterManager.cpp
#include "akk/engine/cluster/ClusterManager.hpp"
#include "akk/engine/manifest/Manifest.hpp"

#include <atomic>
#include <chrono>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <utility>

namespace akkaradb::engine::cluster {
    namespace {
        constexpr uint64_t PRIMARY_LEASE_WINDOW_US = 30'000'000;

        [[nodiscard]] uint64_t nowUs() noexcept {
            return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch()
            ).count());
        }
    } // namespace

    class ClusterManager::Impl {
        public:
            Impl(std::filesystem::path dbDir, ClusterConfig config, uint64_t selfNodeId, ClusterRuntimeOptions runtimeOptions)
                : config_{std::move(config)}, selfNodeId_{selfNodeId}, runtimeOptions_{std::move(runtimeOptions)} {
                config_.validate();
                if (!config_.isStandalone() && config_.findById(selfNodeId_) == nullptr) {
                    throw std::runtime_error("ClusterManager: selfNodeId not found in cluster config");
                }
                if (!dbDir.empty()) {
                    clusterManifest_ = manifest::Manifest::create(dbDir / "cluster.akmf", false);
                    clusterManifest_->start();
                }
            }

            ~Impl() { close(); }

            void setRoleChangeCallback(RoleChangeCallback callback) {
                std::lock_guard lock{callbackMutex_};
                callback_ = std::move(callback);
            }

            void start() {
                if (running_.exchange(true)) { return; }
                try {
                    if (config_.isStandalone()) {
                        const auto* self = config_.findById(selfNodeId_);
                        {
                            std::lock_guard lock{primaryMutex_};
                            primaryNodeId_.store(self ? self->nodeId : 0, std::memory_order_release);
                            primaryHost_ = self ? self->host : std::string{};
                            primaryReplPort_ = self ? self->replPort : 0;
                        }
                        recordSelfJoin();
                        setRole(NodeRole::STANDALONE);
                        return;
                    }
                    if (configureFromManifestLease()) {
                        recordSelfJoin();
                        return;
                    }
                    if (configureFromExpiredManifestLease()) {
                        recordSelfJoin();
                        return;
                    }
                    selectRole();
                    recordSelfJoin();
                }
                catch (...) {
                    running_.store(false, std::memory_order_release);
                    throw;
                }
            }

            void close() {
                const bool wasRunning = running_.exchange(false);
                if (wasRunning) { recordSelfLeave(); }
            }

            NodeRole role() const noexcept { return role_.load(); }
            uint64_t selfNodeId() const noexcept { return selfNodeId_; }
            bool isStandalone() const noexcept { return config_.isStandalone(); }

            std::string primaryHost() const {
                std::lock_guard lock{primaryMutex_};
                return primaryHost_;
            }

            uint64_t primaryNodeId() const noexcept { return primaryNodeId_.load(); }

            uint16_t primaryReplPort() const {
                std::lock_guard lock{primaryMutex_};
                return primaryReplPort_;
            }

            std::vector<NodeInfo> activeNodes() const {
                std::vector<NodeInfo> out;
                if (!clusterManifest_) { return out; }

                for (const auto& configured : config_.nodes()) {
                    const auto join = latestManifestJoinFor(configured.nodeId);
                    if (!join.has_value()) { continue; }
                    if (manifestNodeLeftAfter(configured.nodeId, join->tsUs)) { continue; }

                    NodeInfo active = configured;
                    if (!join->host.empty()) { active.host = join->host; }
                    if (join->replPort != 0) { active.replPort = join->replPort; }
                    out.push_back(std::move(active));
                }
                return out;
            }

        private:
            void setRole(NodeRole role) {
                const NodeRole old = role_.exchange(role);
                if (old == role) { return; }
                RoleChangeCallback cb;
                {
                    std::lock_guard lock{callbackMutex_};
                    cb = callback_;
                }
                if (cb) { cb(role); }
            }

            void selectRole() {
                switch (runtimeOptions_.startupRole) {
                    case NodeStartupRole::PRIMARY: configurePrimarySelf();
                        setRole(NodeRole::PRIMARY);
                        return;
                    case NodeStartupRole::REPLICA: configureReplicaPrimary();
                        setRole(NodeRole::REPLICA);
                        return;
                    case NodeStartupRole::AUTO: default: throw std::runtime_error(
                            "ClusterManager: explicit startup role is required for non-standalone modes (PRIMARY or REPLICA)"
                        );
                }
            }

            bool configureFromManifestLease() {
                if (!clusterManifest_) { return false; }
                const auto lease = clusterManifest_->lastPrimaryLease();
                if (!lease.has_value() || lease->leaseUntilUs <= nowUs()) { return false; }
                if (manifestNodeLeftAfter(lease->nodeId, lease->tsUs)) { return false; }

                const auto* primary = config_.findById(lease->nodeId);
                if (primary == nullptr || !primary->coordinatorEligible()) { return false; }

                if (runtimeOptions_.startupRole == NodeStartupRole::PRIMARY && lease->nodeId != selfNodeId_) { return false; }
                if (runtimeOptions_.startupRole == NodeStartupRole::REPLICA && lease->nodeId == selfNodeId_) { return false; }

                if (lease->nodeId == selfNodeId_) {
                    configurePrimarySelf();
                    setRole(NodeRole::PRIMARY);
                    return true;
                }

                std::string host = runtimeOptions_.primaryHost;
                uint16_t replPort = runtimeOptions_.primaryReplPort;
                uint64_t primaryNodeId = runtimeOptions_.primaryNodeId;

                if (primaryNodeId != 0 && primaryNodeId != lease->nodeId) { return false; }
                if (primaryNodeId == 0) { primaryNodeId = runtimeOptions_.secure.expectedPrimaryNodeId; }
                if (primaryNodeId != 0 && primaryNodeId != lease->nodeId) { return false; }
                primaryNodeId = lease->nodeId;

                if (host.empty() || replPort == 0) {
                    if (const auto advertised = manifestJoinFor(lease->nodeId, lease->tsUs); advertised.has_value()) {
                        if (host.empty()) { host = advertised->host; }
                        if (replPort == 0) { replPort = advertised->replPort; }
                    }
                }
                if (host.empty()) { host = primary->host; }
                if (replPort == 0) { replPort = primary->replPort; }
                if (host.empty() || replPort == 0) { return false; }

                {
                    std::lock_guard lock{primaryMutex_};
                    primaryNodeId_.store(primaryNodeId, std::memory_order_release);
                    primaryHost_ = std::move(host);
                    primaryReplPort_ = replPort;
                }
                setRole(NodeRole::REPLICA);
                return true;
            }

            bool configureFromExpiredManifestLease() {
                if (!clusterManifest_ || runtimeOptions_.startupRole != NodeStartupRole::AUTO) { return false; }
                if (config_.consistency().mode == ConsistencyMode::RAFT_QUORUM) { return false; }

                const auto lease = clusterManifest_->lastPrimaryLease();
                if (!lease.has_value()) { return false; }
                const bool expired = lease->leaseUntilUs <= nowUs();
                const bool left = manifestNodeLeftAfter(lease->nodeId, lease->tsUs);
                if (!expired && !left) { return false; }
                if (!isDeterministicPrimaryCandidate()) { return false; }

                configurePrimarySelf();
                setRole(NodeRole::PRIMARY);
                return true;
            }

            bool isDeterministicPrimaryCandidate() const noexcept {
                const auto* self = config_.findById(selfNodeId_);
                if (self == nullptr || !self->coordinatorEligible()) { return false; }

                uint64_t selectedNodeId = 0;
                for (const auto& node : config_.nodes()) {
                    if (!node.coordinatorEligible()) { continue; }
                    if (selectedNodeId == 0 || node.nodeId < selectedNodeId) { selectedNodeId = node.nodeId; }
                }
                return selectedNodeId == selfNodeId_;
            }

            bool manifestNodeLeftAfter(uint64_t nodeId, uint64_t tsUs) const {
                if (!clusterManifest_) { return false; }
                for (const auto& leave : clusterManifest_->nodeLeaves()) {
                    if (leave.nodeId == nodeId && leave.tsUs > tsUs) { return true; }
                }
                return false;
            }

            std::optional<manifest::Manifest::NodeJoinEvent> manifestJoinFor(uint64_t nodeId, uint64_t afterTsUs) const {
                if (!clusterManifest_) { return std::nullopt; }
                std::optional<manifest::Manifest::NodeJoinEvent> latest;
                for (const auto& join : clusterManifest_->nodeJoins()) {
                    if (join.nodeId != nodeId || join.tsUs < afterTsUs) { continue; }
                    if (!latest.has_value() || join.tsUs >= latest->tsUs) { latest = join; }
                }
                return latest;
            }

            std::optional<manifest::Manifest::NodeJoinEvent> latestManifestJoinFor(uint64_t nodeId) const {
                if (!clusterManifest_) { return std::nullopt; }
                std::optional<manifest::Manifest::NodeJoinEvent> latest;
                for (const auto& join : clusterManifest_->nodeJoins()) {
                    if (join.nodeId != nodeId) { continue; }
                    if (!latest.has_value() || join.tsUs >= latest->tsUs) { latest = join; }
                }
                return latest;
            }

            void configurePrimarySelf() {
                const auto* self = config_.findById(selfNodeId_);
                if (self == nullptr) { throw std::runtime_error("ClusterManager: self node is missing from config"); }
                if (!self->coordinatorEligible()) {
                    throw std::runtime_error("ClusterManager: self node is not coordinator-eligible and cannot start as PRIMARY");
                }
                {
                    std::lock_guard lock{primaryMutex_};
                    primaryNodeId_.store(self->nodeId, std::memory_order_release);
                    primaryHost_ = self->host;
                    primaryReplPort_ = self->replPort;
                }
                if (clusterManifest_) { clusterManifest_->primaryLease(self->nodeId, nowUs() + PRIMARY_LEASE_WINDOW_US); }
            }

            void configureReplicaPrimary() {
                uint64_t primaryNodeId = runtimeOptions_.primaryNodeId;
                std::string primaryHost = runtimeOptions_.primaryHost;
                uint16_t primaryReplPort = runtimeOptions_.primaryReplPort;

                if (primaryNodeId == 0) { primaryNodeId = runtimeOptions_.secure.expectedPrimaryNodeId; }
                if (primaryNodeId == 0) {
                    throw std::runtime_error("ClusterManager: REPLICA startup requires primaryNodeId or secure.expectedPrimaryNodeId");
                }
                if (primaryNodeId == selfNodeId_) {
                    throw std::runtime_error("ClusterManager: REPLICA startup cannot target self as primary");
                }

                if (const auto* configured = config_.findById(primaryNodeId); configured != nullptr) {
                    if (primaryHost.empty()) { primaryHost = configured->host; }
                    if (primaryReplPort == 0) { primaryReplPort = configured->replPort; }
                    if (!configured->coordinatorEligible()) {
                        throw std::runtime_error("ClusterManager: configured primary node is not coordinator-eligible");
                    }
                }

                if (primaryHost.empty() || primaryReplPort == 0) {
                    throw std::runtime_error("ClusterManager: REPLICA startup requires a reachable primary host and replication port");
                }

                {
                    std::lock_guard lock{primaryMutex_};
                    primaryNodeId_.store(primaryNodeId, std::memory_order_release);
                    primaryHost_ = std::move(primaryHost);
                    primaryReplPort_ = primaryReplPort;
                }
            }

            void recordSelfJoin() {
                if (!clusterManifest_) { return; }
                const auto* self = config_.findById(selfNodeId_);
                if (self == nullptr) { return; }
                clusterManifest_->nodeJoin(self->nodeId, self->replPort, self->host);
            }

            void recordSelfLeave() {
                if (!clusterManifest_) { return; }
                clusterManifest_->nodeLeave(selfNodeId_);
            }

            ClusterConfig config_;
            uint64_t selfNodeId_;
            ClusterRuntimeOptions runtimeOptions_;
            std::atomic<NodeRole> role_{NodeRole::STANDALONE};
            std::atomic<bool> running_{false};
            std::unique_ptr<manifest::Manifest> clusterManifest_;

            mutable std::mutex primaryMutex_;
            std::atomic<uint64_t> primaryNodeId_{0};
            std::string primaryHost_;
            uint16_t primaryReplPort_ = 0;

            std::mutex callbackMutex_;
            RoleChangeCallback callback_;
    };

    std::unique_ptr<ClusterManager> ClusterManager::create(
        std::filesystem::path dbDir,
        ClusterConfig config,
        uint64_t selfNodeId,
        ClusterRuntimeOptions runtimeOptions
    ) {
        return std::unique_ptr<ClusterManager>(
            new ClusterManager(std::make_unique<Impl>(std::move(dbDir), std::move(config), selfNodeId, std::move(runtimeOptions)))
        );
    }

    ClusterManager::ClusterManager(std::unique_ptr<Impl> impl) : impl_{std::move(impl)} {}
    ClusterManager::~ClusterManager() = default;

    void ClusterManager::setRoleChangeCallback(RoleChangeCallback callback) { impl_->setRoleChangeCallback(std::move(callback)); }
    void ClusterManager::start() { impl_->start(); }
    void ClusterManager::close() { impl_->close(); }
    NodeRole ClusterManager::role() const noexcept { return impl_->role(); }
    uint64_t ClusterManager::selfNodeId() const noexcept { return impl_->selfNodeId(); }
    std::string ClusterManager::primaryHost() const { return impl_->primaryHost(); }
    uint64_t ClusterManager::primaryNodeId() const noexcept { return impl_->primaryNodeId(); }
    uint16_t ClusterManager::primaryReplPort() const { return impl_->primaryReplPort(); }
    bool ClusterManager::isStandalone() const noexcept { return impl_->isStandalone(); }
    std::vector<NodeInfo> ClusterManager::activeNodes() const { return impl_->activeNodes(); }
} // namespace akkaradb::engine::cluster

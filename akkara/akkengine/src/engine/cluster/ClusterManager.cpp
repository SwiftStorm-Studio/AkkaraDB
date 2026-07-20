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
} // namespace akkaradb::engine::cluster

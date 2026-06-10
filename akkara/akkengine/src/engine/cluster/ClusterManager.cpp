/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

// akkengine/src/engine/cluster/ClusterManager.cpp
#include "akk/engine/cluster/ClusterManager.hpp"

#include <atomic>
#include <mutex>
#include <stdexcept>
#include <utility>

namespace akkaradb::engine::cluster {
    namespace {
        const NodeInfo* configuredPrimary(const ClusterConfig& config) noexcept {
            const NodeInfo* best = nullptr;
            for (const auto& node : config.nodes()) {
                if (!node.coordinatorEligible()) { continue; }
                if (best == nullptr || node.nodeId < best->nodeId) { best = &node; }
            }
            return best;
        }
    } // namespace

    class ClusterManager::Impl {
        public:
            Impl(std::filesystem::path dbDir, ClusterConfig config, uint64_t selfNodeId)
                : config_{std::move(config)}, selfNodeId_{selfNodeId} {
                (void)dbDir;
                config_.validate();
                if (!config_.isStandalone() && config_.findById(selfNodeId_) == nullptr) {
                    throw std::runtime_error("ClusterManager: selfNodeId not found in cluster config");
                }
            }

            ~Impl() { close(); }

            void setRoleChangeCallback(RoleChangeCallback callback) {
                std::lock_guard lock{callbackMutex_};
                callback_ = std::move(callback);
            }

            void start() {
                if (config_.isStandalone()) {
                    setRole(NodeRole::STANDALONE);
                    return;
                }
                if (running_.exchange(true)) { return; }
                electRole();
            }

            void close() { running_.store(false); }

            NodeRole role() const noexcept { return role_.load(); }
            uint64_t selfNodeId() const noexcept { return selfNodeId_; }
            bool isStandalone() const noexcept { return config_.isStandalone(); }

            std::string primaryHost() const {
                std::lock_guard lock{primaryMutex_};
                return primaryHost_;
            }

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

            void electRole() {
                const auto* primary = configuredPrimary(config_);
                if (primary == nullptr) { throw std::runtime_error("ClusterManager: cluster config has no coordinator-eligible node"); }

                {
                    std::lock_guard lock{primaryMutex_};
                    primaryHost_ = primary->host;
                    primaryReplPort_ = primary->replPort;
                }

                setRole(primary->nodeId == selfNodeId_ ? NodeRole::PRIMARY : NodeRole::REPLICA);
            }

            ClusterConfig config_;
            uint64_t selfNodeId_;
            std::atomic<NodeRole> role_{NodeRole::STANDALONE};
            std::atomic<bool> running_{false};

            mutable std::mutex primaryMutex_;
            std::string primaryHost_;
            uint16_t primaryReplPort_ = 0;

            std::mutex callbackMutex_;
            RoleChangeCallback callback_;
    };

    std::unique_ptr<ClusterManager> ClusterManager::create(std::filesystem::path dbDir, ClusterConfig config, uint64_t selfNodeId) {
        return std::unique_ptr<ClusterManager>(new ClusterManager(std::make_unique<Impl>(std::move(dbDir), std::move(config), selfNodeId)));
    }

    ClusterManager::ClusterManager(std::unique_ptr<Impl> impl) : impl_{std::move(impl)} {}
    ClusterManager::~ClusterManager() = default;

    void ClusterManager::setRoleChangeCallback(RoleChangeCallback callback) { impl_->setRoleChangeCallback(std::move(callback)); }
    void ClusterManager::start() { impl_->start(); }
    void ClusterManager::close() { impl_->close(); }
    NodeRole ClusterManager::role() const noexcept { return impl_->role(); }
    uint64_t ClusterManager::selfNodeId() const noexcept { return impl_->selfNodeId(); }
    std::string ClusterManager::primaryHost() const { return impl_->primaryHost(); }
    uint16_t ClusterManager::primaryReplPort() const { return impl_->primaryReplPort(); }
    bool ClusterManager::isStandalone() const noexcept { return impl_->isStandalone(); }
} // namespace akkaradb::engine::cluster

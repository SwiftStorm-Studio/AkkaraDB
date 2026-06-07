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
        const NodeInfo* configured_primary(const ClusterConfig& config) noexcept {
            const NodeInfo* best = nullptr;
            for (const auto& node : config.nodes()) {
                if (!node.coordinator_eligible()) { continue; }
                if (best == nullptr || node.node_id < best->node_id) { best = &node; }
            }
            return best;
        }
    } // namespace

    class ClusterManager::Impl {
        public:
            Impl(std::filesystem::path db_dir, ClusterConfig config, uint64_t self_node_id)
                : config_{std::move(config)}, self_node_id_{self_node_id} {
                (void)db_dir;
                config_.validate();
                if (!config_.is_standalone() && config_.find_by_id(self_node_id_) == nullptr) {
                    throw std::runtime_error("ClusterManager: self_node_id not found in cluster config");
                }
            }

            ~Impl() { close(); }

            void set_role_change_callback(RoleChangeCallback callback) {
                std::lock_guard lock{callback_mutex_};
                callback_ = std::move(callback);
            }

            void start() {
                if (config_.is_standalone()) {
                    set_role(NodeRole::Standalone);
                    return;
                }
                if (running_.exchange(true)) { return; }
                elect_role();
            }

            void close() { running_.store(false); }

            NodeRole role() const noexcept { return role_.load(); }
            uint64_t self_node_id() const noexcept { return self_node_id_; }
            bool is_standalone() const noexcept { return config_.is_standalone(); }

            std::string primary_host() const {
                std::lock_guard lock{primary_mutex_};
                return primary_host_;
            }

            uint16_t primary_repl_port() const {
                std::lock_guard lock{primary_mutex_};
                return primary_repl_port_;
            }

        private:
            void set_role(NodeRole role) {
                const NodeRole old = role_.exchange(role);
                if (old == role) { return; }
                RoleChangeCallback cb;
                {
                    std::lock_guard lock{callback_mutex_};
                    cb = callback_;
                }
                if (cb) { cb(role); }
            }

            void elect_role() {
                const auto* primary = configured_primary(config_);
                if (primary == nullptr) { throw std::runtime_error("ClusterManager: cluster config has no coordinator-eligible node"); }

                {
                    std::lock_guard lock{primary_mutex_};
                    primary_host_ = primary->host;
                    primary_repl_port_ = primary->repl_port;
                }

                set_role(primary->node_id == self_node_id_ ? NodeRole::Primary : NodeRole::Replica);
            }

            ClusterConfig config_;
            uint64_t self_node_id_;
            std::atomic<NodeRole> role_{NodeRole::Standalone};
            std::atomic<bool> running_{false};

            mutable std::mutex primary_mutex_;
            std::string primary_host_;
            uint16_t primary_repl_port_ = 0;

            std::mutex callback_mutex_;
            RoleChangeCallback callback_;
    };

    std::unique_ptr<ClusterManager> ClusterManager::create(std::filesystem::path db_dir, ClusterConfig config, uint64_t self_node_id) {
        return std::unique_ptr<ClusterManager>(
            new ClusterManager(std::make_unique<Impl>(std::move(db_dir), std::move(config), self_node_id))
        );
    }

    ClusterManager::ClusterManager(std::unique_ptr<Impl> impl) : impl_{std::move(impl)} {}
    ClusterManager::~ClusterManager() = default;

    void ClusterManager::set_role_change_callback(RoleChangeCallback callback) { impl_->set_role_change_callback(std::move(callback)); }
    void ClusterManager::start() { impl_->start(); }
    void ClusterManager::close() { impl_->close(); }
    NodeRole ClusterManager::role() const noexcept { return impl_->role(); }
    uint64_t ClusterManager::self_node_id() const noexcept { return impl_->self_node_id(); }
    std::string ClusterManager::primary_host() const { return impl_->primary_host(); }
    uint16_t ClusterManager::primary_repl_port() const { return impl_->primary_repl_port(); }
    bool ClusterManager::is_standalone() const noexcept { return impl_->is_standalone(); }
} // namespace akkaradb::engine::cluster

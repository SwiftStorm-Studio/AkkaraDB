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

// akkengine/src/engine/cluster/ClusterRuntime.cpp
#include "akk/engine/cluster/ClusterRuntime.hpp"

#include <mutex>
#include <stdexcept>
#include <utility>

namespace akkaradb::engine::cluster {
    class ClusterRuntime::Impl {
        public:
            Impl(
                std::filesystem::path db_dir,
                ClusterConfig config,
                uint64_t self_node_id,
                ClusterEngineCallbacks callbacks,
                ClusterRuntimeOptions runtime_options
            )
                : config_{std::move(config)},
                  router_{config_},
                  manager_{ClusterManager::create(std::move(db_dir), config_, self_node_id)},
                  self_node_id_{self_node_id},
                  callbacks_{std::move(callbacks)},
                  runtime_options_{std::move(runtime_options)} {
                manager_->set_role_change_callback(
                    [this](NodeRole role) {
                        install_role(role);
                        if (callbacks_.role_change) { callbacks_.role_change(role); }
                    }
                );
            }

            ~Impl() { close(); }

            void start() {
                {
                    std::lock_guard lock{mutex_};
                    if (started_) { return; }
                    started_ = true;
                }
                manager_->start();
                install_role(manager_->role());
            }

            void close() {
                std::lock_guard lock{mutex_};
                stop_replication();
                manager_->close();
                started_ = false;
            }

            NodeRole role() const noexcept { return manager_->role(); }

            const ClusterRouter& router() const noexcept { return router_; }

            void ship_entry(
                uint64_t seq,
                ReplOpType op,
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint8_t record_flags,
                uint64_t source_node_id
            ) {
                std::lock_guard lock{mutex_};
                if (server_) { server_->ship_entry(seq, op, key, value, record_flags, source_node_id); }
            }

            void ship_blob(uint64_t seq, uint64_t blob_id, std::span<const uint8_t> content) {
                std::lock_guard lock{mutex_};
                if (server_) { server_->ship_blob(seq, blob_id, content); }
            }

        private:
            void install_role(NodeRole role) {
                std::lock_guard lock{mutex_};
                stop_replication();

                if (role == NodeRole::Primary) {
                    const auto* self = config_.find_by_id(self_node_id_);
                    if (!self && !config_.is_standalone()) { throw std::runtime_error("ClusterRuntime: self node is missing from config"); }
                    const uint16_t repl_port = self ? self->repl_port : 0;
                    server_ = ReplicationServer::create(repl_port, self_node_id_, callbacks_.get_current_seq, config_.ack_policy(), runtime_options_);
                    server_->start();
                }
                else if (role == NodeRole::Replica) {
                    client_ = ReplicationClient::create(
                        manager_->primary_host(),
                        manager_->primary_repl_port(),
                        self_node_id_,
                        callbacks_.get_last_seq,
                        runtime_options_
                    );
                    client_->set_apply_callback(callbacks_.apply);
                    client_->set_blob_callback(callbacks_.apply_blob);
                    client_->start();
                }
            }

            void stop_replication() {
                if (client_) {
                    client_->close();
                    client_.reset();
                }
                if (server_) {
                    server_->close();
                    server_.reset();
                }
            }

            ClusterConfig config_;
            ClusterRouter router_;
            std::unique_ptr<ClusterManager> manager_;
            uint64_t self_node_id_;
            ClusterEngineCallbacks callbacks_;
            ClusterRuntimeOptions runtime_options_;

            mutable std::mutex mutex_;
            bool started_ = false;
            std::unique_ptr<ReplicationServer> server_;
            std::unique_ptr<ReplicationClient> client_;
    };

    std::unique_ptr<ClusterRuntime> ClusterRuntime::create(
        std::filesystem::path db_dir,
        ClusterConfig config,
        uint64_t self_node_id,
        ClusterEngineCallbacks callbacks,
        ClusterRuntimeOptions runtime_options
    ) {
        return std::unique_ptr<ClusterRuntime>(
            new ClusterRuntime(std::make_unique<Impl>(std::move(db_dir), std::move(config), self_node_id, std::move(callbacks), std::move(runtime_options)))
        );
    }

    ClusterRuntime::ClusterRuntime(std::unique_ptr<Impl> impl) : impl_{std::move(impl)} {}

    ClusterRuntime::~ClusterRuntime() = default;

    void ClusterRuntime::start() { impl_->start(); }

    void ClusterRuntime::close() { impl_->close(); }

    NodeRole ClusterRuntime::role() const noexcept { return impl_->role(); }

    const ClusterRouter& ClusterRuntime::router() const noexcept { return impl_->router(); }

    void ClusterRuntime::ship_entry(
        uint64_t seq,
        ReplOpType op,
        std::span<const uint8_t> key,
        std::span<const uint8_t> value,
        uint8_t record_flags,
        uint64_t source_node_id
    ) { impl_->ship_entry(seq, op, key, value, record_flags, source_node_id); }

    void ClusterRuntime::ship_blob(uint64_t seq, uint64_t blob_id, std::span<const uint8_t> content) { impl_->ship_blob(seq, blob_id, content); }
} // namespace akkaradb::engine::cluster

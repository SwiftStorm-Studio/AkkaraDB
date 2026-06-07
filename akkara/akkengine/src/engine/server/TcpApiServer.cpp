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

// akkengine/src/engine/server/TcpApiServer.cpp
#include "akk/engine/server/TcpApiServer.hpp"

#include "akk/engine/server/ApiFraming.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <thread>

namespace akkaradb::engine::server {
    namespace {
        constexpr uint32_t kMaxValueBytes = 64u * 1024u * 1024u;
        constexpr size_t kReadBufferBytes = 64u * 1024u;

        [[nodiscard]] bool valid_header(const ApiRequestHeader& header) {
            return std::memcmp(header.magic, REQUEST_MAGIC, sizeof(header.magic)) == 0 && header.version == PROTOCOL_VERSION && header.
                val_len <= kMaxValueBytes;
        }

        class BufferedReader {
            public:
                bool read_exact(detail::Connection& connection, uint8_t* out, size_t size) {
                    size_t copied = 0;
                    while (copied < size) {
                        if (pos_ == len_ && !refill(connection)) { return false; }

                        const size_t take = std::min(size - copied, len_ - pos_);
                        std::memcpy(out + copied, buffer_.data() + pos_, take);
                        pos_ += take;
                        copied += take;
                    }
                    return true;
                }

            private:
                bool refill(detail::Connection& connection) {
                    len_ = connection.recv_some(buffer_.data(), buffer_.size());
                    pos_ = 0;
                    return len_ > 0;
                }

                std::array<uint8_t, kReadBufferBytes> buffer_{};
                size_t pos_ = 0;
                size_t len_ = 0;
        };
    }

    TcpApiServer::TcpApiServer(AkkEngine& engine, AkkEngineOptions::ApiOptions options) : engine_{engine}, options_{std::move(options)} {}

    std::unique_ptr<TcpApiServer> TcpApiServer::create(AkkEngine& engine, AkkEngineOptions::ApiOptions options) {
        return std::unique_ptr<TcpApiServer>{new TcpApiServer{engine, std::move(options)}};
    }

    TcpApiServer::~TcpApiServer() { close(); }

    void TcpApiServer::start() {
        listen_socket_ = detail::listen_on(options_.bind_host, options_.tcp_port, "TcpApiServer");
        running_.store(true, std::memory_order_release);
        const uint32_t workers = worker_count();
        worker_threads_.reserve(workers);
        for (uint32_t i = 0; i < workers; ++i) { worker_threads_.emplace_back([this] { worker_loop(); }); }
        accept_thread_ = std::thread([this] { accept_loop(); });
    }

    void TcpApiServer::close() {
        if (!running_.exchange(false, std::memory_order_acq_rel)) { return; }
        detail::shutdown_socket(listen_socket_);
        detail::close_socket(listen_socket_);
        listen_socket_ = detail::BAD_SOCKET_VALUE;
        {
            std::lock_guard lock(queue_mu_);
            for (const detail::socket_t client : pending_clients_) { detail::close_socket(client); }
            pending_clients_.clear();
        }
        {
            std::lock_guard lock(active_mu_);
            for (const detail::socket_t client : active_clients_) { detail::shutdown_socket(client); }
        }
        queue_cv_.notify_all();
        if (accept_thread_.joinable()) { accept_thread_.join(); }
        for (auto& worker : worker_threads_) {
            if (worker.joinable()) { worker.join(); }
        }
        worker_threads_.clear();
    }

    void TcpApiServer::accept_loop() {
        while (running_.load(std::memory_order_acquire)) {
            const detail::socket_t client = ::accept(listen_socket_, nullptr, nullptr);
            if (!detail::socket_ok(client)) { break; }

            enqueue_client(client);
        }
    }

    void TcpApiServer::enqueue_client(detail::socket_t client) {
        if (!running_.load(std::memory_order_acquire)) {
            detail::close_socket(client);
            return;
        }

        {
            std::lock_guard lock(queue_mu_);
            const uint32_t limit = options_.tcp_accept_queue_limit == 0 ? 4096u : options_.tcp_accept_queue_limit;
            if (pending_clients_.size() >= limit) {
                detail::close_socket(client);
                return;
            }
            pending_clients_.push_back(client);
        }
        queue_cv_.notify_one();
    }

    void TcpApiServer::worker_loop() {
        for (;;) {
            detail::socket_t client = detail::BAD_SOCKET_VALUE;
            {
                std::unique_lock lock(queue_mu_);
                queue_cv_.wait(lock, [this] { return !running_.load(std::memory_order_acquire) || !pending_clients_.empty(); });
                if (pending_clients_.empty()) {
                    if (!running_.load(std::memory_order_acquire)) { return; }
                    continue;
                }
                client = pending_clients_.front();
                pending_clients_.pop_front();
            }

            {
                std::lock_guard lock(active_mu_);
                active_clients_.insert(client);
            }

            {
                detail::Connection connection{client};
                if (options_.transport_mode == cluster::TransportMode::TLS) {
                    try { connection.enable_tls(options_.tls); }
                    catch (...) {
                        std::lock_guard lock(active_mu_);
                        active_clients_.erase(client);
                        continue;
                    }
                }
                handle_connection(connection);
                connection.shutdown();
            }

            {
                std::lock_guard lock(active_mu_);
                active_clients_.erase(client);
            }
        }
    }

    void TcpApiServer::handle_connection(detail::Connection& connection) {
        BufferedReader reader;
        std::vector<uint8_t> key_buffer;
        std::vector<uint8_t> value_buffer;
        std::vector<uint8_t> output_buffer;
        std::vector<uint8_t> response_buffer;

        while (running_.load(std::memory_order_relaxed)) {
            ApiRequestHeader header{};
            if (!reader.read_exact(connection, reinterpret_cast<uint8_t*>(&header), sizeof(header))) { break; }

            if (!valid_header(header)) {
                encode_error(header.request_id, response_buffer);
                (void)connection.send_all(response_buffer.data(), response_buffer.size());
                break;
            }

            key_buffer.resize(header.key_len);
            value_buffer.resize(header.val_len);
            if (!key_buffer.empty() && !reader.read_exact(connection, key_buffer.data(), key_buffer.size())) { break; }
            if (!value_buffer.empty() && !reader.read_exact(connection, value_buffer.data(), value_buffer.size())) { break; }

            uint32_t received_crc = 0;
            if (!reader.read_exact(connection, reinterpret_cast<uint8_t*>(&received_crc), sizeof(received_crc))) { break; }
            const std::span<const uint8_t> key{key_buffer.data(), key_buffer.size()};
            const std::span<const uint8_t> value{value_buffer.data(), value_buffer.size()};
            if (received_crc != crc32c(key, value)) {
                encode_error(header.request_id, response_buffer);
                (void)connection.send_all(response_buffer.data(), response_buffer.size());
                break;
            }

            switch (header.opcode) {
                case ApiOp::Put: engine_.put(key, value);
                    encode_response(ApiStatus::Ok, header.request_id, {}, response_buffer);
                    break;
                case ApiOp::Get: output_buffer.clear();
                    if (engine_.get_into(key, output_buffer)) {
                        encode_response(
                            ApiStatus::Ok,
                            header.request_id,
                            std::span<const uint8_t>{output_buffer.data(), output_buffer.size()},
                            response_buffer
                        );
                    }
                    else { encode_response(ApiStatus::NotFound, header.request_id, {}, response_buffer); }
                    break;
                case ApiOp::Remove: engine_.remove(key);
                    encode_response(ApiStatus::Ok, header.request_id, {}, response_buffer);
                    break;
                case ApiOp::GetAt: {
                    if (value_buffer.size() != sizeof(uint64_t)) {
                        encode_error(header.request_id, response_buffer);
                        break;
                    }
                    uint64_t seq = 0;
                    std::memcpy(&seq, value_buffer.data(), sizeof(seq));
                    auto historical_value = engine_.get_at(key, seq);
                    if (historical_value) {
                        encode_response(
                            ApiStatus::Ok,
                            header.request_id,
                            std::span<const uint8_t>{historical_value->data(), historical_value->size()},
                            response_buffer
                        );
                    }
                    else { encode_response(ApiStatus::NotFound, header.request_id, {}, response_buffer); }
                    break;
                }
                default: encode_error(header.request_id, response_buffer);
                    break;
            }

            if (!connection.send_all(response_buffer.data(), response_buffer.size())) { break; }
        }
    }

    uint32_t TcpApiServer::worker_count() const {
        if (options_.tcp_worker_threads > 0) { return options_.tcp_worker_threads; }
        const unsigned hw = std::thread::hardware_concurrency();
        return std::max(2u, hw == 0 ? 4u : hw);
    }
}

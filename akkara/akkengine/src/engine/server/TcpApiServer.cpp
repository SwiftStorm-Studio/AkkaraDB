/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 */

// akkengine/src/engine/server/TcpApiServer.cpp
#include "akk/engine/server/TcpApiServer.hpp"

#include "akk/engine/server/ApiFraming.hpp"

#include <cstring>

namespace akkaradb::engine::server {
    namespace {
        constexpr uint32_t kMaxValueBytes = 64u * 1024u * 1024u;

        [[nodiscard]] bool valid_header(const ApiRequestHeader& header) {
            return std::memcmp(header.magic, REQUEST_MAGIC, sizeof(header.magic)) == 0 && header.version == PROTOCOL_VERSION && header.val_len <=
                kMaxValueBytes;
        }
    }

    TcpApiServer::TcpApiServer(AkkEngine& engine, AkkEngineOptions::ApiOptions options) : engine_{engine}, options_{std::move(options)} {}

    std::unique_ptr<TcpApiServer> TcpApiServer::create(AkkEngine& engine, AkkEngineOptions::ApiOptions options) {
        return std::unique_ptr < TcpApiServer >
        {
            new TcpApiServer{engine, std::move(options)}
        };
    }

    TcpApiServer::~TcpApiServer() { close(); }

    void TcpApiServer::start() {
        listen_socket_ = detail::listen_on(options_.bind_host, options_.tcp_port, "TcpApiServer");
        running_.store(true, std::memory_order_release);
        accept_thread_ = std::thread([this] { accept_loop(); });
    }

    void TcpApiServer::close() {
        if (!running_.exchange(false, std::memory_order_acq_rel)) { return; }
        detail::shutdown_socket(listen_socket_);
        detail::close_socket(listen_socket_);
        listen_socket_ = detail::BAD_SOCKET_VALUE;
        if (accept_thread_.joinable()) { accept_thread_.join(); }
    }

    void TcpApiServer::accept_loop() {
        while (running_.load(std::memory_order_acquire)) {
            const detail::socket_t client = ::accept(listen_socket_, nullptr, nullptr);
            if (!detail::socket_ok(client)) { break; }

            std::thread(
                [this, client] {
                    detail::Connection connection{client};
                    if (options_.transport_mode == cluster::TransportMode::TLS) {
                        try { connection.enable_tls(options_.tls); }
                        catch (...) { return; }
                    }
                    handle_connection(connection);
                    connection.shutdown();
                }
            ).detach();
        }
    }

    void TcpApiServer::handle_connection(detail::Connection& connection) {
        std::vector<uint8_t> key_buffer;
        std::vector<uint8_t> value_buffer;
        std::vector<uint8_t> output_buffer;
        std::vector<uint8_t> response_buffer;

        while (running_.load(std::memory_order_relaxed)) {
            ApiRequestHeader header{};
            if (!connection.recv_all(reinterpret_cast<uint8_t*>(&header), sizeof(header))) { break; }

            if (!valid_header(header)) {
                encode_error(header.request_id, response_buffer);
                (void)connection.send_all(response_buffer.data(), response_buffer.size());
                break;
            }

            key_buffer.resize(header.key_len);
            value_buffer.resize(header.val_len);
            if (!key_buffer.empty() && !connection.recv_all(key_buffer.data(), key_buffer.size())) { break; }
            if (!value_buffer.empty() && !connection.recv_all(value_buffer.data(), value_buffer.size())) { break; }

            uint32_t received_crc = 0;
            if (!connection.recv_all(reinterpret_cast<uint8_t*>(&received_crc), sizeof(received_crc))) { break; }
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
}

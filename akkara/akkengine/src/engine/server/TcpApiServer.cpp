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
#include <limits>
#include <thread>

namespace akkaradb::engine::server {
    namespace {
        constexpr uint32_t kMaxValueBytes = 64u * 1024u * 1024u;
        constexpr uint32_t kDefaultMaxBatchItems = 4096u;
        constexpr size_t kReadBufferBytes = 64u * 1024u;

        [[nodiscard]] bool valid_header(const ApiRequestHeader& header) {
            return std::memcmp(header.magic, REQUEST_MAGIC, sizeof(header.magic)) == 0 && header.version == PROTOCOL_VERSION && header.
                val_len <= kMaxValueBytes;
        }

        [[nodiscard]] bool read_batch_count(std::span<const uint8_t> payload, uint32_t& out) noexcept {
            if (payload.size() < sizeof(uint32_t)) { return false; }
            std::memcpy(&out, payload.data(), sizeof(uint32_t));
            return true;
        }

        void close_client_socket(detail::socket_t client) noexcept {
            detail::shutdown_socket(client);
            detail::close_socket(client);
        }

        struct RequestFrame {
            ApiRequestHeader header{};
            std::span<const uint8_t> key;
            std::span<const uint8_t> value;
            uint32_t received_crc = 0;
            size_t wire_size = 0;
        };

        enum class FrameReadStatus : uint8_t {
            Ok, NeedMore, Closed, Invalid,
        };

        class BufferedInput {
            public:
                BufferedInput() { buffer_.reserve(kReadBufferBytes); }

                FrameReadStatus read_frame(detail::Connection& connection, RequestFrame& out, bool blocking) {
                    if (!ensure(connection, sizeof(ApiRequestHeader), blocking)) {
                        return blocking ? FrameReadStatus::Closed : FrameReadStatus::NeedMore;
                    }

                    std::memcpy(&out.header, buffer_.data() + pos_, sizeof(out.header));
                    if (!valid_header(out.header)) { return FrameReadStatus::Invalid; }

                    const size_t key_len = out.header.key_len;
                    const size_t value_len = out.header.val_len;
                    if (value_len > kMaxValueBytes) { return FrameReadStatus::Invalid; }
                    if (key_len > std::numeric_limits<size_t>::max() - sizeof(ApiRequestHeader) - sizeof(uint32_t) - value_len) {
                        return FrameReadStatus::Invalid;
                    }

                    const size_t total = sizeof(ApiRequestHeader) + key_len + value_len + sizeof(uint32_t);
                    if (!ensure(connection, total, blocking)) { return blocking ? FrameReadStatus::Closed : FrameReadStatus::NeedMore; }

                    const uint8_t* p = buffer_.data() + pos_ + sizeof(ApiRequestHeader);
                    out.key = {p, key_len};
                    p += key_len;
                    out.value = {p, value_len};
                    p += value_len;
                    std::memcpy(&out.received_crc, p, sizeof(out.received_crc));
                    out.wire_size = total;
                    return FrameReadStatus::Ok;
                }

                void consume(size_t size) noexcept {
                    pos_ += size;
                    if (pos_ >= buffer_.size()) {
                        buffer_.clear();
                        pos_ = 0;
                    }
                }

            private:
                [[nodiscard]] size_t available() const noexcept { return buffer_.size() - pos_; }

                bool ensure(detail::Connection& connection, size_t size, bool blocking) {
                    while (available() < size) {
                        if (!blocking) { return false; }
                        if (!refill(connection)) { return false; }
                    }
                    return true;
                }

                void compact() {
                    if (pos_ == 0) { return; }
                    if (pos_ >= buffer_.size()) {
                        buffer_.clear();
                        pos_ = 0;
                        return;
                    }

                    std::memmove(buffer_.data(), buffer_.data() + pos_, buffer_.size() - pos_);
                    buffer_.resize(buffer_.size() - pos_);
                    pos_ = 0;
                }

                bool refill(detail::Connection& connection) {
                    compact();
                    const size_t offset = buffer_.size();
                    buffer_.resize(offset + kReadBufferBytes);
                    const size_t got = connection.recv_some(buffer_.data() + offset, kReadBufferBytes);
                    if (got == 0) {
                        buffer_.resize(offset);
                        return false;
                    }
                    buffer_.resize(offset + got);
                    return true;
                }

                std::vector<uint8_t> buffer_;
                size_t pos_ = 0;
        };
    }

    TcpApiServer::TcpApiServer(AkkEngine& engine, AkkEngineOptions::ApiOptions options) : engine_{engine}, options_{std::move(options)} {}

    std::unique_ptr<TcpApiServer> TcpApiServer::create(AkkEngine& engine, AkkEngineOptions::ApiOptions options) {
        return std::unique_ptr<TcpApiServer>{new TcpApiServer{engine, std::move(options)}};
    }

    TcpApiServer::~TcpApiServer() { close(); }

    void TcpApiServer::start() {
        if (running_.load(std::memory_order_acquire)) { return; }
        listen_socket_ = detail::listen_on(options_.bind_host, options_.tcp_port, "TcpApiServer", detail::make_socket_tuning(options_));
        try {
            const uint32_t workers = worker_count();
            worker_threads_.reserve(workers);
            running_.store(true, std::memory_order_release);
            for (uint32_t i = 0; i < workers; ++i) { worker_threads_.emplace_back([this] { worker_loop(); }); }
            accept_thread_ = std::thread([this] { accept_loop(); });
        }
        catch (...) {
            running_.store(false, std::memory_order_release);
            detail::shutdown_socket(listen_socket_);
            detail::close_socket(listen_socket_);
            listen_socket_ = detail::BAD_SOCKET_VALUE;
            queue_cv_.notify_all();
            if (accept_thread_.joinable()) { accept_thread_.join(); }
            for (auto& worker : worker_threads_) {
                if (worker.joinable()) { worker.join(); }
            }
            worker_threads_.clear();
            throw;
        }
    }

    void TcpApiServer::close() {
        if (!running_.exchange(false, std::memory_order_acq_rel)) { return; }
        detail::shutdown_socket(listen_socket_);
        detail::close_socket(listen_socket_);
        listen_socket_ = detail::BAD_SOCKET_VALUE;
        {
            std::lock_guard lock(queue_mu_);
            for (const PendingClient& pending : pending_clients_) { close_client_socket(pending.socket); }
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
            if (!detail::socket_ok(client)) {
                if (!running_.load(std::memory_order_acquire)) { break; }
                if (detail::last_accept_error_is_transient()) { continue; }
                break;
            }
            detail::apply_socket_tuning(client, detail::make_socket_tuning(options_), true);
            connections_accepted_total_.fetch_add(1, std::memory_order_relaxed);

            enqueue_client(client);
        }
    }

    void TcpApiServer::enqueue_client(detail::socket_t client) {
        if (!running_.load(std::memory_order_acquire)) {
            close_client_socket(client);
            return;
        }

        std::vector<detail::socket_t> expired;
        {
            std::lock_guard lock(queue_mu_);
            const Clock::time_point now = Clock::now();
            prune_expired_pending_locked(now, expired);
            const uint32_t limit = accept_queue_limit();
            if (pending_clients_.size() >= limit) {
                accept_queue_rejected_total_.fetch_add(1, std::memory_order_relaxed);
                expired.push_back(client);
            }
            else {
                pending_clients_.push_back(PendingClient{client, now});
                record_accept_queue_depth(pending_clients_.size());
            }
        }

        for (const detail::socket_t expired_client : expired) { close_client_socket(expired_client); }
        if (!expired.empty() && expired.back() == client) { return; }
        queue_cv_.notify_one();
    }

    void TcpApiServer::worker_loop() {
        for (;;) {
            detail::socket_t client = detail::BAD_SOCKET_VALUE;
            std::vector<detail::socket_t> expired;
            {
                std::unique_lock lock(queue_mu_);
                for (;;) {
                    queue_cv_.wait(lock, [this] { return !running_.load(std::memory_order_acquire) || !pending_clients_.empty(); });
                    if (!running_.load(std::memory_order_acquire)) { break; }
                    prune_expired_pending_locked(Clock::now(), expired);
                    if (!pending_clients_.empty()) {
                        client = pending_clients_.front().socket;
                        pending_clients_.pop_front();
                        break;
                    }
                }
            }

            for (const detail::socket_t expired_client : expired) { close_client_socket(expired_client); }
            if (!detail::socket_ok(client)) { return; }

            {
                std::lock_guard lock(active_mu_);
                active_clients_.insert(client);
            }
            connections_active_.fetch_add(1, std::memory_order_relaxed);

            {
                detail::Connection connection{client};
                if (options_.transport_mode == cluster::TransportMode::TLS) {
                    try { connection.enable_tls(options_.tls); }
                    catch (...) {
                        std::lock_guard lock(active_mu_);
                        active_clients_.erase(client);
                        connections_active_.fetch_sub(1, std::memory_order_relaxed);
                        connections_closed_total_.fetch_add(1, std::memory_order_relaxed);
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
            connections_active_.fetch_sub(1, std::memory_order_relaxed);
            connections_closed_total_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    void TcpApiServer::handle_connection(detail::Connection& connection) {
        BufferedInput reader;
        std::vector<uint8_t> output_buffer;
        std::vector<uint8_t> response_buffer;
        std::vector<uint8_t> write_buffer;
        std::vector<ApiBatchPutItem> batch_put_items;
        std::vector<AkkEngine::BatchPutEntry> engine_put_items;
        std::vector<std::span<const uint8_t>> batch_get_keys;
        std::vector<AkkEngine::BatchGetResult> engine_get_results;
        std::vector<ApiBatchGetResult> wire_get_results;

        const auto flush_pending = [&]() -> bool {
            if (write_buffer.empty()) { return true; }
            const size_t bytes = write_buffer.size();
            if (!connection.send_all(write_buffer.data(), write_buffer.size())) {
                backpressure_disconnects_total_.fetch_add(1, std::memory_order_relaxed);
                return false;
            }
            bytes_sent_total_.fetch_add(bytes, std::memory_order_relaxed);
            write_buffer.clear();
            return true;
        };

        const auto append_response = [&](const std::vector<uint8_t>& response) -> bool {
            if (response.empty()) { return true; }

            const uint64_t max_pending = options_.tcp_max_pending_response_bytes;
            if (max_pending > 0 && !write_buffer.empty() && write_buffer.size() + response.size() > max_pending) {
                backpressure_flushes_total_.fetch_add(1, std::memory_order_relaxed);
                if (!flush_pending()) { return false; }
            }

            responses_total_.fetch_add(1, std::memory_order_relaxed);
            if (max_pending > 0 && response.size() > max_pending) {
                if (!connection.send_all(response.data(), response.size())) {
                    backpressure_disconnects_total_.fetch_add(1, std::memory_order_relaxed);
                    return false;
                }
                bytes_sent_total_.fetch_add(response.size(), std::memory_order_relaxed);
                return true;
            }

            write_buffer.insert(write_buffer.end(), response.begin(), response.end());
            if (max_pending > 0 && write_buffer.size() >= max_pending) {
                backpressure_flushes_total_.fetch_add(1, std::memory_order_relaxed);
                return flush_pending();
            }
            return true;
        };

        const auto process_frame = [&](const RequestFrame& frame) -> bool {
            requests_total_.fetch_add(1, std::memory_order_relaxed);
            bytes_received_total_.fetch_add(frame.wire_size, std::memory_order_relaxed);

            if (frame.received_crc != crc32c(frame.key, frame.value)) {
                crc_errors_total_.fetch_add(1, std::memory_order_relaxed);
                encode_error(frame.header.request_id, response_buffer);
                return false;
            }

            try {
                switch (frame.header.opcode) {
                case ApiOp::Put:
                    engine_.put(frame.key, frame.value);
                    encode_response(ApiStatus::Ok, frame.header.request_id, {}, response_buffer);
                    break;
                case ApiOp::Get:
                    output_buffer.clear();
                    if (engine_.get_into(frame.key, output_buffer)) {
                        encode_response(
                            ApiStatus::Ok,
                            frame.header.request_id,
                            std::span<const uint8_t>{output_buffer.data(), output_buffer.size()},
                            response_buffer
                        );
                    }
                    else { encode_response(ApiStatus::NotFound, frame.header.request_id, {}, response_buffer); }
                    break;
                case ApiOp::Remove:
                    engine_.remove(frame.key);
                    encode_response(ApiStatus::Ok, frame.header.request_id, {}, response_buffer);
                    break;
                case ApiOp::GetAt: {
                    if (frame.value.size() != sizeof(uint64_t)) {
                        protocol_errors_total_.fetch_add(1, std::memory_order_relaxed);
                        encode_error(frame.header.request_id, response_buffer);
                        return false;
                    }
                    uint64_t seq = 0;
                    std::memcpy(&seq, frame.value.data(), sizeof(seq));
                    auto historical_value = engine_.get_at(frame.key, seq);
                    if (historical_value) {
                        encode_response(
                            ApiStatus::Ok,
                            frame.header.request_id,
                            std::span<const uint8_t>{historical_value->data(), historical_value->size()},
                            response_buffer
                        );
                    }
                    else { encode_response(ApiStatus::NotFound, frame.header.request_id, {}, response_buffer); }
                    break;
                }
                case ApiOp::BatchPut: {
                    uint32_t count = 0;
                    const uint32_t max_batch = max_batch_items();
                    if (frame.header.key_len != 0 || !read_batch_count(frame.value, count) || count > max_batch ||
                        !decode_batch_put(frame.value, max_batch, batch_put_items)) {
                        protocol_errors_total_.fetch_add(1, std::memory_order_relaxed);
                        encode_error(frame.header.request_id, response_buffer);
                        return false;
                    }

                    engine_put_items.clear();
                    engine_put_items.reserve(batch_put_items.size());
                    for (const ApiBatchPutItem& item : batch_put_items) { engine_put_items.push_back({item.key, item.value}); }
                    engine_.put_batch(std::span<const AkkEngine::BatchPutEntry>{engine_put_items.data(), engine_put_items.size()});
                    batch_put_items_total_.fetch_add(batch_put_items.size(), std::memory_order_relaxed);
                    encode_response(ApiStatus::Ok, frame.header.request_id, {}, response_buffer);
                    break;
                }
                case ApiOp::BatchGet: {
                    uint32_t count = 0;
                    const uint32_t max_batch = max_batch_items();
                    if (frame.header.key_len != 0 || !read_batch_count(frame.value, count) || count > max_batch ||
                        !decode_batch_get(frame.value, max_batch, batch_get_keys)) {
                        protocol_errors_total_.fetch_add(1, std::memory_order_relaxed);
                        encode_error(frame.header.request_id, response_buffer);
                        return false;
                    }

                    engine_get_results = engine_.get_batch(
                        std::span<const std::span<const uint8_t>>{batch_get_keys.data(), batch_get_keys.size()}
                    );
                    wire_get_results.clear();
                    wire_get_results.reserve(engine_get_results.size());
                    for (const auto& result : engine_get_results) {
                        wire_get_results.push_back(
                            ApiBatchGetResult{
                                result.found ? ApiStatus::Ok : ApiStatus::NotFound,
                                std::span<const uint8_t>{result.value.data(), result.value.size()}
                            }
                        );
                    }
                    batch_get_items_total_.fetch_add(batch_get_keys.size(), std::memory_order_relaxed);
                    encode_batch_get_response(
                        frame.header.request_id,
                        std::span<const ApiBatchGetResult>{wire_get_results.data(), wire_get_results.size()},
                        response_buffer
                    );
                    break;
                }
                default:
                    protocol_errors_total_.fetch_add(1, std::memory_order_relaxed);
                    encode_error(frame.header.request_id, response_buffer);
                    break;
                }
            }
            catch (...) {
                protocol_errors_total_.fetch_add(1, std::memory_order_relaxed);
                encode_error(frame.header.request_id, response_buffer);
                return false;
            }

            return true;
        };

        const uint32_t pipeline_limit = std::max<uint32_t>(1, options_.tcp_pipeline_batch_limit);
        while (running_.load(std::memory_order_relaxed)) {
            RequestFrame frame;
            FrameReadStatus status = reader.read_frame(connection, frame, true);
            if (status == FrameReadStatus::Closed || status == FrameReadStatus::NeedMore) { break; }
            if (status == FrameReadStatus::Invalid) {
                protocol_errors_total_.fetch_add(1, std::memory_order_relaxed);
                encode_error(frame.header.request_id, response_buffer);
                (void)append_response(response_buffer);
                (void)flush_pending();
                break;
            }

            bool keep_connection = process_frame(frame);
            reader.consume(frame.wire_size);
            if (!append_response(response_buffer)) { break; }

            uint32_t frames_in_batch = 1;
            while (keep_connection && frames_in_batch < pipeline_limit) {
                RequestFrame pipelined;
                status = reader.read_frame(connection, pipelined, false);
                if (status == FrameReadStatus::NeedMore || status == FrameReadStatus::Closed) { break; }
                if (status == FrameReadStatus::Invalid) {
                    protocol_errors_total_.fetch_add(1, std::memory_order_relaxed);
                    encode_error(pipelined.header.request_id, response_buffer);
                    keep_connection = false;
                    (void)append_response(response_buffer);
                    break;
                }

                keep_connection = process_frame(pipelined);
                reader.consume(pipelined.wire_size);
                if (!append_response(response_buffer)) { return; }
                ++frames_in_batch;
            }

            if (frames_in_batch > 1) { pipeline_batches_total_.fetch_add(1, std::memory_order_relaxed); }
            if (!flush_pending() || !keep_connection) { break; }
        }
    }

    uint32_t TcpApiServer::worker_count() const {
        if (options_.tcp_worker_threads > 0) { return options_.tcp_worker_threads; }
        const unsigned hw = std::thread::hardware_concurrency();
        const uint32_t base = hw == 0 ? 4u : hw;
        return std::max(2u, base);
    }

    uint32_t TcpApiServer::accept_queue_limit() const noexcept {
        return options_.tcp_accept_queue_limit == 0 ? 4096u : options_.tcp_accept_queue_limit;
    }

    uint32_t TcpApiServer::accept_queue_timeout_ms() const noexcept {
        return options_.tcp_accept_queue_timeout_ms;
    }

    void TcpApiServer::prune_expired_pending_locked(Clock::time_point now, std::vector<detail::socket_t>& expired) {
        const uint32_t timeout_ms = accept_queue_timeout_ms();
        if (timeout_ms == 0) { return; }

        const auto timeout = std::chrono::milliseconds{timeout_ms};
        while (!pending_clients_.empty() && now - pending_clients_.front().enqueued_at >= timeout) {
            expired.push_back(pending_clients_.front().socket);
            pending_clients_.pop_front();
            accept_queue_expired_total_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    void TcpApiServer::record_accept_queue_depth(size_t depth) noexcept {
        uint64_t observed = accept_queue_peak_depth_.load(std::memory_order_relaxed);
        while (depth > observed && !accept_queue_peak_depth_.compare_exchange_weak(
            observed,
            static_cast<uint64_t>(depth),
            std::memory_order_relaxed,
            std::memory_order_relaxed
        )) {}
    }

    uint32_t TcpApiServer::max_batch_items() const noexcept {
        return options_.tcp_max_batch_items == 0 ? kDefaultMaxBatchItems : options_.tcp_max_batch_items;
    }

    AkkEngineOptions::ApiIoBackend TcpApiServer::resolved_io_backend() const noexcept {
        if (options_.tcp_io_backend != AkkEngineOptions::ApiIoBackend::Auto) { return options_.tcp_io_backend; }
        return AkkEngineOptions::ApiIoBackend::ThreadPool;
    }

    EngineStats::ApiStats TcpApiServer::stats() const noexcept {
        EngineStats::ApiStats out;
        out.enabled = true;
        out.tcp_enabled = true;
        out.tcp_tls_enabled = options_.transport_mode == cluster::TransportMode::TLS;
        out.tcp_io_backend = static_cast<uint8_t>(resolved_io_backend());
        out.tcp_worker_threads = worker_count();
        out.tcp_accept_queue_limit = accept_queue_limit();
        out.tcp_accept_queue_timeout_ms = accept_queue_timeout_ms();
        out.tcp_listen_backlog = options_.tcp_listen_backlog;
        out.tcp_read_timeout_ms = options_.tcp_read_timeout_ms;
        out.tcp_write_timeout_ms = options_.tcp_write_timeout_ms;
        out.tcp_connections_accepted_total = connections_accepted_total_.load(std::memory_order_relaxed);
        out.tcp_connections_closed_total = connections_closed_total_.load(std::memory_order_relaxed);
        out.tcp_connections_active = connections_active_.load(std::memory_order_relaxed);
        {
            std::lock_guard lock(queue_mu_);
            out.tcp_accept_queue_depth = pending_clients_.size();
        }
        out.tcp_accept_queue_peak_depth = accept_queue_peak_depth_.load(std::memory_order_relaxed);
        out.tcp_accept_queue_rejected_total = accept_queue_rejected_total_.load(std::memory_order_relaxed);
        out.tcp_accept_queue_expired_total = accept_queue_expired_total_.load(std::memory_order_relaxed);
        out.tcp_requests_total = requests_total_.load(std::memory_order_relaxed);
        out.tcp_responses_total = responses_total_.load(std::memory_order_relaxed);
        out.tcp_bytes_received_total = bytes_received_total_.load(std::memory_order_relaxed);
        out.tcp_bytes_sent_total = bytes_sent_total_.load(std::memory_order_relaxed);
        out.tcp_protocol_errors_total = protocol_errors_total_.load(std::memory_order_relaxed);
        out.tcp_crc_errors_total = crc_errors_total_.load(std::memory_order_relaxed);
        out.tcp_pipeline_batches_total = pipeline_batches_total_.load(std::memory_order_relaxed);
        out.tcp_backpressure_flushes_total = backpressure_flushes_total_.load(std::memory_order_relaxed);
        out.tcp_backpressure_disconnects_total = backpressure_disconnects_total_.load(std::memory_order_relaxed);
        out.tcp_batch_put_items_total = batch_put_items_total_.load(std::memory_order_relaxed);
        out.tcp_batch_get_items_total = batch_get_items_total_.load(std::memory_order_relaxed);
        return out;
    }
}

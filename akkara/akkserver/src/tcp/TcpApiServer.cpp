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

// akkserver/src/tcp/TcpApiServer.cpp
#include "akk/engine/server/TcpApiServer.hpp"

#include "akk/engine/server/tcp/detail/TcpApiConnection.hpp"

#include <algorithm>
#include <thread>
#include <vector>

namespace akkaradb::engine::server {
    namespace {
        void closeClientSocket(detail::SocketHandle client) noexcept {
            detail::shutdownSocket(client);
            detail::closeSocket(client);
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
        if (running_.load(std::memory_order_acquire)) { return; }
        listenSocket_ = detail::listenOn(options_.bindHost, options_.tcpPort, "TcpApiServer", detail::makeSocketTuning(options_));
        try {
            const uint32_t workers = workerCount();
            workerThreads_.reserve(workers);
            running_.store(true, std::memory_order_release);
            for (uint32_t i = 0; i < workers; ++i) { workerThreads_.emplace_back([this] { workerLoop(); }); }
            acceptThread_ = std::thread([this] { acceptLoop(); });
        }
        catch (...) {
            running_.store(false, std::memory_order_release);
            detail::shutdownSocket(listenSocket_);
            detail::closeSocket(listenSocket_);
            listenSocket_ = detail::BAD_SOCKET_VALUE;
            queueCv_.notify_all();
            if (acceptThread_.joinable()) { acceptThread_.join(); }
            for (auto& worker : workerThreads_) { if (worker.joinable()) { worker.join(); } }
            workerThreads_.clear();
            throw;
        }
    }

    void TcpApiServer::close() {
        if (!running_.exchange(false, std::memory_order_acq_rel)) { return; }
        detail::shutdownSocket(listenSocket_);
        detail::closeSocket(listenSocket_);
        listenSocket_ = detail::BAD_SOCKET_VALUE;
        {
            std::lock_guard lock(queueMu_);
            for (const PendingClient& pending : pendingClients_) { closeClientSocket(pending.socket); }
            pendingClients_.clear();
        }
        {
            std::lock_guard lock(activeMu_);
            for (const detail::SocketHandle client : activeClients_) { detail::shutdownSocket(client); }
        }
        queueCv_.notify_all();
        if (acceptThread_.joinable()) { acceptThread_.join(); }
        for (auto& worker : workerThreads_) { if (worker.joinable()) { worker.join(); } }
        workerThreads_.clear();
    }

    void TcpApiServer::acceptLoop() {
        while (running_.load(std::memory_order_acquire)) {
            const detail::SocketHandle client = ::accept(listenSocket_, nullptr, nullptr);
            if (!detail::socketOk(client)) {
                if (!running_.load(std::memory_order_acquire)) { break; }
                if (detail::lastAcceptErrorIsTransient()) { continue; }
                break;
            }
            detail::applySocketTuning(client, detail::makeSocketTuning(options_), true);
            connectionsAcceptedTotal_.fetch_add(1, std::memory_order_relaxed);
            enqueueClient(client);
        }
    }

    void TcpApiServer::enqueueClient(detail::SocketHandle client) {
        if (!running_.load(std::memory_order_acquire)) {
            closeClientSocket(client);
            return;
        }

        std::vector<detail::SocketHandle> expired;
        {
            std::lock_guard lock(queueMu_);
            const Clock::time_point now = Clock::now();
            pruneExpiredPendingLocked(now, expired);
            const uint32_t limit = acceptQueueLimit();
            if (pendingClients_.size() >= limit) {
                acceptQueueRejectedTotal_.fetch_add(1, std::memory_order_relaxed);
                expired.push_back(client);
            }
            else {
                pendingClients_.push_back(PendingClient{client, now});
                recordAcceptQueueDepth(pendingClients_.size());
            }
        }

        for (const detail::SocketHandle expiredClient : expired) { closeClientSocket(expiredClient); }
        if (!expired.empty() && expired.back() == client) { return; }
        queueCv_.notify_one();
    }

    void TcpApiServer::workerLoop() {
        for (;;) {
            detail::SocketHandle client = detail::BAD_SOCKET_VALUE;
            std::vector<detail::SocketHandle> expired;
            {
                std::unique_lock lock(queueMu_);
                for (;;) {
                    queueCv_.wait(lock, [this] { return !running_.load(std::memory_order_acquire) || !pendingClients_.empty(); });
                    if (!running_.load(std::memory_order_acquire)) { break; }
                    pruneExpiredPendingLocked(Clock::now(), expired);
                    if (!pendingClients_.empty()) {
                        client = pendingClients_.front().socket;
                        pendingClients_.pop_front();
                        break;
                    }
                }
            }

            for (const detail::SocketHandle expiredClient : expired) { closeClientSocket(expiredClient); }
            if (!detail::socketOk(client)) { return; }

            {
                std::lock_guard lock(activeMu_);
                activeClients_.insert(client);
            }
            connectionsActive_.fetch_add(1, std::memory_order_relaxed);

            {
                detail::Connection connection{client};
                if (options_.transportMode == AkkEngineOptions::ApiTransportMode::TLS) {
                    try { connection.enableTls(options_.tls); }
                    catch (...) {
                        std::lock_guard lock(activeMu_);
                        activeClients_.erase(client);
                        connectionsActive_.fetch_sub(1, std::memory_order_relaxed);
                        connectionsClosedTotal_.fetch_add(1, std::memory_order_relaxed);
                        continue;
                    }
                }
                handleConnection(connection);
                connection.shutdown();
            }

            {
                std::lock_guard lock(activeMu_);
                activeClients_.erase(client);
            }
            connectionsActive_.fetch_sub(1, std::memory_order_relaxed);
            connectionsClosedTotal_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    void TcpApiServer::handleConnection(detail::Connection& connection) {
        tcp::ConnectionHandler{
            engine_,
            options_,
            tcp::ConnectionCounters{
                requestsTotal_,
                responsesTotal_,
                bytesReceivedTotal_,
                bytesSentTotal_,
                protocolErrorsTotal_,
                crcErrorsTotal_,
                pipelineBatchesTotal_,
                backpressureFlushesTotal_,
                backpressureDisconnectsTotal_,
                batchPutItemsTotal_,
                batchGetItemsTotal_,
            }
        }.handle(connection, running_);
    }

    uint32_t TcpApiServer::workerCount() const {
        if (options_.tcpWorkerThreads > 0) { return options_.tcpWorkerThreads; }
        const unsigned hw = std::thread::hardware_concurrency();
        const uint32_t base = hw == 0 ? 4u : hw;
        return std::max(2u, base);
    }

    uint32_t TcpApiServer::acceptQueueLimit() const noexcept {
        return options_.tcpAcceptQueueLimit == 0 ? 4096u : options_.tcpAcceptQueueLimit;
    }

    uint32_t TcpApiServer::acceptQueueTimeoutMs() const noexcept { return options_.tcpAcceptQueueTimeoutMs; }

    void TcpApiServer::pruneExpiredPendingLocked(Clock::time_point now, std::vector<detail::SocketHandle>& expired) {
        const uint32_t timeoutMs = acceptQueueTimeoutMs();
        if (timeoutMs == 0) { return; }

        const auto timeout = std::chrono::milliseconds{timeoutMs};
        while (!pendingClients_.empty() && now - pendingClients_.front().enqueuedAt >= timeout) {
            expired.push_back(pendingClients_.front().socket);
            pendingClients_.pop_front();
            acceptQueueExpiredTotal_.fetch_add(1, std::memory_order_relaxed);
        }
        recordAcceptQueueDepth(pendingClients_.size());
    }

    void TcpApiServer::recordAcceptQueueDepth(size_t depth) noexcept {
        uint64_t peak = acceptQueuePeakDepth_.load(std::memory_order_relaxed);
        while (depth > peak && !acceptQueuePeakDepth_.compare_exchange_weak(
            peak,
            depth,
            std::memory_order_relaxed,
            std::memory_order_relaxed
        )) {}
    }

    EngineStats::ApiStats TcpApiServer::stats() const noexcept {
        EngineStats::ApiStats out;
        out.tcpEnabled = true;
        out.tcpTlsEnabled = options_.transportMode == AkkEngineOptions::ApiTransportMode::TLS;
        out.tcpIoBackend = static_cast<uint8_t>(resolvedIoBackend());
        out.tcpWorkerThreads = workerCount();
        out.tcpAcceptQueueLimit = acceptQueueLimit();
        out.tcpAcceptQueueTimeoutMs = acceptQueueTimeoutMs();
        out.tcpListenBacklog = options_.tcpListenBacklog;
        out.tcpReadTimeoutMs = options_.tcpReadTimeoutMs;
        out.tcpWriteTimeoutMs = options_.tcpWriteTimeoutMs;
        out.tcpConnectionsAcceptedTotal = connectionsAcceptedTotal_.load(std::memory_order_relaxed);
        out.tcpConnectionsClosedTotal = connectionsClosedTotal_.load(std::memory_order_relaxed);
        out.tcpConnectionsActive = connectionsActive_.load(std::memory_order_relaxed);
        {
            std::lock_guard lock(queueMu_);
            out.tcpAcceptQueueDepth = static_cast<uint64_t>(pendingClients_.size());
        }
        out.tcpAcceptQueuePeakDepth = acceptQueuePeakDepth_.load(std::memory_order_relaxed);
        out.tcpAcceptQueueRejectedTotal = acceptQueueRejectedTotal_.load(std::memory_order_relaxed);
        out.tcpAcceptQueueExpiredTotal = acceptQueueExpiredTotal_.load(std::memory_order_relaxed);
        out.tcpRequestsTotal = requestsTotal_.load(std::memory_order_relaxed);
        out.tcpResponsesTotal = responsesTotal_.load(std::memory_order_relaxed);
        out.tcpBytesReceivedTotal = bytesReceivedTotal_.load(std::memory_order_relaxed);
        out.tcpBytesSentTotal = bytesSentTotal_.load(std::memory_order_relaxed);
        out.tcpProtocolErrorsTotal = protocolErrorsTotal_.load(std::memory_order_relaxed);
        out.tcpCrcErrorsTotal = crcErrorsTotal_.load(std::memory_order_relaxed);
        out.tcpPipelineBatchesTotal = pipelineBatchesTotal_.load(std::memory_order_relaxed);
        out.tcpBackpressureFlushesTotal = backpressureFlushesTotal_.load(std::memory_order_relaxed);
        out.tcpBackpressureDisconnectsTotal = backpressureDisconnectsTotal_.load(std::memory_order_relaxed);
        out.tcpBatchPutItemsTotal = batchPutItemsTotal_.load(std::memory_order_relaxed);
        out.tcpBatchGetItemsTotal = batchGetItemsTotal_.load(std::memory_order_relaxed);
        return out;
    }

    AkkEngineOptions::ApiIoBackend TcpApiServer::resolvedIoBackend() const noexcept {
        if (options_.tcpIoBackend != AkkEngineOptions::ApiIoBackend::AUTO) { return options_.tcpIoBackend; }
        return AkkEngineOptions::ApiIoBackend::THREAD_POOL;
    }

    extern "C" AKKARADB_API_SERVER_API bool akkaradb_api_tcp_register() noexcept {
        return akkaradb::engine::server::registerAkkApiTransportFactory(
            akkaradb::engine::AkkEngineOptions::ApiBackend::TCP,
            [](
            akkaradb::engine::AkkEngine& engine,
            const akkaradb::engine::AkkEngineOptions::ApiOptions& options
        ) -> std::unique_ptr<akkaradb::engine::server::IAkkApiTransport> {
                return akkaradb::engine::server::TcpApiServer::create(engine, options);
            }
        );
    }
}

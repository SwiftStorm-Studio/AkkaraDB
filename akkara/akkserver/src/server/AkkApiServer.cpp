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

// akkserver/src/server/AkkApiServer.cpp
#include "akk/engine/server/AkkApiServer.hpp"

#include <filesystem>
#include <stdexcept>
#include <utility>

namespace akkaradb::engine::server {
    namespace {
        [[nodiscard]] const char* backendName(AkkEngineOptions::ApiBackend backend) noexcept {
            switch (backend) {
                case AkkEngineOptions::ApiBackend::HTTP: return "HTTP";
                case AkkEngineOptions::ApiBackend::TCP: return "TCP";
                case AkkEngineOptions::ApiBackend::GRPC: return "GRPC";
            }
            return "unknown";
        }

        [[nodiscard]] const std::filesystem::path& backendPath(
            const AkkEngineOptions::ApiOptions& options,
            AkkEngineOptions::ApiBackend backend
        ) noexcept {
            switch (backend) {
                case AkkEngineOptions::ApiBackend::HTTP: return options.httpBackendPath.empty()
                                                                    ? options.transportBackendPath
                                                                    : options.httpBackendPath;
                case AkkEngineOptions::ApiBackend::TCP: return options.tcpBackendPath.empty()
                                                                   ? options.transportBackendPath
                                                                   : options.tcpBackendPath;
                case AkkEngineOptions::ApiBackend::GRPC: return options.grpcBackendPath.empty()
                                                                    ? options.transportBackendPath
                                                                    : options.grpcBackendPath;
            }
            return options.transportBackendPath;
        }
    }

    AkkApiServer::AkkApiServer() = default;

    std::unique_ptr<AkkApiServer> AkkApiServer::create(AkkEngine& engine, const AkkEngineOptions::ApiOptions& options) {
        auto server = std::unique_ptr < AkkApiServer >
        {
            new AkkApiServer()
        };
        auto backends = options.backends;
        if (backends.empty()) {
            #ifdef AKKARADB_API_HAS_HTTP_BACKEND
            backends.push_back(AkkEngineOptions::ApiBackend::HTTP);
            #endif
            #ifdef AKKARADB_API_HAS_TCP_BACKEND
            backends.push_back(AkkEngineOptions::ApiBackend::TCP);
            #endif
            #ifdef AKKARADB_API_HAS_GRPC_BACKEND
            backends.push_back(AkkEngineOptions::ApiBackend::GRPC);
            #endif
        }

        if (backends.empty()) { throw std::runtime_error("AkkApiServer: this build does not contain any API transport backends"); }

        for (const auto backend : backends) {
            if (!akkApiTransportFactoryAvailable(backend) && !loadAkkApiTransportBackend(backend, backendPath(options, backend))) {
                const auto detail = lastAkkApiTransportBackendLoadError(backend);
                throw std::runtime_error(
                    detail.empty()
                        ? std::string{"AkkApiServer: "} + backendName(backend) + " API transport backend library is not available"
                        : std::string{"AkkApiServer: "} + backendName(backend) + " API transport backend library is not available: " +
                        detail
                );
            }
            server->transports_.push_back(createAkkApiTransport(backend, engine, options));
        }
        return server;
    }

    AkkApiServer::~AkkApiServer() { close(); }

    void AkkApiServer::start() {
        try { for (auto& transport : transports_) { transport->start(); } }
        catch (...) {
            close();
            throw;
        }
    }

    void AkkApiServer::close() {
        for (auto& transport : transports_) { if (transport) { transport->close(); } }
        transports_.clear();
    }

    EngineStats::ApiStats AkkApiServer::stats() const noexcept {
        EngineStats::ApiStats out;
        for (const auto& transport : transports_) {
            if (!transport) { continue; }
            const auto next = transport->stats();
            out.enabled = out.enabled || next.enabled;

            if (next.httpEnabled) {
                out.httpEnabled = true;
                out.httpTlsEnabled = next.httpTlsEnabled;
                out.httpPort = next.httpPort;
                out.httpMaxBatchItems = next.httpMaxBatchItems;
                out.httpMaxScanItems = next.httpMaxScanItems;
                out.httpMaxHistoryEntries = next.httpMaxHistoryEntries;
                out.httpMaxContentLength = next.httpMaxContentLength;
                out.httpConnectionsAcceptedTotal = next.httpConnectionsAcceptedTotal;
                out.httpConnectionsClosedTotal = next.httpConnectionsClosedTotal;
                out.httpConnectionsActive = next.httpConnectionsActive;
                out.httpRequestsTotal = next.httpRequestsTotal;
                out.httpResponsesTotal = next.httpResponsesTotal;
                out.httpBytesReceivedTotal = next.httpBytesReceivedTotal;
                out.httpBytesSentTotal = next.httpBytesSentTotal;
                out.httpProtocolErrorsTotal = next.httpProtocolErrorsTotal;
                out.httpErrorsTotal = next.httpErrorsTotal;
                out.httpBatchPutItemsTotal = next.httpBatchPutItemsTotal;
                out.httpBatchGetItemsTotal = next.httpBatchGetItemsTotal;
            }

            if (next.tcpEnabled) {
                out.tcpEnabled = true;
                out.tcpTlsEnabled = next.tcpTlsEnabled;
                out.tcpWorkerThreads = next.tcpWorkerThreads;
                out.tcpAcceptQueueLimit = next.tcpAcceptQueueLimit;
                out.tcpAcceptQueueTimeoutMs = next.tcpAcceptQueueTimeoutMs;
                out.tcpListenBacklog = next.tcpListenBacklog;
                out.tcpReadTimeoutMs = next.tcpReadTimeoutMs;
                out.tcpWriteTimeoutMs = next.tcpWriteTimeoutMs;
                out.tcpConnectionsAcceptedTotal = next.tcpConnectionsAcceptedTotal;
                out.tcpConnectionsClosedTotal = next.tcpConnectionsClosedTotal;
                out.tcpConnectionsActive = next.tcpConnectionsActive;
                out.tcpAcceptQueueDepth = next.tcpAcceptQueueDepth;
                out.tcpAcceptQueuePeakDepth = next.tcpAcceptQueuePeakDepth;
                out.tcpAcceptQueueRejectedTotal = next.tcpAcceptQueueRejectedTotal;
                out.tcpAcceptQueueExpiredTotal = next.tcpAcceptQueueExpiredTotal;
                out.tcpRequestsTotal = next.tcpRequestsTotal;
                out.tcpResponsesTotal = next.tcpResponsesTotal;
                out.tcpBytesReceivedTotal = next.tcpBytesReceivedTotal;
                out.tcpBytesSentTotal = next.tcpBytesSentTotal;
                out.tcpProtocolErrorsTotal = next.tcpProtocolErrorsTotal;
                out.tcpCrcErrorsTotal = next.tcpCrcErrorsTotal;
                out.tcpPipelineBatchesTotal = next.tcpPipelineBatchesTotal;
                out.tcpBackpressureFlushesTotal = next.tcpBackpressureFlushesTotal;
                out.tcpBackpressureDisconnectsTotal = next.tcpBackpressureDisconnectsTotal;
                out.tcpBatchPutItemsTotal = next.tcpBatchPutItemsTotal;
                out.tcpBatchGetItemsTotal = next.tcpBatchGetItemsTotal;
                out.tcpIoBackend = next.tcpIoBackend;
            }

            if (next.grpcEnabled) {
                out.grpcEnabled = true;
                out.grpcTlsEnabled = next.grpcTlsEnabled;
                out.grpcPort = next.grpcPort;
                out.grpcWorkerThreads = next.grpcWorkerThreads;
                out.grpcCompletionQueues = next.grpcCompletionQueues;
                out.grpcMinPollers = next.grpcMinPollers;
                out.grpcMaxPollers = next.grpcMaxPollers;
                out.grpcMaxConcurrentStreams = next.grpcMaxConcurrentStreams;
                out.grpcResourceQuotaBytes = next.grpcResourceQuotaBytes;
                out.grpcMaxBatchItems = next.grpcMaxBatchItems;
                out.grpcMaxScanItems = next.grpcMaxScanItems;
                out.grpcMaxHistoryEntries = next.grpcMaxHistoryEntries;
                out.grpcRequestsTotal = next.grpcRequestsTotal;
                out.grpcResponsesTotal = next.grpcResponsesTotal;
                out.grpcActiveRequests = next.grpcActiveRequests;
                out.grpcErrorsTotal = next.grpcErrorsTotal;
                out.grpcBatchPutItemsTotal = next.grpcBatchPutItemsTotal;
                out.grpcBatchGetItemsTotal = next.grpcBatchGetItemsTotal;
            }
        }
        out.enabled = out.enabled || !transports_.empty();
        return out;
    }
}

extern "C" AKKARADB_API_SERVER_API bool akkaradb_api_server_register() noexcept {
    return akkaradb::engine::server::registerAkkApiServerFactory(
        [](akkaradb::engine::AkkEngine& engine, const akkaradb::engine::AkkEngineOptions::ApiOptions& options) {
            return std::unique_ptr<akkaradb::engine::server::IAkkApiServer>{
                akkaradb::engine::server::AkkApiServer::create(engine, options).release()
            };
        }
    );
}

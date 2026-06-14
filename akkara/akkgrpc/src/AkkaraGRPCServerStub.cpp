#include "akkaradb/grpc/AkkaraGRPCServer.hpp"

#include "akk/engine/server/AkkApiTransportProvider.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

namespace akkaradb::grpcapi {
    class AkkaraGRPCServer::Impl {
        public:
            Impl(engine::AkkEngine& engine, AkkaraGRPCServerOptions options) : engine_{engine}, options_{std::move(options)} {}

            void start() {
                throw std::runtime_error("AkkaraGRPCServer: this build was configured without Protobuf/gRPC support");
            }

            void close() noexcept {}

            [[nodiscard]] AkkaraGRPCServerStats stats() const noexcept {
                AkkaraGRPCServerStats out;
                out.running = false;
                out.tlsEnabled = !options_.certPath.empty();
                out.port = options_.port;
                return out;
            }

        private:
            engine::AkkEngine& engine_;
            AkkaraGRPCServerOptions options_;
    };

    std::unique_ptr<AkkaraGRPCServer> AkkaraGRPCServer::create(engine::AkkEngine& engine, AkkaraGRPCServerOptions options) {
        return std::unique_ptr<AkkaraGRPCServer>{new AkkaraGRPCServer{std::make_unique<Impl>(engine, std::move(options))}};
    }

    AkkaraGRPCServer::AkkaraGRPCServer(std::unique_ptr<Impl> impl) : impl_{std::move(impl)} {}

    AkkaraGRPCServer::~AkkaraGRPCServer() { close(); }

    void AkkaraGRPCServer::start() { impl_->start(); }

    void AkkaraGRPCServer::close() { if (impl_) { impl_->close(); } }

    AkkaraGRPCServerStats AkkaraGRPCServer::stats() const noexcept { return impl_ ? impl_->stats() : AkkaraGRPCServerStats{}; }
}

namespace {
    [[nodiscard]] akkaradb::grpcapi::AkkaraGRPCServerOptions makeGRPCOptions(
        const akkaradb::engine::AkkEngineOptions::ApiOptions& options
    ) {
        akkaradb::grpcapi::AkkaraGRPCServerOptions grpcOptions;
        grpcOptions.bindHost = options.bindHost;
        grpcOptions.port = options.grpcPort;
        grpcOptions.maxReceiveMessageBytes = static_cast<int>(options.tcpMaxPendingResponseBytes == 0
            ? 64ULL * 1024ULL * 1024ULL
            : std::min<uint64_t>(options.tcpMaxPendingResponseBytes, static_cast<uint64_t>(std::numeric_limits<int>::max())));
        grpcOptions.maxSendMessageBytes = grpcOptions.maxReceiveMessageBytes;
        grpcOptions.workerThreads = options.grpcWorkerThreads;
        grpcOptions.completionQueues = options.grpcCompletionQueues;
        grpcOptions.minPollers = options.grpcMinPollers;
        grpcOptions.maxPollers = options.grpcMaxPollers;
        grpcOptions.maxConcurrentStreams = options.grpcMaxConcurrentStreams;
        grpcOptions.resourceQuotaBytes = options.grpcResourceQuotaBytes;
        grpcOptions.maxBatchItems = options.grpcMaxBatchItems;
        grpcOptions.maxScanItems = options.grpcMaxScanItems;
        grpcOptions.maxHistoryEntries = options.grpcMaxHistoryEntries;
        if (options.transportMode == akkaradb::engine::AkkEngineOptions::ApiTransportMode::TLS) {
            grpcOptions.certPath = options.tls.certPath;
            grpcOptions.keyPath = options.tls.keyPath;
            grpcOptions.rootCertPath = options.tls.caPath;
            grpcOptions.requireClientCert = options.tls.verifyPeer && !options.tls.caPath.empty();
        }
        return grpcOptions;
    }

    class GRPCApiTransport final : public akkaradb::engine::server::IAkkApiTransport {
        public:
            GRPCApiTransport(
                akkaradb::engine::AkkEngine& engine,
                const akkaradb::engine::AkkEngineOptions::ApiOptions& options
            )
                : server_{akkaradb::grpcapi::AkkaraGRPCServer::create(engine, makeGRPCOptions(options))} {}

            void start() override { server_->start(); }
            void close() override { if (server_) { server_->close(); } }

            [[nodiscard]] akkaradb::engine::EngineStats::ApiStats stats() const noexcept override {
                akkaradb::engine::EngineStats::ApiStats out;
                if (!server_) { return out; }
                const auto grpcStats = server_->stats();
                out.enabled = grpcStats.running;
                out.grpcEnabled = true;
                out.grpcTlsEnabled = grpcStats.tlsEnabled;
                out.grpcPort = grpcStats.port;
                out.grpcWorkerThreads = grpcStats.workerThreads;
                out.grpcCompletionQueues = grpcStats.completionQueues;
                out.grpcMinPollers = grpcStats.minPollers;
                out.grpcMaxPollers = grpcStats.maxPollers;
                out.grpcMaxConcurrentStreams = grpcStats.maxConcurrentStreams;
                out.grpcResourceQuotaBytes = grpcStats.resourceQuotaBytes;
                out.grpcMaxBatchItems = grpcStats.maxBatchItems;
                out.grpcMaxScanItems = grpcStats.maxScanItems;
                out.grpcMaxHistoryEntries = grpcStats.maxHistoryEntries;
                out.grpcRequestsTotal = grpcStats.requestsTotal;
                out.grpcResponsesTotal = grpcStats.responsesTotal;
                out.grpcActiveRequests = grpcStats.activeRequests;
                out.grpcErrorsTotal = grpcStats.errorsTotal;
                out.grpcBatchPutItemsTotal = grpcStats.batchPutItemsTotal;
                out.grpcBatchGetItemsTotal = grpcStats.batchGetItemsTotal;
                return out;
            }

        private:
            std::unique_ptr<akkaradb::grpcapi::AkkaraGRPCServer> server_;
    };
}

extern "C" AKKARADB_GRPC_API bool akkaradb_api_grpc_register() noexcept {
    return akkaradb::engine::server::registerAkkApiTransportFactory(
        akkaradb::engine::AkkEngineOptions::ApiBackend::GRPC,
        [](akkaradb::engine::AkkEngine& engine, const akkaradb::engine::AkkEngineOptions::ApiOptions& options)
            -> std::unique_ptr<akkaradb::engine::server::IAkkApiTransport> {
            return std::make_unique<GRPCApiTransport>(engine, options);
        }
    );
}

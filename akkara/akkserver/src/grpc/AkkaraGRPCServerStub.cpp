#include "akkaradb/grpc/AkkaraGRPCServer.hpp"

#include <stdexcept>
#include <utility>

namespace akkaradb::grpcapi {
    class AkkaraGRPCServer::Impl {
        public:
            explicit Impl(AkkaraGRPCServerOptions options) : options_{std::move(options)} {}

            void start() { throw std::runtime_error("AkkaraGRPCServer: this build was configured without Protobuf/gRPC support"); }

            void close() noexcept {}

            [[nodiscard]] AkkaraGRPCServerStats stats() const noexcept {
                AkkaraGRPCServerStats out;
                out.running = false;
                out.tlsEnabled = !options_.certPath.empty();
                out.port = options_.port;
                return out;
            }

        private:
            AkkaraGRPCServerOptions options_;
    };

    std::unique_ptr<AkkaraGRPCServer> AkkaraGRPCServer::create(engine::AkkEngine& engine, AkkaraGRPCServerOptions options) {
        (void)engine;
        return std::unique_ptr<AkkaraGRPCServer>{new AkkaraGRPCServer{std::make_unique<Impl>(std::move(options))}};
    }

    AkkaraGRPCServer::AkkaraGRPCServer(std::unique_ptr<Impl> impl) : impl_{std::move(impl)} {}

    AkkaraGRPCServer::~AkkaraGRPCServer() { close(); }

    void AkkaraGRPCServer::start() { impl_->start(); }

    void AkkaraGRPCServer::close() { if (impl_) { impl_->close(); } }

    AkkaraGRPCServerStats AkkaraGRPCServer::stats() const noexcept { return impl_ ? impl_->stats() : AkkaraGRPCServerStats{}; }
}

extern "C" AKKARADB_GRPC_API bool akkaradb_api_grpc_register() noexcept { return false; }

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

// benchmarks/api/apiServerTest.cpp
#include "TestErrorHandlers.hpp"

#include "akk/engine/AkkEngine.hpp"

#include <cstring>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <span>
#include <string>
#include <string_view>

using namespace akkaradb::engine;

namespace {
    [[nodiscard]] std::span<const uint8_t> bytes(std::string_view value) {
        return {reinterpret_cast<const uint8_t*>(value.data()), value.size()};
    }

    [[nodiscard]] bool hasFlag(int argc, char** argv, const char* flag) {
        for (int i = 1; i < argc; ++i) {
            if (std::strcmp(argv[i], flag) == 0) { return true; }
        }
        return false;
    }

    [[nodiscard]] uint16_t uint16Arg(int argc, char** argv, const char* flag, uint16_t fallback) {
        for (int i = 1; i + 1 < argc; ++i) {
            if (std::strcmp(argv[i], flag) == 0) { return static_cast<uint16_t>(std::stoul(argv[i + 1])); }
        }
        return fallback;
    }

    [[nodiscard]] uint32_t uint32Arg(int argc, char** argv, const char* flag, uint32_t fallback) {
        for (int i = 1; i + 1 < argc; ++i) {
            if (std::strcmp(argv[i], flag) == 0) { return static_cast<uint32_t>(std::stoul(argv[i + 1])); }
        }
        return fallback;
    }

    [[nodiscard]] uint64_t uint64Arg(int argc, char** argv, const char* flag, uint64_t fallback) {
        for (int i = 1; i + 1 < argc; ++i) {
            if (std::strcmp(argv[i], flag) == 0) { return static_cast<uint64_t>(std::stoull(argv[i + 1])); }
        }
        return fallback;
    }

    [[nodiscard]] std::string stringArg(int argc, char** argv, const char* flag, std::string fallback) {
        for (int i = 1; i + 1 < argc; ++i) {
            if (std::strcmp(argv[i], flag) == 0) { return argv[i + 1]; }
        }
        return fallback;
    }

    [[nodiscard]] std::filesystem::path defaultDataDir() {
        auto path = std::filesystem::temp_directory_path() / "akkaradbApiServerTest";
        std::error_code ec;
        std::filesystem::create_directories(path, ec);
        return path;
    }
}

int main(int argc, char** argv) {
    akkaradb::test::installMsvcTestErrorHandlers();

    const bool wantHttp = hasFlag(argc, argv, "--http");
    const bool wantTcp = hasFlag(argc, argv, "--tcp");
    const bool wantBoth = hasFlag(argc, argv, "--both");
    const bool tls = hasFlag(argc, argv, "--tls");

    const uint16_t httpPort = uint16Arg(argc, argv, "--http-port", 7070);
    const uint16_t tcpPort = uint16Arg(argc, argv, "--tcp-port", 7071);
    const uint32_t tcpWorkerThreads = uint32Arg(argc, argv, "--tcp-worker-threads", 0);
    const uint32_t tcpAcceptQueueLimit = uint32Arg(argc, argv, "--tcp-accept-queue-limit", 4096);
    const uint32_t tcpAcceptQueueTimeoutMs = uint32Arg(argc, argv, "--tcp-accept-queue-timeout-ms", 60000);
    const uint32_t tcpListenBacklog = uint32Arg(argc, argv, "--tcp-listen-backlog", 1024);
    const uint32_t tcpPipelineBatchLimit = uint32Arg(argc, argv, "--tcp-pipeline-batch-limit", 64);
    const uint32_t tcpMaxBatchItems = uint32Arg(argc, argv, "--tcp-max-batch-items", 4096);
    const uint64_t tcpMaxPendingResponseBytes = uint64Arg(argc, argv, "--tcp-max-pending-response-bytes", 8ULL * 1024ULL * 1024ULL);
    const uint32_t tcpReadTimeoutMs = uint32Arg(argc, argv, "--tcp-read-timeout-ms", 60000);
    const uint32_t tcpWriteTimeoutMs = uint32Arg(argc, argv, "--tcp-write-timeout-ms", 30000);
    const std::string bindHost = stringArg(argc, argv, "--bind", "127.0.0.1");
    const std::string dataDirArg = stringArg(argc, argv, "--data-dir", defaultDataDir().string());

    AkkEngineOptions options;
    options.paths.dataDir = dataDirArg;
    options.components.walEnabled = false;
    options.components.blobEnabled = false;
    options.components.manifestEnabled = false;
    options.components.sstEnabled = false;
    options.components.versionLogEnabled = true;
    options.components.apiEnabled = true;
    options.api.bindHost = bindHost;
    options.api.httpPort = httpPort;
    options.api.tcpPort = tcpPort;
    options.api.tcpWorkerThreads = tcpWorkerThreads;
    options.api.tcpAcceptQueueLimit = tcpAcceptQueueLimit;
    options.api.tcpAcceptQueueTimeoutMs = tcpAcceptQueueTimeoutMs;
    options.api.tcpListenBacklog = tcpListenBacklog;
    options.api.tcpPipelineBatchLimit = tcpPipelineBatchLimit;
    options.api.tcpMaxBatchItems = tcpMaxBatchItems;
    options.api.tcpMaxPendingResponseBytes = tcpMaxPendingResponseBytes;
    options.api.tcpReadTimeoutMs = tcpReadTimeoutMs;
    options.api.tcpWriteTimeoutMs = tcpWriteTimeoutMs;
    options.api.transportMode = tls ? cluster::TransportMode::TLS : cluster::TransportMode::PLAIN;
    options.api.backends.clear();

    const bool useHttp = wantBoth || wantHttp || !wantTcp;
    const bool useTcp = wantBoth || wantTcp;
    if (useHttp) { options.api.backends.push_back(AkkEngineOptions::ApiBackend::HTTP); }
    if (useTcp) { options.api.backends.push_back(AkkEngineOptions::ApiBackend::TCP); }

    auto engine = AkkEngine::open(options);
    engine->put(bytes("hello"), bytes("world"));
    engine->put(bytes("akkaradb"), bytes("specv5 api server"));

    std::printf("\nAkkaraDB API server is running on %s\n", bindHost.c_str());
    if (useHttp) {
        std::printf("  HTTP %s://%s:%u\n", tls ? "https" : "http", bindHost.c_str(), httpPort);
        if (!tls) {
            std::printf("  curl -s -X POST \"http://%s:%u/v1/put?key=foo\" --data \"bar\"\n", bindHost.c_str(), httpPort);
            std::printf("  curl -s \"http://%s:%u/v1/get?key=hello\"\n", bindHost.c_str(), httpPort);
            std::printf("  curl -s -X DELETE \"http://%s:%u/v1/remove?key=hello\"\n", bindHost.c_str(), httpPort);
            std::printf("  curl -s \"http://%s:%u/v1/ping\"\n", bindHost.c_str(), httpPort);
        }
    }
    if (useTcp) {
        std::printf("  TCP  %s:%u (%s)\n", bindHost.c_str(), tcpPort, tls ? "TLS AK5 protocol" : "Plain AK5 protocol");
        std::printf(
            "       workers=%u maxBatchItems=%u pipelineBatchLimit=%u acceptQueueLimit=%u\n",
            tcpWorkerThreads,
            tcpMaxBatchItems,
            tcpPipelineBatchLimit,
            tcpAcceptQueueLimit
        );
        std::printf("       acceptQueueTimeoutMs=%u\n", tcpAcceptQueueTimeoutMs);
        std::printf("       readTimeoutMs=%u writeTimeoutMs=%u\n", tcpReadTimeoutMs, tcpWriteTimeoutMs);
    }
    std::printf("\nType \"stop\" and press Enter to shut down.\n\n");

    std::string line;
    while (std::getline(std::cin, line)) {
        if (line == "stop") { break; }
        std::printf("type \"stop\" to exit\n");
    }

    engine->close();
    return 0;
}

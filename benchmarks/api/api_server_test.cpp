/*
 * AkkaraDB - API server interactive runner
 */

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

    [[nodiscard]] bool has_flag(int argc, char** argv, const char* flag) {
        for (int i = 1; i < argc; ++i) {
            if (std::strcmp(argv[i], flag) == 0) { return true; }
        }
        return false;
    }

    [[nodiscard]] uint16_t uint16_arg(int argc, char** argv, const char* flag, uint16_t fallback) {
        for (int i = 1; i + 1 < argc; ++i) {
            if (std::strcmp(argv[i], flag) == 0) { return static_cast<uint16_t>(std::stoul(argv[i + 1])); }
        }
        return fallback;
    }

    [[nodiscard]] std::string string_arg(int argc, char** argv, const char* flag, std::string fallback) {
        for (int i = 1; i + 1 < argc; ++i) {
            if (std::strcmp(argv[i], flag) == 0) { return argv[i + 1]; }
        }
        return fallback;
    }

    [[nodiscard]] std::filesystem::path default_data_dir() {
        auto path = std::filesystem::temp_directory_path() / "akkaradb_api_server_test";
        std::error_code ec;
        std::filesystem::create_directories(path, ec);
        return path;
    }
}

int main(int argc, char** argv) {
    const bool want_http = has_flag(argc, argv, "--http");
    const bool want_tcp = has_flag(argc, argv, "--tcp");
    const bool want_both = has_flag(argc, argv, "--both");
    const bool tls = has_flag(argc, argv, "--tls");

    const uint16_t http_port = uint16_arg(argc, argv, "--http-port", 7070);
    const uint16_t tcp_port = uint16_arg(argc, argv, "--tcp-port", 7071);
    const std::string bind_host = string_arg(argc, argv, "--bind", "127.0.0.1");
    const std::string data_dir_arg = string_arg(argc, argv, "--data-dir", default_data_dir().string());

    AkkEngineOptions options;
    options.paths.data_dir = data_dir_arg;
    options.components.wal_enabled = false;
    options.components.blob_enabled = false;
    options.components.manifest_enabled = false;
    options.components.sst_enabled = false;
    options.components.version_log_enabled = true;
    options.components.api_enabled = true;
    options.api.bind_host = bind_host;
    options.api.http_port = http_port;
    options.api.tcp_port = tcp_port;
    options.api.transport_mode = tls ? cluster::TransportMode::TLS : cluster::TransportMode::Plain;
    options.api.backends.clear();

    const bool use_http = want_both || want_http || !want_tcp;
    const bool use_tcp = want_both || want_tcp;
    if (use_http) { options.api.backends.push_back(AkkEngineOptions::ApiBackend::Http); }
    if (use_tcp) { options.api.backends.push_back(AkkEngineOptions::ApiBackend::Tcp); }

    auto engine = AkkEngine::open(options);
    engine->put(bytes("hello"), bytes("world"));
    engine->put(bytes("akkaradb"), bytes("specv5 api server"));

    std::printf("\nAkkaraDB API server is running on %s\n", bind_host.c_str());
    if (use_http) {
        std::printf("  HTTP %s://%s:%u\n", tls ? "https" : "http", bind_host.c_str(), http_port);
        if (!tls) {
            std::printf("  curl -s -X POST \"http://%s:%u/v1/put?key=foo\" --data \"bar\"\n", bind_host.c_str(), http_port);
            std::printf("  curl -s \"http://%s:%u/v1/get?key=hello\"\n", bind_host.c_str(), http_port);
            std::printf("  curl -s -X DELETE \"http://%s:%u/v1/remove?key=hello\"\n", bind_host.c_str(), http_port);
            std::printf("  curl -s \"http://%s:%u/v1/ping\"\n", bind_host.c_str(), http_port);
        }
    }
    if (use_tcp) { std::printf("  TCP  %s:%u (%s)\n", bind_host.c_str(), tcp_port, tls ? "TLS AK5 protocol" : "Plain AK5 protocol"); }
    std::printf("\nType \"stop\" and press Enter to shut down.\n\n");

    std::string line;
    while (std::getline(std::cin, line)) {
        if (line == "stop") { break; }
        std::printf("type \"stop\" to exit\n");
    }

    engine->close();
    return 0;
}

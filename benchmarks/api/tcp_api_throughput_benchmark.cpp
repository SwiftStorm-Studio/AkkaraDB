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

// benchmarks/api/tcp_api_throughput_benchmark.cpp
#include "TestErrorHandlers.hpp"

#include "akk/engine/AkkEngine.hpp"
#include "akk/engine/server/ApiFraming.hpp"

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

using namespace akkaradb::engine;
using namespace akkaradb::engine::server;

namespace {
    #ifdef _WIN32
    using socket_t = SOCKET;
    constexpr socket_t bad_socket = INVALID_SOCKET;

    void net_init() {
        static std::once_flag once;
        std::call_once(
            once,
            [] {
                WSADATA data{};
                if (::WSAStartup(MAKEWORD(2, 2), &data) != 0) { throw std::runtime_error("WSAStartup failed"); }
            }
        );
    }

    void close_socket(socket_t socket) {
        if (socket != INVALID_SOCKET) { ::closesocket(socket); }
    }
    #else
    using socket_t = int;
    constexpr socket_t bad_socket = -1;
    void net_init() {}
    void close_socket(socket_t socket) {
        if (socket >= 0) { ::close(socket); }
    }
    #endif

    [[nodiscard]] std::span<const uint8_t> bytes(std::string_view value) {
        return {reinterpret_cast<const uint8_t*>(value.data()), value.size()};
    }

    [[nodiscard]] bool socket_ok(socket_t socket) {
        #ifdef _WIN32
        return socket != INVALID_SOCKET;
        #else
        return socket >= 0;
        #endif
    }

    [[nodiscard]] socket_t connect_tcp(const char* host, uint16_t port) {
        net_init();
        addrinfo hints{};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_protocol = IPPROTO_TCP;

        const std::string port_text = std::to_string(port);
        addrinfo* results = nullptr;
        if (::getaddrinfo(host, port_text.c_str(), &hints, &results) != 0) { return bad_socket; }

        socket_t connected = bad_socket;
        for (addrinfo* it = results; it != nullptr; it = it->ai_next) {
            socket_t candidate = ::socket(it->ai_family, it->ai_socktype, it->ai_protocol);
            if (!socket_ok(candidate)) { continue; }
            const int rc = ::connect(
                candidate,
                it->ai_addr,
                #ifdef _WIN32
                static_cast<int>(it->ai_addrlen)
                #else
                it->ai_addrlen
                #endif
            );
            if (rc == 0) {
                connected = candidate;
                break;
            }
            close_socket(candidate);
        }
        ::freeaddrinfo(results);
        return connected;
    }

    bool send_all(socket_t socket, const uint8_t* data, size_t size) {
        size_t sent = 0;
        while (sent < size) {
            #ifdef _WIN32
            const int rc = ::send(socket, reinterpret_cast<const char*>(data + sent), static_cast<int>(size - sent), 0);
            #else
            const ssize_t rc = ::send(socket, data + sent, size - sent, 0);
            #endif
            if (rc <= 0) { return false; }
            sent += static_cast<size_t>(rc);
        }
        return true;
    }

    bool recv_all(socket_t socket, uint8_t* data, size_t size) {
        size_t got = 0;
        while (got < size) {
            #ifdef _WIN32
            const int rc = ::recv(socket, reinterpret_cast<char*>(data + got), static_cast<int>(size - got), 0);
            #else
            const ssize_t rc = ::recv(socket, data + got, size - got, 0);
            #endif
            if (rc <= 0) { return false; }
            got += static_cast<size_t>(rc);
        }
        return true;
    }

    [[nodiscard]] std::vector<uint8_t> make_request(uint32_t request_id, ApiOp op, std::span<const uint8_t> key, std::span<const uint8_t> value = {}) {
        ApiRequestHeader header{};
        std::memcpy(header.magic, REQUEST_MAGIC, sizeof(header.magic));
        header.version = PROTOCOL_VERSION;
        header.opcode = op;
        header.request_id = request_id;
        header.key_len = static_cast<uint16_t>(key.size());
        header.val_len = static_cast<uint32_t>(value.size());

        const uint32_t checksum = crc32c(key, value);
        std::vector<uint8_t> wire(sizeof(header) + key.size() + value.size() + sizeof(checksum));
        uint8_t* p = wire.data();
        std::memcpy(p, &header, sizeof(header));
        p += sizeof(header);
        if (!key.empty()) {
            std::memcpy(p, key.data(), key.size());
            p += key.size();
        }
        if (!value.empty()) {
            std::memcpy(p, value.data(), value.size());
            p += value.size();
        }
        std::memcpy(p, &checksum, sizeof(checksum));
        return wire;
    }

    bool read_response(socket_t socket, uint32_t expected_request_id, std::vector<uint8_t>& value) {
        ApiResponseHeader header{};
        if (!recv_all(socket, reinterpret_cast<uint8_t*>(&header), sizeof(header))) { return false; }
        if (std::memcmp(header.magic, RESPONSE_MAGIC, sizeof(header.magic)) != 0 || header.request_id != expected_request_id) { return false; }

        value.resize(header.val_len);
        if (!value.empty() && !recv_all(socket, value.data(), value.size())) { return false; }

        uint32_t received_crc = 0;
        if (!recv_all(socket, reinterpret_cast<uint8_t*>(&received_crc), sizeof(received_crc))) { return false; }
        if (received_crc != crc32c(std::span<const uint8_t>{value.data(), value.size()})) { return false; }
        return header.status == ApiStatus::Ok;
    }

    [[nodiscard]] std::filesystem::path temp_dir() {
        const auto dir = std::filesystem::temp_directory_path() / "akkaradb_tcp_api_throughput";
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
        std::filesystem::create_directories(dir, ec);
        return dir;
    }

    [[nodiscard]] uint16_t default_port() {
        const auto ticks = std::chrono::steady_clock::now().time_since_epoch().count();
        return static_cast<uint16_t>(31000 + (ticks % 10000));
    }

    [[nodiscard]] size_t size_arg(int argc, char** argv, const char* flag, size_t fallback) {
        const std::string prefix = std::string{flag} + "=";
        for (int i = 1; i < argc; ++i) {
            const std::string_view arg{argv[i]};
            if (arg.starts_with(prefix)) { return static_cast<size_t>(std::stoull(std::string{arg.substr(prefix.size())})); }
            if (arg == flag && i + 1 < argc) { return static_cast<size_t>(std::stoull(argv[i + 1])); }
        }
        return fallback;
    }

    [[nodiscard]] uint16_t port_arg(int argc, char** argv, const char* flag, uint16_t fallback) {
        return static_cast<uint16_t>(size_arg(argc, argv, flag, fallback));
    }

    [[nodiscard]] std::string string_arg(int argc, char** argv, const char* flag, std::string fallback) {
        const std::string prefix = std::string{flag} + "=";
        for (int i = 1; i < argc; ++i) {
            const std::string_view arg{argv[i]};
            if (arg.starts_with(prefix)) { return std::string{arg.substr(prefix.size())}; }
            if (arg == flag && i + 1 < argc) { return argv[i + 1]; }
        }
        return fallback;
    }

    [[nodiscard]] double percentile(std::vector<uint64_t> values, double p) {
        if (values.empty()) { return 0.0; }
        std::sort(values.begin(), values.end());
        const size_t index = std::min(values.size() - 1, static_cast<size_t>((values.size() - 1) * p));
        return static_cast<double>(values[index]) / 1000.0;
    }
}

int main(int argc, char** argv) {
    akkara::test::install_msvc_test_error_handlers();

    const size_t operations = size_arg(argc, argv, "--ops", 100000);
    const size_t clients = std::max<size_t>(1, size_arg(argc, argv, "--clients", std::thread::hardware_concurrency()));
    const size_t payload_size = size_arg(argc, argv, "--payload", 128);
    const size_t keyspace = std::max<size_t>(1, size_arg(argc, argv, "--keyspace", 65536));
    const uint16_t port = port_arg(argc, argv, "--port", default_port());
    const std::string mode = string_arg(argc, argv, "--mode", "mixed");
    if (mode != "get" && mode != "put" && mode != "mixed") {
        std::cerr << "invalid --mode: expected get, put, or mixed\n";
        return 2;
    }

    std::vector<std::string> keys;
    keys.reserve(keyspace);
    for (size_t i = 0; i < keyspace; ++i) { keys.push_back("key-" + std::to_string(i)); }

    std::vector<uint8_t> value(payload_size);
    for (size_t i = 0; i < value.size(); ++i) { value[i] = static_cast<uint8_t>(i); }

    AkkEngineOptions options;
    options.paths.data_dir = temp_dir();
    options.components.wal_enabled = false;
    options.components.blob_enabled = false;
    options.components.manifest_enabled = false;
    options.components.sst_enabled = false;
    options.components.version_log_enabled = false;
    options.components.api_enabled = true;
    options.api.bind_host = "127.0.0.1";
    options.api.tcp_port = port;
    options.api.backends = {AkkEngineOptions::ApiBackend::Tcp};
    options.api.transport_mode = cluster::TransportMode::Plain;
    options.api.tcp_worker_threads = static_cast<uint32_t>(clients);
    options.runtime.writer_threads = static_cast<uint32_t>(clients);

    auto engine = AkkEngine::open(options);
    if (mode == "get" || mode == "mixed") {
        for (const auto& key : keys) { engine->put(bytes(key), std::span<const uint8_t>{value.data(), value.size()}); }
    }

    std::atomic<bool> start{false};
    std::atomic<size_t> ready{0};
    std::atomic<size_t> completed{0};
    std::atomic<size_t> failures{0};
    std::vector<std::thread> threads;
    std::vector<std::vector<uint64_t>> samples(clients);

    const size_t per_client = (operations + clients - 1) / clients;
    const auto bench_start_connect = std::chrono::steady_clock::now();

    for (size_t client_id = 0; client_id < clients; ++client_id) {
        threads.emplace_back(
            [&, client_id] {
                socket_t socket = connect_tcp("127.0.0.1", port);
                if (!socket_ok(socket)) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                    ready.fetch_add(1, std::memory_order_release);
                    return;
                }

                std::vector<uint8_t> response_value;
                std::vector<uint8_t> request;
                auto& local_samples = samples[client_id];
                local_samples.reserve((per_client + 1023) / 1024);

                ready.fetch_add(1, std::memory_order_release);
                while (!start.load(std::memory_order_acquire)) { std::this_thread::yield(); }

                const size_t begin = client_id * per_client;
                const size_t end = std::min(operations, begin + per_client);
                for (size_t i = begin; i < end; ++i) {
                    const auto& key = keys[i % keys.size()];
                    const bool put_op = mode == "put" || (mode == "mixed" && (i & 1u) == 1u);
                    const uint32_t request_id = static_cast<uint32_t>(i + 1);

                    const auto t0 = std::chrono::steady_clock::now();
                    if (put_op) {
                        request = make_request(request_id, ApiOp::Put, bytes(key), std::span<const uint8_t>{value.data(), value.size()});
                    }
                    else {
                        request = make_request(request_id, ApiOp::Get, bytes(key));
                    }

                    if (!send_all(socket, request.data(), request.size()) || !read_response(socket, request_id, response_value)) {
                        failures.fetch_add(1, std::memory_order_relaxed);
                        break;
                    }

                    completed.fetch_add(1, std::memory_order_relaxed);
                    if ((i & 1023u) == 0) {
                        const auto t1 = std::chrono::steady_clock::now();
                        local_samples.push_back(static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count()));
                    }
                }
                close_socket(socket);
            }
        );
    }

    while (ready.load(std::memory_order_acquire) < clients) { std::this_thread::yield(); }
    const auto bench_start = std::chrono::steady_clock::now();
    start.store(true, std::memory_order_release);
    for (auto& thread : threads) { thread.join(); }
    const auto bench_end = std::chrono::steady_clock::now();

    std::vector<uint64_t> all_samples;
    for (auto& sample_set : samples) {
        all_samples.insert(all_samples.end(), sample_set.begin(), sample_set.end());
    }

    const double seconds = std::chrono::duration<double>(bench_end - bench_start).count();
    const double connect_seconds = std::chrono::duration<double>(bench_start - bench_start_connect).count();
    const size_t completed_ops = completed.load(std::memory_order_relaxed);
    const size_t failed = failures.load(std::memory_order_relaxed);

    std::cout << "mode = " << mode << '\n';
    std::cout << "operations = " << operations << '\n';
    std::cout << "clients = " << clients << '\n';
    std::cout << "payload_bytes = " << payload_size << '\n';
    std::cout << "keyspace = " << keyspace << '\n';
    std::cout << "connect_setup_seconds = " << connect_seconds << '\n';
    std::cout << "seconds = " << seconds << '\n';
    std::cout << "completed = " << completed_ops << '\n';
    std::cout << "throughput_ops_sec = " << (seconds > 0.0 ? static_cast<double>(completed_ops) / seconds : 0.0) << '\n';
    std::cout << "failures = " << failed << '\n';
    std::cout << "sampled_latency_us_p50 = " << percentile(all_samples, 0.50) << '\n';
    std::cout << "sampled_latency_us_p95 = " << percentile(all_samples, 0.95) << '\n';
    std::cout << "sampled_latency_us_p99 = " << percentile(all_samples, 0.99) << '\n';

    engine->close();
    return failed == 0 ? 0 : 1;
}

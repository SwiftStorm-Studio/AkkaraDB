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

// benchmarks/api/tcpApiThroughputBenchmark.cpp
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
    using SocketHandle = SOCKET;
    constexpr SocketHandle badSocket = INVALID_SOCKET;

    void netInit() {
        static std::once_flag once;
        std::call_once(
            once,
            [] {
                WSADATA data{};
                if (::WSAStartup(MAKEWORD(2, 2), &data) != 0) { throw std::runtime_error("WSAStartup failed"); }
            }
        );
    }

    void closeSocket(SocketHandle socket) {
        if (socket != INVALID_SOCKET) { ::closesocket(socket); }
    }
    #else
    using SocketHandle = int;
    constexpr SocketHandle badSocket = -1;
    void netInit() {}
    void closeSocket(SocketHandle socket) {
        if (socket >= 0) { ::close(socket); }
    }
    #endif

    [[nodiscard]] std::span<const uint8_t> bytes(std::string_view value) {
        return {reinterpret_cast<const uint8_t*>(value.data()), value.size()};
    }

    [[nodiscard]] bool socketOk(SocketHandle socket) {
        #ifdef _WIN32
        return socket != INVALID_SOCKET;
        #else
        return socket >= 0;
        #endif
    }

    [[nodiscard]] SocketHandle connectTcp(const char* host, uint16_t port) {
        netInit();
        addrinfo hints{};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_protocol = IPPROTO_TCP;

        const std::string portText = std::to_string(port);
        addrinfo* results = nullptr;
        if (::getaddrinfo(host, portText.c_str(), &hints, &results) != 0) { return badSocket; }

        SocketHandle connected = badSocket;
        for (addrinfo* it = results; it != nullptr; it = it->ai_next) {
            SocketHandle candidate = ::socket(it->ai_family, it->ai_socktype, it->ai_protocol);
            if (!socketOk(candidate)) { continue; }
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
            closeSocket(candidate);
        }
        ::freeaddrinfo(results);
        return connected;
    }

    bool sendAll(SocketHandle socket, const uint8_t* data, size_t size) {
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

    bool recvAll(SocketHandle socket, uint8_t* data, size_t size) {
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

    [[nodiscard]] std::vector<uint8_t> makeRequest(uint32_t requestId, ApiOp op, std::span<const uint8_t> key, std::span<const uint8_t> value = {}) {
        ApiRequestHeader header{};
        std::memcpy(header.magic, REQUEST_MAGIC, sizeof(header.magic));
        header.version = PROTOCOL_VERSION;
        header.opcode = op;
        header.requestId = requestId;
        header.keyLen = static_cast<uint16_t>(key.size());
        header.valLen = static_cast<uint32_t>(value.size());

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

    bool readResponse(SocketHandle socket, uint32_t expectedRequestId, std::vector<uint8_t>& value) {
        ApiResponseHeader header{};
        if (!recvAll(socket, reinterpret_cast<uint8_t*>(&header), sizeof(header))) { return false; }
        if (std::memcmp(header.magic, RESPONSE_MAGIC, sizeof(header.magic)) != 0 || header.requestId != expectedRequestId) { return false; }

        value.resize(header.valLen);
        if (!value.empty() && !recvAll(socket, value.data(), value.size())) { return false; }

        uint32_t receivedCrc = 0;
        if (!recvAll(socket, reinterpret_cast<uint8_t*>(&receivedCrc), sizeof(receivedCrc))) { return false; }
        if (receivedCrc != crc32c(std::span<const uint8_t>{value.data(), value.size()})) { return false; }
        return header.status == ApiStatus::OK;
    }

    [[nodiscard]] std::filesystem::path tempDir() {
        const auto dir = std::filesystem::temp_directory_path() / "akkaradbTcpApiThroughput";
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
        std::filesystem::create_directories(dir, ec);
        return dir;
    }

    [[nodiscard]] uint16_t defaultPort() {
        const auto ticks = std::chrono::steady_clock::now().time_since_epoch().count();
        return static_cast<uint16_t>(31000 + (ticks % 10000));
    }

    [[nodiscard]] size_t sizeArg(int argc, char** argv, const char* flag, size_t fallback) {
        const std::string prefix = std::string{flag} + "=";
        for (int i = 1; i < argc; ++i) {
            const std::string_view arg{argv[i]};
            if (arg.starts_with(prefix)) { return static_cast<size_t>(std::stoull(std::string{arg.substr(prefix.size())})); }
            if (arg == flag && i + 1 < argc) { return static_cast<size_t>(std::stoull(argv[i + 1])); }
        }
        return fallback;
    }

    [[nodiscard]] uint16_t portArg(int argc, char** argv, const char* flag, uint16_t fallback) {
        return static_cast<uint16_t>(sizeArg(argc, argv, flag, fallback));
    }

    [[nodiscard]] std::string stringArg(int argc, char** argv, const char* flag, std::string fallback) {
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
    akkaradb::test::installMsvcTestErrorHandlers();

    const size_t operations = sizeArg(argc, argv, "--ops", 100000);
    const size_t clients = std::max<size_t>(1, sizeArg(argc, argv, "--clients", std::thread::hardware_concurrency()));
    const size_t payloadSize = sizeArg(argc, argv, "--payload", 128);
    const size_t keyspace = std::max<size_t>(1, sizeArg(argc, argv, "--keyspace", 65536));
    const uint16_t port = portArg(argc, argv, "--port", defaultPort());
    const std::string mode = stringArg(argc, argv, "--mode", "mixed");
    if (mode != "get" && mode != "put" && mode != "mixed") {
        std::cerr << "invalid --mode: expected get, put, or mixed\n";
        return 2;
    }

    std::vector<std::string> keys;
    keys.reserve(keyspace);
    for (size_t i = 0; i < keyspace; ++i) { keys.push_back("key-" + std::to_string(i)); }

    std::vector<uint8_t> value(payloadSize);
    for (size_t i = 0; i < value.size(); ++i) { value[i] = static_cast<uint8_t>(i); }

    AkkEngineOptions options;
    options.paths.dataDir = tempDir();
    options.components.walEnabled = false;
    options.components.blobEnabled = false;
    options.components.manifestEnabled = false;
    options.components.sstEnabled = false;
    options.components.versionLogEnabled = false;
    options.components.apiEnabled = true;
    options.api.bindHost = "127.0.0.1";
    options.api.tcpPort = port;
    options.api.backends = {AkkEngineOptions::ApiBackend::TCP};
    options.api.transportMode = cluster::TransportMode::PLAIN;
    options.api.tcpWorkerThreads = static_cast<uint32_t>(clients);
    options.runtime.writerThreads = static_cast<uint32_t>(clients);

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

    const size_t perClient = (operations + clients - 1) / clients;
    const auto benchStartConnect = std::chrono::steady_clock::now();

    for (size_t clientId = 0; clientId < clients; ++clientId) {
        threads.emplace_back(
            [&, clientId] {
                SocketHandle socket = connectTcp("127.0.0.1", port);
                if (!socketOk(socket)) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                    ready.fetch_add(1, std::memory_order_release);
                    return;
                }

                std::vector<uint8_t> responseValue;
                std::vector<uint8_t> request;
                auto& localSamples = samples[clientId];
                localSamples.reserve((perClient + 1023) / 1024);

                ready.fetch_add(1, std::memory_order_release);
                while (!start.load(std::memory_order_acquire)) { std::this_thread::yield(); }

                const size_t begin = clientId * perClient;
                const size_t end = std::min(operations, begin + perClient);
                for (size_t i = begin; i < end; ++i) {
                    const auto& key = keys[i % keys.size()];
                    const bool putOp = mode == "put" || (mode == "mixed" && (i & 1u) == 1u);
                    const uint32_t requestId = static_cast<uint32_t>(i + 1);

                    const auto t0 = std::chrono::steady_clock::now();
                    if (putOp) {
                        request = makeRequest(requestId, ApiOp::PUT, bytes(key), std::span<const uint8_t>{value.data(), value.size()});
                    }
                    else {
                        request = makeRequest(requestId, ApiOp::GET, bytes(key));
                    }

                    if (!sendAll(socket, request.data(), request.size()) || !readResponse(socket, requestId, responseValue)) {
                        failures.fetch_add(1, std::memory_order_relaxed);
                        break;
                    }

                    completed.fetch_add(1, std::memory_order_relaxed);
                    if ((i & 1023u) == 0) {
                        const auto t1 = std::chrono::steady_clock::now();
                        localSamples.push_back(static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count()));
                    }
                }
                closeSocket(socket);
            }
        );
    }

    while (ready.load(std::memory_order_acquire) < clients) { std::this_thread::yield(); }
    const auto benchStart = std::chrono::steady_clock::now();
    start.store(true, std::memory_order_release);
    for (auto& thread : threads) { thread.join(); }
    const auto benchEnd = std::chrono::steady_clock::now();

    std::vector<uint64_t> allSamples;
    for (auto& sampleSet : samples) {
        allSamples.insert(allSamples.end(), sampleSet.begin(), sampleSet.end());
    }

    const double seconds = std::chrono::duration<double>(benchEnd - benchStart).count();
    const double connectSeconds = std::chrono::duration<double>(benchStart - benchStartConnect).count();
    const size_t completedOps = completed.load(std::memory_order_relaxed);
    const size_t failed = failures.load(std::memory_order_relaxed);

    std::cout << "mode = " << mode << '\n';
    std::cout << "operations = " << operations << '\n';
    std::cout << "clients = " << clients << '\n';
    std::cout << "payloadBytes = " << payloadSize << '\n';
    std::cout << "keyspace = " << keyspace << '\n';
    std::cout << "connectSetupSeconds = " << connectSeconds << '\n';
    std::cout << "seconds = " << seconds << '\n';
    std::cout << "completed = " << completedOps << '\n';
    std::cout << "throughputOpsSec = " << (seconds > 0.0 ? static_cast<double>(completedOps) / seconds : 0.0) << '\n';
    std::cout << "failures = " << failed << '\n';
    std::cout << "sampledLatencyUsP50 = " << percentile(allSamples, 0.50) << '\n';
    std::cout << "sampledLatencyUsP95 = " << percentile(allSamples, 0.95) << '\n';
    std::cout << "sampledLatencyUsP99 = " << percentile(allSamples, 0.99) << '\n';

    engine->close();
    return failed == 0 ? 0 : 1;
}

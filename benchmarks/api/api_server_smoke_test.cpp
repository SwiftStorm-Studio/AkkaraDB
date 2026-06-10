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

// benchmarks/api/apiServerSmokeTest.cpp
#include "TestErrorHandlers.hpp"

#include "akk/engine/AkkEngine.hpp"
#include "akk/engine/server/ApiFraming.hpp"
#include "akk/net/tls/TlsStream.hpp"

#include <array>
#include <charconv>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

using namespace akkaradb::engine;
using namespace akkaradb::engine::server;

namespace {
    constexpr uint32_t kSmokeIoTimeoutMs = 5000;

    [[nodiscard]] std::span<const uint8_t> bytes(std::string_view value) {
        return {reinterpret_cast<const uint8_t*>(value.data()), value.size()};
    }

    [[nodiscard]] std::string text(std::span<const uint8_t> value) {
        return {reinterpret_cast<const char*>(value.data()), value.size()};
    }

    [[nodiscard]] std::filesystem::path tempDir() {
        const auto dir = std::filesystem::temp_directory_path() / "akkaradbApiServerSmoke";
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
        std::filesystem::create_directories(dir, ec);
        AKK_TEST_CHECK(!ec);
        return dir;
    }

    [[nodiscard]] uint16_t basePort() {
        const auto ticks = std::chrono::steady_clock::now().time_since_epoch().count();
        return static_cast<uint16_t>(25000 + (ticks % 10000));
    }

    void tlsSendAll(akkaradb::net::TlsStream& stream, const uint8_t* data, size_t size) {
        size_t sent = 0;
        while (sent < size) { sent += stream.send(data + sent, size - sent); }
    }

    void tlsRecvAll(akkaradb::net::TlsStream& stream, uint8_t* data, size_t size) {
        size_t got = 0;
        while (got < size) { got += stream.recv(data + got, size - got); }
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

    template <typename T>
    void appendPlain(std::vector<uint8_t>& out, T value) {
        const size_t offset = out.size();
        out.resize(offset + sizeof(T));
        std::memcpy(out.data() + offset, &value, sizeof(T));
    }

    void appendBytes(std::vector<uint8_t>& out, std::span<const uint8_t> bytes) {
        const size_t offset = out.size();
        out.resize(offset + bytes.size());
        if (!bytes.empty()) { std::memcpy(out.data() + offset, bytes.data(), bytes.size()); }
    }

    [[nodiscard]] std::vector<uint8_t> makeBatchPutRequest(
        uint32_t requestId,
        std::span<const std::pair<std::string_view, std::string_view>> items
    ) {
        std::vector<uint8_t> payload;
        appendPlain(payload, static_cast<uint32_t>(items.size()));
        for (const auto& [key, value] : items) {
            appendPlain(payload, static_cast<uint16_t>(key.size()));
            appendPlain(payload, static_cast<uint32_t>(value.size()));
            appendBytes(payload, bytes(key));
            appendBytes(payload, bytes(value));
        }
        return makeRequest(requestId, ApiOp::BATCH_PUT, {}, std::span<const uint8_t>{payload.data(), payload.size()});
    }

    [[nodiscard]] std::vector<uint8_t> makeBatchGetRequest(uint32_t requestId, std::span<const std::string_view> keys) {
        std::vector<uint8_t> payload;
        appendPlain(payload, static_cast<uint32_t>(keys.size()));
        for (const auto key : keys) {
            appendPlain(payload, static_cast<uint16_t>(key.size()));
            appendBytes(payload, bytes(key));
        }
        return makeRequest(requestId, ApiOp::BATCH_GET, {}, std::span<const uint8_t>{payload.data(), payload.size()});
    }

    [[nodiscard]] std::vector<uint8_t> u64Le(uint64_t value) {
        std::vector<uint8_t> out(sizeof(value));
        std::memcpy(out.data(), &value, sizeof(value));
        return out;
    }

    struct TcpResponse {
        ApiStatus status = ApiStatus::ERROR_STATUS;
        uint32_t requestId = 0;
        std::vector<uint8_t> value;
    };

    [[nodiscard]] TcpResponse readResponse(akkaradb::net::TlsStream& stream) {
        ApiResponseHeader header{};
        tlsRecvAll(stream, reinterpret_cast<uint8_t*>(&header), sizeof(header));
        AKK_TEST_CHECK(std::memcmp(header.magic, RESPONSE_MAGIC, sizeof(header.magic)) == 0);

        TcpResponse response;
        response.status = header.status;
        response.requestId = header.requestId;
        response.value.resize(header.valLen);
        if (!response.value.empty()) { tlsRecvAll(stream, response.value.data(), response.value.size()); }

        uint32_t receivedCrc = 0;
        tlsRecvAll(stream, reinterpret_cast<uint8_t*>(&receivedCrc), sizeof(receivedCrc));
        AKK_TEST_CHECK(receivedCrc == crc32c(std::span<const uint8_t>{response.value.data(), response.value.size()}));
        return response;
    }

    struct BatchGetItem {
        ApiStatus status = ApiStatus::ERROR_STATUS;
        std::vector<uint8_t> value;
    };

    [[nodiscard]] std::vector<BatchGetItem> decodeBatchGetResponse(std::span<const uint8_t> payload) {
        size_t pos = 0;
        uint32_t count = 0;
        AKK_TEST_CHECK(payload.size() >= sizeof(count));
        std::memcpy(&count, payload.data(), sizeof(count));
        pos += sizeof(count);

        std::vector<BatchGetItem> out;
        out.reserve(count);
        for (uint32_t i = 0; i < count; ++i) {
            AKK_TEST_CHECK(payload.size() - pos >= sizeof(uint8_t) + sizeof(uint32_t));
            uint8_t status = 0;
            uint32_t valueLen = 0;
            std::memcpy(&status, payload.data() + pos, sizeof(status));
            pos += sizeof(status);
            std::memcpy(&valueLen, payload.data() + pos, sizeof(valueLen));
            pos += sizeof(valueLen);
            AKK_TEST_CHECK(payload.size() - pos >= valueLen);

            BatchGetItem item;
            item.status = static_cast<ApiStatus>(status);
            item.value.assign(payload.begin() + static_cast<std::ptrdiff_t>(pos), payload.begin() + static_cast<std::ptrdiff_t>(pos + valueLen));
            pos += valueLen;
            out.push_back(std::move(item));
        }
        AKK_TEST_CHECK(pos == payload.size());
        return out;
    }

    [[nodiscard]] std::string httpRequest(uint16_t port, std::string_view request) {
        akkaradb::net::TlsStream stream;
        stream.connect("127.0.0.1", port, {}, kSmokeIoTimeoutMs, kSmokeIoTimeoutMs);
        tlsSendAll(stream, reinterpret_cast<const uint8_t*>(request.data()), request.size());

        std::string response;
        std::array<char, 1024> chunk{};
        for (;;) {
            try {
                const size_t got = stream.recv(chunk.data(), chunk.size());
                response.append(chunk.data(), got);
            }
            catch (...) { break; }
            if (response.find("\r\n\r\n") != std::string::npos && response.find("Connection: close") == std::string::npos) {
                const auto headerEnd = response.find("\r\n\r\n");
                const auto cl = response.find("Content-Length:");
                if (cl != std::string::npos && cl < headerEnd) {
                    const auto lineEnd = response.find("\r\n", cl);
                    size_t contentLength = 0;
                    std::string_view value{response.data() + cl + 15, lineEnd - cl - 15};
                    while (!value.empty() && value.front() == ' ') { value.remove_prefix(1); }
                    (void)std::from_chars(value.data(), value.data() + value.size(), contentLength);
                    if (response.size() >= headerEnd + 4 + contentLength) { break; }
                }
            }
        }
        return response;
    }

    void testTcp(uint16_t port, const std::vector<VersionEntry>& history) {
        akkaradb::net::TlsStream stream;
        stream.connect("127.0.0.1", port, {}, kSmokeIoTimeoutMs, kSmokeIoTimeoutMs);

        auto put = makeRequest(1, ApiOp::PUT, bytes("tcp"), bytes("one"));
        tlsSendAll(stream, put.data(), put.size());
        auto putResponse = readResponse(stream);
        AKK_TEST_CHECK(putResponse.status == ApiStatus::OK);
        AKK_TEST_CHECK(putResponse.requestId == 1);

        auto get = makeRequest(2, ApiOp::GET, bytes("tcp"));
        tlsSendAll(stream, get.data(), get.size());
        auto getResponse = readResponse(stream);
        AKK_TEST_CHECK(getResponse.status == ApiStatus::OK);
        AKK_TEST_CHECK(text(getResponse.value) == "one");

        auto getAt = makeRequest(3, ApiOp::GET_AT, bytes("history"), u64Le(history[0].seq));
        tlsSendAll(stream, getAt.data(), getAt.size());
        auto atResponse = readResponse(stream);
        AKK_TEST_CHECK(atResponse.status == ApiStatus::OK);
        AKK_TEST_CHECK(text(atResponse.value) == "v1");

        auto p1 = makeRequest(4, ApiOp::PUT, bytes("p1"), bytes("a"));
        auto p2 = makeRequest(5, ApiOp::PUT, bytes("p2"), bytes("b"));
        tlsSendAll(stream, p1.data(), p1.size());
        tlsSendAll(stream, p2.data(), p2.size());
        auto p1Response = readResponse(stream);
        auto p2Response = readResponse(stream);
        AKK_TEST_CHECK(p1Response.status == ApiStatus::OK);
        AKK_TEST_CHECK(p2Response.status == ApiStatus::OK);

        auto remove = makeRequest(6, ApiOp::REMOVE, bytes("tcp"));
        tlsSendAll(stream, remove.data(), remove.size());
        auto removeResponse = readResponse(stream);
        AKK_TEST_CHECK(removeResponse.status == ApiStatus::OK);

        const std::pair<std::string_view, std::string_view> batchPutItems[] = {
            {"b1", "x"},
            {"b2", "y"},
            {"b3", "z"},
        };
        auto batchPut = makeBatchPutRequest(7, std::span<const std::pair<std::string_view, std::string_view>>{batchPutItems});
        tlsSendAll(stream, batchPut.data(), batchPut.size());
        auto batchPutResponse = readResponse(stream);
        AKK_TEST_CHECK(batchPutResponse.status == ApiStatus::OK);

        const std::string_view batchGetKeys[] = {"b1", "missing", "b2", "b3"};
        auto batchGet = makeBatchGetRequest(8, std::span<const std::string_view>{batchGetKeys});
        tlsSendAll(stream, batchGet.data(), batchGet.size());
        auto batchResponse = readResponse(stream);
        AKK_TEST_CHECK(batchResponse.status == ApiStatus::OK);
        const auto decodedBatch = decodeBatchGetResponse(batchResponse.value);
        AKK_TEST_CHECK(decodedBatch.size() == 4);
        AKK_TEST_CHECK(decodedBatch[0].status == ApiStatus::OK && text(decodedBatch[0].value) == "x");
        AKK_TEST_CHECK(decodedBatch[1].status == ApiStatus::NOT_FOUND);
        AKK_TEST_CHECK(decodedBatch[2].status == ApiStatus::OK && text(decodedBatch[2].value) == "y");
        AKK_TEST_CHECK(decodedBatch[3].status == ApiStatus::OK && text(decodedBatch[3].value) == "z");

        std::vector<uint8_t> pipeline;
        for (uint32_t requestId = 9; requestId < 17; ++requestId) {
            auto request = makeRequest(requestId, ApiOp::GET, bytes("b1"));
            pipeline.insert(pipeline.end(), request.begin(), request.end());
        }
        tlsSendAll(stream, pipeline.data(), pipeline.size());
        for (uint32_t requestId = 9; requestId < 17; ++requestId) {
            auto response = readResponse(stream);
            AKK_TEST_CHECK(response.status == ApiStatus::OK);
            AKK_TEST_CHECK(response.requestId == requestId);
            AKK_TEST_CHECK(text(response.value) == "x");
        }
    }

    void testHttp(uint16_t port) {
        const auto put = httpRequest(
            port,
            "POST /v1/put?key=http HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "Content-Length: 3\r\n"
            "\r\n"
            "two"
        );
        AKK_TEST_CHECK(put.find("204 No Content") != std::string::npos);

        const auto get = httpRequest(
            port,
            "GET /v1/get?key=http HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n"
        );
        AKK_TEST_CHECK(get.find("200 OK") != std::string::npos);
        AKK_TEST_CHECK(get.ends_with("two"));

        const auto ping = httpRequest(
            port,
            "GET /v1/ping HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n"
        );
        AKK_TEST_CHECK(ping.find("200 OK") != std::string::npos);
        AKK_TEST_CHECK(ping.ends_with("pong"));
    }
}

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();

    const uint16_t httpPort = basePort();
    const uint16_t tcpPort = static_cast<uint16_t>(httpPort + 1);

    AkkEngineOptions options;
    options.paths.dataDir = tempDir();
    options.components.blobEnabled = false;
    options.components.manifestEnabled = false;
    options.components.sstEnabled = false;
    options.components.versionLogEnabled = true;
    options.components.apiEnabled = true;
    options.api.bindHost = "127.0.0.1";
    options.api.httpPort = httpPort;
    options.api.tcpPort = tcpPort;
    options.api.tcpReadTimeoutMs = kSmokeIoTimeoutMs;
    options.api.tcpWriteTimeoutMs = kSmokeIoTimeoutMs;

    auto engine = AkkEngine::open(options);
    engine->put(bytes("history"), bytes("v1"));
    engine->put(bytes("history"), bytes("v2"));
    const auto history = engine->history(bytes("history"));
    AKK_TEST_CHECK(history.size() == 2);

    testTcp(tcpPort, history);
    testHttp(httpPort);

    engine->close();
    return 0;
}

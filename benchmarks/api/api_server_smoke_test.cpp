/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// benchmarks/api/api_server_smoke_test.cpp
#include "TestErrorHandlers.hpp"

#include "akk/engine/AkkEngine.hpp"
#include "akk/engine/server/ApiFraming.hpp"
#include "akk/net/tls/TlsStream.hpp"

#include <array>
#include <charconv>
#include <chrono>
#include <cstring>
#include <exception>
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

    [[nodiscard]] std::vector<uint8_t> httpBody(const std::string& response) {
        const auto headerEnd = response.find("\r\n\r\n");
        AKK_TEST_CHECK(headerEnd != std::string::npos);
        return {
            reinterpret_cast<const uint8_t*>(response.data() + headerEnd + 4),
            reinterpret_cast<const uint8_t*>(response.data() + response.size())
        };
    }

    [[nodiscard]] std::string httpRequestWithBody(uint16_t port, std::string_view header, std::span<const uint8_t> body) {
        std::string request{header};
        request.append(reinterpret_cast<const char*>(body.data()), body.size());
        return httpRequest(port, request);
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

        auto ping = makeRequest(17, ApiOp::PING, {});
        tlsSendAll(stream, ping.data(), ping.size());
        auto pingResponse = readResponse(stream);
        AKK_TEST_CHECK(pingResponse.status == ApiStatus::OK);
        AKK_TEST_CHECK(text(pingResponse.value) == "pong");

        auto exists = makeRequest(18, ApiOp::EXISTS, bytes("b1"));
        tlsSendAll(stream, exists.data(), exists.size());
        auto existsResponse = readResponse(stream);
        AKK_TEST_CHECK(existsResponse.status == ApiStatus::OK);
        AKK_TEST_CHECK(existsResponse.value.size() == 1 && existsResponse.value[0] == 1);

        auto count = makeRequest(19, ApiOp::COUNT, bytes("b"), bytes("c"));
        tlsSendAll(stream, count.data(), count.size());
        auto countResponse = readResponse(stream);
        AKK_TEST_CHECK(countResponse.status == ApiStatus::OK);
        AKK_TEST_CHECK(countResponse.value.size() == sizeof(uint64_t));
        uint64_t counted = 0;
        std::memcpy(&counted, countResponse.value.data(), sizeof(counted));
        AKK_TEST_CHECK(counted >= 3);

        std::vector<uint8_t> scanPayload;
        appendPlain(scanPayload, static_cast<uint32_t>(2));
        appendBytes(scanPayload, bytes("c"));
        auto scan = makeRequest(20, ApiOp::SCAN, bytes("b"), std::span<const uint8_t>{scanPayload.data(), scanPayload.size()});
        tlsSendAll(stream, scan.data(), scan.size());
        auto scanResponse = readResponse(stream);
        AKK_TEST_CHECK(scanResponse.status == ApiStatus::OK);
        AKK_TEST_CHECK(scanResponse.value.size() >= sizeof(uint32_t) + sizeof(uint8_t));
        uint32_t scanned = 0;
        std::memcpy(&scanned, scanResponse.value.data(), sizeof(scanned));
        AKK_TEST_CHECK(scanned == 2);
        AKK_TEST_CHECK(scanResponse.value[sizeof(uint32_t)] == 1);

        auto historyRequest = makeRequest(21, ApiOp::HISTORY, bytes("history"));
        tlsSendAll(stream, historyRequest.data(), historyRequest.size());
        auto historyResponse = readResponse(stream);
        AKK_TEST_CHECK(historyResponse.status == ApiStatus::OK);
        AKK_TEST_CHECK(historyResponse.value.size() >= sizeof(uint32_t));
        uint32_t tcpHistoryCount = 0;
        std::memcpy(&tcpHistoryCount, historyResponse.value.data(), sizeof(tcpHistoryCount));
        AKK_TEST_CHECK(tcpHistoryCount >= 2);

        auto forceSync = makeRequest(22, ApiOp::FORCE_SYNC, {});
        tlsSendAll(stream, forceSync.data(), forceSync.size());
        AKK_TEST_CHECK(readResponse(stream).status == ApiStatus::OK);

        auto stats = makeRequest(23, ApiOp::STATS, {});
        tlsSendAll(stream, stats.data(), stats.size());
        auto statsResponse = readResponse(stream);
        AKK_TEST_CHECK(statsResponse.status == ApiStatus::OK);
        AKK_TEST_CHECK(!statsResponse.value.empty());

        std::vector<uint8_t> pipeline;
        for (uint32_t requestId = 25; requestId < 33; ++requestId) {
            auto request = makeRequest(requestId, ApiOp::GET, bytes("b1"));
            pipeline.insert(pipeline.end(), request.begin(), request.end());
        }
        tlsSendAll(stream, pipeline.data(), pipeline.size());
        for (uint32_t requestId = 25; requestId < 33; ++requestId) {
            auto response = readResponse(stream);
            AKK_TEST_CHECK(response.status == ApiStatus::OK);
            AKK_TEST_CHECK(response.requestId == requestId);
            AKK_TEST_CHECK(text(response.value) == "x");
        }
    }

    void testTcpProtocolError(uint16_t port) {
        akkaradb::net::TlsStream stream;
        stream.connect("127.0.0.1", port, {}, kSmokeIoTimeoutMs, kSmokeIoTimeoutMs);

        auto invalidOpcode = makeRequest(100, static_cast<ApiOp>(0x7f), bytes("bad-op"));
        tlsSendAll(stream, invalidOpcode.data(), invalidOpcode.size());

        auto response = readResponse(stream);
        AKK_TEST_CHECK(response.status == ApiStatus::ERROR_STATUS);
        AKK_TEST_CHECK(response.requestId == 100);
        AKK_TEST_CHECK(response.value.empty());

        bool closed = false;
        try {
            auto get = makeRequest(101, ApiOp::GET, bytes("b1"));
            tlsSendAll(stream, get.data(), get.size());
            (void)readResponse(stream);
        }
        catch (...) { closed = true; }
        AKK_TEST_CHECK(closed);
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

        const auto exists = httpRequest(
            port,
            "GET /v1/exists?key=http HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n"
        );
        AKK_TEST_CHECK(exists.find("200 OK") != std::string::npos);
        const auto existsBody = httpBody(exists);
        AKK_TEST_CHECK(existsBody.size() == 1 && existsBody[0] == 1);

        const auto count = httpRequest(
            port,
            "GET /v1/count?start=h&end=i HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n"
        );
        AKK_TEST_CHECK(count.find("200 OK") != std::string::npos);
        const auto countBody = httpBody(count);
        AKK_TEST_CHECK(countBody.size() == sizeof(uint64_t));
        uint64_t httpCount = 0;
        std::memcpy(&httpCount, countBody.data(), sizeof(httpCount));
        AKK_TEST_CHECK(httpCount >= 1);

        const auto scan = httpRequest(
            port,
            "GET /v1/scan?start=h&end=i&limit=1 HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n"
        );
        AKK_TEST_CHECK(scan.find("200 OK") != std::string::npos);
        const auto scanBody = httpBody(scan);
        AKK_TEST_CHECK(scanBody.size() >= sizeof(uint32_t) + sizeof(uint8_t));
        uint32_t httpScanned = 0;
        std::memcpy(&httpScanned, scanBody.data(), sizeof(httpScanned));
        AKK_TEST_CHECK(httpScanned == 1);

        const auto history = httpRequest(
            port,
            "GET /v1/history?key=history HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n"
        );
        AKK_TEST_CHECK(history.find("200 OK") != std::string::npos);
        const auto historyBody = httpBody(history);
        AKK_TEST_CHECK(historyBody.size() >= sizeof(uint32_t) + sizeof(uint8_t));
        uint32_t httpHistoryCount = 0;
        std::memcpy(&httpHistoryCount, historyBody.data(), sizeof(httpHistoryCount));
        AKK_TEST_CHECK(httpHistoryCount >= 2);

        const std::pair<std::string_view, std::string_view> httpBatchPutItems[] = {
            {"hb1", "one"},
            {"hb2", "two"},
        };
        std::vector<uint8_t> batchPutBody;
        appendPlain(batchPutBody, static_cast<uint32_t>(std::size(httpBatchPutItems)));
        for (const auto& [key, value] : httpBatchPutItems) {
            appendPlain(batchPutBody, static_cast<uint32_t>(key.size()));
            appendPlain(batchPutBody, static_cast<uint32_t>(value.size()));
            appendBytes(batchPutBody, bytes(key));
            appendBytes(batchPutBody, bytes(value));
        }
        const std::string batchPutHeader =
            "POST /v1/batchPut HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "Content-Length: " + std::to_string(batchPutBody.size()) + "\r\n"
            "\r\n";
        const auto batchPut = httpRequestWithBody(port, batchPutHeader, batchPutBody);
        AKK_TEST_CHECK(batchPut.find("204 No Content") != std::string::npos);

        const std::string_view httpBatchGetKeys[] = {"hb1", "missing", "hb2"};
        std::vector<uint8_t> batchGetBody;
        appendPlain(batchGetBody, static_cast<uint32_t>(std::size(httpBatchGetKeys)));
        for (const auto key : httpBatchGetKeys) {
            appendPlain(batchGetBody, static_cast<uint32_t>(key.size()));
            appendBytes(batchGetBody, bytes(key));
        }
        const std::string batchGetHeader =
            "POST /v1/batchGet HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "Content-Length: " + std::to_string(batchGetBody.size()) + "\r\n"
            "\r\n";
        const auto batchGet = httpRequestWithBody(port, batchGetHeader, batchGetBody);
        AKK_TEST_CHECK(batchGet.find("200 OK") != std::string::npos);
        const auto decodedBatch = decodeBatchGetResponse(httpBody(batchGet));
        AKK_TEST_CHECK(decodedBatch.size() == 3);
        AKK_TEST_CHECK(decodedBatch[0].status == ApiStatus::OK && text(decodedBatch[0].value) == "one");
        AKK_TEST_CHECK(decodedBatch[1].status == ApiStatus::NOT_FOUND);
        AKK_TEST_CHECK(decodedBatch[2].status == ApiStatus::OK && text(decodedBatch[2].value) == "two");

        const auto forceFlush = httpRequest(
            port,
            "POST /v1/forceFlush HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "Content-Length: 0\r\n"
            "\r\n"
        );
        AKK_TEST_CHECK(forceFlush.find("204 No Content") != std::string::npos);

        const auto stats = httpRequest(
            port,
            "GET /v1/stats HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n"
        );
        AKK_TEST_CHECK(stats.find("200 OK") != std::string::npos);
        AKK_TEST_CHECK(!httpBody(stats).empty());
    }
}

int main() try {
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
    testTcpProtocolError(tcpPort);
    testHttp(httpPort);

    const auto stats = engine->stats();
    AKK_TEST_CHECK(stats.api.tcpEnabled);
    AKK_TEST_CHECK(stats.api.tcpRequestsTotal >= 24);
    AKK_TEST_CHECK(stats.api.tcpResponsesTotal >= 24);
    AKK_TEST_CHECK(stats.api.tcpProtocolErrorsTotal >= 1);

    engine->close();
    return 0;
}
catch (const std::exception& e) {
    akkaradb::test::failFastExit("API SERVER SMOKE EXCEPTION", e.what());
}
catch (...) {
    akkaradb::test::failFastExit("API SERVER SMOKE EXCEPTION", "unknown exception");
}
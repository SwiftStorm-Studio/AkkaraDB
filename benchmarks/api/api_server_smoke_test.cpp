/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// benchmarks/api/api_server_smoke_test.cpp
#include "../smoke/detail/CollectHistory.hpp"
#include "TestErrorHandlers.hpp"

#include "akk/engine/AkkEngine.hpp"
#include "akk/engine/server/ApiFraming.hpp"
#include "akk/net/tls/TlsStream.hpp"

#ifdef AKKARADB_TEST_HAS_GRPC
#include "akkaradb_grpc.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#endif

#include <array>
#include <charconv>
#include <chrono>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

using namespace akkaradb::engine;
using namespace akkaradb::engine::server;

#ifdef AKKARADB_TEST_HAS_GRPC
namespace wire = ::akkaradb::grpcapi::v1;
#endif

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

    struct TcpStreamFrame {
        uint8_t type = 0;
        std::vector<uint8_t> payload;
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

    [[nodiscard]] TcpStreamFrame decodeTcpStreamFrame(std::span<const uint8_t> payload) {
        AKK_TEST_CHECK(payload.size() >= sizeof(uint8_t) + sizeof(uint32_t));
        TcpStreamFrame frame;
        frame.type = payload[0];
        payload = payload.subspan(sizeof(uint8_t));

        uint32_t frameSize = 0;
        std::memcpy(&frameSize, payload.data(), sizeof(frameSize));
        payload = payload.subspan(sizeof(frameSize));
        AKK_TEST_CHECK(payload.size() == frameSize);
        frame.payload.assign(payload.begin(), payload.end());
        return frame;
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

    [[nodiscard]] std::string httpHeaderValue(std::string_view response, std::string_view name) {
        const auto headerEnd = response.find("\r\n\r\n");
        AKK_TEST_CHECK(headerEnd != std::string_view::npos);
        size_t pos = 0;
        while (pos < headerEnd) {
            const auto lineEnd = response.find("\r\n", pos);
            if (lineEnd == std::string_view::npos || lineEnd > headerEnd) { break; }
            const auto line = response.substr(pos, lineEnd - pos);
            const auto colon = line.find(':');
            if (colon != std::string_view::npos && line.substr(0, colon) == name) {
                auto value = line.substr(colon + 1);
                while (!value.empty() && value.front() == ' ') { value.remove_prefix(1); }
                return std::string{value};
            }
            pos = lineEnd + 2;
        }
        return {};
    }

    [[nodiscard]] std::vector<uint8_t> decodeChunkedHttpBody(std::string_view response) {
        const auto headerEnd = response.find("\r\n\r\n");
        AKK_TEST_CHECK(headerEnd != std::string_view::npos);

        size_t pos = headerEnd + 4;
        std::vector<uint8_t> body;
        while (pos < response.size()) {
            const auto sizeEnd = response.find("\r\n", pos);
            AKK_TEST_CHECK(sizeEnd != std::string_view::npos);
            std::string_view sizeText = response.substr(pos, sizeEnd - pos);
            const auto semi = sizeText.find(';');
            if (semi != std::string_view::npos) { sizeText = sizeText.substr(0, semi); }

            size_t chunkSize = 0;
            const auto result = std::from_chars(sizeText.data(), sizeText.data() + sizeText.size(), chunkSize, 16);
            AKK_TEST_CHECK(result.ec == std::errc{});
            pos = sizeEnd + 2;
            if (chunkSize == 0) {
                AKK_TEST_CHECK(response.size() >= pos + 2);
                break;
            }
            AKK_TEST_CHECK(response.size() >= pos + chunkSize + 2);
            body.insert(
                body.end(),
                reinterpret_cast<const uint8_t*>(response.data() + pos),
                reinterpret_cast<const uint8_t*>(response.data() + pos + chunkSize)
            );
            pos += chunkSize;
            AKK_TEST_CHECK(response.substr(pos, 2) == "\r\n");
            pos += 2;
        }
        return body;
    }

    struct HttpStreamFrame {
        uint8_t type = 0;
        std::vector<uint8_t> payload;
    };

    [[nodiscard]] std::vector<HttpStreamFrame> decodeHttpStreamFrames(std::span<const uint8_t> payload, std::string_view prelude) {
        AKK_TEST_CHECK(payload.size() >= prelude.size());
        AKK_TEST_CHECK(std::memcmp(payload.data(), prelude.data(), prelude.size()) == 0);

        payload = payload.subspan(prelude.size());
        std::vector<HttpStreamFrame> frames;
        while (!payload.empty()) {
            AKK_TEST_CHECK(payload.size() >= sizeof(uint8_t) + sizeof(uint32_t));
            HttpStreamFrame frame;
            frame.type = payload[0];
            payload = payload.subspan(sizeof(uint8_t));

            uint32_t frameSize = 0;
            std::memcpy(&frameSize, payload.data(), sizeof(frameSize));
            payload = payload.subspan(sizeof(frameSize));
            AKK_TEST_CHECK(payload.size() >= frameSize);
            frame.payload.assign(payload.begin(), payload.begin() + static_cast<std::ptrdiff_t>(frameSize));
            payload = payload.subspan(frameSize);
            frames.push_back(std::move(frame));
        }
        return frames;
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

        auto scanStream = makeRequest(24, ApiOp::SCAN_STREAM, bytes("b"), std::span<const uint8_t>{scanPayload.data(), scanPayload.size()});
        tlsSendAll(stream, scanStream.data(), scanStream.size());
        auto scanStreamItem1 = decodeTcpStreamFrame(readResponse(stream).value);
        auto scanStreamItem2 = decodeTcpStreamFrame(readResponse(stream).value);
        auto scanStreamEnd = decodeTcpStreamFrame(readResponse(stream).value);
        AKK_TEST_CHECK(scanStreamItem1.type == 1);
        AKK_TEST_CHECK(scanStreamItem2.type == 1);
        AKK_TEST_CHECK(scanStreamEnd.type == 2);
        uint32_t tcpScanStreamCount = 0;
        AKK_TEST_CHECK(scanStreamEnd.payload.size() == sizeof(uint32_t) + sizeof(uint8_t));
        std::memcpy(&tcpScanStreamCount, scanStreamEnd.payload.data(), sizeof(tcpScanStreamCount));
        AKK_TEST_CHECK(tcpScanStreamCount == 2);
        AKK_TEST_CHECK(scanStreamEnd.payload[sizeof(uint32_t)] == 1);

        auto historyRequest = makeRequest(21, ApiOp::HISTORY, bytes("history"));
        tlsSendAll(stream, historyRequest.data(), historyRequest.size());
        auto historyResponse = readResponse(stream);
        AKK_TEST_CHECK(historyResponse.status == ApiStatus::OK);
        AKK_TEST_CHECK(historyResponse.value.size() >= sizeof(uint32_t));
        uint32_t tcpHistoryCount = 0;
        std::memcpy(&tcpHistoryCount, historyResponse.value.data(), sizeof(tcpHistoryCount));
        AKK_TEST_CHECK(tcpHistoryCount >= 2);

        std::vector<uint8_t> historyStreamPayload;
        appendPlain(historyStreamPayload, static_cast<uint32_t>(1));
        auto historyStream = makeRequest(
            22,
            ApiOp::HISTORY_STREAM,
            bytes("history"),
            std::span<const uint8_t>{historyStreamPayload.data(), historyStreamPayload.size()}
        );
        tlsSendAll(stream, historyStream.data(), historyStream.size());
        auto historyStreamItem = decodeTcpStreamFrame(readResponse(stream).value);
        auto historyStreamEnd = decodeTcpStreamFrame(readResponse(stream).value);
        AKK_TEST_CHECK(historyStreamItem.type == 1);
        AKK_TEST_CHECK(historyStreamEnd.type == 2);
        uint32_t tcpHistoryStreamCount = 0;
        AKK_TEST_CHECK(historyStreamEnd.payload.size() == sizeof(uint32_t) + sizeof(uint8_t));
        std::memcpy(&tcpHistoryStreamCount, historyStreamEnd.payload.data(), sizeof(tcpHistoryStreamCount));
        AKK_TEST_CHECK(tcpHistoryStreamCount == 1);
        AKK_TEST_CHECK(historyStreamEnd.payload[sizeof(uint32_t)] == 1);

        auto forceSync = makeRequest(23, ApiOp::FORCE_SYNC, {});
        tlsSendAll(stream, forceSync.data(), forceSync.size());
        AKK_TEST_CHECK(readResponse(stream).status == ApiStatus::OK);

        auto runBlobGc = makeRequest(39, ApiOp::RUN_BLOB_GC, {});
        tlsSendAll(stream, runBlobGc.data(), runBlobGc.size());
        AKK_TEST_CHECK(readResponse(stream).status == ApiStatus::OK);

        auto stats = makeRequest(40, ApiOp::STATS, {});
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

#ifdef AKKARADB_TEST_HAS_GRPC
    void testGrpc(uint16_t port) {
        auto channel = ::grpc::CreateChannel("127.0.0.1:" + std::to_string(port), ::grpc::InsecureChannelCredentials());
        AKK_TEST_CHECK(channel->WaitForConnected(std::chrono::system_clock::now() + std::chrono::seconds(5)));
        auto stub = wire::AkkaraDB::NewStub(channel);

        {
            ::grpc::ClientContext context;
            wire::PingRequest request;
            wire::PingResponse response;
            const auto status = stub->Ping(&context, request, &response);
            AKK_TEST_CHECK(status.ok());
            AKK_TEST_CHECK(response.message() == "pong");
        }

        {
            ::grpc::ClientContext context;
            wire::ScanRequest request;
            request.set_start_key("b");
            request.set_end_key("c");
            request.set_limit(2);
            auto reader = stub->ScanStream(&context, request);

            size_t itemCount = 0;
            bool sawEnd = false;
            wire::ScanStreamFrame frame;
            while (reader->Read(&frame)) {
                if (frame.has_item()) {
                    ++itemCount;
                }
                else if (frame.has_end()) {
                    AKK_TEST_CHECK(frame.end().emitted_count() == 2);
                    AKK_TEST_CHECK(frame.end().truncated());
                    sawEnd = true;
                }
            }
            const auto status = reader->Finish();
            AKK_TEST_CHECK(status.ok());
            AKK_TEST_CHECK(itemCount == 2);
            AKK_TEST_CHECK(sawEnd);
        }

        {
            ::grpc::ClientContext context;
            wire::HistoryRequest request;
            request.set_key("history");
            auto reader = stub->HistoryStream(&context, request);

            size_t itemCount = 0;
            bool sawEnd = false;
            wire::HistoryStreamFrame frame;
            while (reader->Read(&frame)) {
                if (frame.has_item()) {
                    ++itemCount;
                }
                else if (frame.has_end()) {
                    AKK_TEST_CHECK(frame.end().emitted_count() >= 2);
                    AKK_TEST_CHECK(!frame.end().truncated());
                    sawEnd = true;
                }
            }
            const auto status = reader->Finish();
            AKK_TEST_CHECK(status.ok());
            AKK_TEST_CHECK(itemCount >= 2);
            AKK_TEST_CHECK(sawEnd);
        }
    }
#endif

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

    void testClusterRoutingErrors(const std::filesystem::path& dir, uint16_t port) {
        using namespace akkaradb::engine::cluster;
        NodeInfo primary;
        primary.nodeId = 1; primary.host = "127.0.0.1"; primary.dataPort = port;
        primary.replPort = static_cast<uint16_t>(port + 3); primary.capabilities = COORDINATOR_ELIGIBLE | DATA_BEARING;
        auto replica = primary; replica.nodeId = 2; replica.dataPort = static_cast<uint16_t>(port + 10); replica.replPort = static_cast<uint16_t>(port + 13);
        const ClusterConfig config{{primary, replica}, ReplicationMode::MIRROR, {}, {}, {}, {}, 1};
        AkkEngineOptions options;
        options.paths.dataDir = dir / "routing";
        options.components.clusterEnabled = true; options.components.blobEnabled = false; options.components.apiEnabled = true;
        options.cluster.config = config;
        options.cluster.runtime.transportMode = TransportMode::PLAIN;
        options.cluster.runtime.startupRole = NodeStartupRole::REPLICA;
        options.cluster.runtime.clusterGroupId = 24900;
        options.cluster.runtime.apiEndpoints = {{1, port, static_cast<uint16_t>(port + 1), static_cast<uint16_t>(port + 2)}};
        options.api.bindHost = "127.0.0.1";
        options.api.tcpPort = static_cast<uint16_t>(port + 10); options.api.httpPort = static_cast<uint16_t>(port + 11); options.api.grpcPort = static_cast<uint16_t>(port + 12);
        std::filesystem::create_directories(options.paths.dataDir);
        {
            std::ofstream id{options.paths.dataDir / "node.id", std::ios::binary};
            const uint64_t nodeId = 2; id.write(reinterpret_cast<const char*>(&nodeId), sizeof(nodeId));
            AKK_TEST_CHECK(id.good());
        }
        auto engine = AkkEngine::open(options);
        akkaradb::net::TlsStream stream;
        stream.connect("127.0.0.1", options.api.tcpPort, {}, kSmokeIoTimeoutMs, kSmokeIoTimeoutMs);
        const auto request = makeRequest(901, ApiOp::PUT, bytes("routed"), bytes("denied"));
        tlsSendAll(stream, request.data(), request.size());
        const auto result = readResponse(stream);
        AKK_TEST_CHECK(result.status == ApiStatus::ROUTING_ERROR && result.requestId == 901);
        AKK_TEST_CHECK(text(result.value).find("\"code\":\"NOT_OWNER\"") != std::string::npos && text(result.value).find("\"nodeId\":1") != std::string::npos);
        const auto ping = makeRequest(902, ApiOp::PING, {});
        tlsSendAll(stream, ping.data(), ping.size()); AKK_TEST_CHECK(readResponse(stream).status == ApiStatus::OK);
        AKK_TEST_CHECK(engine->stats().api.tcpProtocolErrorsTotal == 0 && !engine->exists(bytes("routed")));
        const auto http = httpRequest(options.api.httpPort,
            "POST /v1/put?key=routed HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nContent-Length: 6\r\n\r\ndenied");
        AKK_TEST_CHECK(http.find("409 Conflict") != std::string::npos && http.find("application/json") != std::string::npos);
        AKK_TEST_CHECK(http.find("\"outcomeUnknown\":false") != std::string::npos);
#ifdef AKKARADB_TEST_HAS_GRPC
        const auto channel = ::grpc::CreateChannel("127.0.0.1:" + std::to_string(options.api.grpcPort), ::grpc::InsecureChannelCredentials());
        auto stub = wire::AkkaraDB::NewStub(channel);
        ::grpc::ClientContext context;
        wire::PutRequest put; put.set_key("routed"); put.set_value("denied");
        wire::Empty response;
        const auto status = stub->Put(&context, put, &response);
        AKK_TEST_CHECK(status.error_code() == ::grpc::StatusCode::FAILED_PRECONDITION);
        AKK_TEST_CHECK(status.error_details().find("\"nodeId\":1") != std::string::npos);
#endif
        stream.close(); engine->close();
    }

    void testClusterPublicQueries(const std::filesystem::path& dir, uint16_t port) {
        using namespace akkaradb::engine::cluster;
        const NodeInfo n1{.nodeId = 1, .host = "127.0.0.1", .dataPort = port,
            .replPort = static_cast<uint16_t>(port + 3), .capabilities = COORDINATOR_ELIGIBLE | DATA_BEARING};
        auto n2 = n1; n2.nodeId = 2; n2.dataPort = static_cast<uint16_t>(port + 10); n2.replPort = static_cast<uint16_t>(port + 13);
        const ClusterConfig config{{n1, n2}, ReplicationMode::MIRROR, {}, {}, {}, {}, 1};
        const auto makeOptions = [&](uint64_t id) {
            AkkEngineOptions options;
            options.paths.dataDir = dir / ("query-node-" + std::to_string(id));
            options.components.clusterEnabled = true; options.components.blobEnabled = false;
            options.components.versionLogEnabled = true; options.components.apiEnabled = id == 2;
            options.cluster.config = config;
            options.cluster.runtime.transportMode = TransportMode::PLAIN;
            options.cluster.runtime.startupRole = id == 1 ? NodeStartupRole::PRIMARY : NodeStartupRole::REPLICA;
            options.cluster.runtime.clusterGroupId = 24950;
            options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
            options.api.bindHost = "127.0.0.1";
            options.api.tcpPort = static_cast<uint16_t>(port + 10); options.api.httpPort = static_cast<uint16_t>(port + 11);
            options.api.grpcPort = static_cast<uint16_t>(port + 12);
            std::filesystem::create_directories(options.paths.dataDir);
            std::ofstream file{options.paths.dataDir / "node.id", std::ios::binary};
            file.write(reinterpret_cast<const char*>(&id), sizeof(id)); AKK_TEST_CHECK(file.good());
            return options;
        };
        auto primary = AkkEngine::open(makeOptions(1)), replica = AkkEngine::open(makeOptions(2));
        primary->put(bytes("query-key"), bytes("before"));
        const auto seq = akk_test::collectHistory(primary->history(bytes("query-key"))).front().seq;
        primary->put(bytes("query-key"), bytes("after"));
        akkaradb::net::TlsStream stream;
        stream.connect("127.0.0.1", static_cast<uint16_t>(port + 10), {}, kSmokeIoTimeoutMs, kSmokeIoTimeoutMs);
        const auto exchange = [&](ApiOp op, std::span<const uint8_t> value = {}) {
            const auto request = makeRequest(950, op, bytes("query-key"), value);
            tlsSendAll(stream, request.data(), request.size());
            auto response = readResponse(stream); AKK_TEST_CHECK(response.status == ApiStatus::OK); return response.value;
        };
        AKK_TEST_CHECK(text(exchange(ApiOp::GET_AT, u64Le(seq))) == "before");
        const auto count = exchange(ApiOp::COUNT);
        uint64_t counted = 0; AKK_TEST_CHECK(count.size() == sizeof(counted));
        std::memcpy(&counted, count.data(), sizeof(counted)); AKK_TEST_CHECK(counted == 1);
        const auto history = exchange(ApiOp::HISTORY);
        uint32_t versions = 0; AKK_TEST_CHECK(history.size() >= sizeof(versions));
        std::memcpy(&versions, history.data(), sizeof(versions)); AKK_TEST_CHECK(versions == 2);
        std::vector<uint8_t> scanPayload; appendPlain(scanPayload, uint32_t{10});
        const auto scan = exchange(ApiOp::SCAN, scanPayload);
        uint32_t scanned = 0; AKK_TEST_CHECK(scan.size() >= sizeof(scanned));
        std::memcpy(&scanned, scan.data(), sizeof(scanned)); AKK_TEST_CHECK(scanned == 1);
        const auto httpPort = static_cast<uint16_t>(port + 11);
        for (const auto path : {"/v1/count", "/v1/scan", "/v1/history?key=query-key"}) {
            const auto response = httpRequest(httpPort, std::string{"GET "} + path +
                " HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
            AKK_TEST_CHECK(response.find("200 OK") != std::string::npos);
        }
#ifdef AKKARADB_TEST_HAS_GRPC
        auto stub = wire::AkkaraDB::NewStub(::grpc::CreateChannel("127.0.0.1:" + std::to_string(port + 12), ::grpc::InsecureChannelCredentials()));
        ::grpc::ClientContext countContext; wire::CountRequest countRequest; wire::CountResponse countResponse;
        AKK_TEST_CHECK(stub->Count(&countContext, countRequest, &countResponse).ok() && countResponse.count() == 1);
        ::grpc::ClientContext scanContext; wire::ScanRequest scanRequest; wire::ScanResponse scanResponse;
        AKK_TEST_CHECK(stub->Scan(&scanContext, scanRequest, &scanResponse).ok() && scanResponse.items_size() == 1);
        ::grpc::ClientContext historyContext; wire::HistoryRequest historyRequest; wire::HistoryResponse historyResponse;
        historyRequest.set_key("query-key");
        AKK_TEST_CHECK(stub->History(&historyContext, historyRequest, &historyResponse).ok() && historyResponse.entries_size() == 2);
#endif
        stream.close(); replica->close(); primary->close();
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

        const auto scanStream = httpRequest(
            port,
            "GET /v1/scan?start=h&end=i&limit=1&stream=1 HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n"
        );
        AKK_TEST_CHECK(scanStream.find("200 OK") != std::string::npos);
        AKK_TEST_CHECK(httpHeaderValue(scanStream, "Transfer-Encoding") == "chunked");
        AKK_TEST_CHECK(httpHeaderValue(scanStream, "Content-Type") == "application/vnd.akkaradb.scan-stream");
        const auto scanStreamFrames =
            decodeHttpStreamFrames(decodeChunkedHttpBody(scanStream), std::string_view{"AKKS\x01", 5});
        AKK_TEST_CHECK(scanStreamFrames.size() == 2);
        AKK_TEST_CHECK(scanStreamFrames[0].type == 1);
        AKK_TEST_CHECK(scanStreamFrames[1].type == 2);
        AKK_TEST_CHECK(scanStreamFrames[0].payload.size() >= sizeof(uint16_t) + sizeof(uint32_t));
        uint32_t scanStreamCount = 0;
        AKK_TEST_CHECK(scanStreamFrames[1].payload.size() == sizeof(uint32_t) + sizeof(uint8_t));
        std::memcpy(&scanStreamCount, scanStreamFrames[1].payload.data(), sizeof(scanStreamCount));
        AKK_TEST_CHECK(scanStreamCount == 1);
        AKK_TEST_CHECK(scanStreamFrames[1].payload[sizeof(uint32_t)] == 1);

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

        const auto historyStream = httpRequest(
            port,
            "GET /v1/history?key=history&stream=1 HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n"
        );
        AKK_TEST_CHECK(historyStream.find("200 OK") != std::string::npos);
        AKK_TEST_CHECK(httpHeaderValue(historyStream, "Transfer-Encoding") == "chunked");
        AKK_TEST_CHECK(httpHeaderValue(historyStream, "Content-Type") == "application/vnd.akkaradb.history-stream");
        const auto historyStreamFrames =
            decodeHttpStreamFrames(decodeChunkedHttpBody(historyStream), std::string_view{"AKKH\x01", 5});
        AKK_TEST_CHECK(historyStreamFrames.size() >= 2);
        AKK_TEST_CHECK(historyStreamFrames.back().type == 2);
        uint32_t historyStreamCount = 0;
        AKK_TEST_CHECK(historyStreamFrames.back().payload.size() == sizeof(uint32_t) + sizeof(uint8_t));
        std::memcpy(&historyStreamCount, historyStreamFrames.back().payload.data(), sizeof(historyStreamCount));
        AKK_TEST_CHECK(historyStreamCount >= 2);
        AKK_TEST_CHECK(historyStreamFrames.back().payload[sizeof(uint32_t)] == 0);

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
    const uint16_t grpcPort = static_cast<uint16_t>(httpPort + 2);

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
    options.api.grpcPort = grpcPort;
    options.api.tcpReadTimeoutMs = kSmokeIoTimeoutMs;
    options.api.tcpWriteTimeoutMs = kSmokeIoTimeoutMs;

    auto engine = AkkEngine::open(options);
    engine->put(bytes("history"), bytes("v1"));
    engine->put(bytes("history"), bytes("v2"));
    const auto history = akk_test::collectHistory(engine->history(bytes("history")));
    AKK_TEST_CHECK(history.size() == 2);

    testTcp(tcpPort, history);
    testTcpProtocolError(tcpPort);
    testHttp(httpPort);
#ifdef AKKARADB_TEST_HAS_GRPC
    testGrpc(grpcPort);
#endif

    const auto stats = engine->stats();
    AKK_TEST_CHECK(stats.api.tcpEnabled);
    AKK_TEST_CHECK(stats.api.tcpRequestsTotal >= 24);
    AKK_TEST_CHECK(stats.api.tcpResponsesTotal >= 24);
    AKK_TEST_CHECK(stats.api.tcpProtocolErrorsTotal >= 1);

    engine->close();
    testClusterRoutingErrors(options.paths.dataDir, static_cast<uint16_t>(httpPort + 30));
    testClusterPublicQueries(options.paths.dataDir, static_cast<uint16_t>(httpPort + 50));
    return 0;
}
catch (const std::exception& e) {
    akkaradb::test::failFastExit("API SERVER SMOKE EXCEPTION", e.what());
}
catch (...) {
    akkaradb::test::failFastExit("API SERVER SMOKE EXCEPTION", "unknown exception");
}

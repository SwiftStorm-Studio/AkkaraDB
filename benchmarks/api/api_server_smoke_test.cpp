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

// benchmarks/api/api_server_smoke_test.cpp
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

    [[nodiscard]] std::filesystem::path temp_dir() {
        const auto dir = std::filesystem::temp_directory_path() / "akkaradb_api_server_smoke";
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
        std::filesystem::create_directories(dir, ec);
        assert(!ec);
        return dir;
    }

    [[nodiscard]] uint16_t base_port() {
        const auto ticks = std::chrono::steady_clock::now().time_since_epoch().count();
        return static_cast<uint16_t>(25000 + (ticks % 10000));
    }

    void tls_send_all(akkaradb::net::TlsStream& stream, const uint8_t* data, size_t size) {
        size_t sent = 0;
        while (sent < size) { sent += stream.send(data + sent, size - sent); }
    }

    void tls_recv_all(akkaradb::net::TlsStream& stream, uint8_t* data, size_t size) {
        size_t got = 0;
        while (got < size) { got += stream.recv(data + got, size - got); }
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

    template <typename T>
    void append_plain(std::vector<uint8_t>& out, T value) {
        const size_t offset = out.size();
        out.resize(offset + sizeof(T));
        std::memcpy(out.data() + offset, &value, sizeof(T));
    }

    void append_bytes(std::vector<uint8_t>& out, std::span<const uint8_t> bytes) {
        const size_t offset = out.size();
        out.resize(offset + bytes.size());
        if (!bytes.empty()) { std::memcpy(out.data() + offset, bytes.data(), bytes.size()); }
    }

    [[nodiscard]] std::vector<uint8_t> make_batch_put_request(
        uint32_t request_id,
        std::span<const std::pair<std::string_view, std::string_view>> items
    ) {
        std::vector<uint8_t> payload;
        append_plain(payload, static_cast<uint32_t>(items.size()));
        for (const auto& [key, value] : items) {
            append_plain(payload, static_cast<uint16_t>(key.size()));
            append_plain(payload, static_cast<uint32_t>(value.size()));
            append_bytes(payload, bytes(key));
            append_bytes(payload, bytes(value));
        }
        return make_request(request_id, ApiOp::BatchPut, {}, std::span<const uint8_t>{payload.data(), payload.size()});
    }

    [[nodiscard]] std::vector<uint8_t> make_batch_get_request(uint32_t request_id, std::span<const std::string_view> keys) {
        std::vector<uint8_t> payload;
        append_plain(payload, static_cast<uint32_t>(keys.size()));
        for (const auto key : keys) {
            append_plain(payload, static_cast<uint16_t>(key.size()));
            append_bytes(payload, bytes(key));
        }
        return make_request(request_id, ApiOp::BatchGet, {}, std::span<const uint8_t>{payload.data(), payload.size()});
    }

    [[nodiscard]] std::vector<uint8_t> u64_le(uint64_t value) {
        std::vector<uint8_t> out(sizeof(value));
        std::memcpy(out.data(), &value, sizeof(value));
        return out;
    }

    struct TcpResponse {
        ApiStatus status = ApiStatus::Error;
        uint32_t request_id = 0;
        std::vector<uint8_t> value;
    };

    [[nodiscard]] TcpResponse read_response(akkaradb::net::TlsStream& stream) {
        ApiResponseHeader header{};
        tls_recv_all(stream, reinterpret_cast<uint8_t*>(&header), sizeof(header));
        assert(std::memcmp(header.magic, RESPONSE_MAGIC, sizeof(header.magic)) == 0);

        TcpResponse response;
        response.status = header.status;
        response.request_id = header.request_id;
        response.value.resize(header.val_len);
        if (!response.value.empty()) { tls_recv_all(stream, response.value.data(), response.value.size()); }

        uint32_t received_crc = 0;
        tls_recv_all(stream, reinterpret_cast<uint8_t*>(&received_crc), sizeof(received_crc));
        assert(received_crc == crc32c(std::span<const uint8_t>{response.value.data(), response.value.size()}));
        return response;
    }

    struct BatchGetItem {
        ApiStatus status = ApiStatus::Error;
        std::vector<uint8_t> value;
    };

    [[nodiscard]] std::vector<BatchGetItem> decode_batch_get_response(std::span<const uint8_t> payload) {
        size_t pos = 0;
        uint32_t count = 0;
        assert(payload.size() >= sizeof(count));
        std::memcpy(&count, payload.data(), sizeof(count));
        pos += sizeof(count);

        std::vector<BatchGetItem> out;
        out.reserve(count);
        for (uint32_t i = 0; i < count; ++i) {
            assert(payload.size() - pos >= sizeof(uint8_t) + sizeof(uint32_t));
            uint8_t status = 0;
            uint32_t value_len = 0;
            std::memcpy(&status, payload.data() + pos, sizeof(status));
            pos += sizeof(status);
            std::memcpy(&value_len, payload.data() + pos, sizeof(value_len));
            pos += sizeof(value_len);
            assert(payload.size() - pos >= value_len);

            BatchGetItem item;
            item.status = static_cast<ApiStatus>(status);
            item.value.assign(payload.begin() + static_cast<std::ptrdiff_t>(pos), payload.begin() + static_cast<std::ptrdiff_t>(pos + value_len));
            pos += value_len;
            out.push_back(std::move(item));
        }
        assert(pos == payload.size());
        return out;
    }

    [[nodiscard]] std::string http_request(uint16_t port, std::string_view request) {
        akkaradb::net::TlsStream stream;
        stream.connect("127.0.0.1", port, {}, kSmokeIoTimeoutMs, kSmokeIoTimeoutMs);
        tls_send_all(stream, reinterpret_cast<const uint8_t*>(request.data()), request.size());

        std::string response;
        std::array<char, 1024> chunk{};
        for (;;) {
            try {
                const size_t got = stream.recv(chunk.data(), chunk.size());
                response.append(chunk.data(), got);
            }
            catch (...) { break; }
            if (response.find("\r\n\r\n") != std::string::npos && response.find("Connection: close") == std::string::npos) {
                const auto header_end = response.find("\r\n\r\n");
                const auto cl = response.find("Content-Length:");
                if (cl != std::string::npos && cl < header_end) {
                    const auto line_end = response.find("\r\n", cl);
                    size_t content_length = 0;
                    std::string_view value{response.data() + cl + 15, line_end - cl - 15};
                    while (!value.empty() && value.front() == ' ') { value.remove_prefix(1); }
                    (void)std::from_chars(value.data(), value.data() + value.size(), content_length);
                    if (response.size() >= header_end + 4 + content_length) { break; }
                }
            }
        }
        return response;
    }

    void test_tcp(uint16_t port, const std::vector<VersionEntry>& history) {
        akkaradb::net::TlsStream stream;
        stream.connect("127.0.0.1", port, {}, kSmokeIoTimeoutMs, kSmokeIoTimeoutMs);

        auto put = make_request(1, ApiOp::Put, bytes("tcp"), bytes("one"));
        tls_send_all(stream, put.data(), put.size());
        auto put_response = read_response(stream);
        assert(put_response.status == ApiStatus::Ok);
        assert(put_response.request_id == 1);

        auto get = make_request(2, ApiOp::Get, bytes("tcp"));
        tls_send_all(stream, get.data(), get.size());
        auto get_response = read_response(stream);
        assert(get_response.status == ApiStatus::Ok);
        assert(text(get_response.value) == "one");

        auto get_at = make_request(3, ApiOp::GetAt, bytes("history"), u64_le(history[0].seq));
        tls_send_all(stream, get_at.data(), get_at.size());
        auto at_response = read_response(stream);
        assert(at_response.status == ApiStatus::Ok);
        assert(text(at_response.value) == "v1");

        auto p1 = make_request(4, ApiOp::Put, bytes("p1"), bytes("a"));
        auto p2 = make_request(5, ApiOp::Put, bytes("p2"), bytes("b"));
        tls_send_all(stream, p1.data(), p1.size());
        tls_send_all(stream, p2.data(), p2.size());
        auto p1_response = read_response(stream);
        auto p2_response = read_response(stream);
        assert(p1_response.status == ApiStatus::Ok);
        assert(p2_response.status == ApiStatus::Ok);

        auto remove = make_request(6, ApiOp::Remove, bytes("tcp"));
        tls_send_all(stream, remove.data(), remove.size());
        auto remove_response = read_response(stream);
        assert(remove_response.status == ApiStatus::Ok);

        const std::pair<std::string_view, std::string_view> batch_put_items[] = {
            {"b1", "x"},
            {"b2", "y"},
            {"b3", "z"},
        };
        auto batch_put = make_batch_put_request(7, std::span<const std::pair<std::string_view, std::string_view>>{batch_put_items});
        tls_send_all(stream, batch_put.data(), batch_put.size());
        auto batch_put_response = read_response(stream);
        assert(batch_put_response.status == ApiStatus::Ok);

        const std::string_view batch_get_keys[] = {"b1", "missing", "b2", "b3"};
        auto batch_get = make_batch_get_request(8, std::span<const std::string_view>{batch_get_keys});
        tls_send_all(stream, batch_get.data(), batch_get.size());
        auto batch_response = read_response(stream);
        assert(batch_response.status == ApiStatus::Ok);
        const auto decoded_batch = decode_batch_get_response(batch_response.value);
        assert(decoded_batch.size() == 4);
        assert(decoded_batch[0].status == ApiStatus::Ok && text(decoded_batch[0].value) == "x");
        assert(decoded_batch[1].status == ApiStatus::NotFound);
        assert(decoded_batch[2].status == ApiStatus::Ok && text(decoded_batch[2].value) == "y");
        assert(decoded_batch[3].status == ApiStatus::Ok && text(decoded_batch[3].value) == "z");

        std::vector<uint8_t> pipeline;
        for (uint32_t request_id = 9; request_id < 17; ++request_id) {
            auto request = make_request(request_id, ApiOp::Get, bytes("b1"));
            pipeline.insert(pipeline.end(), request.begin(), request.end());
        }
        tls_send_all(stream, pipeline.data(), pipeline.size());
        for (uint32_t request_id = 9; request_id < 17; ++request_id) {
            auto response = read_response(stream);
            assert(response.status == ApiStatus::Ok);
            assert(response.request_id == request_id);
            assert(text(response.value) == "x");
        }
    }

    void test_http(uint16_t port) {
        const auto put = http_request(
            port,
            "POST /v1/put?key=http HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "Content-Length: 3\r\n"
            "\r\n"
            "two"
        );
        assert(put.find("204 No Content") != std::string::npos);

        const auto get = http_request(
            port,
            "GET /v1/get?key=http HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n"
        );
        assert(get.find("200 OK") != std::string::npos);
        assert(get.ends_with("two"));

        const auto ping = http_request(
            port,
            "GET /v1/ping HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n"
        );
        assert(ping.find("200 OK") != std::string::npos);
        assert(ping.ends_with("pong"));
    }
}

int main() {
    akkara::test::install_msvc_test_error_handlers();

    const uint16_t http_port = base_port();
    const uint16_t tcp_port = static_cast<uint16_t>(http_port + 1);

    AkkEngineOptions options;
    options.paths.data_dir = temp_dir();
    options.components.blob_enabled = false;
    options.components.manifest_enabled = false;
    options.components.sst_enabled = false;
    options.components.version_log_enabled = true;
    options.components.api_enabled = true;
    options.api.bind_host = "127.0.0.1";
    options.api.http_port = http_port;
    options.api.tcp_port = tcp_port;
    options.api.tcp_read_timeout_ms = kSmokeIoTimeoutMs;
    options.api.tcp_write_timeout_ms = kSmokeIoTimeoutMs;

    auto engine = AkkEngine::open(options);
    engine->put(bytes("history"), bytes("v1"));
    engine->put(bytes("history"), bytes("v2"));
    const auto history = engine->history(bytes("history"));
    assert(history.size() == 2);

    test_tcp(tcp_port, history);
    test_http(http_port);

    engine->close();
    return 0;
}

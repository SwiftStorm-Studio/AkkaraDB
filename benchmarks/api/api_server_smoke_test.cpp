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
#include <cassert>
#include <charconv>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

using namespace akkaradb::engine;
using namespace akkaradb::engine::server;

namespace {
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

    [[nodiscard]] std::string http_request(uint16_t port, std::string_view request) {
        akkaradb::net::TlsStream stream;
        stream.connect("127.0.0.1", port);
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
        stream.connect("127.0.0.1", port);

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
        assert(read_response(stream).status == ApiStatus::Ok);
        assert(read_response(stream).status == ApiStatus::Ok);

        auto remove = make_request(6, ApiOp::Remove, bytes("tcp"));
        tls_send_all(stream, remove.data(), remove.size());
        assert(read_response(stream).status == ApiStatus::Ok);
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

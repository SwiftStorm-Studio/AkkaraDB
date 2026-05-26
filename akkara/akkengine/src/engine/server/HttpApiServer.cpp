/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 */

// akkengine/src/engine/server/HttpApiServer.cpp
#include "akk/engine/server/HttpApiServer.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <cctype>
#include <cstring>

namespace akkaradb::engine::server {
    namespace {
        constexpr size_t kMaxHttpLineBytes = 8192;
        constexpr size_t kMaxContentLength = 64u * 1024u * 1024u;
        constexpr size_t kRecvBufferBytes = 4096;

        [[nodiscard]] bool iequal_prefix(std::string_view line, std::string_view prefix) noexcept {
            if (line.size() < prefix.size()) { return false; }
            for (size_t i = 0; i < prefix.size(); ++i) {
                if (static_cast<char>(std::tolower(static_cast<unsigned char>(line[i]))) != prefix[i]) { return false; }
            }
            return true;
        }

        [[nodiscard]] bool icontains(std::string_view haystack, std::string_view needle) noexcept {
            if (needle.empty()) { return true; }
            if (haystack.size() < needle.size()) { return false; }
            for (size_t i = 0; i <= haystack.size() - needle.size(); ++i) {
                bool ok = true;
                for (size_t j = 0; j < needle.size(); ++j) {
                    if (static_cast<char>(std::tolower(static_cast<unsigned char>(haystack[i + j]))) != needle[j]) {
                        ok = false;
                        break;
                    }
                }
                if (ok) { return true; }
            }
            return false;
        }

        [[nodiscard]] std::string_view reason_phrase(int status_code) noexcept {
            switch (status_code) {
                case 200: return "OK";
                case 204: return "No Content";
                case 400: return "Bad Request";
                case 404: return "Not Found";
                default: return "Internal Server Error";
            }
        }
    }

    HttpApiServer::HttpApiServer(AkkEngine& engine, AkkEngineOptions::ApiOptions options) : engine_{engine}, options_{std::move(options)} {}

    std::unique_ptr<HttpApiServer> HttpApiServer::create(AkkEngine& engine, AkkEngineOptions::ApiOptions options) {
        return std::unique_ptr < HttpApiServer >
        {
            new HttpApiServer{engine, std::move(options)}
        };
    }

    HttpApiServer::~HttpApiServer() { close(); }

    void HttpApiServer::start() {
        listen_socket_ = detail::listen_on(options_.bind_host, options_.http_port, "HttpApiServer");
        running_.store(true, std::memory_order_release);
        accept_thread_ = std::thread([this] { accept_loop(); });
    }

    void HttpApiServer::close() {
        if (!running_.exchange(false, std::memory_order_acq_rel)) { return; }
        detail::shutdown_socket(listen_socket_);
        detail::close_socket(listen_socket_);
        listen_socket_ = detail::BAD_SOCKET_VALUE;
        if (accept_thread_.joinable()) { accept_thread_.join(); }
    }

    void HttpApiServer::accept_loop() {
        while (running_.load(std::memory_order_acquire)) {
            const detail::socket_t client = ::accept(listen_socket_, nullptr, nullptr);
            if (!detail::socket_ok(client)) { break; }

            std::thread(
                [this, client] {
                    detail::Connection connection{client};
                    if (options_.transport_mode == cluster::TransportMode::TLS) {
                        try { connection.enable_tls(options_.tls); }
                        catch (...) { return; }
                    }
                    handle_connection(connection);
                    connection.shutdown();
                }
            ).detach();
        }
    }

    bool HttpApiServer::read_request(detail::Connection& connection, ParsedRequest& request) {
        std::array<uint8_t, kRecvBufferBytes> buffer{};
        size_t pos = 0;
        size_t len = 0;

        const auto refill = [&]() -> bool {
            len = connection.recv_some(buffer.data(), buffer.size());
            pos = 0;
            return len > 0;
        };

        const auto read_byte = [&](char& c) -> bool {
            if (pos >= len && !refill()) { return false; }
            c = static_cast<char>(buffer[pos++]);
            return true;
        };

        const auto read_line = [&](std::string& line) -> bool {
            line.clear();
            char c = 0;
            while (line.size() < kMaxHttpLineBytes) {
                if (!read_byte(c)) { return false; }
                if (c == '\n') {
                    if (!line.empty() && line.back() == '\r') { line.pop_back(); }
                    return true;
                }
                line.push_back(c);
            }
            return false;
        };

        std::string line;
        if (!read_line(line) || line.empty()) { return false; }

        const auto sp1 = line.find(' ');
        const auto sp2 = sp1 == std::string::npos ? std::string::npos : line.find(' ', sp1 + 1);
        if (sp1 == std::string::npos || sp2 == std::string::npos) { return false; }

        request.method = line.substr(0, sp1);
        const std::string target = line.substr(sp1 + 1, sp2 - sp1 - 1);
        const auto qmark = target.find('?');
        request.path = qmark == std::string::npos ? target : target.substr(0, qmark);
        request.query = qmark == std::string::npos ? "" : target.substr(qmark + 1);
        request.body.clear();
        request.keep_alive = true;

        size_t content_length = 0;
        while (read_line(line)) {
            if (line.empty()) { break; }
            if (iequal_prefix(line, "content-length:")) {
                const auto colon = line.find(':');
                std::string_view value{line.data() + colon + 1, line.size() - colon - 1};
                while (!value.empty() && value.front() == ' ') { value.remove_prefix(1); }
                const auto result = std::from_chars(value.data(), value.data() + value.size(), content_length);
                if (result.ec != std::errc{} || content_length > kMaxContentLength) { return false; }
            }
            else if (iequal_prefix(line, "connection:")) {
                const auto colon = line.find(':');
                const std::string_view value{line.data() + colon + 1, line.size() - colon - 1};
                if (icontains(value, "close")) { request.keep_alive = false; }
            }
        }

        if (content_length == 0) { return true; }

        request.body.resize(content_length);
        size_t body_pos = 0;
        const size_t buffered = len - pos;
        if (buffered > 0) {
            const size_t take = std::min(buffered, content_length);
            std::memcpy(request.body.data(), buffer.data() + pos, take);
            pos += take;
            body_pos = take;
        }
        if (body_pos < content_length && !connection.recv_all(request.body.data() + body_pos, content_length - body_pos)) { return false; }
        return true;
    }

    std::string HttpApiServer::query_param(std::string_view query, std::string_view name) {
        while (!query.empty()) {
            const auto amp = query.find('&');
            const auto part = query.substr(0, amp);
            const auto eq = part.find('=');
            if (eq != std::string_view::npos && part.substr(0, eq) == name) { return std::string{part.substr(eq + 1)}; }
            query = amp == std::string_view::npos ? std::string_view{} : query.substr(amp + 1);
        }
        return {};
    }

    std::vector<uint8_t> HttpApiServer::url_decode(std::string_view encoded) {
        std::vector<uint8_t> out;
        out.reserve(encoded.size());
        for (size_t i = 0; i < encoded.size(); ++i) {
            if (encoded[i] == '%' && i + 2 < encoded.size()) {
                unsigned int byte = 0;
                const char hex[2] = {encoded[i + 1], encoded[i + 2]};
                const auto result = std::from_chars(hex, hex + 2, byte, 16);
                if (result.ec == std::errc{}) {
                    out.push_back(static_cast<uint8_t>(byte));
                    i += 2;
                }
                else { out.push_back(static_cast<uint8_t>('%')); }
            }
            else if (encoded[i] == '+') { out.push_back(static_cast<uint8_t>(' ')); }
            else { out.push_back(static_cast<uint8_t>(encoded[i])); }
        }
        return out;
    }

    bool HttpApiServer::send_response(detail::Connection& connection, int status_code, std::span<const uint8_t> body) {
        const std::string header = "HTTP/1.1 " + std::to_string(status_code) + " " + std::string{reason_phrase(status_code)} + "\r\n"
            "Content-Type: application/octet-stream\r\n" "Content-Length: " + std::to_string(body.size()) + "\r\n" "\r\n";
        if (!connection.send_all(reinterpret_cast<const uint8_t*>(header.data()), header.size())) { return false; }
        return body.empty() || connection.send_all(body.data(), body.size());
    }

    bool HttpApiServer::send_empty(detail::Connection& connection, int status_code) { return send_response(connection, status_code, {}); }

    bool HttpApiServer::route(detail::Connection& connection, const ParsedRequest& request, std::vector<uint8_t>& value_buffer) {
        const std::string raw_key = query_param(request.query, "key");
        if (raw_key.empty() && request.path != "/v1/ping") {
            send_empty(connection, 400);
            return request.keep_alive;
        }

        const std::vector<uint8_t> key = url_decode(raw_key);
        const std::span<const uint8_t> key_span{key.data(), key.size()};

        if (request.path == "/v1/put" && request.method == "POST") {
            engine_.put(key_span, std::span<const uint8_t>{request.body.data(), request.body.size()});
            send_empty(connection, 204);
        }
        else if (request.path == "/v1/get" && request.method == "GET") {
            value_buffer.clear();
            if (engine_.get_into(key_span, value_buffer)) {
                send_response(connection, 200, std::span<const uint8_t>{value_buffer.data(), value_buffer.size()});
            }
            else { send_empty(connection, 404); }
        }
        else if (request.path == "/v1/remove" && request.method == "DELETE") {
            engine_.remove(key_span);
            send_empty(connection, 204);
        }
        else if (request.path == "/v1/get_at" && request.method == "GET") {
            const std::string seq_text = query_param(request.query, "seq");
            uint64_t seq = 0;
            const auto result = std::from_chars(seq_text.data(), seq_text.data() + seq_text.size(), seq);
            if (seq_text.empty() || result.ec != std::errc{}) {
                send_empty(connection, 400);
                return request.keep_alive;
            }
            auto value = engine_.get_at(key_span, seq);
            if (value) { send_response(connection, 200, std::span<const uint8_t>{value->data(), value->size()}); }
            else { send_empty(connection, 404); }
        }
        else if (request.path == "/v1/ping" && request.method == "GET") {
            static constexpr std::string_view pong = "pong";
            send_response(connection, 200, {reinterpret_cast<const uint8_t*>(pong.data()), pong.size()});
        }
        else { send_empty(connection, 404); }

        return request.keep_alive;
    }

    void HttpApiServer::handle_connection(detail::Connection& connection) {
        std::vector<uint8_t> value_buffer;
        while (running_.load(std::memory_order_relaxed)) {
            ParsedRequest request;
            if (!read_request(connection, request)) { break; }
            if (!route(connection, request, value_buffer)) { break; }
        }
    }
}

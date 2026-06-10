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

        [[nodiscard]] bool iequalPrefix(std::string_view line, std::string_view prefix) noexcept {
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

        [[nodiscard]] std::string_view reasonPhrase(int statusCode) noexcept {
            switch (statusCode) {
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
        return std::unique_ptr<HttpApiServer>{new HttpApiServer{engine, std::move(options)}};
    }

    HttpApiServer::~HttpApiServer() { close(); }

    void HttpApiServer::start() {
        if (running_.load(std::memory_order_acquire)) { return; }
        listenSocket_ = detail::listenOn(options_.bindHost, options_.httpPort, "HttpApiServer", detail::makeSocketTuning(options_));
        try {
            running_.store(true, std::memory_order_release);
            acceptThread_ = std::thread([this] { acceptLoop(); });
        }
        catch (...) {
            running_.store(false, std::memory_order_release);
            detail::shutdownSocket(listenSocket_);
            detail::closeSocket(listenSocket_);
            listenSocket_ = detail::BAD_SOCKET_VALUE;
            throw;
        }
    }

    void HttpApiServer::close() {
        if (!running_.exchange(false, std::memory_order_acq_rel)) { return; }
        detail::shutdownSocket(listenSocket_);
        detail::closeSocket(listenSocket_);
        listenSocket_ = detail::BAD_SOCKET_VALUE;
        if (acceptThread_.joinable()) { acceptThread_.join(); }
        {
            std::lock_guard lock(clientsMu_);
            for (const detail::SocketHandle client : activeClients_) { detail::shutdownSocket(client); }
        }
        for (auto& thread : connectionThreads_) { if (thread.joinable()) { thread.join(); } }
        connectionThreads_.clear();
    }

    void HttpApiServer::acceptLoop() {
        while (running_.load(std::memory_order_acquire)) {
            const detail::SocketHandle client = ::accept(listenSocket_, nullptr, nullptr);
            if (!detail::socketOk(client)) {
                if (!running_.load(std::memory_order_acquire)) { break; }
                if (detail::lastAcceptErrorIsTransient()) { continue; }
                break;
            }
            detail::applySocketTuning(client, detail::makeSocketTuning(options_), true);

            if (!running_.load(std::memory_order_acquire)) {
                detail::shutdownSocket(client);
                detail::closeSocket(client);
                break;
            }

            {
                std::lock_guard lock(clientsMu_);
                activeClients_.insert(client);
            }

            try {
                connectionThreads_.emplace_back(
                    [this, client] {
                        detail::Connection connection{client};
                        if (options_.transportMode == cluster::TransportMode::TLS) {
                            try { connection.enableTls(options_.tls); }
                            catch (...) {
                                std::lock_guard lock(clientsMu_);
                                activeClients_.erase(client);
                                return;
                            }
                        }
                        handleConnection(connection);
                        connection.shutdown();
                        {
                            std::lock_guard lock(clientsMu_);
                            activeClients_.erase(client);
                        }
                    }
                );
            }
            catch (...) {
                {
                    std::lock_guard lock(clientsMu_);
                    activeClients_.erase(client);
                }
                detail::shutdownSocket(client);
                detail::closeSocket(client);
                throw;
            }
        }
    }

    bool HttpApiServer::readRequest(detail::Connection& connection, ParsedRequest& request) {
        std::array<uint8_t, kRecvBufferBytes> buffer{};
        size_t pos = 0;
        size_t len = 0;

        const auto refill = [&]() -> bool {
            len = connection.recvSome(buffer.data(), buffer.size());
            pos = 0;
            return len > 0;
        };

        const auto readByte = [&](char& c) -> bool {
            if (pos >= len && !refill()) { return false; }
            c = static_cast<char>(buffer[pos++]);
            return true;
        };

        const auto readLine = [&](std::string& line) -> bool {
            line.clear();
            char c = 0;
            while (line.size() < kMaxHttpLineBytes) {
                if (!readByte(c)) { return false; }
                if (c == '\n') {
                    if (!line.empty() && line.back() == '\r') { line.pop_back(); }
                    return true;
                }
                line.push_back(c);
            }
            return false;
        };

        std::string line;
        if (!readLine(line) || line.empty()) { return false; }

        const auto sp1 = line.find(' ');
        const auto sp2 = sp1 == std::string::npos ? std::string::npos : line.find(' ', sp1 + 1);
        if (sp1 == std::string::npos || sp2 == std::string::npos) { return false; }

        request.method = line.substr(0, sp1);
        const std::string target = line.substr(sp1 + 1, sp2 - sp1 - 1);
        const auto qmark = target.find('?');
        request.path = qmark == std::string::npos ? target : target.substr(0, qmark);
        request.query = qmark == std::string::npos ? "" : target.substr(qmark + 1);
        request.body.clear();
        request.keepAlive = true;

        size_t contentLength = 0;
        while (readLine(line)) {
            if (line.empty()) { break; }
            if (iequalPrefix(line, "content-length:")) {
                const auto colon = line.find(':');
                std::string_view value{line.data() + colon + 1, line.size() - colon - 1};
                while (!value.empty() && value.front() == ' ') { value.remove_prefix(1); }
                const auto result = std::from_chars(value.data(), value.data() + value.size(), contentLength);
                if (result.ec != std::errc{} || contentLength > kMaxContentLength) { return false; }
            }
            else if (iequalPrefix(line, "connection:")) {
                const auto colon = line.find(':');
                const std::string_view value{line.data() + colon + 1, line.size() - colon - 1};
                if (icontains(value, "close")) { request.keepAlive = false; }
            }
        }

        if (contentLength == 0) { return true; }

        request.body.resize(contentLength);
        size_t bodyPos = 0;
        const size_t buffered = len - pos;
        if (buffered > 0) {
            const size_t take = std::min(buffered, contentLength);
            std::memcpy(request.body.data(), buffer.data() + pos, take);
            pos += take;
            bodyPos = take;
        }
        if (bodyPos < contentLength && !connection.recvAll(request.body.data() + bodyPos, contentLength - bodyPos)) { return false; }
        return true;
    }

    std::string HttpApiServer::queryParam(std::string_view query, std::string_view name) {
        while (!query.empty()) {
            const auto amp = query.find('&');
            const auto part = query.substr(0, amp);
            const auto eq = part.find('=');
            if (eq != std::string_view::npos && part.substr(0, eq) == name) { return std::string{part.substr(eq + 1)}; }
            query = amp == std::string_view::npos ? std::string_view{} : query.substr(amp + 1);
        }
        return {};
    }

    std::vector<uint8_t> HttpApiServer::urlDecode(std::string_view encoded) {
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

    bool HttpApiServer::sendResponse(detail::Connection& connection, int statusCode, std::span<const uint8_t> body) {
        const std::string header = "HTTP/1.1 " + std::to_string(statusCode) + " " + std::string{reasonPhrase(statusCode)} + "\r\n"
            "Content-Type: application/octet-stream\r\n" "Content-Length: " + std::to_string(body.size()) + "\r\n" "\r\n";
        if (!connection.sendAll(reinterpret_cast<const uint8_t*>(header.data()), header.size())) { return false; }
        return body.empty() || connection.sendAll(body.data(), body.size());
    }

    bool HttpApiServer::sendEmpty(detail::Connection& connection, int statusCode) { return sendResponse(connection, statusCode, {}); }

    bool HttpApiServer::route(detail::Connection& connection, const ParsedRequest& request, std::vector<uint8_t>& valueBuffer) {
        const std::string rawKey = queryParam(request.query, "key");
        if (rawKey.empty() && request.path != "/v1/ping") {
            sendEmpty(connection, 400);
            return request.keepAlive;
        }

        const std::vector<uint8_t> key = urlDecode(rawKey);
        const std::span<const uint8_t> keySpan{key.data(), key.size()};

        if (request.path == "/v1/put" && request.method == "POST") {
            engine_.put(keySpan, std::span<const uint8_t>{request.body.data(), request.body.size()});
            sendEmpty(connection, 204);
        }
        else if (request.path == "/v1/get" && request.method == "GET") {
            valueBuffer.clear();
            if (engine_.getInto(keySpan, valueBuffer)) {
                sendResponse(connection, 200, std::span<const uint8_t>{valueBuffer.data(), valueBuffer.size()});
            }
            else { sendEmpty(connection, 404); }
        }
        else if (request.path == "/v1/remove" && request.method == "DELETE") {
            engine_.remove(keySpan);
            sendEmpty(connection, 204);
        }
        else if (request.path == "/v1/getAt" && request.method == "GET") {
            const std::string seqText = queryParam(request.query, "seq");
            uint64_t seq = 0;
            const auto result = std::from_chars(seqText.data(), seqText.data() + seqText.size(), seq);
            if (seqText.empty() || result.ec != std::errc{}) {
                sendEmpty(connection, 400);
                return request.keepAlive;
            }
            auto value = engine_.getAt(keySpan, seq);
            if (value) { sendResponse(connection, 200, std::span<const uint8_t>{value->data(), value->size()}); }
            else { sendEmpty(connection, 404); }
        }
        else if (request.path == "/v1/ping" && request.method == "GET") {
            static constexpr std::string_view pong = "pong";
            sendResponse(connection, 200, {reinterpret_cast<const uint8_t*>(pong.data()), pong.size()});
        }
        else { sendEmpty(connection, 404); }

        return request.keepAlive;
    }

    void HttpApiServer::handleConnection(detail::Connection& connection) {
        std::vector<uint8_t> valueBuffer;
        while (running_.load(std::memory_order_relaxed)) {
            ParsedRequest request;
            if (!readRequest(connection, request)) { break; }
            if (!route(connection, request, valueBuffer)) { break; }
        }
    }
}

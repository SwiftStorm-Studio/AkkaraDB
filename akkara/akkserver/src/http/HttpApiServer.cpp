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

// akkserver/src/http/HttpApiServer.cpp
#include "akk/engine/server/HttpApiServer.hpp"

#include "akk/engine/server/ApiFraming.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <cctype>
#include <cstring>
#include <span>

namespace akkaradb::engine::server {
    namespace {
        constexpr size_t kMaxHttpLineBytes = 8192;
        constexpr size_t kRecvBufferBytes = 4096;
        constexpr uint8_t kHttpStreamFrameItem = 1;
        constexpr uint8_t kHttpStreamFrameEnd = 2;
        constexpr uint8_t kHttpStreamVersion = 1;
        constexpr std::array<uint8_t, 5> kHttpScanStreamPrelude = {'A', 'K', 'K', 'S', kHttpStreamVersion};
        constexpr std::array<uint8_t, 5> kHttpHistoryStreamPrelude = {'A', 'K', 'K', 'H', kHttpStreamVersion};

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

        template <typename T>
        void appendPlain(std::vector<uint8_t>& out, T value) {
            const size_t offset = out.size();
            out.resize(offset + sizeof(T));
            std::memcpy(out.data() + offset, &value, sizeof(T));
        }

        void appendBytes(std::vector<uint8_t>& out, std::span<const uint8_t> value) {
            const size_t offset = out.size();
            out.resize(offset + value.size());
            if (!value.empty()) { std::memcpy(out.data() + offset, value.data(), value.size()); }
        }

        void encodeBool(bool value, std::vector<uint8_t>& out) {
            out.clear();
            out.push_back(value ? uint8_t{1} : uint8_t{0});
        }

        void encodeU64(uint64_t value, std::vector<uint8_t>& out) {
            out.clear();
            appendPlain(out, value);
        }

        void encodeScan(std::span<const AkkEngine::ScanRecordView> records, bool truncated, std::vector<uint8_t>& out) {
            out.clear();
            appendPlain(out, static_cast<uint32_t>(records.size()));
            appendPlain(out, static_cast<uint8_t>(truncated ? 1 : 0));
            for (const auto& record : records) {
                appendPlain(out, static_cast<uint16_t>(record.key.size()));
                appendPlain(out, static_cast<uint32_t>(record.value.size()));
                appendBytes(out, record.key);
                appendBytes(out, record.value);
            }
        }

        void encodeHistory(std::span<const VersionEntry> entries, bool truncated, std::vector<uint8_t>& out) {
            out.clear();
            appendPlain(out, static_cast<uint32_t>(entries.size()));
            appendPlain(out, static_cast<uint8_t>(truncated ? 1 : 0));
            for (const auto& entry : entries) {
                appendPlain(out, entry.seq);
                appendPlain(out, entry.sourceNodeId);
                appendPlain(out, entry.timestampNs);
                appendPlain(out, static_cast<uint32_t>(entry.flags));
                appendPlain(out, static_cast<uint32_t>(entry.value.size()));
                appendBytes(out, std::span<const uint8_t>{entry.value.data(), entry.value.size()});
            }
        }

        void appendStreamFrameHeader(std::vector<uint8_t>& out, uint8_t type, uint32_t payloadBytes) {
            out.clear();
            out.reserve(1 + sizeof(payloadBytes) + payloadBytes);
            appendPlain(out, type);
            appendPlain(out, payloadBytes);
        }

        void encodeScanStreamFrame(const AkkEngine::ScanRecordView& record, std::vector<uint8_t>& out) {
            const uint32_t payloadBytes = static_cast<uint32_t>(sizeof(uint16_t) + sizeof(uint32_t) + record.key.size() + record.value.
                size());
            appendStreamFrameHeader(out, kHttpStreamFrameItem, payloadBytes);
            appendPlain(out, static_cast<uint16_t>(record.key.size()));
            appendPlain(out, static_cast<uint32_t>(record.value.size()));
            appendBytes(out, record.key);
            appendBytes(out, record.value);
        }

        void encodeHistoryStreamFrame(const VersionEntry& entry, std::vector<uint8_t>& out) {
            const uint32_t payloadBytes = static_cast<uint32_t>(sizeof(entry.seq) + sizeof(entry.sourceNodeId) + sizeof(entry.timestampNs) +
                sizeof(uint32_t) + sizeof(uint32_t) + entry.value.size());
            appendStreamFrameHeader(out, kHttpStreamFrameItem, payloadBytes);
            appendPlain(out, entry.seq);
            appendPlain(out, entry.sourceNodeId);
            appendPlain(out, entry.timestampNs);
            appendPlain(out, static_cast<uint32_t>(entry.flags));
            appendPlain(out, static_cast<uint32_t>(entry.value.size()));
            appendBytes(out, std::span<const uint8_t>{entry.value.data(), entry.value.size()});
        }

        void encodeStreamEndFrame(uint32_t emitted, bool truncated, std::vector<uint8_t>& out) {
            appendStreamFrameHeader(out, kHttpStreamFrameEnd, static_cast<uint32_t>(sizeof(emitted) + sizeof(uint8_t)));
            appendPlain(out, emitted);
            appendPlain(out, static_cast<uint8_t>(truncated ? 1 : 0));
        }

        [[nodiscard]] bool readU64Text(std::string_view text, uint64_t& out) noexcept {
            if (text.empty()) { return false; }
            const auto result = std::from_chars(text.data(), text.data() + text.size(), out);
            return result.ec == std::errc{} && result.ptr == text.data() + text.size();
        }

        [[nodiscard]] bool readPlain(std::span<const uint8_t>& payload, uint32_t& out) noexcept {
            if (payload.size() < sizeof(out)) { return false; }
            std::memcpy(&out, payload.data(), sizeof(out));
            payload = payload.subspan(sizeof(out));
            return true;
        }

        [[nodiscard]] bool decodeHttpBatchPut(std::span<const uint8_t> payload, uint32_t maxItems, std::vector<ApiBatchPutItem>& out) {
            uint32_t count = 0;
            if (!readPlain(payload, count) || count > maxItems) { return false; }
            out.clear();
            out.reserve(count);
            for (uint32_t i = 0; i < count; ++i) {
                uint32_t keyLen = 0;
                uint32_t valueLen = 0;
                if (!readPlain(payload, keyLen) || !readPlain(payload, valueLen) || payload.size() < keyLen + valueLen) { return false; }
                const uint8_t* key = payload.data();
                payload = payload.subspan(keyLen);
                const uint8_t* value = payload.data();
                payload = payload.subspan(valueLen);
                out.push_back(ApiBatchPutItem{{key, keyLen}, {value, valueLen}});
            }
            return payload.empty();
        }

        [[nodiscard]] bool decodeHttpBatchGet(
            std::span<const uint8_t> payload,
            uint32_t maxItems,
            std::vector<std::span<const uint8_t>>& out
        ) {
            uint32_t count = 0;
            if (!readPlain(payload, count) || count > maxItems) { return false; }
            out.clear();
            out.reserve(count);
            for (uint32_t i = 0; i < count; ++i) {
                uint32_t keyLen = 0;
                if (!readPlain(payload, keyLen) || payload.size() < keyLen) { return false; }
                out.push_back({payload.data(), keyLen});
                payload = payload.subspan(keyLen);
            }
            return payload.empty();
        }

        void encodeHttpBatchGet(std::span<const ApiBatchGetResult> results, std::vector<uint8_t>& out) {
            out.clear();
            appendPlain(out, static_cast<uint32_t>(results.size()));
            for (const auto& result : results) {
                appendPlain(out, static_cast<uint8_t>(result.status));
                appendPlain(out, static_cast<uint32_t>(result.value.size()));
                appendBytes(out, result.value);
            }
        }

        void encodeStatsPayload(const EngineStats& stats, std::vector<uint8_t>& out) {
            out.clear();
            appendPlain(out, stats.currentSeq);
            appendPlain(out, stats.nodeId);
            appendPlain(out, stats.putsTotal);
            appendPlain(out, stats.removesTotal);
            appendPlain(out, stats.getsTotal);
            appendPlain(out, stats.getsMemtableHit);
            appendPlain(out, stats.getsSstHit);
            appendPlain(out, stats.getsMiss);
            appendPlain(out, stats.existsTotal);
            appendPlain(out, stats.scansTotal);
            appendPlain(out, stats.blobPutsTotal);

            appendPlain(out, static_cast<uint8_t>(stats.api.enabled));
            appendPlain(out, static_cast<uint8_t>(stats.api.httpEnabled));
            appendPlain(out, static_cast<uint8_t>(stats.api.httpTlsEnabled));
            appendPlain(out, stats.api.httpPort);
            appendPlain(out, stats.api.httpMaxBatchItems);
            appendPlain(out, stats.api.httpMaxScanItems);
            appendPlain(out, stats.api.httpMaxHistoryEntries);
            appendPlain(out, stats.api.httpMaxContentLength);
            appendPlain(out, stats.api.httpConnectionsAcceptedTotal);
            appendPlain(out, stats.api.httpConnectionsClosedTotal);
            appendPlain(out, stats.api.httpConnectionsActive);
            appendPlain(out, stats.api.httpRequestsTotal);
            appendPlain(out, stats.api.httpResponsesTotal);
            appendPlain(out, stats.api.httpBytesReceivedTotal);
            appendPlain(out, stats.api.httpBytesSentTotal);
            appendPlain(out, stats.api.httpProtocolErrorsTotal);
            appendPlain(out, stats.api.httpErrorsTotal);
            appendPlain(out, stats.api.httpBatchPutItemsTotal);
            appendPlain(out, stats.api.httpBatchGetItemsTotal);
            appendPlain(out, static_cast<uint8_t>(stats.api.tcpEnabled));
            appendPlain(out, static_cast<uint8_t>(stats.api.tcpTlsEnabled));
            appendPlain(out, stats.api.tcpIoBackend);
            appendPlain(out, stats.api.tcpWorkerThreads);
            appendPlain(out, stats.api.tcpAcceptQueueLimit);
            appendPlain(out, stats.api.tcpAcceptQueueTimeoutMs);
            appendPlain(out, stats.api.tcpListenBacklog);
            appendPlain(out, stats.api.tcpReadTimeoutMs);
            appendPlain(out, stats.api.tcpWriteTimeoutMs);
            appendPlain(out, stats.api.tcpConnectionsAcceptedTotal);
            appendPlain(out, stats.api.tcpConnectionsClosedTotal);
            appendPlain(out, stats.api.tcpConnectionsActive);
            appendPlain(out, stats.api.tcpAcceptQueueDepth);
            appendPlain(out, stats.api.tcpAcceptQueuePeakDepth);
            appendPlain(out, stats.api.tcpAcceptQueueRejectedTotal);
            appendPlain(out, stats.api.tcpAcceptQueueExpiredTotal);
            appendPlain(out, stats.api.tcpRequestsTotal);
            appendPlain(out, stats.api.tcpResponsesTotal);
            appendPlain(out, stats.api.tcpBytesReceivedTotal);
            appendPlain(out, stats.api.tcpBytesSentTotal);
            appendPlain(out, stats.api.tcpProtocolErrorsTotal);
            appendPlain(out, stats.api.tcpCrcErrorsTotal);
            appendPlain(out, stats.api.tcpPipelineBatchesTotal);
            appendPlain(out, stats.api.tcpBackpressureFlushesTotal);
            appendPlain(out, stats.api.tcpBackpressureDisconnectsTotal);
            appendPlain(out, stats.api.tcpBatchPutItemsTotal);
            appendPlain(out, stats.api.tcpBatchGetItemsTotal);
            appendPlain(out, static_cast<uint8_t>(stats.api.grpcEnabled));
            appendPlain(out, static_cast<uint8_t>(stats.api.grpcTlsEnabled));
            appendPlain(out, stats.api.grpcPort);
            appendPlain(out, stats.api.grpcWorkerThreads);
            appendPlain(out, stats.api.grpcCompletionQueues);
            appendPlain(out, stats.api.grpcMinPollers);
            appendPlain(out, stats.api.grpcMaxPollers);
            appendPlain(out, stats.api.grpcMaxConcurrentStreams);
            appendPlain(out, stats.api.grpcResourceQuotaBytes);
            appendPlain(out, stats.api.grpcMaxBatchItems);
            appendPlain(out, stats.api.grpcMaxScanItems);
            appendPlain(out, stats.api.grpcMaxHistoryEntries);
            appendPlain(out, stats.api.grpcRequestsTotal);
            appendPlain(out, stats.api.grpcResponsesTotal);
            appendPlain(out, stats.api.grpcActiveRequests);
            appendPlain(out, stats.api.grpcErrorsTotal);
            appendPlain(out, stats.api.grpcBatchPutItemsTotal);
            appendPlain(out, stats.api.grpcBatchGetItemsTotal);

            appendPlain(out, stats.memtable.shardCount);
            appendPlain(out, stats.memtable.thresholdBytesPerShard);
            appendPlain(out, stats.memtable.approxBytes);
            appendPlain(out, stats.memtable.putsApplied);
            appendPlain(out, stats.memtable.removesApplied);
            appendPlain(out, stats.memtable.flushesCompleted);
            appendPlain(out, stats.memtable.bytesFlushed);

            appendPlain(out, static_cast<uint8_t>(stats.wal.enabled));
            appendPlain(out, stats.wal.shardCount);
            appendPlain(out, stats.wal.entriesWritten);
            appendPlain(out, stats.wal.bytesWritten);
            appendPlain(out, stats.wal.batchesFlushed);
            appendPlain(out, stats.wal.syncsExecuted);
            appendPlain(out, stats.wal.segmentRotations);

            appendPlain(out, static_cast<uint8_t>(stats.blob.enabled));
            appendPlain(out, stats.blob.thresholdBytes);
            appendPlain(out, stats.blob.blobsWritten);
            appendPlain(out, stats.blob.bytesUncompressed);
            appendPlain(out, stats.blob.bytesOnDisk);
            appendPlain(out, stats.blob.blobsDeleted);
            appendPlain(out, stats.blob.gcCycles);

            appendPlain(out, static_cast<uint8_t>(stats.sst.enabled));
            appendPlain(out, static_cast<uint32_t>(stats.sst.levels.size()));
            for (const auto& level : stats.sst.levels) {
                appendPlain(out, static_cast<int32_t>(level.level));
                appendPlain(out, static_cast<uint64_t>(level.fileCount));
                appendPlain(out, level.bytes);
                appendPlain(out, level.budgetBytes);
            }
            appendPlain(out, static_cast<uint64_t>(stats.sst.fileCount));
            appendPlain(out, stats.sst.bytes);
            appendPlain(out, static_cast<uint64_t>(stats.sst.l0FileCount));
            appendPlain(out, static_cast<uint8_t>(stats.sst.compactionPending));
            appendPlain(out, stats.sst.compactionsCompleted);
            appendPlain(out, stats.sst.filesCompacted);
            appendPlain(out, stats.sst.bytesCompactedIn);
            appendPlain(out, stats.sst.bytesCompactedOut);
            appendPlain(out, stats.sst.l0Stalls);

            appendPlain(out, static_cast<uint8_t>(stats.vlog.enabled));
            appendPlain(out, stats.vlog.syncMode);
            appendPlain(out, stats.vlog.groupN);
            appendPlain(out, stats.vlog.groupMicros);
            appendPlain(out, stats.vlog.groupBytes);
            appendPlain(out, stats.vlog.asyncMaxPendingBytes);
            appendPlain(out, stats.vlog.indexedKeys);
            appendPlain(out, stats.vlog.indexedEntries);
            appendPlain(out, stats.vlog.rollbackEntries);
            appendPlain(out, stats.vlog.pendingWrites);
            appendPlain(out, stats.vlog.pendingBytes);
            appendPlain(out, stats.vlog.durableBytes);
            appendPlain(out, static_cast<uint8_t>(stats.vlog.flushThreadRunning));
            appendPlain(out, stats.vlog.segmentCount);
            appendPlain(out, stats.vlog.activeSegmentBytes);
            appendPlain(out, stats.vlog.recoveryDurationMicros);
            appendPlain(out, stats.vlog.recoveredSegmentCount);
            appendPlain(out, stats.vlog.recoveredEntryCount);
            appendPlain(out, stats.vlog.sidecarFallbackCount);
            appendPlain(out, stats.vlog.sidecarRebuildFailures);
            appendPlain(out, stats.vlog.retentionPrunedSegments);
            appendPlain(out, stats.vlog.retentionBaseEntriesWritten);
            appendPlain(out, stats.vlog.parallelQueueRejects);
            appendPlain(out, stats.vlog.parallelLaneCount);
            appendPlain(out, stats.vlog.parallelPendingWrites);
            appendPlain(out, stats.vlog.parallelPendingBytes);
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

    EngineStats::ApiStats HttpApiServer::stats() const noexcept {
        EngineStats::ApiStats out;
        out.enabled = running_.load(std::memory_order_acquire);
        out.httpEnabled = true;
        out.httpTlsEnabled = options_.transportMode == AkkEngineOptions::ApiTransportMode::TLS;
        out.httpPort = options_.httpPort;
        out.httpMaxBatchItems = maxBatchItems();
        out.httpMaxScanItems = maxScanItems();
        out.httpMaxHistoryEntries = maxHistoryEntries();
        out.httpMaxContentLength = maxContentLength();
        out.httpConnectionsAcceptedTotal = connectionsAcceptedTotal_.load(std::memory_order_relaxed);
        out.httpConnectionsClosedTotal = connectionsClosedTotal_.load(std::memory_order_relaxed);
        {
            std::lock_guard lock(clientsMu_);
            out.httpConnectionsActive = activeClients_.size();
        }
        out.httpRequestsTotal = requestsTotal_.load(std::memory_order_relaxed);
        out.httpResponsesTotal = responsesTotal_.load(std::memory_order_relaxed);
        out.httpBytesReceivedTotal = bytesReceivedTotal_.load(std::memory_order_relaxed);
        out.httpBytesSentTotal = bytesSentTotal_.load(std::memory_order_relaxed);
        out.httpProtocolErrorsTotal = protocolErrorsTotal_.load(std::memory_order_relaxed);
        out.httpErrorsTotal = errorsTotal_.load(std::memory_order_relaxed);
        out.httpBatchPutItemsTotal = batchPutItemsTotal_.load(std::memory_order_relaxed);
        out.httpBatchGetItemsTotal = batchGetItemsTotal_.load(std::memory_order_relaxed);
        return out;
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
            connectionsAcceptedTotal_.fetch_add(1, std::memory_order_relaxed);

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
                        if (options_.transportMode == AkkEngineOptions::ApiTransportMode::TLS) {
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
                        connectionsClosedTotal_.fetch_add(1, std::memory_order_relaxed);
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

    bool HttpApiServer::readRequest(detail::Connection& connection, ParsedRequest& request, bool& protocolError) {
        protocolError = false;
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
        if (sp1 == std::string::npos || sp2 == std::string::npos) {
            protocolError = true;
            return false;
        }

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
                if (result.ec != std::errc{} || contentLength > maxContentLength()) {
                    protocolError = true;
                    return false;
                }
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
        bytesReceivedTotal_.fetch_add(contentLength, std::memory_order_relaxed);
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

    bool HttpApiServer::wantsStreaming(std::string_view value) noexcept {
        if (value.empty()) { return false; }

        std::string lowered;
        lowered.reserve(value.size());
        for (const char c : value) { lowered.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c)))); }

        return lowered != "0" && lowered != "false" && lowered != "off" && lowered != "no";
    }

    bool HttpApiServer::sendResponse(detail::Connection& connection, int statusCode, std::span<const uint8_t> body) {
        const std::string header = "HTTP/1.1 " + std::to_string(statusCode) + " " + std::string{reasonPhrase(statusCode)} + "\r\n"
            "Content-Type: application/octet-stream\r\n" "Content-Length: " + std::to_string(body.size()) + "\r\n" "\r\n";
        if (!connection.sendAll(reinterpret_cast<const uint8_t*>(header.data()), header.size())) { return false; }
        const bool sent = body.empty() || connection.sendAll(body.data(), body.size());
        if (sent) {
            responsesTotal_.fetch_add(1, std::memory_order_relaxed);
            bytesSentTotal_.fetch_add(header.size() + body.size(), std::memory_order_relaxed);
            if (statusCode >= 400) { errorsTotal_.fetch_add(1, std::memory_order_relaxed); }
        }
        return sent;
    }

    bool HttpApiServer::sendChunkedResponseHeader(
        detail::Connection& connection,
        int statusCode,
        std::string_view contentType,
        uint64_t& bytesSent
    ) {
        const std::string header = "HTTP/1.1 " + std::to_string(statusCode) + " " + std::string{reasonPhrase(statusCode)} + "\r\n"
            "Content-Type: " + std::string{contentType} + "\r\n" "Transfer-Encoding: chunked\r\n" "X-Akkara-Stream-Version: 1\r\n" "\r\n";
        if (!connection.sendAll(reinterpret_cast<const uint8_t*>(header.data()), header.size())) { return false; }
        bytesSent += header.size();
        return true;
    }

    bool HttpApiServer::sendChunk(detail::Connection& connection, std::span<const uint8_t> body, uint64_t& bytesSent) {
        std::array < char, 32 > sizeBuffer{};
        const auto result = std::to_chars(sizeBuffer.data(), sizeBuffer.data() + sizeBuffer.size(), body.size(), 16);
        if (result.ec != std::errc{}) { return false; }

        static constexpr std::string_view kCrLf = "\r\n";
        const size_t sizeWidth = static_cast<size_t>(result.ptr - sizeBuffer.data());
        if (!connection.sendAll(reinterpret_cast<const uint8_t*>(sizeBuffer.data()), sizeWidth) || !connection.sendAll(
            reinterpret_cast<const uint8_t*>(kCrLf.data()),
            kCrLf.size()
        ) || (!body.empty() && !connection.sendAll(body.data(), body.size())) || !connection.sendAll(
            reinterpret_cast<const uint8_t*>(kCrLf.data()),
            kCrLf.size()
        )) { return false; }

        bytesSent += sizeWidth + kCrLf.size() + body.size() + kCrLf.size();
        return true;
    }

    bool HttpApiServer::finishChunkedResponse(detail::Connection& connection, int statusCode, uint64_t bytesSent) {
        static constexpr std::string_view kChunkedEnd = "0\r\n\r\n";
        if (!connection.sendAll(reinterpret_cast<const uint8_t*>(kChunkedEnd.data()), kChunkedEnd.size())) { return false; }
        responsesTotal_.fetch_add(1, std::memory_order_relaxed);
        bytesSentTotal_.fetch_add(bytesSent + kChunkedEnd.size(), std::memory_order_relaxed);
        if (statusCode >= 400) { errorsTotal_.fetch_add(1, std::memory_order_relaxed); }
        return true;
    }

    bool HttpApiServer::sendText(detail::Connection& connection, int statusCode, std::string_view body) {
        return sendResponse(connection, statusCode, {reinterpret_cast<const uint8_t*>(body.data()), body.size()});
    }

    bool HttpApiServer::sendEmpty(detail::Connection& connection, int statusCode) { return sendResponse(connection, statusCode, {}); }

    bool HttpApiServer::route(detail::Connection& connection, const ParsedRequest& request, std::vector<uint8_t>& valueBuffer) {
        requestsTotal_.fetch_add(1, std::memory_order_relaxed);

        const bool needsKey = request.path == "/v1/put" || request.path == "/v1/putHinted" || request.path == "/v1/get" || request.path ==
            "/v1/remove" || request.path == "/v1/removeHinted" || request.path == "/v1/getAt" || request.path == "/v1/exists" || request.
            path == "/v1/history" || request.path == "/v1/rollbackKey";
        const std::string rawKey = queryParam(request.query, "key");
        if (needsKey && rawKey.empty()) {
            sendEmpty(connection, 400);
            return request.keepAlive;
        }

        const std::vector<uint8_t> key = urlDecode(rawKey);
        const std::span<const uint8_t> keySpan{key.data(), key.size()};

        try {
            if (request.path == "/v1/put" && request.method == "POST") {
                engine_.put(keySpan, std::span<const uint8_t>{request.body.data(), request.body.size()});
                sendEmpty(connection, 204);
            }
            else if (request.path == "/v1/putHinted" && request.method == "POST") {
                uint64_t fp64 = 0;
                uint64_t miniKey = 0;
                if (!readU64Text(queryParam(request.query, "fp64"), fp64) || !readU64Text(queryParam(request.query, "miniKey"), miniKey)) {
                    sendEmpty(connection, 400);
                    return request.keepAlive;
                }
                engine_.putHinted(keySpan, std::span<const uint8_t>{request.body.data(), request.body.size()}, fp64, miniKey);
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
            else if (request.path == "/v1/removeHinted" && request.method == "DELETE") {
                uint64_t fp64 = 0;
                uint64_t miniKey = 0;
                if (!readU64Text(queryParam(request.query, "fp64"), fp64) || !readU64Text(queryParam(request.query, "miniKey"), miniKey)) {
                    sendEmpty(connection, 400);
                    return request.keepAlive;
                }
                engine_.removeHinted(keySpan, fp64, miniKey);
                sendEmpty(connection, 204);
            }
            else if (request.path == "/v1/getAt" && request.method == "GET") {
                uint64_t seq = 0;
                if (!readU64Text(queryParam(request.query, "seq"), seq)) {
                    sendEmpty(connection, 400);
                    return request.keepAlive;
                }
                auto value = engine_.getAt(keySpan, seq);
                if (value) { sendResponse(connection, 200, std::span<const uint8_t>{value->data(), value->size()}); }
                else { sendEmpty(connection, 404); }
            }
            else if (request.path == "/v1/exists" && request.method == "GET") {
                encodeBool(engine_.exists(keySpan), valueBuffer);
                sendResponse(connection, 200, std::span<const uint8_t>{valueBuffer.data(), valueBuffer.size()});
            }
            else if (request.path == "/v1/count" && request.method == "GET") {
                const auto start = urlDecode(queryParam(request.query, "start"));
                const auto end = urlDecode(queryParam(request.query, "end"));
                encodeU64(
                    static_cast<uint64_t>(engine_.count(
                        std::span<const uint8_t>{start.data(), start.size()},
                        std::span<const uint8_t>{end.data(), end.size()}
                    )),
                    valueBuffer
                );
                sendResponse(connection, 200, std::span<const uint8_t>{valueBuffer.data(), valueBuffer.size()});
            }
            else if (request.path == "/v1/scan" && request.method == "GET") {
                uint64_t requestedLimit = 0;
                const auto limitText = queryParam(request.query, "limit");
                if (!limitText.empty() && !readU64Text(limitText, requestedLimit)) {
                    sendEmpty(connection, 400);
                    return request.keepAlive;
                }
                const uint32_t limit = requestedLimit == 0
                                           ? maxScanItems()
                                           : static_cast<uint32_t>(std::min<uint64_t>(requestedLimit, maxScanItems()));
                const auto start = urlDecode(queryParam(request.query, "start"));
                const auto end = urlDecode(queryParam(request.query, "end"));
                const bool stream = wantsStreaming(queryParam(request.query, "stream"));
                core::BufferArena arena;
                auto scanRange = engine_.scan(
                    arena,
                    std::span<const uint8_t>{start.data(), start.size()},
                    std::span<const uint8_t>{end.data(), end.size()}
                );
                if (stream) {
                    uint64_t sentBytes = 0;
                    if (!sendChunkedResponseHeader(connection, 200, "application/vnd.akkaradb.scan-stream", sentBytes) || !sendChunk(
                        connection,
                        std::span<const uint8_t>{kHttpScanStreamPrelude.data(), kHttpScanStreamPrelude.size()},
                        sentBytes
                    )) { return false; }

                    uint32_t emitted = 0;
                    bool truncated = false;
                    for (const auto& record : scanRange) {
                        if (limit != 0 && emitted >= limit) {
                            truncated = true;
                            break;
                        }
                        encodeScanStreamFrame(record, valueBuffer);
                        if (!sendChunk(connection, std::span<const uint8_t>{valueBuffer.data(), valueBuffer.size()}, sentBytes)) {
                            return false;
                        }
                        ++emitted;
                    }

                    encodeStreamEndFrame(emitted, truncated, valueBuffer);
                    if (!sendChunk(connection, std::span<const uint8_t>{valueBuffer.data(), valueBuffer.size()}, sentBytes)) {
                        return false;
                    }
                    if (!finishChunkedResponse(connection, 200, sentBytes)) { return false; }
                }
                else {
                    std::vector<AkkEngine::ScanRecordView> records;
                    records.reserve(limit);
                    bool truncated = false;
                    for (const auto& record : scanRange) {
                        if (records.size() >= limit) {
                            truncated = true;
                            break;
                        }
                        records.push_back(record);
                    }
                    encodeScan(std::span<const AkkEngine::ScanRecordView>{records.data(), records.size()}, truncated, valueBuffer);
                    sendResponse(connection, 200, std::span<const uint8_t>{valueBuffer.data(), valueBuffer.size()});
                }
            }
            else if (request.path == "/v1/history" && request.method == "GET") {
                const bool stream = wantsStreaming(queryParam(request.query, "stream"));
                auto entries = engine_.history(keySpan);
                if (stream) {
                    uint64_t sentBytes = 0;
                    if (!sendChunkedResponseHeader(connection, 200, "application/vnd.akkaradb.history-stream", sentBytes) || !sendChunk(
                        connection,
                        std::span<const uint8_t>{kHttpHistoryStreamPrelude.data(), kHttpHistoryStreamPrelude.size()},
                        sentBytes
                    )) { return false; }

                    const uint32_t maxEntries = maxHistoryEntries();
                    uint32_t emitted = 0;
                    bool truncated = false;
                    for (const auto& entry : entries) {
                        if (maxEntries != 0 && emitted >= maxEntries) {
                            truncated = true;
                            break;
                        }
                        encodeHistoryStreamFrame(entry, valueBuffer);
                        if (!sendChunk(connection, std::span<const uint8_t>{valueBuffer.data(), valueBuffer.size()}, sentBytes)) {
                            return false;
                        }
                        ++emitted;
                    }

                    encodeStreamEndFrame(emitted, truncated, valueBuffer);
                    if (!sendChunk(connection, std::span<const uint8_t>{valueBuffer.data(), valueBuffer.size()}, sentBytes)) {
                        return false;
                    }
                    if (!finishChunkedResponse(connection, 200, sentBytes)) { return false; }
                }
                else {
                    bool truncated = false;
                    const uint32_t maxEntries = maxHistoryEntries();
                    if (maxEntries != 0 && entries.size() > maxEntries) {
                        entries.resize(maxEntries);
                        truncated = true;
                    }
                    encodeHistory(std::span<const VersionEntry>{entries.data(), entries.size()}, truncated, valueBuffer);
                    sendResponse(connection, 200, std::span<const uint8_t>{valueBuffer.data(), valueBuffer.size()});
                }
            }
            else if (request.path == "/v1/rollbackTo" && request.method == "POST") {
                uint64_t targetSeq = 0;
                if (!readU64Text(queryParam(request.query, "seq"), targetSeq)) {
                    sendEmpty(connection, 400);
                    return request.keepAlive;
                }
                engine_.rollbackTo(targetSeq);
                sendEmpty(connection, 204);
            }
            else if (request.path == "/v1/rollbackKey" && request.method == "POST") {
                uint64_t targetSeq = 0;
                if (!readU64Text(queryParam(request.query, "seq"), targetSeq)) {
                    sendEmpty(connection, 400);
                    return request.keepAlive;
                }
                engine_.rollbackKey(keySpan, targetSeq);
                sendEmpty(connection, 204);
            }
            else if (request.path == "/v1/batchPut" && request.method == "POST") {
                uint32_t count = 0;
                std::vector<ApiBatchPutItem> items;
                if (request.body.size() < sizeof(uint32_t)) {
                    sendEmpty(connection, 400);
                    return request.keepAlive;
                }
                std::memcpy(&count, request.body.data(), sizeof(uint32_t));
                if (count > maxBatchItems() || !decodeHttpBatchPut(
                    std::span<const uint8_t>{request.body.data(), request.body.size()},
                    maxBatchItems(),
                    items
                )) {
                    sendEmpty(connection, 400);
                    return request.keepAlive;
                }
                std::vector<AkkEngine::BatchPutEntry> entries;
                entries.reserve(items.size());
                for (const auto& item : items) { entries.push_back({item.key, item.value}); }
                engine_.putBatch(std::span<const AkkEngine::BatchPutEntry>{entries.data(), entries.size()});
                batchPutItemsTotal_.fetch_add(items.size(), std::memory_order_relaxed);
                sendEmpty(connection, 204);
            }
            else if (request.path == "/v1/batchGet" && request.method == "POST") {
                std::vector<std::span<const uint8_t>> keys;
                if (!decodeHttpBatchGet(std::span<const uint8_t>{request.body.data(), request.body.size()}, maxBatchItems(), keys)) {
                    sendEmpty(connection, 400);
                    return request.keepAlive;
                }
                const auto values = engine_.getBatch(std::span<const std::span<const uint8_t>>{keys.data(), keys.size()});
                std::vector<ApiBatchGetResult> results;
                results.reserve(values.size());
                for (const auto& value : values) {
                    results.push_back(
                        ApiBatchGetResult{
                            value.found ? ApiStatus::OK : ApiStatus::NOT_FOUND,
                            std::span<const uint8_t>{value.value.data(), value.value.size()}
                        }
                    );
                }
                encodeHttpBatchGet(std::span<const ApiBatchGetResult>{results.data(), results.size()}, valueBuffer);
                batchGetItemsTotal_.fetch_add(keys.size(), std::memory_order_relaxed);
                sendResponse(connection, 200, std::span<const uint8_t>{valueBuffer.data(), valueBuffer.size()});
            }
            else if (request.path == "/v1/forceSync" && request.method == "POST") {
                engine_.forceSync();
                sendEmpty(connection, 204);
            }
            else if (request.path == "/v1/forceFlush" && request.method == "POST") {
                engine_.forceFlush();
                sendEmpty(connection, 204);
            }
            else if (request.path == "/v1/runBlobGc" && request.method == "POST") {
                engine_.runBlobGc();
                sendEmpty(connection, 204);
            }
            else if (request.path == "/v1/stats" && request.method == "GET") {
                encodeStatsPayload(engine_.stats(), valueBuffer);
                sendResponse(connection, 200, std::span<const uint8_t>{valueBuffer.data(), valueBuffer.size()});
            }
            else if (request.path == "/v1/ping" && request.method == "GET") {
                static constexpr std::string_view pong = "pong";
                sendText(connection, 200, pong);
            }
            else { sendEmpty(connection, 404); }
        }
        catch (...) {
            errorsTotal_.fetch_add(1, std::memory_order_relaxed);
            sendEmpty(connection, 500);
        }

        return request.keepAlive;
    }

    void HttpApiServer::handleConnection(detail::Connection& connection) {
        std::vector<uint8_t> valueBuffer;
        while (running_.load(std::memory_order_relaxed)) {
            ParsedRequest request;
            bool protocolError = false;
            if (!readRequest(connection, request, protocolError)) {
                if (protocolError) { protocolErrorsTotal_.fetch_add(1, std::memory_order_relaxed); }
                break;
            }
            if (!route(connection, request, valueBuffer)) { break; }
        }
    }

    uint32_t HttpApiServer::maxBatchItems() const noexcept { return options_.httpMaxBatchItems == 0 ? 4096u : options_.httpMaxBatchItems; }

    uint32_t HttpApiServer::maxScanItems() const noexcept { return options_.httpMaxScanItems == 0 ? 4096u : options_.httpMaxScanItems; }

    uint32_t HttpApiServer::maxHistoryEntries() const noexcept {
        return options_.httpMaxHistoryEntries == 0 ? 4096u : options_.httpMaxHistoryEntries;
    }

    uint64_t HttpApiServer::maxContentLength() const noexcept {
        return options_.httpMaxContentLength == 0 ? 64ULL * 1024ULL * 1024ULL : options_.httpMaxContentLength;
    }
}

extern "C" AKKARADB_API_SERVER_API bool akkaradb_api_http_register() noexcept {
    return akkaradb::engine::server::registerAkkApiTransportFactory(
        akkaradb::engine::AkkEngineOptions::ApiBackend::HTTP,
        [](akkaradb::engine::AkkEngine& engine, const akkaradb::engine::AkkEngineOptions::ApiOptions& options) {
            return std::unique_ptr<akkaradb::engine::server::IAkkApiTransport>{
                akkaradb::engine::server::HttpApiServer::create(engine, options).release()
            };
        }
    );
}

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

// akkserver/src/tcp/TcpApiServer.cpp
#include "akk/engine/server/TcpApiServer.hpp"

#include "akk/engine/server/ApiFraming.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <thread>

namespace akkaradb::engine::server {
    namespace {
        constexpr uint32_t kMaxValueBytes = 64u * 1024u * 1024u;
        constexpr uint32_t kDefaultMaxBatchItems = 4096u;
        constexpr size_t kReadBufferBytes = 64u * 1024u;
        constexpr uint8_t kStreamFrameItem = 1;
        constexpr uint8_t kStreamFrameEnd = 2;

        [[nodiscard]] bool validMagic(const ApiRequestHeader& header) noexcept {
            return std::memcmp(header.magic, REQUEST_MAGIC, sizeof(header.magic)) == 0;
        }

        [[nodiscard]] bool validHeader(const ApiRequestHeader& header) noexcept {
            return validMagic(header) && header.version == PROTOCOL_VERSION && header.valLen <= kMaxValueBytes;
        }

        [[nodiscard]] bool readBatchCount(std::span<const uint8_t> payload, uint32_t& out) noexcept {
            if (payload.size() < sizeof(uint32_t)) { return false; }
            std::memcpy(&out, payload.data(), sizeof(uint32_t));
            return true;
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

        [[nodiscard]] bool readU64(std::span<const uint8_t> payload, uint64_t& out) noexcept {
            if (payload.size() != sizeof(out)) { return false; }
            std::memcpy(&out, payload.data(), sizeof(out));
            return true;
        }

        [[nodiscard]] bool readScanPayload(std::span<const uint8_t> payload, uint32_t& limit, std::span<const uint8_t>& endKey) noexcept {
            if (payload.empty()) {
                limit = 0;
                endKey = {};
                return true;
            }
            if (payload.size() < sizeof(limit)) { return false; }
            std::memcpy(&limit, payload.data(), sizeof(limit));
            endKey = payload.subspan(sizeof(limit));
            return true;
        }

        [[nodiscard]] bool readOptionalLimit(std::span<const uint8_t> payload, uint32_t& limit) noexcept {
            if (payload.empty()) {
                limit = 0;
                return true;
            }
            if (payload.size() != sizeof(limit)) { return false; }
            std::memcpy(&limit, payload.data(), sizeof(limit));
            return true;
        }

        void encodeBoolPayload(bool value, std::vector<uint8_t>& out) {
            out.clear();
            out.push_back(value ? uint8_t{1} : uint8_t{0});
        }

        void encodeU64Payload(uint64_t value, std::vector<uint8_t>& out) {
            out.clear();
            appendPlain(out, value);
        }

        void encodeScanPayload(std::span<const AkkEngine::ScanRecordView> records, bool truncated, std::vector<uint8_t>& out) {
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

        void encodeHistoryPayload(std::span<const VersionEntry> entries, std::vector<uint8_t>& out) {
            out.clear();
            appendPlain(out, static_cast<uint32_t>(entries.size()));
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

        void encodeScanStreamPayload(const AkkEngine::ScanRecordView& record, std::vector<uint8_t>& out) {
            const uint32_t payloadBytes = static_cast<uint32_t>(sizeof(uint16_t) + sizeof(uint32_t) + record.key.size() + record.value.
                size());
            appendStreamFrameHeader(out, kStreamFrameItem, payloadBytes);
            appendPlain(out, static_cast<uint16_t>(record.key.size()));
            appendPlain(out, static_cast<uint32_t>(record.value.size()));
            appendBytes(out, record.key);
            appendBytes(out, record.value);
        }

        void encodeHistoryStreamPayload(const VersionEntry& entry, std::vector<uint8_t>& out) {
            const uint32_t payloadBytes = static_cast<uint32_t>(sizeof(entry.seq) + sizeof(entry.sourceNodeId) + sizeof(entry.timestampNs) +
                sizeof(uint32_t) + sizeof(uint32_t) + entry.value.size());
            appendStreamFrameHeader(out, kStreamFrameItem, payloadBytes);
            appendPlain(out, entry.seq);
            appendPlain(out, entry.sourceNodeId);
            appendPlain(out, entry.timestampNs);
            appendPlain(out, static_cast<uint32_t>(entry.flags));
            appendPlain(out, static_cast<uint32_t>(entry.value.size()));
            appendBytes(out, std::span<const uint8_t>{entry.value.data(), entry.value.size()});
        }

        void encodeStreamEndPayload(uint32_t emitted, bool truncated, std::vector<uint8_t>& out) {
            appendStreamFrameHeader(out, kStreamFrameEnd, static_cast<uint32_t>(sizeof(emitted) + sizeof(uint8_t)));
            appendPlain(out, emitted);
            appendPlain(out, static_cast<uint8_t>(truncated ? 1 : 0));
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
        }

        void closeClientSocket(detail::SocketHandle client) noexcept {
            detail::shutdownSocket(client);
            detail::closeSocket(client);
        }

        struct RequestFrame {
            ApiRequestHeader header{};
            std::span<const uint8_t> key;
            std::span<const uint8_t> value;
            uint32_t receivedCrc = 0;
            size_t wireSize = 0;
            bool requestIdUsable = false;
        };

        enum class FrameReadStatus : uint8_t {
            OK, NEED_MORE, CLOSED, INVALID,
        };

        class BufferedInput {
            public:
                BufferedInput() { buffer_.reserve(kReadBufferBytes); }

                FrameReadStatus readFrame(detail::Connection& connection, RequestFrame& out, bool blocking) {
                    if (!ensure(connection, sizeof(ApiRequestHeader), blocking)) {
                        return blocking ? FrameReadStatus::CLOSED : FrameReadStatus::NEED_MORE;
                    }

                    std::memcpy(&out.header, buffer_.data() + pos_, sizeof(out.header));
                    out.requestIdUsable = validMagic(out.header);
                    out.wireSize = sizeof(ApiRequestHeader);
                    if (!validHeader(out.header)) { return FrameReadStatus::INVALID; }

                    const size_t keyLen = out.header.keyLen;
                    const size_t valueLen = out.header.valLen;
                    if (valueLen > kMaxValueBytes) { return FrameReadStatus::INVALID; }
                    if (keyLen > std::numeric_limits<size_t>::max() - sizeof(ApiRequestHeader) - sizeof(uint32_t) - valueLen) {
                        return FrameReadStatus::INVALID;
                    }

                    const size_t total = sizeof(ApiRequestHeader) + keyLen + valueLen + sizeof(uint32_t);
                    if (!ensure(connection, total, blocking)) { return blocking ? FrameReadStatus::CLOSED : FrameReadStatus::NEED_MORE; }

                    const uint8_t* p = buffer_.data() + pos_ + sizeof(ApiRequestHeader);
                    out.key = {p, keyLen};
                    p += keyLen;
                    out.value = {p, valueLen};
                    p += valueLen;
                    std::memcpy(&out.receivedCrc, p, sizeof(out.receivedCrc));
                    out.wireSize = total;
                    return FrameReadStatus::OK;
                }

                void consume(size_t size) noexcept {
                    pos_ += size;
                    if (pos_ >= buffer_.size()) {
                        buffer_.clear();
                        pos_ = 0;
                    }
                }

            private:
                [[nodiscard]] size_t available() const noexcept { return buffer_.size() - pos_; }

                bool ensure(detail::Connection& connection, size_t size, bool blocking) {
                    while (available() < size) {
                        if (!blocking) { return false; }
                        if (!refill(connection)) { return false; }
                    }
                    return true;
                }

                void compact() {
                    if (pos_ == 0) { return; }
                    if (pos_ >= buffer_.size()) {
                        buffer_.clear();
                        pos_ = 0;
                        return;
                    }

                    std::memmove(buffer_.data(), buffer_.data() + pos_, buffer_.size() - pos_);
                    buffer_.resize(buffer_.size() - pos_);
                    pos_ = 0;
                }

                bool refill(detail::Connection& connection) {
                    compact();
                    const size_t offset = buffer_.size();
                    buffer_.resize(offset + kReadBufferBytes);
                    const size_t got = connection.recvSome(buffer_.data() + offset, kReadBufferBytes);
                    if (got == 0) {
                        buffer_.resize(offset);
                        return false;
                    }
                    buffer_.resize(offset + got);
                    return true;
                }

                std::vector<uint8_t> buffer_;
                size_t pos_ = 0;
        };
    }

    TcpApiServer::TcpApiServer(AkkEngine& engine, AkkEngineOptions::ApiOptions options) : engine_{engine}, options_{std::move(options)} {}

    std::unique_ptr<TcpApiServer> TcpApiServer::create(AkkEngine& engine, AkkEngineOptions::ApiOptions options) {
        return std::unique_ptr < TcpApiServer >
        {
            new TcpApiServer{engine, std::move(options)}
        };
    }

    TcpApiServer::~TcpApiServer() { close(); }

    void TcpApiServer::start() {
        if (running_.load(std::memory_order_acquire)) { return; }
        listenSocket_ = detail::listenOn(options_.bindHost, options_.tcpPort, "TcpApiServer", detail::makeSocketTuning(options_));
        try {
            const uint32_t workers = workerCount();
            workerThreads_.reserve(workers);
            running_.store(true, std::memory_order_release);
            for (uint32_t i = 0; i < workers; ++i) { workerThreads_.emplace_back([this] { workerLoop(); }); }
            acceptThread_ = std::thread([this] { acceptLoop(); });
        }
        catch (...) {
            running_.store(false, std::memory_order_release);
            detail::shutdownSocket(listenSocket_);
            detail::closeSocket(listenSocket_);
            listenSocket_ = detail::BAD_SOCKET_VALUE;
            queueCv_.notify_all();
            if (acceptThread_.joinable()) { acceptThread_.join(); }
            for (auto& worker : workerThreads_) { if (worker.joinable()) { worker.join(); } }
            workerThreads_.clear();
            throw;
        }
    }

    void TcpApiServer::close() {
        if (!running_.exchange(false, std::memory_order_acq_rel)) { return; }
        detail::shutdownSocket(listenSocket_);
        detail::closeSocket(listenSocket_);
        listenSocket_ = detail::BAD_SOCKET_VALUE;
        {
            std::lock_guard lock(queueMu_);
            for (const PendingClient& pending : pendingClients_) { closeClientSocket(pending.socket); }
            pendingClients_.clear();
        }
        {
            std::lock_guard lock(activeMu_);
            for (const detail::SocketHandle client : activeClients_) { detail::shutdownSocket(client); }
        }
        queueCv_.notify_all();
        if (acceptThread_.joinable()) { acceptThread_.join(); }
        for (auto& worker : workerThreads_) { if (worker.joinable()) { worker.join(); } }
        workerThreads_.clear();
    }

    void TcpApiServer::acceptLoop() {
        while (running_.load(std::memory_order_acquire)) {
            const detail::SocketHandle client = ::accept(listenSocket_, nullptr, nullptr);
            if (!detail::socketOk(client)) {
                if (!running_.load(std::memory_order_acquire)) { break; }
                if (detail::lastAcceptErrorIsTransient()) { continue; }
                break;
            }
            detail::applySocketTuning(client, detail::makeSocketTuning(options_), true);
            connectionsAcceptedTotal_.fetch_add(1, std::memory_order_relaxed);

            enqueueClient(client);
        }
    }

    void TcpApiServer::enqueueClient(detail::SocketHandle client) {
        if (!running_.load(std::memory_order_acquire)) {
            closeClientSocket(client);
            return;
        }

        std::vector<detail::SocketHandle> expired;
        {
            std::lock_guard lock(queueMu_);
            const Clock::time_point now = Clock::now();
            pruneExpiredPendingLocked(now, expired);
            const uint32_t limit = acceptQueueLimit();
            if (pendingClients_.size() >= limit) {
                acceptQueueRejectedTotal_.fetch_add(1, std::memory_order_relaxed);
                expired.push_back(client);
            }
            else {
                pendingClients_.push_back(PendingClient{client, now});
                recordAcceptQueueDepth(pendingClients_.size());
            }
        }

        for (const detail::SocketHandle expiredClient : expired) { closeClientSocket(expiredClient); }
        if (!expired.empty() && expired.back() == client) { return; }
        queueCv_.notify_one();
    }

    void TcpApiServer::workerLoop() {
        for (;;) {
            detail::SocketHandle client = detail::BAD_SOCKET_VALUE;
            std::vector<detail::SocketHandle> expired;
            {
                std::unique_lock lock(queueMu_);
                for (;;) {
                    queueCv_.wait(lock, [this] { return !running_.load(std::memory_order_acquire) || !pendingClients_.empty(); });
                    if (!running_.load(std::memory_order_acquire)) { break; }
                    pruneExpiredPendingLocked(Clock::now(), expired);
                    if (!pendingClients_.empty()) {
                        client = pendingClients_.front().socket;
                        pendingClients_.pop_front();
                        break;
                    }
                }
            }

            for (const detail::SocketHandle expiredClient : expired) { closeClientSocket(expiredClient); }
            if (!detail::socketOk(client)) { return; }

            {
                std::lock_guard lock(activeMu_);
                activeClients_.insert(client);
            }
            connectionsActive_.fetch_add(1, std::memory_order_relaxed);

            {
                detail::Connection connection{client};
                if (options_.transportMode == AkkEngineOptions::ApiTransportMode::TLS) {
                    try { connection.enableTls(options_.tls); }
                    catch (...) {
                        std::lock_guard lock(activeMu_);
                        activeClients_.erase(client);
                        connectionsActive_.fetch_sub(1, std::memory_order_relaxed);
                        connectionsClosedTotal_.fetch_add(1, std::memory_order_relaxed);
                        continue;
                    }
                }
                handleConnection(connection);
                connection.shutdown();
            }

            {
                std::lock_guard lock(activeMu_);
                activeClients_.erase(client);
            }
            connectionsActive_.fetch_sub(1, std::memory_order_relaxed);
            connectionsClosedTotal_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    void TcpApiServer::handleConnection(detail::Connection& connection) {
        BufferedInput reader;
        std::vector<uint8_t> outputBuffer;
        std::vector<uint8_t> responseBuffer;
        std::vector<uint8_t> writeBuffer;
        std::vector<ApiBatchPutItem> batchPutItems;
        std::vector<AkkEngine::BatchPutEntry> enginePutItems;
        std::vector<std::span<const uint8_t>> batchGetKeys;
        std::vector<AkkEngine::BatchGetResult> engineGetResults;
        std::vector<ApiBatchGetResult> wireGetResults;
        std::vector<AkkEngine::ScanRecordView> scanRecords;

        const auto flushPending = [&]() -> bool {
            if (writeBuffer.empty()) { return true; }
            const size_t bytes = writeBuffer.size();
            if (!connection.sendAll(writeBuffer.data(), writeBuffer.size())) {
                backpressureDisconnectsTotal_.fetch_add(1, std::memory_order_relaxed);
                return false;
            }
            bytesSentTotal_.fetch_add(bytes, std::memory_order_relaxed);
            writeBuffer.clear();
            return true;
        };

        const auto appendResponse = [&](const std::vector<uint8_t>& response) -> bool {
            if (response.empty()) { return true; }

            const uint64_t maxPending = options_.tcpMaxPendingResponseBytes;
            if (maxPending > 0 && !writeBuffer.empty() && writeBuffer.size() + response.size() > maxPending) {
                backpressureFlushesTotal_.fetch_add(1, std::memory_order_relaxed);
                if (!flushPending()) { return false; }
            }

            responsesTotal_.fetch_add(1, std::memory_order_relaxed);
            if (maxPending > 0 && response.size() > maxPending) {
                if (!connection.sendAll(response.data(), response.size())) {
                    backpressureDisconnectsTotal_.fetch_add(1, std::memory_order_relaxed);
                    return false;
                }
                bytesSentTotal_.fetch_add(response.size(), std::memory_order_relaxed);
                return true;
            }

            writeBuffer.insert(writeBuffer.end(), response.begin(), response.end());
            if (maxPending > 0 && writeBuffer.size() >= maxPending) {
                backpressureFlushesTotal_.fetch_add(1, std::memory_order_relaxed);
                return flushPending();
            }
            return true;
        };

        const auto sendResponsePayload = [&](ApiStatus status, uint32_t requestId, std::span<const uint8_t> payload) -> bool {
            encodeResponse(status, requestId, payload, responseBuffer);
            return appendResponse(responseBuffer);
        };

        const auto processFrame = [&](const RequestFrame& frame) -> bool {
            requestsTotal_.fetch_add(1, std::memory_order_relaxed);
            bytesReceivedTotal_.fetch_add(frame.wireSize, std::memory_order_relaxed);
            responseBuffer.clear();

            if (frame.receivedCrc != crc32c(frame.key, frame.value)) {
                crcErrorsTotal_.fetch_add(1, std::memory_order_relaxed);
                encodeError(frame.header.requestId, responseBuffer);
                return false;
            }

            try {
                switch (frame.header.opcode) {
                    case ApiOp::PUT: engine_.put(frame.key, frame.value);
                        encodeResponse(ApiStatus::OK, frame.header.requestId, {}, responseBuffer);
                        break;
                    case ApiOp::GET: outputBuffer.clear();
                        if (engine_.getInto(frame.key, outputBuffer)) {
                            encodeResponse(
                                ApiStatus::OK,
                                frame.header.requestId,
                                std::span<const uint8_t>{outputBuffer.data(), outputBuffer.size()},
                                responseBuffer
                            );
                        }
                        else { encodeResponse(ApiStatus::NOT_FOUND, frame.header.requestId, {}, responseBuffer); }
                        break;
                    case ApiOp::REMOVE: engine_.remove(frame.key);
                        encodeResponse(ApiStatus::OK, frame.header.requestId, {}, responseBuffer);
                        break;
                    case ApiOp::GET_AT: {
                        if (frame.value.size() != sizeof(uint64_t)) {
                            protocolErrorsTotal_.fetch_add(1, std::memory_order_relaxed);
                            encodeError(frame.header.requestId, responseBuffer);
                            return false;
                        }
                        uint64_t seq = 0;
                        std::memcpy(&seq, frame.value.data(), sizeof(seq));
                        auto historicalValue = engine_.getAt(frame.key, seq);
                        if (historicalValue) {
                            encodeResponse(
                                ApiStatus::OK,
                                frame.header.requestId,
                                std::span<const uint8_t>{historicalValue->data(), historicalValue->size()},
                                responseBuffer
                            );
                        }
                        else { encodeResponse(ApiStatus::NOT_FOUND, frame.header.requestId, {}, responseBuffer); }
                        break;
                    }
                    case ApiOp::BATCH_PUT: {
                        uint32_t count = 0;
                        const uint32_t maxBatch = maxBatchItems();
                        if (frame.header.keyLen != 0 || !readBatchCount(frame.value, count) || count > maxBatch || !decodeBatchPut(
                            frame.value,
                            maxBatch,
                            batchPutItems
                        )) {
                            protocolErrorsTotal_.fetch_add(1, std::memory_order_relaxed);
                            encodeError(frame.header.requestId, responseBuffer);
                            return false;
                        }

                        enginePutItems.clear();
                        enginePutItems.reserve(batchPutItems.size());
                        for (const ApiBatchPutItem& item : batchPutItems) { enginePutItems.push_back({item.key, item.value}); }
                        engine_.putBatch(std::span<const AkkEngine::BatchPutEntry>{enginePutItems.data(), enginePutItems.size()});
                        batchPutItemsTotal_.fetch_add(batchPutItems.size(), std::memory_order_relaxed);
                        encodeResponse(ApiStatus::OK, frame.header.requestId, {}, responseBuffer);
                        break;
                    }
                    case ApiOp::BATCH_GET: {
                        uint32_t count = 0;
                        const uint32_t maxBatch = maxBatchItems();
                        if (frame.header.keyLen != 0 || !readBatchCount(frame.value, count) || count > maxBatch || !decodeBatchGet(
                            frame.value,
                            maxBatch,
                            batchGetKeys
                        )) {
                            protocolErrorsTotal_.fetch_add(1, std::memory_order_relaxed);
                            encodeError(frame.header.requestId, responseBuffer);
                            return false;
                        }

                        engineGetResults = engine_.getBatch(
                            std::span<const std::span<const uint8_t>>{batchGetKeys.data(), batchGetKeys.size()}
                        );
                        wireGetResults.clear();
                        wireGetResults.reserve(engineGetResults.size());
                        for (const auto& result : engineGetResults) {
                            wireGetResults.push_back(
                                ApiBatchGetResult{
                                    result.found ? ApiStatus::OK : ApiStatus::NOT_FOUND,
                                    std::span<const uint8_t>{result.value.data(), result.value.size()}
                                }
                            );
                        }
                        batchGetItemsTotal_.fetch_add(batchGetKeys.size(), std::memory_order_relaxed);
                        encodeBatchGetResponse(
                            frame.header.requestId,
                            std::span<const ApiBatchGetResult>{wireGetResults.data(), wireGetResults.size()},
                            responseBuffer
                        );
                        break;
                    }
                    case ApiOp::PING: {
                        static constexpr std::string_view pong = "pong";
                        encodeResponse(
                            ApiStatus::OK,
                            frame.header.requestId,
                            {reinterpret_cast<const uint8_t*>(pong.data()), pong.size()},
                            responseBuffer
                        );
                        break;
                    }
                    case ApiOp::EXISTS: encodeBoolPayload(engine_.exists(frame.key), outputBuffer);
                        encodeResponse(
                            ApiStatus::OK,
                            frame.header.requestId,
                            std::span<const uint8_t>{outputBuffer.data(), outputBuffer.size()},
                            responseBuffer
                        );
                        break;
                    case ApiOp::COUNT: encodeU64Payload(static_cast<uint64_t>(engine_.count(frame.key, frame.value)), outputBuffer);
                        encodeResponse(
                            ApiStatus::OK,
                            frame.header.requestId,
                            std::span<const uint8_t>{outputBuffer.data(), outputBuffer.size()},
                            responseBuffer
                        );
                        break;
                    case ApiOp::SCAN: {
                        uint32_t limit = 0;
                        std::span<const uint8_t> endKey;
                        if (!readScanPayload(frame.value, limit, endKey)) {
                            protocolErrorsTotal_.fetch_add(1, std::memory_order_relaxed);
                            encodeError(frame.header.requestId, responseBuffer);
                            return false;
                        }

                        core::BufferArena arena;
                        scanRecords.clear();
                        bool truncated = false;
                        uint32_t emitted = 0;
                        for (const auto& record : engine_.scan(arena, frame.key, endKey)) {
                            if (limit != 0 && emitted >= limit) {
                                truncated = true;
                                break;
                            }
                            scanRecords.push_back(record);
                            ++emitted;
                        }
                        encodeScanPayload(
                            std::span<const AkkEngine::ScanRecordView>{scanRecords.data(), scanRecords.size()},
                            truncated,
                            outputBuffer
                        );
                        encodeResponse(
                            ApiStatus::OK,
                            frame.header.requestId,
                            std::span<const uint8_t>{outputBuffer.data(), outputBuffer.size()},
                            responseBuffer
                        );
                        break;
                    }
                    case ApiOp::HISTORY: {
                        const auto entries = engine_.history(frame.key);
                        encodeHistoryPayload(std::span<const VersionEntry>{entries.data(), entries.size()}, outputBuffer);
                        encodeResponse(
                            ApiStatus::OK,
                            frame.header.requestId,
                            std::span<const uint8_t>{outputBuffer.data(), outputBuffer.size()},
                            responseBuffer
                        );
                        break;
                    }
                    case ApiOp::SCAN_STREAM: {
                        uint32_t limit = 0;
                        std::span<const uint8_t> endKey;
                        if (!readScanPayload(frame.value, limit, endKey)) {
                            protocolErrorsTotal_.fetch_add(1, std::memory_order_relaxed);
                            encodeError(frame.header.requestId, responseBuffer);
                            return false;
                        }

                        core::BufferArena arena;
                        uint32_t emitted = 0;
                        bool truncated = false;
                        for (const auto& record : engine_.scan(arena, frame.key, endKey)) {
                            if (limit != 0 && emitted >= limit) {
                                truncated = true;
                                break;
                            }
                            encodeScanStreamPayload(record, outputBuffer);
                            if (!sendResponsePayload(
                                ApiStatus::OK,
                                frame.header.requestId,
                                std::span<const uint8_t>{outputBuffer.data(), outputBuffer.size()}
                            )) {
                                responseBuffer.clear();
                                return false;
                            }
                            ++emitted;
                        }

                        encodeStreamEndPayload(emitted, truncated, outputBuffer);
                        if (!sendResponsePayload(
                            ApiStatus::OK,
                            frame.header.requestId,
                            std::span<const uint8_t>{outputBuffer.data(), outputBuffer.size()}
                        )) {
                            responseBuffer.clear();
                            return false;
                        }
                        responseBuffer.clear();
                        break;
                    }
                    case ApiOp::HISTORY_STREAM: {
                        uint32_t limit = 0;
                        if (!readOptionalLimit(frame.value, limit)) {
                            protocolErrorsTotal_.fetch_add(1, std::memory_order_relaxed);
                            encodeError(frame.header.requestId, responseBuffer);
                            return false;
                        }

                        uint32_t emitted = 0;
                        bool truncated = false;
                        for (const auto& entry : engine_.history(frame.key)) {
                            if (limit != 0 && emitted >= limit) {
                                truncated = true;
                                break;
                            }
                            encodeHistoryStreamPayload(entry, outputBuffer);
                            if (!sendResponsePayload(
                                ApiStatus::OK,
                                frame.header.requestId,
                                std::span<const uint8_t>{outputBuffer.data(), outputBuffer.size()}
                            )) {
                                responseBuffer.clear();
                                return false;
                            }
                            ++emitted;
                        }

                        encodeStreamEndPayload(emitted, truncated, outputBuffer);
                        if (!sendResponsePayload(
                            ApiStatus::OK,
                            frame.header.requestId,
                            std::span<const uint8_t>{outputBuffer.data(), outputBuffer.size()}
                        )) {
                            responseBuffer.clear();
                            return false;
                        }
                        responseBuffer.clear();
                        break;
                    }
                    case ApiOp::ROLLBACK_TO: {
                        uint64_t targetSeq = 0;
                        if (!readU64(frame.value, targetSeq)) {
                            protocolErrorsTotal_.fetch_add(1, std::memory_order_relaxed);
                            encodeError(frame.header.requestId, responseBuffer);
                            return false;
                        }
                        engine_.rollbackTo(targetSeq);
                        encodeResponse(ApiStatus::OK, frame.header.requestId, {}, responseBuffer);
                        break;
                    }
                    case ApiOp::ROLLBACK_KEY: {
                        uint64_t targetSeq = 0;
                        if (!readU64(frame.value, targetSeq)) {
                            protocolErrorsTotal_.fetch_add(1, std::memory_order_relaxed);
                            encodeError(frame.header.requestId, responseBuffer);
                            return false;
                        }
                        engine_.rollbackKey(frame.key, targetSeq);
                        encodeResponse(ApiStatus::OK, frame.header.requestId, {}, responseBuffer);
                        break;
                    }
                    case ApiOp::FORCE_SYNC: engine_.forceSync();
                        encodeResponse(ApiStatus::OK, frame.header.requestId, {}, responseBuffer);
                        break;
                    case ApiOp::FORCE_FLUSH: engine_.forceFlush();
                        encodeResponse(ApiStatus::OK, frame.header.requestId, {}, responseBuffer);
                        break;
                    case ApiOp::STATS: encodeStatsPayload(engine_.stats(), outputBuffer);
                        encodeResponse(
                            ApiStatus::OK,
                            frame.header.requestId,
                            std::span<const uint8_t>{outputBuffer.data(), outputBuffer.size()},
                            responseBuffer
                        );
                        break;
                    default: protocolErrorsTotal_.fetch_add(1, std::memory_order_relaxed);
                        encodeError(frame.header.requestId, responseBuffer);
                        return false;
                }
            }
            catch (...) {
                protocolErrorsTotal_.fetch_add(1, std::memory_order_relaxed);
                encodeError(frame.header.requestId, responseBuffer);
                return false;
            }

            return true;
        };

        const uint32_t pipelineLimit = std::max<uint32_t>(1, options_.tcpPipelineBatchLimit);
        while (running_.load(std::memory_order_relaxed)) {
            RequestFrame frame;
            FrameReadStatus status = reader.readFrame(connection, frame, true);
            if (status == FrameReadStatus::CLOSED || status == FrameReadStatus::NEED_MORE) { break; }
            if (status == FrameReadStatus::INVALID) {
                protocolErrorsTotal_.fetch_add(1, std::memory_order_relaxed);
                bytesReceivedTotal_.fetch_add(frame.wireSize, std::memory_order_relaxed);
                if (frame.requestIdUsable) {
                    encodeError(frame.header.requestId, responseBuffer);
                    (void)appendResponse(responseBuffer);
                    (void)flushPending();
                }
                break;
            }

            bool keepConnection = processFrame(frame);
            reader.consume(frame.wireSize);
            if (!appendResponse(responseBuffer)) { break; }

            uint32_t framesInBatch = 1;
            while (keepConnection && framesInBatch < pipelineLimit) {
                RequestFrame pipelined;
                status = reader.readFrame(connection, pipelined, false);
                if (status == FrameReadStatus::NEED_MORE || status == FrameReadStatus::CLOSED) { break; }
                if (status == FrameReadStatus::INVALID) {
                    protocolErrorsTotal_.fetch_add(1, std::memory_order_relaxed);
                    bytesReceivedTotal_.fetch_add(pipelined.wireSize, std::memory_order_relaxed);
                    keepConnection = false;
                    if (pipelined.requestIdUsable) {
                        encodeError(pipelined.header.requestId, responseBuffer);
                        (void)appendResponse(responseBuffer);
                    }
                    break;
                }

                keepConnection = processFrame(pipelined);
                reader.consume(pipelined.wireSize);
                if (!appendResponse(responseBuffer)) { return; }
                ++framesInBatch;
            }

            if (framesInBatch > 1) { pipelineBatchesTotal_.fetch_add(1, std::memory_order_relaxed); }
            if (!flushPending() || !keepConnection) { break; }
        }
    }

    uint32_t TcpApiServer::workerCount() const {
        if (options_.tcpWorkerThreads > 0) { return options_.tcpWorkerThreads; }
        const unsigned hw = std::thread::hardware_concurrency();
        const uint32_t base = hw == 0 ? 4u : hw;
        return std::max(2u, base);
    }

    uint32_t TcpApiServer::acceptQueueLimit() const noexcept {
        return options_.tcpAcceptQueueLimit == 0 ? 4096u : options_.tcpAcceptQueueLimit;
    }

    uint32_t TcpApiServer::acceptQueueTimeoutMs() const noexcept { return options_.tcpAcceptQueueTimeoutMs; }

    void TcpApiServer::pruneExpiredPendingLocked(Clock::time_point now, std::vector<detail::SocketHandle>& expired) {
        const uint32_t timeoutMs = acceptQueueTimeoutMs();
        if (timeoutMs == 0) { return; }

        const auto timeout = std::chrono::milliseconds{timeoutMs};
        while (!pendingClients_.empty() && now - pendingClients_.front().enqueuedAt >= timeout) {
            expired.push_back(pendingClients_.front().socket);
            pendingClients_.pop_front();
            acceptQueueExpiredTotal_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    void TcpApiServer::recordAcceptQueueDepth(size_t depth) noexcept {
        uint64_t observed = acceptQueuePeakDepth_.load(std::memory_order_relaxed);
        while (depth > observed && !acceptQueuePeakDepth_.compare_exchange_weak(
            observed,
            static_cast<uint64_t>(depth),
            std::memory_order_relaxed,
            std::memory_order_relaxed
        )) {}
    }

    uint32_t TcpApiServer::maxBatchItems() const noexcept {
        return options_.tcpMaxBatchItems == 0 ? kDefaultMaxBatchItems : options_.tcpMaxBatchItems;
    }

    AkkEngineOptions::ApiIoBackend TcpApiServer::resolvedIoBackend() const noexcept {
        if (options_.tcpIoBackend != AkkEngineOptions::ApiIoBackend::AUTO) { return options_.tcpIoBackend; }
        return AkkEngineOptions::ApiIoBackend::THREAD_POOL;
    }

    EngineStats::ApiStats TcpApiServer::stats() const noexcept {
        EngineStats::ApiStats out;
        out.enabled = true;
        out.tcpEnabled = true;
        out.tcpTlsEnabled = options_.transportMode == AkkEngineOptions::ApiTransportMode::TLS;
        out.tcpIoBackend = static_cast<uint8_t>(resolvedIoBackend());
        out.tcpWorkerThreads = workerCount();
        out.tcpAcceptQueueLimit = acceptQueueLimit();
        out.tcpAcceptQueueTimeoutMs = acceptQueueTimeoutMs();
        out.tcpListenBacklog = options_.tcpListenBacklog;
        out.tcpReadTimeoutMs = options_.tcpReadTimeoutMs;
        out.tcpWriteTimeoutMs = options_.tcpWriteTimeoutMs;
        out.tcpConnectionsAcceptedTotal = connectionsAcceptedTotal_.load(std::memory_order_relaxed);
        out.tcpConnectionsClosedTotal = connectionsClosedTotal_.load(std::memory_order_relaxed);
        out.tcpConnectionsActive = connectionsActive_.load(std::memory_order_relaxed);
        {
            std::lock_guard lock(queueMu_);
            out.tcpAcceptQueueDepth = pendingClients_.size();
        }
        out.tcpAcceptQueuePeakDepth = acceptQueuePeakDepth_.load(std::memory_order_relaxed);
        out.tcpAcceptQueueRejectedTotal = acceptQueueRejectedTotal_.load(std::memory_order_relaxed);
        out.tcpAcceptQueueExpiredTotal = acceptQueueExpiredTotal_.load(std::memory_order_relaxed);
        out.tcpRequestsTotal = requestsTotal_.load(std::memory_order_relaxed);
        out.tcpResponsesTotal = responsesTotal_.load(std::memory_order_relaxed);
        out.tcpBytesReceivedTotal = bytesReceivedTotal_.load(std::memory_order_relaxed);
        out.tcpBytesSentTotal = bytesSentTotal_.load(std::memory_order_relaxed);
        out.tcpProtocolErrorsTotal = protocolErrorsTotal_.load(std::memory_order_relaxed);
        out.tcpCrcErrorsTotal = crcErrorsTotal_.load(std::memory_order_relaxed);
        out.tcpPipelineBatchesTotal = pipelineBatchesTotal_.load(std::memory_order_relaxed);
        out.tcpBackpressureFlushesTotal = backpressureFlushesTotal_.load(std::memory_order_relaxed);
        out.tcpBackpressureDisconnectsTotal = backpressureDisconnectsTotal_.load(std::memory_order_relaxed);
        out.tcpBatchPutItemsTotal = batchPutItemsTotal_.load(std::memory_order_relaxed);
        out.tcpBatchGetItemsTotal = batchGetItemsTotal_.load(std::memory_order_relaxed);
        return out;
    }
}

extern "C" AKKARADB_API_SERVER_API bool akkaradb_api_tcp_register() noexcept {
    return akkaradb::engine::server::registerAkkApiTransportFactory(
        akkaradb::engine::AkkEngineOptions::ApiBackend::TCP,
        [](akkaradb::engine::AkkEngine& engine, const akkaradb::engine::AkkEngineOptions::ApiOptions& options) {
            return std::unique_ptr<akkaradb::engine::server::IAkkApiTransport>{
                akkaradb::engine::server::TcpApiServer::create(engine, options).release()
            };
        }
    );
}

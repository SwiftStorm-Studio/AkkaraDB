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

// akkserver/src/tcp/TcpApiPayloadCodec.cpp
#include "akk/engine/server/tcp/detail/TcpApiPayloadCodec.hpp"

#include <cstring>

namespace akkaradb::engine::server::tcp {
    namespace {
        constexpr uint8_t kStreamFrameItem = 1;
        constexpr uint8_t kStreamFrameEnd = 2;

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

        void appendStreamFrameHeader(std::vector<uint8_t>& out, uint8_t type, uint32_t payloadBytes) {
            out.clear();
            out.reserve(1 + sizeof(payloadBytes) + payloadBytes);
            appendPlain(out, type);
            appendPlain(out, payloadBytes);
        }
    }

    bool readBatchCount(std::span<const uint8_t> payload, uint32_t& out) noexcept {
        if (payload.size() < sizeof(uint32_t)) { return false; }
        std::memcpy(&out, payload.data(), sizeof(uint32_t));
        return true;
    }

    bool readU64(std::span<const uint8_t> payload, uint64_t& out) noexcept {
        if (payload.size() != sizeof(out)) { return false; }
        std::memcpy(&out, payload.data(), sizeof(out));
        return true;
    }

    bool readScanPayload(std::span<const uint8_t> payload, uint32_t& limit, std::span<const uint8_t>& endKey) noexcept {
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

    bool readOptionalLimit(std::span<const uint8_t> payload, uint32_t& limit) noexcept {
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

    void encodeScanStreamPayload(const AkkEngine::ScanRecordView& record, std::vector<uint8_t>& out) {
        const uint32_t payloadBytes = static_cast<uint32_t>(sizeof(uint16_t) + sizeof(uint32_t) + record.key.size() + record.value.size());
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
    }
}

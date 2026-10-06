/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/cluster/ReplFraming.cpp
#include "akk/engine/cluster/ReplFraming.hpp"

#include <cstring>

#include "akk/cpu/CRC32C.hpp"

namespace akkaradb::engine::cluster {
    namespace {
        void writeU32(std::vector<uint8_t>& b, uint32_t v) {
            for (size_t i = 0; i < 4; ++i) { b.push_back(static_cast<uint8_t>(v >> (8 * i))); }
        }

        void writeU64(std::vector<uint8_t>& b, uint64_t v) {
            for (size_t i = 0; i < 8; ++i) { b.push_back(static_cast<uint8_t>(v >> (8 * i))); }
        }

        void writeU32At(uint8_t* b, size_t off, uint32_t v) {
            for (size_t i = 0; i < 4; ++i) { b[off + i] = static_cast<uint8_t>(v >> (8 * i)); }
        }

        uint32_t readU32(std::span<const uint8_t> b, size_t off) {
            return static_cast<uint32_t>(b[off]) | (static_cast<uint32_t>(b[off + 1]) << 8) | (static_cast<uint32_t>(b[off + 2]) << 16) | (
                static_cast<uint32_t>(b[off + 3]) << 24);
        }

        uint64_t readU64(std::span<const uint8_t> b, size_t off) {
            uint64_t v = 0;
            for (size_t i = 0; i < 8; ++i) { v |= static_cast<uint64_t>(b[off + i]) << (8 * i); }
            return v;
        }

        bool readBytes(std::span<const uint8_t> p, size_t& cursor, size_t len, std::vector<uint8_t>& out) {
            if (cursor + len > p.size()) { return false; }
            out.assign(p.begin() + static_cast<std::ptrdiff_t>(cursor), p.begin() + static_cast<std::ptrdiff_t>(cursor + len));
            cursor += len;
            return true;
        }
    } // namespace

    std::vector<uint8_t> encodeFrame(ReplMsgType type, std::span<const uint8_t> payload, uint8_t flags) {
        if (payload.size() > ReplFrameHeader::MAX_PAYLOAD_SIZE) { return {}; }
        std::vector<uint8_t> wire(ReplFrameHeader::SIZE + payload.size());
        writeU32At(wire.data(), 0, ReplFrameHeader::MAGIC);
        wire[4] = static_cast<uint8_t>(type);
        wire[5] = flags;
        writeU32At(wire.data(), 6, static_cast<uint32_t>(payload.size()));
        const uint32_t crc = cpu::CRC32C(reinterpret_cast<const std::byte*>(payload.data()), payload.size());
        writeU32At(wire.data(), 10, crc);
        if (!payload.empty()) { std::memcpy(wire.data() + ReplFrameHeader::SIZE, payload.data(), payload.size()); }
        return wire;
    }

    bool decodeFrame(std::span<const uint8_t> wire, DecodedFrame& out) {
        if (wire.size() < ReplFrameHeader::SIZE) { return false; }
        if (readU32(wire, 0) != ReplFrameHeader::MAGIC) { return false; }
        const auto payloadLen = readU32(wire, 6);
        if (payloadLen > ReplFrameHeader::MAX_PAYLOAD_SIZE) { return false; }
        if (wire.size() != ReplFrameHeader::SIZE + payloadLen) { return false; }
        const auto payload = wire.subspan(ReplFrameHeader::SIZE, payloadLen);
        const uint32_t crc = cpu::CRC32C(reinterpret_cast<const std::byte*>(payload.data()), payload.size());
        if (crc != readU32(wire, 10)) { return false; }
        out.type = static_cast<ReplMsgType>(wire[4]);
        out.flags = wire[5];
        out.payload.assign(payload.begin(), payload.end());
        return true;
    }

    namespace {
        struct ForwardReader {
            std::span<const uint8_t> bytes;
            size_t position = 0;
            bool integer(uint64_t& out, size_t width) {
                if (width > bytes.size() - position) { return false; }
                out = 0;
                for (size_t i = 0; i < width; ++i) { out |= uint64_t{bytes[position++]} << (8 * i); }
                return true;
            }
            bool buffer(std::vector<uint8_t>& out, size_t limit = MAX_FORWARD_PAYLOAD) {
                uint64_t length;
                return integer(length, 4) && length <= limit && readBytes(bytes, position, static_cast<size_t>(length), out);
            }
            bool string(std::string& out, size_t limit) {
                std::vector<uint8_t> bufferValue;
                if (!buffer(bufferValue, limit)) { return false; }
                out.assign(bufferValue.begin(), bufferValue.end());
                return true;
            }
        };
        void forwardBytes(std::vector<uint8_t>& out, std::span<const uint8_t> bytes) {
            writeU32(out, static_cast<uint32_t>(bytes.size()));
            out.insert(out.end(), bytes.begin(), bytes.end());
        }
        void forwardString(std::vector<uint8_t>& out, const std::string& value) {
            forwardBytes(out, {reinterpret_cast<const uint8_t*>(value.data()), value.size()});
        }
    }

    std::vector<uint8_t> encodeForwardRequest(const ForwardRequest& request) {
        size_t size = FORWARD_REQUEST_BASE_SIZE;
        if (request.entries.size() > 65536 || request.timeoutMs == 0 || request.timeoutMs > 300000 ||
            static_cast<uint8_t>(request.operation) > static_cast<uint8_t>(ForwardOperation::CLUSTER_ADMIN) ||
            (request.operation == ForwardOperation::PUT_BATCH && request.entries.empty()) ||
            (request.operation != ForwardOperation::PUT_BATCH && request.entries.size() != 1)) { return {}; }
        for (const auto& entry : request.entries) {
            if (entry.key.size() > MAX_FORWARD_PAYLOAD || entry.value.size() > MAX_FORWARD_PAYLOAD) { return {}; }
            if ((request.operation == ForwardOperation::GET || request.operation == ForwardOperation::QUERY_REQUEST || request.operation == ForwardOperation::REMOVE ||
                request.operation == ForwardOperation::REMOVE_REQUEST) && !entry.value.empty()) { return {}; }
            size += 8 + entry.key.size() + entry.value.size();
            if (size > MAX_FORWARD_PAYLOAD) { return {}; }
        }
        std::vector<uint8_t> out;
        out.reserve(size);
        writeU64(out, request.requestId);
        out.push_back(static_cast<uint8_t>(request.operation));
        writeU32(out, request.timeoutMs);
        out.insert(out.end(), request.deduplicationId.nonce.begin(), request.deduplicationId.nonce.end());
        writeU64(out, request.deduplicationId.expiresAtUnixMs);
        writeU32(out, static_cast<uint32_t>(request.entries.size()));
        for (const auto& entry : request.entries) { forwardBytes(out, entry.key); forwardBytes(out, entry.value); }
        return encodeFrame(ReplMsgType::FORWARD_REQUEST, out);
    }

    bool decodeForwardRequest(std::span<const uint8_t> bytes, ForwardRequest& request) {
        if (bytes.size() > MAX_FORWARD_PAYLOAD) { return false; }
        ForwardRequest result;
        ForwardReader reader{bytes};
        uint64_t op, timeout, count;
        if (!reader.integer(result.requestId, 8) || !reader.integer(op, 1) || op > static_cast<uint8_t>(ForwardOperation::CLUSTER_ADMIN) ||
            !reader.integer(timeout, 4) || timeout == 0 || timeout > 300000) { return false; }
        result.operation = static_cast<ForwardOperation>(op);
        result.timeoutMs = static_cast<uint32_t>(timeout);
        for (auto& byte : result.deduplicationId.nonce) {
            uint64_t value;
            if (!reader.integer(value, 1)) { return false; }
            byte = static_cast<uint8_t>(value);
        }
        if (!reader.integer(result.deduplicationId.expiresAtUnixMs, 8) || !reader.integer(count, 4) || count > 65536 ||
            count > (bytes.size() - reader.position) / 8) { return false; }
        if ((result.operation != ForwardOperation::PUT_BATCH && count != 1) ||
            (result.operation == ForwardOperation::PUT_BATCH && count == 0)) { return false; }
        result.entries.resize(static_cast<size_t>(count));
        for (auto& entry : result.entries) {
            if (!reader.buffer(entry.key) || !reader.buffer(entry.value)) { return false; }
            if ((result.operation == ForwardOperation::GET || result.operation == ForwardOperation::QUERY_REQUEST || result.operation == ForwardOperation::REMOVE ||
                result.operation == ForwardOperation::REMOVE_REQUEST) && !entry.value.empty()) { return false; }
        }
        if (reader.position != bytes.size()) { return false; }
        request = std::move(result);
        return true;
    }

    std::vector<uint8_t> encodeForwardResponse(const ForwardResponse& response) {
        if (response.value.size() > MAX_FORWARD_PAYLOAD || response.target.host.size() > 1024 || response.message.size() > 4096 ||
            response.value.size() + response.target.host.size() + response.message.size() + 80 > MAX_FORWARD_PAYLOAD) { return {}; }
        std::vector<uint8_t> out;
        writeU64(out, response.requestId);
        out.push_back(response.success ? 1 : 0);
        out.push_back(response.found ? 1 : 0);
        out.push_back(static_cast<uint8_t>(response.requestResult.status));
        writeU64(out, response.requestResult.sequence);
        writeU64(out, response.requestResult.logIndex);
        forwardBytes(out, response.value);
        out.push_back(static_cast<uint8_t>(response.errorCode));
        writeU64(out, response.target.nodeId);
        forwardString(out, response.target.host);
        for (auto port : {response.target.tcpPort, response.target.httpPort, response.target.grpcPort, response.target.replPort}) {
            out.push_back(static_cast<uint8_t>(port)); out.push_back(static_cast<uint8_t>(port >> 8));
        }
        writeU64(out, response.target.configurationEpoch);
        writeU64(out, response.target.raftTerm);
        forwardString(out, response.message);
        return encodeFrame(ReplMsgType::FORWARD_RESPONSE, out);
    }

    bool decodeForwardResponse(std::span<const uint8_t> bytes, ForwardResponse& response) {
        if (bytes.size() > MAX_FORWARD_PAYLOAD) { return false; }
        ForwardResponse result;
        ForwardReader reader{bytes};
        uint64_t success, found, status, code;
        if (!reader.integer(result.requestId, 8) || !reader.integer(success, 1) || success > 1 ||
            !reader.integer(found, 1) || found > 1 || !reader.integer(status, 1) || status > static_cast<uint8_t>(ClusterRequestStatus::EXPIRED) ||
            !reader.integer(result.requestResult.sequence, 8) || !reader.integer(result.requestResult.logIndex, 8) ||
            !reader.buffer(result.value) || !reader.integer(code, 1) || code > static_cast<uint8_t>(ClusterRoutingCode::LOCAL_ONLY) ||
            !reader.integer(result.target.nodeId, 8) || !reader.string(result.target.host, 1024)) { return false; }
        result.success = success != 0; result.found = found != 0;
        result.requestResult.status = static_cast<ClusterRequestStatus>(status);
        result.errorCode = static_cast<ClusterRoutingCode>(code);
        for (auto* port : {&result.target.tcpPort, &result.target.httpPort, &result.target.grpcPort, &result.target.replPort}) {
            uint64_t value;
            if (!reader.integer(value, 2)) { return false; }
            *port = static_cast<uint16_t>(value);
        }
        if (!reader.integer(result.target.configurationEpoch, 8) || !reader.integer(result.target.raftTerm, 8) ||
            !reader.string(result.message, 4096) || reader.position != bytes.size()) { return false; }
        response = std::move(result);
        return true;
    }

    std::vector<uint8_t> encodeClientHello(const ClientHello& hello) {
        std::vector<uint8_t> p;
        writeU64(p, hello.nodeId);
        writeU64(p, hello.lastSeq);
        p.push_back(static_cast<uint8_t>(hello.role));
        p.push_back((static_cast<uint8_t>(hello.mirrorFencingMode) << 1) | (hello.forceSnapshot ? 1 : 0));
        writeU64(p, hello.groupId);
        writeU64(p, hello.groupEpoch);
        p.insert(p.end(), hello.clusterId.begin(), hello.clusterId.end());
        p.insert(p.end(), hello.configFingerprint.begin(), hello.configFingerprint.end());
        return encodeFrame(ReplMsgType::CLIENT_HELLO, p);
    }

    std::vector<uint8_t> encodeServerHello(const ServerHello& hello) {
        std::vector<uint8_t> p;
        writeU64(p, hello.nodeId);
        writeU64(p, hello.currentSeq);
        p.push_back(static_cast<uint8_t>(hello.role));
        p.push_back(static_cast<uint8_t>(hello.mirrorFencingMode));
        writeU64(p, hello.groupId);
        writeU64(p, hello.groupEpoch);
        p.insert(p.end(), hello.clusterId.begin(), hello.clusterId.end());
        p.insert(p.end(), hello.configFingerprint.begin(), hello.configFingerprint.end());
        return encodeFrame(ReplMsgType::SERVER_HELLO, p);
    }

    std::vector<uint8_t> encodeEntry(const ReplEntry& entry) {
        std::vector<uint8_t> p;
        writeU64(p, entry.seq);
        writeU64(p, entry.sourceNodeId);
        p.push_back(static_cast<uint8_t>(entry.op));
        p.push_back(entry.recordFlags);
        writeU32(p, static_cast<uint32_t>(entry.key.size()));
        writeU32(p, static_cast<uint32_t>(entry.value.size()));
        p.insert(p.end(), entry.key.begin(), entry.key.end());
        p.insert(p.end(), entry.value.begin(), entry.value.end());
        return encodeFrame(ReplMsgType::ENTRY, p);
    }

    std::vector<uint8_t> encodeBlob(const ReplBlob& blob) {
        std::vector<uint8_t> p;
        writeU64(p, blob.seq);
        writeU64(p, blob.blobId);
        writeU64(p, static_cast<uint64_t>(blob.content.size()));
        p.insert(p.end(), blob.content.begin(), blob.content.end());
        return encodeFrame(ReplMsgType::BLOB_PUT, p);
    }

    std::vector<uint8_t> encodeAck(const ReplAck& ack) {
        std::vector<uint8_t> p;
        writeU64(p, ack.seq);
        p.push_back(static_cast<uint8_t>(ack.stage));
        p.push_back(0);
        return encodeFrame(ReplMsgType::ACK, p);
    }

    std::vector<uint8_t> encodeSnapshotBegin(const ReplSnapshotBegin& begin) {
        std::vector<uint8_t> p;
        writeU64(p, begin.snapshotSeq);
        writeU64(p, begin.entryCount);
        return encodeFrame(ReplMsgType::SNAPSHOT_BEGIN, p);
    }

    std::vector<uint8_t> encodeSnapshotEntry(const ReplSnapshotEntry& entry) {
        std::vector<uint8_t> p;
        writeU32(p, static_cast<uint32_t>(entry.key.size()));
        writeU32(p, static_cast<uint32_t>(entry.value.size()));
        p.insert(p.end(), entry.key.begin(), entry.key.end());
        p.insert(p.end(), entry.value.begin(), entry.value.end());
        return encodeFrame(ReplMsgType::SNAPSHOT_ENTRY, p);
    }

    std::vector<uint8_t> encodeSnapshotEnd(uint64_t snapshotSeq) {
        std::vector<uint8_t> p;
        writeU64(p, snapshotSeq);
        return encodeFrame(ReplMsgType::SNAPSHOT_END, p);
    }

    std::vector<uint8_t> encodeReadRequest(const ReadRequest& request) {
        std::vector<uint8_t> p;
        writeU64(p, request.requestId);
        writeU64(p, request.snapshotSeq);
        writeU32(p, static_cast<uint32_t>(request.key.size()));
        p.insert(p.end(), request.key.begin(), request.key.end());
        return encodeFrame(ReplMsgType::READ_REQUEST, p);
    }

    std::vector<uint8_t> encodeReadResponse(const ReadResponse& response) {
        std::vector<uint8_t> p;
        writeU64(p, response.requestId);
        p.push_back(static_cast<uint8_t>(response.status));
        p.push_back(response.recordFlags);
        writeU64(p, response.seq);
        writeU32(p, static_cast<uint32_t>(response.value.size()));
        p.insert(p.end(), response.value.begin(), response.value.end());
        return encodeFrame(ReplMsgType::READ_RESPONSE, p);
    }

    std::vector<uint8_t> encodeStripeControlRequest(const StripeControlRequest& request) {
        std::vector<uint8_t> p;
        writeU64(p, request.requestId);
        p.push_back(static_cast<uint8_t>(request.action));
        writeU64(p, request.ownerNodeId);
        writeU64(p, request.fenceToken);
        writeU32(p, static_cast<uint32_t>(request.key.size()));
        writeU32(p, static_cast<uint32_t>(request.metadata.size()));
        p.insert(p.end(), request.key.begin(), request.key.end());
        p.insert(p.end(), request.metadata.begin(), request.metadata.end());
        return encodeFrame(ReplMsgType::STRIPE_CONTROL_REQUEST, p);
    }

    std::vector<uint8_t> encodeStripeControlResponse(const StripeControlResponse& response) {
        std::vector<uint8_t> p;
        writeU64(p, response.requestId);
        p.push_back(static_cast<uint8_t>(response.status));
        writeU64(p, response.authorityNodeId);
        writeU64(p, response.fenceToken);
        writeU32(p, static_cast<uint32_t>(response.metadata.size()));
        p.insert(p.end(), response.metadata.begin(), response.metadata.end());
        return encodeFrame(ReplMsgType::STRIPE_CONTROL_RESPONSE, p);
    }

    bool decodeClientHello(std::span<const uint8_t> payload, ClientHello& out) {
        if (payload.size() != 82 || payload[17] > 5) { return false; }
        out.nodeId = readU64(payload, 0);
        out.lastSeq = readU64(payload, 8);
        out.role = static_cast<NodeRole>(payload[16]);
        out.mirrorFencingMode = static_cast<MirrorFencingMode>(payload[17] >> 1);
        out.forceSnapshot = (payload[17] & 1) != 0;
        out.groupId = readU64(payload, 18);
        out.groupEpoch = readU64(payload, 26);
        std::memcpy(out.clusterId.data(), payload.data() + 34, out.clusterId.size());
        std::memcpy(out.configFingerprint.data(), payload.data() + 50, out.configFingerprint.size());
        return true;
    }

    bool decodeServerHello(std::span<const uint8_t> payload, ServerHello& out) {
        if (payload.size() != 82 || payload[17] > 2) { return false; }
        out.nodeId = readU64(payload, 0);
        out.currentSeq = readU64(payload, 8);
        out.role = static_cast<NodeRole>(payload[16]);
        out.mirrorFencingMode = static_cast<MirrorFencingMode>(payload[17]);
        out.groupId = readU64(payload, 18);
        out.groupEpoch = readU64(payload, 26);
        std::memcpy(out.clusterId.data(), payload.data() + 34, out.clusterId.size());
        std::memcpy(out.configFingerprint.data(), payload.data() + 50, out.configFingerprint.size());
        return true;
    }

    bool decodeEntry(std::span<const uint8_t> payload, ReplEntry& out) {
        if (payload.size() < 26) { return false; }
        out.seq = readU64(payload, 0);
        out.sourceNodeId = readU64(payload, 8);
        out.op = static_cast<ReplOpType>(payload[16]);
        out.recordFlags = payload[17];
        const uint32_t keyLen = readU32(payload, 18);
        const uint32_t valLen = readU32(payload, 22);
        size_t cursor = 26;
        return readBytes(payload, cursor, keyLen, out.key) && readBytes(payload, cursor, valLen, out.value) && cursor == payload.size();
    }

    bool decodeBlob(std::span<const uint8_t> payload, ReplBlob& out) {
        if (payload.size() < 24) { return false; }
        out.seq = readU64(payload, 0);
        out.blobId = readU64(payload, 8);
        const uint64_t contentLen = readU64(payload, 16);
        if (contentLen > payload.size() - 24) { return false; }
        size_t cursor = 24;
        return readBytes(payload, cursor, static_cast<size_t>(contentLen), out.content) && cursor == payload.size();
    }

    bool decodeAck(std::span<const uint8_t> payload, ReplAck& out) {
        if (payload.size() != 10) { return false; }
        out.seq = readU64(payload, 0);
        out.stage = static_cast<AckStage>(payload[8]);
        return out.stage == AckStage::RECEIVED || out.stage == AckStage::APPLIED || out.stage == AckStage::DURABLE;
    }

    bool decodeSnapshotBegin(std::span<const uint8_t> payload, ReplSnapshotBegin& out) {
        if (payload.size() != 16) { return false; }
        out.snapshotSeq = readU64(payload, 0);
        out.entryCount = readU64(payload, 8);
        return true;
    }

    bool decodeSnapshotEntry(std::span<const uint8_t> payload, ReplSnapshotEntry& out) {
        if (payload.size() < 8) { return false; }
        const uint32_t keyLen = readU32(payload, 0);
        const uint32_t valueLen = readU32(payload, 4);
        size_t cursor = 8;
        return readBytes(payload, cursor, keyLen, out.key) && readBytes(payload, cursor, valueLen, out.value) && cursor == payload.size();
    }

    bool decodeSnapshotEnd(std::span<const uint8_t> payload, uint64_t& snapshotSeq) {
        if (payload.size() != 8) { return false; }
        snapshotSeq = readU64(payload, 0);
        return true;
    }

    bool decodeReadRequest(std::span<const uint8_t> payload, ReadRequest& out) {
        if (payload.size() < 20) { return false; }
        out.requestId = readU64(payload, 0);
        out.snapshotSeq = readU64(payload, 8);
        const uint32_t keyLen = readU32(payload, 16);
        size_t cursor = 20;
        return readBytes(payload, cursor, keyLen, out.key) && cursor == payload.size();
    }

    bool decodeReadResponse(std::span<const uint8_t> payload, ReadResponse& out) {
        if (payload.size() < 22) { return false; }
        out.requestId = readU64(payload, 0);
        out.status = static_cast<ReadStatus>(payload[8]);
        out.recordFlags = payload[9];
        out.seq = readU64(payload, 10);
        const uint32_t valueLen = readU32(payload, 18);
        size_t cursor = 22;
        return readBytes(payload, cursor, valueLen, out.value) && cursor == payload.size();
    }

    bool decodeStripeControlRequest(std::span<const uint8_t> payload, StripeControlRequest& out) {
        if (payload.size() < 33) { return false; }
        out.requestId = readU64(payload, 0);
        out.action = static_cast<StripeControlAction>(payload[8]);
        if (out.action != StripeControlAction::ACQUIRE && out.action != StripeControlAction::COMMIT &&
            out.action != StripeControlAction::RELEASE && out.action != StripeControlAction::READ_METADATA &&
            out.action != StripeControlAction::REPAIR_METADATA && out.action != StripeControlAction::ROLLBACK_WATERMARK &&
            out.action != StripeControlAction::ROLLBACK_KEY && out.action != StripeControlAction::ROLLBACK_STREAM &&
            out.action != StripeControlAction::ROLLBACK_APPLY) { return false; }
        out.ownerNodeId = readU64(payload, 9);
        out.fenceToken = readU64(payload, 17);
        const uint32_t keyLen = readU32(payload, 25);
        const uint32_t metadataLen = readU32(payload, 29);
        size_t cursor = 33;
        return readBytes(payload, cursor, keyLen, out.key) && readBytes(payload, cursor, metadataLen, out.metadata) &&
               cursor == payload.size();
    }

    bool decodeStripeControlResponse(std::span<const uint8_t> payload, StripeControlResponse& out) {
        if (payload.size() < 29) { return false; }
        out.requestId = readU64(payload, 0);
        out.status = static_cast<StripeControlStatus>(payload[8]);
        out.authorityNodeId = readU64(payload, 9);
        out.fenceToken = readU64(payload, 17);
        const uint32_t metadataLen = readU32(payload, 25);
        size_t cursor = 29;
        if (!readBytes(payload, cursor, metadataLen, out.metadata) || cursor != payload.size()) { return false; }
        return out.status == StripeControlStatus::GRANTED || out.status == StripeControlStatus::COMMITTED ||
               out.status == StripeControlStatus::RELEASED || out.status == StripeControlStatus::BUSY ||
               out.status == StripeControlStatus::REJECTED || out.status == StripeControlStatus::ERROR_STATUS ||
               out.status == StripeControlStatus::FOUND || out.status == StripeControlStatus::NOT_FOUND;
    }
} // namespace akkaradb::engine::cluster

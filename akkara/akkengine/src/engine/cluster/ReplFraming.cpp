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
        void writeU16(std::vector<uint8_t>& b, uint16_t v) {
            b.push_back(static_cast<uint8_t>(v));
            b.push_back(static_cast<uint8_t>(v >> 8));
        }

        void writeU32(std::vector<uint8_t>& b, uint32_t v) {
            for (size_t i = 0; i < 4; ++i) { b.push_back(static_cast<uint8_t>(v >> (8 * i))); }
        }

        void writeU64(std::vector<uint8_t>& b, uint64_t v) {
            for (size_t i = 0; i < 8; ++i) { b.push_back(static_cast<uint8_t>(v >> (8 * i))); }
        }

        void writeU32At(uint8_t* b, size_t off, uint32_t v) {
            for (size_t i = 0; i < 4; ++i) { b[off + i] = static_cast<uint8_t>(v >> (8 * i)); }
        }

        uint16_t readU16(std::span<const uint8_t> b, size_t off) {
            return static_cast<uint16_t>(b[off]) | static_cast<uint16_t>(static_cast<uint16_t>(b[off + 1]) << 8);
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
        if (wire.size() != ReplFrameHeader::SIZE + payloadLen) { return false; }
        const auto payload = wire.subspan(ReplFrameHeader::SIZE, payloadLen);
        const uint32_t crc = cpu::CRC32C(reinterpret_cast<const std::byte*>(payload.data()), payload.size());
        if (crc != readU32(wire, 10)) { return false; }
        out.type = static_cast<ReplMsgType>(wire[4]);
        out.flags = wire[5];
        out.payload.assign(payload.begin(), payload.end());
        return true;
    }

    std::vector<uint8_t> encodeClientHello(const ClientHello& hello) {
        std::vector<uint8_t> p;
        writeU64(p, hello.nodeId);
        writeU64(p, hello.lastSeq);
        p.push_back(static_cast<uint8_t>(hello.role));
        p.push_back(0);
        return encodeFrame(ReplMsgType::CLIENT_HELLO, p);
    }

    std::vector<uint8_t> encodeServerHello(const ServerHello& hello) {
        std::vector<uint8_t> p;
        writeU64(p, hello.nodeId);
        writeU64(p, hello.currentSeq);
        p.push_back(static_cast<uint8_t>(hello.role));
        p.push_back(0);
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

    bool decodeClientHello(std::span<const uint8_t> payload, ClientHello& out) {
        if (payload.size() != 18) { return false; }
        out.nodeId = readU64(payload, 0);
        out.lastSeq = readU64(payload, 8);
        out.role = static_cast<NodeRole>(payload[16]);
        return true;
    }

    bool decodeServerHello(std::span<const uint8_t> payload, ServerHello& out) {
        if (payload.size() != 18) { return false; }
        out.nodeId = readU64(payload, 0);
        out.currentSeq = readU64(payload, 8);
        out.role = static_cast<NodeRole>(payload[16]);
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
} // namespace akkaradb::engine::cluster

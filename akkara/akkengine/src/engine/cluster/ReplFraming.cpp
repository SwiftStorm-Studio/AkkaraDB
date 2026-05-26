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

// akkengine/src/engine/cluster/ReplFraming.cpp
#include "akk/engine/cluster/ReplFraming.hpp"

#include <cstring>

#include "akk/cpu/CRC32C.hpp"

namespace akkaradb::engine::cluster {
    namespace {
        void write_u16(std::vector<uint8_t>& b, uint16_t v) {
            b.push_back(static_cast<uint8_t>(v));
            b.push_back(static_cast<uint8_t>(v >> 8));
        }

        void write_u32(std::vector<uint8_t>& b, uint32_t v) { for (size_t i = 0; i < 4; ++i) { b.push_back(static_cast<uint8_t>(v >> (8 * i))); } }

        void write_u64(std::vector<uint8_t>& b, uint64_t v) { for (size_t i = 0; i < 8; ++i) { b.push_back(static_cast<uint8_t>(v >> (8 * i))); } }

        void write_u32_at(uint8_t* b, size_t off, uint32_t v) { for (size_t i = 0; i < 4; ++i) { b[off + i] = static_cast<uint8_t>(v >> (8 * i)); } }

        uint16_t read_u16(std::span<const uint8_t> b, size_t off) {
            return static_cast<uint16_t>(b[off]) | static_cast<uint16_t>(static_cast<uint16_t>(b[off + 1]) << 8);
        }

        uint32_t read_u32(std::span<const uint8_t> b, size_t off) {
            return static_cast<uint32_t>(b[off]) | (static_cast<uint32_t>(b[off + 1]) << 8) | (static_cast<uint32_t>(b[off + 2]) << 16) | (static_cast<uint32_t>
                (b[off + 3]) << 24);
        }

        uint64_t read_u64(std::span<const uint8_t> b, size_t off) {
            uint64_t v = 0;
            for (size_t i = 0; i < 8; ++i) { v |= static_cast<uint64_t>(b[off + i]) << (8 * i); }
            return v;
        }

        bool read_bytes(std::span<const uint8_t> p, size_t& cursor, size_t len, std::vector<uint8_t>& out) {
            if (cursor + len > p.size()) { return false; }
            out.assign(p.begin() + static_cast<std::ptrdiff_t>(cursor), p.begin() + static_cast<std::ptrdiff_t>(cursor + len));
            cursor += len;
            return true;
        }
    } // namespace

    std::vector<uint8_t> encode_frame(ReplMsgType type, std::span<const uint8_t> payload, uint8_t flags) {
        std::vector<uint8_t> wire(ReplFrameHeader::SIZE + payload.size());
        write_u32_at(wire.data(), 0, ReplFrameHeader::MAGIC);
        wire[4] = static_cast<uint8_t>(type);
        wire[5] = flags;
        write_u32_at(wire.data(), 6, static_cast<uint32_t>(payload.size()));
        const uint32_t crc = cpu::CRC32C(reinterpret_cast<const std::byte*>(payload.data()), payload.size());
        write_u32_at(wire.data(), 10, crc);
        if (!payload.empty()) { std::memcpy(wire.data() + ReplFrameHeader::SIZE, payload.data(), payload.size()); }
        return wire;
    }

    bool decode_frame(std::span<const uint8_t> wire, DecodedFrame& out) {
        if (wire.size() < ReplFrameHeader::SIZE) { return false; }
        if (read_u32(wire, 0) != ReplFrameHeader::MAGIC) { return false; }
        const auto payload_len = read_u32(wire, 6);
        if (wire.size() != ReplFrameHeader::SIZE + payload_len) { return false; }
        const auto payload = wire.subspan(ReplFrameHeader::SIZE, payload_len);
        const uint32_t crc = cpu::CRC32C(reinterpret_cast<const std::byte*>(payload.data()), payload.size());
        if (crc != read_u32(wire, 10)) { return false; }
        out.type = static_cast<ReplMsgType>(wire[4]);
        out.flags = wire[5];
        out.payload.assign(payload.begin(), payload.end());
        return true;
    }

    std::vector<uint8_t> encode_client_hello(const ClientHello& hello) {
        std::vector<uint8_t> p;
        write_u64(p, hello.node_id);
        write_u64(p, hello.last_seq);
        p.push_back(static_cast<uint8_t>(hello.role));
        p.push_back(0);
        return encode_frame(ReplMsgType::ClientHello, p);
    }

    std::vector<uint8_t> encode_server_hello(const ServerHello& hello) {
        std::vector<uint8_t> p;
        write_u64(p, hello.node_id);
        write_u64(p, hello.current_seq);
        p.push_back(static_cast<uint8_t>(hello.role));
        p.push_back(0);
        return encode_frame(ReplMsgType::ServerHello, p);
    }

    std::vector<uint8_t> encode_entry(const ReplEntry& entry) {
        std::vector<uint8_t> p;
        write_u64(p, entry.seq);
        write_u64(p, entry.source_node_id);
        p.push_back(static_cast<uint8_t>(entry.op));
        p.push_back(entry.record_flags);
        write_u32(p, static_cast<uint32_t>(entry.key.size()));
        write_u32(p, static_cast<uint32_t>(entry.value.size()));
        p.insert(p.end(), entry.key.begin(), entry.key.end());
        p.insert(p.end(), entry.value.begin(), entry.value.end());
        return encode_frame(ReplMsgType::Entry, p);
    }

    std::vector<uint8_t> encode_blob(const ReplBlob& blob) {
        std::vector<uint8_t> p;
        write_u64(p, blob.seq);
        write_u64(p, blob.blob_id);
        write_u64(p, static_cast<uint64_t>(blob.content.size()));
        p.insert(p.end(), blob.content.begin(), blob.content.end());
        return encode_frame(ReplMsgType::BlobPut, p);
    }

    std::vector<uint8_t> encode_ack(const ReplAck& ack) {
        std::vector<uint8_t> p;
        write_u64(p, ack.seq);
        return encode_frame(ReplMsgType::Ack, p);
    }

    std::vector<uint8_t> encode_read_request(const ReadRequest& request) {
        std::vector<uint8_t> p;
        write_u64(p, request.request_id);
        write_u64(p, request.snapshot_seq);
        write_u32(p, static_cast<uint32_t>(request.key.size()));
        p.insert(p.end(), request.key.begin(), request.key.end());
        return encode_frame(ReplMsgType::ReadRequest, p);
    }

    std::vector<uint8_t> encode_read_response(const ReadResponse& response) {
        std::vector<uint8_t> p;
        write_u64(p, response.request_id);
        p.push_back(static_cast<uint8_t>(response.status));
        p.push_back(response.record_flags);
        write_u64(p, response.seq);
        write_u32(p, static_cast<uint32_t>(response.value.size()));
        p.insert(p.end(), response.value.begin(), response.value.end());
        return encode_frame(ReplMsgType::ReadResponse, p);
    }

    bool decode_client_hello(std::span<const uint8_t> payload, ClientHello& out) {
        if (payload.size() != 18) { return false; }
        out.node_id = read_u64(payload, 0);
        out.last_seq = read_u64(payload, 8);
        out.role = static_cast<NodeRole>(payload[16]);
        return true;
    }

    bool decode_server_hello(std::span<const uint8_t> payload, ServerHello& out) {
        if (payload.size() != 18) { return false; }
        out.node_id = read_u64(payload, 0);
        out.current_seq = read_u64(payload, 8);
        out.role = static_cast<NodeRole>(payload[16]);
        return true;
    }

    bool decode_entry(std::span<const uint8_t> payload, ReplEntry& out) {
        if (payload.size() < 26) { return false; }
        out.seq = read_u64(payload, 0);
        out.source_node_id = read_u64(payload, 8);
        out.op = static_cast<ReplOpType>(payload[16]);
        out.record_flags = payload[17];
        const uint32_t key_len = read_u32(payload, 18);
        const uint32_t val_len = read_u32(payload, 22);
        size_t cursor = 26;
        return read_bytes(payload, cursor, key_len, out.key) && read_bytes(payload, cursor, val_len, out.value) && cursor == payload.size();
    }

    bool decode_blob(std::span<const uint8_t> payload, ReplBlob& out) {
        if (payload.size() < 24) { return false; }
        out.seq = read_u64(payload, 0);
        out.blob_id = read_u64(payload, 8);
        const uint64_t content_len = read_u64(payload, 16);
        if (content_len > payload.size() - 24) { return false; }
        size_t cursor = 24;
        return read_bytes(payload, cursor, static_cast<size_t>(content_len), out.content) && cursor == payload.size();
    }

    bool decode_ack(std::span<const uint8_t> payload, ReplAck& out) {
        if (payload.size() != 8) { return false; }
        out.seq = read_u64(payload, 0);
        return true;
    }

    bool decode_read_request(std::span<const uint8_t> payload, ReadRequest& out) {
        if (payload.size() < 20) { return false; }
        out.request_id = read_u64(payload, 0);
        out.snapshot_seq = read_u64(payload, 8);
        const uint32_t key_len = read_u32(payload, 16);
        size_t cursor = 20;
        return read_bytes(payload, cursor, key_len, out.key) && cursor == payload.size();
    }

    bool decode_read_response(std::span<const uint8_t> payload, ReadResponse& out) {
        if (payload.size() < 22) { return false; }
        out.request_id = read_u64(payload, 0);
        out.status = static_cast<ReadStatus>(payload[8]);
        out.record_flags = payload[9];
        out.seq = read_u64(payload, 10);
        const uint32_t value_len = read_u32(payload, 18);
        size_t cursor = 22;
        return read_bytes(payload, cursor, value_len, out.value) && cursor == payload.size();
    }
} // namespace akkaradb::engine::cluster

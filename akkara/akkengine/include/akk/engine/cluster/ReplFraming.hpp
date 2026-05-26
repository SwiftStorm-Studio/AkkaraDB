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

// akkengine/include/akk/engine/cluster/ReplFraming.hpp
#pragma once

#include <cstdint>
#include <span>
#include <vector>

#include "akk/engine/cluster/ClusterConfig.hpp"

namespace akkaradb::engine::cluster {
    /**
     * ReplMsgType - Replication wire message discriminator.
     */
    enum class ReplMsgType : uint8_t {
        ClientHello = 0x01,
        ///< Replica -> primary handshake with last applied seq.
        ServerHello = 0x02,
        ///< Primary -> replica handshake response.
        Entry = 0x10,
        ///< Replicated put/remove entry.
        BlobPut = 0x11,
        ///< Replicated external blob payload.
        Ack = 0x12,
        ///< Replica acknowledgement for an Entry seq.
        ReadRequest = 0x20,
        ///< Reserved point-in-time read request.
        ReadResponse = 0x21,
        ///< Reserved point-in-time read response.
    };

    /**
     * ReplOpType - Storage mutation represented by ReplEntry.
     */
    enum class ReplOpType : uint8_t {
        Put = 1, Remove = 2,
    };

    /**
     * ReadStatus - Result code for replicated read responses.
     */
    enum class ReadStatus : uint8_t {
        Found = 0, NotFound = 1, Error = 2,
    };

    /**
     * ReplFrameHeader - Fixed replication frame header.
     *
     * Wire layout, little-endian:
     *   [magic:u32][type:u8][flags:u8][payload_len:u32][payload_crc32c:u32]
     *
     * The header is followed by payload_len bytes.  CRC covers only the
     * payload, not the header.
     */
    struct ReplFrameHeader {
        static constexpr uint32_t MAGIC = 0x35524B41; // "AKR5"
        static constexpr size_t SIZE = 14;

        ReplMsgType type{}; ///< Message discriminator.
        uint8_t flags = 0; ///< Reserved per-frame flags.
        uint32_t payload_len = 0; ///< Payload byte length.
        uint32_t crc32c = 0; ///< CRC32C of the payload bytes.
    };

    /**
     * DecodedFrame - Validated frame returned by decode_frame().
     */
    struct DecodedFrame {
        ReplMsgType type{};
        uint8_t flags = 0;
        std::vector<uint8_t> payload;
    };

    /** Replica-to-primary handshake payload. */
    struct ClientHello {
        uint64_t node_id = 0; ///< Replica node id.
        uint64_t last_seq = 0; ///< Last sequence already applied by the replica.
        NodeRole role = NodeRole::Replica; ///< Expected to be NodeRole::Replica.
    };

    /** Primary-to-replica handshake response payload. */
    struct ServerHello {
        uint64_t node_id = 0; ///< Primary node id.
        uint64_t current_seq = 0; ///< Primary's current sequence at handshake time.
        NodeRole role = NodeRole::Primary; ///< Expected to be NodeRole::Primary.
    };

    /** Replicated key/value mutation payload. */
    struct ReplEntry {
        uint64_t seq = 0; ///< Monotonic sequence assigned by the source engine.
        uint64_t source_node_id = 0; ///< Node that originally produced the entry.
        ReplOpType op = ReplOpType::Put; ///< Mutation type.
        uint8_t record_flags = 0; ///< Record flags preserved for storage apply.
        std::vector<uint8_t> key; ///< Raw key bytes.
        std::vector<uint8_t> value; ///< Raw value bytes, empty for Remove.
    };

    /** Replicated blob payload. */
    struct ReplBlob {
        uint64_t seq = 0; ///< Sequence associated with the blob reference.
        uint64_t blob_id = 0; ///< Stable blob identifier.
        std::vector<uint8_t> content; ///< Raw blob content.
    };

    /** Replica acknowledgement payload. */
    struct ReplAck {
        uint64_t seq = 0; ///< Highest entry sequence acknowledged by the replica.
    };

    /** Reserved point-in-time read request payload. */
    struct ReadRequest {
        uint64_t request_id = 0;
        uint64_t snapshot_seq = 0;
        std::vector<uint8_t> key;
    };

    /** Reserved point-in-time read response payload. */
    struct ReadResponse {
        uint64_t request_id = 0;
        ReadStatus status = ReadStatus::Error;
        uint8_t record_flags = 0;
        uint64_t seq = 0;
        std::vector<uint8_t> value;
    };

    /**
     * Encodes a complete frame with header, payload, and payload CRC32C.
     */
    [[nodiscard]] std::vector<uint8_t> encode_frame(ReplMsgType type, std::span<const uint8_t> payload, uint8_t flags = 0);

    /**
     * Decodes and validates a complete frame.
     *
     * @return false on short input, bad magic, length mismatch, or CRC mismatch.
     */
    [[nodiscard]] bool decode_frame(std::span<const uint8_t> wire, DecodedFrame& out);

    /** Encodes a ClientHello frame. */
    [[nodiscard]] std::vector<uint8_t> encode_client_hello(const ClientHello& hello);

    /** Encodes a ServerHello frame. */
    [[nodiscard]] std::vector<uint8_t> encode_server_hello(const ServerHello& hello);

    /** Encodes a ReplEntry frame. */
    [[nodiscard]] std::vector<uint8_t> encode_entry(const ReplEntry& entry);

    /** Encodes a ReplBlob frame. */
    [[nodiscard]] std::vector<uint8_t> encode_blob(const ReplBlob& blob);

    /** Encodes a ReplAck frame. */
    [[nodiscard]] std::vector<uint8_t> encode_ack(const ReplAck& ack);

    /** Encodes a ReadRequest frame. */
    [[nodiscard]] std::vector<uint8_t> encode_read_request(const ReadRequest& request);

    /** Encodes a ReadResponse frame. */
    [[nodiscard]] std::vector<uint8_t> encode_read_response(const ReadResponse& response);

    /** Decodes a ClientHello payload. */
    [[nodiscard]] bool decode_client_hello(std::span<const uint8_t> payload, ClientHello& out);

    /** Decodes a ServerHello payload. */
    [[nodiscard]] bool decode_server_hello(std::span<const uint8_t> payload, ServerHello& out);

    /** Decodes a ReplEntry payload. */
    [[nodiscard]] bool decode_entry(std::span<const uint8_t> payload, ReplEntry& out);

    /** Decodes a ReplBlob payload. */
    [[nodiscard]] bool decode_blob(std::span<const uint8_t> payload, ReplBlob& out);

    /** Decodes a ReplAck payload. */
    [[nodiscard]] bool decode_ack(std::span<const uint8_t> payload, ReplAck& out);

    /** Decodes a ReadRequest payload. */
    [[nodiscard]] bool decode_read_request(std::span<const uint8_t> payload, ReadRequest& out);

    /** Decodes a ReadResponse payload. */
    [[nodiscard]] bool decode_read_response(std::span<const uint8_t> payload, ReadResponse& out);
} // namespace akkaradb::engine::cluster

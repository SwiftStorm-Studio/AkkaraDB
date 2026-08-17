/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/cluster/ReplFraming.hpp
#pragma once

#include <cstdint>
#include <span>
#include <vector>

#include "akk/engine/cluster/AkkClusterRuntimeExport.hpp"
#include "akk/engine/cluster/ClusterConfig.hpp"

namespace akkaradb::engine::cluster {
    /**
     * ReplMsgType - Replication wire message discriminator.
     */
    enum class ReplMsgType : uint8_t {
        CLIENT_HELLO = 0x01,
        ///< Replica -> primary handshake with last applied seq.
        SERVER_HELLO = 0x02,
        ///< Primary -> replica handshake response.
        ENTRY = 0x10,
        ///< Replicated put/remove entry.
        BLOB_PUT = 0x11,
        ///< Replicated external blob payload.
        ACK = 0x12,
        ///< Replica acknowledgement for an Entry seq.
        RESYNC_REQUIRED = 0x13,
        ///< Primary cannot provide a contiguous retained history for this replica.
        SNAPSHOT_BEGIN = 0x14,
        SNAPSHOT_ENTRY = 0x15,
        SNAPSHOT_END = 0x16,
        READ_REQUEST = 0x20,
        ///< Reserved point-in-time read request.
        READ_RESPONSE = 0x21,
        ///< Reserved point-in-time read response.
        RAFT_REQUEST_VOTE = 0x30,
        ///< Raft RequestVote RPC.
        RAFT_REQUEST_VOTE_RESPONSE = 0x31,
        ///< Raft RequestVote RPC response.
        RAFT_APPEND_ENTRIES = 0x32,
        ///< Raft AppendEntries RPC, including heartbeats.
        RAFT_APPEND_ENTRIES_RESPONSE = 0x33,
        ///< Raft AppendEntries RPC response.
        RAFT_INSTALL_SNAPSHOT = 0x34,
        ///< Raft InstallSnapshot RPC.
        RAFT_INSTALL_SNAPSHOT_RESPONSE = 0x35,
        ///< Raft InstallSnapshot RPC response.
        RAFT_TIMEOUT_NOW = 0x36,
        ///< Raft leadership transfer election trigger.
        RAFT_TIMEOUT_NOW_RESPONSE = 0x37,
        ///< Raft leadership transfer election trigger response.
    };

    /**
     * ReplOpType - Storage mutation represented by ReplEntry.
     */
    enum class ReplOpType : uint8_t {
        PUT = 1, REMOVE = 2,
    };

    /**
     * ReadStatus - Result code for replicated read responses.
     */
    enum class ReadStatus : uint8_t {
        FOUND = 0, NOT_FOUND = 1, ERROR_STATUS = 2,
    };

    /**
     * ReplFrameHeader - Fixed replication frame header.
     *
     * Wire layout, little-endian:
     *   [magic:u32][type:u8][flags:u8][payloadLen:u32][payloadCrc32c:u32]
     *
     * The header is followed by payloadLen bytes.  CRC covers only the
     * payload, not the header.
     */
    struct AKKARADB_CLUSTER_RUNTIME_API ReplFrameHeader {
        static constexpr uint32_t MAGIC = 0x35524B41; // "AKR5"
        static constexpr size_t SIZE = 14;
        static constexpr uint32_t MAX_PAYLOAD_SIZE = 128u * 1024u * 1024u;

        ReplMsgType type{}; ///< Message discriminator.
        uint8_t flags = 0; ///< Reserved per-frame flags.
        uint32_t payloadLen = 0; ///< Payload byte length.
        uint32_t crc32c = 0; ///< CRC32C of the payload bytes.
    };

    /**
     * DecodedFrame - Validated frame returned by decodeFrame().
     */
    struct AKKARADB_CLUSTER_RUNTIME_API DecodedFrame {
        ReplMsgType type{};
        uint8_t flags = 0;
        std::vector<uint8_t> payload;
    };

    /** Replica-to-primary handshake payload. */
    struct AKKARADB_CLUSTER_RUNTIME_API ClientHello {
        uint64_t nodeId = 0; ///< Replica node id.
        uint64_t lastSeq = 0; ///< Last sequence already applied by the replica.
        NodeRole role = NodeRole::REPLICA; ///< Expected to be NodeRole::REPLICA.
        uint64_t groupId = 0; ///< Non-Raft cluster group identity.
        uint64_t groupEpoch = 1; ///< Non-Raft cluster group epoch.
    };

    /** Primary-to-replica handshake response payload. */
    struct AKKARADB_CLUSTER_RUNTIME_API ServerHello {
        uint64_t nodeId = 0; ///< Primary node id.
        uint64_t currentSeq = 0; ///< Primary's current sequence at handshake time.
        NodeRole role = NodeRole::PRIMARY; ///< Expected to be NodeRole::PRIMARY.
        uint64_t groupId = 0; ///< Non-Raft cluster group identity.
        uint64_t groupEpoch = 1; ///< Non-Raft cluster group epoch.
    };

    /** Replicated key/value mutation payload. */
    struct AKKARADB_CLUSTER_RUNTIME_API ReplEntry {
        uint64_t seq = 0; ///< Monotonic sequence assigned by the source engine.
        uint64_t sourceNodeId = 0; ///< Node that originally produced the entry.
        ReplOpType op = ReplOpType::PUT; ///< Mutation type.
        uint8_t recordFlags = 0; ///< Record flags preserved for storage apply.
        std::vector<uint8_t> key; ///< Raw key bytes.
        std::vector<uint8_t> value; ///< Raw value bytes, empty for Remove.
    };

    /** Replicated blob payload. */
    struct AKKARADB_CLUSTER_RUNTIME_API ReplBlob {
        uint64_t seq = 0; ///< Sequence associated with the blob reference.
        uint64_t blobId = 0; ///< Stable blob identifier.
        std::vector<uint8_t> content; ///< Raw blob content.
    };

    /** Replica acknowledgement payload. */
    struct AKKARADB_CLUSTER_RUNTIME_API ReplAck {
        uint64_t seq = 0; ///< Highest entry sequence acknowledged by the replica.
        AckStage stage = AckStage::APPLIED; ///< Replication lifecycle stage reached for seq.
    };

    struct AKKARADB_CLUSTER_RUNTIME_API ReplSnapshotBegin {
        uint64_t snapshotSeq = 0;
        uint64_t entryCount = 0;
    };

    struct AKKARADB_CLUSTER_RUNTIME_API ReplSnapshotEntry {
        std::vector<uint8_t> key;
        std::vector<uint8_t> value;
    };

    /** Reserved point-in-time read request payload. */
    struct AKKARADB_CLUSTER_RUNTIME_API ReadRequest {
        uint64_t requestId = 0;
        uint64_t snapshotSeq = 0;
        std::vector<uint8_t> key;
    };

    /** Reserved point-in-time read response payload. */
    struct AKKARADB_CLUSTER_RUNTIME_API ReadResponse {
        uint64_t requestId = 0;
        ReadStatus status = ReadStatus::ERROR_STATUS;
        uint8_t recordFlags = 0;
        uint64_t seq = 0;
        std::vector<uint8_t> value;
    };

    /**
     * Encodes a complete frame with header, payload, and payload CRC32C.
     */
    [[nodiscard]] AKKARADB_CLUSTER_RUNTIME_API std::vector<uint8_t> encodeFrame(
        ReplMsgType type,
        std::span<const uint8_t> payload,
        uint8_t flags = 0
    );

    /**
     * Decodes and validates a complete frame.
     *
     * @return false on short input, bad magic, length mismatch, or CRC mismatch.
     */
    [[nodiscard]] AKKARADB_CLUSTER_RUNTIME_API bool decodeFrame(std::span<const uint8_t> wire, DecodedFrame& out);

    /** Encodes a ClientHello frame. */
    [[nodiscard]] AKKARADB_CLUSTER_RUNTIME_API std::vector<uint8_t> encodeClientHello(const ClientHello& hello);

    /** Encodes a ServerHello frame. */
    [[nodiscard]] AKKARADB_CLUSTER_RUNTIME_API std::vector<uint8_t> encodeServerHello(const ServerHello& hello);

    /** Encodes a ReplEntry frame. */
    [[nodiscard]] AKKARADB_CLUSTER_RUNTIME_API std::vector<uint8_t> encodeEntry(const ReplEntry& entry);

    /** Encodes a ReplBlob frame. */
    [[nodiscard]] AKKARADB_CLUSTER_RUNTIME_API std::vector<uint8_t> encodeBlob(const ReplBlob& blob);

    /** Encodes a ReplAck frame. */
    [[nodiscard]] AKKARADB_CLUSTER_RUNTIME_API std::vector<uint8_t> encodeAck(const ReplAck& ack);
    [[nodiscard]] AKKARADB_CLUSTER_RUNTIME_API std::vector<uint8_t> encodeSnapshotBegin(const ReplSnapshotBegin& begin);
    [[nodiscard]] AKKARADB_CLUSTER_RUNTIME_API std::vector<uint8_t> encodeSnapshotEntry(const ReplSnapshotEntry& entry);
    [[nodiscard]] AKKARADB_CLUSTER_RUNTIME_API std::vector<uint8_t> encodeSnapshotEnd(uint64_t snapshotSeq);

    /** Encodes a ReadRequest frame. */
    [[nodiscard]] AKKARADB_CLUSTER_RUNTIME_API std::vector<uint8_t> encodeReadRequest(const ReadRequest& request);

    /** Encodes a ReadResponse frame. */
    [[nodiscard]] AKKARADB_CLUSTER_RUNTIME_API std::vector<uint8_t> encodeReadResponse(const ReadResponse& response);

    /** Decodes a ClientHello payload. */
    [[nodiscard]] AKKARADB_CLUSTER_RUNTIME_API bool decodeClientHello(std::span<const uint8_t> payload, ClientHello& out);

    /** Decodes a ServerHello payload. */
    [[nodiscard]] AKKARADB_CLUSTER_RUNTIME_API bool decodeServerHello(std::span<const uint8_t> payload, ServerHello& out);

    /** Decodes a ReplEntry payload. */
    [[nodiscard]] AKKARADB_CLUSTER_RUNTIME_API bool decodeEntry(std::span<const uint8_t> payload, ReplEntry& out);

    /** Decodes a ReplBlob payload. */
    [[nodiscard]] AKKARADB_CLUSTER_RUNTIME_API bool decodeBlob(std::span<const uint8_t> payload, ReplBlob& out);

    /** Decodes a ReplAck payload. */
    [[nodiscard]] AKKARADB_CLUSTER_RUNTIME_API bool decodeAck(std::span<const uint8_t> payload, ReplAck& out);
    [[nodiscard]] AKKARADB_CLUSTER_RUNTIME_API bool decodeSnapshotBegin(std::span<const uint8_t> payload, ReplSnapshotBegin& out);
    [[nodiscard]] AKKARADB_CLUSTER_RUNTIME_API bool decodeSnapshotEntry(std::span<const uint8_t> payload, ReplSnapshotEntry& out);
    [[nodiscard]] AKKARADB_CLUSTER_RUNTIME_API bool decodeSnapshotEnd(std::span<const uint8_t> payload, uint64_t& snapshotSeq);

    /** Decodes a ReadRequest payload. */
    [[nodiscard]] AKKARADB_CLUSTER_RUNTIME_API bool decodeReadRequest(std::span<const uint8_t> payload, ReadRequest& out);

    /** Decodes a ReadResponse payload. */
    [[nodiscard]] AKKARADB_CLUSTER_RUNTIME_API bool decodeReadResponse(std::span<const uint8_t> payload, ReadResponse& out);
} // namespace akkaradb::engine::cluster

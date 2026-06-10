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

// akkengine/include/akk/engine/manifest/ManifestFraming.hpp
#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <vector>
#include "akk/cpu/CRC32C.hpp"

namespace akkaradb::engine::manifest {
    // ============================================================================
    // ManifestRecordType
    // ============================================================================

    /**
     * ManifestRecordType - Discriminator for manifest record payload.
     */
    enum class ManifestRecordType : uint8_t {
        STRIPE_COMMIT = 0x01,
        ///< Stripe counter advance
        SST_SEAL = 0x02,
        ///< New SST file sealed (L0 flush only)
        SST_DELETE = 0x03,
        ///< SST file deleted (legacy; superseded by CompactionCommit)
        COMPACTION_START = 0x04,
        ///< Compaction began (informational hint)
        COMPACTION_END = 0x05,
        ///< Compaction completed (legacy single-output format)
        CHECKPOINT = 0x06,
        ///< Checkpoint marker
        TRUNCATE = 0x07,
        ///< Manifest truncate marker
        /**
         * Atomic compaction commit  Ereplaces CompactionEnd + SSTDelete.
         *
         * A single CRC-protected record that simultaneously adds all output files
         * and removes all input files from the live set.  If this record is absent
         * (e.g. process killed mid-write, detected by CRC mismatch), replay treats
         * the old input files as still live and discards the orphan output files.
         */
        COMPACTION_COMMIT = 0x08,

        // ── Cluster events (v4) ──────────────────────────────────────────
        NODE_JOIN = 0x10,
        ///< A node joined the cluster
        NODE_LEAVE = 0x11,
        ///< A node left the cluster
        PRIMARY_LEASE = 0x12,
        ///< Primary lease record (nodeId + expiry)
    };

    // ============================================================================
    // ManifestFileHeader - Written once at the start of each .akmf file
    // ============================================================================

    /**
     * ManifestFileHeader - Fixed 32-byte header at the start of every .akmf file.
     *
     * On-disk layout (32 bytes, all fields LE):
     * [magic:u32][version:u16][flags:u16][fileSeq:u32][createdAtUs:u64][crc32c:u32][reserved:u8ÁE]
     *
     * Design:
     * - magic: Format / corruption detection
     * - version: Forward-compatibility
     * - fileSeq: Rotation counter (0 = base file, N = Nth rotated file)
     * - createdAtUs: Microseconds since epoch
     * - crc32c: CRC32C of this header with crc32c field zeroed
     */
    #pragma pack(push, 1)
    struct ManifestFileHeader {
        static constexpr uint32_t MAGIC = 0x35564D41; ///< "AMV5" (Manifest v5)
        static constexpr uint16_t VERSION = 0x0001;

        uint32_t magic;
        uint16_t version;
        uint16_t flags;
        uint32_t fileSeq; ///< Rotation counter
        uint64_t createdAtUs; ///< Creation timestamp (μs since epoch)
        uint32_t crc32c;
        uint8_t reserved[8];

        static constexpr size_t SIZE = 32;

        [[nodiscard]] bool verifyMagic() const noexcept { return magic == MAGIC; }
        [[nodiscard]] bool verifyVersion() const noexcept { return version == VERSION; }

        /**
         * Verifies the header checksum.
         * Recomputes CRC with crc32c field zeroed and compares.
         */
        [[nodiscard]] bool verifyChecksum() const noexcept;

        /**
         * Builds and returns a valid header as a byte array.
         * Computes and fills crc32c automatically.
         */
        [[nodiscard]] static ManifestFileHeader build(uint32_t fileSeq) noexcept;

        /**
         * Serializes this header to a 32-byte output buffer.
         */
        void serialize(uint8_t out[SIZE]) const noexcept;
    };
    #pragma pack(pop)

    static_assert(sizeof(ManifestFileHeader) == 32, "ManifestFileHeader must be 32 bytes");

    // ============================================================================
    // ManifestRecordHeader - Prefix for every manifest record
    // ============================================================================

    /**
     * ManifestRecordHeader - 8-byte header preceding each manifest record payload.
     *
     * On-disk layout (8 bytes, all fields LE):
     * [type:u8][flags:u8][payloadLen:u16][crc32c:u32]
     *
     * Design:
     * - type: ManifestRecordType discriminator
     * - flags: Reserved (0 for now)
     * - payloadLen: Byte length of the payload that follows
     * - crc32c: CRC32C of payload bytes only
     */
    #pragma pack(push, 1)
    struct ManifestRecordHeader {
        uint8_t type;
        uint8_t flags;
        uint16_t payloadLen;
        uint32_t crc32c;

        static constexpr size_t SIZE = 8;

        /**
         * Builds a header for a given payload.
         */
        [[nodiscard]] static ManifestRecordHeader build(ManifestRecordType type, const uint8_t* payload, uint16_t payloadLen) noexcept;

        /**
         * Serializes this header to an 8-byte output buffer.
         */
        void serialize(uint8_t out[SIZE]) const noexcept;

        /**
         * Deserializes a header from 8 bytes.
         */
        [[nodiscard]] static ManifestRecordHeader deserialize(const uint8_t in[SIZE]) noexcept;

        /**
         * Verifies the payload CRC.
         */
        [[nodiscard]] bool verifyPayload(const uint8_t* payload, uint16_t len) const noexcept {
            return crc32c == cpu::CRC32C(reinterpret_cast<const std::byte*>(payload), len);
        }
    };
    #pragma pack(pop)

    static_assert(sizeof(ManifestRecordHeader) == 8, "ManifestRecordHeader must be 8 bytes");

    // ============================================================================
    // Sentinel values
    // ============================================================================

    static constexpr uint64_t MANIFEST_ABSENT_U64 = UINT64_MAX; ///< Optional u64 absent sentinel

    // ============================================================================
    // Encode functions  Eproduce raw payload bytes for each record type
    // ============================================================================

    /**
     * Encodes a StripeCommit payload.
     * Payload (16 bytes): [tsUs:u64][stripeCount:u64]
     */
    [[nodiscard]] std::vector<uint8_t> encodeStripeCommit(uint64_t tsUs, uint64_t stripeCount);

    /**
     * Encodes an SSTSeal payload.
     *
     * Payload fixed (24 bytes):
     *   [tsUs:u64][entries:u64][level:u8][keyFlags:u8][nameLen:u16][fkLen:u16][lkLen:u16]
     * Variable: name bytes, firstKey bytes (if keyFlags bit0), lastKey bytes (if keyFlags bit1)
     */
    [[nodiscard]] std::vector<uint8_t> encodeSstSeal(
        uint64_t tsUs,
        int level,
        const std::string& name,
        uint64_t entries,
        const std::optional<std::string>& firstKeyHex,
        const std::optional<std::string>& lastKeyHex
    );

    /**
     * Encodes an SSTDelete payload.
     * Payload fixed (10 bytes): [tsUs:u64][nameLen:u16]
     * Variable: name bytes
     */
    [[nodiscard]] std::vector<uint8_t> encodeSstDelete(uint64_t tsUs, const std::string& name);

    /**
     * Encodes a CompactionStart payload.
     * Payload fixed (12 bytes): [tsUs:u64][level:u8][inputCount:u8][reserved:u16]
     * Variable: [len:u16][bytes] ÁEinput_count
     */
    [[nodiscard]] std::vector<uint8_t> encodeCompactionStart(uint64_t tsUs, int level, const std::vector<std::string>& inputs);

    /**
     * Encodes a CompactionEnd payload.
     *
     * Payload fixed (28 bytes):
     *   [tsUs:u64][entries:u64][level:u8][keyFlags:u8][inputCount:u8][reserved:u8]
     *   [outLen:u16][fkLen:u16][lkLen:u16][reserved:u16]
     * Variable: output bytes, firstKey bytes, lastKey bytes, [len:u16 + bytes] ÁEinput_count
     */
    [[nodiscard]] std::vector<uint8_t> encodeCompactionEnd(
        uint64_t tsUs,
        int level,
        const std::string& output,
        const std::vector<std::string>& inputs,
        uint64_t entries,
        const std::optional<std::string>& firstKeyHex,
        const std::optional<std::string>& lastKeyHex
    );

    /**
     * Encodes a Checkpoint payload.
     *
     * Payload fixed (26 bytes): [tsUs:u64][stripe:u64][lastSeq:u64][nameLen:u16]
     * Variable: name bytes
     *
     * stripe / lastSeq use MANIFEST_ABSENT_U64 when not present.
     * nameLen = 0 when no name.
     */
    [[nodiscard]] std::vector<uint8_t> encodeCheckpoint(
        uint64_t tsUs,
        const std::optional<std::string>& name,
        const std::optional<uint64_t>& stripe,
        const std::optional<uint64_t>& lastSeq
    );

    /**
     * Encodes a Truncate payload.
     * Payload fixed (10 bytes): [tsUs:u64][reasonLen:u16]
     * Variable: reason bytes
     */
    [[nodiscard]] std::vector<uint8_t> encodeTruncate(uint64_t tsUs, const std::optional<std::string>& reason);

    /**
     * Encodes a CompactionCommit payload (atomic multi-file compaction result).
     *
     * Payload fixed (12 bytes):
     *   [tsUs:u64][outputCount:u8][inputCount:u8][reserved:u16]
     * Variable:
     *   outputCount ÁE[nameLen:u16][name bytes]
     *   inputCount  ÁE[nameLen:u16][name bytes]
     *
     * During replay, this record atomically:
     *   - Adds all outputFiles to the live set
     *   - Removes all inputFiles from the live set
     * If this record is absent (CRC mismatch = interrupted write), the live set
     * is left unchanged (old input files remain live; orphan outputs are ignored).
     */
    [[nodiscard]] std::vector<uint8_t> encodeCompactionCommit(
        uint64_t tsUs,
        const std::vector<std::string>& outputFiles,
        const std::vector<std::string>& inputFiles
    );

    // ============================================================================
    // Decode helpers  Eparse payload bytes into structured fields
    // ============================================================================

    struct DecodedStripeCommit {
        uint64_t tsUs;
        uint64_t stripeCount;
    };

    struct DecodedSSTSeal {
        uint64_t tsUs;
        uint64_t entries;
        int level;
        std::string name;
        std::optional<std::string> firstKeyHex;
        std::optional<std::string> lastKeyHex;
    };

    struct DecodedSSTDelete {
        uint64_t tsUs;
        std::string name;
    };

    struct DecodedCompactionStart {
        uint64_t tsUs;
        int level;
        std::vector<std::string> inputs;
    };

    struct DecodedCompactionEnd {
        uint64_t tsUs;
        uint64_t entries;
        int level;
        std::string output;
        std::vector<std::string> inputs;
        std::optional<std::string> firstKeyHex;
        std::optional<std::string> lastKeyHex;
    };

    struct DecodedCheckpoint {
        uint64_t tsUs;
        std::optional<uint64_t> stripe;
        std::optional<uint64_t> lastSeq;
        std::optional<std::string> name;
    };

    struct DecodedTruncate {
        uint64_t tsUs;
        std::optional<std::string> reason;
    };

    struct DecodedCompactionCommit {
        uint64_t tsUs;
        std::vector<std::string> outputFiles;
        std::vector<std::string> inputFiles;
    };

    /**
     * Decode functions.  Return false if payload is malformed / too short.
     */
    [[nodiscard]] bool decodeStripeCommit(const uint8_t* payload, uint16_t len, DecodedStripeCommit& out);
    [[nodiscard]] bool decodeSstSeal(const uint8_t* payload, uint16_t len, DecodedSSTSeal& out);
    [[nodiscard]] bool decodeSstDelete(const uint8_t* payload, uint16_t len, DecodedSSTDelete& out);
    [[nodiscard]] bool decodeCompactionStart(const uint8_t* payload, uint16_t len, DecodedCompactionStart& out);
    [[nodiscard]] bool decodeCompactionEnd(const uint8_t* payload, uint16_t len, DecodedCompactionEnd& out);
    [[nodiscard]] bool decodeCheckpoint(const uint8_t* payload, uint16_t len, DecodedCheckpoint& out);
    [[nodiscard]] bool decodeTruncate(const uint8_t* payload, uint16_t len, DecodedTruncate& out);
    [[nodiscard]] bool decodeCompactionCommit(const uint8_t* payload, uint16_t len, DecodedCompactionCommit& out);

    // ========================================================================
    // Cluster event encode / decode (v4)
    // ========================================================================

    /**
     * Encodes a NodeJoin payload.
     * Payload: [tsUs:u64][nodeId:u64][replPort:u16][hostLen:u16][host bytes]
     */
    [[nodiscard]] std::vector<uint8_t> encodeNodeJoin(uint64_t tsUs, uint64_t nodeId, uint16_t replPort, const std::string& host);

    /**
     * Encodes a NodeLeave payload.
     * Payload (16 bytes): [tsUs:u64][nodeId:u64]
     */
    [[nodiscard]] std::vector<uint8_t> encodeNodeLeave(uint64_t tsUs, uint64_t nodeId);

    /**
     * Encodes a PrimaryLease payload.
     * Payload (24 bytes): [tsUs:u64][nodeId:u64][leaseUntilUs:u64]
     */
    [[nodiscard]] std::vector<uint8_t> encodePrimaryLease(uint64_t tsUs, uint64_t nodeId, uint64_t leaseUntilUs);

    struct DecodedNodeJoin {
        uint64_t tsUs;
        uint64_t nodeId;
        uint16_t replPort;
        std::string host;
    };

    struct DecodedNodeLeave {
        uint64_t tsUs;
        uint64_t nodeId;
    };

    struct DecodedPrimaryLease {
        uint64_t tsUs;
        uint64_t nodeId;
        uint64_t leaseUntilUs;
    };

    [[nodiscard]] bool decodeNodeJoin(const uint8_t* payload, uint16_t len, DecodedNodeJoin& out);
    [[nodiscard]] bool decodeNodeLeave(const uint8_t* payload, uint16_t len, DecodedNodeLeave& out);
    [[nodiscard]] bool decodePrimaryLease(const uint8_t* payload, uint16_t len, DecodedPrimaryLease& out);
} // namespace akkaradb::engine::manifest

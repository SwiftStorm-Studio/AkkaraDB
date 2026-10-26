/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
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
#include "akkaradb/Export.hpp"

namespace akkaradb::engine::manifest {
    // ============================================================================
    // ManifestRecordType
    // ============================================================================

    enum class ManifestRecordType : uint8_t {
        STRIPE_COMMIT = 0x01,
        ///< Stripe counter advance
        SST_SEAL = 0x02,
        ///< New SST file sealed (L0 flush only)
        SST_DELETE = 0x03,
        ///< SST file deleted (individual SST deletion)
        COMPACTION_START = 0x04,
        ///< Compaction began (informational hint)
        COMPACTION_END = 0x05,
        ///< Compaction completed (single-output completion event)
        CHECKPOINT = 0x06,
        ///< Checkpoint marker
        TRUNCATE = 0x07,
        ///< Manifest truncate marker

        COMPACTION_COMMIT = 0x08,
        SST_BLOB_REFS = 0x09,
        ///< Blob ids referenced by one SST file

        // Cluster events (v1)
        NODE_JOIN = 0x10,
        ///< A node joined the cluster
        NODE_LEAVE = 0x11,
        ///< A node left the cluster
        PRIMARY_LEASE = 0x12,
        ///< Primary lease record (nodeId + expiry)

        // Blob lifecycle events (v1)
        BLOB_PUT = 0x20,
        ///< Blob file was durably written
        BLOB_DELETE = 0x21,
        ///< Blob file was deleted by GC
    };

    // ============================================================================
    // ManifestFileHeader - Written once at the start of each .akmf file
    // ============================================================================

    /**
     * ManifestFileHeader - Fixed 32-byte header at the start of every .akmf file.
     *
     * On-disk layout (32 bytes, all fields LE):
     * [magic:u32][version:u16][flags:u16][fileSeq:u32][createdAtUs:u64][crc32c:u32][reserved:u8 x 8]
     *
     * Design:
     * - magic: Format / corruption detection
     * - version: Forward-compatibility
     * - fileSeq: Rotation counter (0 = base file, N = Nth rotated file)
     * - createdAtUs: Microseconds since epoch
     * - crc32c: CRC32C of this header with crc32c field zeroed
     */
    #pragma pack(push, 1)
    struct AKDB_API ManifestFileHeader {
        static constexpr uint32_t MAGIC = 0x31564D41; ///< "AMV1" (Manifest v1)
        static constexpr uint16_t VERSION = 0x0001;

        uint32_t magic;
        uint16_t version;
        uint16_t flags;
        uint32_t fileSeq; ///< Rotation counter
        uint64_t createdAtUs; ///< Creation timestamp (microseconds since epoch)
        uint32_t crc32c;
        uint8_t reserved[8];

        static constexpr size_t SIZE = 32;

        [[nodiscard]] bool verifyMagic() const noexcept { return magic == MAGIC; }
        [[nodiscard]] bool verifyVersion() const noexcept { return version == VERSION; }

        [[nodiscard]] bool verifyChecksum() const noexcept;

        [[nodiscard]] static ManifestFileHeader build(uint32_t fileSeq) noexcept;

        void serialize(uint8_t out[SIZE]) const noexcept;
    };
    #pragma pack(pop)

    static_assert(sizeof(ManifestFileHeader) == 32, "ManifestFileHeader must be 32 bytes");

    // ============================================================================
    // ManifestRecordHeader - Prefix for every manifest record
    // ============================================================================

    #pragma pack(push, 1)
    // V1 record: type:u8, flags:u8, reserved:u16, payloadLen:u32, payload CRC:u32, header CRC:u32.
    // Strings and collection counts in v1 payloads use checked u32 fields.
    struct AKDB_API ManifestRecordHeader {
        uint8_t type;
        uint8_t flags;
        uint16_t reserved;
        uint32_t payloadLen;
        uint32_t crc32c;
        uint32_t headerCrc32c;

        static constexpr size_t SIZE = 16;

        [[nodiscard]] static ManifestRecordHeader build(ManifestRecordType type, const uint8_t* payload, uint32_t payloadLen) noexcept;

        void serialize(uint8_t out[SIZE]) const noexcept;

        [[nodiscard]] static ManifestRecordHeader deserialize(const uint8_t in[SIZE]) noexcept;

        [[nodiscard]] bool verifyChecksum() const noexcept;

        [[nodiscard]] bool verifyPayload(const uint8_t* payload, uint32_t len) const noexcept {
            return crc32c == cpu::CRC32C(reinterpret_cast<const std::byte*>(payload), len);
        }
    };
    #pragma pack(pop)

    static_assert(sizeof(ManifestRecordHeader) == 16, "ManifestRecordHeader must be 16 bytes");

    // ============================================================================
    // Sentinel values
    // ============================================================================

    static constexpr uint64_t MANIFEST_ABSENT_U64 = UINT64_MAX; ///< Optional u64 absent sentinel

    // ============================================================================
    // Encode functions - produce raw payload bytes for each record type
    // ============================================================================

    [[nodiscard]] AKDB_API std::vector<uint8_t> encodeStripeCommit(uint64_t tsUs, uint64_t stripeCount);

    [[nodiscard]] AKDB_API std::vector<uint8_t> encodeSstSeal(
        uint64_t tsUs,
        int level,
        const std::string& name,
        uint64_t entries,
        const std::optional<std::string>& firstKeyHex,
        const std::optional<std::string>& lastKeyHex
    );

    [[nodiscard]] AKDB_API std::vector<uint8_t> encodeSstDelete(uint64_t tsUs, const std::string& name);

    [[nodiscard]] AKDB_API std::vector<uint8_t> encodeCompactionStart(uint64_t tsUs, int level, const std::vector<std::string>& inputs);

    [[nodiscard]] AKDB_API std::vector<uint8_t> encodeCompactionEnd(
        uint64_t tsUs,
        int level,
        const std::string& output,
        const std::vector<std::string>& inputs,
        uint64_t entries,
        const std::optional<std::string>& firstKeyHex,
        const std::optional<std::string>& lastKeyHex
    );

    [[nodiscard]] AKDB_API std::vector<uint8_t> encodeCheckpoint(
        uint64_t tsUs,
        const std::optional<std::string>& name,
        const std::optional<uint64_t>& stripe,
        const std::optional<uint64_t>& lastSeq
    );

    [[nodiscard]] AKDB_API std::vector<uint8_t> encodeTruncate(uint64_t tsUs, const std::optional<std::string>& reason);

    [[nodiscard]] AKDB_API std::vector<uint8_t> encodeCompactionCommit(
        uint64_t tsUs,
        const std::vector<std::string>& outputFiles,
        const std::vector<std::string>& inputFiles
    );

    struct AKDB_API SSTBlobRefEntry {
        std::vector<uint8_t> key;
        uint64_t seq;
        uint8_t flags;
        std::optional<uint64_t> blobId;
    };

    [[nodiscard]] AKDB_API std::vector<uint8_t> encodeSstBlobRefs(
        uint64_t tsUs,
        const std::string& name,
        const std::vector<SSTBlobRefEntry>& entries
    );

    // ============================================================================
    // Decode helpers - parse payload bytes into structured fields
    // ============================================================================

    struct AKDB_API DecodedStripeCommit {
        uint64_t tsUs;
        uint64_t stripeCount;
    };

    struct AKDB_API DecodedSSTSeal {
        uint64_t tsUs;
        uint64_t entries;
        int level;
        std::string name;
        std::optional<std::string> firstKeyHex;
        std::optional<std::string> lastKeyHex;
    };

    struct AKDB_API DecodedSSTDelete {
        uint64_t tsUs;
        std::string name;
    };

    struct AKDB_API DecodedCompactionStart {
        uint64_t tsUs;
        int level;
        std::vector<std::string> inputs;
    };

    struct AKDB_API DecodedCompactionEnd {
        uint64_t tsUs;
        uint64_t entries;
        int level;
        std::string output;
        std::vector<std::string> inputs;
        std::optional<std::string> firstKeyHex;
        std::optional<std::string> lastKeyHex;
    };

    struct AKDB_API DecodedCheckpoint {
        uint64_t tsUs;
        std::optional<uint64_t> stripe;
        std::optional<uint64_t> lastSeq;
        std::optional<std::string> name;
    };

    struct AKDB_API DecodedTruncate {
        uint64_t tsUs;
        std::optional<std::string> reason;
    };

    struct AKDB_API DecodedCompactionCommit {
        uint64_t tsUs;
        std::vector<std::string> outputFiles;
        std::vector<std::string> inputFiles;
    };

    struct AKDB_API DecodedSSTBlobRefs {
        uint64_t tsUs;
        std::string name;
        std::vector<SSTBlobRefEntry> entries;
    };

    [[nodiscard]] AKDB_API bool decodeStripeCommit(const uint8_t* payload, uint32_t len, DecodedStripeCommit& out);
    [[nodiscard]] AKDB_API bool decodeSstSeal(const uint8_t* payload, uint32_t len, DecodedSSTSeal& out);
    [[nodiscard]] AKDB_API bool decodeSstDelete(const uint8_t* payload, uint32_t len, DecodedSSTDelete& out);
    [[nodiscard]] AKDB_API bool decodeCompactionStart(const uint8_t* payload, uint32_t len, DecodedCompactionStart& out);
    [[nodiscard]] AKDB_API bool decodeCompactionEnd(const uint8_t* payload, uint32_t len, DecodedCompactionEnd& out);
    [[nodiscard]] AKDB_API bool decodeCheckpoint(const uint8_t* payload, uint32_t len, DecodedCheckpoint& out);
    [[nodiscard]] AKDB_API bool decodeTruncate(const uint8_t* payload, uint32_t len, DecodedTruncate& out);
    [[nodiscard]] AKDB_API bool decodeCompactionCommit(const uint8_t* payload, uint32_t len, DecodedCompactionCommit& out);
    [[nodiscard]] AKDB_API bool decodeSstBlobRefs(const uint8_t* payload, uint32_t len, DecodedSSTBlobRefs& out);

    // ========================================================================
    // Blob lifecycle encode / decode (v1)
    // ========================================================================

    [[nodiscard]] AKDB_API std::vector<uint8_t> encodeBlobPut(
        uint64_t tsUs,
        uint64_t blobId,
        uint64_t totalSize,
        uint64_t storedSize,
        uint32_t contentCrc32c,
        uint32_t codec
    );

    [[nodiscard]] AKDB_API std::vector<uint8_t> encodeBlobDelete(uint64_t tsUs, uint64_t blobId);

    struct AKDB_API DecodedBlobPut {
        uint64_t tsUs;
        uint64_t blobId;
        uint64_t totalSize;
        uint64_t storedSize;
        uint32_t contentCrc32c;
        uint32_t codec;
    };

    struct AKDB_API DecodedBlobDelete {
        uint64_t tsUs;
        uint64_t blobId;
    };

    [[nodiscard]] AKDB_API bool decodeBlobPut(const uint8_t* payload, uint32_t len, DecodedBlobPut& out);
    [[nodiscard]] AKDB_API bool decodeBlobDelete(const uint8_t* payload, uint32_t len, DecodedBlobDelete& out);

    // ========================================================================
    // Cluster event encode / decode (v1)
    // ========================================================================

    [[nodiscard]] AKDB_API std::vector<uint8_t> encodeNodeJoin(uint64_t tsUs, uint64_t nodeId, uint16_t replPort, const std::string& host);

    [[nodiscard]] AKDB_API std::vector<uint8_t> encodeNodeLeave(uint64_t tsUs, uint64_t nodeId);

    [[nodiscard]] AKDB_API std::vector<uint8_t> encodePrimaryLease(uint64_t tsUs, uint64_t nodeId, uint64_t leaseUntilUs);

    struct AKDB_API DecodedNodeJoin {
        uint64_t tsUs;
        uint64_t nodeId;
        uint16_t replPort;
        std::string host;
    };

    struct AKDB_API DecodedNodeLeave {
        uint64_t tsUs;
        uint64_t nodeId;
    };

    struct AKDB_API DecodedPrimaryLease {
        uint64_t tsUs;
        uint64_t nodeId;
        uint64_t leaseUntilUs;
    };

    [[nodiscard]] AKDB_API bool decodeNodeJoin(const uint8_t* payload, uint32_t len, DecodedNodeJoin& out);
    [[nodiscard]] AKDB_API bool decodeNodeLeave(const uint8_t* payload, uint32_t len, DecodedNodeLeave& out);
    [[nodiscard]] AKDB_API bool decodePrimaryLease(const uint8_t* payload, uint32_t len, DecodedPrimaryLease& out);
} // namespace akkaradb::engine::manifest

/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/manifest/ManifestFraming.cpp
#include "akk/engine/manifest/ManifestFraming.hpp"
#include <chrono>
#include <stdexcept>

namespace akkaradb::engine::manifest {
    // ============================================================================
    // Internal helpers
    // ============================================================================

    namespace {
        uint64_t nowUs() noexcept {
            return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch()
            ).count());
        }

        // Write a little-endian u16 into a byte buffer at offset.
        inline void writeU16(uint8_t* buf, size_t off, uint16_t v) noexcept {
            buf[off] = static_cast<uint8_t>(v);
            buf[off + 1] = static_cast<uint8_t>(v >> 8);
        }

        // Write a little-endian u32 into a byte buffer at offset.
        inline void writeU32(uint8_t* buf, size_t off, uint32_t v) noexcept {
            buf[off] = static_cast<uint8_t>(v);
            buf[off + 1] = static_cast<uint8_t>(v >> 8);
            buf[off + 2] = static_cast<uint8_t>(v >> 16);
            buf[off + 3] = static_cast<uint8_t>(v >> 24);
        }

        // Write a little-endian u64 into a byte buffer at offset.
        inline void writeU64(uint8_t* buf, size_t off, uint64_t v) noexcept {
            buf[off] = static_cast<uint8_t>(v);
            buf[off + 1] = static_cast<uint8_t>(v >> 8);
            buf[off + 2] = static_cast<uint8_t>(v >> 16);
            buf[off + 3] = static_cast<uint8_t>(v >> 24);
            buf[off + 4] = static_cast<uint8_t>(v >> 32);
            buf[off + 5] = static_cast<uint8_t>(v >> 40);
            buf[off + 6] = static_cast<uint8_t>(v >> 48);
            buf[off + 7] = static_cast<uint8_t>(v >> 56);
        }

        // Read a little-endian u16 from a byte buffer at offset.
        inline uint16_t readU16(const uint8_t* buf, size_t off) noexcept {
            return static_cast<uint16_t>(buf[off]) | (static_cast<uint16_t>(buf[off + 1]) << 8);
        }

        // Read a little-endian u32 from a byte buffer at offset.
        inline uint32_t readU32(const uint8_t* buf, size_t off) noexcept {
            return static_cast<uint32_t>(buf[off]) | (static_cast<uint32_t>(buf[off + 1]) << 8) | (static_cast<uint32_t>(buf[off + 2]) <<
                16) | (static_cast<uint32_t>(buf[off + 3]) << 24);
        }

        // Read a little-endian u64 from a byte buffer at offset.
        inline uint64_t readU64(const uint8_t* buf, size_t off) noexcept {
            return static_cast<uint64_t>(buf[off]) | (static_cast<uint64_t>(buf[off + 1]) << 8) | (static_cast<uint64_t>(buf[off + 2]) <<
                16) | (static_cast<uint64_t>(buf[off + 3]) << 24) | (static_cast<uint64_t>(buf[off + 4]) << 32) | (static_cast<uint64_t>(buf
                [off + 5]) << 40) | (static_cast<uint64_t>(buf[off + 6]) << 48) | (static_cast<uint64_t>(buf[off + 7]) << 56);
        }

        // Append a string with a u16 length prefix.
        void appendLengthPrefixed(std::vector<uint8_t>& buf, const std::string& s) {
            const auto len = static_cast<uint16_t>(s.size());
            const size_t off = buf.size();
            buf.resize(off + 2 + len);
            writeU16(buf.data(), off, len);
            std::memcpy(buf.data() + off + 2, s.data(), len);
        }

        // Read a length-prefixed string from payload at cursor; advances cursor.
        bool readLengthPrefixed(const uint8_t* payload, uint16_t payloadLen, size_t& cursor, std::string& out) {
            if (cursor + 2 > payloadLen) { return false; }
            const uint16_t len = readU16(payload, cursor);
            cursor += 2;
            if (cursor + len > payloadLen) { return false; }
            out.assign(reinterpret_cast<const char*>(payload + cursor), len);
            cursor += len;
            return true;
        }
    } // anonymous namespace

    // ============================================================================
    // ManifestFileHeader
    // ============================================================================

    ManifestFileHeader ManifestFileHeader::build(uint32_t fileSeq) noexcept {
        ManifestFileHeader hdr{};
        hdr.magic = MAGIC;
        hdr.version = VERSION;
        hdr.flags = 0;
        hdr.fileSeq = fileSeq;
        hdr.createdAtUs = nowUs();
        hdr.crc32c = 0;
        std::memset(hdr.reserved, 0, sizeof(hdr.reserved));

        // Compute CRC over serialized header with crc32c field zeroed
        uint8_t tmp[SIZE];
        hdr.serialize(tmp);
        hdr.crc32c = cpu::CRC32C(reinterpret_cast<const std::byte*>(tmp), SIZE);

        return hdr;
    }

    void ManifestFileHeader::serialize(uint8_t out[SIZE]) const noexcept {
        writeU32(out, 0, magic);
        writeU16(out, 4, version);
        writeU16(out, 6, flags);
        writeU32(out, 8, fileSeq);
        writeU64(out, 12, createdAtUs);
        writeU32(out, 20, crc32c);
        std::memcpy(out + 24, reserved, 8);
    }

    bool ManifestFileHeader::verifyChecksum() const noexcept {
        uint8_t tmp[SIZE];
        // Serialize with crc32c = 0
        writeU32(tmp, 0, magic);
        writeU16(tmp, 4, version);
        writeU16(tmp, 6, flags);
        writeU32(tmp, 8, fileSeq);
        writeU64(tmp, 12, createdAtUs);
        writeU32(tmp, 20, 0); // zeroed crc32c
        std::memcpy(tmp + 24, reserved, 8);

        return crc32c == cpu::CRC32C(reinterpret_cast<const std::byte*>(tmp), SIZE);
    }

    // ============================================================================
    // ManifestRecordHeader
    // ============================================================================

    ManifestRecordHeader ManifestRecordHeader::build(ManifestRecordType rtype, const uint8_t* payload, uint16_t payloadLen) noexcept {
        ManifestRecordHeader hdr{};
        hdr.type = static_cast<uint8_t>(rtype);
        hdr.flags = 0;
        hdr.payloadLen = payloadLen;
        hdr.crc32c = cpu::CRC32C(reinterpret_cast<const std::byte*>(payload), payloadLen);
        return hdr;
    }

    void ManifestRecordHeader::serialize(uint8_t out[SIZE]) const noexcept {
        out[0] = type;
        out[1] = flags;
        writeU16(out, 2, payloadLen);
        writeU32(out, 4, crc32c);
    }

    ManifestRecordHeader ManifestRecordHeader::deserialize(const uint8_t in[SIZE]) noexcept {
        ManifestRecordHeader hdr{};
        hdr.type = in[0];
        hdr.flags = in[1];
        hdr.payloadLen = readU16(in, 2);
        hdr.crc32c = readU32(in, 4);
        return hdr;
    }

    // ============================================================================
    // Encode functions
    // ============================================================================

    std::vector<uint8_t> encodeStripeCommit(uint64_t tsUs, uint64_t stripeCount) {
        std::vector<uint8_t> p(16);
        writeU64(p.data(), 0, tsUs);
        writeU64(p.data(), 8, stripeCount);
        return p;
    }

    std::vector<uint8_t> encodeSstSeal(
        uint64_t tsUs,
        int level,
        const std::string& name,
        uint64_t entries,
        const std::optional<std::string>& firstKeyHex,
        const std::optional<std::string>& lastKeyHex
    ) {
        const uint16_t fkLen = firstKeyHex ? static_cast<uint16_t>(firstKeyHex->size()) : 0;
        const uint16_t lkLen = lastKeyHex ? static_cast<uint16_t>(lastKeyHex->size()) : 0;
        const uint16_t nameLen = static_cast<uint16_t>(name.size());
        const uint8_t keyFlags = (firstKeyHex ? 0x01 : 0x00) | (lastKeyHex ? 0x02 : 0x00);

        // Fixed header: 24 bytes
        const size_t total = 24 + nameLen + fkLen + lkLen;
        std::vector<uint8_t> p(total);
        uint8_t* b = p.data();

        writeU64(b, 0, tsUs);
        writeU64(b, 8, entries);
        b[16] = static_cast<uint8_t>(level);
        b[17] = keyFlags;
        writeU16(b, 18, nameLen);
        writeU16(b, 20, fkLen);
        writeU16(b, 22, lkLen);

        size_t off = 24;
        std::memcpy(b + off, name.data(), nameLen);
        off += nameLen;
        if (firstKeyHex) {
            std::memcpy(b + off, firstKeyHex->data(), fkLen);
            off += fkLen;
        }
        if (lastKeyHex) { std::memcpy(b + off, lastKeyHex->data(), lkLen); }

        return p;
    }

    std::vector<uint8_t> encodeSstDelete(uint64_t tsUs, const std::string& name) {
        const uint16_t nameLen = static_cast<uint16_t>(name.size());
        std::vector<uint8_t> p(10 + nameLen);
        writeU64(p.data(), 0, tsUs);
        writeU16(p.data(), 8, nameLen);
        std::memcpy(p.data() + 10, name.data(), nameLen);
        return p;
    }

    std::vector<uint8_t> encodeCompactionStart(uint64_t tsUs, int level, const std::vector<std::string>& inputs) {
        const auto inputCount = static_cast<uint8_t>(inputs.size());

        // Fixed: 12 bytes + [u16 len + bytes] per input
        size_t varSize = 0;
        for (const auto& s : inputs) { varSize += 2 + s.size(); }

        std::vector<uint8_t> p(12 + varSize);
        uint8_t* b = p.data();
        writeU64(b, 0, tsUs);
        b[8] = static_cast<uint8_t>(level);
        b[9] = inputCount;
        writeU16(b, 10, 0); // reserved

        size_t off = 12;
        for (const auto& s : inputs) {
            writeU16(b, off, static_cast<uint16_t>(s.size()));
            off += 2;
            std::memcpy(b + off, s.data(), s.size());
            off += s.size();
        }
        return p;
    }

    std::vector<uint8_t> encodeCompactionEnd(
        uint64_t tsUs,
        int level,
        const std::string& output,
        const std::vector<std::string>& inputs,
        uint64_t entries,
        const std::optional<std::string>& firstKeyHex,
        const std::optional<std::string>& lastKeyHex
    ) {
        const uint16_t outLen = static_cast<uint16_t>(output.size());
        const uint16_t fkLen = firstKeyHex ? static_cast<uint16_t>(firstKeyHex->size()) : 0;
        const uint16_t lkLen = lastKeyHex ? static_cast<uint16_t>(lastKeyHex->size()) : 0;
        const uint8_t inputCount = static_cast<uint8_t>(inputs.size());
        const uint8_t keyFlags = (firstKeyHex ? 0x01 : 0x00) | (lastKeyHex ? 0x02 : 0x00);

        // Fixed: 28 bytes
        size_t varSize = outLen + fkLen + lkLen;
        for (const auto& s : inputs) { varSize += 2 + s.size(); }

        std::vector<uint8_t> p(28 + varSize);
        uint8_t* b = p.data();

        writeU64(b, 0, tsUs);
        writeU64(b, 8, entries);
        b[16] = static_cast<uint8_t>(level);
        b[17] = keyFlags;
        b[18] = inputCount;
        b[19] = 0; // reserved
        writeU16(b, 20, outLen);
        writeU16(b, 22, fkLen);
        writeU16(b, 24, lkLen);
        writeU16(b, 26, 0); // reserved

        size_t off = 28;
        std::memcpy(b + off, output.data(), outLen);
        off += outLen;
        if (firstKeyHex) {
            std::memcpy(b + off, firstKeyHex->data(), fkLen);
            off += fkLen;
        }
        if (lastKeyHex) {
            std::memcpy(b + off, lastKeyHex->data(), lkLen);
            off += lkLen;
        }
        for (const auto& s : inputs) {
            writeU16(b, off, static_cast<uint16_t>(s.size()));
            off += 2;
            std::memcpy(b + off, s.data(), s.size());
            off += s.size();
        }
        return p;
    }

    std::vector<uint8_t> encodeCheckpoint(
        uint64_t tsUs,
        const std::optional<std::string>& name,
        const std::optional<uint64_t>& stripe,
        const std::optional<uint64_t>& lastSeq
    ) {
        const uint16_t nameLen = name ? static_cast<uint16_t>(name->size()) : 0;

        // Fixed: 26 bytes
        std::vector<uint8_t> p(26 + nameLen);
        uint8_t* b = p.data();

        writeU64(b, 0, tsUs);
        writeU64(b, 8, stripe.value_or(MANIFEST_ABSENT_U64));
        writeU64(b, 16, lastSeq.value_or(MANIFEST_ABSENT_U64));
        writeU16(b, 24, nameLen);

        if (name) { std::memcpy(b + 26, name->data(), nameLen); }
        return p;
    }

    std::vector<uint8_t> encodeTruncate(uint64_t tsUs, const std::optional<std::string>& reason) {
        const uint16_t reasonLen = reason ? static_cast<uint16_t>(reason->size()) : 0;
        std::vector<uint8_t> p(10 + reasonLen);
        writeU64(p.data(), 0, tsUs);
        writeU16(p.data(), 8, reasonLen);
        if (reason) { std::memcpy(p.data() + 10, reason->data(), reasonLen); }
        return p;
    }

    // ============================================================================
    // Decode functions
    // ============================================================================

    bool decodeStripeCommit(const uint8_t* payload, uint16_t len, DecodedStripeCommit& out) {
        if (len < 16) { return false; }
        out.tsUs = readU64(payload, 0);
        out.stripeCount = readU64(payload, 8);
        return true;
    }

    bool decodeSstSeal(const uint8_t* payload, uint16_t len, DecodedSSTSeal& out) {
        if (len < 24) { return false; }

        out.tsUs = readU64(payload, 0);
        out.entries = readU64(payload, 8);
        out.level = static_cast<int>(payload[16]);
        const uint8_t keyFlags = payload[17];
        const uint16_t nameLen = readU16(payload, 18);
        const uint16_t fkLen = readU16(payload, 20);
        const uint16_t lkLen = readU16(payload, 22);

        size_t required = 24u + nameLen + fkLen + lkLen;
        if (len < required) { return false; }

        size_t off = 24;
        out.name.assign(reinterpret_cast<const char*>(payload + off), nameLen);
        off += nameLen;

        if (keyFlags & 0x01) { out.firstKeyHex.emplace(reinterpret_cast<const char*>(payload + off), fkLen); }
        off += fkLen; // advance unconditionally  Efk bytes always occupy fkLen space
        if (keyFlags & 0x02) { out.lastKeyHex.emplace(reinterpret_cast<const char*>(payload + off), lkLen); }
        return true;
    }

    bool decodeSstDelete(const uint8_t* payload, uint16_t len, DecodedSSTDelete& out) {
        if (len < 10) { return false; }
        out.tsUs = readU64(payload, 0);
        const uint16_t nameLen = readU16(payload, 8);
        if (len < 10u + nameLen) { return false; }
        out.name.assign(reinterpret_cast<const char*>(payload + 10), nameLen);
        return true;
    }

    bool decodeCompactionStart(const uint8_t* payload, uint16_t len, DecodedCompactionStart& out) {
        if (len < 12) { return false; }
        out.tsUs = readU64(payload, 0);
        out.level = static_cast<int>(payload[8]);
        const uint8_t inputCount = payload[9];

        size_t cursor = 12;
        out.inputs.clear();
        out.inputs.reserve(inputCount);
        for (uint8_t i = 0; i < inputCount; ++i) {
            std::string s;
            if (!readLengthPrefixed(payload, len, cursor, s)) { return false; }
            out.inputs.push_back(std::move(s));
        }
        return true;
    }

    bool decodeCompactionEnd(const uint8_t* payload, uint16_t len, DecodedCompactionEnd& out) {
        if (len < 28) { return false; }

        out.tsUs = readU64(payload, 0);
        out.entries = readU64(payload, 8);
        out.level = static_cast<int>(payload[16]);
        const uint8_t keyFlags = payload[17];
        const uint8_t inputCount = payload[18];
        const uint16_t outLen = readU16(payload, 20);
        const uint16_t fkLen = readU16(payload, 22);
        const uint16_t lkLen = readU16(payload, 24);

        size_t required = 28u + outLen + fkLen + lkLen;
        if (len < required) { return false; }

        size_t off = 28;
        out.output.assign(reinterpret_cast<const char*>(payload + off), outLen);
        off += outLen;

        if (keyFlags & 0x01) { out.firstKeyHex.emplace(reinterpret_cast<const char*>(payload + off), fkLen); }
        off += fkLen; // advance unconditionally  Efk bytes always occupy fkLen space
        if (keyFlags & 0x02) { out.lastKeyHex.emplace(reinterpret_cast<const char*>(payload + off), lkLen); }
        off += lkLen; // advance unconditionally  Einputs follow immediately

        out.inputs.clear();
        out.inputs.reserve(inputCount);
        for (uint8_t i = 0; i < inputCount; ++i) {
            std::string s;
            if (!readLengthPrefixed(payload, len, off, s)) { return false; }
            out.inputs.push_back(std::move(s));
        }
        return true;
    }

    bool decodeCheckpoint(const uint8_t* payload, uint16_t len, DecodedCheckpoint& out) {
        if (len < 26) { return false; }

        out.tsUs = readU64(payload, 0);

        const uint64_t stripe = readU64(payload, 8);
        const uint64_t lastSeq = readU64(payload, 16);
        const uint16_t nameLen = readU16(payload, 24);

        out.stripe = (stripe != MANIFEST_ABSENT_U64) ? std::optional(stripe) : std::nullopt;
        out.lastSeq = (lastSeq != MANIFEST_ABSENT_U64) ? std::optional(lastSeq) : std::nullopt;

        if (nameLen > 0) {
            if (len < 26u + nameLen) { return false; }
            out.name.emplace(reinterpret_cast<const char*>(payload + 26), nameLen);
        }
        return true;
    }

    bool decodeTruncate(const uint8_t* payload, uint16_t len, DecodedTruncate& out) {
        if (len < 10) { return false; }
        out.tsUs = readU64(payload, 0);
        const uint16_t reasonLen = readU16(payload, 8);
        if (reasonLen > 0) {
            if (len < 10u + reasonLen) { return false; }
            out.reason.emplace(reinterpret_cast<const char*>(payload + 10), reasonLen);
        }
        return true;
    }

    std::vector<uint8_t> encodeCompactionCommit(
        uint64_t tsUs,
        const std::vector<std::string>& outputFiles,
        const std::vector<std::string>& inputFiles
    ) {
        const auto outputCount = static_cast<uint8_t>(outputFiles.size());
        const auto inputCount = static_cast<uint8_t>(inputFiles.size());

        // Fixed: 12 bytes + [u16 len + bytes] per output + [u16 len + bytes] per input
        size_t varSize = 0;
        for (const auto& s : outputFiles) { varSize += 2 + s.size(); }
        for (const auto& s : inputFiles) { varSize += 2 + s.size(); }

        std::vector<uint8_t> p(12 + varSize);
        uint8_t* b = p.data();
        writeU64(b, 0, tsUs);
        b[8] = outputCount;
        b[9] = inputCount;
        writeU16(b, 10, 0); // reserved

        size_t off = 12;
        for (const auto& s : outputFiles) {
            writeU16(b, off, static_cast<uint16_t>(s.size()));
            off += 2;
            std::memcpy(b + off, s.data(), s.size());
            off += s.size();
        }
        for (const auto& s : inputFiles) {
            writeU16(b, off, static_cast<uint16_t>(s.size()));
            off += 2;
            std::memcpy(b + off, s.data(), s.size());
            off += s.size();
        }
        return p;
    }

    bool decodeCompactionCommit(const uint8_t* payload, uint16_t len, DecodedCompactionCommit& out) {
        if (len < 12) { return false; }
        out.tsUs = readU64(payload, 0);
        const uint8_t outputCount = payload[8];
        const uint8_t inputCount = payload[9];

        size_t cursor = 12;
        out.outputFiles.clear();
        out.outputFiles.reserve(outputCount);
        for (uint8_t i = 0; i < outputCount; ++i) {
            std::string s;
            if (!readLengthPrefixed(payload, len, cursor, s)) { return false; }
            out.outputFiles.push_back(std::move(s));
        }
        out.inputFiles.clear();
        out.inputFiles.reserve(inputCount);
        for (uint8_t i = 0; i < inputCount; ++i) {
            std::string s;
            if (!readLengthPrefixed(payload, len, cursor, s)) { return false; }
            out.inputFiles.push_back(std::move(s));
        }
        return true;
    }

    // ============================================================================
    // Cluster event encode / decode (v4)
    // ============================================================================

    std::vector<uint8_t> encodeNodeJoin(uint64_t tsUs, uint64_t nodeId, uint16_t replPort, const std::string& host) {
        const uint16_t hostLen = static_cast<uint16_t>(host.size());
        std::vector<uint8_t> p(20 + hostLen);
        uint8_t* b = p.data();
        writeU64(b, 0, tsUs);
        writeU64(b, 8, nodeId);
        writeU16(b, 16, replPort);
        writeU16(b, 18, hostLen);
        std::memcpy(b + 20, host.data(), hostLen);
        return p;
    }

    std::vector<uint8_t> encodeNodeLeave(uint64_t tsUs, uint64_t nodeId) {
        std::vector<uint8_t> p(16);
        writeU64(p.data(), 0, tsUs);
        writeU64(p.data(), 8, nodeId);
        return p;
    }

    std::vector<uint8_t> encodePrimaryLease(uint64_t tsUs, uint64_t nodeId, uint64_t leaseUntilUs) {
        std::vector<uint8_t> p(24);
        writeU64(p.data(), 0, tsUs);
        writeU64(p.data(), 8, nodeId);
        writeU64(p.data(), 16, leaseUntilUs);
        return p;
    }

    bool decodeNodeJoin(const uint8_t* payload, uint16_t len, DecodedNodeJoin& out) {
        if (len < 20) { return false; }
        out.tsUs = readU64(payload, 0);
        out.nodeId = readU64(payload, 8);
        out.replPort = readU16(payload, 16);
        const uint16_t hostLen = readU16(payload, 18);
        if (len < 20u + hostLen) { return false; }
        out.host.assign(reinterpret_cast<const char*>(payload + 20), hostLen);
        return true;
    }

    bool decodeNodeLeave(const uint8_t* payload, uint16_t len, DecodedNodeLeave& out) {
        if (len < 16) { return false; }
        out.tsUs = readU64(payload, 0);
        out.nodeId = readU64(payload, 8);
        return true;
    }

    bool decodePrimaryLease(const uint8_t* payload, uint16_t len, DecodedPrimaryLease& out) {
        if (len < 24) { return false; }
        out.tsUs = readU64(payload, 0);
        out.nodeId = readU64(payload, 8);
        out.leaseUntilUs = readU64(payload, 16);
        return true;
    }
} // namespace akkaradb::engine::manifest

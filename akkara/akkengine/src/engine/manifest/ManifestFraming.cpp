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
#include <limits>
#include <stdexcept>
#include <utility>

namespace akkaradb::engine::manifest {
    namespace {
        void put(uint8_t* out, uint64_t value, size_t width) noexcept {
            for (size_t i = 0; i < width; ++i) {
                out[i] = static_cast<uint8_t>(value >> (i * 8));
            }
        }
        uint64_t get(const uint8_t* in, size_t width) noexcept {
            uint64_t value = 0;
            for (size_t i = 0; i < width; ++i) {
                value |= static_cast<uint64_t>(in[i]) << (i * 8);
            }
            return value;
        }
        uint32_t checkedSize(size_t size) {
            if (size > UINT32_MAX) {
                throw std::length_error("Manifest: payload exceeds the v1 u32 limit");
            }
            return static_cast<uint32_t>(size);
        }
        struct Writer {
            std::vector<uint8_t> bytes;
            void integer(uint64_t value, size_t width) {
                if (width > UINT32_MAX - bytes.size()) {
                    throw std::length_error("Manifest: payload too large");
                }
                const auto offset = bytes.size();
                bytes.resize(offset + width);
                put(bytes.data() + offset, value, width);
            }
            void u8(uint8_t v) { integer(v, 1); }
            void u16(uint16_t v) { integer(v, 2); }
            void u32(uint32_t v) { integer(v, 4); }
            void u64(uint64_t v) { integer(v, 8); }
            void string(const std::string& value) {
                const auto n = checkedSize(value.size());
                u32(n);
                if (n > UINT32_MAX - bytes.size()) {
                    throw std::length_error("Manifest: payload too large");
                }
                bytes.insert(bytes.end(), value.begin(), value.end());
            }
            void optionalString(const std::optional<std::string>& value) {
                u8(value.has_value());
                if (value) {
                    string(*value);
                }
            }
            void strings(const std::vector<std::string>& values) {
                u32(checkedSize(values.size()));
                for (const auto& value : values) {
                    string(value);
                }
            }
            void level(int value) {
                if (value < 0) {
                    throw std::invalid_argument("Manifest: negative SST level");
                }
                u32(static_cast<uint32_t>(value));
            }
        };
        struct Reader {
            const uint8_t* data;
            size_t size;
            size_t offset = 0;
            bool valid = true;
            uint64_t integer(size_t width) {
                if (!valid || width > size - offset) {
                    valid = false;
                    return 0;
                }
                auto v = get(data + offset, width);
                offset += width;
                return v;
            }
            uint8_t u8() { return static_cast<uint8_t>(integer(1)); }
            uint16_t u16() { return static_cast<uint16_t>(integer(2)); }
            uint32_t u32() { return static_cast<uint32_t>(integer(4)); }
            uint64_t u64() { return integer(8); }
            std::string string() {
                const auto n = u32();
                if (!valid || n > size - offset) {
                    valid = false;
                    return {};
                }
                std::string v(reinterpret_cast<const char*>(data + offset), n);
                offset += n;
                return v;
            }
            std::optional<std::string> optionalString() {
                const auto present = u8();
                if (present > 1) {
                    valid = false;
                }
                if (present == 1) {
                    return string();
                }
                return std::nullopt;
            }
            std::vector<std::string> strings() {
                const auto count = u32();
                std::vector<std::string> values;
                if (!valid || count > (size - offset) / 4) {
                    valid = false;
                    return values;
                }
                values.reserve(count);
                for (uint32_t i = 0; i < count && valid; ++i) {
                    values.push_back(string());
                }
                return values;
            }
            int level() {
                const auto v = u32();
                if (v > static_cast<uint32_t>(std::numeric_limits<int>::max())) {
                    valid = false;
                    return 0;
                }
                return static_cast<int>(v);
            }
            bool done() const { return valid && offset == size; }
        };
        std::optional<uint64_t> optionalU64(uint64_t v) {
            return v == MANIFEST_ABSENT_U64 ? std::nullopt : std::optional<uint64_t>{v};
        }
    } // namespace

    ManifestFileHeader ManifestFileHeader::build(uint32_t fileSeq) noexcept {
        ManifestFileHeader hdr{};
        hdr.magic = MAGIC;
        hdr.version = VERSION;
        hdr.fileSeq = fileSeq;
        hdr.createdAtUs = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
        uint8_t bytes[SIZE];
        hdr.serialize(bytes);
        hdr.crc32c = cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes), SIZE);
        return hdr;
    }
    void ManifestFileHeader::serialize(uint8_t out[SIZE]) const noexcept {
        put(out, magic, 4);
        put(out + 4, version, 2);
        put(out + 6, flags, 2);
        put(out + 8, fileSeq, 4);
        put(out + 12, createdAtUs, 8);
        put(out + 20, crc32c, 4);
        std::memcpy(out + 24, reserved, 8);
    }
    bool ManifestFileHeader::verifyChecksum() const noexcept {
        auto copy = *this;
        copy.crc32c = 0;
        uint8_t bytes[SIZE];
        copy.serialize(bytes);
        return crc32c == cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes), SIZE);
    }
    ManifestRecordHeader ManifestRecordHeader::build(ManifestRecordType type, const uint8_t* payload, uint32_t payloadLen) noexcept {
        ManifestRecordHeader hdr{};
        hdr.type = static_cast<uint8_t>(type);
        hdr.payloadLen = payloadLen;
        hdr.crc32c = cpu::CRC32C(reinterpret_cast<const std::byte*>(payload), payloadLen);
        uint8_t bytes[SIZE];
        hdr.serialize(bytes);
        hdr.headerCrc32c = cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes), SIZE);
        return hdr;
    }
    void ManifestRecordHeader::serialize(uint8_t out[SIZE]) const noexcept {
        out[0] = type;
        out[1] = flags;
        put(out + 2, reserved, 2);
        put(out + 4, payloadLen, 4);
        put(out + 8, crc32c, 4);
        put(out + 12, headerCrc32c, 4);
    }
    ManifestRecordHeader ManifestRecordHeader::deserialize(const uint8_t in[SIZE]) noexcept {
        return {in[0],
                in[1],
                static_cast<uint16_t>(get(in + 2, 2)),
                static_cast<uint32_t>(get(in + 4, 4)),
                static_cast<uint32_t>(get(in + 8, 4)),
                static_cast<uint32_t>(get(in + 12, 4))};
    }
    bool ManifestRecordHeader::verifyChecksum() const noexcept {
        auto copy = *this;
        copy.headerCrc32c = 0;
        uint8_t bytes[SIZE];
        copy.serialize(bytes);
        return flags == 0 && reserved == 0 && headerCrc32c == cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes), SIZE);
    }

    std::vector<uint8_t> encodeStripeCommit(uint64_t ts, uint64_t count) {
        Writer w;
        w.u64(ts);
        w.u64(count);
        return std::move(w.bytes);
    }
    std::vector<uint8_t> encodeSstSeal(uint64_t ts, int level, const std::string& name, uint64_t entries,
                                       const std::optional<std::string>& first, const std::optional<std::string>& last) {
        Writer w;
        w.u64(ts);
        w.u64(entries);
        w.level(level);
        w.string(name);
        w.optionalString(first);
        w.optionalString(last);
        return std::move(w.bytes);
    }
    std::vector<uint8_t> encodeSstDelete(uint64_t ts, const std::string& name) {
        Writer w;
        w.u64(ts);
        w.string(name);
        return std::move(w.bytes);
    }
    std::vector<uint8_t> encodeCompactionStart(uint64_t ts, int level, const std::vector<std::string>& inputs) {
        Writer w;
        w.u64(ts);
        w.level(level);
        w.strings(inputs);
        return std::move(w.bytes);
    }
    std::vector<uint8_t> encodeCompactionEnd(uint64_t ts, int level, const std::string& output, const std::vector<std::string>& inputs,
                                             uint64_t entries, const std::optional<std::string>& first,
                                             const std::optional<std::string>& last) {
        Writer w;
        w.u64(ts);
        w.u64(entries);
        w.level(level);
        w.string(output);
        w.optionalString(first);
        w.optionalString(last);
        w.strings(inputs);
        return std::move(w.bytes);
    }
    std::vector<uint8_t> encodeCheckpoint(uint64_t ts, const std::optional<std::string>& name, const std::optional<uint64_t>& stripe,
                                          const std::optional<uint64_t>& seq) {
        Writer w;
        w.u64(ts);
        w.u64(stripe.value_or(MANIFEST_ABSENT_U64));
        w.u64(seq.value_or(MANIFEST_ABSENT_U64));
        w.optionalString(name);
        return std::move(w.bytes);
    }
    std::vector<uint8_t> encodeTruncate(uint64_t ts, const std::optional<std::string>& reason) {
        Writer w;
        w.u64(ts);
        w.optionalString(reason);
        return std::move(w.bytes);
    }
    std::vector<uint8_t> encodeCompactionCommit(uint64_t ts, const std::vector<std::string>& outputs,
                                                const std::vector<std::string>& inputs) {
        Writer w;
        w.u64(ts);
        w.strings(outputs);
        w.strings(inputs);
        return std::move(w.bytes);
    }
    std::vector<uint8_t> encodeSstBlobRefs(uint64_t ts, const std::string& name, const std::vector<SSTBlobRefEntry>& entries) {
        Writer w;
        w.u64(ts);
        w.string(name);
        w.u32(checkedSize(entries.size()));
        for (const auto& e : entries) {
            w.u64(e.seq);
            w.u64(e.blobId.value_or(MANIFEST_ABSENT_U64));
            w.u8(e.flags);
            w.string(std::string(e.key.begin(), e.key.end()));
        }
        return std::move(w.bytes);
    }
    std::vector<uint8_t> encodeBlobPut(uint64_t ts, uint64_t id, uint64_t total, uint64_t stored, uint32_t crc, uint32_t codec) {
        Writer w;
        w.u64(ts);
        w.u64(id);
        w.u64(total);
        w.u64(stored);
        w.u32(crc);
        w.u32(codec);
        return std::move(w.bytes);
    }
    std::vector<uint8_t> encodeBlobDelete(uint64_t ts, uint64_t id) {
        Writer w;
        w.u64(ts);
        w.u64(id);
        return std::move(w.bytes);
    }
    std::vector<uint8_t> encodeNodeJoin(uint64_t ts, uint64_t id, uint16_t port, const std::string& host) {
        Writer w;
        w.u64(ts);
        w.u64(id);
        w.u16(port);
        w.string(host);
        return std::move(w.bytes);
    }
    std::vector<uint8_t> encodeNodeLeave(uint64_t ts, uint64_t id) {
        Writer w;
        w.u64(ts);
        w.u64(id);
        return std::move(w.bytes);
    }
    std::vector<uint8_t> encodePrimaryLease(uint64_t ts, uint64_t id, uint64_t until) {
        Writer w;
        w.u64(ts);
        w.u64(id);
        w.u64(until);
        return std::move(w.bytes);
    }

    bool decodeStripeCommit(const uint8_t* p, uint32_t len, DecodedStripeCommit& out) {
        Reader r{p, len};
        out.tsUs = r.u64();
        out.stripeCount = r.u64();
        return r.done();
    }
    bool decodeSstSeal(const uint8_t* p, uint32_t len, DecodedSSTSeal& out) {
        Reader r{p, len};
        out.tsUs = r.u64();
        out.entries = r.u64();
        out.level = r.level();
        out.name = r.string();
        out.firstKeyHex = r.optionalString();
        out.lastKeyHex = r.optionalString();
        return r.done();
    }
    bool decodeSstDelete(const uint8_t* p, uint32_t len, DecodedSSTDelete& out) {
        Reader r{p, len};
        out.tsUs = r.u64();
        out.name = r.string();
        return r.done();
    }
    bool decodeCompactionStart(const uint8_t* p, uint32_t len, DecodedCompactionStart& out) {
        Reader r{p, len};
        out.tsUs = r.u64();
        out.level = r.level();
        out.inputs = r.strings();
        return r.done();
    }
    bool decodeCompactionEnd(const uint8_t* p, uint32_t len, DecodedCompactionEnd& out) {
        Reader r{p, len};
        out.tsUs = r.u64();
        out.entries = r.u64();
        out.level = r.level();
        out.output = r.string();
        out.firstKeyHex = r.optionalString();
        out.lastKeyHex = r.optionalString();
        out.inputs = r.strings();
        return r.done();
    }
    bool decodeCheckpoint(const uint8_t* p, uint32_t len, DecodedCheckpoint& out) {
        Reader r{p, len};
        out.tsUs = r.u64();
        out.stripe = optionalU64(r.u64());
        out.lastSeq = optionalU64(r.u64());
        out.name = r.optionalString();
        return r.done();
    }
    bool decodeTruncate(const uint8_t* p, uint32_t len, DecodedTruncate& out) {
        Reader r{p, len};
        out.tsUs = r.u64();
        out.reason = r.optionalString();
        return r.done();
    }
    bool decodeCompactionCommit(const uint8_t* p, uint32_t len, DecodedCompactionCommit& out) {
        Reader r{p, len};
        out.tsUs = r.u64();
        out.outputFiles = r.strings();
        out.inputFiles = r.strings();
        return r.done();
    }
    bool decodeSstBlobRefs(const uint8_t* p, uint32_t len, DecodedSSTBlobRefs& out) {
        Reader r{p, len};
        out.tsUs = r.u64();
        out.name = r.string();
        const auto count = r.u32();
        out.entries.clear();
        if (!r.valid || count > (r.size - r.offset) / 21) {
            return false;
        }
        out.entries.reserve(count);
        for (uint32_t i = 0; i < count && r.valid; ++i) {
            SSTBlobRefEntry e{};
            e.seq = r.u64();
            e.blobId = optionalU64(r.u64());
            e.flags = r.u8();
            const auto key = r.string();
            e.key.assign(key.begin(), key.end());
            out.entries.push_back(std::move(e));
        }
        return r.done();
    }
    bool decodeBlobPut(const uint8_t* p, uint32_t len, DecodedBlobPut& out) {
        Reader r{p, len};
        out.tsUs = r.u64();
        out.blobId = r.u64();
        out.totalSize = r.u64();
        out.storedSize = r.u64();
        out.contentCrc32c = r.u32();
        out.codec = r.u32();
        return r.done();
    }
    bool decodeBlobDelete(const uint8_t* p, uint32_t len, DecodedBlobDelete& out) {
        Reader r{p, len};
        out.tsUs = r.u64();
        out.blobId = r.u64();
        return r.done();
    }
    bool decodeNodeJoin(const uint8_t* p, uint32_t len, DecodedNodeJoin& out) {
        Reader r{p, len};
        out.tsUs = r.u64();
        out.nodeId = r.u64();
        out.replPort = r.u16();
        out.host = r.string();
        return r.done();
    }
    bool decodeNodeLeave(const uint8_t* p, uint32_t len, DecodedNodeLeave& out) {
        Reader r{p, len};
        out.tsUs = r.u64();
        out.nodeId = r.u64();
        return r.done();
    }
    bool decodePrimaryLease(const uint8_t* p, uint32_t len, DecodedPrimaryLease& out) {
        Reader r{p, len};
        out.tsUs = r.u64();
        out.nodeId = r.u64();
        out.leaseUntilUs = r.u64();
        return r.done();
    }
} // namespace akkaradb::engine::manifest

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

// akkengine/src/engine/manifest/ManifestFraming.cpp
#include "akk/engine/manifest/ManifestFraming.hpp"
#include <chrono>
#include <stdexcept>

namespace akkaradb::engine::manifest {
    // ============================================================================
    // Internal helpers
    // ============================================================================

    namespace {
        uint64_t now_us() noexcept {
            return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch()
            ).count());
        }

        // Write a little-endian u16 into a byte buffer at offset.
        inline void write_u16(uint8_t* buf, size_t off, uint16_t v) noexcept {
            buf[off] = static_cast<uint8_t>(v);
            buf[off + 1] = static_cast<uint8_t>(v >> 8);
        }

        // Write a little-endian u32 into a byte buffer at offset.
        inline void write_u32(uint8_t* buf, size_t off, uint32_t v) noexcept {
            buf[off] = static_cast<uint8_t>(v);
            buf[off + 1] = static_cast<uint8_t>(v >> 8);
            buf[off + 2] = static_cast<uint8_t>(v >> 16);
            buf[off + 3] = static_cast<uint8_t>(v >> 24);
        }

        // Write a little-endian u64 into a byte buffer at offset.
        inline void write_u64(uint8_t* buf, size_t off, uint64_t v) noexcept {
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
        inline uint16_t read_u16(const uint8_t* buf, size_t off) noexcept {
            return static_cast<uint16_t>(buf[off]) | (static_cast<uint16_t>(buf[off + 1]) << 8);
        }

        // Read a little-endian u32 from a byte buffer at offset.
        inline uint32_t read_u32(const uint8_t* buf, size_t off) noexcept {
            return static_cast<uint32_t>(buf[off]) | (static_cast<uint32_t>(buf[off + 1]) << 8) | (static_cast<uint32_t>(buf[off + 2]) <<
                16) | (static_cast<uint32_t>(buf[off + 3]) << 24);
        }

        // Read a little-endian u64 from a byte buffer at offset.
        inline uint64_t read_u64(const uint8_t* buf, size_t off) noexcept {
            return static_cast<uint64_t>(buf[off]) | (static_cast<uint64_t>(buf[off + 1]) << 8) | (static_cast<uint64_t>(buf[off + 2]) <<
                16) | (static_cast<uint64_t>(buf[off + 3]) << 24) | (static_cast<uint64_t>(buf[off + 4]) << 32) | (static_cast<uint64_t>(buf
                [off + 5]) << 40) | (static_cast<uint64_t>(buf[off + 6]) << 48) | (static_cast<uint64_t>(buf[off + 7]) << 56);
        }

        // Append a string with a u16 length prefix.
        void append_length_prefixed(std::vector<uint8_t>& buf, const std::string& s) {
            const auto len = static_cast<uint16_t>(s.size());
            const size_t off = buf.size();
            buf.resize(off + 2 + len);
            write_u16(buf.data(), off, len);
            std::memcpy(buf.data() + off + 2, s.data(), len);
        }

        // Read a length-prefixed string from payload at cursor; advances cursor.
        bool read_length_prefixed(const uint8_t* payload, uint16_t payload_len, size_t& cursor, std::string& out) {
            if (cursor + 2 > payload_len) { return false; }
            const uint16_t len = read_u16(payload, cursor);
            cursor += 2;
            if (cursor + len > payload_len) { return false; }
            out.assign(reinterpret_cast<const char*>(payload + cursor), len);
            cursor += len;
            return true;
        }
    } // anonymous namespace

    // ============================================================================
    // ManifestFileHeader
    // ============================================================================

    ManifestFileHeader ManifestFileHeader::build(uint32_t file_seq) noexcept {
        ManifestFileHeader hdr{};
        hdr.magic = MAGIC;
        hdr.version = VERSION;
        hdr.flags = 0;
        hdr.file_seq = file_seq;
        hdr.created_at_us = now_us();
        hdr.crc32c = 0;
        std::memset(hdr.reserved, 0, sizeof(hdr.reserved));

        // Compute CRC over serialized header with crc32c field zeroed
        uint8_t tmp[SIZE];
        hdr.serialize(tmp);
        hdr.crc32c = cpu::CRC32C(reinterpret_cast<const std::byte*>(tmp), SIZE);

        return hdr;
    }

    void ManifestFileHeader::serialize(uint8_t out[SIZE]) const noexcept {
        write_u32(out, 0, magic);
        write_u16(out, 4, version);
        write_u16(out, 6, flags);
        write_u32(out, 8, file_seq);
        write_u64(out, 12, created_at_us);
        write_u32(out, 20, crc32c);
        std::memcpy(out + 24, reserved, 8);
    }

    bool ManifestFileHeader::verify_checksum() const noexcept {
        uint8_t tmp[SIZE];
        // Serialize with crc32c = 0
        write_u32(tmp, 0, magic);
        write_u16(tmp, 4, version);
        write_u16(tmp, 6, flags);
        write_u32(tmp, 8, file_seq);
        write_u64(tmp, 12, created_at_us);
        write_u32(tmp, 20, 0); // zeroed crc32c
        std::memcpy(tmp + 24, reserved, 8);

        return crc32c == cpu::CRC32C(reinterpret_cast<const std::byte*>(tmp), SIZE);
    }

    // ============================================================================
    // ManifestRecordHeader
    // ============================================================================

    ManifestRecordHeader ManifestRecordHeader::build(ManifestRecordType rtype, const uint8_t* payload, uint16_t payload_len) noexcept {
        ManifestRecordHeader hdr{};
        hdr.type = static_cast<uint8_t>(rtype);
        hdr.flags = 0;
        hdr.payload_len = payload_len;
        hdr.crc32c = cpu::CRC32C(reinterpret_cast<const std::byte*>(payload), payload_len);
        return hdr;
    }

    void ManifestRecordHeader::serialize(uint8_t out[SIZE]) const noexcept {
        out[0] = type;
        out[1] = flags;
        write_u16(out, 2, payload_len);
        write_u32(out, 4, crc32c);
    }

    ManifestRecordHeader ManifestRecordHeader::deserialize(const uint8_t in[SIZE]) noexcept {
        ManifestRecordHeader hdr{};
        hdr.type = in[0];
        hdr.flags = in[1];
        hdr.payload_len = read_u16(in, 2);
        hdr.crc32c = read_u32(in, 4);
        return hdr;
    }

    // ============================================================================
    // Encode functions
    // ============================================================================

    std::vector<uint8_t> encode_stripe_commit(uint64_t ts_us, uint64_t stripe_count) {
        std::vector<uint8_t> p(16);
        write_u64(p.data(), 0, ts_us);
        write_u64(p.data(), 8, stripe_count);
        return p;
    }

    std::vector<uint8_t> encode_sst_seal(
        uint64_t ts_us,
        int level,
        const std::string& name,
        uint64_t entries,
        const std::optional<std::string>& first_key_hex,
        const std::optional<std::string>& last_key_hex
    ) {
        const uint16_t fk_len = first_key_hex ? static_cast<uint16_t>(first_key_hex->size()) : 0;
        const uint16_t lk_len = last_key_hex ? static_cast<uint16_t>(last_key_hex->size()) : 0;
        const uint16_t name_len = static_cast<uint16_t>(name.size());
        const uint8_t key_flags = (first_key_hex ? 0x01 : 0x00) | (last_key_hex ? 0x02 : 0x00);

        // Fixed header: 24 bytes
        const size_t total = 24 + name_len + fk_len + lk_len;
        std::vector<uint8_t> p(total);
        uint8_t* b = p.data();

        write_u64(b, 0, ts_us);
        write_u64(b, 8, entries);
        b[16] = static_cast<uint8_t>(level);
        b[17] = key_flags;
        write_u16(b, 18, name_len);
        write_u16(b, 20, fk_len);
        write_u16(b, 22, lk_len);

        size_t off = 24;
        std::memcpy(b + off, name.data(), name_len);
        off += name_len;
        if (first_key_hex) {
            std::memcpy(b + off, first_key_hex->data(), fk_len);
            off += fk_len;
        }
        if (last_key_hex) { std::memcpy(b + off, last_key_hex->data(), lk_len); }

        return p;
    }

    std::vector<uint8_t> encode_sst_delete(uint64_t ts_us, const std::string& name) {
        const uint16_t name_len = static_cast<uint16_t>(name.size());
        std::vector<uint8_t> p(10 + name_len);
        write_u64(p.data(), 0, ts_us);
        write_u16(p.data(), 8, name_len);
        std::memcpy(p.data() + 10, name.data(), name_len);
        return p;
    }

    std::vector<uint8_t> encode_compaction_start(uint64_t ts_us, int level, const std::vector<std::string>& inputs) {
        const auto input_count = static_cast<uint8_t>(inputs.size());

        // Fixed: 12 bytes + [u16 len + bytes] per input
        size_t var_size = 0;
        for (const auto& s : inputs) { var_size += 2 + s.size(); }

        std::vector<uint8_t> p(12 + var_size);
        uint8_t* b = p.data();
        write_u64(b, 0, ts_us);
        b[8] = static_cast<uint8_t>(level);
        b[9] = input_count;
        write_u16(b, 10, 0); // reserved

        size_t off = 12;
        for (const auto& s : inputs) {
            write_u16(b, off, static_cast<uint16_t>(s.size()));
            off += 2;
            std::memcpy(b + off, s.data(), s.size());
            off += s.size();
        }
        return p;
    }

    std::vector<uint8_t> encode_compaction_end(
        uint64_t ts_us,
        int level,
        const std::string& output,
        const std::vector<std::string>& inputs,
        uint64_t entries,
        const std::optional<std::string>& first_key_hex,
        const std::optional<std::string>& last_key_hex
    ) {
        const uint16_t out_len = static_cast<uint16_t>(output.size());
        const uint16_t fk_len = first_key_hex ? static_cast<uint16_t>(first_key_hex->size()) : 0;
        const uint16_t lk_len = last_key_hex ? static_cast<uint16_t>(last_key_hex->size()) : 0;
        const uint8_t input_count = static_cast<uint8_t>(inputs.size());
        const uint8_t key_flags = (first_key_hex ? 0x01 : 0x00) | (last_key_hex ? 0x02 : 0x00);

        // Fixed: 28 bytes
        size_t var_size = out_len + fk_len + lk_len;
        for (const auto& s : inputs) { var_size += 2 + s.size(); }

        std::vector<uint8_t> p(28 + var_size);
        uint8_t* b = p.data();

        write_u64(b, 0, ts_us);
        write_u64(b, 8, entries);
        b[16] = static_cast<uint8_t>(level);
        b[17] = key_flags;
        b[18] = input_count;
        b[19] = 0; // reserved
        write_u16(b, 20, out_len);
        write_u16(b, 22, fk_len);
        write_u16(b, 24, lk_len);
        write_u16(b, 26, 0); // reserved

        size_t off = 28;
        std::memcpy(b + off, output.data(), out_len);
        off += out_len;
        if (first_key_hex) {
            std::memcpy(b + off, first_key_hex->data(), fk_len);
            off += fk_len;
        }
        if (last_key_hex) {
            std::memcpy(b + off, last_key_hex->data(), lk_len);
            off += lk_len;
        }
        for (const auto& s : inputs) {
            write_u16(b, off, static_cast<uint16_t>(s.size()));
            off += 2;
            std::memcpy(b + off, s.data(), s.size());
            off += s.size();
        }
        return p;
    }

    std::vector<uint8_t> encode_checkpoint(
        uint64_t ts_us,
        const std::optional<std::string>& name,
        const std::optional<uint64_t>& stripe,
        const std::optional<uint64_t>& last_seq
    ) {
        const uint16_t name_len = name ? static_cast<uint16_t>(name->size()) : 0;

        // Fixed: 26 bytes
        std::vector<uint8_t> p(26 + name_len);
        uint8_t* b = p.data();

        write_u64(b, 0, ts_us);
        write_u64(b, 8, stripe.value_or(MANIFEST_ABSENT_U64));
        write_u64(b, 16, last_seq.value_or(MANIFEST_ABSENT_U64));
        write_u16(b, 24, name_len);

        if (name) { std::memcpy(b + 26, name->data(), name_len); }
        return p;
    }

    std::vector<uint8_t> encode_truncate(uint64_t ts_us, const std::optional<std::string>& reason) {
        const uint16_t reason_len = reason ? static_cast<uint16_t>(reason->size()) : 0;
        std::vector<uint8_t> p(10 + reason_len);
        write_u64(p.data(), 0, ts_us);
        write_u16(p.data(), 8, reason_len);
        if (reason) { std::memcpy(p.data() + 10, reason->data(), reason_len); }
        return p;
    }

    // ============================================================================
    // Decode functions
    // ============================================================================

    bool decode_stripe_commit(const uint8_t* payload, uint16_t len, DecodedStripeCommit& out) {
        if (len < 16) { return false; }
        out.ts_us = read_u64(payload, 0);
        out.stripe_count = read_u64(payload, 8);
        return true;
    }

    bool decode_sst_seal(const uint8_t* payload, uint16_t len, DecodedSSTSeal& out) {
        if (len < 24) { return false; }

        out.ts_us = read_u64(payload, 0);
        out.entries = read_u64(payload, 8);
        out.level = static_cast<int>(payload[16]);
        const uint8_t key_flags = payload[17];
        const uint16_t name_len = read_u16(payload, 18);
        const uint16_t fk_len = read_u16(payload, 20);
        const uint16_t lk_len = read_u16(payload, 22);

        size_t required = 24u + name_len + fk_len + lk_len;
        if (len < required) { return false; }

        size_t off = 24;
        out.name.assign(reinterpret_cast<const char*>(payload + off), name_len);
        off += name_len;

        if (key_flags & 0x01) { out.first_key_hex.emplace(reinterpret_cast<const char*>(payload + off), fk_len); }
        off += fk_len; // advance unconditionally  Efk bytes always occupy fk_len space
        if (key_flags & 0x02) { out.last_key_hex.emplace(reinterpret_cast<const char*>(payload + off), lk_len); }
        return true;
    }

    bool decode_sst_delete(const uint8_t* payload, uint16_t len, DecodedSSTDelete& out) {
        if (len < 10) { return false; }
        out.ts_us = read_u64(payload, 0);
        const uint16_t name_len = read_u16(payload, 8);
        if (len < 10u + name_len) { return false; }
        out.name.assign(reinterpret_cast<const char*>(payload + 10), name_len);
        return true;
    }

    bool decode_compaction_start(const uint8_t* payload, uint16_t len, DecodedCompactionStart& out) {
        if (len < 12) { return false; }
        out.ts_us = read_u64(payload, 0);
        out.level = static_cast<int>(payload[8]);
        const uint8_t input_count = payload[9];

        size_t cursor = 12;
        out.inputs.clear();
        out.inputs.reserve(input_count);
        for (uint8_t i = 0; i < input_count; ++i) {
            std::string s;
            if (!read_length_prefixed(payload, len, cursor, s)) { return false; }
            out.inputs.push_back(std::move(s));
        }
        return true;
    }

    bool decode_compaction_end(const uint8_t* payload, uint16_t len, DecodedCompactionEnd& out) {
        if (len < 28) { return false; }

        out.ts_us = read_u64(payload, 0);
        out.entries = read_u64(payload, 8);
        out.level = static_cast<int>(payload[16]);
        const uint8_t key_flags = payload[17];
        const uint8_t input_count = payload[18];
        const uint16_t out_len = read_u16(payload, 20);
        const uint16_t fk_len = read_u16(payload, 22);
        const uint16_t lk_len = read_u16(payload, 24);

        size_t required = 28u + out_len + fk_len + lk_len;
        if (len < required) { return false; }

        size_t off = 28;
        out.output.assign(reinterpret_cast<const char*>(payload + off), out_len);
        off += out_len;

        if (key_flags & 0x01) { out.first_key_hex.emplace(reinterpret_cast<const char*>(payload + off), fk_len); }
        off += fk_len; // advance unconditionally  Efk bytes always occupy fk_len space
        if (key_flags & 0x02) { out.last_key_hex.emplace(reinterpret_cast<const char*>(payload + off), lk_len); }
        off += lk_len; // advance unconditionally  Einputs follow immediately

        out.inputs.clear();
        out.inputs.reserve(input_count);
        for (uint8_t i = 0; i < input_count; ++i) {
            std::string s;
            if (!read_length_prefixed(payload, len, off, s)) { return false; }
            out.inputs.push_back(std::move(s));
        }
        return true;
    }

    bool decode_checkpoint(const uint8_t* payload, uint16_t len, DecodedCheckpoint& out) {
        if (len < 26) { return false; }

        out.ts_us = read_u64(payload, 0);

        const uint64_t stripe = read_u64(payload, 8);
        const uint64_t last_seq = read_u64(payload, 16);
        const uint16_t name_len = read_u16(payload, 24);

        out.stripe = (stripe != MANIFEST_ABSENT_U64) ? std::optional(stripe) : std::nullopt;
        out.last_seq = (last_seq != MANIFEST_ABSENT_U64) ? std::optional(last_seq) : std::nullopt;

        if (name_len > 0) {
            if (len < 26u + name_len) { return false; }
            out.name.emplace(reinterpret_cast<const char*>(payload + 26), name_len);
        }
        return true;
    }

    bool decode_truncate(const uint8_t* payload, uint16_t len, DecodedTruncate& out) {
        if (len < 10) { return false; }
        out.ts_us = read_u64(payload, 0);
        const uint16_t reason_len = read_u16(payload, 8);
        if (reason_len > 0) {
            if (len < 10u + reason_len) { return false; }
            out.reason.emplace(reinterpret_cast<const char*>(payload + 10), reason_len);
        }
        return true;
    }

    std::vector<uint8_t> encode_compaction_commit(
        uint64_t ts_us,
        const std::vector<std::string>& output_files,
        const std::vector<std::string>& input_files
    ) {
        const auto output_count = static_cast<uint8_t>(output_files.size());
        const auto input_count = static_cast<uint8_t>(input_files.size());

        // Fixed: 12 bytes + [u16 len + bytes] per output + [u16 len + bytes] per input
        size_t var_size = 0;
        for (const auto& s : output_files) { var_size += 2 + s.size(); }
        for (const auto& s : input_files) { var_size += 2 + s.size(); }

        std::vector<uint8_t> p(12 + var_size);
        uint8_t* b = p.data();
        write_u64(b, 0, ts_us);
        b[8] = output_count;
        b[9] = input_count;
        write_u16(b, 10, 0); // reserved

        size_t off = 12;
        for (const auto& s : output_files) {
            write_u16(b, off, static_cast<uint16_t>(s.size()));
            off += 2;
            std::memcpy(b + off, s.data(), s.size());
            off += s.size();
        }
        for (const auto& s : input_files) {
            write_u16(b, off, static_cast<uint16_t>(s.size()));
            off += 2;
            std::memcpy(b + off, s.data(), s.size());
            off += s.size();
        }
        return p;
    }

    bool decode_compaction_commit(const uint8_t* payload, uint16_t len, DecodedCompactionCommit& out) {
        if (len < 12) { return false; }
        out.ts_us = read_u64(payload, 0);
        const uint8_t output_count = payload[8];
        const uint8_t input_count = payload[9];

        size_t cursor = 12;
        out.output_files.clear();
        out.output_files.reserve(output_count);
        for (uint8_t i = 0; i < output_count; ++i) {
            std::string s;
            if (!read_length_prefixed(payload, len, cursor, s)) { return false; }
            out.output_files.push_back(std::move(s));
        }
        out.input_files.clear();
        out.input_files.reserve(input_count);
        for (uint8_t i = 0; i < input_count; ++i) {
            std::string s;
            if (!read_length_prefixed(payload, len, cursor, s)) { return false; }
            out.input_files.push_back(std::move(s));
        }
        return true;
    }

    // ============================================================================
    // Cluster event encode / decode (v4)
    // ============================================================================

    std::vector<uint8_t> encode_node_join(uint64_t ts_us, uint64_t node_id, uint16_t repl_port, const std::string& host) {
        const uint16_t host_len = static_cast<uint16_t>(host.size());
        std::vector<uint8_t> p(20 + host_len);
        uint8_t* b = p.data();
        write_u64(b, 0, ts_us);
        write_u64(b, 8, node_id);
        write_u16(b, 16, repl_port);
        write_u16(b, 18, host_len);
        std::memcpy(b + 20, host.data(), host_len);
        return p;
    }

    std::vector<uint8_t> encode_node_leave(uint64_t ts_us, uint64_t node_id) {
        std::vector<uint8_t> p(16);
        write_u64(p.data(), 0, ts_us);
        write_u64(p.data(), 8, node_id);
        return p;
    }

    std::vector<uint8_t> encode_primary_lease(uint64_t ts_us, uint64_t node_id, uint64_t lease_until_us) {
        std::vector<uint8_t> p(24);
        write_u64(p.data(), 0, ts_us);
        write_u64(p.data(), 8, node_id);
        write_u64(p.data(), 16, lease_until_us);
        return p;
    }

    bool decode_node_join(const uint8_t* payload, uint16_t len, DecodedNodeJoin& out) {
        if (len < 20) { return false; }
        out.ts_us = read_u64(payload, 0);
        out.node_id = read_u64(payload, 8);
        out.repl_port = read_u16(payload, 16);
        const uint16_t host_len = read_u16(payload, 18);
        if (len < 20u + host_len) { return false; }
        out.host.assign(reinterpret_cast<const char*>(payload + 20), host_len);
        return true;
    }

    bool decode_node_leave(const uint8_t* payload, uint16_t len, DecodedNodeLeave& out) {
        if (len < 16) { return false; }
        out.ts_us = read_u64(payload, 0);
        out.node_id = read_u64(payload, 8);
        return true;
    }

    bool decode_primary_lease(const uint8_t* payload, uint16_t len, DecodedPrimaryLease& out) {
        if (len < 24) { return false; }
        out.ts_us = read_u64(payload, 0);
        out.node_id = read_u64(payload, 8);
        out.lease_until_us = read_u64(payload, 16);
        return true;
    }
} // namespace akkaradb::engine::manifest

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

// akkengine/src/engine/wal/WalFraming.cpp
#include "akk/engine/wal/WalFraming.hpp"

#include "akk/cpu/CRC32C.hpp"

#include <array>
#include <cstring>
#include <limits>
#include <stdexcept>

namespace akkaradb::engine::wal {
    namespace {
        void write_u16(uint8_t* out, size_t off, uint16_t v) noexcept {
            out[off] = static_cast<uint8_t>(v & 0xffu);
            out[off + 1] = static_cast<uint8_t>((v >> 8) & 0xffu);
        }

        void write_u32(uint8_t* out, size_t off, uint32_t v) noexcept {
            out[off] = static_cast<uint8_t>(v & 0xffu);
            out[off + 1] = static_cast<uint8_t>((v >> 8) & 0xffu);
            out[off + 2] = static_cast<uint8_t>((v >> 16) & 0xffu);
            out[off + 3] = static_cast<uint8_t>((v >> 24) & 0xffu);
        }

        void write_u64(uint8_t* out, size_t off, uint64_t v) noexcept {
            for (size_t i = 0; i < 8; ++i) { out[off + i] = static_cast<uint8_t>((v >> (i * 8)) & 0xffu); }
        }

        [[nodiscard]] uint16_t read_u16(const uint8_t* in, size_t off) noexcept {
            return static_cast<uint16_t>(in[off]) | (static_cast<uint16_t>(in[off + 1]) << 8);
        }

        [[nodiscard]] uint32_t read_u32(const uint8_t* in, size_t off) noexcept {
            return static_cast<uint32_t>(in[off]) | (static_cast<uint32_t>(in[off + 1]) << 8) | (static_cast<uint32_t>(in[off + 2]) << 16) | (static_cast<
                uint32_t>(in[off + 3]) << 24);
        }

        [[nodiscard]] uint64_t read_u64(const uint8_t* in, size_t off) noexcept {
            uint64_t v = 0;
            for (size_t i = 0; i < 8; ++i) { v |= static_cast<uint64_t>(in[off + i]) << (i * 8); }
            return v;
        }

        [[nodiscard]] uint32_t crc32c_bytes(const uint8_t* data, size_t size) noexcept { return cpu::CRC32C(reinterpret_cast<const std::byte*>(data), size); }
    } // namespace

    bool WalSegmentHeader::verify_checksum() const noexcept {
        std::array<uint8_t, SIZE> buf{};
        WalSegmentHeader tmp = *this;
        const uint32_t stored = tmp.crc32c;
        tmp.crc32c = 0;
        tmp.serialize(buf.data());
        return stored == crc32c_bytes(buf.data(), buf.size());
    }

    WalSegmentHeader WalSegmentHeader::build(uint16_t shard_id_value, uint64_t segment_id_value, uint64_t created_us_value) noexcept {
        WalSegmentHeader hdr{};
        hdr.segment_id = segment_id_value;
        hdr.created_us = created_us_value;
        hdr.first_seq = 0;
        hdr.last_seq = 0;
        hdr.magic = MAGIC;
        hdr.crc32c = 0;
        hdr.version = VERSION;
        hdr.header_size = SIZE;
        hdr.shard_id = shard_id_value;
        hdr.flags = 0;

        std::array<uint8_t, SIZE> buf{};
        hdr.serialize(buf.data());
        hdr.crc32c = crc32c_bytes(buf.data(), buf.size());
        return hdr;
    }

    void WalSegmentHeader::serialize(uint8_t out[SIZE]) const noexcept {
        write_u64(out, 0, segment_id);
        write_u64(out, 8, created_us);
        write_u64(out, 16, first_seq);
        write_u64(out, 24, last_seq);
        write_u32(out, 32, magic);
        write_u32(out, 36, crc32c);
        write_u16(out, 40, version);
        write_u16(out, 42, header_size);
        write_u16(out, 44, shard_id);
        write_u16(out, 46, flags);
    }

    WalSegmentHeader WalSegmentHeader::deserialize(const uint8_t in[SIZE]) noexcept {
        WalSegmentHeader hdr{};
        hdr.segment_id = read_u64(in, 0);
        hdr.created_us = read_u64(in, 8);
        hdr.first_seq = read_u64(in, 16);
        hdr.last_seq = read_u64(in, 24);
        hdr.magic = read_u32(in, 32);
        hdr.crc32c = read_u32(in, 36);
        hdr.version = read_u16(in, 40);
        hdr.header_size = read_u16(in, 42);
        hdr.shard_id = read_u16(in, 44);
        hdr.flags = read_u16(in, 46);
        return hdr;
    }

    bool WalEntryHeader::verify_lengths(size_t max_entry_bytes) const noexcept {
        if (entry_len < SIZE || entry_len > max_entry_bytes) { return false; }
        const uint64_t expected = static_cast<uint64_t>(SIZE) + key_len + value_len;
        return expected == entry_len;
    }

    bool WalEntryHeader::verify_checksum(std::span<const uint8_t> key, std::span<const uint8_t> value) const noexcept {
        return crc32c == entry_crc32c(*this, key, value);
    }

    WalEntryHeader WalEntryHeader::build(
        uint64_t seq_value,
        uint64_t key_fp64_value,
        uint16_t key_len_value,
        uint32_t value_len_value,
        uint16_t flags_value
    ) noexcept {
        WalEntryHeader hdr{};
        hdr.seq = seq_value;
        hdr.key_fp64 = key_fp64_value;
        hdr.entry_len = static_cast<uint32_t>(SIZE + key_len_value + value_len_value);
        hdr.value_len = value_len_value;
        hdr.key_len = key_len_value;
        hdr.flags = flags_value;
        hdr.crc32c = 0;
        return hdr;
    }

    void WalEntryHeader::serialize(uint8_t out[SIZE]) const noexcept {
        write_u64(out, 0, seq);
        write_u64(out, 8, key_fp64);
        write_u32(out, 16, entry_len);
        write_u32(out, 20, value_len);
        write_u16(out, 24, key_len);
        write_u16(out, 26, flags);
        write_u32(out, 28, crc32c);
    }

    WalEntryHeader WalEntryHeader::deserialize(const uint8_t in[SIZE]) noexcept {
        WalEntryHeader hdr{};
        hdr.seq = read_u64(in, 0);
        hdr.key_fp64 = read_u64(in, 8);
        hdr.entry_len = read_u32(in, 16);
        hdr.value_len = read_u32(in, 20);
        hdr.key_len = read_u16(in, 24);
        hdr.flags = read_u16(in, 26);
        hdr.crc32c = read_u32(in, 28);
        return hdr;
    }

    uint32_t entry_crc32c(const WalEntryHeader& header, std::span<const uint8_t> key, std::span<const uint8_t> value) {
        std::vector<uint8_t> buf;
        buf.resize(WalEntryHeader::SIZE + key.size() + value.size());

        WalEntryHeader tmp = header;
        tmp.crc32c = 0;
        tmp.serialize(buf.data());

        uint8_t* p = buf.data() + WalEntryHeader::SIZE;
        if (!key.empty()) {
            std::memcpy(p, key.data(), key.size());
            p += key.size();
        }
        if (!value.empty()) { std::memcpy(p, value.data(), value.size()); }

        return crc32c_bytes(buf.data(), buf.size());
    }

    std::vector<uint8_t> serialize_entry(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t seq, uint64_t key_fp64, uint16_t flags) {
        if (key.size() > std::numeric_limits<uint16_t>::max()) { throw std::invalid_argument("WAL key too large"); }
        if (value.size() > std::numeric_limits<uint32_t>::max()) { throw std::invalid_argument("WAL value too large"); }
        const uint64_t total = static_cast<uint64_t>(WalEntryHeader::SIZE) + key.size() + value.size();
        if (total > std::numeric_limits<uint32_t>::max()) { throw std::invalid_argument("WAL entry too large"); }

        WalEntryHeader hdr = WalEntryHeader::build(seq, key_fp64, static_cast<uint16_t>(key.size()), static_cast<uint32_t>(value.size()), flags);

        std::vector<uint8_t> out;
        out.resize(hdr.entry_len);
        hdr.serialize(out.data());

        uint8_t* p = out.data() + WalEntryHeader::SIZE;
        if (!key.empty()) {
            std::memcpy(p, key.data(), key.size());
            p += key.size();
        }
        if (!value.empty()) { std::memcpy(p, value.data(), value.size()); }

        hdr.crc32c = crc32c_bytes(out.data(), out.size());
        hdr.serialize(out.data());
        return out;
    }
} // namespace akkaradb::engine::wal

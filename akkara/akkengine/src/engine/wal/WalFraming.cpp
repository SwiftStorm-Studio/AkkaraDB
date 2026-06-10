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
        void writeU16(uint8_t* out, size_t off, uint16_t v) noexcept {
            out[off] = static_cast<uint8_t>(v & 0xffu);
            out[off + 1] = static_cast<uint8_t>((v >> 8) & 0xffu);
        }

        void writeU32(uint8_t* out, size_t off, uint32_t v) noexcept {
            out[off] = static_cast<uint8_t>(v & 0xffu);
            out[off + 1] = static_cast<uint8_t>((v >> 8) & 0xffu);
            out[off + 2] = static_cast<uint8_t>((v >> 16) & 0xffu);
            out[off + 3] = static_cast<uint8_t>((v >> 24) & 0xffu);
        }

        void writeU64(uint8_t* out, size_t off, uint64_t v) noexcept {
            for (size_t i = 0; i < 8; ++i) { out[off + i] = static_cast<uint8_t>((v >> (i * 8)) & 0xffu); }
        }

        [[nodiscard]] uint16_t readU16(const uint8_t* in, size_t off) noexcept {
            return static_cast<uint16_t>(in[off]) | (static_cast<uint16_t>(in[off + 1]) << 8);
        }

        [[nodiscard]] uint32_t readU32(const uint8_t* in, size_t off) noexcept {
            return static_cast<uint32_t>(in[off]) | (static_cast<uint32_t>(in[off + 1]) << 8) | (static_cast<uint32_t>(in[off + 2]) << 16) |
                (static_cast<uint32_t>(in[off + 3]) << 24);
        }

        [[nodiscard]] uint64_t readU64(const uint8_t* in, size_t off) noexcept {
            uint64_t v = 0;
            for (size_t i = 0; i < 8; ++i) { v |= static_cast<uint64_t>(in[off + i]) << (i * 8); }
            return v;
        }

        [[nodiscard]] uint32_t crc32cBytes(const uint8_t* data, size_t size) noexcept {
            return cpu::CRC32C(reinterpret_cast<const std::byte*>(data), size);
        }
    } // namespace

    bool WalSegmentHeader::verifyChecksum() const noexcept {
        std::array<uint8_t, SIZE> buf{};
        WalSegmentHeader tmp = *this;
        const uint32_t stored = tmp.crc32c;
        tmp.crc32c = 0;
        tmp.serialize(buf.data());
        return stored == crc32cBytes(buf.data(), buf.size());
    }

    WalSegmentHeader WalSegmentHeader::build(uint16_t shardIdValue, uint64_t segmentIdValue, uint64_t createdUsValue) noexcept {
        WalSegmentHeader hdr{};
        hdr.segmentId = segmentIdValue;
        hdr.createdUs = createdUsValue;
        hdr.firstSeq = 0;
        hdr.lastSeq = 0;
        hdr.magic = MAGIC;
        hdr.crc32c = 0;
        hdr.version = VERSION;
        hdr.headerSize = SIZE;
        hdr.shardId = shardIdValue;
        hdr.flags = 0;

        std::array<uint8_t, SIZE> buf{};
        hdr.serialize(buf.data());
        hdr.crc32c = crc32cBytes(buf.data(), buf.size());
        return hdr;
    }

    void WalSegmentHeader::serialize(uint8_t out[SIZE]) const noexcept {
        writeU64(out, 0, segmentId);
        writeU64(out, 8, createdUs);
        writeU64(out, 16, firstSeq);
        writeU64(out, 24, lastSeq);
        writeU32(out, 32, magic);
        writeU32(out, 36, crc32c);
        writeU16(out, 40, version);
        writeU16(out, 42, headerSize);
        writeU16(out, 44, shardId);
        writeU16(out, 46, flags);
    }

    WalSegmentHeader WalSegmentHeader::deserialize(const uint8_t in[SIZE]) noexcept {
        WalSegmentHeader hdr{};
        hdr.segmentId = readU64(in, 0);
        hdr.createdUs = readU64(in, 8);
        hdr.firstSeq = readU64(in, 16);
        hdr.lastSeq = readU64(in, 24);
        hdr.magic = readU32(in, 32);
        hdr.crc32c = readU32(in, 36);
        hdr.version = readU16(in, 40);
        hdr.headerSize = readU16(in, 42);
        hdr.shardId = readU16(in, 44);
        hdr.flags = readU16(in, 46);
        return hdr;
    }

    bool WalEntryHeader::verifyLengths(size_t maxEntryBytes) const noexcept {
        if (entryLen < SIZE || entryLen > maxEntryBytes) { return false; }
        const uint64_t expected = static_cast<uint64_t>(SIZE) + keyLen + valueLen;
        return expected == entryLen;
    }

    bool WalEntryHeader::verifyChecksum(std::span<const uint8_t> key, std::span<const uint8_t> value) const noexcept {
        return crc32c == entryCrc32c(*this, key, value);
    }

    WalEntryHeader WalEntryHeader::build(
        uint64_t seqValue,
        uint64_t keyFp64Value,
        uint16_t keyLenValue,
        uint32_t valueLenValue,
        uint16_t flagsValue
    ) noexcept {
        WalEntryHeader hdr{};
        hdr.seq = seqValue;
        hdr.keyFp64 = keyFp64Value;
        hdr.entryLen = static_cast<uint32_t>(SIZE + keyLenValue + valueLenValue);
        hdr.valueLen = valueLenValue;
        hdr.keyLen = keyLenValue;
        hdr.flags = flagsValue;
        hdr.crc32c = 0;
        return hdr;
    }

    void WalEntryHeader::serialize(uint8_t out[SIZE]) const noexcept {
        writeU64(out, 0, seq);
        writeU64(out, 8, keyFp64);
        writeU32(out, 16, entryLen);
        writeU32(out, 20, valueLen);
        writeU16(out, 24, keyLen);
        writeU16(out, 26, flags);
        writeU32(out, 28, crc32c);
    }

    WalEntryHeader WalEntryHeader::deserialize(const uint8_t in[SIZE]) noexcept {
        WalEntryHeader hdr{};
        hdr.seq = readU64(in, 0);
        hdr.keyFp64 = readU64(in, 8);
        hdr.entryLen = readU32(in, 16);
        hdr.valueLen = readU32(in, 20);
        hdr.keyLen = readU16(in, 24);
        hdr.flags = readU16(in, 26);
        hdr.crc32c = readU32(in, 28);
        return hdr;
    }

    uint32_t entryCrc32c(const WalEntryHeader& header, std::span<const uint8_t> key, std::span<const uint8_t> value) {
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

        return crc32cBytes(buf.data(), buf.size());
    }

    std::vector<uint8_t> serializeEntry(
        std::span<const uint8_t> key,
        std::span<const uint8_t> value,
        uint64_t seq,
        uint64_t keyFp64,
        uint16_t flags
    ) {
        if (key.size() > std::numeric_limits<uint16_t>::max()) { throw std::invalid_argument("WAL key too large"); }
        if (value.size() > std::numeric_limits<uint32_t>::max()) { throw std::invalid_argument("WAL value too large"); }
        const uint64_t total = static_cast<uint64_t>(WalEntryHeader::SIZE) + key.size() + value.size();
        if (total > std::numeric_limits<uint32_t>::max()) { throw std::invalid_argument("WAL entry too large"); }

        WalEntryHeader hdr = WalEntryHeader::build(
            seq,
            keyFp64,
            static_cast<uint16_t>(key.size()),
            static_cast<uint32_t>(value.size()),
            flags
        );

        std::vector<uint8_t> out;
        out.resize(hdr.entryLen);
        hdr.serialize(out.data());

        uint8_t* p = out.data() + WalEntryHeader::SIZE;
        if (!key.empty()) {
            std::memcpy(p, key.data(), key.size());
            p += key.size();
        }
        if (!value.empty()) { std::memcpy(p, value.data(), value.size()); }

        hdr.crc32c = crc32cBytes(out.data(), out.size());
        hdr.serialize(out.data());
        return out;
    }
} // namespace akkaradb::engine::wal
